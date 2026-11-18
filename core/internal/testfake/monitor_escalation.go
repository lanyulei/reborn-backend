package testfake

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// MonitorEscalations 升级策略、各级通知进度的内存仓储：语义对齐 internal/data/monitor/escalation.go
// （范围过滤、同一归属下重名、乐观锁、被规则引用时不可删除、进度只增不减）。规则引用数经 RuleRefs 预置
// 或由 Rules（规则列表的来源）现算。
type MonitorEscalations struct {
	mu       sync.Mutex
	items    map[string]*biz.Escalation
	progress map[string]map[int32]MonitorEscalationProgress
	writes   int
	// Rules 非空时按它返回的规则统计各策略的引用数（通常传 MonitorStore 的规则视图）。
	Rules func() []*biz.Rule
	// RuleRefs Rules 为空时的引用数：策略 ID → 引用它的规则数。
	RuleRefs map[string]int32
	// Groups 非空时 Create 校验归属分组存在（返回 false 即 ErrGroupNotFound）。
	Groups func(id string) bool
}

// MonitorEscalationProgress 事件在某一级上的通知进度。
type MonitorEscalationProgress struct {
	NotifyCount    int32
	LastNotifyTime time.Time
}

// NewMonitorEscalations 构造空仓储。
func NewMonitorEscalations() *MonitorEscalations {
	return &MonitorEscalations{items: map[string]*biz.Escalation{}, progress: map[string]map[int32]MonitorEscalationProgress{}, RuleRefs: map[string]int32{}}
}

// Seed 预置一条策略（Version 为 0 时取 1）。
func (s *MonitorEscalations) Seed(e *biz.Escalation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneEscalation(e)
	if cp.Version == 0 {
		cp.Version = 1
	}
	s.items[cp.ID] = cp
}

// Remove 直接移除一条策略（模拟已被删除）。
func (s *MonitorEscalations) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
}

// Writes 写入（即 monitor_cfg_version 的 monitor_escalation 行被 bump）的次数。
func (s *MonitorEscalations) Writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

// Progress 事件在某一级上的通知进度；没有记录时 ok 为 false。
func (s *MonitorEscalations) Progress(alertID string, level int32) (MonitorEscalationProgress, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.progress[alertID][level]
	return p, ok
}

func cloneEscalation(e *biz.Escalation) *biz.Escalation {
	cp := *e
	cp.Spec.Levels = slices.Clone(e.Spec.Levels)
	return &cp
}

// Get implements biz.EscalationReader.
func (s *MonitorEscalations) Get(_ context.Context, id string) (*biz.Escalation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[id]
	if !ok {
		return nil, biz.ErrEscalationNotFound
	}
	return cloneEscalation(e), nil
}

// ListByIDs implements biz.EscalationReader.
func (s *MonitorEscalations) ListByIDs(_ context.Context, ids []string) ([]*biz.Escalation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*biz.Escalation
	for _, id := range ids {
		if e, ok := s.items[id]; ok {
			out = append(out, cloneEscalation(e))
		}
	}
	return out, nil
}

// List implements biz.EscalationReader：全局策略在前，其余按名称排序。
func (s *MonitorEscalations) List(_ context.Context, f biz.EscalationFilter, page, pageSize int64) ([]*biz.Escalation, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*biz.Escalation
	for _, e := range s.items {
		global := e.Spec.GroupID == ""
		switch {
		case global && !f.IncludeGlobal,
			!global && !inGroups(f.Groups, e.Spec.GroupID),
			f.Global != nil && global != *f.Global,
			f.Keyword != "" && !strings.Contains(e.Spec.Name, f.Keyword) && !strings.Contains(e.Spec.Description, f.Keyword),
			f.Enabled != nil && e.Spec.Enabled != *f.Enabled:
			continue
		}
		out = append(out, cloneEscalation(e))
	}
	slices.SortFunc(out, func(a, b *biz.Escalation) int {
		if ga, gb := a.Spec.GroupID == "", b.Spec.GroupID == ""; ga != gb {
			if ga {
				return -1
			}
			return 1
		}
		return cmp.Or(cmp.Compare(a.Spec.Name, b.Spec.Name), cmp.Compare(a.ID, b.ID))
	})
	items, total := pageOf(out, page, pageSize)
	return items, total, nil
}

// refs 策略被引用的规则数（调用方持锁）。
func (s *MonitorEscalations) refs(id string) int32 {
	if s.Rules == nil {
		return s.RuleRefs[id]
	}
	var n int32
	for _, r := range s.Rules() {
		if r.Spec.EscalationID == id {
			n++
		}
	}
	return n
}

// RuleCounts implements biz.EscalationUsage.
func (s *MonitorEscalations) RuleCounts(_ context.Context, ids []string) (map[string]int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int32{}
	for _, id := range ids {
		if n := s.refs(id); n > 0 {
			out[id] = n
		}
	}
	return out, nil
}

func (s *MonitorEscalations) nameTaken(groupID, name, exceptID string) bool {
	for _, e := range s.items {
		if e.ID != exceptID && e.Spec.GroupID == groupID && e.Spec.Name == name {
			return true
		}
	}
	return false
}

// Create implements biz.EscalationWriter.
func (s *MonitorEscalations) Create(_ context.Context, e *biz.Escalation, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Spec.GroupID != "" && s.Groups != nil && !s.Groups(e.Spec.GroupID) {
		return biz.ErrGroupNotFound
	}
	if s.nameTaken(e.Spec.GroupID, e.Spec.Name, "") {
		return biz.ErrEscalationNameExists
	}
	cp := cloneEscalation(e)
	now := time.Now()
	cp.Version, cp.CreateTime, cp.UpdateTime, cp.UpdateUser = 1, now, now, actor
	s.items[cp.ID] = cp
	s.writes++
	return nil
}

// Update implements biz.EscalationWriter：不改归属分组。
func (s *MonitorEscalations) Update(_ context.Context, e *biz.Escalation, expectVersion int32, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.items[e.ID]
	switch {
	case !ok:
		return biz.ErrEscalationNotFound
	case cur.Version != expectVersion:
		return biz.ErrVersionConflict
	case s.nameTaken(cur.Spec.GroupID, e.Spec.Name, e.ID):
		return biz.ErrEscalationNameExists
	}
	groupID := cur.Spec.GroupID
	cur.Spec = cloneEscalation(e).Spec
	cur.Spec.GroupID = groupID
	cur.Version, cur.UpdateTime, cur.UpdateUser = cur.Version+1, time.Now(), actor
	s.writes++
	return nil
}

// Delete implements biz.EscalationWriter.
func (s *MonitorEscalations) Delete(_ context.Context, id string, expectVersion int32, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.items[id]
	if !ok {
		return biz.ErrEscalationNotFound
	}
	if n := s.refs(id); n > 0 {
		return biz.EscalationInUse(int(n))
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	delete(s.items, id)
	s.writes++
	return nil
}

// TouchLevelTx implements biz.EscalationProgress：已通知次数只增不减。
func (s *MonitorEscalations) TouchLevelTx(_ context.Context, _ txrun.Tx, alertID string, level, seq int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.progress[alertID] == nil {
		s.progress[alertID] = map[int32]MonitorEscalationProgress{}
	}
	s.progress[alertID][level] = MonitorEscalationProgress{NotifyCount: max(s.progress[alertID][level].NotifyCount, seq), LastNotifyTime: time.Now()}
	return nil
}

// MonitorEngineEvents 自监控事件发件箱的内存仓储（语义对齐 internal/data/monitor/engine_event.go；
// 失败退避不模拟）。
type MonitorEngineEvents struct {
	mu   sync.Mutex
	rows []*MonitorEngineEventRow
}

// MonitorEngineEventRow 自监控事件发件箱的一行。
type MonitorEngineEventRow struct {
	biz.EngineEvent
	Status    int32
	LastError string
}

// NewMonitorEngineEvents 构造空仓储。
func NewMonitorEngineEvents() *MonitorEngineEvents { return &MonitorEngineEvents{} }

// Seed 预置一条待派发的行（引擎的产物）。
func (s *MonitorEngineEvents) Seed(ev biz.EngineEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, &MonitorEngineEventRow{EngineEvent: ev, Status: mc.OutboxPending})
}

// Row 取一行的副本；不存在返回 nil。
func (s *MonitorEngineEvents) Row(id string) *MonitorEngineEventRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.row(id); r != nil {
		cp := *r
		return &cp
	}
	return nil
}

func (s *MonitorEngineEvents) row(id string) *MonitorEngineEventRow {
	for _, r := range s.rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// ClaimTx implements biz.EngineEventRepo：最早入箱的在前；同一对象更早的待派发行挡住后面的。
func (s *MonitorEngineEvents) ClaimTx(_ context.Context, _ txrun.Tx, exclude []string) (*biz.EngineEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocked := map[string]bool{}
	for _, r := range s.rows {
		if r.Status != mc.OutboxPending {
			continue
		}
		if !blocked[r.ObjectID] && !slices.Contains(exclude, r.ID) {
			ev := r.EngineEvent
			return &ev, nil
		}
		blocked[r.ObjectID] = true
	}
	return nil, nil
}

// FinishTx implements biz.EngineEventRepo.
func (s *MonitorEngineEvents) FinishTx(_ context.Context, _ txrun.Tx, id string, status int32, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.row(id); r != nil && r.Status == mc.OutboxPending {
		r.Status, r.LastError = status, reason
	}
	return nil
}

// Fail implements biz.EngineEventRepo.
func (s *MonitorEngineEvents) Fail(_ context.Context, id, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(id)
	if r == nil || r.Status != mc.OutboxPending {
		return false, nil
	}
	r.Attempts++
	r.LastError = reason
	if r.Attempts >= mc.OutboxMaxAttempts {
		r.Status = mc.OutboxDropped
	}
	return r.Status == mc.OutboxDropped, nil
}

// SeedOutboxLevel 预置一条带级数列的待派发发件箱行（升级通知：kind = 4、seq 为编码值、level 为级数；
// 也用来构造不自洽的行）。
func (s *MonitorStore) SeedOutboxLevel(id, alertID string, kind, seq, level int32) {
	s.SeedOutbox(id, alertID, kind, seq)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.ID == id {
			r.Level = level
		}
	}
}

// SetRule 直接改写规则内容（不留版本；测试里绕过用例校验预置引用关系）。
func (s *MonitorStore) SetRule(id string, mutate func(r *biz.Rule)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rules[id]; ok {
		mutate(r)
	}
}
