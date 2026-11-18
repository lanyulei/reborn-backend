package testfake

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// MonitorInhibitStore 抑制规则与记录规则的内存仓储（实现 biz.InhibitReader / InhibitNames / InhibitWriter /
// InhibitStats / RecordRuleReader / RecordRuleWriter / DatasourceWriteTargets）。无事务回滚语义。
// 引用数据（活跃告警的抑制计数、数据源的写入状态、任务行）经导出字段 / Seed* 预置。
type MonitorInhibitStore struct {
	mu       sync.Mutex
	inhibits map[string]*biz.Inhibit
	gone     map[string]string
	records  map[string]*biz.RecordRule
	cfg      int
	// Counts 各抑制规则当前抑制的活跃事件数；Targets 数据源的写入状态；Groups 非 nil 时写入校验分组存在。
	Counts  map[string]int32
	Targets map[string]biz.DatasourceWriteTarget
	Groups  map[string]bool
}

// NewMonitorInhibitStore new a MonitorInhibitStore.
func NewMonitorInhibitStore() *MonitorInhibitStore {
	return &MonitorInhibitStore{
		inhibits: map[string]*biz.Inhibit{}, gone: map[string]string{}, records: map[string]*biz.RecordRule{},
		Counts: map[string]int32{}, Targets: map[string]biz.DatasourceWriteTarget{},
	}
}

// InhibitCfgBumps monitor_inhibit 配置版本被 bump 的次数。
func (s *MonitorInhibitStore) InhibitCfgBumps() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// SeedRecordTask 以评估引擎的身份写入记录规则的任务行。
func (s *MonitorInhibitStore) SeedRecordTask(ruleID string, task biz.RecordTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.records[ruleID]; ok {
		r.Task = &task
	}
}

func (s *MonitorInhibitStore) groupOK(id string) bool {
	return s.Groups == nil || id == "" || s.Groups[id]
}

func inGroupsOrGlobal(groups biz.GroupFilter, includeGlobal bool, groupID string) bool {
	if groupID == "" {
		return includeGlobal
	}
	return inGroups(groups, groupID)
}

// Get implements biz.InhibitReader.
func (s *MonitorInhibitStore) Get(_ context.Context, id string) (*biz.Inhibit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inhibits[id]
	if !ok {
		return nil, biz.ErrInhibitNotFound
	}
	cp := *in
	return &cp, nil
}

// List implements biz.InhibitReader.
func (s *MonitorInhibitStore) List(_ context.Context, f biz.InhibitFilter, page, pageSize int64) ([]*biz.Inhibit, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []*biz.Inhibit
	for _, in := range s.inhibits {
		sp := &in.Spec
		if !inGroupsOrGlobal(f.Groups, f.IncludeGlobal, sp.GroupID) ||
			(f.Keyword != "" && !strings.Contains(sp.Name+sp.Description, f.Keyword)) ||
			(f.Enabled != nil && sp.Enabled != *f.Enabled) {
			continue
		}
		cp := *in
		items = append(items, &cp)
	}
	slices.SortFunc(items, func(a, b *biz.Inhibit) int { return cmp.Compare(b.ID, a.ID) })
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

// ListEnabled implements biz.InhibitReader.
func (s *MonitorInhibitStore) ListEnabled(_ context.Context, limit int) ([]*biz.Inhibit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []*biz.Inhibit
	for _, in := range s.inhibits {
		if in.Spec.Enabled {
			cp := *in
			items = append(items, &cp)
		}
	}
	slices.SortFunc(items, func(a, b *biz.Inhibit) int { return cmp.Compare(a.ID, b.ID) })
	return items[:min(len(items), limit)], nil
}

// InhibitNames implements biz.InhibitNames（含已删除的）。
func (s *MonitorInhibitStore) InhibitNames(_ context.Context, ids []string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for _, id := range ids {
		if in, ok := s.inhibits[id]; ok {
			out[id] = in.Spec.Name
		} else if name, ok := s.gone[id]; ok {
			out[id] = name
		}
	}
	return out, nil
}

// InhibitedCounts implements biz.InhibitStats.
func (s *MonitorInhibitStore) InhibitedCounts(_ context.Context, ids []string) (map[string]int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int32{}
	for _, id := range ids {
		if n := s.Counts[id]; n > 0 {
			out[id] = n
		}
	}
	return out, nil
}

// Create implements biz.InhibitWriter.
func (s *MonitorInhibitStore) Create(_ context.Context, in *biz.Inhibit, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.groupOK(in.Spec.GroupID) {
		return biz.ErrGroupNotFound
	}
	cp := *in
	cp.Version, cp.CreateUser, cp.UpdateUser, cp.CreateTime, cp.UpdateTime = 1, actor, actor, time.Now(), time.Now()
	s.inhibits[in.ID] = &cp
	s.cfg++
	return nil
}

// Update implements biz.InhibitWriter.
func (s *MonitorInhibitStore) Update(_ context.Context, in *biz.Inhibit, expectVersion int32, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.groupOK(in.Spec.GroupID) {
		return biz.ErrGroupNotFound
	}
	cur, ok := s.inhibits[in.ID]
	if !ok {
		return biz.ErrInhibitNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	cur.Spec, cur.Version, cur.UpdateUser, cur.UpdateTime = in.Spec, cur.Version+1, actor, time.Now()
	s.cfg++
	return nil
}

// Delete implements biz.InhibitWriter.
func (s *MonitorInhibitStore) Delete(_ context.Context, id string, expectVersion int32, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.inhibits[id]
	if !ok {
		return biz.ErrInhibitNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	s.gone[id] = cur.Spec.Name
	delete(s.inhibits, id)
	s.cfg++
	return nil
}

// WriteTargets implements biz.DatasourceWriteTargets.
func (s *MonitorInhibitStore) WriteTargets(_ context.Context, ids []string) (map[string]biz.DatasourceWriteTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]biz.DatasourceWriteTarget{}
	for _, id := range ids {
		if t, ok := s.Targets[id]; ok {
			out[id] = t
		}
	}
	return out, nil
}

// RecordRules 返回记录规则仓储视图（biz.RecordRuleReader + biz.RecordRuleWriter）。
func (s *MonitorInhibitStore) RecordRules() MonitorRecordRuleView { return MonitorRecordRuleView{s} }

// MonitorRecordRuleView 记录规则仓储视图。
type MonitorRecordRuleView struct{ s *MonitorInhibitStore }

func recordStatus(r *biz.RecordRule) int32 {
	if !r.Spec.Enabled || r.Task == nil {
		return mc.RecordNever
	}
	return r.Task.LastStatus
}

func cloneRecord(r *biz.RecordRule) *biz.RecordRule {
	cp := *r
	if r.Task != nil {
		t := *r.Task
		cp.Task = &t
	}
	return &cp
}

// List implements biz.RecordRuleReader.
func (v MonitorRecordRuleView) List(_ context.Context, f biz.RecordRuleFilter, page, pageSize int64) ([]*biz.RecordRule, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.RecordRule
	for _, r := range v.s.records {
		sp := &r.Spec
		if !inGroups(f.Groups, sp.GroupID) || (f.DatasourceID != "" && sp.DatasourceID != f.DatasourceID) ||
			(f.Keyword != "" && !strings.Contains(sp.Name+sp.Description+sp.Expr, f.Keyword)) ||
			(f.Enabled != nil && sp.Enabled != *f.Enabled) || (f.LastStatus != nil && recordStatus(r) != *f.LastStatus) {
			continue
		}
		items = append(items, cloneRecord(r))
	}
	slices.SortFunc(items, func(a, b *biz.RecordRule) int { return cmp.Compare(b.ID, a.ID) })
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

// Get implements biz.RecordRuleReader.
func (v MonitorRecordRuleView) Get(_ context.Context, id string) (*biz.RecordRule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r, ok := v.s.records[id]
	if !ok {
		return nil, biz.ErrRecordRuleNotFound
	}
	return cloneRecord(r), nil
}

func (v MonitorRecordRuleView) find(groupID, datasourceID, name string) string {
	for id, r := range v.s.records {
		if r.Spec.GroupID == groupID && r.Spec.DatasourceID == datasourceID && r.Spec.Name == name {
			return id
		}
	}
	return ""
}

// FindByName implements biz.RecordRuleReader.
func (v MonitorRecordRuleView) FindByName(_ context.Context, groupID, datasourceID, name string) (string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return v.find(groupID, datasourceID, name), nil
}

func (v MonitorRecordRuleView) checkRefs(sp *biz.RecordRuleSpec, selfID string) error {
	if !v.s.groupOK(sp.GroupID) {
		return biz.ErrGroupNotFound
	}
	t, ok := v.s.Targets[sp.DatasourceID]
	if !ok {
		return biz.Detail(biz.ErrRecordRuleInvalid, "the selected datasource no longer exists")
	}
	if sp.Enabled && !t.HasWriteURL {
		return biz.ErrRecordRuleNoWriteURL
	}
	if dup := v.find(sp.GroupID, sp.DatasourceID, sp.Name); dup != "" && dup != selfID {
		return biz.ErrRecordRuleNameExists
	}
	return nil
}

// Create implements biz.RecordRuleWriter.
func (v MonitorRecordRuleView) Create(_ context.Context, r *biz.RecordRule, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.checkRefs(&r.Spec, ""); err != nil {
		return err
	}
	cp := *r
	cp.Version, cp.UpdateUser, cp.CreateTime, cp.UpdateTime = 1, actor, time.Now(), time.Now()
	v.s.records[r.ID] = &cp
	r.Version = 1
	return nil
}

// Update implements biz.RecordRuleWriter.
func (v MonitorRecordRuleView) Update(_ context.Context, r *biz.RecordRule, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.records[r.ID]
	if !ok {
		return biz.ErrRecordRuleNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	if err := v.checkRefs(&r.Spec, r.ID); err != nil {
		return err
	}
	cur.Spec, cur.Version, cur.UpdateUser, cur.UpdateTime = r.Spec, cur.Version+1, actor, time.Now()
	r.Version = cur.Version
	return nil
}

// Delete implements biz.RecordRuleWriter.
func (v MonitorRecordRuleView) Delete(_ context.Context, id string, expectVersion int32, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.records[id]
	if !ok {
		return biz.ErrRecordRuleNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	delete(v.s.records, id)
	return nil
}
