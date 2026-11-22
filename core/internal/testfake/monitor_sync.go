package testfake

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// MonitorSyncStore 监控域资源索引、工作日历快照与事件动作仓储的内存实现（monitor-ci-sync /
// monitor-calendar / monitor-action 的用例测试与 HTTP 层测试用）。并发安全。
type MonitorSyncStore struct {
	mu       sync.Mutex
	cis      map[string]*biz.CIEntry
	syncTime map[string]time.Time
	cfg      map[string]int64
	calendar *mc.CalendarSnapshot
	actions  []*MonitorActionRow
	// syncing 模拟同步锁：一轮同步进行中。
	syncing bool
	// Now 时钟（领取动作、记同步时间用）；缺省 time.Now。
	Now func() time.Time
}

// MonitorActionRow 内存里的一行事件动作。
type MonitorActionRow struct {
	ID        string
	AlertID   string
	Kind      int32
	Status    int32
	Payload   mc.ActionPayload
	Attempts  int32
	NextTime  time.Time
	LastError string
	Done      bool
}

// NewMonitorSyncStore new a MonitorSyncStore.
func NewMonitorSyncStore() *MonitorSyncStore {
	return &MonitorSyncStore{cis: map[string]*biz.CIEntry{}, syncTime: map[string]time.Time{}, cfg: map[string]int64{}, Now: time.Now}
}

func cloneCIEntry(e *biz.CIEntry) *biz.CIEntry {
	cp := *e
	cp.Tags, cp.UpstreamIDs = slices.Clone(e.Tags), slices.Clone(e.UpstreamIDs)
	cp.Attrs = make(map[string][]string, len(e.Attrs))
	for k, v := range e.Attrs {
		cp.Attrs[k] = slices.Clone(v)
	}
	if e.SourceUpdateTime != nil {
		at := *e.SourceUpdateTime
		cp.SourceUpdateTime = &at
	}
	return &cp
}

// SeedCI 放入一行资源索引。
func (s *MonitorSyncStore) SeedCI(e *biz.CIEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cis[e.ID] = cloneCIEntry(e)
	s.syncTime[e.ID] = s.Now()
}

// CI 取一行资源索引的副本；不存在返回 nil。
func (s *MonitorSyncStore) CI(id string) *biz.CIEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.cis[id]; ok {
		return cloneCIEntry(e)
	}
	return nil
}

// CfgVersionOf 某张配置表的版本号（未写过为 0）。
func (s *MonitorSyncStore) CfgVersionOf(name string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg[name]
}

// SeedAction 放入一个待执行的事件动作（立即可领取）。
func (s *MonitorSyncStore) SeedAction(id, alertID string, kind int32, payload mc.ActionPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, &MonitorActionRow{ID: id, AlertID: alertID, Kind: kind, Status: mc.ActionPending, Payload: payload, NextTime: s.Now()})
}

// Action 取一行事件动作的副本；不存在返回 nil。
func (s *MonitorSyncStore) Action(id string) *MonitorActionRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.actions {
		if a.ID == id {
			cp := *a
			return &cp
		}
	}
	return nil
}

// SetAction 修改一行事件动作（模拟租约到期、别的实例重新领取等）。
func (s *MonitorSyncStore) SetAction(id string, mutate func(a *MonitorActionRow)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.actions {
		if a.ID == id {
			mutate(a)
		}
	}
}

// monitorCIIndexView 资源索引的读写视图。
type monitorCIIndexView struct{ s *MonitorSyncStore }

// CIIndex 资源索引仓储（读、配置版本、同步入口）。
func (s *MonitorSyncStore) CIIndex() interface {
	biz.CIIndexReader
	biz.CfgVersionReader
	biz.CIIndexStore
} {
	return monitorCIIndexView{s}
}

// Briefs implements biz.CIIndexReader.
func (v monitorCIIndexView) Briefs(_ context.Context, ids []string) (map[string]*biz.CIEntry, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := make(map[string]*biz.CIEntry, len(ids))
	for _, id := range ids {
		if e, ok := v.s.cis[id]; ok {
			out[id] = cloneCIEntry(e)
		}
	}
	return out, nil
}

// ListValid implements biz.CIIndexReader.
func (v monitorCIIndexView) ListValid(_ context.Context, limit int) ([]*biz.CIEntry, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := make([]*biz.CIEntry, 0, len(v.s.cis))
	for _, id := range slices.Sorted(maps.Keys(v.s.cis)) {
		if e := v.s.cis[id]; e.Valid && len(out) < limit {
			out = append(out, cloneCIEntry(e))
		}
	}
	return out, nil
}

// Summary implements biz.CIIndexReader.
func (v monitorCIIndexView) Summary(context.Context) (int32, *time.Time, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var valid int32
	var last *time.Time
	for id, e := range v.s.cis {
		if e.Valid {
			valid++
		}
		if at := v.s.syncTime[id]; last == nil || at.After(*last) {
			last = &at
		}
	}
	return valid, last, nil
}

// CfgVersion implements biz.CfgVersionReader.
func (v monitorCIIndexView) CfgVersion(_ context.Context, name string) (int64, error) {
	return v.s.CfgVersionOf(name), nil
}

// Sync implements biz.CIIndexStore：同步锁被占用时不执行；fn 出错时本轮的写入全部回滚。
func (v monitorCIIndexView) Sync(ctx context.Context, fn func(ctx context.Context, w biz.CIIndexWriter, r biz.CIIndexReconciler) (int, error)) (bool, error) {
	v.s.mu.Lock()
	if v.s.syncing {
		v.s.mu.Unlock()
		return false, nil
	}
	v.s.syncing = true
	tx := &monitorCIIndexTx{now: v.s.Now(), cis: make(map[string]*biz.CIEntry, len(v.s.cis)), touched: map[string]struct{}{}}
	for id, e := range v.s.cis {
		tx.cis[id] = cloneCIEntry(e)
	}
	v.s.mu.Unlock()

	changed, err := fn(ctx, tx, tx)

	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	v.s.syncing = false
	if err != nil {
		return true, err
	}
	v.s.cis = tx.cis
	for id := range tx.touched {
		v.s.syncTime[id] = tx.now
	}
	if changed > 0 {
		v.s.cfg[mc.CfgCIIndex]++
	}
	return true, nil
}

// monitorCIIndexTx 一轮同步里的工作副本（只被这一轮使用）。
type monitorCIIndexTx struct {
	now     time.Time
	cis     map[string]*biz.CIEntry
	touched map[string]struct{}
}

// HighWater implements biz.CIIndexWriter.
func (t *monitorCIIndexTx) HighWater(context.Context) (*time.Time, error) {
	var hw *time.Time
	for _, e := range t.cis {
		if e.SourceUpdateTime != nil && (hw == nil || e.SourceUpdateTime.After(*hw)) {
			at := *e.SourceUpdateTime
			hw = &at
		}
	}
	return hw, nil
}

func sameCIEntry(a, b *biz.CIEntry) bool {
	return a.ModelCode == b.ModelCode && a.Name == b.Name && a.Env == b.Env && a.Status == b.Status && a.Owner == b.Owner &&
		a.Valid == b.Valid && slices.Equal(a.Tags, b.Tags) && maps.EqualFunc(a.Attrs, b.Attrs, slices.Equal[[]string])
}

// Upsert implements biz.CIIndexWriter：内容没变的行不算写入；上游资源不动；更新时间只进不退。
func (t *monitorCIIndexTx) Upsert(_ context.Context, entries []*biz.CIEntry) (int, error) {
	changed := 0
	for _, in := range entries {
		e := cloneCIEntry(in)
		cur, ok := t.cis[e.ID]
		if ok {
			e.UpstreamIDs = cur.UpstreamIDs
			if cur.SourceUpdateTime != nil && (e.SourceUpdateTime == nil || cur.SourceUpdateTime.After(*e.SourceUpdateTime)) {
				e.SourceUpdateTime = cur.SourceUpdateTime
			}
			sameTime := (cur.SourceUpdateTime == nil) == (e.SourceUpdateTime == nil) &&
				(cur.SourceUpdateTime == nil || cur.SourceUpdateTime.Equal(*e.SourceUpdateTime))
			if sameCIEntry(cur, e) && sameTime {
				continue
			}
		} else {
			e.UpstreamIDs = nil
		}
		t.cis[e.ID] = e
		t.touched[e.ID] = struct{}{}
		changed++
	}
	return changed, nil
}

// InvalidateExcept implements biz.CIIndexReconciler.
func (t *monitorCIIndexTx) InvalidateExcept(_ context.Context, keep []string) (int, error) {
	kept := make(map[string]struct{}, len(keep))
	for _, id := range keep {
		kept[id] = struct{}{}
	}
	changed := 0
	for id, e := range t.cis {
		if _, ok := kept[id]; !ok && e.Valid {
			e.Valid = false
			t.touched[id] = struct{}{}
			changed++
		}
	}
	return changed, nil
}

// SetUpstreams implements biz.CIIndexReconciler.
func (t *monitorCIIndexTx) SetUpstreams(_ context.Context, upstreams map[string][]string) (int, error) {
	changed := 0
	for id, e := range t.cis {
		if want := upstreams[id]; !slices.Equal(e.UpstreamIDs, want) {
			e.UpstreamIDs = slices.Clone(want)
			t.touched[id] = struct{}{}
			changed++
		}
	}
	return changed, nil
}

// monitorCalendarView 工作日历快照的读写视图。
type monitorCalendarView struct{ s *MonitorSyncStore }

// Calendar 工作日历快照仓储。
func (s *MonitorSyncStore) Calendar() interface {
	biz.CalendarReader
	biz.CalendarStore
} {
	return monitorCalendarView{s}
}

func sameCalendar(a, b *mc.CalendarSnapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Timezone == b.Timezone && slices.Equal(a.Weekdays, b.Weekdays) && slices.Equal(a.Segments, b.Segments) &&
		slices.Equal(a.Holidays, b.Holidays) && slices.Equal(a.Workdays, b.Workdays)
}

// Snapshot implements biz.CalendarReader.
func (v monitorCalendarView) Snapshot(context.Context) (mc.CalendarSnapshot, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.calendar == nil {
		return mc.CalendarSnapshot{}, nil
	}
	return *v.s.calendar, nil
}

// Save implements biz.CalendarStore.
func (v monitorCalendarView) Save(_ context.Context, snap *mc.CalendarSnapshot) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if sameCalendar(v.s.calendar, snap) {
		return false, nil
	}
	v.s.calendar = nil
	if snap != nil {
		cp := *snap
		v.s.calendar = &cp
	}
	v.s.cfg[mc.CfgCalendar]++
	return true, nil
}

// monitorActionView 事件动作仓储视图。
type monitorActionView struct{ s *MonitorSyncStore }

// Actions 事件动作仓储。
func (s *MonitorSyncStore) Actions() biz.ActionRepo { return monitorActionView{s} }

// Claim implements biz.ActionRepo.
func (v monitorActionView) Claim(_ context.Context, limit int, lease time.Duration) ([]*biz.AlertAction, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	now := v.s.Now()
	var out []*biz.AlertAction
	for _, a := range v.s.actions {
		if len(out) >= limit {
			break
		}
		if a.Status != mc.ActionPending || a.NextTime.After(now) {
			continue
		}
		// 同一事件的自动建单还没有结论时，不领取它的「工单追加恢复记录」（与仓储同一口径）。
		if a.Kind == mc.ActionTicketNote && slices.ContainsFunc(v.s.actions, func(t *MonitorActionRow) bool {
			return t.AlertID == a.AlertID && t.Kind == mc.ActionTicket && t.Status == mc.ActionPending
		}) {
			continue
		}
		a.Attempts++
		a.NextTime = now.Add(lease)
		out = append(out, &biz.AlertAction{ID: a.ID, AlertID: a.AlertID, Kind: a.Kind, Payload: a.Payload, Attempts: a.Attempts})
	}
	return out, nil
}

func (v monitorActionView) held(a *biz.AlertAction) *MonitorActionRow {
	for _, row := range v.s.actions {
		if row.ID == a.ID && row.Status == mc.ActionPending && row.Attempts == a.Attempts {
			return row
		}
	}
	return nil
}

// Renew implements biz.ActionRepo.
func (v monitorActionView) Renew(_ context.Context, a *biz.AlertAction, lease time.Duration) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	row := v.held(a)
	if row == nil {
		return false, nil
	}
	row.NextTime = v.s.Now().Add(lease)
	return true, nil
}

// FinishTx implements biz.ActionRepo.
func (v monitorActionView) FinishTx(_ context.Context, _ txrun.Tx, a *biz.AlertAction, status int32, note string) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	row := v.held(a)
	if row == nil {
		return false, nil
	}
	row.Status, row.LastError, row.Done = status, note, true
	return true, nil
}

// Retry implements biz.ActionRepo.
func (v monitorActionView) Retry(_ context.Context, a *biz.AlertAction, backoff time.Duration, reason string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if row := v.held(a); row != nil {
		row.LastError, row.NextTime = reason, v.s.Now().Add(backoff)
	}
	return nil
}
