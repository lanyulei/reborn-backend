package testfake

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 监控域统计 / 引擎状态 / 聚合视图 / 班次告警数的内存仓储。统计直接在 MonitorStore 预置的告警事件与通知
// 记录上计算，口径与 data 层的 SQL 一致。

// MonitorStats 告警统计的内存实现（biz.StatsReader + biz.StatsRanker）。
type MonitorStats struct {
	s *MonitorStore
	// mu 保护以下各项。
	mu sync.Mutex
	// mutedOnce / inhibitedOnce 时间线里有过屏蔽 / 抑制条目的事件。
	mutedOnce, inhibitedOnce map[string]bool
	calls                    int
}

// Stats 返回基于本仓储告警数据的统计实现。
func (s *MonitorStore) Stats() *MonitorStats {
	return &MonitorStats{s: s, mutedOnce: map[string]bool{}, inhibitedOnce: map[string]bool{}}
}

// MarkTimeline 预置「事件曾被屏蔽 / 抑制过」（时间线里的 mute / inhibit 条目）。
func (m *MonitorStats) MarkTimeline(alertID string, muted, inhibited bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mutedOnce[alertID], m.inhibitedOnce[alertID] = muted, inhibited
}

// Calls 仓储被查询的次数（断言空范围不查询）。
func (m *MonitorStats) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *MonitorStats) called() {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
}

// alerts 取满足 keep 的事件副本指针（按 ID 排序，结果确定）。
func (m *MonitorStats) alerts(groups biz.GroupFilter, keep func(*biz.Alert) bool) []*biz.Alert {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	var out []*biz.Alert
	for _, a := range m.s.alerts {
		if inGroups(groups, a.GroupID) && keep(a) {
			out = append(out, a)
		}
	}
	slices.SortFunc(out, func(a, b *biz.Alert) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func inRange(r biz.StatsRange) func(*biz.Alert) bool {
	return func(a *biz.Alert) bool { return !a.FirstFireTime.Before(r.Start) && a.FirstFireTime.Before(r.End) }
}

// Overview implements biz.StatsReader.
func (m *MonitorStats) Overview(_ context.Context, groups biz.GroupFilter, dayStart, dayEnd time.Time) (*biz.AlertOverview, error) {
	m.called()
	var o biz.AlertOverview
	for _, a := range m.alerts(groups, func(*biz.Alert) bool { return true }) {
		if a.Status == mc.AlertFiring {
			o.Firing++
			o.FiringBySeverity[a.Severity-1]++
			if a.HandleState() == biz.HandleUnacked {
				o.Unacked++
			}
			if a.Muted {
				o.Muted++
			}
			if a.Inhibited {
				o.Inhibited++
			}
		}
		if !a.FirstFireTime.Before(dayStart) && a.FirstFireTime.Before(dayEnd) {
			o.TodayFired++
		}
		if a.ResolveTime != nil && !a.ResolveTime.Before(dayStart) && a.ResolveTime.Before(dayEnd) {
			switch a.Status {
			case mc.AlertResolved:
				o.TodayResolved++
			case mc.AlertClosed:
				o.TodayClosed++
			}
		}
	}
	return &o, nil
}

func dimKey(a *biz.Alert, dim string) string {
	if dim == biz.StatsByGroup {
		return a.GroupID
	}
	return strconv.Itoa(int(a.Severity))
}

// Trend implements biz.StatsReader.
func (m *MonitorStats) Trend(_ context.Context, r biz.StatsRange, dim, timezone string) ([]biz.StatsCell, error) {
	m.called()
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, err
	}
	counts := map[[2]string]int32{}
	for _, a := range m.alerts(r.Groups, inRange(r)) {
		counts[[2]string{a.FirstFireTime.In(loc).Format(time.DateOnly), dimKey(a, dim)}]++
	}
	out := make([]biz.StatsCell, 0, len(counts))
	for k, n := range counts {
		out = append(out, biz.StatsCell{Day: k[0], Key: k[1], Count: n})
	}
	slices.SortFunc(out, func(a, b biz.StatsCell) int { return cmp.Or(cmp.Compare(a.Day, b.Day), cmp.Compare(a.Key, b.Key)) })
	return out, nil
}

// Efficiency implements biz.StatsReader.
func (m *MonitorStats) Efficiency(_ context.Context, r biz.StatsRange, dim string) ([]biz.EfficiencyRow, error) {
	m.called()
	rows := map[string]*biz.EfficiencyRow{}
	for _, a := range m.alerts(r.Groups, inRange(r)) {
		k := dimKey(a, dim)
		row := rows[k]
		if row == nil {
			row = &biz.EfficiencyRow{Key: k}
			rows[k] = row
		}
		row.Alerts++
		if a.AckTime != nil {
			row.Acked++
			row.AckSumS += a.AckTime.Sub(a.FirstFireTime).Seconds()
		}
		if a.Status == mc.AlertResolved && a.ResolveTime != nil {
			row.Resolved++
			row.ResolveSumS += a.ResolveTime.Sub(a.FirstFireTime).Seconds()
		}
	}
	out := make([]biz.EfficiencyRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	slices.SortFunc(out, func(a, b biz.EfficiencyRow) int { return cmp.Compare(a.Key, b.Key) })
	return out, nil
}

// Top implements biz.StatsRanker.
func (m *MonitorStats) Top(_ context.Context, r biz.StatsRange, by biz.TopBy, limit int) ([]biz.TopRow, int64, error) {
	m.called()
	rows := map[string]*biz.TopRow{}
	latest := map[string]time.Time{}
	var total int64
	for _, a := range m.alerts(r.Groups, inRange(r)) {
		k, ok := a.RuleID, true
		switch by.Kind {
		case biz.TopByCI:
			k, ok = a.CIID, a.CIID != ""
		case biz.TopByLabel:
			k, ok = a.Labels[by.Label]
		}
		if !ok {
			continue
		}
		total++
		row := rows[k]
		if row == nil {
			row = &biz.TopRow{Key: k}
			rows[k] = row
		}
		row.Count++
		row.MaxSeverity = max(row.MaxSeverity, a.Severity)
		if a.Status == mc.AlertFiring {
			row.Firing++
		}
		if by.Kind == biz.TopByRule && !a.FirstFireTime.Before(latest[k]) {
			latest[k], row.Name, row.GroupID = a.FirstFireTime, a.RuleName, a.GroupID
		}
	}
	out := make([]biz.TopRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, *row)
	}
	slices.SortFunc(out, func(a, b biz.TopRow) int { return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Key, b.Key)) })
	return out[:min(len(out), limit)], total, nil
}

// Noise implements biz.StatsRanker.
func (m *MonitorStats) Noise(_ context.Context, r biz.StatsRange) (*biz.AlertNoise, error) {
	m.called()
	alerts := m.alerts(r.Groups, inRange(r))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	var n biz.AlertNoise
	for _, a := range alerts {
		n.Alerts++
		if a.Muted || m.mutedOnce[a.ID] {
			n.Muted++
		}
		if a.Inhibited || m.inhibitedOnce[a.ID] {
			n.Inhibited++
		}
		notified := a.NotifyCount > 0
		for _, l := range m.s.logs {
			if l.AlertID != a.ID {
				continue
			}
			notified = true
			switch l.Source {
			case mc.NotifySourceRule:
				n.RuleNotifications++
			case mc.NotifySourceSubscription:
				n.SubscriptionNotifications++
			case mc.NotifySourceEscalation:
				n.EscalationNotifications++
			}
		}
		if notified {
			n.Notified++
		}
	}
	return &n, nil
}

// SeedNotifyLog 预置一条通知记录（统计与班次告警数用；派发用例自己写的记录不必预置）。
func (s *MonitorStore) SeedNotifyLog(l *biz.NotifyLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *l
	s.logs = append(s.logs, &cp)
}

// ShiftCounter 基于本仓储通知记录的班次告警数实现（biz.ShiftAlertCounter）。
func (s *MonitorStore) ShiftCounter() biz.ShiftAlertCounter { return monitorShiftView{s} }

type monitorShiftView struct{ s *MonitorStore }

// CountNotified implements biz.ShiftAlertCounter.
func (v monitorShiftView) CountNotified(_ context.Context, windows []biz.ShiftWindow) ([]int32, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := make([]int32, len(windows))
	for i, w := range windows {
		seen := map[string]struct{}{}
		for _, l := range v.s.logs {
			if slices.Contains(l.Recipients, w.UserID) && !l.CreateTime.Before(w.Start) && l.CreateTime.Before(w.End) {
				seen[l.AlertID] = struct{}{}
			}
		}
		out[i] = int32(len(seen)) // #nosec G115 -- 测试数据
	}
	return out, nil
}

// MonitorEngine 引擎运行状态的内存实现（biz.EngineReader + biz.EngineIssues）：字段即返回值。
type MonitorEngine struct {
	InstanceItems   []*biz.EngineInstance
	SummaryValue    biz.EngineSummary
	DatasourceItems []*biz.EngineDatasourceIssue
	RuleItems       []*biz.EngineRuleIssue
}

// Instances implements biz.EngineReader.
func (e *MonitorEngine) Instances(context.Context, time.Duration, time.Duration) ([]*biz.EngineInstance, error) {
	return slices.Clone(e.InstanceItems), nil
}

// Summary implements biz.EngineReader.
func (e *MonitorEngine) Summary(context.Context) (*biz.EngineSummary, error) {
	s := e.SummaryValue
	return &s, nil
}

// Datasources implements biz.EngineIssues.
func (e *MonitorEngine) Datasources(_ context.Context, limit int) ([]*biz.EngineDatasourceIssue, error) {
	return slices.Clone(e.DatasourceItems[:min(len(e.DatasourceItems), limit)]), nil
}

// Rules implements biz.EngineIssues.
func (e *MonitorEngine) Rules(_ context.Context, groups biz.GroupFilter, threshold, limit int) ([]*biz.EngineRuleIssue, error) {
	out := make([]*biz.EngineRuleIssue, 0, len(e.RuleItems))
	for _, r := range e.RuleItems {
		if inGroups(groups, r.GroupID) && int(r.Failures) >= threshold && len(out) < limit {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

// MonitorViews 聚合视图的内存实现（biz.SavedViewReader + biz.SavedViewWriter）。
type MonitorViews struct {
	mu    sync.Mutex
	items map[string]*biz.SavedView
}

// NewMonitorViews 构造空的聚合视图仓储。
func NewMonitorViews() *MonitorViews { return &MonitorViews{items: map[string]*biz.SavedView{}} }

func cloneView(v *biz.SavedView) *biz.SavedView {
	cp := *v
	cp.Spec.Filters.Severities = slices.Clone(v.Spec.Filters.Severities)
	cp.Spec.Filters.Labels = slices.Clone(v.Spec.Filters.Labels)
	return &cp
}

// ListVisible implements biz.SavedViewReader.
func (m *MonitorViews) ListVisible(_ context.Context, ownerID string, limit int) ([]*biz.SavedView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*biz.SavedView, 0, len(m.items))
	for _, v := range m.items {
		if v.Spec.IsPublic || v.OwnerID == ownerID {
			out = append(out, cloneView(v))
		}
	}
	slices.SortFunc(out, func(a, b *biz.SavedView) int {
		if a.Spec.IsPublic != b.Spec.IsPublic {
			if a.Spec.IsPublic {
				return -1
			}
			return 1
		}
		return cmp.Or(cmp.Compare(a.Spec.Name, b.Spec.Name), cmp.Compare(a.ID, b.ID))
	})
	return out[:min(len(out), limit)], nil
}

// Get implements biz.SavedViewReader.
func (m *MonitorViews) Get(_ context.Context, id string) (*biz.SavedView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[id]
	if !ok {
		return nil, biz.ErrAlertViewNotFound
	}
	return cloneView(v), nil
}

func (m *MonitorViews) nameTaken(ownerID, name, exceptID string) bool {
	for _, v := range m.items {
		if v.OwnerID == ownerID && v.Spec.Name == name && v.ID != exceptID {
			return true
		}
	}
	return false
}

// Create implements biz.SavedViewWriter.
func (m *MonitorViews) Create(_ context.Context, v *biz.SavedView, maxPersonal int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !v.Spec.IsPublic {
		n := 0
		for _, it := range m.items {
			if it.OwnerID == v.OwnerID && !it.Spec.IsPublic {
				n++
			}
		}
		if n >= maxPersonal {
			return biz.Detail(biz.ErrAlertViewInvalid, "at most %d personal views per user", maxPersonal)
		}
	}
	if m.nameTaken(v.OwnerID, v.Spec.Name, "") {
		return biz.ErrAlertViewNameExists
	}
	v.CreateTime = time.Now()
	v.UpdateTime = v.CreateTime
	m.items[v.ID] = cloneView(v)
	return nil
}

// Update implements biz.SavedViewWriter.
func (m *MonitorViews) Update(_ context.Context, v *biz.SavedView, wasPublic bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[v.ID]
	if !ok || cur.OwnerID != v.OwnerID || cur.Spec.IsPublic != wasPublic {
		return biz.ErrAlertViewNotFound
	}
	if m.nameTaken(v.OwnerID, v.Spec.Name, v.ID) {
		return biz.ErrAlertViewNameExists
	}
	v.CreateTime, v.UpdateTime = cur.CreateTime, time.Now()
	m.items[v.ID] = cloneView(v)
	return nil
}

// Delete implements biz.SavedViewWriter.
func (m *MonitorViews) Delete(_ context.Context, id, ownerID string, wasPublic bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.items[id]
	if !ok || cur.OwnerID != ownerID || cur.Spec.IsPublic != wasPublic {
		return biz.ErrAlertViewNotFound
	}
	delete(m.items, id)
	return nil
}
