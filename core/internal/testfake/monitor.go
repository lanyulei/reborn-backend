package testfake

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// MonitorStore 监控告警域的内存仓储：语义对齐 internal/data/monitor（范围过滤、乐观锁、重名、
// 引用检查、版本快照、发件箱领取顺序）。无事务回滚语义——原子性由 data 层集成测试覆盖。
// 运行表（告警事件、评估任务、发件箱）由评估引擎写，这里经 Seed* 预置。
type MonitorStore struct {
	mu         sync.Mutex
	now        func() time.Time
	groups     map[string]*biz.RuleGroup
	scopes     []*biz.RoleScope
	sources    map[string]*biz.Datasource
	secrets    map[string][]byte
	rules      map[string]*biz.Rule
	deleted    map[string]bool
	versions   map[string][]*biz.RuleVersion
	tasks      map[string][]*biz.EvalTask
	records    []*biz.EvalRecord
	alerts     map[string]*biz.Alert
	logs       []*biz.NotifyLog
	mutes      map[string]*biz.Mute
	subs       map[string]*biz.Subscription
	outbox     []*MonitorOutboxRow
	cfg        map[string]int
	failInsert error
	failCount  int
	// treeReads / countReads 分组树与规则计数被读取的次数（断言派发不做多余的读取）。
	treeReads  int
	countReads int
}

// GroupReads 分组树与规则计数各被读取了多少次。
func (s *MonitorStore) GroupReads() (tree, counts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.treeReads, s.countReads
}

// MonitorOutboxRow 发件箱的一行。
type MonitorOutboxRow struct {
	biz.OutboxItem
	Status    int32
	LastError string
	Create    time.Time
}

// NewMonitorStore 构造内存仓储，预置迁移内置的根分组。
func NewMonitorStore() *MonitorStore {
	s := &MonitorStore{
		now: time.Now, groups: map[string]*biz.RuleGroup{}, sources: map[string]*biz.Datasource{}, secrets: map[string][]byte{},
		rules: map[string]*biz.Rule{}, deleted: map[string]bool{}, versions: map[string][]*biz.RuleVersion{},
		tasks: map[string][]*biz.EvalTask{}, alerts: map[string]*biz.Alert{}, mutes: map[string]*biz.Mute{},
		subs: map[string]*biz.Subscription{}, cfg: map[string]int{},
	}
	s.groups[mc.RootGroupID] = &biz.RuleGroup{ID: mc.RootGroupID, Path: "/" + mc.RootGroupID + "/", Name: "默认分组", CreateTime: s.now(), UpdateTime: s.now()}
	return s
}

// CfgVersion 某张配置表被 bump 的次数。
func (s *MonitorStore) CfgVersion(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg[name]
}

// SeedGroup 预置分组（parentID 为空 = 顶级）。
func (s *MonitorStore) SeedGroup(id, parentID, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := "/"
	if p, ok := s.groups[parentID]; ok {
		path = p.Path
	}
	s.groups[id] = &biz.RuleGroup{ID: id, ParentID: parentID, Path: path + id + "/", Name: name, CreateTime: s.now(), UpdateTime: s.now()}
}

// SeedRoleScope 预置角色授权。
func (s *MonitorStore) SeedRoleScope(roleID, groupID string, level int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopes = append(s.scopes, &biz.RoleScope{RoleID: roleID, GroupID: groupID, Level: level, UpdateTime: s.now()})
}

// SeedAlert 预置告警事件（评估引擎的产物）。
func (s *MonitorStore) SeedAlert(a *biz.Alert) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *a
	s.alerts[a.ID] = &cp
}

// SeedOutbox 预置一条待派发的发件箱行。
func (s *MonitorStore) SeedOutbox(id, alertID string, kind, seq int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox = append(s.outbox, &MonitorOutboxRow{
		OutboxItem: biz.OutboxItem{ID: id, AlertID: alertID, Kind: kind, Seq: seq}, Status: mc.OutboxPending,
		Create: s.now().Add(time.Duration(len(s.outbox)) * time.Millisecond),
	})
}

// SeedEvalTask 预置评估任务。
func (s *MonitorStore) SeedEvalTask(t *biz.EvalTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *t
	s.tasks[t.RuleID] = append(s.tasks[t.RuleID], &cp)
}

// SeedEvalRecord 预置判定记录。
func (s *MonitorStore) SeedEvalRecord(r *biz.EvalRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *r
	s.records = append(s.records, &cp)
}

// RearmOutbox 模拟评估引擎在屏蔽解除后把已丢弃的行重新置为待派发：状态回到待派发，失败次数与
// 错误清零，入箱时间取当前（与 backend/monitor 的 queueOutbox 同语义）。行不是已丢弃状态时不动。
func (s *MonitorStore) RearmOutbox(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.ID == id && r.Status == mc.OutboxDropped {
			r.Status, r.Attempts, r.LastError, r.Create = mc.OutboxPending, 0, "", s.now()
		}
	}
}

// SetAlert 改写告警事件（模拟评估引擎推进状态：屏蔽、恢复、关闭）。
func (s *MonitorStore) SetAlert(id string, mutate func(a *biz.Alert)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.alerts[id]; ok {
		mutate(a)
	}
}

// OutboxRow 读取发件箱行的当前状态（不存在返回 nil）。
func (s *MonitorStore) OutboxRow(id string) *MonitorOutboxRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.outbox {
		if r.ID == id {
			cp := *r
			return &cp
		}
	}
	return nil
}

// NotifyLogs 全部通知记录（按写入顺序）。
func (s *MonitorStore) NotifyLogs() []*biz.NotifyLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.logs)
}

// Alert 读取告警事件的当前状态。
func (s *MonitorStore) Alert(id string) *biz.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.alerts[id]; ok {
		cp := *a
		return &cp
	}
	return nil
}

// FailNextLogInsert 让下一次写通知记录失败（模拟派发事务中途出错）。
func (s *MonitorStore) FailNextLogInsert(err error) { s.FailLogInserts(1, err) }

// FailLogInserts 让接下来 n 次写通知记录失败。
func (s *MonitorStore) FailLogInserts(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failInsert, s.failCount = err, n
}

func inGroups(f biz.GroupFilter, groupID string) bool {
	return f.All || slices.Contains(f.IDs, groupID)
}

func pageOf[T any](items []T, page, pageSize int64) ([]T, int64) {
	total := int64(len(items))
	start := min((page-1)*pageSize, total)
	return items[start:min(start+pageSize, total)], total
}

func cloneRule(r *biz.Rule) *biz.Rule {
	cp := *r
	cp.Spec.Queries = slices.Clone(r.Spec.Queries)
	cp.Spec.DatasourceIDs = slices.Clone(r.Spec.DatasourceIDs)
	return &cp
}

// ---- 分组与角色授权 ----

type monitorGroupView struct{ s *MonitorStore }

// Groups 返回分组读写仓储视图。
func (s *MonitorStore) Groups() interface {
	biz.GroupReader
	biz.GroupWriter
} {
	return monitorGroupView{s}
}

func (v monitorGroupView) ListAll(context.Context) ([]*biz.RuleGroup, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	v.s.treeReads++
	out := make([]*biz.RuleGroup, 0, len(v.s.groups))
	for _, g := range v.s.groups {
		cp := *g
		out = append(out, &cp)
	}
	slices.SortFunc(out, func(a, b *biz.RuleGroup) int {
		return cmp.Or(cmp.Compare(len(a.Path), len(b.Path)), cmp.Compare(a.Sort, b.Sort), cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// RuleCounts implements biz.GroupReader.
func (v monitorGroupView) RuleCounts(context.Context) (map[string]int32, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	v.s.countReads++
	out := map[string]int32{}
	for _, r := range v.s.rules {
		out[r.Spec.GroupID]++
	}
	return out, nil
}

func (v monitorGroupView) nameTaken(parentID, name, exceptID string) bool {
	for _, g := range v.s.groups {
		if g.ParentID == parentID && g.Name == name && g.ID != exceptID {
			return true
		}
	}
	return false
}

func (v monitorGroupView) Create(_ context.Context, g *biz.RuleGroup, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if len(v.s.groups) >= biz.MaxGroups {
		return biz.GroupInvalid("at most %d rule groups", biz.MaxGroups)
	}
	path := "/"
	if g.ParentID != "" {
		p, ok := v.s.groups[g.ParentID]
		if !ok {
			return biz.ErrGroupNotFound
		}
		if strings.Count(p.Path, "/")-1 >= biz.MaxGroupDepth {
			return biz.GroupInvalid("rule groups can be nested at most %d levels", biz.MaxGroupDepth)
		}
		path = p.Path
	}
	if v.nameTaken(g.ParentID, g.Name, "") {
		return biz.ErrGroupNameExists
	}
	cp := *g
	cp.Path, cp.CreateTime, cp.UpdateTime = path+g.ID+"/", v.s.now(), v.s.now()
	g.Path = cp.Path
	v.s.groups[g.ID] = &cp
	v.s.cfg[mc.CfgRuleGroup]++
	return nil
}

func (v monitorGroupView) Update(_ context.Context, g *biz.RuleGroup, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.groups[g.ID]
	if !ok {
		return biz.ErrGroupNotFound
	}
	if v.nameTaken(cur.ParentID, g.Name, g.ID) {
		return biz.ErrGroupNameExists
	}
	cur.Name, cur.Description, cur.Sort, cur.UpdateTime = g.Name, g.Description, g.Sort, v.s.now()
	v.s.cfg[mc.CfgRuleGroup]++
	return nil
}

func (v monitorGroupView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.groups[id]; !ok {
		return biz.ErrGroupNotFound
	}
	for _, g := range v.s.groups {
		if g.ParentID == id {
			return biz.ErrGroupInUse
		}
	}
	for _, r := range v.s.rules {
		if r.Spec.GroupID == id {
			return biz.ErrGroupInUse
		}
	}
	mutes, subs := 0, 0
	for _, m := range v.s.mutes {
		if m.Spec.GroupID == id {
			mutes++
		}
	}
	for _, sub := range v.s.subs {
		if slices.Contains(sub.Spec.GroupIDs, id) {
			subs++
		}
	}
	if mutes > 0 || subs > 0 {
		return biz.GroupReferenced(mutes, subs)
	}
	delete(v.s.groups, id)
	v.s.scopes = slices.DeleteFunc(v.s.scopes, func(rs *biz.RoleScope) bool { return rs.GroupID == id })
	v.s.cfg[mc.CfgRuleGroup]++
	return nil
}

type monitorRoleScopeView struct{ s *MonitorStore }

// RoleScopes 返回角色授权仓储视图。
func (s *MonitorStore) RoleScopes() biz.RoleScopeRepo { return monitorRoleScopeView{s} }

func (v monitorRoleScopeView) filter(keep func(*biz.RoleScope) bool) []*biz.RoleScope {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var out []*biz.RoleScope
	for _, rs := range v.s.scopes {
		if keep(rs) {
			cp := *rs
			out = append(out, &cp)
		}
	}
	return out
}

func (v monitorRoleScopeView) ListByRoles(_ context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	return v.filter(func(rs *biz.RoleScope) bool { return slices.Contains(roleIDs, rs.RoleID) }), nil
}

func (v monitorRoleScopeView) ListByGroups(_ context.Context, groupIDs []string) ([]*biz.RoleScope, error) {
	return v.filter(func(rs *biz.RoleScope) bool { return slices.Contains(groupIDs, rs.GroupID) }), nil
}

func (v monitorRoleScopeView) Replace(_ context.Context, groupID string, items []*biz.RoleScope, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.groups[groupID]; !ok {
		return biz.ErrGroupNotFound
	}
	v.s.scopes = slices.DeleteFunc(v.s.scopes, func(rs *biz.RoleScope) bool { return rs.GroupID == groupID })
	for _, it := range items {
		v.s.scopes = append(v.s.scopes, &biz.RoleScope{RoleID: it.RoleID, GroupID: groupID, Level: it.Level, UpdateUser: actor, UpdateTime: v.s.now()})
	}
	return nil
}

// ---- 数据源 ----

type monitorDatasourceView struct{ s *MonitorStore }

// Datasources 返回数据源仓储视图。
func (s *MonitorStore) Datasources() interface {
	biz.DatasourceReader
	biz.DatasourceLookup
	biz.DatasourceWriter
} {
	return monitorDatasourceView{s}
}

func (v monitorDatasourceView) view(d *biz.Datasource) *biz.Datasource {
	cp := *d
	cp.HasSecret = len(v.s.secrets[d.ID]) > 0
	for _, r := range v.s.rules {
		if slices.Contains(r.Spec.DatasourceIDs, d.ID) {
			cp.RuleCount++
		}
	}
	return &cp
}

func (v monitorDatasourceView) all() []*biz.Datasource {
	out := make([]*biz.Datasource, 0, len(v.s.sources))
	for _, d := range v.s.sources {
		out = append(out, v.view(d))
	}
	slices.SortFunc(out, func(a, b *biz.Datasource) int { return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID)) })
	return out
}

func (v monitorDatasourceView) List(_ context.Context, f biz.DatasourceFilter, page, pageSize int64) ([]*biz.Datasource, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := slices.DeleteFunc(v.all(), func(d *biz.Datasource) bool {
		return (f.Keyword != "" && !strings.Contains(d.Name+d.URL+d.Description, f.Keyword)) ||
			(f.Enabled != nil && d.Enabled != *f.Enabled) || (f.HealthStatus != nil && d.HealthStatus != *f.HealthStatus)
	})
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

func (v monitorDatasourceView) Get(_ context.Context, id string) (*biz.Datasource, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	d, ok := v.s.sources[id]
	if !ok {
		return nil, biz.ErrDatasourceNotFound
	}
	return v.view(d), nil
}

func (v monitorDatasourceView) ListOptions(_ context.Context, enabledOnly bool) ([]*biz.Datasource, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return slices.DeleteFunc(v.all(), func(d *biz.Datasource) bool { return enabledOnly && !d.Enabled }), nil
}

func (v monitorDatasourceView) Briefs(_ context.Context, ids []string) (map[string]biz.DatasourceBrief, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]biz.DatasourceBrief{}
	for _, id := range ids {
		if d, ok := v.s.sources[id]; ok {
			out[id] = biz.DatasourceBrief{Name: d.Name, Enabled: d.Enabled}
		}
	}
	return out, nil
}

func (v monitorDatasourceView) GetSecret(_ context.Context, id string) ([]byte, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.sources[id]; !ok {
		return nil, biz.ErrDatasourceNotFound
	}
	return v.s.secrets[id], nil
}

func (v monitorDatasourceView) put(d *biz.Datasource, actor string) {
	if d.IsDefault {
		for _, o := range v.s.sources {
			if o.ID != d.ID && o.IsDefault {
				o.IsDefault = false
				o.Version++
			}
		}
	}
	cp := *d
	cp.UpdateUser, cp.UpdateTime = actor, v.s.now()
	v.s.sources[d.ID] = &cp
	v.s.cfg[mc.CfgDatasource]++
}

func (v monitorDatasourceView) nameTaken(name, exceptID string) bool {
	for _, d := range v.s.sources {
		if d.Name == name && d.ID != exceptID {
			return true
		}
	}
	return false
}

func (v monitorDatasourceView) Create(_ context.Context, d *biz.Datasource, sealed []byte, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.nameTaken(d.Name, "") {
		return biz.ErrDatasourceNameExists
	}
	d.Version, d.CreateTime = 1, v.s.now()
	v.put(d, actor)
	if len(sealed) > 0 {
		v.s.secrets[d.ID] = sealed
	}
	return nil
}

func (v monitorDatasourceView) Update(_ context.Context, d *biz.Datasource, secret biz.SecretChange, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.sources[d.ID]
	if !ok {
		return biz.ErrDatasourceNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	if v.nameTaken(d.Name, d.ID) {
		return biz.ErrDatasourceNameExists
	}
	d.Version, d.CreateTime = cur.Version+1, cur.CreateTime
	d.HealthStatus, d.HealthError, d.HealthTime = cur.HealthStatus, cur.HealthError, cur.HealthTime
	v.put(d, actor)
	if secret.Set {
		delete(v.s.secrets, d.ID)
		if len(secret.Sealed) > 0 {
			v.s.secrets[d.ID] = secret.Sealed
		}
	}
	return nil
}

func (v monitorDatasourceView) Delete(_ context.Context, id, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.sources[id]; !ok {
		return biz.ErrDatasourceNotFound
	}
	var names []string
	for _, r := range v.s.rules {
		if slices.Contains(r.Spec.DatasourceIDs, id) {
			names = append(names, r.Spec.Name)
		}
	}
	if len(names) > 0 {
		slices.Sort(names)
		return biz.DatasourceInUse(names, len(names))
	}
	delete(v.s.sources, id)
	delete(v.s.secrets, id)
	v.s.cfg[mc.CfgDatasource]++
	return nil
}

// ---- 规则 ----

type monitorRuleView struct{ s *MonitorStore }

// Rules 返回规则仓储视图。
func (s *MonitorStore) Rules() interface {
	biz.RuleReader
	biz.RuleLookup
	biz.RuleWriter
} {
	return monitorRuleView{s}
}

func (v monitorRuleView) sorted(keep func(*biz.Rule) bool) []*biz.Rule {
	var out []*biz.Rule
	for _, r := range v.s.rules {
		if keep(r) {
			out = append(out, cloneRule(r))
		}
	}
	slices.SortFunc(out, func(a, b *biz.Rule) int {
		return cmp.Or(cmp.Compare(a.Spec.GroupID, b.Spec.GroupID), cmp.Compare(a.Spec.Name, b.Spec.Name), cmp.Compare(a.ID, b.ID))
	})
	return out
}

func (v monitorRuleView) List(_ context.Context, f biz.RuleFilter, page, pageSize int64) ([]*biz.Rule, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.sorted(func(r *biz.Rule) bool {
		s := &r.Spec
		if !inGroups(f.Groups, s.GroupID) || (f.Enabled != nil && s.Enabled != *f.Enabled) ||
			(f.EscalationID != "" && s.EscalationID != f.EscalationID) {
			return false
		}
		if f.Keyword != "" && !strings.Contains(s.Name+s.Description+s.Queries[0].Expr, f.Keyword) {
			return false
		}
		if f.Severity != nil && !slices.ContainsFunc(s.Queries, func(q mc.Query) bool { return q.Severity == *f.Severity }) {
			return false
		}
		if f.DatasourceID != "" && s.DatasourceScope != mc.DatasourceScopeAll && !slices.Contains(s.DatasourceIDs, f.DatasourceID) {
			return false
		}
		for k, val := range f.Labels {
			if s.Labels[k] != val {
				return false
			}
		}
		return true
	})
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

func (v monitorRuleView) Get(_ context.Context, id string) (*biz.Rule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r, ok := v.s.rules[id]
	if !ok {
		return nil, biz.ErrRuleNotFound
	}
	return cloneRule(r), nil
}

func (v monitorRuleView) ListByIDs(_ context.Context, ids []string) ([]*biz.Rule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return v.sorted(func(r *biz.Rule) bool { return slices.Contains(ids, r.ID) }), nil
}

func (v monitorRuleView) Names(_ context.Context, ids []string) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]string{}
	for _, id := range ids {
		if r, ok := v.s.rules[id]; ok {
			out[id] = r.Spec.Name
		}
	}
	return out, nil
}

func (v monitorRuleView) FindByNames(_ context.Context, groupID string, names []string) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]string{}
	for _, r := range v.s.rules {
		if r.Spec.GroupID == groupID && slices.Contains(names, r.Spec.Name) {
			out[r.Spec.Name] = r.ID
		}
	}
	return out, nil
}

func (v monitorRuleView) ListByGroups(_ context.Context, groups biz.GroupFilter, limit int) ([]*biz.Rule, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.sorted(func(r *biz.Rule) bool { return inGroups(groups, r.Spec.GroupID) })
	return items[:min(len(items), limit)], nil
}

func (v monitorRuleView) nameTaken(groupID, name, exceptID string) bool {
	for _, r := range v.s.rules {
		if r.Spec.GroupID == groupID && r.Spec.Name == name && r.ID != exceptID {
			return true
		}
	}
	return false
}

func (v monitorRuleView) checkRefs(spec *biz.RuleSpec) error {
	if _, ok := v.s.groups[spec.GroupID]; !ok {
		return biz.ErrGroupNotFound
	}
	for _, id := range spec.DatasourceIDs {
		if _, ok := v.s.sources[id]; !ok {
			return biz.ErrRuleDatasourceMissing
		}
	}
	return nil
}

func (v monitorRuleView) snapshot(r *biz.Rule, change biz.RuleChange) {
	spec := cloneRule(r).Spec
	v.s.versions[r.ID] = append(v.s.versions[r.ID], &biz.RuleVersion{
		RuleID: r.ID, Version: r.Version, Action: change.Action, Summary: change.Summary, OperatorID: change.Operator,
		CreateTime: v.s.now(), Spec: &spec,
	})
}

func (v monitorRuleView) Create(_ context.Context, r *biz.Rule, change biz.RuleChange) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.checkRefs(&r.Spec); err != nil {
		return err
	}
	if v.nameTaken(r.Spec.GroupID, r.Spec.Name, "") {
		return biz.ErrRuleNameExists
	}
	now := v.s.now()
	r.Version, r.SpecTime, r.CreateTime, r.UpdateTime = 1, now, now, now
	v.s.rules[r.ID] = cloneRule(r)
	v.snapshot(r, change)
	return nil
}

func (v monitorRuleView) Update(_ context.Context, r *biz.Rule, expectVersion int32, change biz.RuleChange) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.rules[r.ID]
	if !ok {
		return biz.ErrRuleNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	if err := v.checkRefs(&r.Spec); err != nil {
		return err
	}
	if v.nameTaken(r.Spec.GroupID, r.Spec.Name, r.ID) {
		return biz.ErrRuleNameExists
	}
	r.Version, r.UpdateTime, r.CreateTime, r.SpecTime = cur.Version+1, v.s.now(), cur.CreateTime, cur.SpecTime
	if change.BumpSpecTime {
		r.SpecTime = v.s.now()
	}
	v.s.rules[r.ID] = cloneRule(r)
	v.snapshot(r, change)
	return nil
}

func (v monitorRuleView) Delete(_ context.Context, r *biz.Rule, change biz.RuleChange) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.rules[r.ID]
	if !ok {
		return biz.ErrRuleNotFound
	}
	if cur.Version != r.Version {
		return biz.ErrVersionConflict
	}
	cur.Version++
	v.snapshot(cur, change)
	delete(v.s.rules, r.ID)
	v.s.deleted[r.ID] = true
	return nil
}

type monitorVersionView struct{ s *MonitorStore }

// Versions 返回规则版本仓储视图。
func (s *MonitorStore) Versions() biz.RuleVersionReader { return monitorVersionView{s} }

func (v monitorVersionView) List(_ context.Context, ruleID string, page, pageSize int64) ([]*biz.RuleVersion, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.RuleVersion
	for _, ver := range slices.Backward(v.s.versions[ruleID]) {
		cp := *ver
		cp.Spec = nil
		items = append(items, &cp)
	}
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

func (v monitorVersionView) Get(_ context.Context, ruleID string, version int32) (*biz.RuleVersion, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	for _, ver := range v.s.versions[ruleID] {
		if ver.Version == version {
			cp := *ver
			spec := *ver.Spec
			cp.Spec = &spec
			return &cp, nil
		}
	}
	return nil, biz.ErrRuleVersionNotFound
}

type monitorEvalView struct{ s *MonitorStore }

// Evals 返回评估状态仓储视图。
func (s *MonitorStore) Evals() biz.EvalReader { return monitorEvalView{s} }

func (v monitorEvalView) ListTasks(_ context.Context, ruleIDs []string) (map[string][]*biz.EvalTask, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string][]*biz.EvalTask{}
	for _, id := range ruleIDs {
		for _, t := range v.s.tasks[id] {
			cp := *t
			out[id] = append(out[id], &cp)
		}
	}
	return out, nil
}

func (v monitorEvalView) ListRecords(_ context.Context, f biz.EvalRecordFilter, page, pageSize int64) ([]*biz.EvalRecord, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var items []*biz.EvalRecord
	for _, r := range v.s.records {
		if r.RuleID == f.RuleID && (f.DatasourceID == "" || r.DatasourceID == f.DatasourceID) && (f.Status == nil || r.Status == *f.Status) {
			items = append(items, r)
		}
	}
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

// ---- 告警事件与通知记录 ----

type monitorAlertView struct{ s *MonitorStore }

// Alerts 返回告警事件仓储视图。
func (s *MonitorStore) Alerts() interface {
	biz.AlertReader
	biz.AlertStats
} {
	return monitorAlertView{s}
}

func (v monitorAlertView) match(a *biz.Alert, f biz.AlertFilter) bool {
	switch {
	case !inGroups(f.Groups, a.GroupID),
		len(f.Statuses) > 0 && !slices.Contains(f.Statuses, a.Status),
		len(f.RuleIDs) > 0 && !slices.Contains(f.RuleIDs, a.RuleID),
		len(f.DatasourceIDs) > 0 && !slices.Contains(f.DatasourceIDs, a.DatasourceID),
		len(f.Severities) > 0 && !slices.Contains(f.Severities, a.Severity),
		f.Muted != nil && a.Muted != *f.Muted,
		f.Fingerprint != "" && a.Fingerprint != f.Fingerprint,
		f.Keyword != "" && !strings.Contains(a.RuleName, f.Keyword),
		f.FireStart != nil && a.FirstFireTime.Before(*f.FireStart),
		f.FireEnd != nil && a.FirstFireTime.After(*f.FireEnd),
		f.FireAfter != nil && cmp.Or(a.FirstFireTime.Compare(f.FireAfter.FirstFireTime), cmp.Compare(a.ID, f.FireAfter.ID)) <= 0,
		f.HistorySince != nil && a.Status != mc.AlertFiring && a.LastFireTime.Before(*f.HistorySince):
		return false
	}
	for k, val := range f.Labels {
		if a.Labels[k] != val {
			return false
		}
	}
	switch {
	case f.CIID != "" && a.CIID != f.CIID,
		f.Acked != nil && (a.AckUser != "") != *f.Acked,
		f.Inhibited != nil && a.Inhibited != *f.Inhibited,
		f.AckUserID != "" && a.AckUser != f.AckUserID,
		f.HandleState != 0 && a.HandleState() != f.HandleState:
		return false
	}
	if f.RecipientID != "" {
		notified := slices.ContainsFunc(v.s.logs, func(l *biz.NotifyLog) bool {
			return l.AlertID == a.ID && slices.Contains(l.Recipients, f.RecipientID)
		})
		return notified || (f.OrAckUser && a.AckUser == f.RecipientID)
	}
	return true
}

func (v monitorAlertView) filtered(f biz.AlertFilter) []*biz.Alert {
	var out []*biz.Alert
	for _, a := range v.s.alerts {
		if v.match(a, f) {
			cp := *a
			out = append(out, &cp)
		}
	}
	slices.SortFunc(out, func(a, b *biz.Alert) int {
		if f.FireAfter != nil {
			return cmp.Or(a.FirstFireTime.Compare(b.FirstFireTime), cmp.Compare(a.ID, b.ID))
		}
		return cmp.Or(b.LastFireTime.Compare(a.LastFireTime), cmp.Compare(b.ID, a.ID))
	})
	return out
}

func (v monitorAlertView) List(_ context.Context, f biz.AlertFilter, page, pageSize int64) ([]*biz.Alert, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out, total := pageOf(v.filtered(f), page, pageSize)
	return out, total, nil
}

func (v monitorAlertView) Scan(_ context.Context, f biz.AlertFilter, limit int) ([]*biz.Alert, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.filtered(f)
	return items[:min(len(items), limit)], nil
}

func (v monitorAlertView) Get(_ context.Context, id string) (*biz.Alert, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	a, ok := v.s.alerts[id]
	if !ok {
		return nil, biz.ErrAlertNotFound
	}
	cp := *a
	return &cp, nil
}

func (v monitorAlertView) Aggregate(_ context.Context, by biz.AggregateBy, f biz.AlertFilter, limit int) ([]*biz.AlertAggregate, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	index := map[string]*biz.AlertAggregate{}
	var total int64
	for _, a := range v.filtered(f) {
		key, name := "", ""
		switch by.Kind {
		case biz.AggregateByRule:
			key, name = a.RuleID, a.RuleName
		case biz.AggregateBySeverity:
			key = string(rune('0' + a.Severity))
		case biz.AggregateByGroup:
			key = a.GroupID
		case biz.AggregateByLabel:
			key = a.Labels[by.Label]
		}
		it, ok := index[key]
		if !ok {
			it = &biz.AlertAggregate{Key: key, Name: name}
			index[key] = it
		}
		it.Count++
		it.MaxSeverity = max(it.MaxSeverity, a.Severity)
		it.SeverityCounts[a.Severity-1]++
		if a.LastFireTime.After(it.LastFireTime) {
			it.LastFireTime = a.LastFireTime
		}
		total++
	}
	out := make([]*biz.AlertAggregate, 0, len(index))
	for _, it := range index {
		out = append(out, it)
	}
	slices.SortFunc(out, func(a, b *biz.AlertAggregate) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(a.Key, b.Key))
	})
	return out[:min(len(out), limit)], total, nil
}

func (v monitorAlertView) MutedCounts(_ context.Context, muteIDs []string) (map[string]int32, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]int32{}
	for _, a := range v.s.alerts {
		if a.Status == mc.AlertFiring && a.Muted && slices.Contains(muteIDs, a.MuteID) {
			out[a.MuteID]++
		}
	}
	return out, nil
}

type monitorLogView struct{ s *MonitorStore }

// Logs 返回通知记录读仓储视图。
func (s *MonitorStore) Logs() biz.NotifyLogReader { return monitorLogView{s} }

func (v monitorLogView) ListByAlert(_ context.Context, alertID string, limit int) ([]*biz.NotifyLog, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var out []*biz.NotifyLog
	for _, l := range v.s.logs {
		if l.AlertID == alertID && len(out) < limit {
			cp := *l
			if sub, ok := v.s.subs[l.SubscriptionID]; ok {
				cp.SubscriptionName = sub.Spec.Name
			}
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (v monitorLogView) IsRecipient(_ context.Context, alertID, userID string) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return slices.ContainsFunc(v.s.logs, func(l *biz.NotifyLog) bool {
		return l.AlertID == alertID && slices.Contains(l.Recipients, userID)
	}), nil
}

func (v monitorLogView) SubscriptionHits(_ context.Context, ids []string, since time.Time) (map[string]biz.SubscriptionHit, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]biz.SubscriptionHit{}
	for _, l := range v.s.logs {
		if l.SubscriptionID == "" || !slices.Contains(ids, l.SubscriptionID) {
			continue
		}
		h := out[l.SubscriptionID]
		if l.CreateTime.After(h.LastTime) {
			h.LastTime = l.CreateTime
		}
		if !l.CreateTime.Before(since) {
			h.Count++
		}
		out[l.SubscriptionID] = h
	}
	return out, nil
}

// ---- 屏蔽规则 ----

type monitorMuteView struct{ s *MonitorStore }

// Mutes 返回屏蔽规则仓储视图。
func (s *MonitorStore) Mutes() interface {
	biz.MuteReader
	biz.MuteWriter
} {
	return monitorMuteView{s}
}

func (v monitorMuteView) List(_ context.Context, f biz.MuteFilter, page, pageSize int64) ([]*biz.Mute, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	now := v.s.now()
	var items []*biz.Mute
	for _, m := range v.s.mutes {
		s := &m.Spec
		expired := s.EndTime != nil && !now.Before(*s.EndTime)
		switch {
		case s.GroupID == "" && !f.IncludeGlobal,
			s.GroupID != "" && !inGroups(f.Groups, s.GroupID),
			f.Keyword != "" && !strings.Contains(s.Name+s.Reason, f.Keyword),
			f.Enabled != nil && s.Enabled != *f.Enabled,
			f.Mode != nil && s.Mode != *f.Mode,
			f.State == biz.MuteStateDisabled && s.Enabled,
			f.State == biz.MuteStateExpired && (!s.Enabled || !expired),
			f.Live && (!s.Enabled || expired):
			continue
		}
		cp := *m
		items = append(items, &cp)
	}
	slices.SortFunc(items, func(a, b *biz.Mute) int { return cmp.Compare(b.ID, a.ID) })
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

func (v monitorMuteView) Get(_ context.Context, id string) (*biz.Mute, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	m, ok := v.s.mutes[id]
	if !ok {
		return nil, biz.ErrMuteNotFound
	}
	cp := *m
	return &cp, nil
}

func (v monitorMuteView) Names(_ context.Context, ids []string) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	out := map[string]string{}
	for _, id := range ids {
		if m, ok := v.s.mutes[id]; ok {
			out[id] = m.Spec.Name
		}
	}
	return out, nil
}

func (v monitorMuteView) Create(_ context.Context, m *biz.Mute, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cp := *m
	cp.Version, cp.CreateUser, cp.UpdateUser, cp.CreateTime, cp.UpdateTime = 1, actor, actor, v.s.now(), v.s.now()
	v.s.mutes[m.ID] = &cp
	v.s.cfg[mc.CfgMute]++
	return nil
}

func (v monitorMuteView) Update(_ context.Context, m *biz.Mute, expectVersion int32, actor string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.mutes[m.ID]
	if !ok {
		return biz.ErrMuteNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	cur.Spec, cur.Version, cur.UpdateUser, cur.UpdateTime = m.Spec, cur.Version+1, actor, v.s.now()
	v.s.cfg[mc.CfgMute]++
	return nil
}

func (v monitorMuteView) Delete(_ context.Context, id string, expectVersion int32, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.mutes[id]
	if !ok {
		return biz.ErrMuteNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	delete(v.s.mutes, id)
	v.s.cfg[mc.CfgMute]++
	return nil
}

// ---- 订阅规则 ----

type monitorSubscriptionView struct{ s *MonitorStore }

// Subscriptions 返回订阅规则仓储视图。
func (s *MonitorStore) Subscriptions() interface {
	biz.SubscriptionReader
	biz.SubscriptionWriter
} {
	return monitorSubscriptionView{s}
}

func intersects(a, b []string) bool {
	return slices.ContainsFunc(a, func(x string) bool { return slices.Contains(b, x) })
}

func (v monitorSubscriptionView) sorted(keep func(*biz.Subscription) bool) []*biz.Subscription {
	var out []*biz.Subscription
	for _, s := range v.s.subs {
		if keep(s) {
			cp := *s
			out = append(out, &cp)
		}
	}
	slices.SortFunc(out, func(a, b *biz.Subscription) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

func (v monitorSubscriptionView) List(_ context.Context, f biz.SubscriptionFilter, page, pageSize int64) ([]*biz.Subscription, int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.sorted(func(s *biz.Subscription) bool {
		switch {
		case f.OwnerID != "" && s.OwnerID != f.OwnerID,
			f.OwnerID == "" && !f.Groups.All && s.OwnerID != f.ViewerID && !intersects(s.Spec.GroupIDs, f.Groups.IDs),
			len(f.CoverGroupIDs) > 0 && !intersects(s.Spec.GroupIDs, f.CoverGroupIDs),
			f.Keyword != "" && !strings.Contains(s.Spec.Name, f.Keyword),
			f.Enabled != nil && s.Spec.Enabled != *f.Enabled:
			return false
		}
		return true
	})
	out, total := pageOf(items, page, pageSize)
	return out, total, nil
}

func (v monitorSubscriptionView) Get(_ context.Context, id string) (*biz.Subscription, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	s, ok := v.s.subs[id]
	if !ok {
		return nil, biz.ErrSubscriptionNotFound
	}
	cp := *s
	return &cp, nil
}

func (v monitorSubscriptionView) ListCandidates(_ context.Context, c biz.SubscriptionCandidate, limit int) ([]*biz.Subscription, int, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	items := v.sorted(func(sub *biz.Subscription) bool {
		s := &sub.Spec
		event := s.OnFiring
		if c.Resolved {
			event = s.OnResolved
		}
		return s.Enabled && event && (s.FollowRepeat || !c.RepeatOnly) && intersects(s.GroupIDs, c.GroupIDs) &&
			(len(s.RuleIDs) == 0 || slices.Contains(s.RuleIDs, c.RuleID)) &&
			(len(s.DatasourceIDs) == 0 || slices.Contains(s.DatasourceIDs, c.DatasourceID)) && slices.Contains(s.Severities, c.Severity)
	})
	return items[:min(len(items), limit)], len(items), nil
}

func (v monitorSubscriptionView) Create(_ context.Context, s *biz.Subscription) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	for _, g := range s.Spec.GroupIDs {
		if _, ok := v.s.groups[g]; !ok {
			return biz.ErrGroupNotFound
		}
	}
	cp := *s
	cp.Version, cp.CreateTime, cp.UpdateTime = 1, v.s.now(), v.s.now()
	v.s.subs[s.ID] = &cp
	return nil
}

func (v monitorSubscriptionView) Update(_ context.Context, s *biz.Subscription, expectVersion int32, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.subs[s.ID]
	if !ok {
		return biz.ErrSubscriptionNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	cur.Spec, cur.Version, cur.UpdateTime = s.Spec, cur.Version+1, v.s.now()
	return nil
}

func (v monitorSubscriptionView) Delete(_ context.Context, id string, expectVersion int32, _ string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur, ok := v.s.subs[id]
	if !ok {
		return biz.ErrSubscriptionNotFound
	}
	if cur.Version != expectVersion {
		return biz.ErrVersionConflict
	}
	delete(v.s.subs, id)
	return nil
}

// ---- 发件箱与派发写入 ----

type monitorOutboxView struct{ s *MonitorStore }

// Outbox 返回发件箱与派发写入仓储视图。
func (s *MonitorStore) Outbox() interface {
	biz.OutboxRepo
	biz.DispatchWriter
} {
	return monitorOutboxView{s}
}

// ClaimTx 取最早的待派发行；同一事件更早的待派发行存在时不领取后面的（失败退避不模拟）。
func (v monitorOutboxView) ClaimTx(_ context.Context, _ txrun.Tx, exclude []string) (*biz.OutboxItem, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	blocked := map[string]bool{}
	for _, r := range v.s.outbox {
		if r.Status != mc.OutboxPending {
			continue
		}
		if !blocked[r.AlertID] && !slices.Contains(exclude, r.ID) {
			item := r.OutboxItem
			return &item, nil
		}
		blocked[r.AlertID] = true
	}
	return nil, nil
}

func (v monitorOutboxView) row(id string) *MonitorOutboxRow {
	for _, r := range v.s.outbox {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (v monitorOutboxView) FinishTx(_ context.Context, _ txrun.Tx, id string, status int32, reason string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if r := v.row(id); r != nil && r.Status == mc.OutboxPending {
		r.Status, r.LastError = status, reason
	}
	return nil
}

func (v monitorOutboxView) Fail(_ context.Context, id, reason string) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	r := v.row(id)
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

func (v monitorOutboxView) InsertLogTx(_ context.Context, _ txrun.Tx, l *biz.NotifyLog) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if v.s.failCount > 0 {
		v.s.failCount--
		return v.s.failInsert
	}
	cp := *l
	cp.CreateTime = v.s.now()
	v.s.logs = append(v.s.logs, &cp)
	return nil
}

func (v monitorOutboxView) TouchAlertTx(_ context.Context, _ txrun.Tx, alertID string, seq int32) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	a, ok := v.s.alerts[alertID]
	if !ok {
		return nil
	}
	a.NotifyCount = max(a.NotifyCount, seq)
	now := v.s.now()
	a.LastNotifyTime = &now
	return nil
}

// ---- 跨域窄接口桩 ----

// MonitorOrg 监控域跨域依赖的内存桩：角色、组织目录、部门负责人、值班与值班表名称。
// 字段在装配后只读；测试在启动前填好。
type MonitorOrg struct {
	// Admins 超级管理员用户。
	Admins []string
	// UserRoles 用户 → 启用角色。
	UserRoles map[string][]string
	// Roles 全部角色。
	Roles []biz.RoleRef
	// Users 可用用户 → 显示名。
	Users map[string]string
	// Disabled 已停用的用户 → 显示名：仍能查到名字（历史记录要显示），但不是可用用户。
	Disabled map[string]string
	// AlertReaders 持有「查询告警事件」接口权限的用户；DatasourceEditors 持有「编辑监控数据源」接口
	// 权限的用户。超级管理员恒有。
	AlertReaders      []string
	DatasourceEditors []string
	// Depts 部门 → 名称；Leaders 部门 → 负责人。
	Depts   map[string]string
	Leaders map[string][]string
	// Schedules 启用的值班表 → 名称；OnDutyUsers 值班表 → 当前值班人。
	Schedules   map[string]string
	OnDutyUsers map[string][]string
}

// EnabledRoleIDs implements biz.RoleSource.
func (o *MonitorOrg) EnabledRoleIDs(_ context.Context, userID string) (bool, []string, error) {
	if slices.Contains(o.Admins, userID) {
		return true, nil, nil
	}
	return false, o.UserRoles[userID], nil
}

// ListRoles implements biz.RoleDirectory.
func (o *MonitorOrg) ListRoles(context.Context) ([]biz.RoleRef, error) { return o.Roles, nil }

// ActiveUserIDs implements biz.Directory.
func (o *MonitorOrg) ActiveUserIDs(_ context.Context, ids []string) ([]string, error) {
	var out []string
	for _, id := range ids {
		if _, ok := o.Users[id]; ok && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out, nil
}

func pick(src map[string]string, ids []string) map[string]string {
	out := map[string]string{}
	for _, id := range ids {
		if name, ok := src[id]; ok {
			out[id] = name
		}
	}
	return out
}

// UserNames implements biz.Directory：停用的用户照样返回名字（与 system 域的口径一致）。
func (o *MonitorOrg) UserNames(_ context.Context, ids []string) (map[string]string, error) {
	out := pick(o.Users, ids)
	maps.Copy(out, pick(o.Disabled, ids))
	return out, nil
}

// CanReadAlerts implements biz.Permissions.
func (o *MonitorOrg) CanReadAlerts(_ context.Context, userID string) (bool, error) {
	return slices.Contains(o.Admins, userID) || slices.Contains(o.AlertReaders, userID), nil
}

// CanEditDatasource implements biz.Permissions.
func (o *MonitorOrg) CanEditDatasource(_ context.Context, userID string) (bool, error) {
	return slices.Contains(o.Admins, userID) || slices.Contains(o.DatasourceEditors, userID), nil
}

// DeptNames implements biz.Directory.
func (o *MonitorOrg) DeptNames(_ context.Context, ids []string) (map[string]string, error) {
	return pick(o.Depts, ids), nil
}

// DeptLeaderIDs implements biz.DeptLeaders.
func (o *MonitorOrg) DeptLeaderIDs(_ context.Context, deptID string, _ int, _ string) ([]string, error) {
	return o.Leaders[deptID], nil
}

// OnDuty implements biz.Duty.
func (o *MonitorOrg) OnDuty(_ context.Context, scheduleID string, _ time.Time) ([]string, error) {
	return o.OnDutyUsers[scheduleID], nil
}

// ScheduleNames implements biz.Schedules.
func (o *MonitorOrg) ScheduleNames(_ context.Context, ids []string) (map[string]string, error) {
	return pick(o.Schedules, ids), nil
}

// MonitorCipher 凭据加解密桩：Enabled=false 模拟主密钥未配置；密文 = "sealed:" + 明文。
type MonitorCipher struct {
	Enabled bool
}

// Seal implements biz.SecretCipher.
func (c *MonitorCipher) Seal(plaintext string) ([]byte, error) {
	if !c.Enabled {
		return nil, biz.ErrSecretUnavailable
	}
	return []byte("sealed:" + plaintext), nil
}

// Open implements biz.SecretCipher.
func (c *MonitorCipher) Open(sealed []byte) (string, error) {
	if !c.Enabled {
		return "", biz.ErrSecretUnavailable
	}
	return strings.TrimPrefix(string(sealed), "sealed:"), nil
}

// MonitorQueryClient 数据源查询桩：记录收到的连接配置；Err 非空时查询失败。
type MonitorQueryClient struct {
	mu     sync.Mutex
	Err    error
	Result *biz.QueryResult
	conns  []biz.Connection
	// Block 非空时查询阻塞到它被关闭（并发额度测试用）。
	Block chan struct{}
}

// Conns 收到过的连接配置（含凭据明文，供断言「沿用已保存凭据」）。
func (c *MonitorQueryClient) Conns() []biz.Connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.conns)
}

// Validate implements biz.QueryClient：只做最基本的地址检查。
func (c *MonitorQueryClient) Validate(conn biz.Connection) error {
	if !strings.HasPrefix(conn.URL, "http://") && !strings.HasPrefix(conn.URL, "https://") {
		return biz.DatasourceInvalid("base url scheme must be http or https")
	}
	return nil
}

func (c *MonitorQueryClient) run(conn biz.Connection) (*biz.QueryResult, error) {
	c.mu.Lock()
	c.conns = append(c.conns, conn)
	block, err, res := c.Block, c.Err, c.Result
	c.mu.Unlock()
	if block != nil {
		<-block
	}
	if err != nil {
		return nil, err
	}
	if res == nil {
		res = &biz.QueryResult{Type: "vector"}
	}
	cp := *res
	return &cp, nil
}

// Instant implements biz.QueryClient.
func (c *MonitorQueryClient) Instant(_ context.Context, conn biz.Connection, _ string, _ time.Time, _ int) (*biz.QueryResult, error) {
	return c.run(conn)
}

// Range implements biz.QueryClient.
func (c *MonitorQueryClient) Range(_ context.Context, conn biz.Connection, _ string, _, _ time.Time, _ time.Duration, _ int) (*biz.QueryResult, error) {
	return c.run(conn)
}

// MonitorTargets 通知对象候选项的内存桩（biz.TargetSource）。
type MonitorTargets struct {
	ScheduleItems []biz.TargetSchedule
	DeptItems     []biz.TargetDept
	UserItems     []biz.TargetUser
}

// Schedules implements biz.TargetSource.
func (m *MonitorTargets) Schedules(context.Context) ([]biz.TargetSchedule, error) {
	return m.ScheduleItems, nil
}

// Depts implements biz.TargetSource.
func (m *MonitorTargets) Depts(context.Context) ([]biz.TargetDept, error) { return m.DeptItems, nil }

// Users implements biz.TargetSource：登录名 / 昵称包含关键字的用户，至多 limit 个。
func (m *MonitorTargets) Users(_ context.Context, keyword string, limit int64) ([]biz.TargetUser, error) {
	out := make([]biz.TargetUser, 0, len(m.UserItems))
	for _, u := range m.UserItems {
		if int64(len(out)) < limit && (strings.Contains(u.Username, keyword) || strings.Contains(u.Nickname, keyword)) {
			out = append(out, u)
		}
	}
	return out, nil
}
