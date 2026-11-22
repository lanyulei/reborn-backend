package biz

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	incidentv1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizincident "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/google/wire"
)

// 故障管理域的跨域适配器（组合根）：incident 域定义的窄接口（biz/incident/deps.go）在这里用 system / oncall /
// monitor / cmdb / ticket / workflow / job / assistant / file 各域**已有的公开用例方法**实现。依赖单向
// （incident → 各域）：各域不感知故障域；对它们的改动只有几处可选过滤字段（cmdb 历史按资源集合、job 执行按
// 资源集合、k8s 变更按时间窗、workflow 实例按业务对象与时间窗）与监控域新增的只读告警事件视图。

// IncidentProviderSet 上述适配器的构造函数，由 ProviderSet 聚合。
var IncidentProviderSet = wire.NewSet(
	NewIncidentDirectory, NewIncidentAdmins, NewIncidentAccounts, NewIncidentPermissions, NewIncidentDuty, NewIncidentOncallGroups, NewIncidentSchedules,
	NewIncidentAlerts, NewIncidentMatchers, NewIncidentCMDB, NewIncidentChangeSources, NewIncidentTickets, NewIncidentWorkflows,
	NewIncidentJobs, NewIncidentSummarizer, NewIncidentFiles, NewIncidentWorkdays, NewIncidentSiteURL,
)

// ---------------------------------------------------------------------------
// system
// ---------------------------------------------------------------------------

type incidentDirectory struct {
	dir *bizsystem.DirectoryUsecase
}

// NewIncidentDirectory 构造组织目录适配器。
func NewIncidentDirectory(dir *bizsystem.DirectoryUsecase) bizincident.Directory {
	return incidentDirectory{dir: dir}
}

func (d incidentDirectory) ActiveUserIDs(ctx context.Context, ids []string) ([]string, error) {
	return d.dir.ActiveUserIDs(ctx, ids)
}

func (d incidentDirectory) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	return d.dir.UserNames(ctx, ids)
}

// PrimaryDept implements bizincident.Directory：没有主部门时取第一个部门。
func (d incidentDirectory) PrimaryDept(ctx context.Context, userID string) (string, error) {
	primary, all, err := d.dir.PrimaryDept(ctx, userID)
	if err != nil {
		return "", err
	}
	if primary == "" && len(all) > 0 {
		return all[0], nil
	}
	return primary, nil
}

type incidentAdmins struct {
	authz *bizsystem.AuthzUsecase
}

// NewIncidentAdmins 构造超级管理员判定器。
func NewIncidentAdmins(authz *bizsystem.AuthzUsecase) bizincident.Admins {
	return incidentAdmins{authz: authz}
}

func (a incidentAdmins) IsAdmin(ctx context.Context, userID string) (bool, error) {
	isAdmin, _, err := a.authz.EnabledRoleIDs(ctx, userID)
	return isAdmin, err
}

// incidentAccounts 把 system 域的账号类型判定适配为故障域的 Accounts（Agent 建议能否申请自动通过）。
type incidentAccounts struct {
	users *bizsystem.UserUsecase
}

// NewIncidentAccounts 构造账号类型判定适配器。
func NewIncidentAccounts(users *bizsystem.UserUsecase) bizincident.Accounts {
	return incidentAccounts{users: users}
}

// IsMachine implements bizincident.Accounts.
func (a incidentAccounts) IsMachine(ctx context.Context, userID string) (bool, error) {
	return a.users.IsMachine(ctx, userID)
}

type incidentPermissions struct {
	authz *bizsystem.AuthzUsecase
}

// NewIncidentPermissions 构造接口权限判定器（操作码与授权中间件同一口径）。
func NewIncidentPermissions(authz *bizsystem.AuthzUsecase) bizincident.Permissions {
	return incidentPermissions{authz: authz}
}

func (p incidentPermissions) CanReadIncidents(ctx context.Context, userID string) (bool, error) {
	return p.authz.Allowed(ctx, userID, bizsystem.OperationCode(incidentv1.OperationIncidentServiceGetIncident))
}

// ---------------------------------------------------------------------------
// oncall
// ---------------------------------------------------------------------------

// incidentOncallActor 以系统身份读值班域的只读列表（分组成员、值班表）。
var incidentOncallActor = bizoncall.Actor{IsAdmin: true}

type incidentDuty struct {
	duty      *bizoncall.DutyUsecase
	schedules *bizoncall.ScheduleUsecase
}

// NewIncidentDuty 构造值班人适配器。
func NewIncidentDuty(duty *bizoncall.DutyUsecase, schedules *bizoncall.ScheduleUsecase) bizincident.Duty {
	return incidentDuty{duty: duty, schedules: schedules}
}

func (d incidentDuty) OnDuty(ctx context.Context, scheduleID string, at time.Time) ([]string, error) {
	return d.duty.OnDuty(ctx, scheduleID, at)
}

func (d incidentDuty) OnDutyByRole(ctx context.Context, scheduleID string) (primary, backup []string, err error) {
	duties, err := d.schedules.Current(ctx, "", scheduleID)
	if err != nil {
		return nil, nil, err
	}
	for _, du := range duties {
		if du.Primary != nil && du.Primary.UserID != "" {
			primary = append(primary, du.Primary.UserID)
		}
		if du.Backup != nil && du.Backup.UserID != "" {
			backup = append(backup, du.Backup.UserID)
		}
	}
	return primary, backup, nil
}

type incidentOncallGroups struct {
	groups *bizoncall.GroupUsecase
}

// NewIncidentOncallGroups 构造值班分组管理员适配器。
func NewIncidentOncallGroups(groups *bizoncall.GroupUsecase) bizincident.OncallGroups {
	return incidentOncallGroups{groups: groups}
}

func (g incidentOncallGroups) AdminIDs(ctx context.Context, groupID string) ([]string, error) {
	members, err := g.groups.ListMembers(ctx, incidentOncallActor, groupID)
	if kerrors.Is(err, bizoncall.ErrGroupNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, mb := range members {
		if mb.Role == bizoncall.MemberRoleAdmin && mb.Available {
			out = append(out, mb.UserID)
		}
	}
	return out, nil
}

type incidentSchedules struct {
	schedules *bizoncall.ScheduleUsecase
}

// NewIncidentSchedules 构造值班表候选与名称适配器。
func NewIncidentSchedules(schedules *bizoncall.ScheduleUsecase) bizincident.Schedules {
	return incidentSchedules{schedules: schedules}
}

func (s incidentSchedules) Schedules(ctx context.Context) ([]bizincident.Named, error) {
	all, err := s.schedules.List(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]bizincident.Named, 0, len(all))
	for _, sc := range all {
		if sc.Status == bizoncall.StatusEnabled {
			name := sc.Name
			if sc.GroupName != "" {
				name = sc.GroupName + " / " + sc.Name
			}
			out = append(out, bizincident.Named{ID: sc.ID, Name: name})
		}
		if len(out) >= 500 {
			break
		}
	}
	return out, nil
}

func (s incidentSchedules) ScheduleNames(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		sc, err := s.schedules.Get(ctx, id)
		if errors.Is(err, bizoncall.ErrScheduleNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if sc.Status == bizoncall.StatusEnabled {
			out[id] = sc.Name
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// monitor
// ---------------------------------------------------------------------------

type incidentAlerts struct {
	feed   bizmonitor.AlertFeedReader
	lookup bizmonitor.AlertLookup
	alerts bizmonitor.AlertReader
	events bizmonitor.AlertEventRepo
	scopes *bizmonitor.ScopeUsecase
}

// NewIncidentAlerts 构造告警事件适配器（监控域只读 + 一条「关联故障」说明的回写）。
func NewIncidentAlerts(feed bizmonitor.AlertFeedReader, lookup bizmonitor.AlertLookup, alerts bizmonitor.AlertReader,
	events bizmonitor.AlertEventRepo, scopes *bizmonitor.ScopeUsecase,
) bizincident.Alerts {
	return incidentAlerts{feed: feed, lookup: lookup, alerts: alerts, events: events, scopes: scopes}
}

func alertRefOf(a *bizmonitor.Alert) *bizincident.AlertRef {
	ref := &bizincident.AlertRef{
		ID: a.ID, RuleID: a.RuleID, RuleName: a.RuleName, GroupID: a.GroupID, Severity: a.Severity, Status: a.Status, Labels: a.Labels,
		CIID: a.CIID, FirstFireTime: a.FirstFireTime, ResolveTime: a.ResolveTime, Muted: a.Muted, Inhibited: a.Inhibited,
	}
	if a.Value != nil {
		ref.Value = mc.FormatValue(*a.Value)
	}
	return ref
}

func (m incidentAlerts) Feed(ctx context.Context, afterSeq int64, limit int) ([]*bizincident.AlertSignal, error) {
	items, err := m.feed.ListAfter(ctx, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	return signalsOf(items), nil
}

func (m incidentAlerts) FeedRange(ctx context.Context, afterSeq, untilSeq int64, limit int) ([]*bizincident.AlertSignal, error) {
	items, err := m.feed.ListBetween(ctx, afterSeq, untilSeq, limit)
	if err != nil {
		return nil, err
	}
	return signalsOf(items), nil
}

func signalsOf(items []*bizmonitor.AlertFeedItem) []*bizincident.AlertSignal {
	out := make([]*bizincident.AlertSignal, 0, len(items))
	for _, it := range items {
		out = append(out, &bizincident.AlertSignal{Seq: it.Seq, Kind: it.Type, Time: it.Time, Alert: *alertRefOf(it.Alert)})
	}
	return out
}

func (m incidentAlerts) CountFiring(ctx context.Context, ids []string) (int, error) {
	return m.lookup.CountFiring(ctx, ids)
}

func (m incidentAlerts) Get(ctx context.Context, id string) (*bizincident.AlertRef, error) {
	a, err := m.alerts.Get(ctx, id)
	if kerrors.Is(err, bizmonitor.ErrAlertNotFound) {
		return nil, bizincident.ErrAlertNotFound
	}
	if err != nil {
		return nil, err
	}
	return alertRefOf(a), nil
}

func (m incidentAlerts) ListByIDs(ctx context.Context, ids []string) ([]*bizincident.AlertRef, error) {
	items, err := m.lookup.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]*bizincident.AlertRef, 0, len(items))
	for _, a := range items {
		out = append(out, alertRefOf(a))
	}
	return out, nil
}

func (m incidentAlerts) RecentFiring(ctx context.Context, since time.Time, limit int) ([]*bizincident.AlertRef, error) {
	items, err := m.alerts.Scan(ctx, bizmonitor.AlertFilter{Groups: bizmonitor.GroupFilter{All: true}, FireStart: &since}, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*bizincident.AlertRef, 0, len(items))
	for _, a := range items {
		out = append(out, alertRefOf(a))
	}
	return out, nil
}

// CanRead implements bizincident.Alerts：按监控域的分组数据范围。
func (m incidentAlerts) CanRead(ctx context.Context, userID string, alert *bizincident.AlertRef) (bool, error) {
	scope, err := m.scopes.Resolve(ctx, userID)
	if err != nil {
		return false, err
	}
	return scope.IsAdmin() || scope.CanRead(alert.GroupID), nil
}

func (m incidentAlerts) Note(ctx context.Context, alertID, operator, text string) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	return m.events.Append(ctx, &bizmonitor.AlertEvent{ID: id, AlertID: alertID, Type: mc.AlertEventIncident, Operator: operator, Detail: mc.AlertEventDetail{Text: text}})
}

type incidentMatchers struct{}

// NewIncidentMatchers 构造标签匹配编译器（kit/monitorcontract，与监控域同一实现）。
func NewIncidentMatchers() bizincident.MatcherCompiler { return incidentMatchers{} }

func (incidentMatchers) Compile(matchers []bizincident.Matcher) (bizincident.MatcherSet, error) {
	if len(matchers) == 0 {
		return (*mc.Matchers)(nil), nil
	}
	in := make([]mc.Matcher, 0, len(matchers))
	for _, m := range matchers {
		in = append(in, mc.Matcher{Label: m.Label, Op: m.Op, Value: m.Value, Values: m.Values})
	}
	return mc.CompileMatchers(in)
}

// ---------------------------------------------------------------------------
// cmdb
// ---------------------------------------------------------------------------

type incidentCMDB struct {
	cis       *bizcmdb.CIUsecase
	relations *bizcmdb.RelationUsecase
	scopes    *bizcmdb.ScopeUsecase
	topology  []bizmonitor.TopologyRelation
}

// NewIncidentCMDB 构造资源目录适配器（拓扑方向沿用 monitor.topology_relations 配置）。
func NewIncidentCMDB(cis *bizcmdb.CIUsecase, relations *bizcmdb.RelationUsecase, scopes *bizcmdb.ScopeUsecase, c *conf.Monitor) (bizincident.CMDB, error) {
	topology, err := bizmonitor.TopologyRelations(c)
	if err != nil {
		return nil, err
	}
	return incidentCMDB{cis: cis, relations: relations, scopes: scopes, topology: topology}, nil
}

func cardOf(ci *bizcmdb.CI) *bizincident.CICard {
	card := &bizincident.CICard{ID: ci.ID, Name: ci.Name, ModelCode: ci.ModelCode, ModelName: ci.ModelName, Env: ci.Env, Status: ci.Status, OwnerName: ci.OwnerName, Attrs: ci.Attrs}
	if ci.OwnerName != "" {
		card.OwnerID = ci.Owner
	}
	return card
}

// Cards implements bizincident.CMDB：逐个读取（资源列表没有按 ID 集合的过滤），至多 200 个。
func (m incidentCMDB) Cards(ctx context.Context, ids []string) ([]*bizincident.CICard, error) {
	out := make([]*bizincident.CICard, 0, len(ids))
	for _, id := range ids[:min(len(ids), 200)] {
		ci, err := m.cis.GetByID(ctx, id)
		if kerrors.Is(err, bizcmdb.ErrCINotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, cardOf(ci))
	}
	return out, nil
}

// edgesDown 把拓扑边按配置的方向整理成「上游 → 下游」。
func (m incidentCMDB) edgesDown(edges []*bizcmdb.TopologyEdge) (down, up map[string][]string) {
	down, up = map[string][]string{}, map[string][]string{}
	for _, e := range edges {
		i := slices.IndexFunc(m.topology, func(r bizmonitor.TopologyRelation) bool { return r.TypeCode == e.TypeCode })
		if i < 0 {
			continue
		}
		upper, lower := e.SrcID, e.DstID
		if !m.topology[i].UpstreamSrc {
			upper, lower = e.DstID, e.SrcID
		}
		down[upper] = append(down[upper], lower)
		up[lower] = append(up[lower], upper)
	}
	return down, up
}

// Downstream implements bizincident.CMDB.
func (m incidentCMDB) Downstream(ctx context.Context, ciID string, depth, limit int) ([]*bizincident.CINode, bool, error) {
	nodes, edges, truncated, err := m.relations.Neighbors(ctx, ciID, "", bizcmdb.TopologyMaxDepth, bizcmdb.TopologyMaxNodes)
	if kerrors.Is(err, bizcmdb.ErrCINotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	byID := make(map[string]*bizcmdb.TopologyNode, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	down, _ := m.edgesDown(edges)
	seen := map[string]struct{}{ciID: {}}
	frontier := []string{ciID}
	out := make([]*bizincident.CINode, 0, 16)
	for level := 1; level <= depth && len(frontier) > 0; level++ {
		var next []string
		for _, cur := range frontier {
			for _, id := range down[cur] {
				n := byID[id]
				if _, dup := seen[id]; dup || n == nil {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, &bizincident.CINode{ID: n.ID, Name: n.Name, ModelCode: n.ModelCode, ModelName: n.ModelName, Depth: int32(level), Via: ciID}) // #nosec G115 -- 层数至多 3
				next = append(next, id)
				if len(out) >= limit {
					return out, true, nil
				}
			}
		}
		frontier = next
	}
	return out, truncated, nil
}

// Upstream implements bizincident.CMDB.
func (m incidentCMDB) Upstream(ctx context.Context, ciID string) ([]string, error) {
	_, edges, _, err := m.relations.Neighbors(ctx, ciID, "", bizcmdb.TopologyMaxDepth, bizcmdb.TopologyMaxNodes)
	if kerrors.Is(err, bizcmdb.ErrCINotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_, up := m.edgesDown(edges)
	seen := map[string]struct{}{ciID: {}}
	frontier := []string{ciID}
	var out []string
	for level := 0; level < bizcmdb.TopologyMaxDepth && len(frontier) > 0; level++ {
		var next []string
		for _, cur := range frontier {
			for _, id := range up[cur] {
				if _, dup := seen[id]; dup {
					continue
				}
				seen[id] = struct{}{}
				out = append(out, id)
				next = append(next, id)
			}
		}
		frontier = next
	}
	return out, nil
}

// FindByNames implements bizincident.CMDB：按名称精确匹配（关键字搜索后再筛）。
func (m incidentCMDB) FindByNames(ctx context.Context, names []string) ([]*bizincident.CICard, error) {
	var out []*bizincident.CICard
	for _, name := range names[:min(len(names), 50)] {
		items, _, err := m.cis.List(ctx, bizcmdb.CIFilter{Keyword: name}, 1, 20)
		if err != nil {
			return nil, err
		}
		for _, ci := range items {
			if strings.EqualFold(ci.Name, name) {
				out = append(out, cardOf(ci))
				break
			}
		}
	}
	return out, nil
}

// Viewer implements bizincident.CMDB：须持有「查询资源详情」接口权限，再按数据范围判断。
func (m incidentCMDB) Viewer(ctx context.Context, userID string) (func(modelCode, env string) bool, error) {
	ok, err := m.scopes.Allowed(ctx, userID, bizsystem.OperationCode("/cmdb.v1.CmdbCIService/GetCI"))
	if err != nil {
		return nil, err
	}
	if !ok {
		return func(string, string) bool { return false }, nil
	}
	scope, err := m.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	return scope.Allows, nil
}

// History implements bizincident.CMDB.
func (m incidentCMDB) History(ctx context.Context, ciIDs []string, from, to time.Time, limit int) ([]*bizincident.ChangeItem, error) {
	items, _, err := m.cis.ListHistory(ctx, bizcmdb.HistoryFilter{CIIDs: ciIDs, From: &from, To: &to}, 1, int64(min(limit, 100)))
	if err != nil {
		return nil, err
	}
	out := make([]*bizincident.ChangeItem, 0, len(items))
	for _, h := range items {
		out = append(out, &bizincident.ChangeItem{
			Domain: "cmdb", Kind: h.Action, RefID: h.ID, Summary: h.CIName + " " + h.Action, Time: h.CreateTime, OperatorID: h.Operator, CIID: h.CIID,
			URL: "/cmdb/ci/" + h.CIID,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 变更来源：job / workflow / k8s
// ---------------------------------------------------------------------------

type incidentJobChanges struct {
	execs *bizjob.ExecutionUsecase
}

func (s incidentJobChanges) Domain() string { return "job" }

func (s incidentJobChanges) Changes(ctx context.Context, q bizincident.ChangeQuery) ([]*bizincident.ChangeItem, string, error) {
	if len(q.CIIDs) == 0 {
		return nil, "", nil
	}
	items, _, err := s.execs.List(ctx, bizjob.ExecutionFilter{CIIDs: q.CIIDs[:min(len(q.CIIDs), 100)], StartTime: &q.From, EndTime: &q.To}, 1, int64(min(q.Limit, 100)))
	if err != nil {
		return nil, "", err
	}
	out := make([]*bizincident.ChangeItem, 0, len(items))
	for _, e := range items {
		name := e.TemplateName
		if name == "" {
			name = "快速执行"
		}
		out = append(out, &bizincident.ChangeItem{Domain: "job", Kind: "execution", RefID: e.ID, Summary: "作业执行：" + name, Time: e.CreateTime, OperatorID: e.Creator, URL: "/job/execution/" + e.ID})
	}
	return out, "", nil
}

type incidentWorkflowChanges struct {
	runtime *bizworkflow.RuntimeUsecase
}

func (s incidentWorkflowChanges) Domain() string { return "workflow" }

func (s incidentWorkflowChanges) Changes(ctx context.Context, q bizincident.ChangeQuery) ([]*bizincident.ChangeItem, string, error) {
	ids := append(slices.Clone(q.CIIDs[:min(len(q.CIIDs), 100)]), q.IncidentID)
	items, _, _, err := s.runtime.ListInstances(ctx, bizworkflow.InstanceFilter{BusinessIDs: ids, StartAfter: &q.From, StartBefore: &q.To}, 1, int64(min(q.Limit, 100)))
	if err != nil {
		return nil, "", err
	}
	out := make([]*bizincident.ChangeItem, 0, len(items))
	for _, it := range items {
		if it.BusinessType == bizincident.BusinessType {
			continue // 本故障自己发起的流程不算变更
		}
		ci := ""
		if slices.Contains(q.CIIDs, it.BusinessID) {
			ci = it.BusinessID
		}
		out = append(out, &bizincident.ChangeItem{Domain: "workflow", Kind: "instance", RefID: it.ID, Summary: "流程：" + it.Title, Time: it.StartTime, OperatorID: it.Initiator, CIID: ci, URL: "/workflow/instance/" + it.ID})
	}
	return out, "", nil
}

type incidentK8sChanges struct {
	changes bizk8s.ChangeReader
}

func (s incidentK8sChanges) Domain() string { return "k8s" }

// Changes implements bizincident.ChangeSource：k8s 变更与 CMDB 资源无映射，只按时间窗粗筛并说明。
func (s incidentK8sChanges) Changes(ctx context.Context, q bizincident.ChangeQuery) ([]*bizincident.ChangeItem, string, error) {
	items, _, err := s.changes.List(ctx, bizk8s.ChangeFilter{Start: &q.From, End: &q.To}, 1, int64(min(q.Limit, 50)))
	if err != nil {
		return nil, "", err
	}
	out := make([]*bizincident.ChangeItem, 0, len(items))
	for _, c := range items {
		target := strings.TrimSpace(c.ResKind + " " + c.Name)
		if c.Namespace != "" {
			target = c.ResKind + " " + c.Namespace + "/" + c.Name
		}
		out = append(out, &bizincident.ChangeItem{Domain: "k8s", Kind: "change", RefID: c.ID, Summary: c.ClusterName + " " + target + "：" + c.Summary, Time: c.CreateTime, OperatorID: c.RequesterID, URL: "/k8s/change?id=" + c.ID})
	}
	note := ""
	if len(out) > 0 {
		note = "容器平台变更未与资源关联，只按时间窗筛选"
	}
	return out, note, nil
}

// NewIncidentChangeSources 装配各域的变更来源。
func NewIncidentChangeSources(execs *bizjob.ExecutionUsecase, runtime *bizworkflow.RuntimeUsecase, changes bizk8s.ChangeReader) bizincident.ChangeSources {
	return bizincident.ChangeSources{incidentJobChanges{execs: execs}, incidentWorkflowChanges{runtime: runtime}, incidentK8sChanges{changes: changes}}
}

// ---------------------------------------------------------------------------
// ticket
// ---------------------------------------------------------------------------

type incidentTickets struct {
	catalog *bizticket.CatalogUsecase
	tickets *bizticket.Usecase
}

// NewIncidentTickets 构造工单适配器。
func NewIncidentTickets(catalog *bizticket.CatalogUsecase, tickets *bizticket.Usecase) bizincident.Tickets {
	return incidentTickets{catalog: catalog, tickets: tickets}
}

func (t incidentTickets) Templates(ctx context.Context) ([]bizincident.Named, error) {
	categories, err := t.catalog.List(ctx, true)
	if err != nil {
		return nil, err
	}
	var out []bizincident.Named
	for _, c := range categories {
		for _, tpl := range c.Templates {
			out = append(out, bizincident.Named{ID: tpl.ID, Name: c.Name + " / " + tpl.Name})
		}
	}
	return out, nil
}

func (t incidentTickets) Template(ctx context.Context, templateID, userID string) (*bizincident.Named, error) {
	tpl, _, err := t.catalog.GetTemplate(ctx, templateID, userID)
	if errors.Is(err, bizticket.ErrTemplateNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if tpl.Status != bizticket.TemplateEnabled {
		return nil, nil
	}
	return &bizincident.Named{ID: tpl.ID, Name: tpl.Name}, nil
}

// descriptionFields 用来承载故障说明的表单字段编码（文本类型）。
var descriptionFields = []string{"description", "desc", "content", "detail", "details", "remark", "summary", "reason"}

func ticketInfoOf(tk *bizticket.Ticket) *bizincident.TicketInfo {
	return &bizincident.TicketInfo{ID: tk.ID, Number: tk.Number, Title: tk.Title, Status: tk.Status, Creator: tk.Creator, Finished: tk.Status != bizticket.StatusProcessing}
}

// Create implements bizincident.Tickets：说明填进模板里第一个文本类说明字段（没有就不填）。
func (t incidentTickets) Create(ctx context.Context, p bizincident.TicketCreate) (*bizincident.TicketInfo, error) {
	tpl, _, err := t.catalog.GetTemplate(ctx, p.TemplateID, p.Creator)
	if err != nil {
		return nil, err
	}
	data := map[string]any{}
	if p.Description != "" {
		for _, f := range tpl.Form {
			if (f.Type == formschema.TypeString || f.Type == formschema.TypeText) && slices.Contains(descriptionFields, strings.ToLower(f.Code)) {
				limit := 255
				if f.Type == formschema.TypeText {
					limit = 4000
				}
				if f.Rule.MaxLen != nil && *f.Rule.MaxLen > 1 && int(*f.Rule.MaxLen) < limit {
					limit = int(*f.Rule.MaxLen)
				}
				v := p.Description
				if f.Type == formschema.TypeString {
					v = strings.Join(strings.Fields(v), " ")
				}
				if r := []rune(v); len(r) > limit {
					v = string(r[:limit-1]) + "…"
				}
				data[f.Code] = v
				break
			}
		}
	}
	tk, err := t.tickets.Create(ctx, bizticket.CreateParams{
		TemplateID: p.TemplateID, Title: p.Title, Priority: p.Priority, Data: data, Creator: p.Creator, CreatorDept: p.CreatorDept,
		RequestID: p.RequestID, ResourceIDs: p.ResourceIDs,
	})
	if err != nil {
		return nil, err
	}
	return ticketInfoOf(tk), nil
}

func (t incidentTickets) View(ctx context.Context, ticketID, userID string) (*bizincident.TicketInfo, error) {
	d, err := t.tickets.Get(ctx, ticketID, userID)
	if errors.Is(err, bizticket.ErrTicketNotFound) || errors.Is(err, bizticket.ErrTicketForbidden) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ticketInfoOf(d.Ticket), nil
}

func (t incidentTickets) Comment(ctx context.Context, ticketID, userID, content string) error {
	_, _, err := t.tickets.AddComment(ctx, ticketID, userID, content, "")
	return err
}

// ---------------------------------------------------------------------------
// workflow
// ---------------------------------------------------------------------------

type incidentWorkflows struct {
	defs    *bizworkflow.DefinitionUsecase
	runtime *bizworkflow.RuntimeUsecase
}

// NewIncidentWorkflows 构造流程适配器。
func NewIncidentWorkflows(defs *bizworkflow.DefinitionUsecase, runtime *bizworkflow.RuntimeUsecase) bizincident.Workflows {
	return incidentWorkflows{defs: defs, runtime: runtime}
}

func (w incidentWorkflows) Definitions(ctx context.Context) ([]bizincident.Named, error) {
	enabled, published := bizworkflow.DefinitionEnabled, true
	defs, err := monitorPages(func(page, pageSize int64) ([]*bizworkflow.Definition, int64, error) {
		return w.defs.List(ctx, bizworkflow.DefinitionFilter{Status: &enabled, Published: &published}, page, pageSize)
	})
	if err != nil {
		return nil, err
	}
	out := make([]bizincident.Named, 0, len(defs))
	for _, d := range defs {
		out = append(out, bizincident.Named{ID: d.Key, Name: d.Name})
	}
	return out, nil
}

func (w incidentWorkflows) Startable(ctx context.Context, key string) (string, bool, error) {
	if key == "" {
		return "", false, nil
	}
	_, ok, err := w.defs.KeyStartable(ctx, key)
	if err != nil || !ok {
		return "", false, err
	}
	enabled := bizworkflow.DefinitionEnabled
	defs, _, err := w.defs.List(ctx, bizworkflow.DefinitionFilter{Keyword: key, Status: &enabled}, 1, 50)
	if err != nil {
		return "", false, err
	}
	for _, d := range defs {
		if d.Key == key {
			return d.Name, true, nil
		}
	}
	return key, true, nil
}

func (w incidentWorkflows) Start(ctx context.Context, p bizincident.WorkflowStart) (string, error) {
	in, err := w.runtime.Start(ctx, bizworkflow.StartParams{
		DefinitionKey: p.DefinitionKey, BusinessType: bizincident.BusinessType, BusinessID: p.BusinessID, Title: p.Title,
		Initiator: p.Initiator, Variables: p.Variables, RequestID: p.RequestID,
	})
	if err != nil {
		return "", err
	}
	return in.ID, nil
}

func (w incidentWorkflows) InstanceStatus(ctx context.Context, instanceID string) (int32, error) {
	d, err := w.runtime.Get(ctx, instanceID, "", true)
	if err != nil {
		return 0, err
	}
	return d.State.Instance.Status, nil
}

// ---------------------------------------------------------------------------
// job
// ---------------------------------------------------------------------------

type incidentJobs struct {
	templates *bizjob.TemplateUsecase
	execs     *bizjob.ExecutionUsecase
}

// NewIncidentJobs 构造作业适配器。
func NewIncidentJobs(templates *bizjob.TemplateUsecase, execs *bizjob.ExecutionUsecase) bizincident.Jobs {
	return incidentJobs{templates: templates, execs: execs}
}

func jobTemplateOf(t *bizjob.Template) *bizincident.JobTemplate {
	out := &bizincident.JobTemplate{ID: t.ID, Name: t.Name, Description: t.Description, Enabled: t.Enabled}
	for _, p := range t.Params {
		out.ParamNames = append(out.ParamNames, p.Code)
	}
	return out
}

func (j incidentJobs) Templates(ctx context.Context, userID string) ([]*bizincident.JobTemplate, error) {
	enabled := true
	items, err := monitorPages(func(page, pageSize int64) ([]*bizjob.Template, int64, error) {
		return j.templates.List(ctx, userID, bizjob.TemplateFilter{Enabled: &enabled}, page, pageSize)
	})
	if err != nil {
		return nil, err
	}
	out := make([]*bizincident.JobTemplate, 0, len(items))
	for _, t := range items {
		out = append(out, jobTemplateOf(t))
	}
	return out, nil
}

func (j incidentJobs) Template(ctx context.Context, userID, id string) (*bizincident.JobTemplate, error) {
	t, err := j.templates.Get(ctx, userID, id)
	if err != nil {
		if e := kerrors.FromError(err); e != nil && (e.Code == 404 || e.Code == 403) {
			return nil, nil
		}
		return nil, err
	}
	return jobTemplateOf(t), nil
}

func jobExecutionOf(e *bizjob.Execution) *bizincident.JobExecution {
	out := &bizincident.JobExecution{ID: e.ID, Status: e.Status}
	switch e.Status {
	case jc.StatusSuccess:
		out.Finished, out.Succeeded, out.Summary = true, true, fmt.Sprintf("执行成功：%d 台主机", e.HostOK)
	case jc.StatusFailed, jc.StatusPartial, jc.StatusTimeout, jc.StatusUnknown, jc.StatusRejected, jc.StatusCanceled:
		out.Finished, out.Summary = true, fmt.Sprintf("执行未成功（状态 %d）：成功 %d / 失败 %d / 不可达 %d", e.Status, e.HostOK, e.HostFailed, e.HostUnreachable)
		if e.Error != "" {
			out.Summary += "：" + e.Error
		}
	}
	return out
}

// Submit implements bizincident.Jobs：以告警自愈的触发类型提交——作业域对非手动提交按执行人此刻的权限判定。
func (j incidentJobs) Submit(ctx context.Context, userID string, in bizincident.JobSubmit) (*bizincident.JobExecution, error) {
	e, err := j.execs.Submit(ctx, userID, bizjob.SubmitInput{
		TemplateID: in.TemplateID, Params: in.Params, CIIDs: in.CIIDs, RequestID: in.RequestID, Trigger: jc.TriggerAlert, TriggerRef: in.RequestID,
	})
	if err != nil {
		return nil, err
	}
	return jobExecutionOf(e), nil
}

func (j incidentJobs) Execution(ctx context.Context, id string) (*bizincident.JobExecution, error) {
	e, err := j.execs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return jobExecutionOf(e), nil
}

// ---------------------------------------------------------------------------
// assistant / file / workflow calendar / 配置
// ---------------------------------------------------------------------------

// NewIncidentSummarizer 构造故障域的文本摘要适配器（与监控域同一模型客户端、同一并发上限）。
// 不复用监控域的 Summarizer 实例：它依赖助手的 AgentUsecase，而 AgentUsecase 又经页内上下文提供者依赖故障用例，
// 直接复用会成环；这里只取模型客户端与启用开关，摘要逻辑与并发名额和监控域同一实现。
func NewIncidentSummarizer(cfg bizassistant.AgentConfig, model bizassistant.ChatModel) bizincident.Summarizer {
	return incidentSummarizer{monitorSummarizer: monitorSummarizer{model: model, sem: make(chan struct{}, monitorSummaryConcurrency)}, enabled: cfg.Enabled}
}

type incidentSummarizer struct {
	monitorSummarizer
	enabled bool
}

// Available implements bizincident.Summarizer.
func (s incidentSummarizer) Available() bool { return s.enabled }

type incidentFiles struct {
	files *bizfile.Usecase
}

// NewIncidentFiles 构造附件绑定适配器。
func NewIncidentFiles(files *bizfile.Usecase) bizincident.Files { return incidentFiles{files: files} }

func (f incidentFiles) BindTx(ctx context.Context, tx txrun.Tx, uploader, incidentID string, fileIDs []string) error {
	return f.files.Bind(ctx, tx, uploader, bizincident.BusinessType, incidentID, fileIDs)
}

type incidentWorkdays struct {
	calendar *bizworkflow.CalendarUsecase
}

// NewIncidentWorkdays 构造工作日历适配器（复盘期限按工作日）。
func NewIncidentWorkdays(calendar *bizworkflow.CalendarUsecase) bizincident.Workdays {
	return incidentWorkdays{calendar: calendar}
}

func (w incidentWorkdays) AddWorkdays(ctx context.Context, from time.Time, days int) (time.Time, error) {
	return w.calendar.Due(ctx, from, bizworkflow.Duration{Value: days, Unit: bizworkflow.UnitDay, WorkTime: true})
}

// NewIncidentSiteURL 前端站点地址（沿用 monitor.site_url）。
func NewIncidentSiteURL(c *conf.Monitor) (bizincident.SiteURL, error) {
	raw := strings.TrimSpace(c.GetSiteUrl())
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errors.New("monitor.site_url must be an http(s) address")
	}
	return bizincident.SiteURL(strings.TrimRight(raw, "/")), nil
}
