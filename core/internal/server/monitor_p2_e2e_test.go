package server

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"time"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	monitorservice "github.com/lanyulei/reborn/backend/core/internal/service/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
)

// 监控域第二、三期的 e2e 装配：用例全部是真实实现，仓储用 testfake 的内存实现，跨域窄接口（工单 / 流程 /
// CMDB / 值班 / 助手 / 通知路由）用本文件里的小桩。接口权限判定（Permissions / OncallAccess）用组合根里
// 真实的适配器，与授权中间件同一口径。

// monitorP2 第二、三期的内存仓储与跨域桩：测试经它预置运行数据、断言写入。
type monitorP2 struct {
	ops         *testfake.MonitorAlertOps
	escalations *testfake.MonitorEscalations
	sync        *testfake.MonitorSyncStore
	inhibits    *testfake.MonitorInhibitStore
	engine      *testfake.MonitorEngine
	views       *testfake.MonitorViews
	envs        *testfake.MonitorCIEnvs
	cross       *monitorCross
}

// monitorP2Handles 内存仓储（*testfake.MonitorStore）→ 其第二、三期的装配产物。monitorServices 结构体
// 定义在第一期的测试文件里，这里用旁路表把句柄带给测试。
var monitorP2Handles sync.Map

func monitorP2Of(ms *testfake.MonitorStore) *monitorP2 {
	h, _ := monitorP2Handles.Load(ms)
	return h.(*monitorP2)
}

// e2eTicket 桩里的一张工单。
type e2eTicket struct {
	info      bizmonitor.TicketInfo
	requestID string
	resources []string
	comments  []string
}

// monitorCross 跨域窄接口的内存桩（一个类型实现全部：各接口的方法名互不相同）。字段由 mu 保护。
type monitorCross struct {
	mu sync.Mutex
	// tickets 已创建的工单；ticketViewers 工单 ID → 另外可查看它的用户（提单人恒可查看）。
	tickets       []*e2eTicket
	ticketViewers map[string][]string
	// cards 资源 ID → 卡片；ciViewers 资源 ID → 能在 CMDB 里查看它的用户；impact 资源 ID → 下游资源。
	cards     map[string]*bizmonitor.CICard
	ciViewers map[string][]string
	impact    map[string][]bizmonitor.CIImpact
	// oncallGroups 值班分组；groupAdmins 分组 → 管理员；shifts 值班表 → 班次。
	oncallGroups []bizmonitor.OncallGroup
	groupAdmins  map[string][]string
	shifts       map[string][]bizmonitor.ShiftSlot
	// routeMatched 通知路由试算的结论；aiAvailable 助手是否可用。
	routeMatched bool
	aiAvailable  bool
}

const (
	e2eTemplateID  = "018f3e1a-0e10-7000-8000-000000000001"
	e2eWorkflowKey = "incident"
	e2eOncallGroup = "018f3e1a-0e11-7000-8000-000000000001"
	e2eSchedule    = "018f3e1a-0e00-7000-8000-000000000001"
)

func newMonitorCross() *monitorCross {
	return &monitorCross{
		ticketViewers: map[string][]string{}, cards: map[string]*bizmonitor.CICard{}, ciViewers: map[string][]string{},
		impact: map[string][]bizmonitor.CIImpact{}, groupAdmins: map[string][]string{}, shifts: map[string][]bizmonitor.ShiftSlot{},
		oncallGroups: []bizmonitor.OncallGroup{{ID: e2eOncallGroup, Name: "运维组"}}, routeMatched: true,
	}
}

// Templates implements bizmonitor.TicketCatalog.
func (c *monitorCross) Templates(context.Context) ([]bizmonitor.TicketTemplate, error) {
	return []bizmonitor.TicketTemplate{{ID: e2eTemplateID, Name: "故障处理", CategoryName: "运维"}}, nil
}

// Template implements bizmonitor.TicketCatalog.
func (c *monitorCross) Template(_ context.Context, templateID, _ string) (*bizmonitor.TicketTemplate, error) {
	if templateID != e2eTemplateID {
		return nil, nil
	}
	return &bizmonitor.TicketTemplate{ID: e2eTemplateID, Name: "故障处理", CategoryName: "运维"}, nil
}

// Create implements bizmonitor.TicketDesk（按提单人 + 幂等键去重）。
func (c *monitorCross) Create(_ context.Context, p bizmonitor.TicketCreate) (*bizmonitor.TicketInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tickets {
		if t.requestID == p.RequestID && t.info.Creator == p.Creator {
			info := t.info
			return &info, nil
		}
	}
	n := strconv.Itoa(len(c.tickets) + 1)
	t := &e2eTicket{
		info: bizmonitor.TicketInfo{
			ID: "018f3e1a-0e12-7000-8000-00000000000" + n, Number: "INC-20261118-000" + n,
			Title: p.Title, Status: 1, Creator: p.Creator,
		},
		requestID: p.RequestID, resources: slices.Clone(p.ResourceIDs),
	}
	c.tickets = append(c.tickets, t)
	info := t.info
	return &info, nil
}

// View implements bizmonitor.TicketDesk.
func (c *monitorCross) View(_ context.Context, ticketID, userID string) (*bizmonitor.TicketInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tickets {
		if t.info.ID == ticketID && (t.info.Creator == userID || slices.Contains(c.ticketViewers[ticketID], userID)) {
			info := t.info
			return &info, nil
		}
	}
	return nil, nil
}

// Comment implements bizmonitor.TicketDesk.
func (c *monitorCross) Comment(_ context.Context, ticketID, _, content string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tickets {
		if t.info.ID == ticketID {
			t.comments = append(t.comments, content)
		}
	}
	return nil
}

func (c *monitorCross) ticketList() []e2eTicket {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]e2eTicket, 0, len(c.tickets))
	for _, t := range c.tickets {
		out = append(out, *t)
	}
	return out
}

// Definitions implements bizmonitor.Workflows.
func (c *monitorCross) Definitions(context.Context) ([]bizmonitor.WorkflowDef, error) {
	return []bizmonitor.WorkflowDef{{Key: e2eWorkflowKey, Name: "故障应急"}}, nil
}

// Definition implements bizmonitor.Workflows.
func (c *monitorCross) Definition(_ context.Context, key string) (*bizmonitor.WorkflowDef, error) {
	if key != e2eWorkflowKey {
		return nil, nil
	}
	return &bizmonitor.WorkflowDef{Key: e2eWorkflowKey, Name: "故障应急"}, nil
}

// Start implements bizmonitor.Workflows.
func (c *monitorCross) Start(context.Context, bizmonitor.WorkflowStart) (string, error) {
	return "018f3e1a-0e13-7000-8000-000000000001", nil
}

// Card implements bizmonitor.CMDBViewer：只有登记为可查看的用户拿得到卡片。
func (c *monitorCross) Card(_ context.Context, userID, ciID string) (*bizmonitor.CICard, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	card, ok := c.cards[ciID]
	if !ok {
		return nil, false, nil
	}
	if !slices.Contains(c.ciViewers[ciID], userID) {
		return nil, true, nil
	}
	cp := *card
	return &cp, true, nil
}

// Impact implements bizmonitor.CMDBViewer：调用方看不到的节点只留 ID、名称与层级。
func (c *monitorCross) Impact(_ context.Context, userID, ciID string, _, limit int) ([]bizmonitor.CIImpact, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nodes := c.impact[ciID]
	out := make([]bizmonitor.CIImpact, 0, len(nodes))
	for _, n := range nodes[:min(len(nodes), limit)] {
		if n.CanView = slices.Contains(c.ciViewers[n.ID], userID); !n.CanView {
			n = bizmonitor.CIImpact{ID: n.ID, Name: n.Name, Depth: n.Depth}
		}
		out = append(out, n)
	}
	return out, len(nodes) > limit, nil
}

// Models implements bizmonitor.CMDBCatalog.
func (c *monitorCross) Models(context.Context) ([]bizmonitor.CIModel, error) {
	return []bizmonitor.CIModel{{Code: "host", Name: "主机"}}, nil
}

// Groups implements bizmonitor.OncallGroups.
func (c *monitorCross) Groups(context.Context) ([]bizmonitor.OncallGroup, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.oncallGroups), nil
}

// AdminIDs implements bizmonitor.OncallGroups.
func (c *monitorCross) AdminIDs(_ context.Context, groupID string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.groupAdmins[groupID]), nil
}

// OnDutyByRole implements bizmonitor.DutyRoster.
func (c *monitorCross) OnDutyByRole(context.Context, string) (primary, backup []string, err error) {
	return nil, nil, nil
}

// Shifts implements bizmonitor.Shifts。
func (c *monitorCross) Shifts(_ context.Context, scheduleID string, from, to time.Time, limit int) ([]bizmonitor.ShiftSlot, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	slots, ok := c.shifts[scheduleID]
	if !ok {
		return nil, false, nil
	}
	out := make([]bizmonitor.ShiftSlot, 0, len(slots))
	for _, s := range slots {
		if s.End.After(from) && s.Start.Before(to) && len(out) < limit {
			out = append(out, s)
		}
	}
	return out, true, nil
}

// Available implements bizmonitor.Summarizer.
func (c *monitorCross) Available() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.aiAvailable
}

// Summarize implements bizmonitor.Summarizer.
func (c *monitorCross) Summarize(context.Context, string, string) (string, error) {
	return "摘要", nil
}

// RouteMatched implements bizmonitor.RouteProbe.
func (c *monitorCross) RouteMatched(context.Context, string, int32, map[string]string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.routeMatched, nil
}

// ValidateWriteURL implements bizmonitor.WriteURLValidator：地址策略由 data 层的集成测试覆盖，这里恒通过。
func (c *monitorCross) ValidateWriteURL(string) error { return nil }

func (c *monitorCross) set(mutate func(c *monitorCross)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	mutate(c)
}

// monitorWriteTargets 以数据源的内存仓储回答「数据源能否写回」（记录规则用）。
type monitorWriteTargets struct {
	sources bizmonitor.DatasourceReader
}

// WriteTargets implements bizmonitor.DatasourceWriteTargets.
func (w monitorWriteTargets) WriteTargets(ctx context.Context, ids []string) (map[string]bizmonitor.DatasourceWriteTarget, error) {
	out := make(map[string]bizmonitor.DatasourceWriteTarget, len(ids))
	for _, id := range ids {
		if d, err := w.sources.Get(ctx, id); err == nil {
			out[id] = bizmonitor.DatasourceWriteTarget{Name: d.Name, Enabled: d.Enabled, HasWriteURL: d.RemoteWriteURL != ""}
		}
	}
	return out, nil
}

// monitorP2Input 第一期装配里已有、第二、三期要共用的部件。
type monitorP2Input struct {
	ms      *testfake.MonitorStore
	store   *testfake.Store
	scopes  *bizmonitor.ScopeUsecase
	users   monitorUsers
	org     *testfake.MonitorOrg
	perms   bizmonitor.Permissions
	authz   *biz.AuthzUsecase
	query   *testfake.MonitorQueryClient
	targets *bizmonitor.TargetUsecase
}

// monitorP2Stack 第二、三期装配的产物。
type monitorP2Stack struct {
	datasource *monitorservice.DatasourceService
	rule       *monitorservice.RuleService
	alert      *monitorservice.AlertService
	dispatch   *bizmonitor.DispatchUsecase
	p2         monitorservice.P2Services
}

// newMonitorP2 装配第二、三期：规则 / 告警 / 数据源 / 派发用例经 Provide* 接上新的协作者，另建六个新服务。
func newMonitorP2(in monitorP2Input) monitorP2Stack {
	ms, users, org := in.ms, in.users, in.org
	cross := newMonitorCross()
	h := &monitorP2{
		ops: testfake.NewMonitorAlertOps(ms), escalations: testfake.NewMonitorEscalations(), sync: testfake.NewMonitorSyncStore(),
		inhibits: testfake.NewMonitorInhibitStore(), engine: &testfake.MonitorEngine{}, views: testfake.NewMonitorViews(),
		envs: &testfake.MonitorCIEnvs{}, cross: cross,
	}
	monitorP2Handles.Store(ms, h)
	// 升级策略的引用数按规则仓储现算（删除被引用的策略要 409）。
	h.escalations.Rules = func() []*bizmonitor.Rule {
		rules, _ := ms.Rules().ListByGroups(context.Background(), bizmonitor.GroupFilter{All: true}, 10000)
		return rules
	}
	txm := testfake.NewTxManager()
	events := biznotify.NewNotificationUsecase(in.store, nil)
	cis := h.sync.CIIndex()

	sources := bizmonitor.ProvideDatasourceUsecase(ms.Datasources(), ms.Datasources(), ms.Datasources(),
		&testfake.MonitorCipher{Enabled: true}, in.query, users, in.perms, cross)
	ruleUC := bizmonitor.ProvideRuleUsecase(in.scopes, ms.Rules(), ms.Rules(), ms.Rules(), ms.Versions(), ms.Evals(), ms.Datasources(), users, org,
		h.escalations, cross, cross, nil)
	dispatch, err := bizmonitor.ProvideDispatchUsecase(txm, ms.Outbox(), ms.Outbox(), ms.Alerts(), ms.Rules(), ms.Subscriptions(),
		in.scopes, users, org, org, events, testLogger(), nil, bizmonitor.NewDispatchExt(h.escalations, h.escalations, cis, cross, cross))
	if err != nil {
		panic(err) // 未给配置，不会失败
	}
	simulate, err := bizmonitor.NewSimulateUsecase(bizmonitor.SimulateDeps{
		Scopes: in.scopes, Rules: ruleUC, Sources: sources, Mutes: ms.Mutes(), Alerts: ms.Alerts(), Inhibits: h.inhibits, CIs: cis, Cfg: cis,
		Calendar: h.sync.Calendar(), Escalations: h.escalations, Subs: ms.Subscriptions(), Dir: users, Leaders: org, Duty: org,
		Roster: cross, Groups: cross, Schedules: org, Routes: cross, Events: events, Txm: txm,
	}, testLogger(), nil)
	if err != nil {
		panic(err)
	}
	options := bizmonitor.NewRuleOptionUsecase(cross, cross, cross, h.envs, cross, nil)

	decorator := bizmonitor.NewAlertDecorator(users, h.inhibits, cis, h.ops, h.ops)
	detail := bizmonitor.NewAlertDetailUsecase(h.ops, h.ops, h.ops, cis, cross, cross, users, ms.Mutes(), h.inhibits)
	alertUC := bizmonitor.ProvideAlertUsecase(in.scopes, ms.Alerts(), ms.Alerts(), ms.Logs(), ms.Rules(), ms.Mutes(), users, in.perms, decorator, detail)
	actions := bizmonitor.NewAlertActionUsecase(alertUC, detail, txm, h.ops, h.ops, h.ops, h.ops, h.ops, cross, cross)
	shiftCounts := bizmonitor.NewShiftCountUsecase(in.perms, topbiz.NewMonitorOncallAccess(in.authz), users, cross, ms.ShiftCounter())

	stats := ms.Stats()
	return monitorP2Stack{
		datasource: monitorservice.NewDatasourceService(sources),
		rule:       monitorservice.ProvideRuleService(ruleUC, in.targets, options, simulate),
		alert:      monitorservice.ProvideAlertService(alertUC, actions, shiftCounts),
		dispatch:   dispatch,
		p2: monitorservice.P2Services{
			Escalation: monitorservice.NewEscalationService(bizmonitor.NewEscalationUsecase(in.scopes, h.escalations, h.escalations, h.escalations, cross, users, org)),
			Inhibit: monitorservice.NewInhibitService(bizmonitor.ProvideInhibitUsecase(
				in.scopes, h.inhibits, h.inhibits, h.inhibits, ms.Alerts(), ms.Rules(), ms.Rules(), ms.Mutes(), users, decorator)),
			RecordRule: monitorservice.NewRecordRuleService(bizmonitor.NewRecordRuleUsecase(
				in.scopes, h.inhibits.RecordRules(), h.inhibits.RecordRules(), monitorWriteTargets{sources: ms.Datasources()}, users)),
			AlertView: monitorservice.NewAlertViewService(bizmonitor.NewAlertViewUsecase(in.scopes, h.views, h.views, users)),
			Stats:     monitorservice.NewStatsService(bizmonitor.NewStatsUsecase(in.scopes, stats, stats, ms.Rules(), cis)),
			Engine:    monitorservice.NewEngineService(bizmonitor.NewEngineUsecase(in.scopes, h.engine, h.engine, cis)),
		},
	}
}
