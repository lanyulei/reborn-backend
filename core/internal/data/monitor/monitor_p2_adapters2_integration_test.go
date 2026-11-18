//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	dataticket "github.com/lanyulei/reborn/backend/core/internal/data/ticket"
	dataworkflow "github.com/lanyulei/reborn/backend/core/internal/data/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 工单 / 流程两个域的组合根适配器（internal/biz/monitor_p2.go 的 TicketCatalog、TicketDesk、Workflows、
// EngineAudience）接到真实的 ticket_* / workflow_* / system_* 表上，并把用到它们的两条监控链路走通：
// 手动转工单（AlertActionUsecase.CreateTicket）与事件动作循环（ActionUsecase）。

const (
	// a2Lead 工单流程的审批人（不是监控域的任何角色）。
	a2Lead     = "018f3e1a-0d00-7000-8000-0000000000a1"
	a2Dept     = "018f3e1a-0d02-7000-8000-0000000000a1"
	a2Role     = "018f3e1a-0d01-7000-8000-0000000000a1"
	a2RoleCode = "monp2_engine_ops"
	a2TicketWF = "monp2_ticket_flow"
	a2HealWF   = "monp2_heal_flow"
)

// a2Graph 发起 → a2Lead 审批 → 结束（工单 / 流程实例停在审批上，保持处理中）。
const a2Graph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"approve","type":"userTask","name":"审批","assignee":{"rules":[{"kind":"user","userIds":["` + a2Lead + `"]}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[{"id":"e1","source":"start","target":"approve"},{"id":"e2","source":"approve","target":"end"}],
	"settings":{}}`

// a2Nobody 工单域的权限桩：没有人持有管理视图 / 目录维护权限（只剩提单人与流程参与者能看工单）。
type a2Nobody struct{}

func (a2Nobody) CanViewAll(context.Context, string) (bool, error)       { return false, nil }
func (a2Nobody) CanManageCatalog(context.Context, string) (bool, error) { return false, nil }

// a2Files 工单域的附件端口桩（这两条链路都不带附件）。
type a2Files struct{}

func (a2Files) Bind(context.Context, txrun.Tx, string, string, string, []string) error { return nil }

func (a2Files) ListByBiz(context.Context, string, string) ([]*bizfile.File, error) { return nil, nil }

func (a2Files) ReleaseFiles(context.Context, txrun.Tx, string, string, []string) error { return nil }

// a2Resources 工单域的 CMDB 资源端口桩（事件没有关联资源）。
type a2Resources struct{}

func (a2Resources) Resolve(context.Context, string, []string) ([]bizticket.Resource, error) {
	return nil, nil
}

type a2Env struct {
	*env
	grp      *biz.RuleGroup
	rule     *biz.Rule
	dir      *bizsystem.DirectoryUsecase
	tickets  *bizticket.Usecase
	catalogs *bizticket.CatalogUsecase
	catalog  biz.TicketCatalog
	desk     biz.TicketDesk
	flows    biz.Workflows
	tpl      *bizticket.Template
}

// a2Tables 本文件写到的其他域的表：开始前与结束后都清空，免得影响同包的其它测试与重复运行。
const a2Tables = `ticket_resource, ticket_comment, ticket_order, ticket_template, ticket_category, ticket_sequence,
	workflow_job, workflow_delegation, workflow_history, workflow_task_assignee, workflow_task, workflow_token, workflow_instance,
	workflow_definition_version, workflow_definition, notify_event, notify_dedup,
	system_refresh_token, system_user_role, system_dept_leader, system_user_dept, system_dept, "system_user"`

func newA2Env(t *testing.T) *a2Env {
	t.Helper()
	e := newEnv(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	reset := func() {
		if _, err := e.pool.Exec(context.Background(), "TRUNCATE TABLE "+a2Tables+" CASCADE"); err != nil {
			t.Errorf("reset ticket / workflow tables: %v", err)
		}
		if _, err := e.pool.Exec(context.Background(), "DELETE FROM system_role WHERE id = $1", a2Role); err != nil {
			t.Errorf("reset role: %v", err)
		}
	}
	reset()
	t.Cleanup(reset)
	// 用户与监控域测试主体同一批 ID：Alice（可管理）、Bob（只读）可用，Carol 已停用。
	exec(`INSERT INTO "system_user" (id, username, password, nickname, status) VALUES
		($1,'a2alice','x','Alice',1), ($2,'a2bob','x','Bob',1), ($3,'a2carol','x','Carol',2), ($4,'a2lead','x','审批人',1)`,
		uAlice, uBob, uCarol, a2Lead)
	exec(`INSERT INTO system_dept (id, parent_id, path, name) VALUES ($1::uuid, NULL, '/' || $1::text || '/', '运维组')`, a2Dept)
	exec(`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary) VALUES (gen_random_uuid(), $1, $3, true), (gen_random_uuid(), $2, $3, true)`,
		uAlice, uBob, a2Dept)
	exec(`INSERT INTO system_role (id, name, code) VALUES ($1, '监控运维', $2)`, a2Role, a2RoleCode)
	exec(`INSERT INTO system_user_role (id, user_id, role_id) VALUES (gen_random_uuid(), $1, $3), (gen_random_uuid(), $2, $3)`, uAlice, uCarol, a2Role)

	d := e.d
	catalogRepo, ticketRepo, comments := dataticket.NewCatalogRepo(d), dataticket.NewTicketRepo(d), dataticket.NewCommentRepo(d)
	defRepo, instRepo := dataworkflow.NewDefinitionRepo(d), dataworkflow.NewInstanceRepo(d)
	dir := bizsystem.NewDirectoryUsecase(datasystem.NewDirectoryRepo(d), datasystem.NewUserRepo(d), datasystem.NewDeptRepo(d))
	txs := basedata.NewTxManager(d)
	access := bizticket.NewAccess(ticketRepo, instRepo, a2Nobody{})
	notify := biznotify.NewNotificationUsecase(datanotify.NewEventRepo(d), nil)
	wfCfg, err := dataworkflow.NewConfigRepo(d, &conf.Notify{})
	if err != nil {
		t.Fatalf("NewConfigRepo() error = %v", err)
	}
	calendar := bizworkflow.NewCalendarUsecase(wfCfg)
	defs := bizworkflow.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, wfCfg, bizworkflow.DefinitionReferrers{catalogRepo})
	rt := bizworkflow.NewRuntimeUsecase(defs, instRepo, instRepo, instRepo, dir, topbiz.NewWorkflowUsers(dir), wfCfg, calendar, txs, notify,
		bizworkflow.Listeners{bizticket.BusinessType: bizticket.NewStatusSyncer(ticketRepo, ticketRepo)},
		bizworkflow.Viewers{bizticket.BusinessType: access}, nil)
	catalogs := bizticket.NewCatalogUsecase(catalogRepo, catalogRepo, catalogRepo, defs, a2Nobody{}, dir)
	tickets := bizticket.NewUsecase(catalogRepo, ticketRepo, ticketRepo, comments, rt, a2Files{}, dir, access, txs,
		bizticket.NewExtensions(ticketRepo, ticketRepo, rt, topbiz.NewTicketDeadlines(calendar), a2Resources{}, notify, quietLogger()))
	for key, name := range map[string]string{a2TicketWF: "故障处理", a2HealWF: "告警自愈"} {
		def, err := defs.Create(ctx, &bizworkflow.Definition{Key: key, Name: name})
		if err != nil {
			t.Fatalf("create workflow %s: %v", key, err)
		}
		rev, issues, err := defs.SaveDraft(ctx, def.ID, []byte(a2Graph), def.Revision)
		if err != nil || len(issues) > 0 {
			t.Fatalf("save draft %s: issues %+v, err %v", key, issues, err)
		}
		if _, err := defs.Publish(ctx, def.ID, rev); err != nil {
			t.Fatalf("publish %s: %v", key, err)
		}
	}
	cat, err := catalogs.CreateCategory(ctx, &bizticket.Category{Name: "故障"})
	if err != nil {
		t.Fatalf("create category: %v", err)
	}
	// 表单没有必填项：自动建单不带表单数据。
	tpl, err := catalogs.CreateTemplate(ctx, &bizticket.Template{
		CategoryID: cat.ID, Name: "故障单", WorkflowKey: a2TicketWF, NumberPrefix: "INC", Status: bizticket.TemplateEnabled,
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}
	g := e.group(mc.RootGroupID, "a2-适配器")
	if err := e.scopes.Replace(ctx, g.ID, []*biz.RoleScope{
		{RoleID: roleA, GroupID: g.ID, Level: mc.ScopeManage}, {RoleID: roleB, GroupID: g.ID, Level: mc.ScopeRead},
	}, uAdmin); err != nil {
		t.Fatalf("grant scopes: %v", err)
	}
	return &a2Env{
		env: e, grp: g, rule: e.rule(g.ID, "磁盘告警"), dir: dir, tickets: tickets, catalogs: catalogs, tpl: tpl,
		catalog: topbiz.NewMonitorTicketCatalog(catalogs), desk: topbiz.NewMonitorTicketDesk(tickets), flows: topbiz.NewMonitorWorkflows(defs, rt),
	}
}

func (e *a2Env) firing() string {
	return e.alert(alertRow{ruleID: e.rule.ID, ruleName: "磁盘告警", groupID: e.grp.ID, severity: 3, status: mc.AlertFiring})
}

// actions 装配事件动作用例：工单 / 流程 / 组织目录都是真实实现。
func (e *a2Env) actions() *biz.ActionUsecase {
	e.t.Helper()
	ops := NewAlertOpsRepo(e.d)
	uc, err := biz.NewActionUsecase(biz.NewActionDeps(basedata.NewTxManager(e.d), NewActionRepo(e.d), e.alerts, ops, ops, ops, NewCIIndexRepo(e.d),
		biz.NewScopeUsecase(e.org, e.scopes, e.groups), e.dir, e.catalog, e.desk, e.flows, mergeCMDB{}, mergeSummary{}, quietLogger(), &conf.Monitor{}))
	if err != nil {
		e.t.Fatalf("NewActionUsecase() error = %v", err)
	}
	return uc
}

func (e *a2Env) alertTicket(alertID string) string {
	e.t.Helper()
	var id *string
	if err := e.pool.QueryRow(context.Background(), "SELECT ticket_id::text FROM monitor_alert WHERE id = $1", alertID).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	if id == nil {
		return ""
	}
	return *id
}

func TestMonitorTicketAdapters_OnRealTables(t *testing.T) {
	e := newA2Env(t)
	ctx := context.Background()

	// 模板目录：启用的模板可见；提单人视角下停用的模板等于不存在。
	list, err := e.catalog.Templates(ctx)
	if err != nil || len(list) != 1 || list[0].ID != e.tpl.ID || list[0].Name != "故障单" || list[0].CategoryName != "故障" {
		t.Fatalf("Templates() = %+v, %v", list, err)
	}
	if got, err := e.catalog.Template(ctx, e.tpl.ID, uAlice); err != nil || got == nil || got.ID != e.tpl.ID {
		t.Fatalf("Template(enabled) = %+v, %v", got, err)
	}
	if got, err := e.catalog.Template(ctx, newID(t), uAlice); err != nil || got != nil {
		t.Fatalf("Template(missing) = %+v, %v; want nil", got, err)
	}
	// 流程目录：已发布的两个流程；不存在的标识返回 nil。
	defs, err := e.flows.Definitions(ctx)
	if err != nil || len(defs) != 2 {
		t.Fatalf("Definitions() = %+v, %v", defs, err)
	}
	if def, err := e.flows.Definition(ctx, a2HealWF); err != nil || def == nil || def.Name != "告警自愈" {
		t.Fatalf("Definition(heal) = %+v, %v", def, err)
	}
	if def, err := e.flows.Definition(ctx, "monp2_nope"); err != nil || def != nil {
		t.Fatalf("Definition(missing) = %+v, %v; want nil", def, err)
	}
	// 自监控通知的收件人：配置的角色里的可用成员（Carol 已停用，不在其中）；未配置返回空。
	if ids, err := topbiz.NewMonitorEngineAudience(e.dir, &conf.Monitor{EngineNotifyRoles: []string{a2RoleCode, " "}}).UserIDs(ctx); err != nil ||
		!slices.Equal(ids, []string{uAlice}) {
		t.Fatalf("EngineAudience.UserIDs() = %v, %v; want only Alice", ids, err)
	}
	if ids, err := topbiz.NewMonitorEngineAudience(e.dir, &conf.Monitor{}).UserIDs(ctx); err != nil || ids != nil {
		t.Fatalf("EngineAudience.UserIDs(no roles) = %v, %v", ids, err)
	}

	// 工单：创建、按幂等键重放、查看权限、评论去重。
	p := biz.TicketCreate{TemplateID: e.tpl.ID, Title: "适配器", Priority: 2, Creator: uAlice, RequestID: newID(t)}
	first, err := e.desk.Create(ctx, p)
	if err != nil || first.Creator != uAlice || first.Number == "" || first.Title != "适配器" {
		t.Fatalf("Create() = %+v, %v", first, err)
	}
	if again, err := e.desk.Create(ctx, p); err != nil || again.ID != first.ID || e.count("SELECT count(*) FROM ticket_order") != 1 {
		t.Fatalf("Create(replay) = %+v, %v; want the same ticket", again, err)
	}
	if v, err := e.desk.View(ctx, first.ID, uAlice); err != nil || v == nil || v.Number != first.Number {
		t.Fatalf("View(creator) = %+v, %v", v, err)
	}
	if v, err := e.desk.View(ctx, first.ID, a2Lead); err != nil || v == nil {
		t.Fatalf("View(approver) = %+v, %v; want visible to the workflow participant", v, err)
	}
	if v, err := e.desk.View(ctx, first.ID, uBob); err != nil || v != nil {
		t.Fatalf("View(non-participant) = %+v, %v; want nil", v, err)
	}
	if v, err := e.desk.View(ctx, newID(t), uAlice); err != nil || v != nil {
		t.Fatalf("View(missing) = %+v, %v; want nil", v, err)
	}
	for range 2 {
		if err := e.desk.Comment(ctx, first.ID, uAlice, "告警已恢复"); err != nil {
			t.Fatalf("Comment() error = %v", err)
		}
	}
	comments, _, err := e.tickets.Comments(ctx, first.ID, uAlice)
	if err != nil || len(comments) != 1 || comments[0].UserID != uAlice || comments[0].Content != "告警已恢复" {
		t.Fatalf("comments = %+v, %v; want exactly one (identical content is not appended twice)", comments, err)
	}
	if err := e.desk.Comment(ctx, first.ID, uBob, "路过"); err == nil {
		t.Fatal("Comment(non-participant) = nil, want the ticket domain to refuse")
	}
}

func TestAlertCreateTicket_OnRealTicketTables(t *testing.T) {
	e := newA2Env(t)
	ctx := context.Background()
	ops := NewAlertOpsRepo(e.d)
	e.org.AlertReaders = []string{uAlice, uBob}
	detail := biz.NewAlertDetailUsecase(ops, ops, ops, stubCIs{}, stubCMDB{}, e.desk, e.org, e.mutes, stubInhibits{})
	alerts := biz.ProvideAlertUsecase(biz.NewScopeUsecase(e.org, e.scopes, e.groups), e.alerts, e.alerts, e.logs, e.rules, e.mutes, e.org, e.org,
		biz.NewAlertDecorator(e.org, stubInhibits{}, stubCIs{}, ops, ops), detail)
	uc := biz.NewAlertActionUsecase(alerts, detail, basedata.NewTxManager(e.d), ops, ops, ops, ops, ops, e.desk, stubCMDB{})
	alertID := e.firing()
	in := biz.AlertTicketInput{TemplateID: e.tpl.ID, RequestID: newID(t)}

	// 只读用户不能转工单；什么都没建。
	if _, _, err := uc.CreateTicket(ctx, uBob, alertID, in); !kerrors.Is(err, biz.ErrForbidden) || e.count("SELECT count(*) FROM ticket_order") != 0 {
		t.Fatalf("CreateTicket(read-only) error = %v, want ErrForbidden and no ticket", err)
	}
	ticket, view, err := uc.CreateTicket(ctx, uAlice, alertID, in)
	if err != nil {
		t.Fatalf("CreateTicket() error = %v", err)
	}
	var creator, title, number string
	var priority int32
	if err := e.pool.QueryRow(ctx, "SELECT creator::text, title, number, priority FROM ticket_order WHERE id = $1", ticket.ID).
		Scan(&creator, &title, &number, &priority); err != nil {
		t.Fatalf("read ticket row: %v", err)
	}
	if creator != uAlice || title != "【严重】磁盘告警" || priority != 3 || number != ticket.Number || number == "" {
		t.Fatalf("ticket row = creator %s, title %q, number %q, priority %d", creator, title, number, priority)
	}
	if e.alertTicket(alertID) != ticket.ID || view.TicketID != ticket.ID || view.TicketNumber != number {
		t.Fatalf("alert ticket = %q, view = %q / %q; want %s / %s", e.alertTicket(alertID), view.TicketID, view.TicketNumber, ticket.ID, number)
	}
	if n := e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2 AND operator = $3
		AND detail->>'ticket_id' = $4 AND detail->>'ticket_number' = $5 AND detail->>'auto' IS NULL`,
		alertID, mc.AlertEventTicket, uAlice, ticket.ID, number); n != 1 {
		t.Fatalf("ticket timeline entries = %d, want 1 carrying the ticket number", n)
	}
	// 工单上挂着流程实例（工单域自己发起）。
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE business_type = $1 AND business_id = $2", bizticket.BusinessType, ticket.ID); n != 1 {
		t.Fatalf("ticket workflow instances = %d", n)
	}
	// 事件详情里的工单卡片：提单人看得到标题，无关的人只看到编号。
	_, x, err := alerts.GetDetail(ctx, uAlice, alertID)
	if err != nil || x.Ticket == nil || x.Ticket.ID != ticket.ID || !x.Ticket.CanView || x.Ticket.Title != title {
		t.Fatalf("detail ticket (creator) = %+v, %v", x.Ticket, err)
	}
	_, x, err = alerts.GetDetail(ctx, uBob, alertID)
	if err != nil || x.Ticket == nil || x.Ticket.CanView || x.Ticket.Title != "" || x.Ticket.Number != number {
		t.Fatalf("detail ticket (non-participant) = %+v, %v; want number only", x.Ticket, err)
	}

	// 带幂等键的重复提交（提单人本人）：返回首次创建的那张工单；不带幂等键再提，或换一个人提：409。都不会
	// 多出工单与时间线条目。
	for name, req := range map[string]biz.AlertTicketInput{"replay": in, "retry with a key": {TemplateID: e.tpl.ID, RequestID: newID(t)}} {
		again, _, err := uc.CreateTicket(ctx, uAlice, alertID, req)
		if req.RequestID == "" {
			if !kerrors.Is(err, biz.ErrAlertTicketExists) || kerrors.FromError(err).Metadata["ticket_id"] != ticket.ID {
				t.Fatalf("CreateTicket(%s, no key) error = %v, want ErrAlertTicketExists with ticket_id %s", name, err, ticket.ID)
			}
			continue
		}
		if err != nil || again.ID != ticket.ID || again.Number != ticket.Number {
			t.Fatalf("CreateTicket(%s) = %+v, %v; want the first ticket %s", name, again, err, ticket.ID)
		}
	}
	if _, _, err := uc.CreateTicket(ctx, uAdmin, alertID, biz.AlertTicketInput{TemplateID: e.tpl.ID, RequestID: newID(t)}); !kerrors.Is(err, biz.ErrAlertTicketExists) ||
		kerrors.FromError(err).Metadata["ticket_id"] != ticket.ID {
		t.Fatalf("CreateTicket(another user) error = %v, want ErrAlertTicketExists with ticket_id %s", err, ticket.ID)
	}
	if tickets, entries := e.count("SELECT count(*) FROM ticket_order"),
		e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2", alertID, mc.AlertEventTicket); tickets != 1 || entries != 1 {
		t.Fatalf("after replays: tickets = %d, timeline entries = %d; want 1 / 1", tickets, entries)
	}
	// 模板停用后转工单：工单域的错误原样返回，事件不被关联。
	other := e.firing()
	off := *e.tpl
	off.Status = bizticket.TemplateDisabled
	if err := e.catalogs.UpdateTemplate(ctx, &off); err != nil {
		t.Fatalf("disable template: %v", err)
	}
	if _, _, err := uc.CreateTicket(ctx, uAlice, other, biz.AlertTicketInput{TemplateID: e.tpl.ID}); !kerrors.Is(err, bizticket.ErrTemplateUnavailable) ||
		e.alertTicket(other) != "" {
		t.Fatalf("CreateTicket(disabled template) error = %v, alert ticket %q; want ErrTemplateUnavailable and no link", err, e.alertTicket(other))
	}
}

func TestActionUsecase_OnRealTicketAndWorkflowTables(t *testing.T) {
	e := newA2Env(t)
	ctx := context.Background()
	uc := e.actions()
	payload := func(agent string) string {
		return `{"template_id":"` + e.tpl.ID + `","agent_user_id":"` + agent + `","definition_key":"` + a2HealWF + `"}`
	}
	events := func(alertID, typ string) int {
		return e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2", alertID, typ)
	}

	// 自动建单 + 触发流程。两者都先模拟「外部调用已成功、落账前进程退出」：循环重做时以事件 ID 为幂等键拿回
	// 首次的工单 / 流程实例，不会多建。
	alertID := e.firing()
	lost, err := e.desk.Create(ctx, biz.TicketCreate{TemplateID: e.tpl.ID, Title: "【严重】磁盘告警", Priority: 3, Creator: uAlice, RequestID: alertID})
	if err != nil {
		t.Fatalf("pre-create ticket: %v", err)
	}
	lostInstance, err := e.flows.Start(ctx, biz.WorkflowStart{DefinitionKey: a2HealWF, AlertID: alertID, Title: "磁盘告警", Initiator: uAlice})
	if err != nil {
		t.Fatalf("pre-start workflow: %v", err)
	}
	ticketAction := e.seedAction(alertID, mc.ActionTicket, payload(uAlice))
	flowAction := e.seedAction(alertID, mc.ActionWorkflow, payload(uAlice))
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for name, id := range map[string]string{"ticket": ticketAction, "workflow": flowAction} {
		if s := e.actionState(id); s.status != mc.ActionDone || !s.done || s.attempts != 1 {
			t.Fatalf("%s action = %+v, want done after one attempt", name, s)
		}
	}
	a, err := e.alerts.Get(ctx, alertID)
	if err != nil || a.TicketID != lost.ID || a.WorkflowInstanceID != lostInstance {
		t.Fatalf("alert = ticket %q, instance %q, %v; want %s / %s", a.TicketID, a.WorkflowInstanceID, err, lost.ID, lostInstance)
	}
	if n := e.count("SELECT count(*) FROM ticket_order WHERE creator = $1", uAlice); n != 1 {
		t.Fatalf("tickets = %d, want exactly one (RequestID = alert id)", n)
	}
	if n := e.count(`SELECT count(*) FROM workflow_instance WHERE business_type = $1 AND business_id = $2 AND initiator = $3 AND id = $4`,
		mc.WorkflowBusinessType, alertID, uAlice, lostInstance); n != 1 {
		t.Fatalf("alert workflow instances = %d, want one with business (monitor_alert, alert id)", n)
	}
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE business_type = $1", mc.WorkflowBusinessType); n != 1 {
		t.Fatalf("monitor_alert workflow instances = %d", n)
	}
	if n := e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2 AND operator = $3
		AND detail->>'ticket_number' = $4 AND detail->>'auto' = 'true'`, alertID, mc.AlertEventTicket, uAlice, lost.Number); n != 1 {
		t.Fatalf("auto ticket timeline entries = %d", n)
	}
	if n := e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = $2 AND detail->>'workflow_instance_id' = $3`,
		alertID, mc.AlertEventWorkflow, lostInstance); n != 1 {
		t.Fatalf("workflow timeline entries = %d", n)
	}
	// 再跑一轮什么都不做。
	if n, err := uc.Run(ctx); err != nil || n != 0 || e.count("SELECT count(*) FROM ticket_order") != 1 {
		t.Fatalf("second Run() = %d, %v", n, err)
	}

	// 告警恢复：以提单人（代理提单人）的名义在工单上追加一条记录；动作被重新领取（落账丢失）也只有一条。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET status = $2, resolve_time = now() WHERE id = $1", alertID, mc.AlertResolved); err != nil {
		t.Fatal(err)
	}
	note := e.seedAction(alertID, mc.ActionTicketNote, "{}")
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("Run(ticket note) error = %v", err)
	}
	if s := e.actionState(note); s.status != mc.ActionDone {
		t.Fatalf("ticket note action = %+v", s)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_action SET status = $2, done_time = NULL, next_time = now() WHERE id = $1", note, mc.ActionPending); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("Run(ticket note again) error = %v", err)
	}
	comments, _, err := e.tickets.Comments(ctx, lost.ID, uAlice)
	if err != nil || len(comments) != 1 || comments[0].UserID != uAlice {
		t.Fatalf("ticket comments = %+v, %v; want exactly one by the ticket creator", comments, err)
	}

	// 代理提单人已停用：立即失败（不重试），记入时间线；不建单、不发起流程。
	disabled := e.firing()
	failTicket := e.seedAction(disabled, mc.ActionTicket, payload(uCarol))
	failFlow := e.seedAction(disabled, mc.ActionWorkflow, payload(uCarol))
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("Run(disabled agent) error = %v", err)
	}
	for name, id := range map[string]string{"ticket": failTicket, "workflow": failFlow} {
		if s := e.actionState(id); s.status != mc.ActionFailed || s.attempts != 1 || !s.done || s.lastErr == "" {
			t.Fatalf("%s action with a disabled agent = %+v, want failed after one attempt", name, s)
		}
	}
	if events(disabled, mc.AlertEventActionFailed) != 2 || e.alertTicket(disabled) != "" || e.count("SELECT count(*) FROM ticket_order") != 1 {
		t.Fatalf("disabled agent: action_failed entries = %d, alert ticket %q", events(disabled, mc.AlertEventActionFailed), e.alertTicket(disabled))
	}

	// 流程标识不存在：流程动作失败。
	noFlow := e.firing()
	missing := e.seedAction(noFlow, mc.ActionWorkflow, `{"agent_user_id":"`+uAlice+`","definition_key":"monp2_nope"}`)
	// 模板已停用：建单动作失败。
	off := *e.tpl
	off.Status = bizticket.TemplateDisabled
	if err := e.catalogs.UpdateTemplate(ctx, &off); err != nil {
		t.Fatalf("disable template: %v", err)
	}
	noTpl := e.firing()
	stale := e.seedAction(noTpl, mc.ActionTicket, payload(uAlice))
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("Run(unusable template / workflow) error = %v", err)
	}
	for name, id := range map[string]string{"missing workflow": missing, "disabled template": stale} {
		if s := e.actionState(id); s.status != mc.ActionFailed || s.attempts != 1 {
			t.Fatalf("%s action = %+v, want failed immediately", name, s)
		}
	}
	if events(noTpl, mc.AlertEventActionFailed) != 1 || events(noFlow, mc.AlertEventActionFailed) != 1 || e.count("SELECT count(*) FROM ticket_order") != 1 {
		t.Fatalf("action_failed entries = %d / %d", events(noTpl, mc.AlertEventActionFailed), events(noFlow, mc.AlertEventActionFailed))
	}
}
