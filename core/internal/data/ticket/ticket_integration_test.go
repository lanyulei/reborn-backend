//go:build integration

package data

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datafile "github.com/lanyulei/reborn/backend/core/internal/data/file"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	dataworkflow "github.com/lanyulei/reborn/backend/core/internal/data/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 固定的测试主体。
const (
	uInit   = "018f3e1a-0a10-7000-8000-000000000001"
	uLead   = "018f3e1a-0a10-7000-8000-000000000002"
	uSuper  = "018f3e1a-0a10-7000-8000-000000000003"
	uOther  = "018f3e1a-0a10-7000-8000-000000000004"
	dTeam   = "018f3e1a-0d10-7000-8000-000000000001"
	flowKey = "ticket_flow"
)

// flowGraph 发起 → 指定成员审批 → 结束。
const flowGraph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"approve","type":"userTask","name":"审批","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[{"id":"e1","source":"start","target":"approve"},{"id":"e2","source":"approve","target":"end"}],
	"settings":{}}`

// skipGraph 唯一的审批节点没有处理人且配置为自动跳过：流程在发起的同一事务里即结束。
const skipGraph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"approve","type":"userTask","name":"审批","assignee":{"onEmpty":"skip","rules":[{"kind":"deptLeader","level":1}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[{"id":"e1","source":"start","target":"approve"},{"id":"e2","source":"approve","target":"end"}],
	"settings":{}}`

var pngData = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)

// supervisors 持有管理视图的用户集合。
type supervisors map[string]bool

func (s supervisors) CanViewAll(_ context.Context, userID string) (bool, error) {
	return s[userID], nil
}

func (s supervisors) CanManageCatalog(_ context.Context, userID string) (bool, error) {
	return s[userID], nil
}

// usersAdapter 把 system 域组织目录适配为引擎的用户目录（与组合根同一转换）。
type usersAdapter struct{ dir *bizsystem.DirectoryUsecase }

func (a usersAdapter) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	return a.dir.UserNames(ctx, ids)
}

func (a usersAdapter) Candidates(context.Context, string, int64) ([]bizworkflow.UserInfo, error) {
	return nil, nil
}

func (a usersAdapter) PrimaryDept(ctx context.Context, userID string) (string, []string, error) {
	return a.dir.PrimaryDept(ctx, userID)
}

type env struct {
	t       *testing.T
	d       *basedata.Data
	catalog *biz.CatalogUsecase
	uc      *biz.Usecase
	files   *bizfile.Usecase
	defs    *bizworkflow.DefinitionUsecase
	rt      *bizworkflow.RuntimeUsecase
	jobs    *bizworkflow.JobUsecase
}

// deadlines 把流程引擎的工作日历适配为工单的时限推算（与组合根同一转换）。
type deadlines struct{ calendar *bizworkflow.CalendarUsecase }

func (d deadlines) Due(ctx context.Context, from time.Time, dur biz.Duration) (time.Time, error) {
	return d.calendar.Due(ctx, from, bizworkflow.Duration{Value: int(dur.Value), Unit: dur.Unit, WorkTime: dur.WorkTime})
}

// resources CMDB 资源桩：以 ciOK 开头的 ID 视为提单人可见的资源。
type resources struct{}

const (
	ciOK1    = "018f3e1a-0c10-7000-8000-000000000001"
	ciOK2    = "018f3e1a-0c10-7000-8000-000000000002"
	ciHidden = "018f3e1a-0c99-7000-8000-000000000001"
)

// resolvedInTx 记下资源解析是否曾在事务内被调用（它走连接池里的另一条连接，必须在事务外）。
var resolvedInTx atomic.Bool

func (resources) Resolve(ctx context.Context, _ string, ids []string) ([]biz.Resource, error) {
	if txrun.FromContext(ctx) != nil {
		resolvedInTx.Store(true)
	}
	var out []biz.Resource
	for _, id := range ids {
		if strings.HasPrefix(id, "018f3e1a-0c10") {
			out = append(out, biz.Resource{ID: id, Name: "db-" + id[len(id)-1:], ModelName: "数据库"})
		}
	}
	return out, nil
}

// newEnv 以真实仓储按组合根的同一方式装配工单 / 流程 / 附件三域。
func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	if cfg, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	} else if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"（集成测试会 TRUNCATE 业务表）", db)
	}
	c := &conf.Data{Database: &kitconf.Database{Source: dsn}, Upload: &conf.Data_Upload{Dir: t.TempDir()}}
	d, cleanup, err := basedata.NewData(c)
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE ticket_resource, ticket_comment, ticket_order, ticket_template, ticket_category, ticket_sequence,
		file_object, workflow_job, workflow_delegation, workflow_webhook, workflow_client, workflow_history, workflow_task_assignee, workflow_task, workflow_token, workflow_instance,
		workflow_definition_version, workflow_definition, notify_event, notify_dedup,
		system_permission, system_grant, system_refresh_token, system_user_role, system_role,
		system_dept_leader, system_user_dept, system_dept, "system_user"`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	seed := []string{
		`INSERT INTO "system_user" (id, username, password, nickname, status) VALUES
			('` + uInit + `','init','x','提单人',1), ('` + uLead + `','lead','x','审批人',1),
			('` + uSuper + `','super','x','主管',1), ('` + uOther + `','other','x','路人',1)`,
		`INSERT INTO system_dept (id, parent_id, path, name) VALUES ('` + dTeam + `', NULL, '/` + dTeam + `/', '研发组')`,
		`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary) VALUES (gen_random_uuid(), '` + uInit + `', '` + dTeam + `', true)`,
	}
	for _, sql := range seed {
		if _, err := d.Pool.Exec(ctx, sql); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	catalogRepo, ticketRepo, comments := NewCatalogRepo(d), NewTicketRepo(d), NewCommentRepo(d)
	defRepo, instRepo, jobRepo := dataworkflow.NewDefinitionRepo(d), dataworkflow.NewInstanceRepo(d), dataworkflow.NewJobRepo(d)
	dir := bizsystem.NewDirectoryUsecase(datasystem.NewDirectoryRepo(d), datasystem.NewUserRepo(d), datasystem.NewDeptRepo(d))
	txs := basedata.NewTxManager(d)
	access := biz.NewAccess(ticketRepo, instRepo, supervisors{uSuper: true})
	notify := biznotify.NewNotificationUsecase(datanotify.NewEventRepo(d), nil)
	wfCfg, err := dataworkflow.NewConfigRepo(d, &conf.Notify{})
	if err != nil {
		t.Fatalf("NewConfigRepo() error = %v", err)
	}
	defs := bizworkflow.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, wfCfg, bizworkflow.DefinitionReferrers{catalogRepo})
	rt := bizworkflow.NewRuntimeUsecase(defs, instRepo, instRepo, instRepo, dir, usersAdapter{dir}, wfCfg,
		bizworkflow.NewCalendarUsecase(wfCfg), txs,
		notify, bizworkflow.Listeners{biz.BusinessType: biz.NewStatusSyncer(ticketRepo, ticketRepo)},
		bizworkflow.Viewers{biz.BusinessType: access}, nil)
	stores, err := datafile.NewStores(c, d)
	if err != nil {
		t.Fatalf("NewStores() error = %v", err)
	}
	fileRepo := datafile.NewFileRepo(d)
	files, err := bizfile.NewUsecase(fileRepo, fileRepo, fileRepo, stores, bizfile.AccessCheckers{biz.BusinessType: biz.NewFileAccess(access, rt)})
	if err != nil {
		t.Fatalf("NewUsecase(file) error = %v", err)
	}
	e := &env{
		t: t, d: d, defs: defs, rt: rt, files: files,
		catalog: biz.NewCatalogUsecase(catalogRepo, catalogRepo, catalogRepo, defs, supervisors{uSuper: true}, dir),
		uc: biz.NewUsecase(catalogRepo, ticketRepo, ticketRepo, comments, rt, files, dir, access, txs,
			biz.NewExtensions(ticketRepo, ticketRepo, rt, deadlines{bizworkflow.NewCalendarUsecase(wfCfg)}, resources{}, notify, slog.New(slog.NewTextHandler(io.Discard, nil)))),
		jobs: bizworkflow.NewJobUsecase(rt, jobRepo, jobRepo, wfCfg, dataworkflow.NewWebhookCaller(), slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	e.publish(flowKey, flowGraph)
	return e
}

func (e *env) publish(key, graph string) {
	e.t.Helper()
	ctx := context.Background()
	d, err := e.defs.Create(ctx, &bizworkflow.Definition{Key: key, Name: "流程-" + key})
	if err != nil {
		e.t.Fatalf("Create(%s) error = %v", key, err)
	}
	rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(graph), d.Revision)
	if err != nil || len(issues) > 0 {
		e.t.Fatalf("SaveDraft(%s) = issues %+v, err %v", key, issues, err)
	}
	if _, err = e.defs.Publish(ctx, d.ID, rev); err != nil {
		e.t.Fatalf("Publish(%s) error = %v", key, err)
	}
}

// template 建一个分类和一张启用的模板（表单：原因必填、金额、对接人）。
func (e *env) template(name, workflowKey string) *biz.Template {
	e.t.Helper()
	ctx := context.Background()
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "分类-" + name})
	if err != nil {
		e.t.Fatalf("CreateCategory() error = %v", err)
	}
	tpl, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: c.ID, Name: name, WorkflowKey: workflowKey, NumberPrefix: "REQ", Status: biz.TemplateEnabled,
		Form: []biz.FormField{
			{Code: "reason", Name: "原因", Type: formschema.TypeString, Required: true},
			{Code: "amount", Name: "金额", Type: formschema.TypeInt},
			{Code: "contact", Name: "对接人", Type: formschema.TypeUser},
		},
	})
	if err != nil {
		e.t.Fatalf("CreateTemplate() error = %v", err)
	}
	return tpl
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.d.Pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count: %v", err)
	}
	return n
}

func TestIntegration_Catalog(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	if _, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "分类-权限申请"}); !errors.Is(err, biz.ErrCategoryNameExists) {
		t.Fatalf("CreateCategory(dup) error = %v, want ErrCategoryNameExists", err)
	}
	spec := func(mut func(*biz.Template)) *biz.Template {
		c := *tpl
		c.Name = "另一张"
		mut(&c)
		return &c
	}
	if _, err := e.catalog.CreateTemplate(ctx, spec(func(c *biz.Template) { c.Name = tpl.Name })); !errors.Is(err, biz.ErrTemplateNameExists) {
		t.Fatalf("CreateTemplate(dup name) error = %v, want ErrTemplateNameExists", err)
	}
	invalid := map[string]func(*biz.Template){
		"unknown workflow": func(c *biz.Template) { c.WorkflowKey = "nope" },
		"reserved code":    func(c *biz.Template) { c.Form = []biz.FormField{{Code: "priority", Type: formschema.TypeInt}} },
		"duplicate code": func(c *biz.Template) {
			c.Form = []biz.FormField{{Code: "a", Type: formschema.TypeInt}, {Code: "a", Type: formschema.TypeInt}}
		},
		"unsupported type": func(c *biz.Template) { c.Form = []biz.FormField{{Code: "a", Type: formschema.TypeRef}} },
		"enum without values": func(c *biz.Template) {
			c.Form = []biz.FormField{{Code: "a", Type: formschema.TypeEnum}}
		},
	}
	for name, mut := range invalid {
		if _, err := e.catalog.CreateTemplate(ctx, spec(mut)); !errors.Is(err, biz.ErrTemplateInvalid) {
			t.Fatalf("CreateTemplate(%s) error = %v, want ErrTemplateInvalid", name, err)
		}
	}
	if _, err := e.catalog.CreateTemplate(ctx, spec(func(c *biz.Template) { c.CategoryID = uOther })); !errors.Is(err, biz.ErrCategoryNotFound) {
		t.Fatalf("CreateTemplate(unknown category) error = %v, want ErrCategoryNotFound", err)
	}
	// 启用的模板须绑定可发起的流程；停用状态下可以先绑定未发布的流程。
	if _, err := e.defs.Create(ctx, &bizworkflow.Definition{Key: "draft_only", Name: "未发布"}); err != nil {
		t.Fatalf("Create(draft_only) error = %v", err)
	}
	if _, err := e.catalog.CreateTemplate(ctx, spec(func(c *biz.Template) { c.WorkflowKey = "draft_only" })); !errors.Is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("CreateTemplate(unpublished workflow) error = %v, want ErrTemplateInvalid", err)
	}
	draft, err := e.catalog.CreateTemplate(ctx, spec(func(c *biz.Template) {
		c.Name, c.WorkflowKey, c.Status = "草稿流程", "draft_only", biz.TemplateDisabled
	}))
	if err != nil {
		t.Fatalf("CreateTemplate(disabled, unpublished workflow) error = %v", err)
	}
	if err := e.catalog.DeleteTemplate(ctx, draft.ID); err != nil {
		t.Fatalf("DeleteTemplate(draft) error = %v", err)
	}
	// 分类下有模板不能删；停用的模板不出现在提单目录里，没有可用模板的分类也不出现。
	if err := e.catalog.DeleteCategory(ctx, tpl.CategoryID); !errors.Is(err, biz.ErrCategoryInUse) {
		t.Fatalf("DeleteCategory(in use) error = %v, want ErrCategoryInUse", err)
	}
	off := *tpl
	off.Status = biz.TemplateDisabled
	if err := e.catalog.UpdateTemplate(ctx, &off); err != nil {
		t.Fatalf("UpdateTemplate(disable) error = %v", err)
	}
	all, err := e.catalog.List(ctx, false)
	if err != nil || len(all) != 1 || len(all[0].Templates) != 1 {
		t.Fatalf("List(all) = %+v, err %v", all, err)
	}
	// 已停用的模板：管理者能读，其余人按不存在处理。
	if got, _, err := e.catalog.GetTemplate(ctx, tpl.ID, uSuper); err != nil || got.ID != tpl.ID {
		t.Fatalf("GetTemplate(disabled, manager) = %+v, err %v", got, err)
	}
	if _, _, err := e.catalog.GetTemplate(ctx, tpl.ID, uInit); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("GetTemplate(disabled, submitter) error = %v, want ErrTemplateNotFound", err)
	}
	if enabled, err := e.catalog.List(ctx, true); err != nil || len(enabled) != 0 {
		t.Fatalf("List(enabled) = %+v, err %v; want empty", enabled, err)
	}
	if _, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}}); !errors.Is(err, biz.ErrTemplateUnavailable) {
		t.Fatalf("Create(disabled template) error = %v, want ErrTemplateUnavailable", err)
	}
	if err := e.catalog.DeleteTemplate(ctx, tpl.ID); err != nil {
		t.Fatalf("DeleteTemplate() error = %v", err)
	}
	if err := e.catalog.DeleteTemplate(ctx, tpl.ID); !errors.Is(err, biz.ErrTemplateNotFound) {
		t.Fatalf("DeleteTemplate(again) error = %v, want ErrTemplateNotFound", err)
	}
	if err := e.catalog.DeleteCategory(ctx, tpl.CategoryID); err != nil {
		t.Fatalf("DeleteCategory() error = %v", err)
	}
}

// 提单 → 审批通过：工单状态由流程在同一事务里同步；可见性覆盖提单人 / 处理人 / 管理视图 / 路人。
func TestIntegration_TicketLifecycle(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	file, err := e.files.Upload(ctx, uInit, "a.png", pngData)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	tk, err := e.uc.Create(ctx, biz.CreateParams{
		TemplateID: tpl.ID, Title: "申请生产库只读", Priority: 3, Creator: uInit, FileIDs: []string{file.ID},
		Data: map[string]any{"reason": "排查问题", "amount": float64(3), "contact": uLead}, RequestID: "req-1",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !strings.HasPrefix(tk.Number, "REQ-") || !strings.HasSuffix(tk.Number, "-0001") || tk.Status != biz.StatusProcessing ||
		tk.InstanceID == "" || tk.CreatorDept != dTeam {
		t.Fatalf("Create() = %+v", tk)
	}
	// 幂等：相同的键返回首次创建的工单，不新增、不占号。
	again, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "重复", Priority: 1, Creator: uInit, Data: map[string]any{"reason": "x"}, RequestID: "req-1"})
	if err != nil || again.ID != tk.ID || e.count("SELECT count(*) FROM ticket_order") != 1 {
		t.Fatalf("Create(same request) = %+v, err %v", again, err)
	}
	// 模板此后怎么改都不影响已有工单的表单快照。
	changed := *tpl
	changed.Form = nil
	if err := e.catalog.UpdateTemplate(ctx, &changed); err != nil {
		t.Fatalf("UpdateTemplate() error = %v", err)
	}
	d, err := e.uc.Get(ctx, tk.ID, uInit)
	if err != nil {
		t.Fatalf("Get(creator) error = %v", err)
	}
	if len(d.Ticket.Form) != 3 || d.Ticket.Data["reason"] != "排查问题" || len(d.Files) != 1 ||
		d.Labels[uInit] != "提单人" || d.Labels[uLead] != "审批人" {
		t.Fatalf("Get() = ticket %+v files %d labels %v", d.Ticket, len(d.Files), d.Labels)
	}
	for user, want := range map[string]bool{uLead: true, uSuper: true, uOther: false} {
		_, err := e.uc.Get(ctx, tk.ID, user)
		if got := err == nil; got != want || (!want && !errors.Is(err, biz.ErrTicketForbidden)) {
			t.Fatalf("Get(%s) error = %v, want visible=%v", user, err, want)
		}
		// 附件与流程进度的可见性跟随工单。
		_, ferr := e.files.Get(ctx, user, file.ID)
		_, werr := e.rt.Get(ctx, tk.InstanceID, user, false)
		if (ferr == nil) != want || (werr == nil) != want {
			t.Fatalf("user %s: file err %v, instance err %v, want visible=%v", user, ferr, werr, want)
		}
	}
	// 评论：能看工单的人才能评论 / 看评论。
	if _, _, err := e.uc.AddComment(ctx, tk.ID, uOther, "hi", ""); !errors.Is(err, biz.ErrTicketForbidden) {
		t.Fatalf("AddComment(stranger) error = %v, want ErrTicketForbidden", err)
	}
	if _, _, err := e.uc.AddComment(ctx, tk.ID, uLead, "  \x00 ", ""); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("AddComment(blank) error = %v, want ErrFormInvalid", err)
	}
	if c, name, err := e.uc.AddComment(ctx, tk.ID, uLead, " 请补充用途 ", ""); err != nil || c.Content != "请补充用途" || name != "审批人" {
		t.Fatalf("AddComment() = %+v, %q, err %v", c, name, err)
	}
	cs, names, err := e.uc.Comments(ctx, tk.ID, uInit)
	if err != nil || len(cs) != 1 || names[uLead] != "审批人" {
		t.Fatalf("Comments() = %d, %v, err %v", len(cs), names, err)
	}
	// 回复：只能回复本工单的顶层评论，回复不能再被回复。
	reply, _, err := e.uc.AddComment(ctx, tk.ID, uInit, "用于测试", cs[0].ID)
	if err != nil || reply.ParentID != cs[0].ID {
		t.Fatalf("AddComment(reply) = %+v, err %v", reply, err)
	}
	if _, _, err := e.uc.AddComment(ctx, tk.ID, uLead, "再回复", reply.ID); !errors.Is(err, biz.ErrCommentParentInvalid) {
		t.Fatalf("AddComment(reply to reply) error = %v, want ErrCommentParentInvalid", err)
	}
	if _, _, err := e.uc.AddComment(ctx, tk.ID, uLead, "回复不存在的评论", tk.ID); !errors.Is(err, biz.ErrCommentParentInvalid) {
		t.Fatalf("AddComment(unknown parent) error = %v, want ErrCommentParentInvalid", err)
	}
	if all, _, err := e.uc.Comments(ctx, tk.ID, uInit); err != nil || len(all) != 2 || all[1].ParentID != cs[0].ID {
		t.Fatalf("Comments() after reply = %+v, err %v", all, err)
	}
	// 列表：我的 / 全部 / 过滤。
	processing := biz.StatusProcessing
	if items, names, total, err := e.uc.List(ctx, biz.TicketFilter{Creator: uInit, Status: &processing, Keyword: "只读"}, 1, 10); err != nil ||
		total != 1 || items[0].ID != tk.ID || names[uInit] != "提单人" {
		t.Fatalf("List(mine) = %d items, total %d, err %v", len(items), total, err)
	}
	if _, _, total, err := e.uc.List(ctx, biz.TicketFilter{Creator: uOther}, 1, 10); err != nil || total != 0 {
		t.Fatalf("List(other) total = %d, err %v", total, err)
	}
	// 待办按业务类型过滤，审批通过后工单同步为已完成。
	todo, _, _, err := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	if err != nil || len(todo) != 1 || todo[0].BusinessID != tk.ID {
		t.Fatalf("ListTasks(todo) = %+v, err %v", todo, err)
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, "同意"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	d, err = e.uc.Get(ctx, tk.ID, uInit)
	if err != nil || d.Ticket.Status != biz.StatusCompleted || d.Ticket.EndTime.IsZero() {
		t.Fatalf("after approve: ticket %+v, err %v", d.Ticket, err)
	}
}

// 提单人撤回 → 工单同步为已撤回。
func TestIntegration_CancelSyncsStatus(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	tk, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := e.rt.Cancel(ctx, tk.InstanceID, uInit, "不需要了"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got := e.count("SELECT status FROM ticket_order WHERE id = $1", tk.ID); got != int(biz.StatusCancelled) {
		t.Fatalf("ticket status = %d, want cancelled", got)
	}
}

// 流程在发起的同一事务里即结束（节点被自动跳过）：工单直接落为已完成。
func TestIntegration_FlowEndsOnStart(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("skip_flow", skipGraph)
	tpl := e.template("自动通过", "skip_flow")
	// 提单人不属于任何部门 → 没有部门负责人 → 审批节点跳过。
	tk, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uOther, Data: map[string]any{"reason": "r"}})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if tk.Status != biz.StatusCompleted || tk.EndTime.IsZero() {
		t.Fatalf("Create() = %+v, want completed", tk)
	}
	var instance string
	if err := e.d.Pool.QueryRow(ctx, "SELECT instance_id::text FROM ticket_order WHERE id = $1 AND status = 2 AND end_time IS NOT NULL", tk.ID).Scan(&instance); err != nil || instance != tk.InstanceID {
		t.Fatalf("persisted ticket: instance %q, err %v", instance, err)
	}
}

// 任一步失败整体回滚：不留工单、不留流程、不占编号、附件仍是临时文件。
func TestIntegration_CreateRollsBack(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	file, err := e.files.Upload(ctx, uInit, "a.png", pngData)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	base := biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}}
	cases := map[string]struct {
		mut  func(*biz.CreateParams)
		want *errors.Error
	}{
		"NUL in form data":       {func(p *biz.CreateParams) { p.Data = map[string]any{"reason": "a\x00b"} }, biz.ErrFormInvalid},
		"blank title":            {func(p *biz.CreateParams) { p.Title = " \x00 " }, biz.ErrFormInvalid},
		"integer out of range":   {func(p *biz.CreateParams) { p.Data = map[string]any{"reason": "r", "amount": 1e19} }, biz.ErrFormInvalid},
		"missing required field": {func(p *biz.CreateParams) { p.Data = map[string]any{} }, biz.ErrFormInvalid},
		"unknown field":          {func(p *biz.CreateParams) { p.Data = map[string]any{"reason": "r", "x": 1} }, biz.ErrFormInvalid},
		"wrong type":             {func(p *biz.CreateParams) { p.Data = map[string]any{"reason": "r", "amount": "many"} }, biz.ErrFormInvalid},
		"unknown template":       {func(p *biz.CreateParams) { p.TemplateID = uOther }, biz.ErrTemplateNotFound},
		"someone else's file":    {func(p *biz.CreateParams) { p.Creator, p.FileIDs = uLead, []string{file.ID} }, biz.ErrFormInvalid},
		// 发起部门不是提单人所属部门：流程引擎拒绝，此时工单与附件绑定都已写入，须一并回滚。
		"foreign dept": {func(p *biz.CreateParams) { p.CreatorDept, p.FileIDs = uOther, []string{file.ID} }, nil},
	}
	for name, c := range cases {
		p := base
		c.mut(&p)
		_, err := e.uc.Create(ctx, p)
		if err == nil || (c.want != nil && !errors.Is(err, c.want)) {
			t.Fatalf("Create(%s) error = %v, want %v", name, err, c.want)
		}
	}
	if n := e.count("SELECT count(*) FROM ticket_order") + e.count("SELECT count(*) FROM workflow_instance") + e.count("SELECT count(*) FROM ticket_sequence"); n != 0 {
		t.Fatalf("leftover rows after failed creates = %d, want 0", n)
	}
	if n := e.count("SELECT count(*) FROM file_object WHERE id = $1 AND biz_id IS NULL", file.ID); n != 1 {
		t.Fatal("attachment must stay unbound after rollback")
	}
}

// 并发提单：编号连续不重复；同一幂等键并发只落一张。
func TestIntegration_ConcurrentCreate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			_, errs[i] = e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Create[%d] error = %v", i, err)
		}
	}
	if distinct, top := e.count("SELECT count(DISTINCT number) FROM ticket_order"), e.count("SELECT max(seq) FROM ticket_sequence"); distinct != n || top != n {
		t.Fatalf("numbers: %d distinct, sequence at %d, want %d", distinct, top, n)
	}
	for i := range n {
		wg.Go(func() {
			_, errs[i] = e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uLead, Data: map[string]any{"reason": "r"}, RequestID: "same"})
		})
	}
	wg.Wait()
	// 抢输的一方回读首张工单：所有请求都成功，且只落一张。
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent Create[%d] error = %v", i, err)
		}
	}
	if got := e.count("SELECT count(*) FROM ticket_order WHERE creator = $1", uLead); got != 1 {
		t.Fatalf("tickets for one request id = %d, want 1", got)
	}
}

// 评论限流：单人一分钟内（跨工单）至多 30 条，并发下也不会被冲破；过了一分钟恢复。
func TestIntegration_CommentRateLimit(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("权限申请", flowKey)
	var ids []string
	for range 2 {
		tk, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}})
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		ids = append(ids, tk.ID)
	}
	const n = 40
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { _, _, errs[i] = e.uc.AddComment(ctx, ids[i%2], uInit, "hi", "") })
	}
	wg.Wait()
	var ok int
	for i, err := range errs {
		switch {
		case err == nil:
			ok++
		case !errors.Is(err, biz.ErrCommentRateLimited):
			t.Fatalf("AddComment[%d] error = %v, want nil or ErrCommentRateLimited", i, err)
		}
	}
	if stored := e.count("SELECT count(*) FROM ticket_comment WHERE user_id = $1", uInit); ok != 30 || stored != 30 {
		t.Fatalf("accepted %d comments, stored %d; want 30", ok, stored)
	}
	// 别人不受影响；一分钟之前的不计入。
	if _, _, err := e.uc.AddComment(ctx, ids[0], uLead, "hi", ""); err != nil {
		t.Fatalf("AddComment(other user) error = %v", err)
	}
	if _, err := e.d.Pool.Exec(ctx, "UPDATE ticket_comment SET create_time = create_time - interval '61 seconds' WHERE user_id = $1", uInit); err != nil {
		t.Fatalf("age comments: %v", err)
	}
	if _, _, err := e.uc.AddComment(ctx, ids[0], uInit, "hi", ""); err != nil {
		t.Fatalf("AddComment(after a minute) error = %v", err)
	}
}

// permGraph 两级审批：第一级可改 amount、必须补 env、看不到 secret；第二级没有配置字段权限。
const permGraph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"first","type":"userTask","name":"初审","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]},
	 "formPermissions":{"amount":"editable","env":"required","secret":"hidden","reason":"readonly"}},
	{"id":"second","type":"userTask","name":"复审","assignee":{"rules":[{"kind":"user","userIds":["` + uOther + `"]}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[{"id":"e1","source":"start","target":"first"},{"id":"e2","source":"first","target":"second"},{"id":"e3","source":"second","target":"end"}],
	"settings":{}}`

// 字段权限：处理人只能改本环节允许的字段，必填没填不能同意，隐藏的字段不给看；改动同步到流程变量。
func TestIntegration_FormPermissions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("perm_flow", permGraph)
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "权限"})
	if err != nil {
		t.Fatal(err)
	}
	tpl, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: c.ID, Name: "字段权限", WorkflowKey: "perm_flow", NumberPrefix: "PERM", Status: biz.TemplateEnabled,
		Form: []biz.FormField{
			{Code: "reason", Name: "原因", Type: formschema.TypeString, Required: true},
			{Code: "amount", Name: "金额", Type: formschema.TypeInt},
			{Code: "env", Name: "环境", Type: formschema.TypeEnum, EnumValues: []string{"prod", "test"}},
			{Code: "secret", Name: "口令", Type: formschema.TypeString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tk, err := e.uc.Create(ctx, biz.CreateParams{
		TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit,
		Data: map[string]any{"reason": "r", "amount": float64(1), "secret": "hunter2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 处理人视角：带字段权限、可编辑，且看不到 hidden 的字段（定义与取值都不返回）。
	d, err := e.uc.Get(ctx, tk.ID, uLead)
	if err != nil || !d.CanEditForm || d.Permissions["env"] != "required" || len(d.Ticket.Form) != 3 {
		t.Fatalf("Get(handler) = perms %v canEdit %v fields %d, err %v", d.Permissions, d.CanEditForm, len(d.Ticket.Form), err)
	}
	if _, leaked := d.Ticket.Data["secret"]; leaked {
		t.Fatal("hidden field leaked to the handler")
	}
	// 流程实例详情里的变量同样不泄露隐藏字段（表单数据就是流程变量）。
	if in, err := e.rt.Get(ctx, tk.InstanceID, uLead, false); err != nil || in.State.Instance.Variables["reason"] != "r" {
		t.Fatalf("Get(instance as handler) error = %v", err)
	} else if _, leaked := in.State.Instance.Variables["secret"]; leaked {
		t.Fatal("hidden field leaked to the handler through workflow variables")
	}
	// 提单人与管理视图看到全部；提单人此刻不能改表单。
	for _, user := range []string{uInit, uSuper} {
		v, err := e.uc.Get(ctx, tk.ID, user)
		if err != nil || len(v.Ticket.Form) != 4 || v.Ticket.Data["secret"] != "hunter2" || v.CanEditForm {
			t.Fatalf("Get(%s) = fields %d canEdit %v, err %v", user, len(v.Ticket.Form), v.CanEditForm, err)
		}
	}
	if err := e.uc.UpdateForm(ctx, tk.ID, uInit, map[string]any{"amount": float64(9)}); !errors.Is(err, biz.ErrFormLocked) {
		t.Fatalf("UpdateForm(by creator) error = %v, want ErrFormLocked", err)
	}
	todo, _, _, _ := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	// 必填的 env 还没填：不能同意。
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, ""); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("Approve(required field empty) error = %v, want ErrFormInvalid", err)
	}
	for name, data := range map[string]map[string]any{
		"readonly field": {"reason": "x"}, "hidden field": {"secret": "x"}, "bad value": {"env": "moon"}, "wrong type": {"amount": "many"},
	} {
		if err := e.uc.UpdateForm(ctx, tk.ID, uLead, data); !errors.Is(err, biz.ErrFormInvalid) {
			t.Errorf("UpdateForm(%s) error = %v, want ErrFormInvalid", name, err)
		}
	}
	for name, data := range map[string]map[string]any{"NUL": {"env": "pr\x00od"}, "out of range": {"amount": 1e19}} {
		if err := e.uc.UpdateForm(ctx, tk.ID, uLead, data); !errors.Is(err, biz.ErrFormInvalid) {
			t.Errorf("UpdateForm(%s) error = %v, want ErrFormInvalid", name, err)
		}
	}
	if err := e.uc.UpdateForm(ctx, tk.ID, uLead, map[string]any{"amount": float64(500), "env": "prod"}); err != nil {
		t.Fatalf("UpdateForm() error = %v", err)
	}
	v, _ := e.uc.Get(ctx, tk.ID, uInit)
	in, err := e.rt.Get(ctx, tk.InstanceID, uInit, false)
	if err != nil || v.Ticket.Data["env"] != "prod" || v.Ticket.Data["secret"] != "hunter2" ||
		in.State.Instance.Variables["env"] != "prod" || in.State.Instance.Variables["amount"] != float64(500) {
		t.Fatalf("after UpdateForm: ticket %v, variables %v, err %v", v.Ticket.Data, in.State.Instance.Variables, err)
	}
	// 改表单留痕在流转记录里；不是处理人的人去「同意」拿到的是引擎的拒绝，不是业务侧的字段提示。
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'amend' AND operator = $2", tk.InstanceID, uLead); n != 1 {
		t.Fatalf("amend history entries = %d, want 1", n)
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uOther, ""); !errors.Is(err, bizworkflow.ErrTaskNotAssignee) {
		t.Fatalf("Approve(by a non-assignee) error = %v, want ErrTaskNotAssignee", err)
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, "ok"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	// 首次响应时间已记下；初审人此后仍看不到隐藏字段，复审人（该环节未配置）看得到。
	if n := e.count("SELECT count(*) FROM ticket_order WHERE id = $1 AND response_time IS NOT NULL", tk.ID); n != 1 {
		t.Fatal("response_time not recorded after the first handler action")
	}
	if d, _ := e.uc.Get(ctx, tk.ID, uLead); d.CanEditForm || len(d.Ticket.Form) != 3 {
		t.Fatalf("Get(former handler) = canEdit %v fields %d", d.CanEditForm, len(d.Ticket.Form))
	}
	if d, _ := e.uc.Get(ctx, tk.ID, uOther); len(d.Ticket.Form) != 4 || d.CanEditForm {
		t.Fatalf("Get(second handler) = fields %d canEdit %v", len(d.Ticket.Form), d.CanEditForm)
	}
	if err := e.uc.UpdateForm(ctx, tk.ID, uLead, map[string]any{"amount": float64(1)}); !errors.Is(err, biz.ErrFormLocked) {
		t.Fatalf("UpdateForm(after handling) error = %v, want ErrFormLocked", err)
	}
}

// 字段权限按模板区分：两张模板共用一个流程、字段码相同，同一个节点上对一张模板隐藏 secret、
// 要求补 env，对另一张模板没有专属配置（沿用通用配置：secret 可编辑）。
func TestIntegration_FormPermissionsByTemplate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	graph := func(byForm string) string {
		return `{"nodes":[
			{"id":"start","type":"start","name":"发起"},
			{"id":"first","type":"userTask","name":"审批","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]},
			 "formPermissions":{"secret":"editable"}` + byForm + `},
			{"id":"end","type":"end","name":"结束"}],
			"edges":[{"id":"e1","source":"start","target":"first"},{"id":"e2","source":"first","target":"end"}],
			"settings":{}}`
	}
	// 模板要先绑定一个已发布的流程，流程里的专属权限又要引用模板 ID：先发布一版不带专属配置的。
	d, err := e.defs.Create(ctx, &bizworkflow.Definition{Key: "perm_tpl_flow", Name: "按模板的字段权限"})
	if err != nil {
		t.Fatal(err)
	}
	rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(graph("")), d.Revision)
	if err != nil || len(issues) > 0 {
		t.Fatalf("SaveDraft(v1) = issues %+v, err %v", issues, err)
	}
	if d, err = e.defs.Publish(ctx, d.ID, rev); err != nil {
		t.Fatal(err)
	}
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "按模板"})
	if err != nil {
		t.Fatal(err)
	}
	form := []biz.FormField{
		{Code: "secret", Name: "口令", Type: formschema.TypeString},
		{Code: "env", Name: "环境", Type: formschema.TypeString},
	}
	create := func(name, prefix string) *biz.Template {
		tpl, err := e.catalog.CreateTemplate(ctx, &biz.Template{
			CategoryID: c.ID, Name: name, WorkflowKey: "perm_tpl_flow", NumberPrefix: prefix, Status: biz.TemplateEnabled, Form: form,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tpl
	}
	strict, plain := create("严格", "STR"), create("普通", "PLN")
	rev, issues, err = e.defs.SaveDraft(ctx, d.ID,
		[]byte(graph(`,"formPermissionsByForm":{"`+strict.ID+`":{"secret":"hidden","env":"required"}}`)), d.Revision)
	if err != nil || len(issues) > 0 {
		t.Fatalf("SaveDraft(v2) = issues %+v, err %v", issues, err)
	}
	if _, err = e.defs.Publish(ctx, d.ID, rev); err != nil {
		t.Fatal(err)
	}
	submit := func(tpl *biz.Template) *biz.Ticket {
		tk, err := e.uc.Create(ctx, biz.CreateParams{
			TemplateID: tpl.ID, Title: tpl.Name, Priority: 2, Creator: uInit, Data: map[string]any{"secret": "hunter2"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return tk
	}
	a, b := submit(strict), submit(plain)
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE id = $1 AND form_key = $2", a.InstanceID, strict.ID); n != 1 {
		t.Fatal("the instance does not carry the template id as its form key")
	}
	// 严格模板：处理人看不到 secret，env 必填，没填不能同意。
	da, err := e.uc.Get(ctx, a.ID, uLead)
	if err != nil || len(da.Ticket.Form) != 1 || da.Permissions["env"] != "required" {
		t.Fatalf("Get(strict, handler) = fields %d perms %v, err %v", len(da.Ticket.Form), da.Permissions, err)
	}
	if _, leaked := da.Ticket.Data["secret"]; leaked {
		t.Fatal("hidden field of the strict template leaked to the handler")
	}
	if err := e.uc.UpdateForm(ctx, a.ID, uLead, map[string]any{"secret": "x"}); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("UpdateForm(strict, hidden field) error = %v, want ErrFormInvalid", err)
	}
	todo, _, _, _ := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	taskOf := func(instanceID string) string {
		for _, it := range todo {
			if it.InstanceID == instanceID {
				return it.TaskID
			}
		}
		t.Fatalf("no pending task on instance %s", instanceID)
		return ""
	}
	if _, err := e.rt.Approve(ctx, taskOf(a.InstanceID), uLead, ""); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("Approve(strict, required field empty) error = %v, want ErrFormInvalid", err)
	}
	// 表单布局：没指定的模板按单列；工单保存的是提单时的布局，模板之后改成双列不影响已有工单。
	if strict.FormColumns != 1 || a.FormColumns != 1 {
		t.Fatalf("form columns = template %d ticket %d, want 1 / 1", strict.FormColumns, a.FormColumns)
	}
	strict.FormColumns = 2
	if err := e.catalog.UpdateTemplate(ctx, strict); err != nil {
		t.Fatal(err)
	}
	if again, _, err := e.catalog.GetTemplate(ctx, strict.ID, uSuper); err != nil || again.FormColumns != 2 {
		t.Fatalf("GetTemplate() after update = %+v, err %v", again, err)
	}
	if old, err := e.uc.Get(ctx, a.ID, uInit); err != nil || old.Ticket.FormColumns != 1 {
		t.Fatalf("existing ticket form columns = %d, err %v, want 1", old.Ticket.FormColumns, err)
	}
	if fresh := submit(strict); fresh.FormColumns != 2 {
		t.Fatalf("new ticket form columns = %d, want 2", fresh.FormColumns)
	} else if got, err := e.uc.Get(ctx, fresh.ID, uInit); err != nil || got.Ticket.FormColumns != 2 {
		t.Fatalf("Get(new ticket) form columns = %d, err %v, want 2", got.Ticket.FormColumns, err)
	}
	strict.FormColumns = 3
	if err := e.catalog.UpdateTemplate(ctx, strict); !errors.Is(err, biz.ErrTemplateInvalid) {
		t.Fatalf("UpdateTemplate(3 columns) error = %v, want ErrTemplateInvalid", err)
	}
	// 普通模板：同一个节点、同一个字段码，沿用通用配置——看得到也改得了 secret，env 不必填。
	db, err := e.uc.Get(ctx, b.ID, uLead)
	if err != nil || len(db.Ticket.Form) != 2 || db.Permissions["secret"] != "editable" || db.Permissions["env"] != "" {
		t.Fatalf("Get(plain, handler) = fields %d perms %v, err %v", len(db.Ticket.Form), db.Permissions, err)
	}
	if err := e.uc.UpdateForm(ctx, b.ID, uLead, map[string]any{"secret": "changed"}); err != nil {
		t.Fatalf("UpdateForm(plain) error = %v", err)
	}
	if _, err := e.rt.Approve(ctx, taskOf(b.InstanceID), uLead, ""); err != nil {
		t.Fatalf("Approve(plain) error = %v", err)
	}
}

// 字段的扩展配置：定义的校验与归一化、显示条件对必填与取值的影响、多选人员的名称回显。
func TestIntegration_FieldExtras(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("extras_flow", flowGraph)
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "扩展"})
	if err != nil {
		t.Fatal(err)
	}
	form := func() []biz.FormField {
		return []biz.FormField{
			{
				Code: "env", Name: "环境", Type: formschema.TypeEnum, EnumValues: []string{"prod", "test"}, Required: true,
				EnumLabels: map[string]string{"prod": "生产"}, Default: "test", Group: "基本信息",
			},
			{
				Code: "window", Name: "变更窗口", Type: formschema.TypeString, Required: true,
				ShowWhen: &biz.ShowWhen{Field: "env", Values: []string{"prod"}},
			},
			{Code: "owners", Name: "负责人", Type: formschema.TypeUser, Multiple: true},
			{Code: "mail", Name: "邮箱", Type: formschema.TypeString, Format: formschema.FormatEmail, Placeholder: "a@b.c"},
		}
	}
	tpl := func(mutate func(f []biz.FormField)) error {
		f := form()
		if mutate != nil {
			mutate(f)
		}
		_, err := e.catalog.CreateTemplate(ctx, &biz.Template{
			CategoryID: c.ID, Name: "扩展-" + time.Now().Format("150405.000000000"), WorkflowKey: "extras_flow", NumberPrefix: "EXT",
			Status: biz.TemplateEnabled, Form: f,
		})
		return err
	}
	for name, mutate := range map[string]func(f []biz.FormField){
		"label for unknown option": func(f []biz.FormField) { f[0].EnumLabels = map[string]string{"moon": "月球"} },
		"default not an option":    func(f []biz.FormField) { f[0].Default = "moon" },
		"default on a user field":  func(f []biz.FormField) { f[2].Default = uLead },
		"multiple on text":         func(f []biz.FormField) { f[1].Multiple = true },
		"depends on later field":   func(f []biz.FormField) { f[0].ShowWhen = &biz.ShowWhen{Field: "window", Values: []string{"x"}} },
		"depends on unknown value": func(f []biz.FormField) { f[1].ShowWhen.Values = []string{"moon"} },
		"format on enum":           func(f []biz.FormField) { f[0].Format = formschema.FormatEmail },
	} {
		if err := tpl(mutate); !errors.Is(err, biz.ErrTemplateInvalid) {
			t.Errorf("CreateTemplate(%s) error = %v, want ErrTemplateInvalid", name, err)
		}
	}
	created, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: c.ID, Name: "扩展", WorkflowKey: "extras_flow", NumberPrefix: "EXT", Status: biz.TemplateEnabled, Form: form(),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := e.catalog.GetTemplate(ctx, created.ID, uSuper)
	if err != nil || got.Form[0].EnumLabels["prod"] != "生产" || got.Form[0].Default != "test" || got.Form[0].Group != "基本信息" ||
		got.Form[1].ShowWhen == nil || !got.Form[2].Multiple || got.Form[3].Placeholder != "a@b.c" {
		t.Fatalf("GetTemplate() form = %+v, err %v", got.Form, err)
	}
	submit := func(data map[string]any) (*biz.Ticket, error) {
		return e.uc.Create(ctx, biz.CreateParams{TemplateID: created.ID, Title: "t", Priority: 2, Creator: uInit, Data: data})
	}
	// 显示条件满足：受控字段必填。
	if _, err := submit(map[string]any{"env": "prod"}); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("Create(prod without window) error = %v, want ErrFormInvalid", err)
	}
	if _, err := submit(map[string]any{"env": "test", "mail": "nope"}); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("Create(bad email) error = %v, want ErrFormInvalid", err)
	}
	// 显示条件不满足：不要求必填，带上来的值不保留；多选人员去重后落成列表。
	tk, err := submit(map[string]any{"env": "test", "window": "stale", "owners": []any{uLead, uOther, uLead}})
	if err != nil {
		t.Fatalf("Create(test) error = %v", err)
	}
	d, err := e.uc.Get(ctx, tk.ID, uInit)
	if err != nil {
		t.Fatal(err)
	}
	if _, kept := d.Ticket.Data["window"]; kept {
		t.Fatal("value of a hidden field was kept")
	}
	if owners, _ := d.Ticket.Data["owners"].([]any); len(owners) != 2 || d.Labels[uLead] == "" || d.Labels[uOther] == "" {
		t.Fatalf("owners = %v labels = %v, want two users with names", d.Ticket.Data["owners"], d.Labels)
	}
	if tk2, err := submit(map[string]any{"env": "prod", "window": "周六 22:00"}); err != nil {
		t.Fatalf("Create(prod with window) error = %v", err)
	} else if d2, _ := e.uc.Get(ctx, tk2.ID, uInit); d2.Ticket.Data["window"] != "周六 22:00" {
		t.Fatalf("window = %v", d2.Ticket.Data["window"])
	}
}

// SLA：按优先级算出时限；超时后巡检各通知一次（多实例并发不重复）；统计口径正确。
func TestIntegration_SLAAndStats(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "SLA"})
	if err != nil {
		t.Fatal(err)
	}
	spec := &biz.Template{
		CategoryID: c.ID, Name: "带 SLA", WorkflowKey: flowKey, NumberPrefix: "SLA", Status: biz.TemplateEnabled,
		Form:        []biz.FormField{{Code: "reason", Name: "原因", Type: formschema.TypeString}},
		SLA:         []biz.SLARule{{Priority: 3, Response: &biz.Duration{Value: 30, Unit: "minute"}, Resolve: &biz.Duration{Value: 4, Unit: "hour"}}},
		SLAEscalate: []string{uSuper}, CILink: biz.CILinkOptional,
	}
	for name, bad := range map[string]func(*biz.Template){
		"duplicate priority": func(t *biz.Template) {
			t.SLA = append(t.SLA, biz.SLARule{Priority: 3, Resolve: &biz.Duration{Value: 1, Unit: "day"}})
		},
		"empty rule": func(t *biz.Template) { t.SLA = []biz.SLARule{{Priority: 1}} },
		"bad unit": func(t *biz.Template) {
			t.SLA = []biz.SLARule{{Priority: 1, Resolve: &biz.Duration{Value: 1, Unit: "week"}}}
		},
		"bad link mode": func(t *biz.Template) { t.CILink = 7 },
	} {
		cp := *spec
		cp.SLA = append([]biz.SLARule(nil), spec.SLA...)
		bad(&cp)
		if _, err := e.catalog.CreateTemplate(ctx, &cp); !errors.Is(err, biz.ErrTemplateInvalid) {
			t.Errorf("CreateTemplate(%s) error = %v, want ErrTemplateInvalid", name, err)
		}
	}
	tpl, err := e.catalog.CreateTemplate(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	if got, labels, err := e.catalog.GetTemplate(ctx, tpl.ID, uSuper); err != nil || len(got.SLA) != 1 || labels[uSuper] != "主管" || got.CILink != biz.CILinkOptional {
		t.Fatalf("GetTemplate() = %+v labels %v err %v", got, labels, err)
	}
	// 提单人只拿到填表所需的内容：SLA 规则、升级对象、绑定的流程与编号前缀不下发。
	if got, labels, err := e.catalog.GetTemplate(ctx, tpl.ID, uInit); err != nil || len(got.Form) != 1 || got.CILink != biz.CILinkOptional ||
		got.SLA != nil || got.SLAEscalate != nil || got.WorkflowKey != "" || got.NumberPrefix != "" || len(labels) != 0 {
		t.Fatalf("GetTemplate(submitter) = %+v labels %v err %v", got, labels, err)
	}
	create := func(priority int32, cis ...string) *biz.Ticket {
		t.Helper()
		tk, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: priority, Creator: uInit, Data: map[string]any{"reason": "r"}, ResourceIDs: cis})
		if err != nil {
			t.Fatalf("Create(priority %d) error = %v", priority, err)
		}
		return tk
	}
	// 关联资源：只能关联自己看得到的；详情带回、可按资源筛选。
	if _, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"}, ResourceIDs: []string{ciOK1, ciHidden}}); !errors.Is(err, biz.ErrFormInvalid) {
		t.Fatalf("Create(inaccessible resource) error = %v, want ErrFormInvalid", err)
	}
	plain := create(2, ciOK1, ciOK2) // 普通优先级没有规则：不设时限
	// 资源解析（走连接池的另一条连接）必须发生在提单事务之外：否则并发提单会把连接池互相等死。
	if resolvedInTx.Load() {
		t.Fatal("linked resources were resolved inside the create transaction")
	}
	if !plain.ResponseDue.IsZero() || !plain.ResolveDue.IsZero() {
		t.Fatalf("no rule for this priority, but dues = %v / %v", plain.ResponseDue, plain.ResolveDue)
	}
	if d, err := e.uc.Get(ctx, plain.ID, uInit); err != nil || len(d.Resources) != 2 || d.Resources[0].ModelName != "数据库" {
		t.Fatalf("Get() resources = %+v, err %v", d.Resources, err)
	}
	if _, _, total, err := e.uc.List(ctx, biz.TicketFilter{ResourceID: ciOK2}, 1, 10); err != nil || total != 1 {
		t.Fatalf("List(by resource) total = %d, err %v", total, err)
	}
	late, fast := create(3), create(3)
	if d := late.ResponseDue.Sub(late.CreateTime); d != 30*time.Minute || late.ResolveDue.Sub(late.CreateTime) != 4*time.Hour {
		t.Fatalf("dues: response +%v, resolve +%v", d, late.ResolveDue.Sub(late.CreateTime))
	}
	// fast 立刻被处理并完成；late 的两个时限都已过。
	todo, _, _, _ := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	for _, it := range todo {
		if it.BusinessID == fast.ID {
			if _, err := e.rt.Approve(ctx, it.TaskID, uLead, "ok"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := e.d.Pool.Exec(ctx,
		"UPDATE ticket_order SET response_due = now() - interval '2 hour', resolve_due = now() - interval '1 hour' WHERE id = $1", late.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, total, err := e.uc.List(ctx, biz.TicketFilter{Overdue: true}, 1, 10); err != nil || total != 1 {
		t.Fatalf("List(overdue) total = %d, err %v", total, err)
	}
	if _, _, total, err := e.uc.List(ctx, biz.TicketFilter{Priority: new(int32(3))}, 1, 10); err != nil || total != 2 {
		t.Fatalf("List(priority) total = %d, err %v", total, err)
	}
	// 并发巡检：响应超时、解决超时各通知一次，收件人 = 当前处理人 + 升级对象。
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				n, err := e.uc.NotifySLA(ctx)
				if err != nil {
					t.Errorf("NotifySLA() error = %v", err)
				}
				if n == 0 || err != nil {
					return
				}
			}
		})
	}
	wg.Wait()
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type = $1", biz.EventSLABreached); n != 2 {
		t.Fatalf("SLA notifications = %d, want 2", n)
	}
	if n := e.count("SELECT sla_notified FROM ticket_order WHERE id = $1", late.ID); n != 3 {
		t.Fatalf("sla_notified = %d, want 3", n)
	}
	// 评价：只有提单人、工单结束后、一次。
	if err := e.uc.Rate(ctx, late.ID, uInit, 5, ""); !errors.Is(err, biz.ErrRatingUnavailable) {
		t.Fatalf("Rate(unfinished) error = %v, want ErrRatingUnavailable", err)
	}
	if err := e.uc.Rate(ctx, fast.ID, uLead, 5, ""); !errors.Is(err, biz.ErrRatingUnavailable) {
		t.Fatalf("Rate(not creator) error = %v, want ErrRatingUnavailable", err)
	}
	if d, _ := e.uc.Get(ctx, fast.ID, uInit); !d.CanRate {
		t.Fatal("creator should be able to rate a finished ticket")
	}
	if err := e.uc.Rate(ctx, fast.ID, uInit, 4, " 很快 "); err != nil {
		t.Fatalf("Rate() error = %v", err)
	}
	if err := e.uc.Rate(ctx, fast.ID, uInit, 1, ""); !errors.Is(err, biz.ErrRatingUnavailable) {
		t.Fatalf("Rate(twice) error = %v, want ErrRatingUnavailable", err)
	}
	st, err := e.uc.Stats(ctx, 7, "")
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	// 指定时区：窗口内仍是这三张工单，按该时区分日（三张都是刚提交的，落在同一天或跨零点的相邻两天）。
	if zoned, err := e.uc.Stats(ctx, 7, "Asia/Shanghai"); err != nil || zoned.Total != 3 || zoned.Timezone != "Asia/Shanghai" ||
		len(zoned.Trend) == 0 || len(zoned.Trend) > 2 {
		t.Fatalf("Stats(zone) = %+v, err %v", zoned, err)
	}
	if st.Total != 3 || st.ByStatus[biz.StatusCompleted] != 1 || st.ByStatus[biz.StatusProcessing] != 2 || st.ByPriority[3] != 2 ||
		st.ResponseTotal != 2 || st.ResponseMet != 1 || st.ResolveTotal != 2 || st.ResolveMet != 1 ||
		st.Rated != 1 || st.AvgRating != 4 || len(st.ByTemplate) != 1 || st.ByTemplate[0].Count != 3 ||
		len(st.Trend) != 1 || st.Trend[0].Created != 3 || st.Trend[0].Closed != 1 {
		t.Fatalf("Stats() = %+v", st)
	}
}

// 超时自动同意与人工同意同一道校验：本环节必填的字段还空着时，系统不替人同意，只提醒。
func TestIntegration_TimeoutApproveRespectsRequiredFields(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("auto_flow", `{"nodes":[
		{"id":"start","type":"start"},
		{"id":"first","type":"userTask","name":"初审","assignee":{"rules":[{"kind":"user","userIds":["`+uLead+`"]}]},
		 "formPermissions":{"env":"required"},"timeout":{"value":1,"unit":"hour","action":"approve"}},
		{"id":"end","type":"end"}],
		"edges":[{"id":"e1","source":"start","target":"first"},{"id":"e2","source":"first","target":"end"}],"settings":{}}`)
	c, err := e.catalog.CreateCategory(ctx, &biz.Category{Name: "超时"})
	if err != nil {
		t.Fatal(err)
	}
	tpl, err := e.catalog.CreateTemplate(ctx, &biz.Template{
		CategoryID: c.ID, Name: "超时自动同意", WorkflowKey: "auto_flow", NumberPrefix: "AUTO", Status: biz.TemplateEnabled,
		Form: []biz.FormField{{Code: "env", Name: "环境", Type: formschema.TypeString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	fire := func(tk *biz.Ticket) {
		t.Helper()
		if _, err := e.d.Pool.Exec(ctx, "UPDATE workflow_job SET due_time = now() - interval '1 second' WHERE instance_id = $1 AND status = 1", tk.InstanceID); err != nil {
			t.Fatal(err)
		}
		if _, err := e.jobs.RunDue(ctx); err != nil {
			t.Fatalf("RunDue() error = %v", err)
		}
	}
	empty, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	fire(empty)
	if got := e.count("SELECT status FROM ticket_order WHERE id = $1", empty.ID); got != int(biz.StatusProcessing) {
		t.Fatalf("ticket with an empty required field was auto-approved: status %d", got)
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'timeout' AND detail->>'blocked' = 'true'", empty.InstanceID); n != 1 {
		t.Fatalf("blocked-timeout history entries = %d, want 1", n)
	}
	filled, err := e.uc.Create(ctx, biz.CreateParams{TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"env": "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	fire(filled)
	if got := e.count("SELECT status FROM ticket_order WHERE id = $1", filled.ID); got != int(biz.StatusCompleted) {
		t.Fatalf("ticket with the required field filled should be auto-approved: status %d", got)
	}
}
