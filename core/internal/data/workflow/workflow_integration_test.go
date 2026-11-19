//go:build integration

package data

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 固定的测试主体。
const (
	uInit  = "018f3e1a-0a00-7000-8000-000000000001"
	uLead  = "018f3e1a-0a00-7000-8000-000000000002"
	uBoss  = "018f3e1a-0a00-7000-8000-000000000003"
	uOps1  = "018f3e1a-0a00-7000-8000-000000000004"
	uOps2  = "018f3e1a-0a00-7000-8000-000000000005"
	uGone  = "018f3e1a-0a00-7000-8000-000000000006"
	dRoot  = "018f3e1a-0d00-7000-8000-000000000001"
	dTeam  = "018f3e1a-0d00-7000-8000-000000000002"
	roleID = "018f3e1a-0e00-7000-8000-000000000001"
)

// usersAdapter 把 system 域组织目录适配为引擎的用户目录（与组合根同一转换）。
type usersAdapter struct{ dir *bizsystem.DirectoryUsecase }

func (a usersAdapter) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	return a.dir.UserNames(ctx, ids)
}

func (a usersAdapter) Candidates(ctx context.Context, keyword string, limit int64) ([]biz.UserInfo, error) {
	users, err := a.dir.CandidateUsers(ctx, keyword, limit)
	out := make([]biz.UserInfo, 0, len(users))
	for _, u := range users {
		out = append(out, biz.UserInfo{ID: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return out, err
}

func (a usersAdapter) PrimaryDept(ctx context.Context, userID string) (string, []string, error) {
	return a.dir.PrimaryDept(ctx, userID)
}

// listenerStub 记录实例结束回调；fail 非空时返回错误（验证整体回滚）。
type listenerStub struct {
	mu       sync.Mutex
	finished []string
	fail     error
}

func (l *listenerStub) OnInstanceFinished(_ context.Context, _ txrun.Tx, in *biz.Instance) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil {
		return l.fail
	}
	l.finished = append(l.finished, fmt.Sprintf("%s:%d", in.BusinessID, in.Status))
	return nil
}

type env struct {
	t        *testing.T
	d        *basedata.Data
	defs     *biz.DefinitionUsecase
	rt       *biz.RuntimeUsecase
	listener *listenerStub
	cfg      *configRepo
	jobs     *jobRepo
	dir      *bizsystem.DirectoryUsecase
}

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
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE workflow_job, workflow_delegation, workflow_webhook, workflow_client, workflow_history, workflow_task_assignee, workflow_task, workflow_token,
		workflow_instance, workflow_definition_version, workflow_definition, notify_event, notify_dedup,
		system_permission, system_grant, system_refresh_token, system_user_role, system_role,
		system_dept_leader, system_user_dept, system_dept, "system_user"`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	seed := []string{
		`INSERT INTO "system_user" (id, username, password, nickname, status) VALUES
			('` + uInit + `','init','x','发起人',1), ('` + uLead + `','lead','x','组长',1), ('` + uBoss + `','boss','x','总监',1),
			('` + uOps1 + `','ops1','x','运维一',1), ('` + uOps2 + `','ops2','x','运维二',1), ('` + uGone + `','gone','x','离职者',4)`,
		`INSERT INTO system_dept (id, parent_id, path, name) VALUES
			('` + dRoot + `', NULL, '/` + dRoot + `/', '总部'), ('` + dTeam + `', '` + dRoot + `', '/` + dRoot + `/` + dTeam + `/', '研发组')`,
		`INSERT INTO system_dept_leader (id, dept_id, user_id, sort) VALUES
			(gen_random_uuid(), '` + dTeam + `', '` + uLead + `', 1), (gen_random_uuid(), '` + dTeam + `', '` + uGone + `', 2),
			(gen_random_uuid(), '` + dRoot + `', '` + uBoss + `', 1)`,
		`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary) VALUES (gen_random_uuid(), '` + uInit + `', '` + dTeam + `', true)`,
		`INSERT INTO system_role (id, name, code, status) VALUES ('` + roleID + `', '运维', 'ops', 1)`,
		`INSERT INTO system_user_role (id, user_id, role_id) VALUES
			(gen_random_uuid(), '` + uOps1 + `', '` + roleID + `'), (gen_random_uuid(), '` + uOps2 + `', '` + roleID + `'), (gen_random_uuid(), '` + uGone + `', '` + roleID + `')`,
	}
	for _, sql := range seed {
		if _, err := d.Pool.Exec(ctx, sql); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	defRepo, instRepo := NewDefinitionRepo(d), NewInstanceRepo(d)
	dir := bizsystem.NewDirectoryUsecase(datasystem.NewDirectoryRepo(d), datasystem.NewUserRepo(d), datasystem.NewDeptRepo(d))
	cfg, err := NewConfigRepo(d, &conf.Notify{Master: &kitconf.Master{Key: "integration-test-master-key"}})
	if err != nil {
		t.Fatalf("NewConfigRepo() error = %v", err)
	}
	defs := biz.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, cfg, nil)
	listener := &listenerStub{}
	txs := basedata.NewTxManager(d)
	rt := biz.NewRuntimeUsecase(defs, instRepo, instRepo, instRepo, dir, usersAdapter{dir}, cfg, biz.NewCalendarUsecase(cfg), txs,
		biznotify.NewNotificationUsecase(datanotify.NewEventRepo(d), nil), biz.Listeners{"ticket": listener}, nil, nil)
	return &env{t: t, d: d, defs: defs, rt: rt, listener: listener, cfg: cfg, jobs: NewJobRepo(d), dir: dir}
}

// publish 创建并发布一个流程定义，返回其标识。
func (e *env) publish(key, graph string) *biz.Definition {
	e.t.Helper()
	ctx := context.Background()
	d, err := e.defs.Create(ctx, &biz.Definition{Key: key, Name: "流程-" + key})
	if err != nil {
		e.t.Fatalf("Create(%s) error = %v", key, err)
	}
	rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(graph), d.Revision)
	if err != nil || len(issues) > 0 {
		e.t.Fatalf("SaveDraft(%s) = issues %+v, err %v", key, issues, err)
	}
	if d, err = e.defs.Publish(ctx, d.ID, rev); err != nil {
		e.t.Fatalf("Publish(%s) error = %v", key, err)
	}
	return d
}

func (e *env) todo(userID string) []*biz.TaskItem {
	e.t.Helper()
	items, _, _, err := e.rt.ListTasks(context.Background(), userID, biz.TaskViewTodo, "", "", 1, 50)
	if err != nil {
		e.t.Fatalf("ListTasks(todo) error = %v", err)
	}
	return items
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.d.Pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count: %v", err)
	}
	return n
}

// 发起人主管审批 → 金额分支（大额走总监）→ 运维角色会签 → 抄送发起人。
const approvalGraph = `{"nodes":[
	{"id":"start","type":"start","name":"发起"},
	{"id":"leader","type":"userTask","name":"主管审批","assignee":{"rules":[{"kind":"deptLeader","level":1}]}},
	{"id":"gw","type":"exclusiveGateway"},
	{"id":"boss","type":"userTask","name":"总监审批","assignee":{"rules":[{"kind":"deptLeader","level":2}]}},
	{"id":"merge","type":"exclusiveGateway"},
	{"id":"ops","type":"userTask","name":"运维处理","assignee":{"mode":"all","rules":[{"kind":"role","roleCodes":["ops"]}]},"onReject":{"to":"leader"}},
	{"id":"cc","type":"cc","name":"抄送","assignee":{"rules":[{"kind":"initiator"}]}},
	{"id":"end","type":"end","name":"结束"}],
	"edges":[
	{"id":"e1","source":"start","target":"leader"},{"id":"e2","source":"leader","target":"gw"},
	{"id":"e3","source":"gw","target":"boss","condition":"form.amount >= 10000","priority":1},
	{"id":"e4","source":"gw","target":"merge","isDefault":true},
	{"id":"e5","source":"boss","target":"merge"},{"id":"e6","source":"merge","target":"ops"},
	{"id":"e7","source":"ops","target":"cc"},{"id":"e8","source":"cc","target":"end"}],
	"settings":{}}`

func TestIntegration_DefinitionLifecycle(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d, err := e.defs.Create(ctx, &biz.Definition{Key: "leave", Name: "请假"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := e.defs.Create(ctx, &biz.Definition{Key: "leave", Name: "重复"}); !errors.Is(err, biz.ErrDefinitionKeyExists) {
		t.Fatalf("Create(dup key) error = %v, want ErrDefinitionKeyExists", err)
	}
	// 未发布不能发起。
	if _, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "leave", Title: "t", Initiator: uInit}); !errors.Is(err, biz.ErrDefinitionUnavailable) {
		t.Fatalf("Start(unpublished) error = %v, want ErrDefinitionUnavailable", err)
	}
	// 草稿可保存未完成状态并带回问题；修订号做乐观锁。
	broken := `{"nodes":[{"id":"start","type":"start"},{"id":"a","type":"userTask"},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"a"},{"id":"2","source":"a","target":"end"}]}`
	rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(broken), d.Revision)
	if err != nil || len(issues) == 0 || rev != d.Revision+1 {
		t.Fatalf("SaveDraft(incomplete) = rev %d issues %+v err %v", rev, issues, err)
	}
	if _, _, err := e.defs.SaveDraft(ctx, d.ID, []byte(broken), d.Revision); !errors.Is(err, biz.ErrDefinitionConflict) {
		t.Fatalf("SaveDraft(stale revision) error = %v, want ErrDefinitionConflict", err)
	}
	if _, err := e.defs.Publish(ctx, d.ID, rev); !errors.Is(err, biz.ErrDefinitionInvalid) {
		t.Fatalf("Publish(invalid draft) error = %v, want ErrDefinitionInvalid", err)
	}
	rev, issues, err = e.defs.SaveDraft(ctx, d.ID, []byte(approvalGraph), rev)
	if err != nil || len(issues) != 0 {
		t.Fatalf("SaveDraft(valid) = issues %+v err %v", issues, err)
	}
	if _, err := e.defs.Publish(ctx, d.ID, rev-1); !errors.Is(err, biz.ErrDefinitionConflict) {
		t.Fatalf("Publish(stale revision) error = %v, want ErrDefinitionConflict", err)
	}
	// 并发发布：版本号连续不重复。
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { _, errs[i] = e.defs.Publish(ctx, d.ID, rev) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Publish[%d] error = %v", i, err)
		}
	}
	versions, err := e.defs.ListVersions(ctx, d.ID)
	if err != nil || len(versions) != 4 || versions[0].Version != 4 || versions[3].Version != 1 {
		t.Fatalf("ListVersions() = %d versions, err %v", len(versions), err)
	}
	if v, err := e.defs.GetVersion(ctx, d.ID, 2); err != nil || len(v.Graph) == 0 {
		t.Fatalf("GetVersion(2) error = %v", err)
	}
	// 停用后不能发起；有进行中的实例不能删除。
	off, on := biz.DefinitionDisabled, biz.DefinitionEnabled
	if _, err := e.defs.Update(ctx, d.ID, biz.DefinitionUpdate{Status: &off}); err != nil {
		t.Fatalf("Update(disable) error = %v", err)
	}
	if _, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "leave", Title: "t", Initiator: uInit}); !errors.Is(err, biz.ErrDefinitionUnavailable) {
		t.Fatalf("Start(disabled) error = %v, want ErrDefinitionUnavailable", err)
	}
	if _, err := e.defs.Update(ctx, d.ID, biz.DefinitionUpdate{Status: &on}); err != nil {
		t.Fatalf("Update(enable) error = %v", err)
	}
	in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "leave", Title: "t", Initiator: uInit})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := e.defs.Delete(ctx, d.ID); !errors.Is(err, biz.ErrDefinitionInUse) {
		t.Fatalf("Delete(with running instance) error = %v, want ErrDefinitionInUse", err)
	}
	if err := e.rt.Cancel(ctx, in.ID, uInit, ""); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := e.defs.Delete(ctx, d.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, _, err := e.defs.Get(ctx, d.ID); !errors.Is(err, biz.ErrDefinitionNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrDefinitionNotFound", err)
	}
}

func TestIntegration_ApprovalFlow(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	def := e.publish("purchase", approvalGraph)

	in, err := e.rt.Start(ctx, biz.StartParams{
		DefinitionKey: "purchase", BusinessType: "ticket", BusinessID: "T-1", Title: "采购服务器",
		Initiator: uInit, Variables: map[string]any{"amount": 20000},
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if in.InitiatorDept != dTeam || in.Version != 1 || in.Status != biz.InstanceRunning {
		t.Fatalf("instance = %+v", in)
	}
	// 同一业务对象只能发起一次。
	if _, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "purchase", BusinessType: "ticket", BusinessID: "T-1", Title: "dup", Initiator: uInit}); !errors.Is(err, biz.ErrBusinessDuplicate) {
		t.Fatalf("Start(dup business) error = %v, want ErrBusinessDuplicate", err)
	}
	// 发起部门须为发起人所属部门。
	if _, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "purchase", Title: "x", Initiator: uInit, InitiatorDept: dRoot}); !errors.Is(err, biz.ErrVariablesInvalid) {
		t.Fatalf("Start(foreign dept) error = %v, want ErrVariablesInvalid", err)
	}

	// 主管审批：本部门负责人里离职者被过滤，只剩组长。
	todo := e.todo(uLead)
	if len(todo) != 1 || todo[0].NodeName != "主管审批" || todo[0].BusinessID != "T-1" {
		t.Fatalf("lead todo = %+v", todo)
	}
	if len(e.todo(uGone)) != 0 {
		t.Fatal("inactive leader must not receive the task")
	}
	// 非处理人不能处理；参与者之外不能查看。
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uBoss, ""); !errors.Is(err, biz.ErrTaskNotAssignee) {
		t.Fatalf("Approve(by stranger) error = %v, want ErrTaskNotAssignee", err)
	}
	if _, err := e.rt.Get(ctx, in.ID, uOps1, false); !errors.Is(err, biz.ErrInstanceForbidden) {
		t.Fatalf("Get(by non-participant) error = %v, want ErrInstanceForbidden", err)
	}
	if _, err := e.rt.Get(ctx, in.ID, uOps1, true); err != nil {
		t.Fatalf("Get(view all) error = %v", err)
	}
	if status, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, "同意"); err != nil || status != biz.InstanceRunning {
		t.Fatalf("Approve(lead) = %d, %v", status, err)
	}
	// 重复提交：任务已处理。
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, ""); !errors.Is(err, biz.ErrTaskNotPending) {
		t.Fatalf("Approve(again) error = %v, want ErrTaskNotPending", err)
	}
	// 大额 → 总监（上一级部门负责人）。
	bossTodo := e.todo(uBoss)
	if len(bossTodo) != 1 || bossTodo[0].NodeName != "总监审批" {
		t.Fatalf("boss todo = %+v", bossTodo)
	}
	if _, err := e.rt.Approve(ctx, bossTodo[0].TaskID, uBoss, ""); err != nil {
		t.Fatalf("Approve(boss) error = %v", err)
	}
	// 运维会签：两名可用运维都要同意；一人拒绝则按节点配置退回主管。
	ops1 := e.todo(uOps1)
	if len(ops1) != 1 || len(e.todo(uOps2)) != 1 {
		t.Fatalf("ops todo = %+v", ops1)
	}
	targets, err := e.rt.ReturnTargets(ctx, ops1[0].TaskID, uOps1)
	if err != nil || len(targets) != 2 || targets[0].ID != "boss" || targets[1].ID != "leader" {
		t.Fatalf("ReturnTargets() = %+v, %v", targets, err)
	}
	if status, err := e.rt.Reject(ctx, ops1[0].TaskID, uOps1, "资料不全"); err != nil || status != biz.InstanceRunning {
		t.Fatalf("Reject(ops1) = %d, %v", status, err)
	}
	if len(e.todo(uOps2)) != 0 {
		t.Fatal("rejecting a countersign task must cancel the other assignees")
	}
	// 回到主管 → 再走一遍 → 运维两人同意 → 抄送 → 结束。
	for _, step := range []string{uLead, uBoss} {
		items := e.todo(step)
		if len(items) != 1 {
			t.Fatalf("todo of %s = %+v", step, items)
		}
		if _, err := e.rt.Approve(ctx, items[0].TaskID, step, ""); err != nil {
			t.Fatalf("Approve(%s) error = %v", step, err)
		}
	}
	taskID := e.todo(uOps1)[0].TaskID
	if status, err := e.rt.Approve(ctx, taskID, uOps1, ""); err != nil || status != biz.InstanceRunning {
		t.Fatalf("Approve(ops1) = %d, %v", status, err)
	}
	if status, err := e.rt.Approve(ctx, taskID, uOps2, ""); err != nil || status != biz.InstanceCompleted {
		t.Fatalf("Approve(ops2) = %d, %v; want completed", status, err)
	}

	// 终态：监听器在同事务内收到回调；详情可读；历史完整且只追加。
	if !slices.Equal(e.listener.finished, []string{fmt.Sprintf("T-1:%d", biz.InstanceCompleted)}) {
		t.Fatalf("listener = %v", e.listener.finished)
	}
	detail, err := e.rt.Get(ctx, in.ID, uInit, false)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	st := detail.State
	if st.Instance.Status != biz.InstanceCompleted || st.Instance.EndTime.IsZero() || detail.Names[uLead] != "组长" {
		t.Fatalf("detail instance = %+v names = %v", st.Instance, detail.Names)
	}
	var actions []string
	for _, h := range st.Histories {
		actions = append(actions, h.Action)
	}
	want := "start,approve,approve,reject,approve,approve,approve,approve,cc,complete"
	if got := strings.Join(actions, ","); got != want {
		t.Fatalf("history actions = %s\nwant            %s", got, want)
	}
	for _, tk := range st.Tokens {
		if tk.Status != biz.TokenDone {
			t.Fatalf("finished instance has live token %+v", tk)
		}
	}
	// 已办 / 抄送 / 我发起的。
	if done, _, total, err := e.rt.ListTasks(ctx, uLead, biz.TaskViewDone, "采购", "ticket", 1, 10); err != nil || total != 2 || done[0].Status != biz.AssigneeApproved {
		t.Fatalf("lead done = total %d err %v", total, err)
	}
	if _, _, total, err := e.rt.ListTasks(ctx, uInit, biz.TaskViewCC, "", "other", 1, 10); err != nil || total != 0 {
		t.Fatalf("cc filtered by another business type = %d err %v, want 0", total, err)
	}
	if _, _, total, err := e.rt.ListTasks(ctx, uInit, biz.TaskViewCC, "", "", 1, 10); err != nil || total != 1 {
		t.Fatalf("initiator cc total = %d err %v", total, err)
	}
	mine, names, total, err := e.rt.ListInstances(ctx, biz.InstanceFilter{Initiator: uInit, DefinitionID: def.ID}, 1, 10)
	if err != nil || total != 1 || mine[0].Title != "采购服务器" || names[uInit] != "发起人" {
		t.Fatalf("my instances = total %d err %v", total, err)
	}
	// 通知事件与业务写同事务落库：任务指派（主管×2、总监×2、运维×2 含并发会签一次指派两人）+ 抄送 + 结束。
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type = $1", biz.EventTaskAssigned); n != 6 {
		t.Fatalf("task.assigned events = %d, want 6", n)
	}
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type IN ($1, $2)", biz.EventTaskCC, biz.EventInstanceFinished); n != 2 {
		t.Fatalf("cc + finished events = %d, want 2", n)
	}
	// 结束后不能再操作。
	if err := e.rt.Terminate(ctx, in.ID, uBoss, "late"); !errors.Is(err, biz.ErrInstanceNotRunning) {
		t.Fatalf("Terminate(finished) error = %v, want ErrInstanceNotRunning", err)
	}
}

// 小额分支、实例绑定发起时的版本、转办 / 改派 / 终止。
func TestIntegration_VersionPinningAndAdminActions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	def := e.publish("purchase", approvalGraph)
	in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "purchase", Title: "小额", Initiator: uInit, Variables: map[string]any{"amount": 10}})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	// 发布一个只有单节点的新版本：在途实例仍按旧版本走。
	single := `{"nodes":[{"id":"start","type":"start"},{"id":"only","type":"userTask","name":"唯一","assignee":{"rules":[{"kind":"user","userIds":["` + uBoss + `"]}]}},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"only"},{"id":"2","source":"only","target":"end"}]}`
	cur, _, err := e.defs.Get(ctx, def.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	rev, _, err := e.defs.SaveDraft(ctx, def.ID, []byte(single), cur.Revision)
	if err != nil {
		t.Fatalf("SaveDraft() error = %v", err)
	}
	if _, err := e.defs.Publish(ctx, def.ID, rev); err != nil {
		t.Fatalf("Publish(v2) error = %v", err)
	}
	lead := e.todo(uLead)
	// 转办：组长 → 运维一。
	if _, err := e.rt.Transfer(ctx, lead[0].TaskID, uLead, uGone, ""); !errors.Is(err, biz.ErrTaskTargetInvalid) {
		t.Fatalf("Transfer(to inactive) error = %v, want ErrTaskTargetInvalid", err)
	}
	if _, err := e.rt.Transfer(ctx, lead[0].TaskID, uLead, strings.ToUpper(uOps1), "出差"); err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	if len(e.todo(uLead)) != 0 || len(e.todo(uOps1)) != 1 {
		t.Fatal("transfer must move the pending seat")
	}
	if _, err := e.rt.Approve(ctx, lead[0].TaskID, uOps1, ""); err != nil {
		t.Fatalf("Approve(transferee) error = %v", err)
	}
	// 小额：跳过总监，直接到运维（旧版本的路径）。
	if len(e.todo(uBoss)) != 0 {
		t.Fatal("small amount must skip the director; in-flight instance must stay on v1")
	}
	ops := e.todo(uOps2)
	if len(ops) != 1 || ops[0].NodeName != "运维处理" {
		t.Fatalf("ops todo = %+v", ops)
	}
	// 改派：换成总监一人。
	if _, err := e.rt.Reassign(ctx, ops[0].TaskID, uBoss, []string{uBoss}, "运维休假"); err != nil {
		t.Fatalf("Reassign() error = %v", err)
	}
	if len(e.todo(uOps2)) != 0 || len(e.todo(uBoss)) != 1 {
		t.Fatal("reassign must replace pending assignees")
	}
	// 终止。
	if err := e.rt.Terminate(ctx, in.ID, uBoss, "预算取消"); err != nil {
		t.Fatalf("Terminate() error = %v", err)
	}
	if len(e.todo(uBoss)) != 0 {
		t.Fatal("terminated instance must leave no todo")
	}
	// 新发起的实例走新版本。
	in2, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "purchase", Title: "新版", Initiator: uInit})
	if err != nil || in2.Version != 2 {
		t.Fatalf("Start(v2) = %+v, %v", in2, err)
	}
	if items := e.todo(uBoss); len(items) != 1 || items[0].NodeName != "唯一" {
		t.Fatalf("v2 todo = %+v", items)
	}
}

// 幂等发起、同一业务对象结束后可再次发起、连接池很小时并发命令不会互相等死。
func TestIntegration_IdempotencyAndPoolUsage(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	single := `{"nodes":[{"id":"start","type":"start"},{"id":"only","type":"userTask","name":"唯一","assignee":{"rules":[{"kind":"role","roleCodes":["ops"]},{"kind":"deptLeader","level":1},{"kind":"formUser","field":"owner"}]}},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"only"},{"id":"2","source":"only","target":"end"}]}`
	e.publish("one", single)

	// 幂等键：重复提交返回同一个实例，不重复发通知。
	const reqID = "018f3e1a-0f00-7000-8000-000000000001"
	p := biz.StartParams{
		DefinitionKey: "one", Title: "幂等", Initiator: uInit, RequestID: reqID,
		// 表单取值不可信：非 UUID 的人员值被丢弃，而不是让查询报错回滚。
		Variables: map[string]any{"owner": []any{"not-a-uuid", uBoss}},
	}
	first, err := e.rt.Start(ctx, p)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	events := e.count("SELECT count(*) FROM notify_event")
	for range 3 {
		again, err := e.rt.Start(ctx, p)
		if err != nil || again.ID != first.ID {
			t.Fatalf("Start(retry) = %+v, %v; want the first instance %s", again, err, first.ID)
		}
	}
	if n := e.count("SELECT count(*) FROM workflow_instance"); n != 1 {
		t.Fatalf("instances after retries = %d, want 1", n)
	}
	if n := e.count("SELECT count(*) FROM notify_event"); n != events {
		t.Fatalf("notify events after retries = %d, want %d", n, events)
	}
	if got := e.todo(uBoss); len(got) != 1 {
		t.Fatalf("form user must be resolved despite a junk sibling value: %+v", got)
	}

	// 同一业务对象：进行中时不可重复发起，结束后可以再发起。
	biz1 := biz.StartParams{DefinitionKey: "one", BusinessType: "doc", BusinessID: "D-1", Title: "v1", Initiator: uInit}
	in1, err := e.rt.Start(ctx, biz1)
	if err != nil {
		t.Fatalf("Start(business) error = %v", err)
	}
	if _, err := e.rt.Start(ctx, biz1); !errors.Is(err, biz.ErrBusinessDuplicate) {
		t.Fatalf("Start(while running) error = %v, want ErrBusinessDuplicate", err)
	}
	if err := e.rt.Cancel(ctx, in1.ID, uInit, ""); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, err := e.rt.Start(ctx, biz1); err != nil {
		t.Fatalf("Start(after finished) error = %v", err)
	}

	// 连接池只有 3 条连接：24 条并发命令各自只占用事务那一条连接，全部能完成。
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	small, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn, MaxConns: 3}})
	if err != nil {
		t.Fatalf("NewData(small pool) error = %v", err)
	}
	defer cleanup()
	defRepo, instRepo := NewDefinitionRepo(small), NewInstanceRepo(small)
	dir := bizsystem.NewDirectoryUsecase(datasystem.NewDirectoryRepo(small), datasystem.NewUserRepo(small), datasystem.NewDeptRepo(small))
	cfg, err := NewConfigRepo(small, &conf.Notify{})
	if err != nil {
		t.Fatalf("NewConfigRepo(small pool) error = %v", err)
	}
	rt := biz.NewRuntimeUsecase(biz.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, cfg, nil), instRepo, instRepo, instRepo, dir,
		usersAdapter{dir}, cfg, biz.NewCalendarUsecase(cfg), basedata.NewTxManager(small),
		biznotify.NewNotificationUsecase(datanotify.NewEventRepo(small), nil), nil, nil, nil)
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, 24)
	for i := range errs {
		wg.Go(func() {
			_, errs[i] = rt.Start(cctx, biz.StartParams{DefinitionKey: "one", Title: fmt.Sprintf("p%d", i), Initiator: uInit})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Start[%d] on a 3-connection pool error = %v", i, err)
		}
	}
}

// 监听器失败 → 整条命令回滚：任务仍待处理，无通知事件残留。
func TestIntegration_ListenerFailureRollsBack(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	single := `{"nodes":[{"id":"start","type":"start"},{"id":"only","type":"userTask","name":"唯一","assignee":{"rules":[{"kind":"user","userIds":["` + uBoss + `"]}]}},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"only"},{"id":"2","source":"only","target":"end"}]}`
	e.publish("one", single)
	in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "one", BusinessType: "ticket", BusinessID: "T-9", Title: "t", Initiator: uInit})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	events := e.count("SELECT count(*) FROM notify_event")
	e.listener.fail = fmt.Errorf("business write failed")
	taskID := e.todo(uBoss)[0].TaskID
	if _, err := e.rt.Approve(ctx, taskID, uBoss, ""); err == nil {
		t.Fatal("Approve() must fail when the listener fails")
	}
	st, err := e.rt.Get(ctx, in.ID, uInit, false)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if st.State.Instance.Status != biz.InstanceRunning || st.MyTaskID != "" || len(e.todo(uBoss)) != 1 {
		t.Fatalf("state after rollback: status %d", st.State.Instance.Status)
	}
	if n := e.count("SELECT count(*) FROM notify_event"); n != events {
		t.Fatalf("notify events after rollback = %d, want %d", n, events)
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1", in.ID); n != 1 {
		t.Fatalf("history rows after rollback = %d, want 1 (start only)", n)
	}
	e.listener.fail = nil
	if status, err := e.rt.Approve(ctx, taskID, uBoss, ""); err != nil || status != biz.InstanceCompleted {
		t.Fatalf("Approve(retry) = %d, %v", status, err)
	}
}

// 多实例并发：同一任务被多人同时处理，恰一人成功（或签）；并行分支同时完成，汇聚不丢不重。
func TestIntegration_ConcurrentCommands(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	anyOf := `{"nodes":[{"id":"start","type":"start"},{"id":"a","type":"userTask","name":"或签","assignee":{"mode":"any","rules":[{"kind":"role","roleCodes":["ops"]},{"kind":"user","userIds":["` + uBoss + `","` + uLead + `"]}]}},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"a"},{"id":"2","source":"a","target":"end"}]}`
	e.publish("race", anyOf)
	for round := range 10 {
		in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "race", Title: fmt.Sprintf("r%d", round), Initiator: uInit})
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		st, err := e.rt.Get(ctx, in.ID, uInit, false)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		taskID := st.State.Tasks[0].ID
		users := []string{uOps1, uOps2, uBoss, uLead}
		var wg sync.WaitGroup
		results := make([]error, len(users))
		for i, u := range users {
			wg.Go(func() { _, results[i] = e.rt.Approve(ctx, taskID, u, "") })
		}
		wg.Wait()
		ok := 0
		for _, err := range results {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, biz.ErrTaskNotPending), errors.Is(err, biz.ErrInstanceNotRunning):
			default:
				t.Fatalf("concurrent Approve error = %v", err)
			}
		}
		if ok != 1 {
			t.Fatalf("round %d: %d approvals succeeded, want exactly 1", round, ok)
		}
		if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'complete'", in.ID); n != 1 {
			t.Fatalf("round %d: complete history rows = %d, want 1", round, n)
		}
	}

	fork := `{"nodes":[{"id":"start","type":"start"},{"id":"f","type":"parallelGateway","join":"j"},
		{"id":"a","type":"userTask","name":"A","assignee":{"rules":[{"kind":"user","userIds":["` + uOps1 + `"]}]}},
		{"id":"b","type":"userTask","name":"B","assignee":{"rules":[{"kind":"user","userIds":["` + uOps2 + `"]}]}},
		{"id":"c","type":"userTask","name":"C","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]}},
		{"id":"j","type":"parallelGateway"},
		{"id":"z","type":"userTask","name":"Z","assignee":{"rules":[{"kind":"user","userIds":["` + uBoss + `"]}]}},{"id":"end","type":"end"}],
		"edges":[{"id":"1","source":"start","target":"f"},{"id":"2","source":"f","target":"a"},{"id":"3","source":"f","target":"b"},{"id":"4","source":"f","target":"c"},
		{"id":"5","source":"a","target":"j"},{"id":"6","source":"b","target":"j"},{"id":"7","source":"c","target":"j"},{"id":"8","source":"j","target":"z"},{"id":"9","source":"z","target":"end"}]}`
	e.publish("fork", fork)
	for round := range 10 {
		in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "fork", Title: fmt.Sprintf("f%d", round), Initiator: uInit})
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		var wg sync.WaitGroup
		users := []string{uOps1, uOps2, uLead}
		results := make([]error, len(users))
		for i, u := range users {
			wg.Go(func() {
				for _, item := range e.todo(u) {
					if item.InstanceID == in.ID {
						_, results[i] = e.rt.Approve(ctx, item.TaskID, u, "")
					}
				}
			})
		}
		wg.Wait()
		for i, err := range results {
			if err != nil {
				t.Fatalf("round %d: branch %d Approve error = %v", round, i, err)
			}
		}
		if n := e.count("SELECT count(*) FROM workflow_task WHERE instance_id = $1 AND node_id = 'z'", in.ID); n != 1 {
			t.Fatalf("round %d: tasks after join = %d, want exactly 1", round, n)
		}
		if n := e.count("SELECT count(*) FROM workflow_token WHERE instance_id = $1 AND status <> $2", in.ID, biz.TokenDone); n != 1 {
			t.Fatalf("round %d: live tokens after join = %d, want 1", round, n)
		}
	}
}
