//go:build integration

package data

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// businessStub 业务域监听器：记录结束 / 进度 / 同意前校验各自收到的业务对象；block 非空时
// 同意前校验不放行。
type businessStub struct {
	mu       sync.Mutex
	finished []string
	progress []string
	guarded  []string
	block    error
}

func (b *businessStub) OnInstanceFinished(_ context.Context, _ txrun.Tx, in *biz.Instance) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finished = append(b.finished, in.BusinessID)
	return nil
}

func (b *businessStub) OnInstanceProgress(_ context.Context, _ txrun.Tx, in *biz.Instance, _ time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.progress = append(b.progress, in.RootBusinessID)
	return nil
}

func (b *businessStub) BeforeApprove(_ context.Context, _ txrun.Tx, in *biz.Instance, _ *biz.Node, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.guarded = append(b.guarded, in.RootBusinessID)
	return b.block
}

// viewerFunc 业务侧的实例查看权限。
type viewerFunc func(ctx context.Context, userID, businessID string) (bool, error)

func (f viewerFunc) CanViewInstance(ctx context.Context, userID, businessID string) (bool, error) {
	return f(ctx, userID, businessID)
}

// runtime 用给定的监听器 / 查看权限另起一个运行时用例（与 e.rt 共用同一个库）。
func (e *env) runtime(listeners biz.Listeners, viewers biz.Viewers) *biz.RuntimeUsecase {
	inst := NewInstanceRepo(e.d)
	return biz.NewRuntimeUsecase(e.defs, inst, inst, inst, e.dir, usersAdapter{e.dir}, e.cfg, biz.NewCalendarUsecase(e.cfg),
		basedata.NewTxManager(e.d), biznotify.NewNotificationUsecase(datanotify.NewEventRepo(e.d), nil), listeners, viewers, nil, nil)
}

// 工单流程里的子流程：子流程自己不挂业务对象，但它的处理人面对的仍是根流程的那个业务对象——
// 待办归到业务对象上、算作参与者、字段权限与同意前校验照常生效；实例结束的监听只对顶层触发。
func TestIntegration_SubProcessBusinessReference(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	inst := NewInstanceRepo(e.d)
	stub := &businessStub{}
	const ticketID = "T-1"
	var rootID string
	viewers := biz.Viewers{"ticket": viewerFunc(func(ctx context.Context, userID, businessID string) (bool, error) {
		if businessID != ticketID {
			return false, nil
		}
		if userID == uOps2 { // 管理视图
			return true, nil
		}
		return inst.IsParticipant(ctx, rootID, userID)
	})}
	rt := e.runtime(biz.Listeners{"ticket": stub}, viewers)

	e.publish("child_flow", line(`{"id":"c","type":"userTask","name":"c","assignee":{"rules":[{"kind":"user","userIds":["`+uOps1+`"]}]},
		"formPermissions":{"secret":"hidden","note":"editable"}}`))
	e.publish("parent_flow", line(task("a", uLead),
		`{"id":"sub","type":"subProcess","name":"子流程","subProcess":{"definitionKey":"child_flow"}}`, task("b", uBoss)))

	root, err := rt.Start(ctx, biz.StartParams{
		DefinitionKey: "parent_flow", Title: "工单", Initiator: uInit, BusinessType: "ticket", BusinessID: ticketID,
		Variables: map[string]any{"secret": "s3cret", "note": "", "n": int64(3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootID = root.ID
	if root.RootID != root.ID || root.RootBusinessID != ticketID {
		t.Fatalf("root reference of a top-level instance = %q / %q", root.RootID, root.RootBusinessID)
	}
	// 变量在发起时即规整为 JSON 取值：与之后从库里读回的类型一致。
	if _, ok := root.Variables["n"].(float64); !ok {
		t.Fatalf("variables must be normalised at start, n is %T", root.Variables["n"])
	}
	todo, _, _, err := rt.ListTasks(ctx, uLead, biz.TaskViewTodo, "", "ticket", 1, 10)
	if err != nil || len(todo) != 1 {
		t.Fatalf("ListTasks(lead) = %d, err %v", len(todo), err)
	}
	if _, err := rt.Approve(ctx, todo[0].TaskID, uLead, ""); err != nil {
		t.Fatal(err)
	}

	// 子流程实例：继承根流程的引用，自己不挂业务对象（唯一约束只属于顶层流程）。
	children, err := inst.Children(ctx, root.ID)
	if err != nil || len(children) != 1 {
		t.Fatalf("Children() = %d, err %v", len(children), err)
	}
	childID := children[0].ID
	child, err := inst.GetInstance(ctx, childID)
	if err != nil || child.BusinessType != "" || child.BusinessID != "" || child.RootID != root.ID ||
		child.RootBusinessType != "ticket" || child.RootBusinessID != ticketID {
		t.Fatalf("child = %+v, err %v", child, err)
	}

	// 子流程处理人的待办按业务类型能查到，并指回那张工单。
	todo, _, total, err := rt.ListTasks(ctx, uOps1, biz.TaskViewTodo, "", "ticket", 1, 10)
	if err != nil || total != 1 || len(todo) != 1 || todo[0].BusinessType != "ticket" || todo[0].BusinessID != ticketID || todo[0].InstanceID != childID {
		t.Fatalf("ListTasks(child handler, ticket) = %+v, total %d, err %v", todo, total, err)
	}
	if items, _, _, _ := rt.ListTasks(ctx, uOps1, biz.TaskViewTodo, "", "other", 1, 10); len(items) != 0 {
		t.Fatalf("ListTasks(other business type) = %d, want 0", len(items))
	}

	// 参与者 / 待处理的人按整棵流程树算。
	for user, want := range map[string]bool{uOps1: true, uLead: true, uInit: true, uOps2: false, uBoss: false} {
		if ok, err := inst.IsParticipant(ctx, root.ID, user); err != nil || ok != want {
			t.Errorf("IsParticipant(root, %s) = %v, %v; want %v", user, ok, err, want)
		}
	}
	if pending, err := rt.PendingUsers(ctx, root.ID); err != nil || !slices.Equal(pending, []string{uOps1}) {
		t.Fatalf("PendingUsers(root) = %v, err %v", pending, err)
	}

	// 字段权限取自子流程的节点：note 可改、secret 对他隐藏。
	fa, err := rt.FormAccess(ctx, root.ID, uOps1)
	if err != nil || !fa.Pending || !fa.Seated || fa.Permissions["note"] != "editable" || !slices.Equal(fa.Hidden, []string{"secret"}) {
		t.Fatalf("FormAccess(root, child handler) = %+v, err %v", fa, err)
	}
	if fa, err := rt.FormAccess(ctx, root.ID, uLead); err != nil || fa.Pending || !fa.Seated || len(fa.Hidden) != 0 {
		t.Fatalf("FormAccess(root, earlier handler) = %+v, err %v", fa, err)
	}
	if fa, err := rt.FormAccess(ctx, root.ID, uOps2); err != nil || fa.Pending || fa.Seated {
		t.Fatalf("FormAccess(root, outsider) = %+v, err %v", fa, err)
	}

	// 实例详情：子流程处理人经业务域放行可以看根流程，隐藏字段不给；管理视图可以打开子流程；
	// 无关的人两个都打不开。
	d, err := rt.Get(ctx, root.ID, uOps1, false)
	if err != nil {
		t.Fatalf("Get(root, child handler) error = %v", err)
	}
	if _, leaked := d.State.Instance.Variables["secret"]; leaked {
		t.Fatal("a field hidden on the child handler's node leaked through the root instance")
	}
	if d, err := rt.Get(ctx, childID, uOps1, false); err != nil || d.MyTaskID == "" {
		t.Fatalf("Get(child, its handler) = %+v, err %v", d, err)
	} else if _, leaked := d.State.Instance.Variables["secret"]; leaked {
		t.Fatal("hidden field leaked through the child instance")
	}
	if d, err := rt.Get(ctx, childID, uOps2, false); err != nil || d.State.Instance.Variables["secret"] != "s3cret" {
		t.Fatalf("Get(child, supervisor) error = %v", err)
	}
	for _, id := range []string{root.ID, childID} {
		if _, err := rt.Get(ctx, id, uBoss, false); !errors.Is(err, biz.ErrInstanceForbidden) {
			t.Fatalf("Get(%s, outsider) error = %v, want ErrInstanceForbidden", id, err)
		}
	}

	// 改表单：以根流程的实例 ID 调用，改的是他任务所在的子流程实例。
	if err := rt.Amend(ctx, root.ID, uOps1, func(_ context.Context, _ txrun.Tx, perms map[string]string) (map[string]any, error) {
		if perms["note"] != "editable" {
			t.Errorf("Amend perms = %v", perms)
		}
		return map[string]any{"note": "补充说明", "count": int32(2)}, nil
	}); err != nil {
		t.Fatalf("Amend(root, child handler) error = %v", err)
	}
	if child, _ = inst.GetInstance(ctx, childID); child.Variables["note"] != "补充说明" || child.Variables["count"] != 2.0 {
		t.Fatalf("child variables after Amend = %v", child.Variables)
	}
	if err := rt.Amend(ctx, root.ID, uOps2, func(context.Context, txrun.Tx, map[string]string) (map[string]any, error) {
		return nil, nil
	}); !errors.Is(err, biz.ErrTaskNotAssignee) {
		t.Fatalf("Amend(outsider) error = %v, want ErrTaskNotAssignee", err)
	}

	// 同意前的业务校验对子流程里的任务同样生效，拿到的是根流程的业务对象。
	stub.block = kerrors.BadRequest("FORM_INVALID", "required field is empty")
	if _, err := rt.Approve(ctx, todo[0].TaskID, uOps1, ""); !errors.Is(err, stub.block) {
		t.Fatalf("Approve(blocked by the business guard) error = %v", err)
	}
	stub.block = nil
	if _, err := rt.Approve(ctx, todo[0].TaskID, uOps1, ""); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(stub.guarded, ticketID) || slices.Contains(stub.guarded, "") {
		t.Fatalf("guard saw business objects %v", stub.guarded)
	}
	if !slices.Contains(stub.progress, ticketID) || slices.Contains(stub.progress, "") {
		t.Fatalf("progress listener saw business objects %v", stub.progress)
	}
	// 子流程结束不等于业务对象结束。
	if len(stub.finished) != 0 {
		t.Fatalf("OnInstanceFinished fired for a child: %v", stub.finished)
	}
	jobs := biz.NewJobUsecase(rt, e.jobs, e.jobs, e.cfg, NewWebhookCaller().WithInsecure(), e.log())
	if n, err := jobs.RunDue(ctx); err != nil || n != 1 {
		t.Fatalf("RunDue(resume) = %d, err %v", n, err)
	}
	todo, _, _, _ = rt.ListTasks(ctx, uBoss, biz.TaskViewTodo, "", "ticket", 1, 10)
	if len(todo) != 1 {
		t.Fatalf("todo(boss) = %d", len(todo))
	}
	if _, err := rt.Approve(ctx, todo[0].TaskID, uBoss, ""); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stub.finished, []string{ticketID}) {
		t.Fatalf("OnInstanceFinished = %v, want exactly once for the ticket", stub.finished)
	}
	// 已办也按业务对象归类。
	done, _, total, err := rt.ListTasks(ctx, uOps1, biz.TaskViewDone, "", "ticket", 1, 10)
	if err != nil || total != 1 || len(done) != 1 || done[0].BusinessID != ticketID || done[0].ActionTime.IsZero() {
		t.Fatalf("ListTasks(done) = %+v, total %d, err %v", done, total, err)
	}
}

// 经开放接口发起的流程，其子流程继承接入凭据：外部系统可以查询并向子流程里的等待节点投递信号。
func TestIntegration_SubProcessInheritsClient(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(`{"id":"wait","type":"timer","timer":{"mode":"signal","signal":"paid"}}`))
	e.publish("parent_flow", line(`{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"child_flow"}}`, task("b", uBoss)))
	clients := biz.NewClientUsecase(e.cfg, NewDefinitionRepo(e.d), e.dir, usersAdapter{e.dir})
	open := biz.NewOpenUsecase(e.rt, e.cfg)
	c, token, err := clients.Create(ctx, &biz.Client{Name: "订单系统", ActingUserID: uInit, DefinitionKeys: []string{"parent_flow"}, Status: biz.ClientEnabled})
	if err != nil {
		t.Fatal(err)
	}
	in, err := open.Start(ctx, token, biz.StartParams{DefinitionKey: "parent_flow", Title: "订单"})
	if err != nil {
		t.Fatal(err)
	}
	children, err := NewInstanceRepo(e.d).Children(ctx, in.ID)
	if err != nil || len(children) != 1 {
		t.Fatalf("Children() = %d, err %v", len(children), err)
	}
	child, err := open.Get(ctx, token, children[0].ID)
	if err != nil || child.ClientID != c.ID {
		t.Fatalf("Get(child via the client) = %+v, err %v", child, err)
	}
	if _, err := open.Signal(ctx, token, children[0].ID, "paid", nil); err != nil {
		t.Fatalf("Signal(child) error = %v", err)
	}
	// 被开放接口凭据允许发起的流程不能停用 / 删除。
	off := biz.DefinitionDisabled
	if _, err := e.defs.Update(ctx, in.DefinitionID, biz.DefinitionUpdate{Status: &off}); !errors.Is(err, biz.ErrDefinitionInUse) {
		t.Fatalf("Update(disable a workflow a client may start) error = %v, want ErrDefinitionInUse", err)
	}
}

// 已办列表、计数与翻页；单条状态的视图走等值条件。
func TestIntegration_TaskViews(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("simple", line(task("a", uLead), `{"id":"cc","type":"cc","name":"抄送","assignee":{"rules":[{"kind":"user","userIds":["`+uLead+`"]}]}}`))
	const n = 5
	for range n {
		e.start("simple", nil)
	}
	for _, it := range e.todo(uLead)[:3] {
		if _, err := e.rt.Approve(ctx, it.TaskID, uLead, ""); err != nil {
			t.Fatal(err)
		}
	}
	for view, want := range map[string]int64{biz.TaskViewTodo: 2, biz.TaskViewDone: 3, biz.TaskViewCC: 3} {
		items, _, total, err := e.rt.ListTasks(ctx, uLead, view, "", "", 1, 2)
		if err != nil || total != want || len(items) != 2 {
			t.Errorf("ListTasks(%s) = %d items, total %d, err %v; want total %d", view, len(items), total, err, want)
		}
	}
	done, _, _, err := e.rt.ListTasks(ctx, uLead, biz.TaskViewDone, "", "", 1, 10)
	if err != nil || !slices.IsSortedFunc(done, func(a, b *biz.TaskItem) int { return b.ActionTime.Compare(a.ActionTime) }) {
		t.Fatalf("done view must be ordered by action time desc, err %v", err)
	}
	if items, _, total, err := e.rt.ListTasks(ctx, uLead, biz.TaskViewDone, "simple", "", 1, 10); err != nil || total != 3 || len(items) != 3 {
		t.Fatalf("ListTasks(done, keyword) = %d / %d, err %v", len(items), total, err)
	}
	if _, _, total, err := e.rt.ListTasks(ctx, uLead, biz.TaskViewDone, "no\x00pe", "", 1, 10); err != nil || total != 0 {
		t.Fatalf("ListTasks(keyword with NUL) total = %d, err %v", total, err)
	}
	// 已办查询用得上按处理时间排好序的部分索引（条件里写明了 action_time IS NOT NULL）。
	var plan string
	rows, err := e.d.Pool.Query(ctx, `EXPLAIN SELECT a.id FROM workflow_task_assignee a
		WHERE a.user_id = $1 AND a.status = ANY($2) AND a.action_time IS NOT NULL ORDER BY a.action_time DESC, a.id DESC LIMIT 20`,
		uLead, []int32{3, 4, 6, 7})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	rows.Close()
	t.Log(plan)
}

// 两类作业分开领取：引擎内部作业的循环不碰自动任务，反之亦然。
func TestIntegration_JobKindsClaimedSeparately(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	hook, err := biz.NewWebhookUsecase(e.cfg).Create(ctx, &biz.Webhook{Name: "hook", URL: "https://hooks.example.com/never", Status: 1})
	if err != nil {
		t.Fatal(err)
	}
	e.publish("mixed", `{"nodes":[{"id":"start","type":"start"},
		{"id":"fork","type":"parallelGateway","join":"join"},
		{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"day"}}},
		{"id":"svc","type":"serviceTask","service":{"webhookId":"`+hook.ID+`"}},
		{"id":"join","type":"parallelGateway"},{"id":"end","type":"end"}],
		"edges":[{"id":"e1","source":"start","target":"fork"},{"id":"e2","source":"fork","target":"wait"},
		{"id":"e3","source":"fork","target":"svc"},{"id":"e4","source":"wait","target":"join"},
		{"id":"e5","source":"svc","target":"join"},{"id":"e6","source":"join","target":"end"}],"settings":{}}`)
	in := e.start("mixed", nil)
	e.due(in.ID)
	internal, err := e.jobs.ClaimDue(ctx, false, 10, time.Minute)
	if err != nil || len(internal) != 1 || internal[0].Kind != biz.JobTimer {
		t.Fatalf("ClaimDue(internal) = %+v, err %v", internal, err)
	}
	service, err := e.jobs.ClaimDue(ctx, true, 10, time.Minute)
	if err != nil || len(service) != 1 || service[0].Kind != biz.JobService {
		t.Fatalf("ClaimDue(service) = %+v, err %v", service, err)
	}
	// 领走的在租约内不会被再次领取。
	for _, svc := range []bool{false, true} {
		if again, err := e.jobs.ClaimDue(ctx, svc, 10, time.Minute); err != nil || len(again) != 0 {
			t.Fatalf("ClaimDue(service=%v) again = %d, err %v", svc, len(again), err)
		}
	}
}

// 交接的游标：失败的实例不会让调用方空转，游标照常越过它们。
func TestIntegration_HandoverCursor(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("simple", line(task("a", uOps1)))
	for range 3 {
		e.start("simple", nil)
	}
	if _, _, _, err := e.rt.Handover(ctx, uOps1, uOps2, uBoss, "调岗", "not-a-uuid"); !errors.Is(err, biz.ErrTaskTargetInvalid) {
		t.Fatalf("Handover(bad cursor) error = %v, want ErrTaskTargetInvalid", err)
	}
	ids, err := NewInstanceRepo(e.d).InstancesWithSeats(ctx, uOps1, "", 10)
	if err != nil || len(ids) != 3 || !slices.IsSorted(ids) {
		t.Fatalf("InstancesWithSeats() = %v, err %v", ids, err)
	}
	rest, err := NewInstanceRepo(e.d).InstancesWithSeats(ctx, uOps1, ids[0], 10)
	if err != nil || !slices.Equal(rest, ids[1:]) {
		t.Fatalf("InstancesWithSeats(after first) = %v, want %v (err %v)", rest, ids[1:], err)
	}
	// 从第二个实例之后开始：只交接最后一个。
	moved, failed, next, err := e.rt.Handover(ctx, uOps1, uOps2, uBoss, "调岗", ids[1])
	if err != nil || moved != 1 || failed != 0 || next != "" {
		t.Fatalf("Handover(cursor) = %d moved, %d failed, next %q, err %v", moved, failed, next, err)
	}
	if len(e.todo(uOps1)) != 2 || len(e.todo(uOps2)) != 1 {
		t.Fatalf("todo ops1 %d ops2 %d", len(e.todo(uOps1)), len(e.todo(uOps2)))
	}
}

// 已发布版本的流程图通不过当前的校验规则（规则收紧后的存量版本）：依赖流程图的动作以
// 409 拒绝，作业进死信而不是无休止地重试，详情照常能打开，撤回 / 终止不受影响。
func TestIntegration_InvalidPublishedVersion(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(task("c", uOps1)))
	e.publish("timed", line(
		`{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"day"}}}`,
		`{"id":"a","type":"userTask","name":"a","assignee":{"rules":[{"kind":"user","userIds":["`+uLead+`"]}]},"formPermissions":{"secret":"hidden"}}`,
		`{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"child_flow"}}`))
	// 三个实例：停在定时节点 / 停在人工任务 / 停在子流程（子实例在跑）。
	waiting := e.start("timed", map[string]any{"secret": "x"})
	atTask := e.start("timed", map[string]any{"secret": "x"})
	e.due(atTask.ID)
	e.run()
	withChild := e.start("timed", nil)
	e.due(withChild.ID)
	e.run()
	for _, it := range e.todo(uLead) {
		if it.InstanceID == withChild.ID {
			if _, err := e.rt.Approve(ctx, it.TaskID, uLead, ""); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(e.todo(uOps1)) != 1 {
		t.Fatalf("child todo = %d", len(e.todo(uOps1)))
	}

	// 把已发布版本的流程图改成不合法的（去掉全部连线），并换一个新的用例实例（不带缓存）。
	if _, err := e.d.Pool.Exec(ctx, `UPDATE workflow_definition_version SET graph = jsonb_set(graph, '{edges}', '[]')
		WHERE id = $1`, waiting.VersionID); err != nil {
		t.Fatal(err)
	}
	defRepo := NewDefinitionRepo(e.d)
	e.defs = biz.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, e.cfg, nil)
	e.rt = e.runtime(nil, nil)

	// 发起、处理任务：409 而不是 500。
	if _, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "timed", Title: "t", Initiator: uInit}); !errors.Is(err, biz.ErrVersionInvalid) {
		t.Fatalf("Start() error = %v, want ErrVersionInvalid", err)
	}
	todo := e.todo(uLead)
	if len(todo) != 1 {
		t.Fatalf("todo(lead) = %d", len(todo))
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, ""); !errors.Is(err, biz.ErrVersionInvalid) || kerrors.Code(err) != 409 {
		t.Fatalf("Approve() error = %v, want 409 ErrVersionInvalid", err)
	}
	// 详情：发起人与只有席位的处理人都能打开；判断不了哪些字段该隐藏时，变量整份不给处理人。
	if d, err := e.rt.Get(ctx, atTask.ID, uInit, false); err != nil || d.State.Instance.Variables["secret"] != "x" || len(d.Graph) == 0 {
		t.Fatalf("Get(initiator) error = %v", err)
	}
	if d, err := e.rt.Get(ctx, atTask.ID, uLead, false); err != nil || len(d.State.Instance.Variables) != 0 {
		t.Fatalf("Get(seat-only viewer) = %v, err %v", d, err)
	}
	if fa, err := e.rt.FormAccess(ctx, atTask.ID, uLead); err != nil || !fa.HideAll || !fa.Seated {
		t.Fatalf("FormAccess() = %+v, err %v", fa, err)
	}
	// 定时作业：一次即转死信（记历史），不再被领取。
	e.due(waiting.ID)
	if n := e.run(); n != 1 {
		t.Fatalf("RunDue() = %d, want 1", n)
	}
	if n := e.count("SELECT count(*) FROM workflow_job WHERE instance_id = $1 AND status = 4", waiting.ID); n != 1 {
		t.Fatalf("dead jobs = %d, want 1", n)
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'serviceFailed'", waiting.ID); n != 1 {
		t.Fatalf("dead-letter history = %d, want 1", n)
	}
	if n := e.run(); n != 0 {
		t.Fatalf("a dead job was picked up again: %d", n)
	}
	// 终止不依赖流程图；随后的「终止子流程」作业也照常执行（子流程自己的版本是好的）。
	if err := e.rt.Terminate(ctx, withChild.ID, uBoss, "流程有问题"); err != nil {
		t.Fatalf("Terminate() error = %v", err)
	}
	for e.run() > 0 {
	}
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE parent_instance_id = $1 AND status = 5", withChild.ID); n != 1 {
		t.Fatalf("terminated children = %d, want 1", n)
	}
	if err := e.rt.Cancel(ctx, atTask.ID, uInit, ""); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
}

// 「终止子流程」的作业在子流程自己的版本已不合法时照常执行。
func TestIntegration_StopJobWithoutGraph(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(task("c", uOps1)))
	e.publish("parent_flow", line(`{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"child_flow"}}`))
	in := e.start("parent_flow", nil)
	children, _ := NewInstanceRepo(e.d).Children(ctx, in.ID)
	if len(children) != 1 {
		t.Fatalf("children = %d", len(children))
	}
	if _, err := e.d.Pool.Exec(ctx, `UPDATE workflow_definition_version v SET graph = jsonb_set(graph, '{edges}', '[]')
		FROM workflow_instance i WHERE i.id = $1 AND v.id = i.version_id`, children[0].ID); err != nil {
		t.Fatal(err)
	}
	defRepo := NewDefinitionRepo(e.d)
	e.defs = biz.NewDefinitionUsecase(defRepo, defRepo, defRepo, defRepo, e.cfg, nil)
	e.rt = e.runtime(nil, nil)
	if err := e.rt.Terminate(ctx, in.ID, uBoss, "不做了"); err != nil {
		t.Fatal(err)
	}
	for e.run() > 0 {
	}
	if got := e.count("SELECT status FROM workflow_instance WHERE id = $1", children[0].ID); got != int(biz.InstanceTerminated) {
		t.Fatalf("child status = %d, want terminated", got)
	}
	if n := e.count("SELECT count(*) FROM workflow_job WHERE status IN (1, 4)"); n != 0 {
		t.Fatalf("unsettled jobs = %d", n)
	}
}

// 同一幂等键的并发发起：都拿到同一个实例，而不是其中一个报「已发起」。
func TestIntegration_ConcurrentStartSameRequest(t *testing.T) {
	e := newEnv(t)
	e.publish("simple", line(task("a", uLead)))
	const req = "018f3e1a-0b00-7000-8000-00000000bbbb"
	const workers = 6
	ids := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			in, err := e.rt.Start(context.Background(), biz.StartParams{DefinitionKey: "simple", Title: "t", Initiator: uInit, RequestID: req})
			if errs[i] = err; err == nil {
				ids[i] = in.ID
			}
		})
	}
	wg.Wait()
	for i := range workers {
		if errs[i] != nil || ids[i] != ids[0] {
			t.Fatalf("Start #%d = %q, err %v; want the same instance %q for every caller", i, ids[i], errs[i], ids[0])
		}
	}
	if n := e.count("SELECT count(*) FROM workflow_instance"); n != 1 {
		t.Fatalf("instances = %d, want 1", n)
	}
}

// 标题只剩 NUL 字符：退回流程名称（通知域不接受空标题）；定义的名称 / 草稿里的 NUL 被拒绝或去掉。
func TestIntegration_NULHandling(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := e.publish("simple", line(task("a", uLead)))
	in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "simple", Title: "\x00", Initiator: uInit})
	if err != nil || in.Title != d.Name {
		t.Fatalf("Start(title of NUL) = %q, err %v; want the definition name", in.Title, err)
	}
	name := "带\x00空字符"
	upd, err := e.defs.Update(ctx, d.ID, biz.DefinitionUpdate{Name: &name})
	if err != nil || upd.Name != "带空字符" {
		t.Fatalf("Update(name with NUL) = %q, err %v", upd.Name, err)
	}
	if _, err := e.defs.Create(ctx, &biz.Definition{Key: "nul", Name: "\x00"}); kerrors.Code(err) != 400 {
		t.Fatalf("Create(name of NUL) error = %v, want 400", err)
	}
	bad := line(`{"id":"a","type":"userTask","name":"a\u0000b","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]}}`)
	if _, _, err := e.defs.SaveDraft(ctx, d.ID, []byte(bad), upd.Revision); !errors.Is(err, biz.ErrDefinitionInvalid) {
		t.Fatalf("SaveDraft(graph with NUL) error = %v, want ErrDefinitionInvalid", err)
	}
	if _, _, _, err := e.rt.ListInstances(ctx, biz.InstanceFilter{Keyword: "a\x00b"}, 1, 10); err != nil {
		t.Fatalf("ListInstances(keyword with NUL) error = %v", err)
	}
	if _, _, err := e.defs.List(ctx, biz.DefinitionFilter{Keyword: "a\x00b"}, 1, 10); err != nil {
		t.Fatalf("List definitions(keyword with NUL) error = %v", err)
	}
}

// 流程通知不带分组键：通知域不会把它们聚合成只发给路由收件人的摘要。
func TestIntegration_NoticesCarryNoGroupKey(t *testing.T) {
	e := newEnv(t)
	e.publish("simple", line(task("a", uLead)))
	e.start("simple", nil)
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type = 'workflow.task.assigned'"); n != 1 {
		t.Fatalf("assigned events = %d, want 1", n)
	}
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type LIKE 'workflow.%' AND COALESCE(group_key, '') <> ''"); n != 0 {
		t.Fatalf("workflow events with a group key = %d, want 0", n)
	}
}
