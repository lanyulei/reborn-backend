//go:build integration

package data

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"

	"github.com/go-kratos/kratos/v3/errors"
)

func (e *env) runner() *biz.JobUsecase {
	return biz.NewJobUsecase(e.rt, e.jobs, e.jobs, e.cfg, NewWebhookCaller().WithInsecure(), e.log())
}

// log 把执行器的日志写进测试输出。
func (e *env) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{e.t}, nil))
}

// testWriter 把执行器的日志写进测试输出（失败时能看到作业为什么没成功）。
type testWriter struct{ t testing.TB }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// due 把实例名下待执行的作业全部拨到已到期（测试里不真的等）。
func (e *env) due(instanceID string) {
	e.t.Helper()
	if _, err := e.d.Pool.Exec(context.Background(),
		"UPDATE workflow_job SET due_time = now() - interval '1 second', lock_until = NULL WHERE instance_id = $1 AND status = 1", instanceID); err != nil {
		e.t.Fatalf("make jobs due: %v", err)
	}
}

// forceDisable 停用一个仍被别的流程当作子流程引用的定义：正常途径会被拒绝（先断言这一点），
// 再直接改库——模拟引用检查之前留下的存量数据 / 并发窗口，运行期仍须正确处置。
func (e *env) forceDisable(definitionID string) {
	e.t.Helper()
	off := biz.DefinitionDisabled
	if _, err := e.defs.Update(context.Background(), definitionID, biz.DefinitionUpdate{Status: &off}); !errors.Is(err, biz.ErrDefinitionInUse) {
		e.t.Fatalf("Update(disable a referenced definition) error = %v, want ErrDefinitionInUse", err)
	}
	if err := e.defs.Delete(context.Background(), definitionID); !errors.Is(err, biz.ErrDefinitionInUse) {
		e.t.Fatalf("Delete(a referenced definition) error = %v, want ErrDefinitionInUse", err)
	}
	if _, err := e.d.Pool.Exec(context.Background(), "UPDATE workflow_definition SET status = 0 WHERE id = $1", definitionID); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) run() int {
	e.t.Helper()
	// 两个循环各跑一轮：引擎内部作业 + 自动任务（线上是两个独立的后台循环）。
	n, err := e.runner().RunDue(context.Background())
	if err != nil {
		e.t.Fatalf("RunDue() error = %v", err)
	}
	m, err := e.runner().RunServices(context.Background())
	if err != nil {
		e.t.Fatalf("RunServices() error = %v", err)
	}
	return n + m
}

func (e *env) start(key string, vars map[string]any) *biz.Instance {
	e.t.Helper()
	in, err := e.rt.Start(context.Background(), biz.StartParams{DefinitionKey: key, Title: "t-" + key, Initiator: uInit, Variables: vars})
	if err != nil {
		e.t.Fatalf("Start(%s) error = %v", key, err)
	}
	return in
}

func (e *env) approveTodo(user string) {
	e.t.Helper()
	todo := e.todo(user)
	if len(todo) != 1 {
		e.t.Fatalf("todo(%s) = %d items, want 1", user, len(todo))
	}
	if _, err := e.rt.Approve(context.Background(), todo[0].TaskID, user, "ok"); err != nil {
		e.t.Fatalf("Approve(by %s) error = %v", user, err)
	}
}

func task(id, user string) string {
	return `{"id":"` + id + `","type":"userTask","name":"` + id + `","assignee":{"rules":[{"kind":"user","userIds":["` + user + `"]}]}}`
}

// line 发起 → nodes 依次相连 → 结束。
func line(nodes ...string) string {
	g := `{"nodes":[{"id":"start","type":"start"},`
	prev, edges := "start", ""
	for i, n := range nodes {
		var parsed struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal([]byte(n), &parsed)
		g += n + ","
		edges += `{"id":"e` + strconv.Itoa(i) + `","source":"` + prev + `","target":"` + parsed.ID + `"},`
		prev = parsed.ID
	}
	return g + `{"id":"end","type":"end"}],"edges":[` + edges + `{"id":"elast","source":"` + prev + `","target":"end"}],"settings":{"admins":["` + uBoss + `"]}}`
}

// 定时节点与任务超时：到期前不动，到期后由执行器推进；重复执行不重复生效。
func TestIntegration_TimerAndTimeout(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("timed", line(
		`{"id":"wait","type":"timer","name":"等一会","timer":{"mode":"delay","duration":{"value":10,"unit":"minute"}}}`,
		`{"id":"a","type":"userTask","name":"a","assignee":{"rules":[{"kind":"user","userIds":["`+uLead+`"]}]},
		  "timeout":{"value":2,"unit":"hour","workTime":true,"action":"approve"}}`,
		task("b", uBoss)))
	in := e.start("timed", nil)
	if n := e.run(); n != 0 {
		t.Fatalf("RunDue() before due = %d, want 0", n)
	}
	d, err := e.rt.Get(ctx, in.ID, uInit, false)
	if err != nil || len(d.State.Jobs) != 1 || d.State.Jobs[0].Kind != biz.JobTimer {
		t.Fatalf("detail waits = %+v, err %v", d.State.Jobs, err)
	}
	e.due(in.ID)
	if n := e.run(); n != 1 {
		t.Fatalf("RunDue() = %d, want 1", n)
	}
	todo := e.todo(uLead)
	if len(todo) != 1 {
		t.Fatalf("todo(lead) after timer = %d", len(todo))
	}
	// 按工作时间计的时限：到期时间落在工作时段内（默认日历 09:00-12:00 / 13:30-18:00）。
	var due time.Time
	if err := e.d.Pool.QueryRow(ctx, "SELECT due_time FROM workflow_task WHERE id = $1", todo[0].TaskID).Scan(&due); err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if h := due.In(loc).Hour(); h < 9 || h > 18 || due.Before(time.Now()) {
		t.Fatalf("task due at %s, want a future working hour", due.In(loc))
	}
	e.due(in.ID)
	e.run()
	if len(e.todo(uLead)) != 0 || len(e.todo(uBoss)) != 1 {
		t.Fatalf("after timeout: lead %d, boss %d", len(e.todo(uLead)), len(e.todo(uBoss)))
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action IN ('timer', 'timeout')", in.ID); n != 2 {
		t.Fatalf("timer/timeout histories = %d, want 2", n)
	}
	// 实例结束后遗留的作业被作废。
	if err := e.rt.Terminate(ctx, in.ID, uBoss, "stop"); err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM workflow_job WHERE instance_id = $1 AND status = 1", in.ID); n != 0 {
		t.Fatalf("pending jobs after terminate = %d", n)
	}
}

// 多个执行器并发领取：每条作业只生效一次。
func TestIntegration_JobsClaimedOnce(t *testing.T) {
	e := newEnv(t)
	e.publish("timed", line(`{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"day"}}}`, task("a", uLead)))
	const n = 12
	for range n {
		e.due(e.start("timed", nil).ID)
	}
	var wg sync.WaitGroup
	var handled atomic.Int64
	for range 4 {
		wg.Go(func() {
			for {
				got, err := e.runner().RunDue(context.Background())
				if err != nil || got == 0 {
					return
				}
				handled.Add(int64(got))
			}
		})
	}
	wg.Wait()
	if got := e.count("SELECT count(*) FROM workflow_history WHERE action = 'timer'"); got != n || len(e.todo(uLead)) != n {
		t.Fatalf("timer fired %d times, todo %d, claimed %d; want %d each", got, len(e.todo(uLead)), handled.Load(), n)
	}
	if got := e.count("SELECT count(*) FROM workflow_job WHERE status <> 2"); got != 0 {
		t.Fatalf("unfinished jobs = %d", got)
	}
}

// 自动任务：签名调用 Webhook、返回值并入流程变量；失败重试、耗尽进死信并通知管理员；
// 管理员可重试或跳过。
func TestIntegration_ServiceTask(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	var (
		fail  atomic.Bool
		calls atomic.Int64
		seen  sync.Map
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		ts := r.Header.Get(HeaderTimestamp)
		if r.Header.Get(HeaderSignature) != sign("s3cret", ts, body) || r.Header.Get(HeaderJobID) == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		seen.Store(r.Header.Get(HeaderJobID), string(body))
		if fail.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"variables":{"account":"gitlab-42"}}`))
	}))
	defer srv.Close()
	hook := &biz.Webhook{ID: "018f3e1a-0f00-7000-8000-000000000001", Name: "开通账号", URL: srv.URL, Secret: "s3cret", Status: biz.WebhookEnabled, UpdateTime: time.Now()}
	if err := e.cfg.CreateWebhook(ctx, hook, 10); err != nil {
		t.Fatalf("CreateWebhook() error = %v", err)
	}
	var stored []byte
	if err := e.d.Pool.QueryRow(ctx, "SELECT secret FROM workflow_webhook WHERE id = $1", hook.ID).Scan(&stored); err != nil || string(stored) == "s3cret" || len(stored) == 0 {
		t.Fatalf("webhook secret must be stored encrypted: %q, err %v", stored, err)
	}
	e.publish("auto", line(`{"id":"svc","type":"serviceTask","name":"开通","service":{"webhookId":"`+hook.ID+`"}}`, task("a", uLead)))

	in := e.start("auto", map[string]any{"reason": "x"})
	if n := e.run(); n != 1 {
		t.Fatalf("RunDue() = %d, want 1", n)
	}
	got, err := e.rt.Get(ctx, in.ID, uInit, false)
	if err != nil || got.State.Instance.Variables["account"] != "gitlab-42" || got.State.Instance.Variables["reason"] != "x" || len(e.todo(uLead)) != 1 {
		t.Fatalf("after service: vars %v, todo %d, err %v", got.State.Instance.Variables, len(e.todo(uLead)), err)
	}
	var sent struct {
		InstanceID string         `json:"instanceId"`
		NodeID     string         `json:"nodeId"`
		Variables  map[string]any `json:"variables"`
	}
	seen.Range(func(_, body any) bool { _ = json.Unmarshal([]byte(body.(string)), &sent); return false })
	if sent.InstanceID != in.ID || sent.NodeID != "svc" || sent.Variables["reason"] != "x" {
		t.Fatalf("webhook payload = %+v", sent)
	}

	// 持续失败：重试 4 次后第 5 次进死信。
	fail.Store(true)
	calls.Store(0)
	in2 := e.start("auto", nil)
	for range 5 {
		e.due(in2.ID)
		e.run()
	}
	var status, attempts int
	if err := e.d.Pool.QueryRow(ctx, "SELECT status, attempts FROM workflow_job WHERE instance_id = $1 AND kind = 'service'", in2.ID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != int(biz.JobDead) || attempts != 5 || calls.Load() != 5 {
		t.Fatalf("job status %d attempts %d calls %d; want dead after 5", status, attempts, calls.Load())
	}
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type = $1", biz.EventServiceFailed); n != 1 {
		t.Fatalf("service-failed notifications = %d, want 1", n)
	}
	e.due(in2.ID)
	if n := e.run(); n != 0 {
		t.Fatalf("a dead job must not be picked up again, RunDue() = %d", n)
	}
	d, _ := e.rt.Get(ctx, in2.ID, uInit, false)
	if len(d.State.Jobs) != 1 || d.State.Jobs[0].Status != biz.JobDead || d.State.Jobs[0].LastError == "" {
		t.Fatalf("detail waits = %+v", d.State.Jobs)
	}
	jobID := d.State.Jobs[0].ID
	// 重试：端点恢复后成功。
	if err := e.rt.RetryJob(ctx, e.jobs, in.ID, jobID); !errors.Is(err, biz.ErrJobNotFound) {
		t.Fatalf("RetryJob(wrong instance) error = %v, want ErrJobNotFound", err)
	}
	fail.Store(false)
	if err := e.rt.RetryJob(ctx, e.jobs, in2.ID, jobID); err != nil {
		t.Fatalf("RetryJob() error = %v", err)
	}
	e.run()
	if len(e.todo(uLead)) != 2 {
		t.Fatalf("todo(lead) after retry = %d, want 2", len(e.todo(uLead)))
	}
	// 跳过：端点被停用（不可重试，直接死信）后由管理员跳过。
	if err := e.cfg.UpdateWebhook(ctx, &biz.Webhook{ID: hook.ID, Name: hook.Name, URL: hook.URL, Status: biz.WebhookDisabled}, false); err != nil {
		t.Fatal(err)
	}
	in3 := e.start("auto", nil)
	e.run()
	d, _ = e.rt.Get(ctx, in3.ID, uInit, false)
	if len(d.State.Jobs) != 1 || d.State.Jobs[0].Status != biz.JobDead {
		t.Fatalf("disabled webhook: waits = %+v", *d.State.Jobs[0])
	}
	if _, err := e.rt.SkipJob(ctx, e.jobs, in3.ID, d.State.Jobs[0].ID, uBoss, "端点下线"); err != nil {
		t.Fatalf("SkipJob() error = %v", err)
	}
	if len(e.todo(uLead)) != 3 {
		t.Fatalf("todo(lead) after skip = %d, want 3", len(e.todo(uLead)))
	}
	// 实例结束后死信一并作废，不能再重试（不会对已结束的流程再调用外部端点）。
	in4 := e.start("auto", nil)
	e.run()
	d, _ = e.rt.Get(ctx, in4.ID, uInit, false)
	dead := d.State.Jobs[0].ID
	if err := e.rt.Terminate(ctx, in4.ID, uBoss, "stop"); err != nil {
		t.Fatal(err)
	}
	if err := e.rt.RetryJob(ctx, e.jobs, in4.ID, dead); !errors.Is(err, biz.ErrInstanceNotRunning) {
		t.Fatalf("RetryJob(finished instance) error = %v, want ErrInstanceNotRunning", err)
	}
	if n := e.count("SELECT count(*) FROM workflow_job WHERE instance_id = $1 AND status IN (1, 4)", in4.ID); n != 0 {
		t.Fatalf("unsettled jobs on a finished instance = %d", n)
	}
	// 仍被已发布的流程引用：不能删。
	if err := e.cfg.DeleteWebhook(ctx, hook.ID); !errors.Is(err, biz.ErrWebhookInUse) {
		t.Fatalf("DeleteWebhook(in use) error = %v, want ErrWebhookInUse", err)
	}
}

func TestIntegration_WebhookAndReferences(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	uc := biz.NewWebhookUsecase(e.cfg)
	for name, url := range map[string]string{
		"http": "http://example.com/hook", "loopback": "https://127.0.0.1/hook", "private": "https://10.0.0.8/hook",
		"credentials": "https://u:p@example.com/hook", "garbage": "::not a url",
	} {
		if _, err := uc.Create(ctx, &biz.Webhook{Name: name, URL: url, Status: 1}); !errors.Is(err, biz.ErrWebhookInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrWebhookInvalid", name, err)
		}
	}
	w, err := uc.Create(ctx, &biz.Webhook{Name: "ok", URL: "https://hooks.example.com/a", Status: 1, Secret: "k"})
	if err != nil || !w.HasSecret || w.Secret != "" {
		t.Fatalf("Create() = %+v, err %v", w, err)
	}
	if _, err := uc.Create(ctx, &biz.Webhook{Name: "ok", URL: "https://hooks.example.com/b", Status: 1}); !errors.Is(err, biz.ErrWebhookNameExists) {
		t.Fatalf("Create(dup) error = %v", err)
	}
	// 不传密钥保留、传空串清除。
	if err := uc.Update(ctx, &biz.Webhook{ID: w.ID, Name: "ok", URL: w.URL, Status: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := e.cfg.Resolve(ctx, w.ID); err != nil || got.Secret != "k" {
		t.Fatalf("Resolve() = %+v, err %v", got, err)
	}
	empty := ""
	if err := uc.Update(ctx, &biz.Webhook{ID: w.ID, Name: "ok", URL: w.URL, Status: 1}, &empty); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.cfg.Resolve(ctx, w.ID); got.Secret != "" {
		t.Fatalf("secret not cleared: %+v", got)
	}
	// 真实调用器拒绝内网地址（即便绕过了保存时的校验）。
	if _, retryable, err := NewWebhookCaller().Call(ctx, &biz.Webhook{URL: "https://127.0.0.1:1/x"}, "job", []byte("{}")); err == nil || retryable {
		t.Fatalf("Call(loopback) = retryable %v, err %v; want a non-retryable error", retryable, err)
	}

	// 发布校验：引用了不存在的 Webhook / 子流程、或把自己当子流程，都不能发布。
	d, err := e.defs.Create(ctx, &biz.Definition{Key: "refs", Name: "引用"})
	if err != nil {
		t.Fatal(err)
	}
	for name, node := range map[string]string{
		"SERVICE_WEBHOOK_MISSING": `{"id":"n","type":"serviceTask","service":{"webhookId":"018f3e1a-0f00-7000-8000-00000000dead"}}`,
		"SUBPROCESS_MISSING":      `{"id":"n","type":"subProcess","subProcess":{"definitionKey":"nope"}}`,
		"SUBPROCESS_SELF":         `{"id":"n","type":"subProcess","subProcess":{"definitionKey":"refs"}}`,
	} {
		rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(line(node, task("a", uLead))), d.Revision)
		if err != nil || len(issues) != 1 || issues[0].Code != name {
			t.Fatalf("SaveDraft(%s) issues = %+v, err %v", name, issues, err)
		}
		if _, err := e.defs.Publish(ctx, d.ID, rev); !errors.Is(err, biz.ErrDefinitionInvalid) {
			t.Fatalf("Publish(%s) error = %v, want ErrDefinitionInvalid", name, err)
		}
		d.Revision = rev
	}
	if err := uc.Delete(ctx, w.ID); err != nil {
		t.Fatalf("Delete(unused) error = %v", err)
	}
}

// 子流程：父流程等子流程结束后继续；父流程结束时仍在进行的子流程被终止；发起不了整体回滚。
func TestIntegration_SubProcess(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(task("c", uOps1)))
	e.publish("parent_flow", line(task("a", uLead), `{"id":"sub","type":"subProcess","name":"子流程","subProcess":{"definitionKey":"child_flow"}}`, task("b", uBoss)))
	child := func(parent string) string {
		var id string
		if err := e.d.Pool.QueryRow(ctx, "SELECT id::text FROM workflow_instance WHERE parent_instance_id = $1", parent).Scan(&id); err != nil {
			t.Fatalf("child of %s: %v", parent, err)
		}
		return id
	}

	in := e.start("parent_flow", map[string]any{"k": "v"})
	e.approveTodo(uLead)
	cid := child(in.ID)
	c, err := e.rt.Get(ctx, cid, uInit, false)
	if err != nil || c.State.Instance.Depth != 1 || c.State.Instance.Variables["k"] != "v" || c.State.Instance.Initiator != uInit {
		t.Fatalf("child = %+v, err %v", c.State.Instance, err)
	}
	if err := e.rt.Cancel(ctx, cid, uInit, ""); !errors.Is(err, biz.ErrInstanceForbidden) {
		t.Fatalf("Cancel(child) error = %v, want ErrInstanceForbidden", err)
	}
	e.approveTodo(uOps1)
	if len(e.todo(uBoss)) != 0 {
		t.Fatal("parent advanced before the resume job ran")
	}
	if n := e.run(); n != 1 || len(e.todo(uBoss)) != 1 {
		t.Fatalf("RunDue() = %d, todo(boss) = %d", n, len(e.todo(uBoss)))
	}
	if d, _ := e.rt.Get(ctx, in.ID, uInit, false); len(d.Children) != 1 || d.Children[0].Status != biz.InstanceCompleted {
		t.Fatalf("children = %+v", d.Children)
	}
	// 业务监听器只对顶层流程触发。
	if len(e.listener.finished) != 0 {
		t.Fatalf("listener fired for a child: %v", e.listener.finished)
	}

	// 父流程被终止 → 子流程随后被终止，且不会再回头唤醒父流程造成副作用。
	in2 := e.start("parent_flow", nil)
	e.approveTodo(uLead)
	cid2 := child(in2.ID)
	if err := e.rt.Terminate(ctx, in2.ID, uBoss, "不做了"); err != nil {
		t.Fatal(err)
	}
	for e.run() > 0 {
	}
	if got := e.count("SELECT status FROM workflow_instance WHERE id = $1", cid2); got != int(biz.InstanceTerminated) {
		t.Fatalf("child status = %d, want terminated", got)
	}
	if n := e.count("SELECT count(*) FROM workflow_job WHERE status = 1"); n != 0 {
		t.Fatalf("pending jobs left = %d", n)
	}

	// 子流程被拒绝 → 父流程以已拒绝结束。
	in3 := e.start("parent_flow", nil)
	e.approveTodo(uLead)
	todo := e.todo(uOps1)
	if _, err := e.rt.Reject(ctx, todo[0].TaskID, uOps1, "no"); err != nil {
		t.Fatal(err)
	}
	e.run()
	if got := e.count("SELECT status FROM workflow_instance WHERE id = $1", in3.ID); got != int(biz.InstanceRejected) {
		t.Fatalf("parent status = %d, want rejected", got)
	}

	// 子流程发起不了（被停用）：审批动作整体回滚，待办还在。
	cd, _, err := e.defs.List(ctx, biz.DefinitionFilter{Keyword: "child_flow"}, 1, 10)
	if err != nil || len(cd) != 1 {
		t.Fatalf("List(child_flow) = %d, err %v", len(cd), err)
	}
	e.forceDisable(cd[0].ID)
	in4 := e.start("parent_flow", nil)
	todo = e.todo(uLead)
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uLead, ""); !errors.Is(err, biz.ErrSubProcessUnavailable) {
		t.Fatalf("Approve(into unavailable sub-process) error = %v, want ErrSubProcessUnavailable", err)
	}
	if len(e.todo(uLead)) != 1 || e.count("SELECT count(*) FROM workflow_instance WHERE parent_instance_id = $1", in4.ID) != 0 {
		t.Fatal("failed sub-process start must roll the command back")
	}
}

// 代理设置：生效期内新到的待办落到代理人头上，被代理人可查看；交接把未了结的席位换人。
func TestIntegration_DelegationAndHandover(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	uc := biz.NewDelegationUsecase(e.cfg, e.dir, usersAdapter{e.dir})
	now := time.Now()
	for name, d := range map[string]*biz.Delegation{
		"self":     {UserID: uLead, DelegateID: uLead, StartTime: now, EndTime: now.Add(time.Hour)},
		"past":     {UserID: uLead, DelegateID: uOps1, StartTime: now.Add(-2 * time.Hour), EndTime: now.Add(-time.Hour)},
		"inactive": {UserID: uLead, DelegateID: uGone, StartTime: now, EndTime: now.Add(time.Hour)},
		"too long": {UserID: uLead, DelegateID: uOps1, StartTime: now, EndTime: now.Add(400 * 24 * time.Hour)},
	} {
		if _, err := uc.Create(ctx, d); !errors.Is(err, biz.ErrDelegationInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrDelegationInvalid", name, err)
		}
	}
	// 并发新增重叠的时间段：只成功一条。
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := range errs {
		wg.Go(func() {
			_, errs[i] = uc.Create(ctx, &biz.Delegation{UserID: uLead, DelegateID: uOps1, StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour), Reason: "休假"})
		})
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else if !errors.Is(err, biz.ErrDelegationInvalid) {
			t.Fatalf("concurrent Create error = %v", err)
		}
	}
	items, names, err := uc.List(ctx, uLead)
	if ok != 1 || err != nil || len(items) != 1 || names[uOps1] == "" {
		t.Fatalf("created %d, listed %d, names %v, err %v", ok, len(items), names, err)
	}

	e.publish("simple", line(task("a", uLead), task("b", uBoss)))
	in := e.start("simple", nil)
	if len(e.todo(uLead)) != 0 || len(e.todo(uOps1)) != 1 {
		t.Fatalf("todo: lead %d, proxy %d; want 0 / 1", len(e.todo(uLead)), len(e.todo(uOps1)))
	}
	if _, err := e.rt.Get(ctx, in.ID, uLead, false); err != nil {
		t.Fatalf("the represented user must be able to view the instance: %v", err)
	}
	if ok, err := NewInstanceRepo(e.d).IsParticipant(ctx, in.ID, uLead); err != nil || !ok {
		t.Fatalf("IsParticipant(represented) = %v, %v", ok, err)
	}
	if err := uc.Delete(ctx, uBoss, items[0].ID); !errors.Is(err, biz.ErrDelegationNotFound) {
		t.Fatalf("Delete(someone else's) error = %v, want ErrDelegationNotFound", err)
	}
	if err := uc.Delete(ctx, uLead, items[0].ID); err != nil {
		t.Fatal(err)
	}

	// 交接：ops1 名下的待办全部换给 ops2；接手人须为可用用户。
	if _, _, _, err := e.rt.Handover(ctx, uOps1, uGone, uBoss, "离职", ""); !errors.Is(err, biz.ErrTaskTargetInvalid) {
		t.Fatalf("Handover(to inactive) error = %v, want ErrTaskTargetInvalid", err)
	}
	e.start("simple", nil) // 代理已删除：这一条落在 lead 本人
	moved, failed, more, err := e.rt.Handover(ctx, uOps1, uOps2, uBoss, "离职", "")
	if err != nil || moved != 1 || failed != 0 || more != "" || len(e.todo(uOps1)) != 0 || len(e.todo(uOps2)) != 1 || len(e.todo(uLead)) != 1 {
		t.Fatalf("Handover() = %d, %v, %v; todo ops1 %d ops2 %d lead %d", moved, more, err, len(e.todo(uOps1)), len(e.todo(uOps2)), len(e.todo(uLead)))
	}
	e.approveTodo(uOps2)
	if len(e.todo(uBoss)) != 1 {
		t.Fatalf("todo(boss) = %d", len(e.todo(uBoss)))
	}
}

func TestIntegration_Calendar(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	uc := biz.NewCalendarUsecase(e.cfg)
	orig, err := uc.Get(ctx)
	if err != nil || orig.Timezone == "" || len(orig.Segments) == 0 {
		t.Fatalf("Get() = %+v, err %v", orig, err)
	}
	t.Cleanup(func() { _ = e.cfg.SaveCalendar(context.Background(), orig) })
	bad := orig
	bad.Timezone = "Nowhere/City"
	if err := uc.Save(ctx, bad); !errors.Is(err, biz.ErrCalendarInvalid) {
		t.Fatalf("Save(bad) error = %v, want ErrCalendarInvalid", err)
	}
	next := orig
	next.Holidays = []string{"2026-10-01"}
	if err := uc.Save(ctx, next); err != nil {
		t.Fatal(err)
	}
	if got, _ := uc.Get(ctx); len(got.Holidays) != 1 {
		t.Fatalf("Get() after save = %+v", got)
	}
	if cal, err := uc.Current(ctx); err != nil || cal == nil {
		t.Fatalf("Current() = %v, %v", cal, err)
	}
}

// 父流程终止与子流程结束并发：两边各往对方名下排作业，不得互相等待成环（死锁）。
func TestIntegration_ParentChildNoDeadlock(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(task("c", uOps1)))
	e.publish("parent_flow", line(`{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"child_flow"}}`, task("b", uBoss)))
	const n = 12
	parents := make([]string, n)
	for i := range parents {
		parents[i] = e.start("parent_flow", nil).ID
	}
	todo := e.todo(uOps1)
	if len(todo) != n {
		t.Fatalf("child tasks = %d, want %d", len(todo), n)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2*n)
	for i := range n {
		wg.Go(func() { errs[i] = e.rt.Terminate(ctx, parents[i], uBoss, "stop") })
		wg.Go(func() { _, errs[n+i] = e.rt.Approve(ctx, todo[i].TaskID, uOps1, "ok") })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent command[%d] error = %v", i, err)
		}
	}
	for e.run() > 0 {
	}
	if left := e.count("SELECT count(*) FROM workflow_instance WHERE status = 1"); left != 0 {
		t.Fatalf("running instances left = %d", left)
	}
	if left := e.count("SELECT count(*) FROM workflow_job WHERE status = 1"); left != 0 {
		t.Fatalf("pending jobs left = %d", left)
	}
}

// 作业执行遇到确定性的业务错误（下一个节点的子流程发起不了）：不反复重试，直接转死信并
// 让管理员看得见；修好之后可以重试。
func TestIntegration_PermanentJobFailure(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("child_flow", line(task("c", uOps1)))
	e.publish("two_subs", line(
		`{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"minute"}}}`,
		`{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"child_flow"}}`, task("b", uBoss)))
	cd, _, err := e.defs.List(ctx, biz.DefinitionFilter{Keyword: "child_flow"}, 1, 10)
	if err != nil || len(cd) != 1 {
		t.Fatal(err)
	}
	on := biz.DefinitionEnabled
	e.forceDisable(cd[0].ID)
	in := e.start("two_subs", nil)
	e.due(in.ID)
	e.run()
	d, err := e.rt.Get(ctx, in.ID, uInit, false)
	if err != nil || len(d.State.Jobs) != 1 || d.State.Jobs[0].Status != biz.JobDead || d.State.Jobs[0].Kind != biz.JobTimer || d.State.Jobs[0].Attempts != 1 {
		t.Fatalf("after a permanent failure: jobs = %+v, err %v", d.State.Jobs, err)
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'serviceFailed'", in.ID); n != 1 {
		t.Fatalf("dead-letter history entries = %d, want 1", n)
	}
	if _, err := e.defs.Update(ctx, cd[0].ID, biz.DefinitionUpdate{Status: &on}); err != nil {
		t.Fatal(err)
	}
	if err := e.rt.RetryJob(ctx, e.jobs, in.ID, d.State.Jobs[0].ID); err != nil {
		t.Fatalf("RetryJob() error = %v", err)
	}
	e.run()
	if len(e.todo(uOps1)) != 1 {
		t.Fatalf("child task after retry = %d, want 1", len(e.todo(uOps1)))
	}
}

// 开放接口：凭令牌认证；只能发起被允许的流程、只能看和操作自己发起的实例；停用 / 重置 /
// 删除后旧令牌立即失效；库内不存令牌明文。
func TestIntegration_OpenAPI(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("open_flow", line(`{"id":"wait","type":"timer","timer":{"mode":"signal","signal":"paid"}}`, task("a", uLead)))
	e.publish("other_flow", line(task("a", uLead)))
	defRepo := NewDefinitionRepo(e.d)
	clients := biz.NewClientUsecase(e.cfg, defRepo, e.dir, usersAdapter{e.dir})
	open := biz.NewOpenUsecase(e.rt, e.cfg)

	for name, c := range map[string]*biz.Client{
		"inactive acting user": {Name: "x", ActingUserID: uGone, DefinitionKeys: []string{"open_flow"}, Status: 1},
		"unknown workflow":     {Name: "x", ActingUserID: uInit, DefinitionKeys: []string{"nope"}, Status: 1},
		"no workflow":          {Name: "x", ActingUserID: uInit, Status: 1},
	} {
		if _, _, err := clients.Create(ctx, c); !errors.Is(err, biz.ErrClientInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrClientInvalid", name, err)
		}
	}
	c, token, err := clients.Create(ctx, &biz.Client{Name: "订单系统", ActingUserID: uInit, DefinitionKeys: []string{"open_flow"}, Status: biz.ClientEnabled})
	if err != nil || len(token) < 40 || c.TokenPrefix != token[:10] {
		t.Fatalf("Create() = %+v, token %q, err %v", c, token, err)
	}
	if n := e.count("SELECT count(*) FROM workflow_client WHERE position($1::bytea in token_hash) > 0 OR token_prefix = $2", []byte(token), token); n != 0 {
		t.Fatal("the token must not be stored in clear text")
	}
	if _, _, err := clients.Create(ctx, &biz.Client{Name: "订单系统", ActingUserID: uInit, DefinitionKeys: []string{"open_flow"}, Status: 1}); !errors.Is(err, biz.ErrClientNameExists) {
		t.Fatalf("Create(dup) error = %v, want ErrClientNameExists", err)
	}
	other, otherToken, err := clients.Create(ctx, &biz.Client{Name: "另一个系统", ActingUserID: uInit, DefinitionKeys: []string{"open_flow", "other_flow"}, Status: biz.ClientEnabled})
	if err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{"", "nope", "wfk_" + "A", token + "x", "Bearer " + token} {
		if _, err := open.Get(ctx, bad, c.ID); !errors.Is(err, biz.ErrClientUnauthorized) {
			t.Errorf("Get(token %q) error = %v, want ErrClientUnauthorized", bad, err)
		}
	}
	if _, err := open.Start(ctx, token, biz.StartParams{DefinitionKey: "other_flow", Title: "t"}); !errors.Is(err, biz.ErrClientForbidden) {
		t.Fatalf("Start(not allowed) error = %v, want ErrClientForbidden", err)
	}
	const req = "018f3e1a-0b00-7000-8000-00000000aaaa"
	in, err := open.Start(ctx, token, biz.StartParams{DefinitionKey: "open_flow", Title: "订单 1", Variables: map[string]any{"amount": 9.0}, RequestID: req, Initiator: uBoss})
	if err != nil || in.Initiator != uInit || in.ClientID != c.ID {
		t.Fatalf("Start() = %+v, err %v (the initiator must be the acting user, not caller-supplied)", in, err)
	}
	// 幂等：相同的键返回同一个实例；别的凭据（同一代表用户）拿同一个键拿不到它。
	if again, err := open.Start(ctx, token, biz.StartParams{DefinitionKey: "open_flow", Title: "重试", RequestID: req}); err != nil || again.ID != in.ID {
		t.Fatalf("Start(same request) = %+v, err %v", again, err)
	}
	if _, err := open.Start(ctx, otherToken, biz.StartParams{DefinitionKey: "open_flow", Title: "撞键", RequestID: req}); !errors.Is(err, biz.ErrBusinessDuplicate) {
		t.Fatalf("Start(other client, same request id) error = %v, want ErrBusinessDuplicate", err)
	}
	// 只能看 / 操作自己发起的。
	for name, call := range map[string]func() error{
		"get":    func() error { _, err := open.Get(ctx, otherToken, in.ID); return err },
		"signal": func() error { _, err := open.Signal(ctx, otherToken, in.ID, "paid", nil); return err },
		"cancel": func() error { _, err := open.Cancel(ctx, otherToken, in.ID, ""); return err },
	} {
		if err := call(); !errors.Is(err, biz.ErrInstanceNotFound) {
			t.Errorf("%s(someone else's instance) error = %v, want ErrInstanceNotFound", name, err)
		}
	}
	if got, err := open.Signal(ctx, token, in.ID, "paid", map[string]any{"paid": true}); err != nil || got.Variables["paid"] != true || len(e.todo(uLead)) != 1 {
		t.Fatalf("Signal() = %+v, err %v, todo %d", got, err, len(e.todo(uLead)))
	}
	if got, err := open.Cancel(ctx, token, in.ID, "不要了"); err != nil || got.Status != biz.InstanceCancelled {
		t.Fatalf("Cancel() = %+v, err %v", got, err)
	}
	if n := e.count("SELECT count(*) FROM workflow_client WHERE id = $1 AND last_used_time IS NOT NULL", c.ID); n != 1 {
		t.Fatal("last_used_time not recorded")
	}

	// 停用、重置、删除：旧令牌立即失效。
	c.Status = biz.ClientDisabled
	if err := clients.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := open.Get(ctx, token, in.ID); !errors.Is(err, biz.ErrClientUnauthorized) {
		t.Fatalf("Get(disabled client) error = %v", err)
	}
	c.Status = biz.ClientEnabled
	if err := clients.Update(ctx, c); err != nil {
		t.Fatal(err)
	}
	fresh, err := clients.Rotate(ctx, c.ID)
	if err != nil || fresh == token {
		t.Fatalf("Rotate() = %q, err %v", fresh, err)
	}
	if _, err := open.Get(ctx, token, in.ID); !errors.Is(err, biz.ErrClientUnauthorized) {
		t.Fatalf("Get(old token after rotation) error = %v", err)
	}
	if _, err := open.Get(ctx, fresh, in.ID); err != nil {
		t.Fatalf("Get(new token) error = %v", err)
	}
	if err := clients.Delete(ctx, other.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := open.Start(ctx, otherToken, biz.StartParams{DefinitionKey: "other_flow", Title: "t"}); !errors.Is(err, biz.ErrClientUnauthorized) {
		t.Fatalf("Start(deleted client) error = %v", err)
	}
	if items, names, err := clients.List(ctx); err != nil || len(items) != 1 || names[uInit] == "" {
		t.Fatalf("List() = %d, %v, err %v", len(items), names, err)
	}
}

// 积压指标：只数到期超过一分钟仍待执行的作业，以及死信。
func TestIntegration_JobBacklog(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("timed", line(`{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"day"}}}`, task("a", uLead)))
	fresh, late, dead := e.start("timed", nil), e.start("timed", nil), e.start("timed", nil)
	if b, err := e.jobs.Backlog(ctx); err != nil || b.Overdue != 0 || b.Dead != 0 || b.OldestSeconds != 0 {
		t.Fatalf("Backlog() with nothing due = %+v, %v", b, err)
	}
	if _, err := e.d.Pool.Exec(ctx, "UPDATE workflow_job SET due_time = now() - interval '10 minutes' WHERE instance_id = $1", late.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.d.Pool.Exec(ctx, "UPDATE workflow_job SET status = 4 WHERE instance_id = $1", dead.ID); err != nil {
		t.Fatal(err)
	}
	b, err := e.jobs.Backlog(ctx)
	if err != nil || b.Overdue != 1 || b.Dead != 1 || b.OldestSeconds < 590 || b.OldestSeconds > 700 {
		t.Fatalf("Backlog() = %+v, %v; want 1 overdue (~600s) and 1 dead", b, err)
	}
	_ = fresh
}
