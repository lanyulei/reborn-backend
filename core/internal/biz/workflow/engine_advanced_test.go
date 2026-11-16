package biz

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

func userTask(id string, users string) string {
	return `{"id":"` + id + `","type":"userTask","name":"` + id + `","assignee":{"rules":[{"kind":"user","userIds":[` + users + `]}]}}`
}

// job 取最近排的一条给定类型的作业。
func (h *harness) job(kind string) *Job {
	h.t.Helper()
	for i := len(h.s.Jobs) - 1; i >= 0; i-- {
		if h.s.Jobs[i].Kind == kind {
			return h.s.Jobs[i]
		}
	}
	h.t.Fatalf("no %s job; jobs = %+v", kind, h.s.Jobs)
	return nil
}

func (h *harness) fire(j *Job) {
	h.t.Helper()
	if err := h.exec().Fire(context.Background(), j); err != nil {
		h.t.Fatalf("Fire(%s) error = %v", j.Kind, err)
	}
}

// seat 取某任务上某用户最新的席位。
func (h *harness) seat(nodeID, user string) *TaskAssignee {
	h.t.Helper()
	for _, t := range h.s.Tasks {
		if t.NodeID != nodeID {
			continue
		}
		for i := len(t.Assignees) - 1; i >= 0; i-- {
			if t.Assignees[i].UserID == user {
				return t.Assignees[i]
			}
		}
	}
	h.t.Fatalf("no seat of %s at %s", user, nodeID)
	return nil
}

// 包容网关：只走条件成立的分支，汇聚等齐被激活的那几条；都不成立走默认分支。
const inclusiveGraph = `{"nodes":[
	{"id":"start","type":"start"},
	{"id":"fork","type":"inclusiveGateway","join":"join"},
	` + `{"id":"a","type":"userTask","name":"a","assignee":{"rules":[{"kind":"user","userIds":["ua"]}]}},
	{"id":"b","type":"userTask","name":"b","assignee":{"rules":[{"kind":"user","userIds":["ub"]}]}},
	{"id":"c","type":"userTask","name":"c","assignee":{"rules":[{"kind":"user","userIds":["uc"]}]}},
	{"id":"join","type":"inclusiveGateway"},
	{"id":"last","type":"userTask","name":"last","assignee":{"rules":[{"kind":"user","userIds":["ul"]}]}},
	{"id":"end","type":"end"}],
	"edges":[{"id":"e0","source":"start","target":"fork"},
	{"id":"ea","source":"fork","target":"a","condition":"form.amount > 100","priority":1},
	{"id":"eb","source":"fork","target":"b","condition":"form.urgent == true","priority":2},
	{"id":"ec","source":"fork","target":"c","isDefault":true},
	{"id":"ja","source":"a","target":"join"},{"id":"jb","source":"b","target":"join"},{"id":"jc","source":"c","target":"join"},
	{"id":"e1","source":"join","target":"last"},{"id":"e2","source":"last","target":"end"}]}`

func TestInclusiveGateway(t *testing.T) {
	cases := map[string]struct {
		form map[string]any
		want []string
	}{
		"two branches":   {map[string]any{"amount": 500.0, "urgent": true}, []string{"a", "b"}},
		"one branch":     {map[string]any{"amount": 500.0, "urgent": false}, []string{"a"}},
		"default branch": {map[string]any{"amount": 1.0}, []string{"c"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := start(t, inclusiveGraph, c.form, nil)
			if got := h.pendingNodes(); !slices.Equal(got, c.want) {
				t.Fatalf("activated = %v, want %v", got, c.want)
			}
			for i, node := range c.want {
				h.approve(node, "u"+node)
				// 最后一条分支走完之前不得越过汇聚。
				if i < len(c.want)-1 && slices.Contains(h.pendingNodes(), "last") {
					t.Fatalf("join released before all activated branches arrived")
				}
			}
			if got := h.pendingNodes(); !slices.Equal(got, []string{"last"}) {
				t.Fatalf("after join pending = %v, want [last]", got)
			}
			h.approve("last", "ul")
			h.wantStatus(InstanceCompleted)
		})
	}
}

func TestInclusiveGatewayValidation(t *testing.T) {
	// 包容分叉须配对汇聚、须有且只有一条默认分支。
	raw := `{"nodes":[{"id":"start","type":"start"},{"id":"fork","type":"inclusiveGateway"},
		` + userTask("a", `"u"`) + `,` + userTask("b", `"u"`) + `,{"id":"end","type":"end"}],
		"edges":[{"id":"e0","source":"start","target":"fork"},
		{"id":"ea","source":"fork","target":"a","condition":"true"},{"id":"eb","source":"fork","target":"b","condition":"true"},
		{"id":"e1","source":"a","target":"end"},{"id":"e2","source":"b","target":"end"}]}`
	g, err := ParseGraph([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	codes := map[string]bool{}
	for _, issue := range g.Validate() {
		codes[issue.Code] = true
	}
	for _, want := range []string{"DEFAULT_BRANCH", "JOIN_MISSING"} {
		if !codes[want] {
			t.Errorf("Validate() codes = %v, missing %s", codes, want)
		}
	}
}

func waitGraph(node string) string {
	return `{"nodes":[{"id":"start","type":"start"},` + userTask("a", `"ua"`) + `,` + node + `,` + userTask("b", `"ub"`) + `,
		{"id":"end","type":"end"}],"edges":[{"id":"e0","source":"start","target":"a"},{"id":"e1","source":"a","target":"w"},
		{"id":"e2","source":"w","target":"b"},{"id":"e3","source":"b","target":"end"}],"settings":{"admins":["adm"]}}`
}

func TestTimerNode(t *testing.T) {
	t.Run("delay", func(t *testing.T) {
		h := start(t, waitGraph(`{"id":"w","type":"timer","name":"w","timer":{"mode":"delay","duration":{"value":2,"unit":"hour"}}}`), nil, nil)
		h.approve("a", "ua")
		j := h.job(JobTimer)
		if d := time.Until(j.DueTime); d < 119*time.Minute || d > 121*time.Minute {
			t.Fatalf("timer due in %v, want ~2h", d)
		}
		if nodes := h.pendingNodes(); len(nodes) != 0 {
			t.Fatalf("pending while waiting = %v", nodes)
		}
		h.fire(j)
		h.fire(j) // 重复领取：位置已离开，不再生效
		if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"b"}) {
			t.Fatalf("pending after timer = %v", nodes)
		}
	})
	t.Run("until in the past is skipped", func(t *testing.T) {
		h := start(t, waitGraph(`{"id":"w","type":"timer","timer":{"mode":"until","field":"at"}}`), map[string]any{"at": "2000-01-01T00:00:00Z"}, nil)
		h.approve("a", "ua")
		if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"b"}) {
			t.Fatalf("pending = %v, want [b]", nodes)
		}
	})
	t.Run("signal", func(t *testing.T) {
		h := start(t, waitGraph(`{"id":"w","type":"timer","timer":{"mode":"signal","signal":"paid"}}`), nil, nil)
		ctx := context.Background()
		if err := h.exec().Signal(ctx, "paid", nil); !errors.Is(err, ErrSignalNotAwaited) {
			t.Fatalf("Signal(before arriving) error = %v, want ErrSignalNotAwaited", err)
		}
		h.approve("a", "ua")
		if err := h.exec().Signal(ctx, "other", nil); !errors.Is(err, ErrSignalNotAwaited) {
			t.Fatalf("Signal(wrong name) error = %v", err)
		}
		if err := h.exec().Signal(ctx, "paid", map[string]any{"paid": true}); err != nil {
			t.Fatalf("Signal() error = %v", err)
		}
		if !h.s.VarsChanged || h.s.Instance.Variables["paid"] != true || !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("after signal: vars %v pending %v", h.s.Instance.Variables, h.pendingNodes())
		}
	})
}

func TestServiceNode(t *testing.T) {
	node := `{"id":"w","type":"serviceTask","name":"w","service":{"webhookId":"018f3e1a-0a00-7000-8000-0000000000aa"}}`
	t.Run("done merges variables", func(t *testing.T) {
		h := start(t, waitGraph(node), map[string]any{"x": 1.0}, nil)
		h.approve("a", "ua")
		j := h.job(JobService)
		if j.Payload["webhookId"] != "018f3e1a-0a00-7000-8000-0000000000aa" || len(h.pendingNodes()) != 0 {
			t.Fatalf("service job = %+v pending %v", j, h.pendingNodes())
		}
		if err := h.exec().ServiceDone(context.Background(), j, map[string]any{"ticket": "T-1"}); err != nil {
			t.Fatalf("ServiceDone() error = %v", err)
		}
		if h.s.Instance.Variables["ticket"] != "T-1" || h.s.Instance.Variables["x"] != 1.0 || !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("after service: vars %v pending %v", h.s.Instance.Variables, h.pendingNodes())
		}
	})
	t.Run("dead then skipped by admin", func(t *testing.T) {
		h := start(t, waitGraph(node), nil, nil)
		h.approve("a", "ua")
		j := h.job(JobService)
		x := h.exec()
		if err := x.JobDead(context.Background(), j, "boom"); err != nil {
			t.Fatalf("JobDead() error = %v", err)
		}
		if len(h.s.Notices) != 1 || h.s.Notices[0].Kind != NoticeServiceFailed || !slices.Equal(h.s.Notices[0].Recipients, []string{"adm"}) {
			t.Fatalf("notices = %+v", h.s.Notices)
		}
		if err := h.exec().SkipWait(context.Background(), j, "adm", "manual"); err != nil {
			t.Fatalf("SkipWait() error = %v", err)
		}
		if err := h.exec().SkipWait(context.Background(), j, "adm", "again"); !errors.Is(err, ErrTaskNotPending) {
			t.Fatalf("SkipWait(again) error = %v, want ErrTaskNotPending", err)
		}
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
}

func TestSubProcessNode(t *testing.T) {
	node := `{"id":"w","type":"subProcess","name":"w","subProcess":{"definitionKey":"child_flow"}}`
	for name, c := range map[string]struct {
		child  int32
		parent int32
	}{
		"child completed": {InstanceCompleted, InstanceRunning},
		"child rejected":  {InstanceRejected, InstanceRejected},
		"child cancelled": {InstanceCancelled, InstanceTerminated},
	} {
		t.Run(name, func(t *testing.T) {
			h := start(t, waitGraph(node), nil, nil)
			h.approve("a", "ua")
			if len(h.s.Spawns) != 1 || h.s.Spawns[0].DefinitionKey != "child_flow" {
				t.Fatalf("spawns = %+v", h.s.Spawns)
			}
			resume := &Job{Kind: JobResume, NodeID: "w", TokenID: h.s.Spawns[0].TokenID, Payload: map[string]any{"child": "c1", "status": float64(c.child)}}
			h.fire(resume)
			h.wantStatus(c.parent)
			if c.parent == InstanceRunning && !slices.Equal(h.pendingNodes(), []string{"b"}) {
				t.Fatalf("pending = %v", h.pendingNodes())
			}
		})
	}
	t.Run("child reports to its parent and cannot be cancelled alone", func(t *testing.T) {
		h := &harness{t: t, g: mustGraph(t, waitGraph(`{"id":"w","type":"cc","assignee":{"rules":[{"kind":"initiator"}]}}`)), dir: &dirStub{}, s: &State{Instance: Instance{
			ID: "child", Initiator: "init", Status: InstanceRunning, ParentInstanceID: "parent", ParentNodeID: "sub", ParentTokenID: "tk",
		}}}
		ctx := context.Background()
		if err := h.exec().Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.exec().Cancel("init", ""); !errors.Is(err, ErrInstanceForbidden) {
			t.Fatalf("Cancel(child) error = %v, want ErrInstanceForbidden", err)
		}
		h.fire(&Job{Kind: JobStop})
		h.wantStatus(InstanceTerminated)
		j := h.job(JobResume)
		if j.InstanceID != "parent" || j.NodeID != "sub" || j.TokenID != "tk" || j.Payload["status"] != InstanceTerminated {
			t.Fatalf("resume job = %+v", j)
		}
	})
}

func TestTaskTimeout(t *testing.T) {
	graph := func(action, extra string) string {
		return `{"nodes":[{"id":"start","type":"start"},
		{"id":"a","type":"userTask","name":"a","assignee":{"rules":[{"kind":"user","userIds":["ua"]}]},
		 "timeout":{"value":3,"unit":"hour","action":"` + action + `"` + extra + `}},
		` + userTask("b", `"ub"`) + `,{"id":"end","type":"end"}],
		"edges":[{"id":"e0","source":"start","target":"a"},{"id":"e1","source":"a","target":"b"},{"id":"e2","source":"b","target":"end"}],
		"settings":{"admins":["adm"]}}`
	}
	t.Run("approve", func(t *testing.T) {
		h := start(t, graph(TimeoutApprove, `,"remind":{"value":1,"unit":"hour"}`), nil, nil)
		timeout, remind := h.job(JobTimeout), h.job(JobRemind)
		if d := timeout.DueTime.Sub(remind.DueTime); d != time.Hour {
			t.Fatalf("remind is %v before the timeout, want 1h", d)
		}
		if h.s.Tasks[0].DueTime != timeout.DueTime {
			t.Fatalf("task due %v, job due %v", h.s.Tasks[0].DueTime, timeout.DueTime)
		}
		h.fire(remind)
		if len(h.s.Notices) != 1 || h.s.Notices[0].Kind != NoticeTaskRemind {
			t.Fatalf("remind notices = %+v", h.s.Notices)
		}
		h.fire(timeout)
		if !slices.Equal(h.pendingNodes(), []string{"b"}) || h.seat("a", "ua").Status != AssigneeCancelled {
			t.Fatalf("after timeout pending %v", h.pendingNodes())
		}
		h.fire(timeout) // 任务已了结：不再生效
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("stale timeout advanced the flow: %v", h.pendingNodes())
		}
	})
	t.Run("auto-approve blocked by the business guard falls back to a reminder", func(t *testing.T) {
		h := start(t, graph(TimeoutApprove, ""), nil, nil)
		blocked := errors.BadRequest("FORM", "field must be filled in")
		x := h.exec().WithApproveGuard(func(context.Context, *Node) error { return blocked })
		if err := x.Fire(context.Background(), h.job(JobTimeout)); err != nil {
			t.Fatalf("Fire() error = %v", err)
		}
		if _, users := h.pending("a"); !slices.Equal(users, []string{"ua"}) || len(h.s.Notices) != 1 || h.s.Notices[0].Kind != NoticeTaskRemind {
			t.Fatalf("after a blocked auto-approval: pending %v notices %+v", users, h.s.Notices)
		}
		if d := h.s.Histories[len(h.s.Histories)-1].Detail; d["action"] != TimeoutRemind || d["blocked"] != true {
			t.Fatalf("history detail = %v", d)
		}
	})
	t.Run("reject", func(t *testing.T) {
		h := start(t, graph(TimeoutReject, ""), nil, nil)
		h.fire(h.job(JobTimeout))
		h.wantStatus(InstanceRejected)
	})
	t.Run("admin", func(t *testing.T) {
		h := start(t, graph(TimeoutAdmin, ""), nil, nil)
		h.fire(h.job(JobTimeout))
		if _, users := h.pending("a"); !slices.Equal(users, []string{"adm"}) {
			t.Fatalf("after escalation assignees = %v", users)
		}
	})
	t.Run("admin unavailable falls back to remind", func(t *testing.T) {
		h := start(t, graph(TimeoutAdmin, ""), nil, &dirStub{inactive: map[string]bool{"adm": true}})
		h.fire(h.job(JobTimeout))
		if _, users := h.pending("a"); !slices.Equal(users, []string{"ua"}) || len(h.s.Notices) != 1 {
			t.Fatalf("assignees = %v notices = %+v", users, h.s.Notices)
		}
	})
	t.Run("handled before due", func(t *testing.T) {
		h := start(t, graph(TimeoutReject, ""), nil, nil)
		j := h.job(JobTimeout)
		h.approve("a", "ua")
		h.fire(j)
		h.wantStatus(InstanceRunning)
	})
}

func collabGraph(mode, users string) string {
	return `{"nodes":[{"id":"start","type":"start"},
		{"id":"a","type":"userTask","name":"a","assignee":{"mode":"` + mode + `","rules":[{"kind":"user","userIds":[` + users + `]}]}},
		` + userTask("b", `"ub"`) + `,{"id":"end","type":"end"}],
		"edges":[{"id":"e0","source":"start","target":"a"},{"id":"e1","source":"a","target":"b"},{"id":"e2","source":"b","target":"end"}]}`
}

func TestAddSign(t *testing.T) {
	ctx := context.Background()
	t.Run("before: returns to the owner after everyone agrees", func(t *testing.T) {
		h := start(t, collabGraph(ModeAny, `"u1"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1", "x2"}, OriginBefore, "看一下"); err != nil {
			t.Fatalf("AddSign() error = %v", err)
		}
		if _, users := h.pending("a"); !slices.Equal(users, []string{"x1", "x2"}) || h.seat("a", "u1").Status != AssigneeSuspended {
			t.Fatalf("after add-sign pending = %v", users)
		}
		if err := h.exec().Approve(ctx, id, "u1", ""); !errors.Is(err, ErrTaskNotPending) {
			t.Fatalf("Approve(suspended owner) error = %v, want ErrTaskNotPending", err)
		}
		h.approve("a", "x1")
		if h.seat("a", "u1").Status != AssigneeSuspended {
			t.Fatal("owner resumed before all add-signers agreed")
		}
		h.approve("a", "x2")
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u1"}) {
			t.Fatalf("owner not resumed: %v", users)
		}
		h.approve("a", "u1")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
	t.Run("after: owner's approval takes effect once they agree", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"u1","u2"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginAfter, "同意，请复核"); err != nil {
			t.Fatalf("AddSign() error = %v", err)
		}
		if h.seat("a", "u1").Status != AssigneeApproved {
			t.Fatal("owner seat should be approved")
		}
		h.approve("a", "u2")
		if !slices.Equal(h.pendingNodes(), []string{"a"}) {
			t.Fatal("task passed while an add-signer was still pending")
		}
		h.approve("a", "x1")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
	t.Run("after in sequential mode hands over to the next in line", func(t *testing.T) {
		h := start(t, collabGraph(ModeSequential, `"u1","u2"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginAfter, ""); err != nil {
			t.Fatal(err)
		}
		if h.seat("a", "u2").Status != AssigneeQueued {
			t.Fatal("next in line activated before the add-signer agreed")
		}
		h.approve("a", "x1")
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u2"}) {
			t.Fatalf("pending = %v, want [u2]", users)
		}
	})
	t.Run("add-signer rejects the task", func(t *testing.T) {
		h := start(t, collabGraph(ModeAny, `"u1"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginBefore, ""); err != nil {
			t.Fatal(err)
		}
		if err := h.exec().Return(ctx, id, "x1", "a", ""); !errors.Is(err, ErrTaskTargetInvalid) {
			t.Fatalf("Return(by add-signer) error = %v, want ErrTaskTargetInvalid", err)
		}
		if err := h.exec().Reject(ctx, id, "x1", "不行"); err != nil {
			t.Fatal(err)
		}
		h.wantStatus(InstanceRejected)
	})
	t.Run("invalid targets", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"u1","u2"`), nil, &dirStub{inactive: map[string]bool{"gone": true}})
		id, _ := h.pending("a")
		for name, users := range map[string][]string{"self": {"u1"}, "already seated": {"u2"}, "inactive": {"gone"}, "none": nil} {
			if err := h.exec().AddSign(ctx, id, "u1", users, OriginBefore, ""); !errors.Is(err, ErrTaskTargetInvalid) {
				t.Errorf("AddSign(%s) error = %v, want ErrTaskTargetInvalid", name, err)
			}
		}
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, "sideways", ""); !errors.Is(err, ErrTaskTargetInvalid) {
			t.Errorf("AddSign(bad position) error = %v", err)
		}
	})
	t.Run("transferring an add-signed seat keeps the relation", func(t *testing.T) {
		h := start(t, collabGraph(ModeAny, `"u1"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginBefore, ""); err != nil {
			t.Fatal(err)
		}
		if err := h.exec().Transfer(ctx, id, "x1", "x9", ""); err != nil {
			t.Fatalf("Transfer() error = %v", err)
		}
		h.approve("a", "x9")
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u1"}) {
			t.Fatalf("owner not resumed after the transferee agreed: %v", users)
		}
	})
}

func TestDelegate(t *testing.T) {
	ctx := context.Background()
	for name, agree := range map[string]bool{"agrees": true, "disagrees": false} {
		t.Run("delegate "+name, func(t *testing.T) {
			h := start(t, collabGraph(ModeAny, `"u1"`), nil, nil)
			id, _ := h.pending("a")
			if err := h.exec().Delegate(ctx, id, "u1", "x1", "帮忙看看"); err != nil {
				t.Fatalf("Delegate() error = %v", err)
			}
			if err := h.exec().Delegate(ctx, id, "x1", "x2", ""); !errors.Is(err, ErrTaskTargetInvalid) {
				t.Fatalf("Delegate(by delegate) error = %v, want ErrTaskTargetInvalid", err)
			}
			var err error
			if agree {
				err = h.exec().Approve(ctx, id, "x1", "可以")
			} else {
				err = h.exec().Reject(ctx, id, "x1", "有风险")
			}
			if err != nil {
				t.Fatal(err)
			}
			// 被委派人的意见不决定任务：回到委派人手上。
			h.wantStatus(InstanceRunning)
			if _, users := h.pending("a"); !slices.Equal(users, []string{"u1"}) {
				t.Fatalf("pending = %v, want [u1]", users)
			}
			if got := h.s.Histories[len(h.s.Histories)-1]; got.Action != ActionReply || got.Detail["agree"] != agree {
				t.Fatalf("history = %+v", got)
			}
		})
	}
}

type proxyStub map[string]string

func (p proxyStub) ActiveProxies(_ context.Context, userIDs []string, _ time.Time) (map[string]string, error) {
	out := map[string]string{}
	for _, u := range userIDs {
		if d, ok := p[u]; ok {
			out[u] = d
		}
	}
	return out, nil
}

func TestProxies(t *testing.T) {
	g := mustGraph(t, collabGraph(ModeAll, `"u1","u2","u3"`))
	dir := &dirStub{inactive: map[string]bool{"away": true}}
	s := &State{Instance: Instance{ID: "inst", Initiator: "init", Status: InstanceRunning}}
	// u1 → p1；u2 的代理人不可用，仍由本人处理；u3 的代理人 p1 已替 u1 代理，u3 仍由本人处理
	//（否则两个人的会签席位并成一个）。
	x := NewExecution(g, s, dir, time.Now()).WithProxies(proxyStub{"u1": "p1", "u2": "away", "u3": "p1"})
	if err := x.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, a := range s.Tasks[0].Assignees {
		got = append(got, a.UserID+"<"+a.OnBehalf)
	}
	if !slices.Equal(got, []string{"p1<u1", "u2<", "u3<"}) {
		t.Fatalf("seats = %v", got)
	}
	// 代理人就在这批人里、或是发起人：不走代理（会签不少人、不能借代理自己审批自己）。
	s2 := &State{Instance: Instance{ID: "inst", Initiator: "init", Status: InstanceRunning}}
	x2 := NewExecution(mustGraph(t, collabGraph(ModeAll, `"u1","u2","u3"`)), s2, &dirStub{}, time.Now()).
		WithProxies(proxyStub{"u1": "u2", "u3": "init"})
	if err := x2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	got = nil
	for _, a := range s2.Tasks[0].Assignees {
		got = append(got, a.UserID+"<"+a.OnBehalf)
	}
	if !slices.Equal(got, []string{"u1<", "u2<", "u3<"}) {
		t.Fatalf("seats with colliding proxies = %v", got)
	}
}

func TestHandover(t *testing.T) {
	ctx := context.Background()
	handover := func(h *harness, from, to string, want int) {
		h.t.Helper()
		if moved, err := h.exec().Handover(ctx, from, to, "adm", "离职"); err != nil || moved != want {
			h.t.Fatalf("Handover(%s→%s) = %d, %v; want %d", from, to, moved, err, want)
		}
	}
	t.Run("suspended owner: the add-signer's result returns to the successor", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"u1","u2"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginBefore, ""); err != nil {
			t.Fatal(err)
		}
		handover(h, "u1", "n1", 1)
		h.approve("a", "x1")
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u2", "n1"}) {
			t.Fatalf("pending = %v, want [u2 n1]", users)
		}
		handover(h, "nobody", "n1", 0)
	})
	t.Run("successor already on the task keeps both seats (no auto-approval)", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"u1","u2"`), nil, nil)
		handover(h, "u1", "u2", 1)
		h.approve("a", "u2")
		if !slices.Equal(h.pendingNodes(), []string{"a"}) {
			t.Fatal("one approval must not satisfy two countersign seats")
		}
		h.approve("a", "u2")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
	t.Run("sequential: the queued successor is not left with nobody able to act", func(t *testing.T) {
		h := start(t, collabGraph(ModeSequential, `"u1","u2"`), nil, nil)
		handover(h, "u1", "u2", 1)
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u2"}) {
			t.Fatalf("pending = %v, want [u2]", users)
		}
		h.approve("a", "u2")
		h.approve("a", "u2")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
	t.Run("sole assignee delegated to the successor: still needs a real approval", func(t *testing.T) {
		h := start(t, collabGraph(ModeAny, `"u1"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().Delegate(ctx, id, "u1", "x1", ""); err != nil {
			t.Fatal(err)
		}
		handover(h, "u1", "x1", 1)
		if !slices.Equal(h.pendingNodes(), []string{"a"}) {
			t.Fatal("handover must not approve the task by itself")
		}
		h.approve("a", "x1") // 以被委派人的席位给出意见
		h.approve("a", "x1") // 再以接手的席位做决定
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
	t.Run("handing over an add-signed seat does not strand its owner", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"u1","u2"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "u1", []string{"x1"}, OriginBefore, ""); err != nil {
			t.Fatal(err)
		}
		handover(h, "x1", "u2", 1)
		h.approve("a", "u2")
		h.approve("a", "u2")
		if _, users := h.pending("a"); !slices.Equal(users, []string{"u1"}) {
			t.Fatalf("owner not resumed: %v", users)
		}
		h.approve("a", "u1")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
}

// 嵌套加签：下级的后加签没处理完，上一级不算了结——既不会卡死，也不会被绕过。
func TestNestedAddSign(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{ModeAll, ModeAny} {
		t.Run(mode, func(t *testing.T) {
			h := start(t, collabGraph(mode, `"a1"`), nil, nil)
			id, _ := h.pending("a")
			if err := h.exec().AddSign(ctx, id, "a1", []string{"x", "z"}, OriginBefore, ""); err != nil {
				t.Fatal(err)
			}
			if err := h.exec().AddSign(ctx, id, "x", []string{"y"}, OriginAfter, ""); err != nil {
				t.Fatal(err)
			}
			h.approve("a", "z")
			if h.seat("a", "a1").Status != AssigneeSuspended {
				t.Fatal("owner resumed while a nested after-signer was still pending")
			}
			h.approve("a", "y")
			if _, users := h.pending("a"); !slices.Equal(users, []string{"a1"}) {
				t.Fatalf("pending = %v, want [a1]", users)
			}
			h.approve("a", "a1")
			if !slices.Equal(h.pendingNodes(), []string{"b"}) {
				t.Fatalf("pending = %v", h.pendingNodes())
			}
		})
	}
	t.Run("after-sign chain on an original seat completes the task", func(t *testing.T) {
		h := start(t, collabGraph(ModeAll, `"a1"`), nil, nil)
		id, _ := h.pending("a")
		if err := h.exec().AddSign(ctx, id, "a1", []string{"x"}, OriginAfter, ""); err != nil {
			t.Fatal(err)
		}
		if err := h.exec().AddSign(ctx, id, "x", []string{"y"}, OriginAfter, ""); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(h.pendingNodes(), []string{"a"}) {
			t.Fatal("task passed before the chain finished")
		}
		h.approve("a", "y")
		if !slices.Equal(h.pendingNodes(), []string{"b"}) {
			t.Fatalf("pending = %v", h.pendingNodes())
		}
	})
}

// 改派后时限重新起算：旧一轮的超时 / 提醒作业作废。
func TestTimeoutRearmedOnReassign(t *testing.T) {
	h := start(t, `{"nodes":[{"id":"start","type":"start"},
		{"id":"a","type":"userTask","name":"a","assignee":{"rules":[{"kind":"user","userIds":["ua"]}]},
		 "timeout":{"value":3,"unit":"hour","action":"approve","remind":{"value":1,"unit":"hour"}}},
		`+userTask("b", `"ub"`)+`,{"id":"end","type":"end"}],
		"edges":[{"id":"e0","source":"start","target":"a"},{"id":"e1","source":"a","target":"b"},{"id":"e2","source":"b","target":"end"}]}`, nil, nil)
	oldTimeout, oldRemind := h.job(JobTimeout), h.job(JobRemind)
	id, _ := h.pending("a")
	x := NewExecution(h.g, h.s, h.dir, time.Now().Add(2*time.Hour))
	if err := x.Reassign(context.Background(), id, "adm", []string{"un"}, "换人"); err != nil {
		t.Fatalf("Reassign() error = %v", err)
	}
	fresh := h.job(JobTimeout)
	if !fresh.DueTime.After(oldTimeout.DueTime) || h.s.Tasks[0].DueTime != fresh.DueTime {
		t.Fatalf("timeout not re-armed: old %v new %v task %v", oldTimeout.DueTime, fresh.DueTime, h.s.Tasks[0].DueTime)
	}
	h.fire(oldRemind)
	if len(h.s.Notices) != 0 {
		t.Fatalf("stale reminder fired: %+v", h.s.Notices)
	}
	h.fire(oldTimeout)
	if !slices.Equal(h.pendingNodes(), []string{"a"}) {
		t.Fatal("stale timeout auto-approved the reassigned task")
	}
	h.fire(fresh)
	if !slices.Equal(h.pendingNodes(), []string{"b"}) {
		t.Fatalf("pending = %v", h.pendingNodes())
	}
}

func TestCalendarAdd(t *testing.T) {
	cal, err := NewCalendar(CalendarConfig{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5},
		Segments: []CalendarSegment{{"09:00", "12:00"}, {"13:00", "18:00"}},
		Holidays: []string{"2026-10-01"}, Workdays: []string{"2026-10-10"},
	})
	if err != nil {
		t.Fatalf("NewCalendar() error = %v", err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	at := func(s string) time.Time {
		v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := []struct {
		name, from string
		d          Duration
		want       string
	}{
		{"within a segment", "2026-09-28 10:00", Duration{Value: 90, Unit: UnitMinute, WorkTime: true}, "2026-09-28 11:30"},
		{"across lunch", "2026-09-28 11:00", Duration{Value: 2, Unit: UnitHour, WorkTime: true}, "2026-09-28 14:00"},
		{"across the night", "2026-09-28 17:00", Duration{Value: 2, Unit: UnitHour, WorkTime: true}, "2026-09-29 10:00"},
		{"starts after hours", "2026-09-28 20:00", Duration{Value: 1, Unit: UnitHour, WorkTime: true}, "2026-09-29 10:00"},
		{"across weekend and holiday", "2026-09-30 17:30", Duration{Value: 1, Unit: UnitHour, WorkTime: true}, "2026-10-02 09:30"},
		{"extra working saturday", "2026-10-09 17:00", Duration{Value: 2, Unit: UnitHour, WorkTime: true}, "2026-10-10 10:00"},
		{"working days", "2026-09-30 15:00", Duration{Value: 2, Unit: UnitDay, WorkTime: true}, "2026-10-05 15:00"},
		{"natural time", "2026-09-30 17:30", Duration{Value: 1, Unit: UnitDay}, "2026-10-01 17:30"},
	}
	for _, c := range cases {
		if got := cal.Add(at(c.from), c.d); !got.Equal(at(c.want)) {
			t.Errorf("%s: Add(%s, %+v) = %s, want %s", c.name, c.from, c.d, got.In(loc).Format("2006-01-02 15:04"), c.want)
		}
	}
	// 没有日历时按自然时间。
	var none *Calendar
	if got := none.Add(at("2026-09-28 17:00"), Duration{Value: 2, Unit: UnitHour, WorkTime: true}); !got.Equal(at("2026-09-28 19:00")) {
		t.Errorf("nil calendar Add = %v", got)
	}
	for name, bad := range map[string]CalendarConfig{
		"timezone": {Timezone: "Mars/Olympus", Weekdays: []int32{1}, Segments: []CalendarSegment{{"09:00", "18:00"}}},
		"weekday":  {Timezone: "UTC", Weekdays: []int32{7}, Segments: []CalendarSegment{{"09:00", "18:00"}}},
		"overlap":  {Timezone: "UTC", Weekdays: []int32{1}, Segments: []CalendarSegment{{"09:00", "12:00"}, {"11:00", "18:00"}}},
		"date":     {Timezone: "UTC", Weekdays: []int32{1}, Segments: []CalendarSegment{{"09:00", "18:00"}}, Holidays: []string{"10/01"}},
	} {
		if _, err := NewCalendar(bad); err == nil {
			t.Errorf("NewCalendar(bad %s) must fail", name)
		}
	}
}

func TestWaitNodeValidation(t *testing.T) {
	for name, c := range map[string]struct{ node, code string }{
		"timer without config":   {`{"id":"w","type":"timer"}`, "TIMER_INVALID"},
		"timer bad signal":       {`{"id":"w","type":"timer","timer":{"mode":"signal","signal":"a b"}}`, "TIMER_INVALID"},
		"timer zero delay":       {`{"id":"w","type":"timer","timer":{"mode":"delay","duration":{"value":0,"unit":"hour"}}}`, "TIMER_INVALID"},
		"service bad webhook":    {`{"id":"w","type":"serviceTask","service":{"webhookId":"nope"}}`, "SERVICE_INVALID"},
		"sub-process bad key":    {`{"id":"w","type":"subProcess","subProcess":{"definitionKey":"Bad Key"}}`, "SUBPROCESS_INVALID"},
		"timeout on a cc node":   {`{"id":"w","type":"cc","assignee":{"rules":[{"kind":"initiator"}]},"timeout":{"value":1,"unit":"hour","action":"remind"}}`, "TIMEOUT_NOT_ALLOWED"},
		"timeout bad action":     {`{"id":"w","type":"userTask","assignee":{"rules":[{"kind":"initiator"}]},"timeout":{"value":1,"unit":"hour","action":"explode"}}`, "TIMEOUT_INVALID"},
		"remind in another unit": {`{"id":"w","type":"userTask","assignee":{"rules":[{"kind":"initiator"}]},"timeout":{"value":2,"unit":"hour","action":"remind","remind":{"value":30,"unit":"minute"}}}`, "TIMEOUT_INVALID"},
		"remind not before due":  {`{"id":"w","type":"userTask","assignee":{"rules":[{"kind":"initiator"}]},"timeout":{"value":1,"unit":"hour","action":"remind","remind":{"value":2,"unit":"hour"}}}`, "TIMEOUT_INVALID"},
	} {
		g, err := ParseGraph([]byte(waitGraph(c.node)))
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(g.Validate(), func(i Issue) bool { return i.Code == c.code }) {
			t.Errorf("%s: Validate() = %+v, want %s", name, g.Validate(), c.code)
		}
	}
	// 只经过等待节点、不经过人工任务的回路仍被拒绝（否则自动任务会无限循环调用）。
	loop := `{"nodes":[{"id":"start","type":"start"},{"id":"gw","type":"exclusiveGateway"},
		{"id":"w","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"minute"}}},{"id":"end","type":"end"}],
		"edges":[{"id":"e0","source":"start","target":"gw"},{"id":"e1","source":"gw","target":"w","condition":"true","priority":1},
		{"id":"e2","source":"gw","target":"end","isDefault":true},{"id":"e3","source":"w","target":"gw"}]}`
	g, _ := ParseGraph([]byte(loop))
	if !slices.ContainsFunc(g.Validate(), func(i Issue) bool { return i.Code == "AUTO_LOOP" }) {
		t.Errorf("loop through a timer only: Validate() = %+v, want AUTO_LOOP", g.Validate())
	}
}
