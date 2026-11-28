package biz

import (
	"context"
	stderrors "errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// blockedTask 一个解析不出处理人的人工任务（规则指向不可用的用户 gone）。policy 是 onEmpty 的原文
// （含前导逗号；空 = 缺省）。
func blockedTask(id, policy string) string {
	return `{"id":"` + id + `","type":"userTask","name":"` + id + `","assignee":{"rules":[{"kind":"user","userIds":["gone"]}]` + policy + `}}`
}

// goneDir gone 不可用的组织目录；inactive 是另外不可用的用户。
func goneDir(inactive map[string]bool) *dirStub {
	off := maps.Clone(inactive)
	if off == nil {
		off = map[string]bool{}
	}
	off["gone"] = true
	return &dirStub{inactive: off}
}

// wantNoAssignee 断言 err 是「人工任务解析不出处理人被拦住」（按原因码匹配 ErrNoAssignee），且说明里带节点 ID 与
// want 这句话。
func wantNoAssignee(t *testing.T, err error, node, want string) {
	t.Helper()
	if !errors.Is(err, ErrNoAssignee) {
		t.Fatalf("error = %v, want ErrNoAssignee", err)
	}
	var ke *errors.Error
	if !stderrors.As(err, &ke) {
		t.Fatalf("error = %v is not a business error", err)
	}
	for _, part := range []string{node, want} {
		if !strings.Contains(ke.Message, part) {
			t.Fatalf("error message %q does not mention %q", ke.Message, part)
		}
	}
}

// skippedForNoAssignee 报告状态里有没有「处理人为空被跳过」的流转历史。
func skippedForNoAssignee(s *State) bool {
	return slices.ContainsFunc(s.Histories, func(h *History) bool {
		return h.Action == ActionSkip && h.Detail["reason"] == SkipReasonNoAssignee
	})
}

// 人工任务解析不出处理人时的处置（onEmpty）：只有明确选了 skip 才跳过；缺省（没写）、block、以及没有
// 可用流程管理员的 admin（没设置，或都不可用）都拦住。拦住 = 发起以 ErrNoAssignee 失败：不建任务、
// 不留「跳过」记录、流程不算走完。
func TestEngine_NoAssigneePolicies(t *testing.T) {
	t.Parallel()
	const skip, block, admin = `,"onEmpty":"skip"`, `,"onEmpty":"block"`, `,"onEmpty":"admin"`
	cases := map[string]struct {
		policy   string
		admins   string // 流程管理员（JSON 数组）；空 = 没有流程管理员
		adminOff bool   // 流程管理员填了，但不可用
		message  string // 拦住时说明里应有的一句话；空 = 不拦住
	}{
		"default blocks":     {message: "must not be skipped"},
		"skip skips":         {policy: skip},
		"block blocks":       {policy: block, message: "must not be skipped"},
		"admin, none set":    {policy: admin, message: "no available admin"},
		"admin, none usable": {policy: admin, admins: `["adm"]`, adminOff: true, message: "no available admin"},
		"admin, usable":      {policy: admin, admins: `["adm"]`},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw := linearGraph(blockedTask("a", tt.policy))
			if tt.admins != "" {
				raw = strings.Replace(raw, `"edges"`, `"settings":{"admins":`+tt.admins+`},"edges"`, 1)
			}
			dir := goneDir(nil)
			dir.inactive["adm"] = tt.adminOff
			s := &State{Instance: Instance{ID: "inst", Initiator: "init", InitiatorDept: "d-team", Status: InstanceRunning}}
			err := NewExecution(mustGraph(t, raw), s, dir, time.Now()).Start(context.Background())
			if tt.message != "" {
				wantNoAssignee(t, err, `"a"`, tt.message)
				if len(s.Tasks) != 0 || s.Instance.Status != InstanceRunning || skippedForNoAssignee(s) {
					t.Fatalf("a blocked start left traces behind: %d tasks, status %d, skipped %v",
						len(s.Tasks), s.Instance.Status, skippedForNoAssignee(s))
				}
				return
			}
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if tt.admins == "" {
				if s.Instance.Status != InstanceCompleted || !skippedForNoAssignee(s) {
					t.Fatalf("status = %d, skipped = %v; want the node skipped and the flow completed",
						s.Instance.Status, skippedForNoAssignee(s))
				}
				return
			}
			if len(s.Tasks) != 1 || !slices.Equal(pendingUsers(s.Tasks[0]), []string{"adm"}) {
				t.Fatalf("tasks = %+v; want one task for the workflow admin", s.Tasks)
			}
		})
	}
}

// 节点没有名称时说明里用节点 ID：看报错的人知道是哪一个节点被拦住了。
func TestEngine_NoAssigneeNamesTheNode(t *testing.T) {
	t.Parallel()
	raw := linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["gone"]}],"onEmpty":"block"}}`)
	s := &State{Instance: Instance{ID: "inst", Initiator: "init", Status: InstanceRunning}}
	wantNoAssignee(t, NewExecution(mustGraph(t, raw), s, goneDir(nil), time.Now()).Start(context.Background()), `"a"`, "must not be skipped")
}

// 同意把流程推到下一个解析不出处理人的节点：这次同意以 ErrNoAssignee 失败，被拦的节点不建任务，
// 也不留「跳过」记录（用例层整体回滚，上一个任务仍待处理，见集成测试）。
func TestEngine_NoAssigneeOnApprove(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1"), blockedTask("b", `,"onEmpty":"block"`)), nil, goneDir(nil))
	id, _ := h.pending("a")
	wantNoAssignee(t, h.exec().Approve(context.Background(), id, "u1", "ok"), `"b"`, "must not be skipped")
	if nodes := h.pendingNodes(); slices.Contains(nodes, "b") {
		t.Fatalf("the blocked node got a task; pending = %v", nodes)
	}
	if h.s.Instance.Status != InstanceRunning || skippedForNoAssignee(h.s) {
		t.Fatalf("status = %d, skipped = %v; want the flow held before the blocked node", h.s.Instance.Status, skippedForNoAssignee(h.s))
	}
}

// 定时到期后才走到被拦的节点：作业执行以 ErrNoAssignee 失败——确定性的业务错误（4xx），作业执行器不
// 重试、直接转死信并通知流程管理员；管理员的「跳过等待节点」同样过不去，流程停在定时节点上。
func TestEngine_NoAssigneeOnTimer(t *testing.T) {
	t.Parallel()
	raw := `{"nodes":[{"id":"s","type":"start"},` + task("a", "any", "u1") +
		`,{"id":"w","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"minute"}}},` +
		blockedTask("b", `,"onEmpty":"block"`) + `,{"id":"e","type":"end"}],
		"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"w"},
		{"id":"e3","source":"w","target":"b"},{"id":"e4","source":"b","target":"e"}],"settings":{}}`
	h := start(t, raw, nil, goneDir(nil))
	h.approve("a", "u1")
	j := h.job(JobTimer)
	err := h.exec().Fire(context.Background(), j)
	wantNoAssignee(t, err, `"b"`, "must not be skipped")
	if !permanent(err) {
		t.Fatal("a blocked task is a deterministic failure: the job executor must dead-letter the job, not retry it")
	}
	if got := reasonOf(err); !strings.Contains(got, `"b"`) {
		t.Fatalf("reason shown to participants = %q, want it to name the blocked node", got)
	}
	// 管理员的「跳过等待节点」同样过不去。引擎在命令失败后留下的内存状态是脏的（线上整条命令回滚、
	// 下一条命令重新读库），所以换一个新实例再试。
	h = start(t, raw, nil, goneDir(nil))
	h.approve("a", "u1")
	wantNoAssignee(t, h.exec().SkipWait(context.Background(), h.job(JobTimer), "adm", "skip it"), `"b"`, "must not be skipped")
	if nodes := h.pendingNodes(); len(nodes) != 0 || h.s.Instance.Status != InstanceRunning {
		t.Fatalf("pending = %v, status = %d; want the flow still waiting at the timer node", nodes, h.s.Instance.Status)
	}
}

// 超时「自动同意」把流程推到被拦的节点：同样以 ErrNoAssignee 失败（超时作业转死信），不会替人同意后放行。
func TestEngine_NoAssigneeOnTimeout(t *testing.T) {
	t.Parallel()
	raw := `{"nodes":[{"id":"s","type":"start"},
		{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u1"]}]},"timeout":{"value":1,"unit":"hour","action":"approve"}},` +
		blockedTask("b", `,"onEmpty":"block"`) + `,{"id":"e","type":"end"}],
		"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"b"},{"id":"e3","source":"b","target":"e"}],"settings":{}}`
	h := start(t, raw, nil, goneDir(nil))
	err := h.exec().Fire(context.Background(), h.job(JobTimeout))
	wantNoAssignee(t, err, `"b"`, "must not be skipped")
	if !permanent(err) {
		t.Fatal("a blocked task is a deterministic failure: the job executor must dead-letter the job, not retry it")
	}
}

// 抄送节点没有收件人：照旧跳过（抄送不拦流程）。
func TestEngine_CCWithoutRecipientsIsSkipped(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(`{"id":"c","type":"cc","assignee":{"rules":[{"kind":"user","userIds":["gone"]}]}}`), nil, goneDir(nil))
	h.wantStatus(InstanceCompleted)
}

// 模拟运行遇到被拦的节点：标出该节点、到此为止，不报错；明确选了跳过的节点标 Skipped。
func TestSimulate_BlockedAndSkipped(t *testing.T) {
	t.Parallel()
	dir := goneDir(nil)
	path := func(res *SimResult) []string {
		var ids []string
		for _, st := range res.Steps {
			ids = append(ids, st.NodeID)
		}
		return ids
	}

	// a（u1）→ b 被拦住：后面的 c、e 不会经过。
	res, err := Simulate(context.Background(), mustGraph(t, linearGraph(
		task("a", "any", "u1"), blockedTask("b", `,"onEmpty":"block"`), userTask("c", `"u3"`))), Instance{Initiator: "init"}, dir, nil, nil)
	if err != nil {
		t.Fatalf("Simulate(blocked) error = %v", err)
	}
	if !slices.Equal(path(res), []string{"s", "a", "b"}) || res.Status != InstanceRunning {
		t.Fatalf("blocked: path = %v, status = %d; want s, a, b and still running", path(res), res.Status)
	}
	if st := res.Steps[2]; !st.Blocked || st.Skipped || st.Assignees != nil {
		t.Fatalf("blocked step = %+v; want Blocked and neither skipped nor assigned", st)
	}
	if st := res.Steps[1]; st.Blocked || st.Skipped || !slices.Equal(st.Assignees, []string{"u1"}) {
		t.Fatalf("step before the blocked one = %+v; want u1 assigned", st)
	}

	// a 明确选了跳过：标 Skipped，流程继续走完。
	res, err = Simulate(context.Background(), mustGraph(t, linearGraph(blockedTask("a", `,"onEmpty":"skip"`), userTask("b", `"u1"`))), Instance{Initiator: "init"}, dir, nil, nil)
	if err != nil {
		t.Fatalf("Simulate(skipped) error = %v", err)
	}
	if !slices.Equal(path(res), []string{"s", "a", "b", "e"}) || res.Status != InstanceCompleted {
		t.Fatalf("skipped: path = %v, status = %d; want the whole flow walked", path(res), res.Status)
	}
	if st := res.Steps[1]; !st.Skipped || st.Blocked || st.Assignees != nil {
		t.Fatalf("skipped step = %+v; want Skipped", st)
	}
}

// 草稿校验（保存草稿、发布、模拟运行、详情页的问题清单）：人工任务选了「转交流程管理员」却没有流程
// 管理员 → ADMINS_REQUIRED，指到那个节点。已发布版本的加载只跑结构校验，这样的存量版本照常能执行。
func TestDefinition_AdminFallbackNeedsAdmins(t *testing.T) {
	t.Parallel()
	const orphan = `{"nodes":[{"id":"s","type":"start"},{"id":"a","type":"userTask","name":"主管审批",` +
		`"assignee":{"rules":[{"kind":"deptLeader","level":1}],"onEmpty":"admin"}},{"id":"e","type":"end"}],` +
		`"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"e"}],"settings":{}}`
	uc := NewDefinitionUsecase(nil, nil, nil, nil, nil, nil)
	ctx := context.Background()
	_, issues, err := uc.validate(ctx, "orphan", []byte(orphan))
	if err != nil || len(issues) != 1 || issues[0].Code != "ADMINS_REQUIRED" || issues[0].NodeID != "a" {
		t.Fatalf("validate() = %+v, %v; want one ADMINS_REQUIRED issue on node a", issues, err)
	}
	// 结构校验不报：存量已发布版本不受影响（运行时由引擎拦住）。
	if _, issues, err := issuesOf([]byte(orphan)); err != nil || len(issues) != 0 {
		t.Fatalf("issuesOf() = %+v, %v; want no structural issue", issues, err)
	}
	// 设了流程管理员、换了处置、或只是抄送节点：没有这条问题。
	for name, raw := range map[string]string{
		"admins set": strings.Replace(orphan, `"settings":{}`, `"settings":{"admins":["adm"]}`, 1),
		"skips":      strings.Replace(orphan, `,"onEmpty":"admin"`, ``, 1),
		"blocks":     strings.Replace(orphan, `"onEmpty":"admin"`, `"onEmpty":"block"`, 1),
		"cc node":    strings.Replace(orphan, `"type":"userTask"`, `"type":"cc"`, 1),
	} {
		if _, issues, err := uc.validate(ctx, "orphan", []byte(raw)); err != nil || len(issues) != 0 {
			t.Errorf("%s: validate() = %+v, %v; want no issue", name, issues, err)
		}
	}
	// 流程图自己还有结构问题（处理人规则为空）时只报结构问题，不解读节点配置。
	_, issues, err = uc.validate(ctx, "orphan", []byte(linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[],"onEmpty":"admin"}}`)))
	if err != nil || !slices.ContainsFunc(issues, func(i Issue) bool { return i.Code == "ASSIGNEE_MISSING" }) ||
		slices.ContainsFunc(issues, func(i Issue) bool { return i.Code == "ADMINS_REQUIRED" }) {
		t.Fatalf("validate(incomplete node) = %+v, %v; want only structural issues", issues, err)
	}
}
