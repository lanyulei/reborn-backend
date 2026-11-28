package biz

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// dirStub 组织目录桩：inactive 中的用户视为不可用。
type dirStub struct {
	inactive map[string]bool
	roles    map[string][]string
	leaders  map[string][]string // deptID → 负责人
	parents  map[string]string   // deptID → 上级
}

func (d *dirStub) ActiveUserIDs(_ context.Context, ids []string) ([]string, error) {
	var out []string
	for _, id := range ids {
		if id != "" && !d.inactive[id] && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (d *dirStub) UserIDsByRoles(_ context.Context, codes []string) ([]string, error) {
	var out []string
	for _, c := range codes {
		out = append(out, d.roles[c]...)
	}
	return out, nil
}

func (d *dirStub) DeptLeaderIDs(_ context.Context, deptID string, level int, exclude string) ([]string, error) {
	for i := 1; i < level && deptID != ""; i++ {
		deptID = d.parents[deptID]
	}
	for ; deptID != ""; deptID = d.parents[deptID] {
		var out []string
		for _, u := range d.leaders[deptID] {
			if u != exclude && !d.inactive[u] {
				out = append(out, u)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, nil
}

func mustGraph(t *testing.T, raw string) *Graph {
	t.Helper()
	g, err := ParseGraph([]byte(raw))
	if err != nil {
		t.Fatalf("ParseGraph() error = %v", err)
	}
	if issues := g.Validate(); len(issues) > 0 {
		t.Fatalf("Validate() issues = %+v", issues)
	}
	return g
}

// run 构造一个新实例并发起；返回可继续执行命令的闭包。
type harness struct {
	t   *testing.T
	g   *Graph
	s   *State
	dir *dirStub
}

func start(t *testing.T, raw string, form map[string]any, dir *dirStub) *harness {
	t.Helper()
	if dir == nil {
		dir = &dirStub{}
	}
	h := &harness{t: t, g: mustGraph(t, raw), dir: dir, s: &State{Instance: Instance{
		ID: "inst", Initiator: "init", InitiatorDept: "d-team", Status: InstanceRunning, Variables: form,
	}}}
	if err := h.exec().Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return h
}

func (h *harness) exec() *Execution {
	h.s.Histories, h.s.Notices = nil, nil
	return NewExecution(h.g, h.s, h.dir, time.Now())
}

// pending 返回某节点上待处理任务的 ID 与当前可处理的用户。
func (h *harness) pending(nodeID string) (string, []string) {
	h.t.Helper()
	for _, t := range h.s.Tasks {
		if t.NodeID == nodeID && t.Status == TaskPending {
			var users []string
			for _, a := range t.Assignees {
				if a.Status == AssigneePending {
					users = append(users, a.UserID)
				}
			}
			return t.ID, users
		}
	}
	h.t.Fatalf("no pending task at node %q; tasks = %s", nodeID, h.dump())
	return "", nil
}

func (h *harness) pendingNodes() []string {
	var out []string
	for _, t := range h.s.Tasks {
		if t.Status == TaskPending {
			out = append(out, t.NodeID)
		}
	}
	slices.Sort(out)
	return out
}

func (h *harness) dump() string {
	b, _ := json.Marshal(h.s.Tasks)
	return string(b)
}

func (h *harness) approve(nodeID, user string) {
	h.t.Helper()
	id, _ := h.pending(nodeID)
	if err := h.exec().Approve(context.Background(), id, user, "ok"); err != nil {
		h.t.Fatalf("Approve(%s by %s) error = %v", nodeID, user, err)
	}
}

func (h *harness) wantStatus(status int32) {
	h.t.Helper()
	if h.s.Instance.Status != status {
		h.t.Fatalf("instance status = %d, want %d; tasks = %s", h.s.Instance.Status, status, h.dump())
	}
	if status != InstanceRunning {
		for _, tk := range h.s.Tokens {
			if tk.Status != TokenDone {
				h.t.Fatalf("finished instance still has live token at %q", tk.NodeID)
			}
		}
		if nodes := h.pendingNodes(); len(nodes) > 0 {
			h.t.Fatalf("finished instance still has pending tasks at %v", nodes)
		}
	}
}

func task(id, mode string, users ...string) string {
	return `{"id":"` + id + `","type":"userTask","name":"` + id + `","assignee":{"mode":"` + mode + `","rules":[{"kind":"user","userIds":["` + strings.Join(users, `","`) + `"]}]}}`
}

const linear = `{"nodes":[{"id":"s","type":"start"},` + "%TASKS%" + `,{"id":"e","type":"end"}],"edges":%EDGES%}`

func linearGraph(tasks ...string) string {
	ids := []string{"s"}
	for _, tk := range tasks {
		var n struct{ ID string }
		_ = json.Unmarshal([]byte(tk), &n)
		ids = append(ids, n.ID)
	}
	ids = append(ids, "e")
	var edges []string
	for i := 0; i+1 < len(ids); i++ {
		edges = append(edges, `{"id":"e`+ids[i]+`","source":"`+ids[i]+`","target":"`+ids[i+1]+`"}`)
	}
	r := strings.NewReplacer("%TASKS%", strings.Join(tasks, ","), "%EDGES%", "["+strings.Join(edges, ",")+"]")
	return r.Replace(linear)
}

func TestEngine_ModeAny(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1", "u2"), task("b", "", "u3")), nil, nil)
	if _, users := h.pending("a"); !slices.Equal(users, []string{"u1", "u2"}) {
		t.Fatalf("pending users = %v", users)
	}
	if len(h.s.Notices) != 1 || h.s.Notices[0].Kind != NoticeTaskAssigned || len(h.s.Notices[0].Recipients) != 2 {
		t.Fatalf("notices = %+v", h.s.Notices[0])
	}
	id, _ := h.pending("a")
	// 非处理人、重复处理。
	if err := h.exec().Approve(context.Background(), id, "stranger", ""); !errors.Is(err, ErrTaskNotAssignee) {
		t.Fatalf("Approve(stranger) error = %v, want ErrTaskNotAssignee", err)
	}
	h.approve("a", "u2")
	if err := h.exec().Approve(context.Background(), id, "u1", ""); !errors.Is(err, ErrTaskNotPending) {
		t.Fatalf("Approve(after done) error = %v, want ErrTaskNotPending", err)
	}
	if err := h.exec().Approve(context.Background(), "missing", "u1", ""); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("Approve(missing task) error = %v, want ErrTaskNotFound", err)
	}
	h.wantStatus(InstanceRunning)
	h.approve("b", "u3")
	h.wantStatus(InstanceCompleted)
	if n := h.s.Notices[len(h.s.Notices)-1]; n.Kind != NoticeInstanceFinished || n.Recipients[0] != "init" {
		t.Fatalf("finish notice = %+v", n)
	}
	if err := h.exec().Cancel("init", ""); !errors.Is(err, ErrInstanceNotRunning) {
		t.Fatalf("Cancel(finished) error = %v, want ErrInstanceNotRunning", err)
	}
}

func TestEngine_ModeAll(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "all", "u1", "u2")), nil, nil)
	h.approve("a", "u1")
	h.wantStatus(InstanceRunning)
	if _, users := h.pending("a"); !slices.Equal(users, []string{"u2"}) {
		t.Fatalf("remaining = %v", users)
	}
	h.approve("a", "u2")
	h.wantStatus(InstanceCompleted)

	// 会签中任一人拒绝即拒绝，其余席位取消。
	h = start(t, linearGraph(task("a", "all", "u1", "u2")), nil, nil)
	id, _ := h.pending("a")
	if err := h.exec().Reject(context.Background(), id, "u2", "no"); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	h.wantStatus(InstanceRejected)
	if a := h.s.Tasks[0].Assignees[0]; a.Status != AssigneeCancelled {
		t.Fatalf("other assignee status = %d, want cancelled", a.Status)
	}
}

func TestEngine_ModeSequential(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "sequential", "u1", "u2", "u3")), nil, nil)
	id, users := h.pending("a")
	if !slices.Equal(users, []string{"u1"}) || !slices.Equal(h.s.Notices[0].Recipients, []string{"u1"}) {
		t.Fatalf("first in sequence = %v notices = %+v", users, h.s.Notices[0])
	}
	if err := h.exec().Approve(context.Background(), id, "u2", ""); !errors.Is(err, ErrTaskNotPending) {
		t.Fatalf("Approve(out of turn) error = %v, want ErrTaskNotPending", err)
	}
	h.approve("a", "u1")
	if _, users := h.pending("a"); !slices.Equal(users, []string{"u2"}) {
		t.Fatalf("second in sequence = %v", users)
	}
	// 转办接替原顺序。
	if err := h.exec().Transfer(context.Background(), id, "u2", "u9", "busy"); err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	h.approve("a", "u9")
	h.approve("a", "u3")
	h.wantStatus(InstanceCompleted)
}

func TestEngine_TransferAndReassign(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1", "u2")), nil, &dirStub{inactive: map[string]bool{"gone": true}})
	id, _ := h.pending("a")
	ctx := context.Background()
	for name, to := range map[string]string{"self": "u1", "already assignee": "u2", "inactive": "gone"} {
		if err := h.exec().Transfer(ctx, id, "u1", to, ""); !errors.Is(err, ErrTaskTargetInvalid) {
			t.Errorf("Transfer(to %s) error = %v, want ErrTaskTargetInvalid", name, err)
		}
	}
	if err := h.exec().Transfer(ctx, id, "u1", "u5", ""); err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	if _, users := h.pending("a"); !slices.Equal(users, []string{"u2", "u5"}) {
		t.Fatalf("after transfer = %v", users)
	}
	if err := h.exec().Reassign(ctx, id, "admin", []string{"gone"}, "r"); !errors.Is(err, ErrTaskTargetInvalid) {
		t.Fatalf("Reassign(no active user) error = %v, want ErrTaskTargetInvalid", err)
	}
	if err := h.exec().Reassign(ctx, id, "admin", []string{"u7", "gone", "u8"}, "handover"); err != nil {
		t.Fatalf("Reassign() error = %v", err)
	}
	if _, users := h.pending("a"); !slices.Equal(users, []string{"u7", "u8"}) {
		t.Fatalf("after reassign = %v", users)
	}
	if err := h.exec().Approve(ctx, id, "u2", ""); !errors.Is(err, ErrTaskNotPending) {
		t.Fatalf("Approve(by replaced assignee) error = %v, want ErrTaskNotPending", err)
	}
	h.approve("a", "u8")
	h.wantStatus(InstanceCompleted)
}

func TestEngine_RejectToAndReturn(t *testing.T) {
	t.Parallel()
	verify := `{"id":"v","type":"userTask","name":"v","assignee":{"rules":[{"kind":"initiator"}]},"onReject":{"to":"a"}}`
	h := start(t, linearGraph(task("a", "any", "u1"), task("b", "any", "u2"), verify), nil, nil)
	ctx := context.Background()
	h.approve("a", "u1")
	// 退回：目标必须是走过的人工任务。
	id, _ := h.pending("b")
	for name, to := range map[string]string{"self": "b", "not visited": "v", "not a task": "e", "missing": "zz"} {
		if err := h.exec().Return(ctx, id, "u2", to, ""); !errors.Is(err, ErrTaskTargetInvalid) {
			t.Errorf("Return(to %s) error = %v, want ErrTaskTargetInvalid", name, err)
		}
	}
	if err := h.exec().Return(ctx, id, "u2", "a", "redo"); err != nil {
		t.Fatalf("Return() error = %v", err)
	}
	if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"a"}) {
		t.Fatalf("after return pending = %v", nodes)
	}
	h.approve("a", "u1")
	h.approve("b", "u2")
	// 验证不通过：按节点配置退回 a，流程不结束。
	id, _ = h.pending("v")
	if err := h.exec().Reject(ctx, id, "init", "not fixed"); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	h.wantStatus(InstanceRunning)
	if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"a"}) {
		t.Fatalf("after reject-to pending = %v", nodes)
	}
	h.approve("a", "u1")
	h.approve("b", "u2")
	h.approve("v", "init")
	h.wantStatus(InstanceCompleted)
}

const branching = `{"nodes":[{"id":"s","type":"start"},{"id":"g","type":"exclusiveGateway"},` +
	`{"id":"big","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["boss"]}]}},` +
	`{"id":"small","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["lead"]}]}},` +
	`{"id":"m","type":"exclusiveGateway"},{"id":"e","type":"end"}],"edges":[` +
	`{"id":"e1","source":"s","target":"g"},` +
	`{"id":"e2","source":"g","target":"big","condition":"form.amount >= 10000","priority":1},` +
	`{"id":"e3","source":"g","target":"e","condition":"form.kind == 'free'","priority":2},` +
	`{"id":"e4","source":"g","target":"small","isDefault":true},` +
	`{"id":"e5","source":"big","target":"m"},{"id":"e6","source":"small","target":"m"},{"id":"e7","source":"m","target":"e"}]}`

func TestEngine_ExclusiveGateway(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		form map[string]any
		want []string
		done bool
	}{
		"first condition":            {map[string]any{"amount": 20000.0, "kind": "x"}, []string{"big"}, false},
		"priority order":             {map[string]any{"amount": 20000.0, "kind": "free"}, []string{"big"}, false},
		"second condition ends":      {map[string]any{"amount": 1.0, "kind": "free"}, nil, true},
		"default":                    {map[string]any{"amount": 1.0, "kind": "x"}, []string{"small"}, false},
		"missing variable → default": {map[string]any{}, []string{"small"}, false},
		"type mismatch → default":    {map[string]any{"amount": "lots", "kind": 5.0}, []string{"small"}, false},
		"nil variables → default":    {nil, []string{"small"}, false},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := start(t, branching, tt.form, nil)
			if nodes := h.pendingNodes(); !slices.Equal(nodes, tt.want) {
				t.Fatalf("pending = %v, want %v", nodes, tt.want)
			}
			if tt.done {
				h.wantStatus(InstanceCompleted)
			}
		})
	}
}

const parallel = `{"nodes":[{"id":"s","type":"start"},{"id":"f","type":"parallelGateway","join":"j"},` +
	`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u1"]}]}},` +
	`{"id":"a2","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u1b"]}]}},` +
	`{"id":"b","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u2"]}]}},` +
	`{"id":"c","type":"cc","assignee":{"rules":[{"kind":"user","userIds":["watcher"]}]}},` +
	`{"id":"j","type":"parallelGateway"},` +
	`{"id":"z","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u3"]}]}},{"id":"e","type":"end"}],"edges":[` +
	`{"id":"1","source":"s","target":"f"},{"id":"2","source":"f","target":"a"},{"id":"3","source":"f","target":"b"},{"id":"4","source":"f","target":"c"},` +
	`{"id":"5","source":"a","target":"a2"},{"id":"6","source":"a2","target":"j"},{"id":"7","source":"b","target":"j"},{"id":"8","source":"c","target":"j"},` +
	`{"id":"9","source":"j","target":"z"},{"id":"10","source":"z","target":"e"}]}`

func TestEngine_ParallelGateway(t *testing.T) {
	t.Parallel()
	h := start(t, parallel, nil, nil)
	if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"a", "b"}) {
		t.Fatalf("after fork pending = %v", nodes)
	}
	// 抄送分支不阻塞，已到达汇聚等待。
	cc := h.s.Tasks[slices.IndexFunc(h.s.Tasks, func(t *Task) bool { return t.Kind == TaskKindCC })]
	if cc.Status != TaskNotified || cc.Assignees[0].Status != AssigneeNotified {
		t.Fatalf("cc task = %+v", cc)
	}
	h.approve("b", "u2")
	if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"a"}) {
		t.Fatalf("join must wait for all branches, pending = %v", nodes)
	}
	h.approve("a", "u1")
	ctx := context.Background()
	// 分支内退回：同区域允许；退到分支外不允许。
	id, _ := h.pending("a2")
	if err := h.exec().Return(ctx, id, "u1b", "b", ""); !errors.Is(err, ErrTaskTargetInvalid) {
		t.Fatalf("Return(to sibling branch) error = %v, want ErrTaskTargetInvalid", err)
	}
	if err := h.exec().Return(ctx, id, "u1b", "a", ""); err != nil {
		t.Fatalf("Return(within branch) error = %v", err)
	}
	h.approve("a", "u1")
	h.approve("a2", "u1b")
	if nodes := h.pendingNodes(); !slices.Equal(nodes, []string{"z"}) {
		t.Fatalf("after join pending = %v", nodes)
	}
	// 汇聚后只应有一个存活的执行位置。
	live := 0
	for _, tk := range h.s.Tokens {
		if tk.Status != TokenDone {
			live++
		}
	}
	if live != 1 {
		t.Fatalf("live tokens after join = %d, want 1", live)
	}
	h.approve("z", "u3")
	h.wantStatus(InstanceCompleted)

	// 并行中某分支拒绝（未配去向）：整个实例拒绝，其余分支任务取消。
	h = start(t, parallel, nil, nil)
	id, _ = h.pending("b")
	if err := h.exec().Reject(ctx, id, "u2", ""); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	h.wantStatus(InstanceRejected)
}

func TestEngine_AssigneeResolution(t *testing.T) {
	t.Parallel()
	dir := &dirStub{
		inactive: map[string]bool{"gone": true},
		roles:    map[string][]string{"ops": {"o1", "o2", "gone"}},
		leaders:  map[string][]string{"d-team": {"init"}, "d-center": {"gone"}, "d-root": {"ceo"}, "d-fin": {"cfo"}},
		parents:  map[string]string{"d-team": "d-center", "d-center": "d-root"},
	}
	rule := func(id, rules, extra string) string {
		return `{"id":"` + id + `","type":"userTask","assignee":{"rules":[` + rules + `]` + extra + `}}`
	}
	cases := map[string]struct {
		node string
		form map[string]any
		want []string // nil = 节点被跳过
	}{
		"role + user union, inactive dropped": {rule("a", `{"kind":"role","roleCodes":["ops"]},{"kind":"user","userIds":["o1","x"]}`, ""), nil, []string{"o1", "o2", "x"}},
		"initiator":                           {rule("a", `{"kind":"initiator"}`, ""), nil, []string{"init"}},
		// 发起人自己是本部门负责人 → 上一级；上一级负责人不可用 → 再上一级。
		"dept leader skips initiator and inactive": {rule("a", `{"kind":"deptLeader","level":1}`, ""), nil, []string{"ceo"}},
		"dept leader level 3":                      {rule("a", `{"kind":"deptLeader","level":3}`, ""), nil, []string{"ceo"}},
		"dept leader of":                           {rule("a", `{"kind":"deptLeaderOf","deptId":"d-fin"}`, ""), nil, []string{"cfo"}},
		"form user (string)":                       {rule("a", `{"kind":"formUser","field":"owner"}`, ""), map[string]any{"owner": "o1"}, []string{"o1"}},
		"form user (array)":                        {rule("a", `{"kind":"formUser","field":"owners"}`, ""), map[string]any{"owners": []any{"o2", "gone", 5.0}}, []string{"o2"}},
		"form user excludes initiator":             {rule("a", `{"kind":"formUser","field":"owners"}`, ""), map[string]any{"owners": []any{"init", "o1"}}, []string{"o1"}},
		"form user only initiator → skip chosen":   {rule("a", `{"kind":"formUser","field":"owner"}`, `,"onEmpty":"skip"`), map[string]any{"owner": "init"}, nil},
		"form dept leader":                         {rule("a", `{"kind":"formDeptLeader","field":"dept"}`, ""), map[string]any{"dept": "d-fin"}, []string{"cfo"}},
		"empty → skip only if chosen":              {rule("a", `{"kind":"user","userIds":["gone"]}`, `,"onEmpty":"skip"`), nil, nil},
		"empty → admin":                            {rule("a", `{"kind":"user","userIds":["gone"]}`, `,"onEmpty":"admin"`), nil, []string{"adm"}},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			raw := strings.Replace(linearGraph(tt.node), `"edges"`, `"settings":{"admins":["adm"]},"edges"`, 1)
			h := start(t, raw, tt.form, dir)
			if tt.want == nil {
				h.wantStatus(InstanceCompleted)
				if !slices.ContainsFunc(h.s.Histories, func(x *History) bool { return x.Action == ActionSkip }) {
					t.Fatal("skipped node must be recorded in history")
				}
				return
			}
			if _, users := h.pending("a"); !slices.Equal(users, tt.want) {
				t.Fatalf("assignees = %v, want %v", users, tt.want)
			}
		})
	}
}

func TestEngine_CancelAndTerminate(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1")), nil, nil)
	if err := h.exec().Cancel("u1", ""); !errors.Is(err, ErrInstanceForbidden) {
		t.Fatalf("Cancel(by non-initiator) error = %v, want ErrInstanceForbidden", err)
	}
	if err := h.exec().Cancel("init", "changed my mind"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	h.wantStatus(InstanceCancelled)

	h = start(t, linearGraph(task("a", "any", "u1")), nil, nil)
	if err := h.exec().Terminate("admin", "policy"); err != nil {
		t.Fatalf("Terminate() error = %v", err)
	}
	h.wantStatus(InstanceTerminated)
	if err := h.exec().Terminate("admin", "again"); !errors.Is(err, ErrInstanceNotRunning) {
		t.Fatalf("Terminate(finished) error = %v, want ErrInstanceNotRunning", err)
	}
	id := h.s.Tasks[0].ID
	if err := h.exec().Approve(context.Background(), id, "u1", ""); !errors.Is(err, ErrInstanceNotRunning) {
		t.Fatalf("Approve(on finished instance) error = %v, want ErrInstanceNotRunning", err)
	}
}

// 回路失控保护：任务总数超过上限后拒绝继续推进，而不是无限增长。
func TestEngine_TaskLimit(t *testing.T) {
	t.Parallel()
	verify := `{"id":"v","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u2"]}]},"onReject":{"to":"a"}}`
	h := start(t, linearGraph(task("a", "any", "u1"), verify), nil, nil)
	ctx := context.Background()
	var err error
	for i := 0; i < maxTasksPerInstance && err == nil; i++ {
		h.approve("a", "u1")
		id, _ := h.pending("v")
		err = h.exec().Reject(ctx, id, "u2", "")
	}
	if !errors.Is(err, ErrInstanceLimit) {
		t.Fatalf("looping forever must hit ErrInstanceLimit, got %v (tasks = %d)", err, len(h.s.Tasks))
	}
}

// 退回只能往回走：退回到 a 之后，a 不能借「曾经到过 c」直接跳到 c 而绕过 b。
func TestEngine_ReturnCannotSkipForward(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1"), task("b", "any", "u2"), task("c", "any", "u3")), nil, nil)
	ctx := context.Background()
	h.approve("a", "u1")
	h.approve("b", "u2")
	id, _ := h.pending("c")
	if err := h.exec().Return(ctx, id, "u3", "a", "redo"); err != nil {
		t.Fatalf("Return(c → a) error = %v", err)
	}
	id, _ = h.pending("a")
	for _, to := range []string{"b", "c"} {
		if err := h.exec().Return(ctx, id, "u1", to, ""); !errors.Is(err, ErrTaskTargetInvalid) {
			t.Fatalf("Return(a → %s) error = %v, want ErrTaskTargetInvalid", to, err)
		}
	}
	h.approve("a", "u1")
	// b 之前只有 a。
	bTask := h.s.Tasks[len(h.s.Tasks)-1]
	if targets := ReturnTargets(h.g, h.s, bTask); len(targets) != 1 || targets[0].ID != "a" {
		t.Fatalf("ReturnTargets(b) = %+v, want [a]", targets)
	}
}

// 反复转办不会让处理人记录无限增长。
func TestEngine_TransferLimit(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1")), nil, nil)
	ctx := context.Background()
	id, _ := h.pending("a")
	from, to := "u1", "u2"
	var err error
	for i := 0; i < 3*maxAssigneesPerTask && err == nil; i++ {
		err = h.exec().Transfer(ctx, id, from, to, "")
		from, to = to, from
	}
	if !errors.Is(err, ErrInstanceLimit) {
		t.Fatalf("endless transfer must hit ErrInstanceLimit, got %v (assignees = %d)", err, len(h.s.Tasks[0].Assignees))
	}
}

// 回写只包含变化的部分：处理一个任务不会把此前已结束的任务再写一遍。
func TestState_ChangedOnly(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(task("a", "any", "u1"), task("b", "any", "u2"), task("c", "any", "u3")), nil, nil)
	h.approve("a", "u1")
	h.approve("b", "u2") // 执行后：a 未变、b 结束、c 新建
	changed := h.s.ChangedTasks()
	var nodes []string
	for _, tk := range changed {
		nodes = append(nodes, tk.NodeID)
	}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []string{"b", "c"}) {
		t.Fatalf("changed tasks = %v, want [b c]", nodes)
	}
	if tokens := h.s.ChangedTokens(); len(tokens) != 1 {
		t.Fatalf("changed tokens = %d, want 1 (the moved token)", len(tokens))
	}
	// 表单字段型规则只采纳有限个取值。
	many := make([]any, 500)
	for i := range many {
		many[i] = "x"
	}
	if got := formStrings(many); len(got) != maxFormTargets {
		t.Fatalf("formStrings cap = %d, want %d", len(got), maxFormTargets)
	}
}
