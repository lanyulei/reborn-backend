package biz

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

// 条件里的数值类型：变量一律规整为 JSON 取值（数字是 float64），发起那一条命令与之后从库里
// 读回时的求值结果一致；求值出错的条件记入历史。
func TestEngine_ConditionNumericConsistency(t *testing.T) {
	t.Parallel()
	gateway := func(cond string) string {
		return `{"nodes":[{"id":"s","type":"start"},{"id":"gw","type":"exclusiveGateway","join":"m"},
			{"id":"big","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u1"]}]}},
			{"id":"m","type":"exclusiveGateway"},{"id":"e","type":"end"}],
			"edges":[{"id":"e1","source":"s","target":"gw"},
			{"id":"hit","source":"gw","target":"big","condition":"` + cond + `","priority":1},
			{"id":"def","source":"gw","target":"m","isDefault":true},
			{"id":"e4","source":"big","target":"m"},{"id":"e5","source":"m","target":"e"}],"settings":{}}`
	}
	cases := map[string]struct {
		cond    string
		hit     bool
		errored bool
	}{
		"comparison":           {"form.n > 2", true, false},
		"equality":             {"form.n == 3", true, false},
		"double arithmetic":    {"form.n * 1.5 > 4.0", true, false},
		"explicit conversion":  {"int(form.n) + 1 > 3", true, false},
		"int arithmetic fails": {"form.n + 1 > 3", false, true},
		"missing variable":     {"form.nope > 3", false, true},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// 同一个值分别以 Go 的整数（发起时业务域传入的）与 float64（库里读回的）进入。
			var outcomes []bool
			for _, raw := range []map[string]any{{"n": int64(3)}, {"n": int32(3)}, {"n": 3.0}} {
				vars, err := normalizeVariables(raw)
				if err != nil {
					t.Fatal(err)
				}
				h := start(t, gateway(tt.cond), vars, nil)
				hit := len(h.pendingNodes()) == 1
				outcomes = append(outcomes, hit)
				errored := slices.ContainsFunc(h.s.Histories, func(x *History) bool {
					if x.Action != ActionConditionError || x.NodeID != "gw" {
						return false
					}
					edges, _ := x.Detail["edges"].([]any)
					entry, _ := edges[0].(map[string]any)
					return len(edges) == 1 && entry["edge"] == "hit" && entry["error"] != ""
				})
				if hit != tt.hit || errored != tt.errored {
					t.Fatalf("n=%T: hit %v (want %v), error recorded %v (want %v)", raw["n"], hit, tt.hit, errored, tt.errored)
				}
			}
			if slices.Contains(outcomes, !outcomes[0]) {
				t.Fatalf("the same value routed differently depending on its Go type: %v", outcomes)
			}
		})
	}
}

func TestNormalizeVariables(t *testing.T) {
	t.Parallel()
	got, err := normalizeVariables(map[string]any{"i": int64(7), "f": 1.5, "s": "x", "list": []string{"a"}, "nested": map[string]any{"k": int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if got["i"] != 7.0 || got["f"] != 1.5 || got["s"] != "x" {
		t.Fatalf("normalised = %#v", got)
	}
	if list, ok := got["list"].([]any); !ok || len(list) != 1 || list[0] != "a" {
		t.Fatalf("list = %#v", got["list"])
	}
	if nested, ok := got["nested"].(map[string]any); !ok || nested["k"] != 1.0 {
		t.Fatalf("nested = %#v", got["nested"])
	}
	if out, err := normalizeVariables(nil); out != nil || err != nil {
		t.Fatalf("nil in = %v, %v", out, err)
	}
	for name, bad := range map[string]map[string]any{
		"NUL":       {"s": "a\x00b"},
		"too large": {"s": strings.Repeat("x", maxVariablesBytes)},
		"not JSON":  {"c": make(chan int)},
	} {
		if _, err := normalizeVariables(bad); err == nil {
			t.Errorf("normalizeVariables(%s) error = nil", name)
		}
	}
}

// 表单里选的人：大小写不同的 ID 不能绕过「不含发起人自己」（节点明确选了跳过：剔除后为空就跳过）。
func TestEngine_FormUserInitiatorCaseInsensitive(t *testing.T) {
	t.Parallel()
	const initiator = "018f3e1a-0a00-7000-8000-00000000abcd"
	node := `{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"formUser","field":"approver"}],"onEmpty":"skip"}}`
	for name, form := range map[string]map[string]any{
		"upper case":      {"approver": strings.ToUpper(initiator)},
		"padded":          {"approver": " " + initiator + " "},
		"array, repeated": {"approver": []any{strings.ToUpper(initiator), initiator}},
	} {
		s := &State{Instance: Instance{ID: "inst", Initiator: initiator, Status: InstanceRunning, Variables: form}}
		if err := NewExecution(mustGraph(t, linearGraph(node)), s, lowerDir{}, time.Now()).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if s.Instance.Status != InstanceCompleted || len(s.Tasks) != 0 {
			t.Errorf("%s: the initiator became their own approver (status %d, %d tasks)", name, s.Instance.Status, len(s.Tasks))
		}
	}
	// 别人照常解析（统一为小写、去重）。
	other := "018F3E1A-0A00-7000-8000-00000000BEEF"
	if got := formIDs([]any{other, strings.ToLower(other), "", 5.0}); !slices.Equal(got, []string{strings.ToLower(other)}) {
		t.Fatalf("formIDs() = %v", got)
	}
}

// lowerDir 像真实的组织目录一样不区分大小写：返回小写的 ID。
type lowerDir struct{}

func (lowerDir) ActiveUserIDs(_ context.Context, ids []string) ([]string, error) {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id = strings.ToLower(id); !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out, nil
}
func (lowerDir) UserIDsByRoles(context.Context, []string) ([]string, error) { return nil, nil }
func (lowerDir) DeptLeaderIDs(context.Context, string, int, string) ([]string, error) {
	return nil, nil
}

// 夏令时切换日：按工作时间推算的时限落在正确的钟点（不偏一个小时）。
func TestCalendarAdd_DST(t *testing.T) {
	t.Parallel()
	cal, err := NewCalendar(CalendarConfig{
		Timezone: "America/New_York", Weekdays: []int32{0, 1, 2, 3, 4, 5, 6},
		Segments: []CalendarSegment{{"09:00", "18:00"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("America/New_York")
	at := func(s string) time.Time {
		v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	two := Duration{Value: 2, Unit: UnitHour, WorkTime: true}
	for from, want := range map[string]string{
		"2026-03-07 17:00": "2026-03-08 10:00", // 次日凌晨拨快一小时
		"2026-10-31 17:00": "2026-11-01 10:00", // 次日凌晨拨慢一小时
		"2026-03-08 09:30": "2026-03-08 11:30", // 切换日当天
		"2026-06-01 17:00": "2026-06-02 10:00", // 平常日子
	} {
		if got := cal.Add(at(from), two).In(loc).Format("2006-01-02 15:04"); got != want {
			t.Errorf("Add(%s, 2 working hours) = %s, want %s", from, got, want)
		}
	}
}

// 回路里明确选了跳过的人工任务解析不出处理人时不再阻塞：自动跳过的累计次数有上限，到上限即拒绝继续。
func TestEngine_AutoSkipLimit(t *testing.T) {
	t.Parallel()
	raw := linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["gone"]}],"onEmpty":"skip"}}`)
	dir := &dirStub{inactive: map[string]bool{"gone": true}}
	h := start(t, raw, nil, dir)
	h.wantStatus(InstanceCompleted)
	if h.s.Instance.AutoSkips != 1 {
		t.Fatalf("AutoSkips = %d, want 1", h.s.Instance.AutoSkips)
	}
	over := &harness{t: t, g: mustGraph(t, raw), dir: dir, s: &State{Instance: Instance{
		ID: "inst", Initiator: "init", Status: InstanceRunning, AutoSkips: maxAutoSkips,
	}}}
	if err := over.exec().Start(context.Background()); err != ErrInstanceLimit {
		t.Fatalf("Start() past the auto-skip limit error = %v, want ErrInstanceLimit", err)
	}
}

// countingDir 统计目录被查询的次数。
type countingDir struct {
	dirStub
	calls int
}

func (d *countingDir) ActiveUserIDs(ctx context.Context, ids []string) ([]string, error) {
	d.calls++
	return d.dirStub.ActiveUserIDs(ctx, ids)
}

// dutyStub 值班查询桩：值班表 ID → 当前的值班人（主在前、备在后）。
type dutyStub map[string][]string

func (d dutyStub) OnDuty(_ context.Context, scheduleID string, _ time.Time) ([]string, error) {
	return slices.Clone(d[scheduleID]), nil
}

// 值班人规则：取值班表此刻的主、备值班人，两人都能处理；发起人本人被剔除；值班表没人
// 值班（或没有接入值班查询）时按「处理人为空」处理。
func TestOncallRule(t *testing.T) {
	t.Parallel()
	graph := func(onEmpty string) string {
		return `{"nodes":[{"id":"s","type":"start"},
			{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"oncall","groupId":"g1","scheduleId":"sch"}]` + onEmpty + `}},
			{"id":"e","type":"end"}],
			"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"e"}],"settings":{"admins":["boss"]}}`
	}
	run := func(raw string, duty Duty, initiator string) *State {
		t.Helper()
		st := &State{Instance: Instance{ID: "inst", Initiator: initiator, Status: InstanceRunning}}
		if err := NewExecution(mustGraph(t, raw), st, &dirStub{}, time.Now()).WithDuty(duty).Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		return st
	}
	pending := func(st *State) []string {
		var users []string
		for _, task := range st.Tasks {
			for _, a := range task.Assignees {
				if task.Status == TaskPending && a.Status == AssigneePending {
					users = append(users, a.UserID)
				}
			}
		}
		return users
	}
	duty := dutyStub{"sch": {"primary", "backup"}}
	// 主、备都是处理人；缺省或签：一人同意即通过。
	st := run(graph(""), duty, "init")
	if got := pending(st); !slices.Equal(got, []string{"primary", "backup"}) {
		t.Fatalf("assignees = %v, want [primary backup]", got)
	}
	x := NewExecution(mustGraph(t, graph("")), st, &dirStub{}, time.Now()).WithDuty(duty)
	if err := x.Approve(context.Background(), st.Tasks[0].ID, "backup", ""); err != nil {
		t.Fatalf("Approve(backup) error = %v", err)
	}
	if st.Instance.Status != InstanceCompleted {
		t.Fatalf("status = %d, want approved after either on-call user approves", st.Instance.Status)
	}
	// 发起人正是主值班：剔除，只剩备值班。
	if got := pending(run(graph(""), duty, "primary")); !slices.Equal(got, []string{"backup"}) {
		t.Fatalf("assignees (initiator is primary) = %v, want [backup]", got)
	}
	// 没人值班：按 onEmpty 转交流程管理员；没有接入值班查询时同样视为空。
	for name, d := range map[string]Duty{"nobody on duty": dutyStub{}, "no duty source": nil} {
		if got := pending(run(graph(`,"onEmpty":"admin"`), d, "init")); !slices.Equal(got, []string{"boss"}) {
			t.Errorf("%s: assignees = %v, want [boss]", name, got)
		}
	}
	// 只有主值班且他就是发起人：剔除后为空。缺省拦住（发起失败）；明确选了跳过才跳过该节点，流程直接结束。
	s := &State{Instance: Instance{ID: "inst", Initiator: "init", Status: InstanceRunning}}
	wantNoAssignee(t, NewExecution(mustGraph(t, graph("")), s, &dirStub{}, time.Now()).WithDuty(dutyStub{"sch": {"init"}}).
		Start(context.Background()), `"a"`, "must not be skipped")
	if st := run(graph(`,"onEmpty":"skip"`), dutyStub{"sch": {"init"}}, "init"); st.Instance.Status != InstanceCompleted || len(pending(st)) != 0 {
		t.Fatalf("status = %d pending = %v, want the node skipped", st.Instance.Status, pending(st))
	}
	// 校验：必须选了值班表。
	g, err := ParseGraph([]byte(strings.Replace(graph(""), `"scheduleId":"sch"`, `"scheduleId":""`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if issues := g.Validate(); !slices.ContainsFunc(issues, func(i Issue) bool { return i.Code == "ASSIGNEE_RULE_INVALID" }) {
		t.Fatalf("Validate() issues = %+v, want ASSIGNEE_RULE_INVALID", issues)
	}
}

// 模拟运行带回路的流程图：总步数有上限、同一节点的处理人只解析一次。
func TestSimulate_LoopIsBounded(t *testing.T) {
	t.Parallel()
	// a → gw：恒真的条件跳回 a（模拟里人工任务一律同意，于是一圈圈走下去）。
	raw := `{"nodes":[{"id":"s","type":"start"},
		{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["u1"]}]}},
		{"id":"gw","type":"exclusiveGateway","join":"m"},{"id":"m","type":"exclusiveGateway"},{"id":"e","type":"end"}],
		"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"gw"},
		{"id":"back","source":"gw","target":"a","condition":"true","priority":1,"jump":true},
		{"id":"def","source":"gw","target":"m","isDefault":true},{"id":"e5","source":"m","target":"e"}],"settings":{}}`
	dir := &countingDir{}
	res, err := Simulate(context.Background(), mustGraph(t, raw), Instance{Initiator: "init"}, dir, nil, nil)
	if err != nil {
		t.Fatalf("Simulate() error = %v", err)
	}
	if res.Status != InstanceRunning {
		t.Fatalf("status = %d, want still running (the loop never ends)", res.Status)
	}
	if len(res.Steps) > maxSimSteps+maxAdvanceSteps {
		t.Fatalf("steps = %d, want bounded by the simulation budget", len(res.Steps))
	}
	if dir.calls != 1 {
		t.Fatalf("directory lookups = %d, want 1 (resolved once per node)", dir.calls)
	}
}

// 跨实例汇总字段权限：只有在每一个节点上都被隐藏的字段才算隐藏。
func TestHiddenOf(t *testing.T) {
	t.Parallel()
	a := &Node{ID: "a", FormPermissions: map[string]string{"x": "hidden", "y": "hidden", "z": "editable"}}
	b := &Node{ID: "b", FormPermissions: map[string]string{"x": "hidden", "y": "readonly"}}
	c := &Node{ID: "c"}
	if got := HiddenOf([]*Node{a, b}, ""); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("HiddenOf(a, b) = %v, want [x]", got)
	}
	if got := HiddenOf([]*Node{a, b, c}, ""); len(got) != 0 {
		t.Fatalf("HiddenOf(a, b, c) = %v, want none (c shows everything)", got)
	}
	if got := HiddenOf(nil, ""); len(got) != 0 {
		t.Fatalf("HiddenOf(nil) = %v", got)
	}
}

// 按表单区分的字段权限：同一字段在某张表单的专属配置里出现就以专属为准，否则回落到通用配置；
// 别的表单的专属配置互不影响。合并多个节点时先各自取对该表单生效的权限。
func TestPermissionsByForm(t *testing.T) {
	t.Parallel()
	a := &Node{
		ID:              "a",
		FormPermissions: map[string]string{"x": "editable", "y": "hidden"},
		FormPermissionsByForm: map[string]map[string]string{
			"tplA": {"x": "hidden", "z": "required"},
			"tplB": {"y": "editable"},
		},
	}
	for form, want := range map[string]map[string]string{
		"":     {"x": "editable", "y": "hidden"},
		"tplA": {"x": "hidden", "y": "hidden", "z": "required"},
		"tplB": {"x": "editable", "y": "editable"},
		"tplC": {"x": "editable", "y": "hidden"}, // 没有专属配置：只用通用的
	} {
		if got := a.PermissionsFor(form); !maps.Equal(got, want) {
			t.Errorf("PermissionsFor(%q) = %v, want %v", form, got, want)
		}
	}
	// 另一个节点对 tplA 没有配置 x：x 在那里只读可见，合并后不再隐藏；y 在两个节点上都隐藏。
	b := &Node{ID: "b", FormPermissions: map[string]string{"y": "hidden"}}
	if got := HiddenOf([]*Node{a, b}, "tplA"); !slices.Equal(got, []string{"y"}) {
		t.Fatalf("HiddenOf(a, b, tplA) = %v, want [y]", got)
	}
	if got := HiddenOf([]*Node{a}, "tplA"); !slices.Equal(got, []string{"x", "y"}) {
		t.Fatalf("HiddenOf(a, tplA) = %v, want [x y]", got)
	}
	if got := PermissionsOf([]*Node{a, b}, "tplA"); got["z"] != "required" || got["x"] != "readonly" {
		t.Fatalf("PermissionsOf(a, b, tplA) = %v", got)
	}
	// 校验：专属配置里的表单标识与权限取值同样受检，条数与通用的合计。
	for name, n := range map[string]*Node{
		"empty form key": {FormPermissionsByForm: map[string]map[string]string{"": {"x": "hidden"}}},
		"empty table":    {FormPermissionsByForm: map[string]map[string]string{"tplA": {}}},
		"bad permission": {FormPermissionsByForm: map[string]map[string]string{"tplA": {"x": "secret"}}},
	} {
		if validFormPermissions(n) {
			t.Errorf("validFormPermissions(%s) = true, want false", name)
		}
	}
	if !validFormPermissions(a) {
		t.Error("validFormPermissions(a) = false, want true")
	}
}

// 工作时间推算按真实时刻逐段消耗：结果从不早于起点；覆盖夏令时切换的那一小时、整天时段
// （00:00–24:00）与平常日子。
func TestCalendarAdd_Instants(t *testing.T) {
	t.Parallel()
	type tc struct {
		zone     string
		segments []CalendarSegment
		from     string // RFC3339：带偏移，能区分回拨时重复出现的那一小时
		d        Duration
		want     string
	}
	allDay := []CalendarSegment{{"00:00", "24:00"}}
	office := []CalendarSegment{{"09:00", "12:00"}, {"13:00", "18:00"}}
	mins := func(n int) Duration { return Duration{Value: n, Unit: UnitMinute, WorkTime: true} }
	hours := func(n int) Duration { return Duration{Value: n, Unit: UnitHour, WorkTime: true} }
	for name, c := range map[string]tc{
		"fall back, second 01:30":     {"America/New_York", allDay, "2026-11-01T01:30:00-05:00", mins(10), "2026-11-01T01:40:00-05:00"},
		"fall back, first 01:30":      {"America/New_York", allDay, "2026-11-01T01:30:00-04:00", hours(1), "2026-11-01T01:30:00-05:00"},
		"spring forward across gap":   {"America/New_York", allDay, "2026-03-08T01:50:00-05:00", mins(30), "2026-03-08T03:20:00-04:00"},
		"all day crosses midnight":    {"Asia/Shanghai", allDay, "2026-06-01T23:30:00+08:00", hours(1), "2026-06-02T00:30:00+08:00"},
		"shanghai within a segment":   {"Asia/Shanghai", office, "2026-06-01T10:00:00+08:00", hours(1), "2026-06-01T11:00:00+08:00"},
		"shanghai over lunch":         {"Asia/Shanghai", office, "2026-06-01T11:30:00+08:00", hours(1), "2026-06-01T13:30:00+08:00"},
		"shanghai into the next day":  {"Asia/Shanghai", office, "2026-06-01T17:30:00+08:00", hours(2), "2026-06-02T10:30:00+08:00"},
		"shanghai before opening":     {"Asia/Shanghai", office, "2026-06-01T07:00:00+08:00", mins(30), "2026-06-01T09:30:00+08:00"},
		"shanghai exactly to the end": {"Asia/Shanghai", office, "2026-06-01T17:00:00+08:00", hours(1), "2026-06-01T18:00:00+08:00"},
		"seconds are truncated":       {"Asia/Shanghai", office, "2026-06-01T10:00:45+08:00", mins(1), "2026-06-01T10:01:00+08:00"},
	} {
		cal, err := NewCalendar(CalendarConfig{Timezone: c.zone, Weekdays: []int32{0, 1, 2, 3, 4, 5, 6}, Segments: c.segments})
		if err != nil {
			t.Fatalf("%s: NewCalendar() error = %v", name, err)
		}
		from, _ := time.Parse(time.RFC3339, c.from)
		want, _ := time.Parse(time.RFC3339, c.want)
		got := cal.Add(from, c.d)
		if !got.Equal(want) {
			t.Errorf("%s: Add(%s) = %s, want %s", name, c.from, got.In(from.Location()).Format(time.RFC3339), c.want)
		}
		if got.Before(from.Truncate(time.Minute)) {
			t.Errorf("%s: result %s is before the start %s", name, got, from)
		}
	}
}
