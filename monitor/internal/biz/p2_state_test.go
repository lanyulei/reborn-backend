package biz

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 第二、三期状态机测试的公共构件。

func atM(minutes int) time.Time { return at(minutes * 60) }

func eq(label, value string) monitorcontract.Matcher {
	return monitorcontract.Matcher{Label: label, Op: monitorcontract.OpEqual, Value: value}
}

// inhibitRule 源 / 目标各按一个标签取值匹配、相等标签为 equal 的抑制规则。
func inhibitRule(id, sourceKind, targetKind string, equal ...string) monitorpipeline.InhibitRule {
	return monitorpipeline.InhibitRule{
		ID:     id,
		Source: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("kind", sourceKind)}},
		Target: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("kind", targetKind)}},
		Equal:  equal,
	}
}

func active(id string, kv ...string) ActiveAlert {
	labels := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	return ActiveAlert{Ref: monitorpipeline.AlertRef{ID: id, RuleID: "r-" + labels["kind"], GroupPath: "/g/", Severity: 2, Labels: labels}}
}

func onCI(a ActiveAlert, ciID string, topology bool) ActiveAlert {
	a.Ref.CIID, a.Topology = ciID, topology
	return a
}

// viewOf 以一组抑制规则与活跃告警算抑制结论（告警按 ID 排序，与缓存的做法一致）。
func viewOf(rules []monitorpipeline.InhibitRule, cis *monitorpipeline.CIIndex, alerts ...ActiveAlert) *InhibitView {
	sort.Slice(alerts, func(i, j int) bool { return alerts[i].Ref.ID < alerts[j].Ref.ID })
	return buildInhibitView(monitorpipeline.CompileInhibits(rules), alerts, cis)
}

func inhibitedIDs(v *InhibitView, ids ...string) string {
	var out []string
	for _, id := range ids {
		if hit, ok := v.Known(id); ok && hit {
			out = append(out, id)
		}
	}
	return strings.Join(out, ",")
}

func hostCI(id, name, ip, env, status string, upstream ...string) monitorpipeline.CI {
	return monitorpipeline.CI{
		ID: id, ModelCode: "host", Name: name, Env: env, Status: status, Owner: "alice", Tags: []string{"core"},
		Attrs: map[string][]string{"ip": {ip}}, UpstreamIDs: upstream,
	}
}

func (s *sim) alertBy(label, value string) *simAlert {
	s.t.Helper()
	var found *simAlert
	for _, a := range s.alerts {
		if a.Labels[label] == value && (found == nil || a.FirstFire.After(found.FirstFire)) {
			found = a
		}
	}
	if found == nil {
		s.t.Fatalf("no alert with %s=%s", label, value)
	}
	return found
}

func (s *sim) eventTypes() string {
	out := make([]string, 0, len(s.events))
	for _, ev := range s.events {
		out = append(out, ev.Type)
	}
	return strings.Join(out, ",")
}

// —— 流水线各阶段与顺序 ——

func pipelineRule() *Rule {
	return baseRule(func(r *Rule) {
		r.For = 0
		r.Labels = map[string]string{"team": "ops"}
		r.Pipeline = monitorpipeline.Spec{
			Relabel: []monitorcontract.RelabelStep{{
				Action: monitorcontract.RelabelReplace, SourceLabels: []string{"instance"}, Regex: `(.+):\d+`, TargetLabel: "host", Replacement: "$1",
			}},
			DropExpr: `{{ if eq $labels.ci_env "dev" }}true{{ end }}`,
			CIMatch:  monitorcontract.CIMatch{Label: "host", By: monitorcontract.CIMatchByAttr, Attr: "ip", EnrichTags: []string{"core"}},
			Scope:    monitorcontract.ResourceScope{Enabled: true, ModelCodes: []string{"host"}},
		}
	})
}

func TestPipelineStagesAndOrder(t *testing.T) {
	rule := pipelineRule()
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	index := func(env string) *monitorpipeline.CIIndex {
		return monitorpipeline.NewCIIndex([]monitorpipeline.CI{
			hostCI("ci-1", "web-1", "10.0.0.1", env, "running"),
			hostCI("ci-2", "web-2", "10.0.0.2", "dev", "running"),
			{ID: "ci-3", ModelCode: "switch", Name: "sw-1", Env: "prod", Attrs: map[string][]string{"ip": {"10.0.0.3"}}},
		})
	}
	results := func() [][]promclient.Series {
		return [][]promclient.Series{q(
			ser(90, "instance", "10.0.0.1:9100"), // 关联到 prod 主机：保留
			ser(91, "instance", "10.0.0.2:9100"), // dev 主机：被条件丢弃（表达式读的是富化标签）
			ser(92, "instance", "10.0.0.3:9100"), // 交换机：不在资源范围内
			ser(93, "instance", "10.0.0.9:9100"), // 没有对应的资源：不在资源范围内
		)}
	}
	set := mergeSamples(rule, testDSID, results(), pipe, index("prod"))
	if len(set.Kept) != 1 || set.DroppedCount != 1 || set.OutOfScopeCount != 2 || len(set.Dropped) != 1 || len(set.OutOfScope) != 2 {
		t.Fatalf("kept %d, dropped %d, out of scope %d", len(set.Kept), set.DroppedCount, set.OutOfScopeCount)
	}
	kept := set.Kept[0]
	// 改写在指纹之前：host 进指纹；富化在指纹之后：ci_ 标签不进指纹。
	want := monitorcontract.Fingerprint(rule.ID, testDSID, map[string]string{"instance": "10.0.0.1:9100", "host": "10.0.0.1", "team": "ops"}, 0)
	if kept.Fingerprint != want {
		t.Fatalf("fingerprint must cover the relabel result and nothing from the CI")
	}
	wantLabels := map[string]string{
		"instance": "10.0.0.1:9100", "host": "10.0.0.1", "team": "ops",
		"ci_env": "prod", "ci_owner": "alice", "ci_model": "host", "ci_tag_core": "true",
	}
	if !reflect.DeepEqual(kept.Labels, wantLabels) || kept.ciID() != "ci-1" || kept.CI.Maintenance() {
		t.Fatalf("labels = %v, ci = %q", kept.Labels, kept.ciID())
	}
	// 资源的属性变了：标签跟着变，指纹不变（告警不会重建）。
	moved := mergeSamples(rule, testDSID, results(), pipe, index("staging"))
	if len(moved.Kept) != 1 || moved.Kept[0].Fingerprint != want || moved.Kept[0].Labels["ci_env"] != "staging" {
		t.Fatalf("a CI attribute change must not change the fingerprint: %+v", moved.Kept)
	}
	// 资源索引尚未加载：不关联；启用了资源范围的规则不产生事件。
	if none := mergeSamples(rule, testDSID, results(), pipe, nil); len(none.Kept) != 0 || none.OutOfScopeCount != 4 {
		t.Fatalf("without a CI index a scoped rule must keep nothing: kept %d, out of scope %d", len(none.Kept), none.OutOfScopeCount)
	}

	// 状态机把被丢弃 / 范围外的点记进判定记录；对已有的事件，它们等同于本轮没有出现。
	s := newSim(t, rule)
	s.cis = index("prod")
	out := s.step(at(0), results()...)
	if got := steps(out); !equalStrings(got, []string{"dropped", "fired", "out_of_scope", "out_of_scope"}) {
		t.Fatalf("steps = %v", got)
	}
	if a := s.alert(); a.CIID != "ci-1" || a.Labels["ci_env"] != "prod" {
		t.Fatalf("alert = %+v", a)
	}
	s.dispatch(at(0))
	s.cis = index("dev") // 这台主机改成了 dev：样本被丢弃，事件走恢复
	out = s.step(at(60), results()...)
	if a := s.alert(); a.Status != monitorcontract.AlertResolved || out.Recovered != 1 {
		t.Fatalf("an alert whose series is now dropped must resolve, status = %d", a.Status)
	}
}

func TestPipelineDropRunsBeforeLevelInhibit(t *testing.T) {
	rule := baseRule(func(r *Rule) {
		r.Queries = []monitorcontract.Query{
			{Expr: "cpu > 85", Severity: monitorcontract.SeverityWarning},
			{Expr: "cpu > 95", Severity: monitorcontract.SeverityCritical},
		}
		r.Pipeline.DropExpr = `{{ if gt $value 94.0 }}true{{ end }}`
	})
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	set := mergeSamples(rule, testDSID, [][]promclient.Series{q(ser(90, "instance", "a")), q(ser(96, "instance", "a"))}, pipe, nil)
	// 严重那一档被丢弃：它不能再去抑制警告那一档。
	if len(set.Kept) != 1 || set.Kept[0].Severity != monitorcontract.SeverityWarning || len(set.Inhibited) != 0 || set.DroppedCount != 1 {
		t.Fatalf("kept = %+v, inhibited = %d, dropped = %d", set.Kept, len(set.Inhibited), set.DroppedCount)
	}
	// 渲染失败的丢弃表达式不丢弃，并计数。
	rule.Pipeline.DropExpr = `{{ humanize $labels.instance }}`
	pipe, err = monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	set = mergeSamples(rule, testDSID, [][]promclient.Series{q(ser(90, "instance", "a")), nil}, pipe, nil)
	if len(set.Kept) != 1 || set.DropErrors != 1 {
		t.Fatalf("kept = %d, drop errors = %d", len(set.Kept), set.DropErrors)
	}
	if note := dropErrorNote(set.DropErrors); !strings.HasPrefix(note, dropErrorPrefix) {
		t.Fatalf("note = %q", note)
	}
}

// —— 抑制结论（快照上的不动点）——

func TestInhibitViewConsistencyModel(t *testing.T) {
	chain := []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b", "host"), inhibitRule("i2", "b", "c", "host")}
	t.Run("an inhibited alert cannot inhibit others", func(t *testing.T) {
		v := viewOf(chain, nil, active("1", "kind", "a", "host", "h"), active("2", "kind", "b", "host", "h"), active("3", "kind", "c", "host", "h"))
		if got := inhibitedIDs(v, "1", "2", "3"); got != "2" || v.Pinned != 0 {
			t.Fatalf("inhibited = %s, pinned = %d", got, v.Pinned)
		}
		// 结果与触发的先后无关：c 最早、a 最晚，仍然只有 b 被抑制。
		v = viewOf(chain, nil, active("9", "kind", "a", "host", "h"), active("5", "kind", "b", "host", "h"), active("1", "kind", "c", "host", "h"))
		if got := inhibitedIDs(v, "9", "5", "1"); got != "5" {
			t.Fatalf("inhibited = %s", got)
		}
		// 相等标签不同：互不相干。
		v = viewOf(chain, nil, active("1", "kind", "a", "host", "h1"), active("2", "kind", "b", "host", "h2"))
		if got := inhibitedIDs(v, "1", "2"); got != "" {
			t.Fatalf("inhibited = %s", got)
		}
	})
	t.Run("source gone lifts the inhibition", func(t *testing.T) {
		v := viewOf(chain, nil, active("2", "kind", "b", "host", "h"), active("3", "kind", "c", "host", "h"))
		if got := inhibitedIDs(v, "2", "3"); got != "3" {
			t.Fatalf("inhibited = %s", got)
		}
	})
	t.Run("a cycle is broken deterministically: the oldest alert wins", func(t *testing.T) {
		cycle := []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b", "host"), inhibitRule("i2", "b", "a", "host")}
		for _, ids := range [][2]string{{"1", "2"}, {"2", "1"}} {
			v := viewOf(cycle, nil, active(ids[0], "kind", "a", "host", "h"), active(ids[1], "kind", "b", "host", "h"))
			if got := inhibitedIDs(v, "1", "2"); got != "2" || v.Pinned != 1 {
				t.Fatalf("ids %v: inhibited = %s, pinned = %d; want the younger alert inhibited", ids, got, v.Pinned)
			}
		}
		// 三个告警成环：a → b → c → a。
		ring := []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b"), inhibitRule("i2", "b", "c"), inhibitRule("i3", "c", "a")}
		ring[0].Equal, ring[1].Equal, ring[2].Equal = []string{"host"}, []string{"host"}, []string{"host"}
		v := viewOf(ring, nil, active("1", "kind", "a", "host", "h"), active("2", "kind", "b", "host", "h"), active("3", "kind", "c", "host", "h"))
		if got := inhibitedIDs(v, "1", "2", "3"); got != "2" || v.Pinned != 1 {
			t.Fatalf("ring: inhibited = %s, pinned = %d", got, v.Pinned)
		}
	})
	t.Run("many independent cycles resolve together and the result is stable", func(t *testing.T) {
		cycle := []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b", "host"), inhibitRule("i2", "b", "a", "host")}
		var alerts []ActiveAlert
		for i := range 200 {
			host := fmt.Sprintf("h%03d", i)
			alerts = append(alerts, active(fmt.Sprintf("%04d-a", i), "kind", "a", "host", host), active(fmt.Sprintf("%04d-b", i), "kind", "b", "host", host))
		}
		v := viewOf(cycle, nil, slices.Clone(alerts)...)
		again := viewOf(cycle, nil, slices.Clone(alerts)...)
		if v.Pinned != 200 || !reflect.DeepEqual(v.known, again.known) {
			t.Fatalf("pinned = %d, stable = %v", v.Pinned, reflect.DeepEqual(v.known, again.known))
		}
		for i := range 200 {
			a, _ := v.Known(fmt.Sprintf("%04d-a", i))
			b, _ := v.Known(fmt.Sprintf("%04d-b", i))
			if a || !b {
				t.Fatalf("pair %d: a inhibited = %v, b inhibited = %v", i, a, b)
			}
		}
	})
	t.Run("a chain longer than the pass budget fails open and stays deterministic", func(t *testing.T) {
		var rules []monitorpipeline.InhibitRule
		var alerts []ActiveAlert
		for i := range 100 {
			rules = append(rules, inhibitRule(fmt.Sprintf("i%03d", i), fmt.Sprintf("k%03d", i), fmt.Sprintf("k%03d", i+1), "host"))
			alerts = append(alerts, active(fmt.Sprintf("%04d", i), "kind", fmt.Sprintf("k%03d", i), "host", "h"))
		}
		v := viewOf(rules, nil, slices.Clone(alerts)...)
		again := viewOf(rules, nil, slices.Clone(alerts)...)
		if !reflect.DeepEqual(v.known, again.known) {
			t.Fatal("the result must only depend on the data")
		}
		// 链头没人抑制它；第二个必被抑制；预算之内解开的部分严格交替。
		if got := inhibitedIDs(v, "0000", "0001", "0002", "0003"); got != "0001,0003" {
			t.Fatalf("head of the chain: inhibited = %s", got)
		}
		if v.Pinned == 0 {
			t.Fatal("alerts beyond the budget must be reported as pinned")
		}
	})
	t.Run("topology: an alert on an upstream CI inhibits downstream alerts of opted-in rules", func(t *testing.T) {
		cis := monitorpipeline.NewCIIndex([]monitorpipeline.CI{
			hostCI("vm", "vm-1", "10.0.0.2", "prod", "running", "host", "vm"), // 上游里混进了自己：忽略
			hostCI("host", "host-1", "10.0.0.1", "prod", "running"),
		})
		up := onCI(active("1", "kind", "hostdown"), "host", false)
		v := viewOf(nil, cis, up, onCI(active("2", "kind", "cpu"), "vm", true), onCI(active("3", "kind", "mem"), "vm", false))
		if got := inhibitedIDs(v, "1", "2", "3"); got != "2" {
			t.Fatalf("inhibited = %s", got)
		}
		ref := monitorpipeline.AlertRef{ID: "2"}
		if hit, ok := v.Match(&ref, nil, true); !ok || hit.Kind != monitorcontract.InhibitTopology || hit.AlertID != "1" {
			t.Fatalf("hit = %+v, %v", hit, ok)
		}
		// 同一个资源上的两个告警不互相抑制（资源不是自己的上游）。
		v = viewOf(nil, cis, onCI(active("2", "kind", "cpu"), "vm", true), onCI(active("3", "kind", "mem"), "vm", true))
		if got := inhibitedIDs(v, "2", "3"); got != "" {
			t.Fatalf("alerts on the same CI inhibited each other: %s", got)
		}
		// 快照之外的事件（刚要触发）对着快照现算。
		fresh := monitorpipeline.AlertRef{CIID: "vm", Labels: map[string]string{"kind": "disk"}}
		v = viewOf(nil, cis, up)
		if _, ok := v.Match(&fresh, []string{"host", "vm"}, true); !ok {
			t.Fatal("a new event on a downstream CI must be inhibited")
		}
		if _, ok := v.Match(&fresh, []string{"host", "vm"}, false); ok {
			t.Fatal("topology inhibition is opt-in per rule")
		}
	})
	t.Run("nil view inhibits nothing", func(t *testing.T) {
		var v *InhibitView
		ref := monitorpipeline.AlertRef{ID: "x"}
		if _, ok := v.Match(&ref, []string{"a"}, true); ok || v.Size() != 0 {
			t.Fatal("nil view must be inert")
		}
		if _, known := v.Known("x"); known {
			t.Fatal("nil view knows nothing")
		}
	})
}

// —— 状态机：抑制 ——

// sourceUp / sourceGone 源告警在 / 不在时的抑制结论：规则「kind=down 抑制同 instance 的 team=ops」。
func sourceView(up bool, targetID string) *InhibitView {
	rules := []monitorpipeline.InhibitRule{{
		ID:     "inh-1",
		Source: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("kind", "down")}},
		Target: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("team", "ops")}},
		Equal:  []string{"instance"},
	}}
	var alerts []ActiveAlert
	if up {
		alerts = append(alerts, active("src-1", "kind", "down", "instance", "a"))
	}
	if targetID != "" {
		alerts = append(alerts, ActiveAlert{Ref: monitorpipeline.AlertRef{
			ID: targetID, RuleID: testRuleID, GroupPath: "/g/", Severity: 2, Labels: map[string]string{"instance": "a", "team": "ops"},
		}})
	}
	return viewOf(rules, nil, alerts...)
}

func opsRule(mod ...func(*Rule)) *Rule {
	return baseRule(append([]func(*Rule){func(r *Rule) {
		r.For = 0
		r.Labels = map[string]string{"team": "ops"}
		r.Actions = monitorcontract.RuleActions{
			Ticket:    monitorcontract.TicketAction{Enabled: true, TemplateID: "tpl-1", AgentUserID: "agent-1"},
			Workflow:  monitorcontract.WorkflowAction{Enabled: true, DefinitionKey: "wf", AgentUserID: "agent-2"},
			AISummary: true,
		}
	}}, mod...)...)
}

func TestAdvanceInhibitedAlertIsStoredSilentAndNotifiedOnLift(t *testing.T) {
	s := newSim(t, opsRule())
	s.inhibits = sourceView(true, "")
	out := s.step(at(0), one...)
	a := s.alert()
	if !a.Inhibited || a.Inhibit != (monitorpipeline.InhibitHit{Kind: monitorcontract.InhibitRule, InhibitID: "inh-1", AlertID: "src-1"}) || a.Muted {
		t.Fatalf("alert = %+v", a)
	}
	if len(s.outbox) != 0 || len(s.actions) != 0 || s.eventTypes() != "fire,inhibit" {
		t.Fatalf("outbox = %v, actions = %v, events = %s", s.kinds(), s.actionKinds(), s.eventTypes())
	}
	if got := steps(out); !equalStrings(got, []string{"inhibited_rule"}) || out.Points[0].InhibitAlertID != "src-1" {
		t.Fatalf("steps = %v, point = %+v", got, out.Points[0])
	}
	// 仍被抑制：什么都不发，时间线不再增长，告警行不重写（内容没变、没到心跳）。
	s.inhibits = sourceView(true, a.ID)
	out = s.step(at(30), one...)
	if len(s.outbox) != 0 || len(s.events) != 2 || len(out.Transition.AlertUpdates) != 0 {
		t.Fatalf("outbox = %v, events = %s, updates = %d", s.kinds(), s.eventTypes(), len(out.Transition.AlertUpdates))
	}
	// 抑制解除：按一次新通知处理，事件动作在这一轮补上。
	s.inhibits = sourceView(false, a.ID)
	out = s.step(at(60), one...)
	if a.Inhibited || a.Inhibit != (monitorpipeline.InhibitHit{}) {
		t.Fatalf("alert still inhibited: %+v", a)
	}
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) || !out.Transition.Outbox[0].Rearm {
		t.Fatalf("outbox = %v", got)
	}
	if got := s.actionKinds(); !slices.Equal(got, []int32{monitorcontract.ActionTicket, monitorcontract.ActionWorkflow, monitorcontract.ActionAISummary}) {
		t.Fatalf("actions = %v", got)
	}
	if s.eventTypes() != "fire,inhibit,uninhibit" || s.events[2].Inhibit.AlertID != "src-1" {
		t.Fatalf("events = %s, detail = %+v", s.eventTypes(), s.events[2])
	}
	if got := steps(out); !equalStrings(got, []string{"firing"}) {
		t.Fatalf("steps = %v", got)
	}
}

func TestAdvanceInhibitLiftIsBoundedWhenTheSourceFlaps(t *testing.T) {
	s := newSim(t, opsRule(func(r *Rule) { r.RepeatInterval = time.Hour }))
	s.step(at(0), one...)
	s.dispatch(at(0))
	id := s.alert().ID
	// 源告警每 20 秒抖一次，持续 4 分钟：距上次通知不足 5 分钟，解除时不补发。
	for i := 1; i <= 12; i++ {
		s.inhibits = sourceView(i%2 == 1, id)
		s.step(at(i*20), one...)
		s.dispatch(at(i * 20))
	}
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
		t.Fatalf("a flapping source caused notifications: %v", got)
	}
	// 被抑制了一阵之后解除：距上次通知已满 5 分钟，补发一条（重复通知，下一个序号）。
	s.inhibits = sourceView(true, id)
	s.step(at(300), one...)
	s.inhibits = sourceView(false, id)
	out := s.step(at(330), one...)
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2"}) || !out.Transition.Outbox[0].Rearm {
		t.Fatalf("outbox = %v", got)
	}
	// 时间线：每次进出各一条——由数据层封顶（见 maxFlapEvents）。
	if n := s.timeline(monitorcontract.AlertEventInhibit) + s.timeline(monitorcontract.AlertEventUninhibit); n != 14 {
		t.Fatalf("inhibit / uninhibit events = %d", n)
	}
}

func TestAdvanceInhibitedAlertsNeverRepeatOrSendRecovery(t *testing.T) {
	s := newSim(t, opsRule(func(r *Rule) { r.RepeatInterval = 5 * time.Minute }))
	s.step(at(0), one...)
	s.dispatch(at(0))
	id := s.alert().ID
	s.inhibits = sourceView(true, id)
	for m := 1; m <= 30; m++ {
		s.step(atM(m), one...)
	}
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
		t.Fatalf("an inhibited alert repeated: %v", got)
	}
	// 被抑制期间恢复：不发恢复通知；已开自动建单的规则照常请 core 在工单上追加记录。
	s.step(atM(31))
	if a := s.alert(); a.Status != monitorcontract.AlertResolved {
		t.Fatalf("status = %d", a.Status)
	}
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
		t.Fatalf("recovery of an inhibited alert was notified: %v", got)
	}
	if got := s.actionKinds(); got[len(got)-1] != monitorcontract.ActionTicketNote {
		t.Fatalf("actions = %v", got)
	}
}

func TestAdvanceTopologyInhibitAndPrecedenceOverMute(t *testing.T) {
	cis := monitorpipeline.NewCIIndex([]monitorpipeline.CI{
		hostCI("vm", "vm-1", "10.0.0.2", "prod", "running", "host"),
		hostCI("host", "host-1", "10.0.0.1", "prod", "running"),
	})
	// 每轮新建：合并样本会接管并改写序列的标签。
	series := func() [][]promclient.Series { return [][]promclient.Series{q(ser(90, "instance", "10.0.0.2:9100"))} }
	upstream := viewOf(nil, cis, onCI(active("up-1", "kind", "hostdown"), "host", false))
	t.Run("opted-in rule is inhibited by the upstream alert", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For, r.TopologyInhibit = 0, true }))
		s.cis, s.inhibits = cis, upstream
		out := s.step(at(0), series()...)
		a := s.alert()
		if !a.Inhibited || a.Inhibit.Kind != monitorcontract.InhibitTopology || a.Inhibit.AlertID != "up-1" || a.Inhibit.InhibitID != "" || a.CIID != "vm" {
			t.Fatalf("alert = %+v", a)
		}
		if got := steps(out); !equalStrings(got, []string{"inhibited_topology"}) || len(s.outbox) != 0 {
			t.Fatalf("steps = %v, outbox = %v", got, s.kinds())
		}
	})
	t.Run("rule without the switch is not", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
		s.cis, s.inhibits = cis, upstream
		s.step(at(0), series()...)
		if a := s.alert(); a.Inhibited || len(s.outbox) != 1 {
			t.Fatalf("alert = %+v, outbox = %v", a, s.kinds())
		}
	})
	t.Run("inhibition is evaluated before mutes; both are recorded", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For, r.TopologyInhibit = 0, true }))
		s.cis, s.inhibits = cis, upstream
		s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 600)}
		out := s.step(at(0), series()...)
		a := s.alert()
		if !a.Inhibited || !a.Muted || a.MuteID != "m1" || s.eventTypes() != "fire,inhibit,mute" {
			t.Fatalf("alert = %+v, events = %s", a, s.eventTypes())
		}
		if got := steps(out); !equalStrings(got, []string{"inhibited_topology"}) {
			t.Fatalf("steps = %v", got)
		}
		// 抑制先解除、屏蔽还在：仍不通知；屏蔽也解除后才通知，且只通知一次。
		s.inhibits = viewOf(nil, cis)
		s.step(at(60), series()...)
		if len(s.outbox) != 0 || a.Inhibited || !a.Muted {
			t.Fatalf("outbox = %v, alert = %+v", s.kinds(), a)
		}
		s.step(at(660), series()...)
		s.step(at(720), series()...)
		if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
			t.Fatalf("outbox = %v", got)
		}
	})
	t.Run("event-mode mute still wins: no alert at all", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For, r.TopologyInhibit = 0, true }))
		s.cis, s.inhibits = cis, upstream
		s.mutes = []*Mute{onceMute("m2", monitorcontract.MuteModeEvent, 0, 600)}
		out := s.step(at(0), series()...)
		if len(s.alerts) != 0 || !equalStrings(steps(out), []string{"muted_event"}) || len(s.events) != 0 || len(s.actions) != 0 {
			t.Fatalf("alerts = %d, steps = %v", len(s.alerts), steps(out))
		}
	})
}

// —— 状态机：维护态自动屏蔽 ——

func TestAdvanceMaintenanceMutesNotificationsOnly(t *testing.T) {
	index := func(status string) *monitorpipeline.CIIndex {
		return monitorpipeline.NewCIIndex([]monitorpipeline.CI{hostCI("ci-1", "web-1", "10.0.0.1", "prod", status)})
	}
	series := func() [][]promclient.Series { return [][]promclient.Series{q(ser(90, "instance", "10.0.0.1:9100"))} }
	s := newSim(t, opsRule())
	s.cis = index(monitorcontract.CIStatusMaintenance)
	out := s.step(at(0), series()...)
	a := s.alert()
	// muted = true 而 mute_id 为空 = 因资源维护态而屏蔽。
	if !a.Muted || a.MuteID != "" || a.Inhibited || a.CIID != "ci-1" {
		t.Fatalf("alert = %+v", a)
	}
	if got := steps(out); !equalStrings(got, []string{"muted_maintenance"}) || len(s.outbox) != 0 || len(s.actions) != 0 {
		t.Fatalf("steps = %v, outbox = %v, actions = %v", got, s.kinds(), s.actionKinds())
	}
	if s.eventTypes() != "fire,mute" || s.events[1].Detail().CIID != "ci-1" || s.events[1].Detail().MuteID != "" {
		t.Fatalf("events = %s, detail = %+v", s.eventTypes(), s.events[1].Detail())
	}
	// 屏蔽规则同时命中时，记的是屏蔽规则。
	s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 30, 90)}
	s.step(at(60), series()...)
	if !a.Muted || a.MuteID != "m1" || s.timeline(monitorcontract.AlertEventMute) != 1 {
		t.Fatalf("alert = %+v, mute events = %d", a, s.timeline(monitorcontract.AlertEventMute))
	}
	// 维护结束：按一次新通知处理，事件动作补上。
	s.cis = index("running")
	s.step(at(120), series()...)
	if a.Muted || !equalStrings(s.kinds(), []string{"1/1"}) || len(s.actions) != 3 || s.timeline(monitorcontract.AlertEventUnmute) != 1 {
		t.Fatalf("alert = %+v, outbox = %v, actions = %v, events = %s", a, s.kinds(), s.actionKinds(), s.eventTypes())
	}
	s.dispatch(at(120))
	// 又进入维护，其间恢复：不发恢复通知。
	s.cis = index(monitorcontract.CIStatusMaintenance)
	s.step(at(180), series()...)
	s.step(at(240))
	if a.Status != monitorcontract.AlertResolved || !a.Muted || !equalStrings(s.kinds(), []string{"1/1"}) {
		t.Fatalf("alert = %+v, outbox = %v", a, s.kinds())
	}
}

// —— 状态机：认领 ——

func TestAdvanceAckStopsRuleNotificationsAndUnackResumesCleanly(t *testing.T) {
	s := newSim(t, baseRule(func(r *Rule) { r.For, r.RepeatInterval = 0, 10*time.Minute }))
	run := func(from, to int) {
		for m := from; m <= to; m++ {
			s.step(atM(m), one...)
			s.dispatch(atM(m))
		}
	}
	run(0, 25) // 0 分触发，10、20 分重复
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2", "2/3"}) {
		t.Fatalf("outbox = %v", got)
	}
	a := s.alert()
	a.AckTime = atM(25)
	run(26, 90)
	if got := s.kinds(); len(got) != 3 {
		t.Fatalf("an acknowledged alert kept repeating: %v", got)
	}
	// 取消认领：距上次通知早已超过间隔，立刻补一条；之后回到每 10 分钟一条，不把认领期间攒下的次数一次发完。
	a.AckTime = time.Time{}
	run(91, 91)
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2", "2/3", "2/4"}) {
		t.Fatalf("outbox = %v", got)
	}
	run(92, 100)
	if got := s.kinds(); len(got) != 4 {
		t.Fatalf("un-ack caused a burst: %v", got)
	}
	run(101, 101)
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "2/2", "2/3", "2/4", "2/5"}) {
		t.Fatalf("outbox = %v", got)
	}
	// 认领期间恢复：恢复通知照发（认领只停止提醒）。
	a.AckTime = atM(102)
	s.step(atM(103))
	if got := s.kinds(); got[len(got)-1] != "3/6" {
		t.Fatalf("outbox = %v", got)
	}
}

func TestAdvanceAckedWhileSuppressedGetsItsFirstNotificationAfterUnack(t *testing.T) {
	s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
	s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 120)}
	s.step(at(0), one...)
	a := s.alert()
	a.AckTime = at(60)
	s.step(at(180), one...) // 屏蔽解除，但正被认领：不通知
	s.step(at(240), one...)
	if len(s.outbox) != 0 {
		t.Fatalf("outbox = %v", s.kinds())
	}
	a.AckTime = time.Time{}
	s.step(at(300), one...)
	s.step(at(360), one...)
	if got := s.kinds(); !equalStrings(got, []string{"1/1"}) {
		t.Fatalf("outbox = %v", got)
	}
}

// —— 状态机：升级 ——

func escalationPolicy(mode int32) *monitorpipeline.EscalationPolicy {
	return &monitorpipeline.EscalationPolicy{
		Mode: mode, MinSeverity: monitorcontract.SeverityWarning,
		Levels: []monitorcontract.EscalationLevel{
			{Trigger: monitorcontract.EscalationTriggerUnacked, AfterM: 15, RepeatIntervalM: 15, MaxNotify: 3},
			{Trigger: monitorcontract.EscalationTriggerUnresolved, AfterM: 60, RepeatIntervalM: 60, MaxNotify: 2},
		},
	}
}

// runEscalation 每分钟评估并派发一轮，共 minutes 分钟；返回各类通知入箱的时刻（分钟）。
func runEscalation(s *sim, from, minutes int) map[string][]int {
	seen := map[*simOutbox]bool{}
	for _, o := range s.outbox {
		seen[o] = true
	}
	at := map[string][]int{}
	for m := from; m < from+minutes; m++ {
		s.step(atM(m), one...)
		for _, o := range s.outbox {
			if !seen[o] {
				seen[o] = true
				key := "rule"
				if o.Kind == monitorcontract.NotifyKindEscalation {
					key = fmt.Sprintf("L%d", o.Level)
				}
				at[key] = append(at[key], m)
			}
		}
		s.dispatch(atM(m))
	}
	return at
}

func TestAdvanceEscalationTimelines(t *testing.T) {
	rule := func() *Rule {
		return baseRule(func(r *Rule) { r.For, r.RepeatInterval, r.MaxNotify = 0, 30*time.Minute, 5 })
	}
	t.Run("stack: every level and the rule repeat on their own cadence and caps", func(t *testing.T) {
		s := newSim(t, rule())
		s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
		got := runEscalation(s, 0, 24*60)
		want := map[string][]int{"rule": {0, 30, 60, 90, 120}, "L1": {15, 30, 45}, "L2": {60, 120}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("timeline = %v, want %v", got, want)
		}
		// 与保存策略时给出的 24 小时估算是同一份逻辑。
		est := monitorpipeline.EstimateVolume(s.escalation, 30, 5)
		if int(est.Rule) != len(got["rule"]) || int(est.Levels[0]) != len(got["L1"]) || int(est.Levels[1]) != len(got["L2"]) {
			t.Fatalf("estimate = %+v, simulated = %v", est, got)
		}
		// 发件箱的编码：seq = level × 100000 + 级内序号；时间线只记「升级到第几级」。
		if kinds := s.kinds(); !slices.Contains(kinds, "4/100001") || !slices.Contains(kinds, "4/100003") || !slices.Contains(kinds, "4/200002") {
			t.Fatalf("outbox = %v", kinds)
		}
		if a := s.alert(); a.EscalationLevel != 2 || s.timeline(monitorcontract.AlertEventEscalate) != 2 || a.NotifyCount != 5 {
			t.Fatalf("level = %d, escalate events = %d, notify_count = %d", a.EscalationLevel, s.timeline(monitorcontract.AlertEventEscalate), a.NotifyCount)
		}
	})
	t.Run("takeover: escalating silences the rule recipients and earlier levels", func(t *testing.T) {
		s := newSim(t, rule())
		s.escalation = escalationPolicy(monitorcontract.EscalationModeTakeover)
		got := runEscalation(s, 0, 24*60)
		want := map[string][]int{"rule": {0}, "L1": {15, 30, 45}, "L2": {60, 120}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("timeline = %v, want %v", got, want)
		}
		est := monitorpipeline.EstimateVolume(s.escalation, 30, 5)
		if est.Total != 6 || int(est.Rule) != len(got["rule"]) {
			t.Fatalf("estimate = %+v", est)
		}
		// 恢复通知不受接替影响。
		s.step(atM(24*60 + 1))
		if kinds := s.kinds(); kinds[len(kinds)-1] != "3/2" {
			t.Fatalf("outbox = %v", kinds)
		}
	})
	t.Run("takeover with a slower second level: level 1 goes quiet once level 2 is reached", func(t *testing.T) {
		s := newSim(t, rule())
		p := escalationPolicy(monitorcontract.EscalationModeTakeover)
		p.Levels[0].MaxNotify, p.Levels[0].RepeatIntervalM = 0, 20
		s.escalation = p
		got := runEscalation(s, 0, 6*60)
		if !reflect.DeepEqual(got["L1"], []int{15, 35, 55}) || !reflect.DeepEqual(got["L2"], []int{60, 120}) {
			t.Fatalf("timeline = %v", got)
		}
	})
	t.Run("severity filter", func(t *testing.T) {
		s := newSim(t, rule())
		p := escalationPolicy(monitorcontract.EscalationModeStack)
		p.MinSeverity = monitorcontract.SeverityCritical
		s.escalation = p
		got := runEscalation(s, 0, 180)
		if len(got["L1"])+len(got["L2"]) != 0 || s.alert().EscalationLevel != 0 {
			t.Fatalf("a warning alert escalated under a critical-only policy: %v", got)
		}
	})
	t.Run("the level on the alert row never goes back", func(t *testing.T) {
		s := newSim(t, rule())
		s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
		runEscalation(s, 0, 70)
		if s.alert().EscalationLevel != 2 {
			t.Fatalf("level = %d", s.alert().EscalationLevel)
		}
		s.escalation = nil // 策略被停用
		before := len(s.outbox)
		runEscalation(s, 70, 5)
		s.mutes = []*Mute{onceMute("m", monitorcontract.MuteModeNotify, 0, 1<<30)}
		s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
		runEscalation(s, 75, 5)
		if a := s.alert(); a.EscalationLevel != 2 {
			t.Fatalf("level went back to %d", a.EscalationLevel)
		}
		for _, k := range s.kinds()[before:] {
			if strings.HasPrefix(k, "4/") {
				t.Fatalf("escalated without a policy / while muted: %v", s.kinds()[before:])
			}
		}
	})
	t.Run("ack stops unacked levels and rule repeats, unresolved levels go on; un-ack resumes", func(t *testing.T) {
		s := newSim(t, rule())
		s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
		got := runEscalation(s, 0, 20) // 0 分触发，15 分升到第 1 级
		if !reflect.DeepEqual(got, map[string][]int{"rule": {0}, "L1": {15}}) {
			t.Fatalf("timeline = %v", got)
		}
		s.alert().AckTime = atM(20)
		got = runEscalation(s, 20, 110)
		if !reflect.DeepEqual(got, map[string][]int{"L2": {60, 120}}) {
			t.Fatalf("acked timeline = %v", got)
		}
		s.alert().AckTime = time.Time{}
		got = runEscalation(s, 130, 40)
		// 第 1 级接着数：第 2、3 次，间隔仍是 15 分钟；规则的重复通知同样只补一条再回到节奏上。
		if !reflect.DeepEqual(got, map[string][]int{"rule": {130, 160}, "L1": {130, 145}}) {
			t.Fatalf("un-acked timeline = %v", got)
		}
	})
	t.Run("muted and inhibited alerts never escalate; lifting resumes", func(t *testing.T) {
		for name, suppress := range map[string]func(*sim, bool){
			"muted": func(s *sim, on bool) {
				s.mutes = nil
				if on {
					s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 1<<30)}
				}
			},
			"inhibited": func(s *sim, on bool) { s.inhibits = sourceView(on, "") },
		} {
			s := newSim(t, opsRule(func(r *Rule) { r.Actions = monitorcontract.RuleActions{} }))
			s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
			suppress(s, true)
			got := runEscalation(s, 0, 180)
			if len(got) != 0 || s.alert().EscalationLevel != 0 {
				t.Fatalf("%s: timeline = %v, level = %d", name, got, s.alert().EscalationLevel)
			}
			suppress(s, false)
			got = runEscalation(s, 180, 1)
			if !reflect.DeepEqual(got, map[string][]int{"rule": {180}, "L1": {180}, "L2": {180}}) {
				t.Fatalf("%s: after lifting = %v", name, got)
			}
		}
	})
	t.Run("stops on resolve", func(t *testing.T) {
		s := newSim(t, rule())
		s.escalation = escalationPolicy(monitorcontract.EscalationModeStack)
		runEscalation(s, 0, 20)
		before := len(s.outbox)
		for m := 20; m < 200; m++ {
			s.step(atM(m))
		}
		if a := s.alert(); a.Status != monitorcontract.AlertResolved {
			t.Fatalf("status = %d", a.Status)
		}
		// 恢复之后只多出一条恢复通知。
		if got := s.kinds()[before:]; !equalStrings(got, []string{"3/2"}) {
			t.Fatalf("outbox after resolve = %v", got)
		}
	})
	t.Run("a disabled or deleted policy means no escalation", func(t *testing.T) {
		s := newSim(t, rule())
		got := runEscalation(s, 0, 120)
		if len(got["L1"]) != 0 || !reflect.DeepEqual(got["rule"], []int{0, 30, 60, 90}) {
			t.Fatalf("timeline = %v", got)
		}
	})
}

func TestAdvanceRearmsADroppedEscalationNotification(t *testing.T) {
	for _, tc := range []struct {
		name    string
		repeatM int32
		retryM  int
	}{{"level repeat interval", 15, 15}, {"no interval: every 10 minutes", 0, 10}} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
			p := escalationPolicy(monitorcontract.EscalationModeStack)
			p.Levels = p.Levels[:1]
			p.Levels[0].RepeatIntervalM = tc.repeatM
			s.escalation = p
			s.step(atM(0), one...)
			s.dispatch(atM(0))
			s.step(atM(15), one...)
			s.drop(atM(15)) // core 派发失败次数用尽：行被丢弃，该级的次数不前进
			for m := 16; m < 15+tc.retryM; m++ {
				out := s.step(atM(m), one...)
				if len(out.Transition.Outbox) != 0 {
					t.Fatalf("minute %d: re-armed too early: %+v", m, out.Transition.Outbox)
				}
			}
			out := s.step(atM(15+tc.retryM), one...)
			if len(out.Transition.Outbox) != 1 || !out.Transition.Outbox[0].Rearm || out.Transition.Outbox[0].Seq != 100001 || s.rearmed != 1 {
				t.Fatalf("outbox = %+v, rearmed = %d", out.Transition.Outbox, s.rearmed)
			}
			// 同一行被重新置为待派发，没有新增行；待派发期间不再追加；时间线不重复记「升级」。
			if got := s.kinds(); !equalStrings(got, []string{"1/1", "4/100001"}) || s.timeline(monitorcontract.AlertEventEscalate) != 1 {
				t.Fatalf("outbox = %v, escalate events = %d", got, s.timeline(monitorcontract.AlertEventEscalate))
			}
			if out := s.step(atM(16+tc.retryM), one...); len(out.Transition.Outbox) != 0 {
				t.Fatalf("appended while pending: %+v", out.Transition.Outbox)
			}
			s.dispatch(atM(16 + tc.retryM))
			if a := s.alert(); a.Levels[0].NotifyCount != 1 {
				t.Fatalf("level progress = %+v", a.Levels)
			}
		})
	}
}

// —— 状态机：事件动作 ——

func TestAdvanceAlertActions(t *testing.T) {
	t.Run("queued once on fire with a snapshot of the rule config", func(t *testing.T) {
		s := newSim(t, opsRule())
		out := s.step(at(0), one...)
		want := []ActionInsert{
			{Kind: monitorcontract.ActionTicket, Payload: monitorcontract.ActionPayload{TemplateID: "tpl-1", AgentUserID: "agent-1"}},
			{Kind: monitorcontract.ActionWorkflow, Payload: monitorcontract.ActionPayload{DefinitionKey: "wf", AgentUserID: "agent-2"}},
			{Kind: monitorcontract.ActionAISummary},
		}
		if len(out.Transition.Actions) != 3 {
			t.Fatalf("actions = %+v", out.Transition.Actions)
		}
		for i, a := range out.Transition.Actions {
			if a.Kind != want[i].Kind || a.Payload != want[i].Payload || a.AlertID != s.alert().ID || a.ID == "" {
				t.Fatalf("action %d = %+v", i, a)
			}
		}
		// 之后的轮次不再产生动作行（不靠唯一键兜底）。
		for i := 1; i < 5; i++ {
			if out := s.step(at(i*60), one...); len(out.Transition.Actions) != 0 {
				t.Fatalf("round %d queued actions again: %+v", i, out.Transition.Actions)
			}
		}
	})
	t.Run("only configured kinds", func(t *testing.T) {
		s := newSim(t, opsRule(func(r *Rule) { r.Actions = monitorcontract.RuleActions{AISummary: true} }))
		s.step(at(0), one...)
		if got := s.actionKinds(); !slices.Equal(got, []int32{monitorcontract.ActionAISummary}) {
			t.Fatalf("actions = %v", got)
		}
		// 没开自动建单、事件也没关联工单：恢复时不入队 ticket_note。
		s.step(at(60))
		if got := s.actionKinds(); len(got) != 1 {
			t.Fatalf("actions = %v", got)
		}
	})
	t.Run("notify-only muted alerts queue nothing until unmuted; re-muting does not queue again", func(t *testing.T) {
		s := newSim(t, opsRule())
		s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, 0, 100), onceMute("m2", monitorcontract.MuteModeNotify, 200, 300)}
		s.step(at(0), one...)
		s.step(at(60), one...)
		if len(s.actions) != 0 {
			t.Fatalf("actions while muted = %v", s.actionKinds())
		}
		out := s.step(at(120), one...)
		if len(out.Transition.Actions) != 3 {
			t.Fatalf("actions on unmute = %+v", out.Transition.Actions)
		}
		s.step(at(240), one...)
		s.step(at(360), one...) // 第二次解除屏蔽：写出同样的三行，被 (alert_id, kind) 唯一键吸收
		if len(s.actions) != 3 {
			t.Fatalf("actions = %v", s.actionKinds())
		}
	})
	t.Run("event-mode mute: no alert, no action", func(t *testing.T) {
		s := newSim(t, opsRule())
		s.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeEvent, 0, 600)}
		s.step(at(0), one...)
		if len(s.actions) != 0 || len(s.alerts) != 0 {
			t.Fatalf("actions = %v", s.actionKinds())
		}
	})
	t.Run("resolve of a ticketed alert queues a ticket note", func(t *testing.T) {
		s := newSim(t, baseRule(func(r *Rule) { r.For = 0 }))
		s.step(at(0), one...)
		s.alert().TicketID = "ticket-1" // 手动转工单（core 写）
		out := s.step(at(60))
		if len(out.Transition.Actions) != 1 || out.Transition.Actions[0].Kind != monitorcontract.ActionTicketNote {
			t.Fatalf("actions = %+v", out.Transition.Actions)
		}
		if s.eventTypes() != "fire,resolve" {
			t.Fatalf("events = %s", s.eventTypes())
		}
	})
}

// —— 手动关闭之后条件仍成立 ——

func TestAdvanceAfterManualCloseTheSeriesStartsOver(t *testing.T) {
	s := newSim(t, baseRule(func(r *Rule) { r.RepeatInterval = 5 * time.Minute }))
	s.step(at(0), one...)
	s.step(at(60), one...)
	s.dispatch(at(60))
	first := s.alert()
	// 引擎执行关闭指令的效果：事件置已关闭、它的状态行被删除（数据层的事务，见集成测试）。
	first.Status = monitorcontract.AlertClosed
	delete(s.states, first.Fingerprint)
	// 条件仍成立：按新序列重新走持续时长，期间没有事件、没有通知。
	out := s.step(at(120), one...)
	if got := steps(out); !equalStrings(got, []string{"pending"}) || s.only().State != monitorcontract.StatePending || len(s.alerts) != 1 {
		t.Fatalf("steps = %v", got)
	}
	out = s.step(at(180), one...)
	if got := steps(out); !equalStrings(got, []string{"fired"}) || len(s.alerts) != 2 {
		t.Fatalf("steps = %v, alerts = %d", got, len(s.alerts))
	}
	second := s.alertBy("instance", "a")
	if second.ID == first.ID || second.Fingerprint != first.Fingerprint || first.Status != monitorcontract.AlertClosed {
		t.Fatalf("first = %+v, second = %+v", first, second)
	}
	// 新事件从第 1 次通知数起；旧事件不再有任何通知。
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "1/1"}) {
		t.Fatalf("outbox = %v", got)
	}
}

// —— 纯函数 ——

func TestAdvanceWithP2InputsIsPure(t *testing.T) {
	cis := monitorpipeline.NewCIIndex([]monitorpipeline.CI{hostCI("ci-1", "web-1", "10.0.0.1", "prod", "running")})
	rule := opsRule(func(r *Rule) { r.RepeatInterval, r.TopologyInhibit = time.Minute, true })
	s := newSim(t, rule)
	s.cis, s.escalation = cis, escalationPolicy(monitorcontract.EscalationModeStack)
	series := func() [][]promclient.Series {
		return [][]promclient.Series{q(ser(90, "instance", "10.0.0.1:9100"), ser(80, "instance", "b"))}
	}
	s.step(atM(0), series()...)
	s.dispatch(atM(0))
	s.inhibits = sourceView(true, "")
	build := func() *RoundInput {
		in := s.input(atM(20), series()...)
		n := 0
		in.NewID = func() (string, error) { n++; return fmt.Sprintf("x-%d", n), nil }
		return in
	}
	in, same := build(), build()
	snapshot := func(in *RoundInput) string {
		return fmt.Sprintf("%+v|%+v|%+v|%+v", in.Prev, in.Samples.Kept, *in.Rule, *in.Escalation)
	}
	before := snapshot(in)
	a, err := Advance(in)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot(in) != before {
		t.Fatal("Advance modified its input")
	}
	b, err := Advance(same)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same input, different output")
	}
	if len(a.Transition.Outbox) == 0 || len(a.Transition.Events) == 0 {
		t.Fatalf("the round was expected to do something: %+v", a.Transition)
	}
}
