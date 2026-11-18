package biz

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 第二、三期全开时的基准：2000 条序列，规则带标签改写 + 资源关联与富化（5000 个资源的索引）+ 条件丢弃 +
// 资源范围 + 拓扑抑制，20 条抑制规则（其中一条命中 5% 的事件），两级升级策略。与第一期的同名基准
// （state_test.go）逐项对照。

const benchSeries = 2000

func benchRule(mode string) *Rule {
	return baseRule(func(r *Rule) {
		r.Labels = map[string]string{"team": "ops"}
		r.Annotations = map[string]string{"summary": "{{ $labels.instance }} cpu is {{ $value | printf \"%.1f\" }}"}
		r.TopologyInhibit, r.EscalationID = true, "esc"
		r.Pipeline = monitorpipeline.Spec{
			Relabel: []monitorcontract.RelabelStep{{
				Action: monitorcontract.RelabelReplace, SourceLabels: []string{"instance"}, Regex: `(.+):\d+`, TargetLabel: "host", Replacement: "$1",
			}},
			DropExpr: `{{ if eq $labels.ci_env "dev" }}true{{ end }}`,
			CIMatch:  monitorcontract.CIMatch{EnrichTags: []string{"core"}},
			Scope:    monitorcontract.ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}},
		}
		if mode == "fire" {
			r.For = 0
		}
	})
}

func benchIP(i int) string { return fmt.Sprintf("10.0.%d.%d", i/250, i%250) }

func benchCIs() *monitorpipeline.CIIndex {
	cis := make([]monitorpipeline.CI, 5000)
	for i := range cis {
		cis[i] = hostCI(fmt.Sprintf("ci-%05d", i), fmt.Sprintf("host-%05d", i), benchIP(i), "prod", "running", "ci-rack")
	}
	return monitorpipeline.NewCIIndex(cis)
}

func benchResults(delta float64) [][]promclient.Series {
	out := make([]promclient.Series, benchSeries)
	for i := range out {
		out[i] = promclient.Series{
			Labels: map[string]string{"__name__": "cpu", "instance": benchIP(i) + ":9100", "job": "node", "dc": "bj"},
			Points: []promclient.Point{{Value: float64(i) + delta}},
		}
	}
	return [][]promclient.Series{out}
}

func benchInhibitRules() []monitorpipeline.InhibitRule {
	rules := make([]monitorpipeline.InhibitRule, 0, 20)
	for i := range 19 {
		rules = append(rules, inhibitRule(fmt.Sprintf("inh-%02d", i), fmt.Sprintf("src-%d", i), fmt.Sprintf("dst-%d", i), "instance"))
	}
	return append(rules, monitorpipeline.InhibitRule{
		ID:     "inh-99",
		Source: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("kind", "down")}},
		Target: monitorcontract.InhibitSelector{Matchers: []monitorcontract.Matcher{eq("team", "ops")}},
		Equal:  []string{"instance"},
	})
}

// benchSources 100 个「主机失联」告警：抑制同 instance 的事件（2000 条里的 5%）。
func benchSources() []ActiveAlert {
	out := make([]ActiveAlert, 100)
	for i := range out {
		out[i] = active(fmt.Sprintf("0-src-%04d", i), "kind", "down", "instance", benchIP(i*20)+":9100")
	}
	return out
}

func benchmarkAdvanceP2(b *testing.B, mode string) {
	rule := benchRule(mode)
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		b.Fatal(err)
	}
	cis := benchCIs()
	inhibitor := monitorpipeline.CompileInhibits(benchInhibitRules())
	policy := escalationPolicy(monitorcontract.EscalationModeStack)
	now := at(0)
	view := buildInhibitView(inhibitor, benchSources(), cis)
	var prev []SeriesState
	if mode == "steady" || mode == "change" || mode == "resolve" {
		// 先让全部序列触发，拼出上一轮状态：已通知一次、20 分钟前触发、已升到第 1 级。
		firing := *rule
		firing.For = 0
		n := 0
		in := &RoundInput{
			Rule: &firing, DatasourceID: testDSID, Samples: mergeSamples(rule, testDSID, benchResults(0), pipe, cis), Now: now,
			Inhibits: view, CIs: cis, Annotations: map[string]map[string]string{},
			NewID: func() (string, error) { n++; return fmt.Sprintf("1-%06d", n), nil },
		}
		firing.Annotations = nil
		out, err := Advance(in)
		if err != nil {
			b.Fatal(err)
		}
		alerts := append([]ActiveAlert(nil), benchSources()...)
		byFP := make(map[string]*AlertInsert, benchSeries)
		for i := range out.Transition.AlertInserts {
			a := &out.Transition.AlertInserts[i]
			byFP[a.Fingerprint] = a
			alerts = append(alerts, ActiveAlert{Topology: true, Ref: monitorpipeline.AlertRef{
				ID: a.ID, RuleID: rule.ID, GroupPath: rule.GroupPath, Severity: a.Severity, Labels: a.Labels, CIID: a.CIID,
			}})
		}
		view = buildInhibitView(inhibitor, alerts, cis)
		levels := make([]monitorpipeline.EscalationLevelState, monitorcontract.MaxEscalationLevels)
		levels[0] = monitorpipeline.EscalationLevelState{NotifyCount: 1, LastNotifyTime: now}
		for _, st := range out.Transition.StateInserts {
			a := byFP[st.Fingerprint]
			inhibited, _ := view.Known(a.ID)
			level := int32(1)
			if inhibited {
				level = 0 // 被抑制的事件不升级
			}
			prev = append(prev, SeriesState{
				Fingerprint: st.Fingerprint, State: st.State, Severity: st.Severity, Value: st.Value,
				Labels:      []byte(`{"instance":"10.0.0.1:9100","host":"10.0.0.1","job":"node","dc":"bj","team":"ops","ci_env":"prod"}`),
				ActiveSince: now, LastSeen: now, AlertID: a.ID, NotifyCount: 1, LastNotifyTime: now,
				AlertSeverity: a.Severity, AlertValue: a.Value, LastFireTime: now, FirstFire: now.Add(-20 * time.Minute),
				Inhibited: inhibited, Inhibit: a.Inhibit, CIID: a.CIID, EscalationLevel: level, Escalation: levels,
			})
		}
		if len(prev) != benchSeries {
			b.Fatalf("setup produced %d states", len(prev))
		}
	}
	delta := 0.0
	if mode == "change" {
		delta = 1
	}
	sets := make([]*SampleSet, 0, 64)
	for range 64 {
		if mode == "resolve" {
			sets = append(sets, mergeSamples(rule, testDSID, [][]promclient.Series{nil}, pipe, cis))
		} else {
			sets = append(sets, mergeSamples(rule, testDSID, benchResults(delta), pipe, cis))
		}
	}
	newID := func() (string, error) { return "018f3e1a-0000-7000-8000-000000000000", nil }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		in := &RoundInput{
			Rule: rule, DatasourceID: testDSID, Prev: prev, Samples: sets[i%len(sets)], Now: now.Add(time.Minute), NewID: newID,
			LastStatus: monitorcontract.EvalFiring, LastEval: now, Inhibits: view, CIs: cis, Escalation: policy,
		}
		out, err := Advance(in)
		var missing *MissingAnnotationsError
		if errors.As(err, &missing) {
			in.Annotations = renderAnnotations(rule.Annotations, missing.Samples)
			out, err = Advance(in)
		}
		if err != nil || int(out.Anomalies+out.Recovered) != benchSeries {
			b.Fatalf("unexpected outcome: %v %+v", err, out)
		}
	}
}

func BenchmarkAdvanceP2NewPending(b *testing.B)   { benchmarkAdvanceP2(b, "pending") }
func BenchmarkAdvanceP2FireWithTpl(b *testing.B)  { benchmarkAdvanceP2(b, "fire") }
func BenchmarkAdvanceP2SteadyFiring(b *testing.B) { benchmarkAdvanceP2(b, "steady") }
func BenchmarkAdvanceP2SteadyChange(b *testing.B) { benchmarkAdvanceP2(b, "change") }
func BenchmarkAdvanceP2ResolveAll(b *testing.B)   { benchmarkAdvanceP2(b, "resolve") }

// BenchmarkMergeSamplesP2 流水线的前半段：改写 → 指纹 → 关联 + 富化 → 丢弃 → 范围 → 级别抑制。
func BenchmarkMergeSamplesP2(b *testing.B) {
	rule := benchRule("pending")
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		b.Fatal(err)
	}
	cis := benchCIs()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		results := benchResults(0)
		b.StartTimer()
		if set := mergeSamples(rule, testDSID, results, pipe, cis); len(set.Kept) != benchSeries {
			b.Fatalf("kept %d", len(set.Kept))
		}
	}
}

// BenchmarkBuildInhibitView 一次抑制快照刷新的计算部分：2100 个活跃告警、20 条抑制规则、拓扑抑制。
func BenchmarkBuildInhibitView(b *testing.B) {
	rule := benchRule("fire")
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		b.Fatal(err)
	}
	cis := benchCIs()
	inhibitor := monitorpipeline.CompileInhibits(benchInhibitRules())
	set := mergeSamples(rule, testDSID, benchResults(0), pipe, cis)
	alerts := benchSources()
	for i := range set.Kept {
		s := &set.Kept[i]
		alerts = append(alerts, ActiveAlert{Topology: true, Ref: monitorpipeline.AlertRef{
			ID: fmt.Sprintf("1-%06d", i), RuleID: rule.ID, GroupPath: rule.GroupPath, Severity: s.Severity, Labels: s.Labels, CIID: s.ciID(),
		}})
	}
	b.ReportAllocs()
	for b.Loop() {
		view := buildInhibitView(inhibitor, append([]ActiveAlert(nil), alerts...), cis)
		if view.Size() != len(alerts) || len(view.hits) != 100 {
			b.Fatalf("size = %d, inhibited = %d", view.Size(), len(view.hits))
		}
	}
}

// TestCIIndexMemory 报告资源索引的内存占用（go test -run TestCIIndexMemory -v）。
func TestCIIndexMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("sizing report")
	}
	const n = 100000
	rows := make([]CIRow, n)
	for i := range rows {
		rows[i] = ciRow(fmt.Sprintf("018f3e1a-0000-7000-8000-%012d", i), fmt.Sprintf("10.%d.%d.%d", i/65536, i/256%256, i%256), "prod", at(0), true)
	}
	store := newFakeStore(t, baseRule())
	store.cis = rows
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	before := heapInUse()
	start := time.Now()
	if err := cache.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	after := heapInUse()
	if cache.CIs().Len() != n {
		t.Fatalf("len = %d", cache.CIs().Len())
	}
	t.Logf("%d CIs: load + index %v, heap +%.1f MiB (%.0f bytes per CI)", n, took.Round(time.Millisecond),
		float64(after-before)/(1<<20), float64(after-before)/n)
}

func heapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
