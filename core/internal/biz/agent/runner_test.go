package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

func TestLooseJSON(t *testing.T) {
	cases := map[string]string{
		`{"a":1}`: `{"a":1}`,
		"前言\n```json\n{\"facts\":[]}\n```\n后记": `{"facts":[]}`,
		`说明 {"x":{"y":"}"}} 尾巴`:                `{"x":{"y":"}"}}`,
		`没有 json`:                              "",
		`{"broken":`:                           "",
		"```\n{\"k\":\"v\"}\n```":              `{"k":"v"}`,
		`{"s":"含有 \"转义\" 与 { 花括号"}`:            `{"s":"含有 \"转义\" 与 { 花括号"}`,
	}
	for in, want := range cases {
		got := string(looseJSON(in))
		if got != want {
			t.Errorf("looseJSON(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseFinding(t *testing.T) {
	f := parseFinding(looseJSON(`{"finding":{"title":"  疑似变更引入 ","conclusion":"c","confidence":180,"evidence":["a"],"next_step":"n"}}`))
	if f == nil || f.Title != "疑似变更引入" || f.Confidence != 100 || f.NextStep != "n" {
		t.Fatalf("parseFinding = %+v", f)
	}
	if parseFinding(looseJSON(`{"finding":{"title":""}}`)) != nil {
		t.Fatal("empty title should yield nil")
	}
	if parseFinding(nil) != nil {
		t.Fatal("nil raw should yield nil")
	}
}

func newState(dry bool, auto AutoSpec) *runState {
	occur := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return &runState{
		run:     &Run{ID: "11111111-1111-7111-8111-111111111111"},
		profile: &Profile{Spec: ProfileSpec{Auto: auto}},
		inc:     &IncidentSnapshot{ID: "22222222-2222-7222-8222-222222222222", OccurTime: occur},
		dryRun:  dry,
		seen:    map[string]struct{}{"22222222-2222-7222-8222-222222222222": {}},
		envs:    map[string]struct{}{},
	}
}

func TestDecide_AddFindingRewritesAndLimits(t *testing.T) {
	s := newState(false, DefaultAuto())
	tc := &bizassistant.ToolCall{ToolName: toolAddFinding, Write: true}
	d := s.decide(context.Background(), tc, json.RawMessage(`{"id":"wrong","finding":{"title":"根因","confidence":250,"next_step":"看看","evidence":["e1"]}}`))
	if !d.Approve || d.Capture {
		t.Fatalf("decide = %+v", d)
	}
	var args map[string]any
	if err := json.Unmarshal(d.Rewrite, &args); err != nil {
		t.Fatal(err)
	}
	if args["id"] != s.inc.ID {
		t.Fatalf("incident id not rewritten: %v", args["id"])
	}
	f := args["finding"].(map[string]any)
	if f["runId"] != s.run.ID || f["confidence"].(float64) != 100 || f["nextStep"] != "看看" {
		t.Fatalf("finding not normalized: %v", f)
	}
	if s.finding == nil || s.finding.Confidence != 100 {
		t.Fatalf("finding not recorded: %+v", s.finding)
	}
	// 执行成功后再写一次 → 拒绝。
	s.observe(tc, json.RawMessage(`{"item":{"id":"33333333-3333-7333-8333-333333333333"}}`))
	if s.run.FindingEventID != "33333333-3333-7333-8333-333333333333" {
		t.Fatalf("finding event id = %q", s.run.FindingEventID)
	}
	if d := s.decide(context.Background(), tc, json.RawMessage(`{"finding":{"title":"again"}}`)); d.Approve || d.Capture {
		t.Fatalf("second finding should be rejected: %+v", d)
	}
}

func TestDecide_DryRunCapturesAndValidatesIDs(t *testing.T) {
	s := newState(true, DefaultAuto())
	alert := "44444444-4444-7444-8444-444444444444"
	// 未出现过的告警 → 拒绝；出现过 → 捕获。
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolAttachAlerts, Write: true}, json.RawMessage(`{"alert_ids":["`+alert+`"]}`))
	if d.Approve || d.Capture || !strings.Contains(d.Reason, "出现过") {
		t.Fatalf("unknown alert should be rejected: %+v", d)
	}
	// 只在别处（告警详情）出现过还不够：必须来自故障的关联告警（上下文 / ListIncidentAlerts）。
	s.observe(&bizassistant.ToolCall{ToolName: "MonitorAlertService_GetAlert"}, json.RawMessage(`{"alert":{"id":"`+strings.ToUpper(alert)+`","env":"prod"}}`))
	d = s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolAttachAlerts, Write: true}, json.RawMessage(`{"alert_ids":["`+alert+`"]}`))
	if d.Approve || d.Capture {
		t.Fatalf("alert seen only in GetAlert must still be rejected: %+v", d)
	}
	s.observe(&bizassistant.ToolCall{ToolName: "IncidentService_ListIncidentAlerts"}, json.RawMessage(`{"items":[{"alertId":"`+strings.ToUpper(alert)+`"}]}`))
	d = s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolAttachAlerts, Write: true}, json.RawMessage(`{"alert_ids":["`+alert+`"]}`))
	if !d.Capture || d.Approve {
		t.Fatalf("dry run should capture: %+v", d)
	}
	// 写工具不在允许范围 → 拒绝；读工具 → 放行。
	if d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: "IncidentService_TransitionIncident", Write: true}, json.RawMessage(`{}`)); d.Approve || d.Capture {
		t.Fatalf("unknown write tool should be rejected: %+v", d)
	}
	if d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: "CmdbCIService_GetCI"}, json.RawMessage(`{"id":"x"}`)); !d.Approve {
		t.Fatalf("read tool should be approved: %+v", d)
	}
	if _, ok := s.envs["prod"]; !ok {
		t.Fatal("env not collected from output")
	}
}

func TestDecide_ProposalAutoEligibility(t *testing.T) {
	auto := DefaultAuto()
	auto.Enabled = true
	s := newState(false, auto)
	pb := "55555555-5555-7555-8555-555555555555"
	s.observe(&bizassistant.ToolCall{ToolName: toolGetContext}, json.RawMessage(`{"playbooks":[{"id":"`+pb+`"}],"impact":[{"env":"test"}]}`))
	s.observe(&bizassistant.ToolCall{ToolName: toolQueryRange}, json.RawMessage(`{"result":[]}`))
	s.finding = &Finding{Title: "t", Confidence: 90}
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolCreateProposal, Write: true},
		json.RawMessage(`{"playbook_id":"`+pb+`","step_index":1,"reason":"r","action":{"kind":"job"},"approve":true,"auto_eligible":false}`))
	if !d.Approve {
		t.Fatalf("proposal should be approved: %+v", d)
	}
	var args map[string]any
	_ = json.Unmarshal(d.Rewrite, &args)
	if _, ok := args["action"]; ok {
		t.Fatal("action must be stripped")
	}
	if _, ok := args["approve"]; ok {
		t.Fatal("approve must be stripped")
	}
	if args["autoEligible"] != true || args["playbookId"] != pb || args["stepIndex"].(float64) != 1 {
		t.Fatalf("args = %v", args)
	}
	ev := args["evidence"].([]any)
	if len(ev) != 1 || ev[0] != "agent_run:"+s.run.ID {
		t.Fatalf("evidence = %v", ev)
	}
	// 生产环境未放开 → 不申请自动。
	s2 := newState(false, auto)
	s2.observe(&bizassistant.ToolCall{ToolName: toolGetContext}, json.RawMessage(`{"playbooks":[{"id":"`+pb+`"}],"impact":[{"env":"prod"}]}`))
	s2.metricEvid = true
	s2.finding = &Finding{Title: "t", Confidence: 95}
	if s2.autoEligible() {
		t.Fatal("prod env should not be auto eligible by default")
	}
	// 置信度不足 → 不申请。
	s.finding.Confidence = 50
	if s.autoEligible() {
		t.Fatal("low confidence should not be auto eligible")
	}
}

func TestClampWindow(t *testing.T) {
	s := newState(false, DefaultAuto())
	args := s.clampWindow(map[string]any{"start": "2025-01-01T00:00:00Z", "end": "2027-01-01T00:00:00Z", "step_s": 1}, "start", "end", true)
	start, _ := time.Parse(time.RFC3339, args["start"].(string))
	if !start.Equal(s.inc.OccurTime.Add(-metricWindow)) {
		t.Fatalf("start not clamped: %v", args["start"])
	}
	if _, ok := args["step_s"]; ok {
		t.Fatal("step_s should be replaced by stepS")
	}
	if args["stepS"].(int) < 15 {
		t.Fatalf("step too small: %v", args["stepS"])
	}
}

func TestRecordDenied(t *testing.T) {
	s := newState(false, DefaultAuto())
	s.recordDenied(bizassistant.HeadlessDenied{Tool: toolAddFinding, Reason: "INCIDENT_FORBIDDEN"})
	s.recordDenied(bizassistant.HeadlessDenied{Tool: "CmdbCIService_GetCI", Reason: "PERMISSION_DENIED"})
	if !s.leaseLost || !s.authDenied || len(s.deniedCalls) != 2 {
		t.Fatalf("state = lease %v auth %v calls %d", s.leaseLost, s.authDenied, len(s.deniedCalls))
	}
}

func TestConfigBackoff(t *testing.T) {
	cfg := NewConfig(nil)
	if cfg.Enabled || cfg.MaxAttempts() != 4 || cfg.Backoff(1) != 30*time.Second || cfg.Backoff(9) != 10*time.Minute {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestClampInstantAndK8sWindow(t *testing.T) {
	s := newState(false, DefaultAuto())
	args := s.clampInstant(map[string]any{"time": "2030-01-01T00:00:00Z"})
	at, _ := time.Parse(time.RFC3339, args["time"].(string))
	if at.After(time.Now().Add(metricWindow + time.Minute)) {
		t.Fatalf("instant time not clamped: %v", args["time"])
	}
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolK8sMetrics}, json.RawMessage(`{"clusterId":"c","start":"2020-01-01T00:00:00Z","end":"2030-01-01T00:00:00Z"}`))
	if !d.Approve {
		t.Fatalf("k8s metrics should be approved: %s", d.Reason)
	}
	var out map[string]any
	_ = json.Unmarshal(d.Rewrite, &out)
	start, _ := time.Parse(time.RFC3339, out["start"].(string))
	if !start.Equal(s.inc.OccurTime.Add(-metricWindow)) {
		t.Fatalf("k8s start not clamped: %v", out["start"])
	}
	if _, ok := out["stepS"]; ok {
		t.Fatal("k8s metrics must not get stepS")
	}
}

func TestTailLogsKeepsLastLines(t *testing.T) {
	lines := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		lines = append(lines, fmt.Sprintf("line %03d %s", i, strings.Repeat("x", 20)))
	}
	raw, _ := json.Marshal(map[string]any{"content": strings.Join(lines, "\n"), "status": 2})
	out := tailLogs(raw)
	if out == nil {
		t.Fatal("expected tail rewrite")
	}
	var v map[string]any
	_ = json.Unmarshal(out, &v)
	content := v["content"].(string)
	if !strings.Contains(content, "line 499") || strings.Contains(content, "line 100 ") {
		t.Fatalf("tail should keep the last %d lines: %.80s", logTailLines, content)
	}
	if v["status"].(float64) != 2 {
		t.Fatal("other fields must survive")
	}
	if tailLogs(json.RawMessage(`{"content":"short"}`)) != nil {
		t.Fatal("short output must not be rewritten")
	}
}

func TestMarkAgentEvents(t *testing.T) {
	raw := json.RawMessage(`{"items":[{"id":"a","actorType":3,"detail":{"finding":{"title":"连接池耗尽"},"text":"评论"}},{"id":"b","actorType":1,"detail":{"text":"人工"}}]}`)
	out := markAgentEvents(raw)
	if out == nil {
		t.Fatal("expected tagging")
	}
	s := string(out)
	if !strings.Contains(s, agentPriorNote+" 连接池耗尽") || !strings.Contains(s, agentPriorNote+" 评论") {
		t.Fatalf("agent items not tagged: %s", s)
	}
	if strings.Contains(s, agentPriorNote+" 人工") {
		t.Fatal("human items must not be tagged")
	}
	if markAgentEvents(json.RawMessage(`{"items":[{"actorType":1}]}`)) != nil {
		t.Fatal("no agent items → no rewrite")
	}
}

func TestDecide_CreateDocumentOnlyForPostmortem(t *testing.T) {
	s := newState(false, DefaultAuto())
	tc := &bizassistant.ToolCall{ToolName: toolCreateDocument, Write: true}
	if d := s.decide(context.Background(), tc, json.RawMessage(`{"title":"x","content":"y"}`)); d.Approve {
		t.Fatal("diagnose profile must not create documents")
	}
	s.profile.Spec.Kind = KindPostmortem
	if d := s.decide(context.Background(), tc, json.RawMessage(`{"title":"x","content":"y"}`)); d.Approve {
		t.Fatal("missing knowledge space must be rejected")
	}
	s.profile.Spec.KnowledgeSpaceID = "33333333-3333-7333-8333-333333333333"
	d := s.decide(context.Background(), tc, json.RawMessage(`{"spaceId":"evil","title":"故障经验","content":"c","source":"manual","publish":true,"ownerId":"o"}`))
	if !d.Approve {
		t.Fatalf("postmortem create document should be approved: %s", d.Reason)
	}
	var out map[string]any
	_ = json.Unmarshal(d.Rewrite, &out)
	if out["spaceId"] != s.profile.Spec.KnowledgeSpaceID || out["source"] != "postmortem" || out["publish"] != false || out["sourceRef"] != s.inc.ID {
		t.Fatalf("document args not forced: %v", out)
	}
	if _, ok := out["ownerId"]; ok {
		t.Fatal("ownerId must be dropped")
	}
	if d := s.decide(context.Background(), tc, json.RawMessage(`{"title":"again","content":"c"}`)); d.Approve {
		t.Fatal("second document must be rejected")
	}
}

func TestDecide_CreateProposalCarriesAutoMaxTargets(t *testing.T) {
	s := newState(false, DefaultAuto())
	s.profile.Spec.Auto.MaxTargets = 7
	pb := "44444444-4444-7444-8444-444444444444"
	s.addSource(seenPlaybook, pb)
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolCreateProposal, Write: true}, json.RawMessage(`{"playbook_id":"`+pb+`","step_index":1,"reason":"r","auto_eligible":true}`))
	if !d.Approve {
		t.Fatalf("proposal should be approved: %s", d.Reason)
	}
	var out map[string]any
	_ = json.Unmarshal(d.Rewrite, &out)
	if out["autoMaxTargets"].(float64) != 7 || out["autoEligible"] != false {
		t.Fatalf("auto fields not set by runner: %v", out)
	}
}

func TestStageOrderForKind(t *testing.T) {
	if got := StageOrderFor(KindPostmortem); len(got) != 2 || got[0] != StageCollect || got[1] != StageConclude {
		t.Fatalf("postmortem order: %v", got)
	}
	if got := StageOrderFor(""); len(got) != len(StageOrder) {
		t.Fatalf("default order: %v", got)
	}
}
