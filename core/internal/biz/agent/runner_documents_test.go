package biz

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

// 即时查询缺省（以及给出未来时刻时）取「窗口内最新的时刻」：未解决故障的窗口上界是 now+30min，
// 此前直接取上界，等于以未来时刻查询，结果恒为空。
func TestClampInstant_DefaultIsLatestNotFuture(t *testing.T) {
	s := newState(false, DefaultAuto())
	before := time.Now().Add(-5 * time.Second)
	at, _ := time.Parse(time.RFC3339, s.clampInstant(map[string]any{})["time"].(string))
	if at.Before(before) || time.Now().Add(time.Second).Before(at) {
		t.Fatalf("default instant must be now (window allows up to +30m), got %v", at)
	}
	// 模型给出的未来时刻同样压到最新时刻，而不是 +30min。
	future, _ := time.Parse(time.RFC3339, s.clampInstant(map[string]any{"time": "2030-01-01T00:00:00Z"})["time"].(string))
	if future.After(time.Now().Add(time.Second)) {
		t.Fatalf("a future instant must be clamped to now, got %v", future)
	}
	// 已恢复的故障：最新时刻是「恢复 + 30 分钟」。
	s.inc.ResolveTime = &[]time.Time{s.inc.OccurTime.Add(time.Hour)}[0]
	resolved, _ := time.Parse(time.RFC3339, s.clampInstant(map[string]any{})["time"].(string))
	if want := s.inc.OccurTime.Add(time.Hour + metricWindow); !resolved.Equal(want) {
		t.Fatalf("resolved incident: default instant = %v, want %v", resolved, want)
	}
	// 早于窗口下界的时刻抬到下界。
	early, _ := time.Parse(time.RFC3339, s.clampInstant(map[string]any{"time": "2020-01-01T00:00:00Z"})["time"].(string))
	if want := s.inc.OccurTime.Add(-metricWindow); !early.Equal(want) {
		t.Fatalf("too-early instant = %v, want %v", early, want)
	}
}

func postmortemState(dry bool) *runState {
	s := newState(dry, DefaultAuto())
	s.profile.Spec.Kind, s.profile.Spec.KnowledgeSpaceID = KindPostmortem, "33333333-3333-7333-8333-333333333333"
	return s
}

// 建知识草稿的写入计数只在成功之后累加：此前在裁决时就 +1，首次调用失败后模型重试会被拒，
// 经验沉淀任务以「无结论」收场。
func TestDecide_CreateDocumentCountsOnlyAfterSuccess(t *testing.T) {
	s := postmortemState(false)
	tc := &bizassistant.ToolCall{ToolName: toolCreateDocument, Write: true}
	call := json.RawMessage(`{"title":"复盘草稿","content":"正文"}`)
	for i := range 2 {
		d := s.decide(context.Background(), tc, call)
		if !d.Approve || d.Capture {
			t.Fatalf("call %d must be approved while no document has been created: %+v", i+1, d)
		}
	}
	if s.documents != 0 {
		t.Fatalf("documents counted before any success: %d", s.documents)
	}
	s.observe(tc, json.RawMessage(`{"document":{"id":"44444444-4444-7444-8444-444444444444","title":"复盘草稿"}}`))
	if s.documents != 1 || !s.findingDone || s.finding == nil {
		t.Fatalf("a successful call must count as the one document and as the finding: documents=%d findingDone=%v", s.documents, s.findingDone)
	}
	if d := s.decide(context.Background(), tc, call); d.Approve {
		t.Fatalf("a second document in the same run must be rejected: %+v", d)
	}
}

// 干跑：经验沉淀类 Profile 的建文档按「已创建」处理，运行才能给出成功结论（此前恒为「无结论」）。
func TestDecide_DryRunCreateDocumentYieldsFinding(t *testing.T) {
	s := postmortemState(true)
	long := strings.Repeat("题", maxFindingTitle+20)
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolCreateDocument, Write: true}, json.RawMessage(`{"title":"`+long+`","content":"正文"}`))
	if !d.Capture {
		t.Fatalf("dry run must capture the call: %+v", d)
	}
	if !s.findingDone || s.finding == nil || len([]rune(s.finding.Title)) != maxFindingTitle {
		t.Fatalf("dry run must record the pending document as the finding: findingDone=%v finding=%+v", s.findingDone, s.finding)
	}
	// 干跑不占用写入计数（计数在 absorb 收到捕获结果时才累加）。
	if s.documents != 0 {
		t.Fatalf("decide must not count in a dry run: %d", s.documents)
	}
}
