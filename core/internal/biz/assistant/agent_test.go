package biz

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 未配置 API Key 时助手停用：Run 在触碰任何依赖前返回 ErrDisabled，且不写出事件。
func TestAgentUsecase_Disabled(t *testing.T) {
	t.Parallel()
	uc := NewAgentUsecase(nil, nil, nil, nil, nil, nil, nil, nil, NewAgentConfig(&conf.Assistant{}), slog.Default())
	if uc.Enabled() {
		t.Fatal("empty api_key must disable the assistant")
	}
	emitted := false
	err := uc.Run(context.Background(), RunInput{UserID: "u", Content: "hi"}, func(Event) error {
		emitted = true
		return nil
	})
	if !errors.Is(err, ErrDisabled) || emitted {
		t.Fatalf("Run() error = %v, emitted = %v; want ErrDisabled and no events", err, emitted)
	}
}

func TestNewAgentConfig(t *testing.T) {
	t.Parallel()
	cfg := NewAgentConfig(nil)
	if cfg.Enabled || cfg.Timeout != defaultRunTimeout || cfg.MaxSteps != defaultMaxSteps || cfg.MaxConcurrentRuns != defaultMaxConcurrentRuns {
		t.Fatalf("nil config defaults wrong: %+v", cfg)
	}
	cfg = NewAgentConfig(&conf.Assistant{ApiKey: "k", Timeout: durationpb.New(time.Minute), MaxSteps: 3, MaxConcurrentRuns: 2})
	if !cfg.Enabled || cfg.Timeout != time.Minute || cfg.MaxSteps != 3 || cfg.MaxConcurrentRuns != 2 {
		t.Fatalf("config not applied: %+v", cfg)
	}
}

// contextWindow 结构修复：缺结果的 tool_call 合成"已取消"tool 消息并紧跟其
// assistant 消息；孤儿 tool 消息丢弃；入参不被改动。
func TestContextWindow_RepairsDanglingToolCalls(t *testing.T) {
	t.Parallel()
	history := []*Message{
		{Role: RoleUser, Content: "q1"},
		{Role: RoleAssistant, ToolCalls: []ToolCallRef{{ID: "a", Name: "t"}, {ID: "b", Name: "t"}}},
		{Role: RoleTool, ToolCallID: "a", Content: "{}"},
		{Role: RoleUser, Content: "q2"},
		{Role: RoleTool, ToolCallID: "orphan", Content: "{}"},
	}
	got := contextWindow(history)
	if len(history) != 5 {
		t.Fatalf("input mutated: %d messages", len(history))
	}
	want := []struct{ role, callID string }{
		{RoleUser, ""}, {RoleAssistant, ""}, {RoleTool, "a"}, {RoleTool, "b"}, {RoleUser, ""},
	}
	if len(got) != len(want) {
		t.Fatalf("contextWindow() = %d messages, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Role != w.role || got[i].ToolCallID != w.callID {
			t.Errorf("message %d = (%s, %q), want (%s, %q)", i, got[i].Role, got[i].ToolCallID, w.role, w.callID)
		}
	}
	if got[3].Content != toolErrorContent(ToolStatusCancelled, cancelledToolMessage) {
		t.Errorf("synthesized tool content = %q", got[3].Content)
	}
}

// contextWindow 有界：超出条数/字节上限时丢弃最旧的单位，assistant 与其
// tool 结果不拆分，窗口从用户消息起始，最新单位即使超限也保留。
func TestContextWindow_Bounded(t *testing.T) {
	t.Parallel()
	history := make([]*Message, 0, 4*maxContextMessages)
	for range maxContextMessages {
		history = append(history,
			&Message{Role: RoleUser, Content: "q"},
			&Message{Role: RoleAssistant, ToolCalls: []ToolCallRef{{ID: "c", Name: "t"}}},
			&Message{Role: RoleTool, ToolCallID: "c", Content: "{}"},
			&Message{Role: RoleAssistant, Content: "a"},
		)
	}
	got := contextWindow(history)
	if len(got) == 0 || len(got) > maxContextMessages {
		t.Fatalf("contextWindow() = %d messages, want 1..%d", len(got), maxContextMessages)
	}
	if got[0].Role != RoleUser {
		t.Fatalf("window starts with %s, want user", got[0].Role)
	}
	if got[len(got)-1] != history[len(history)-1] {
		t.Fatal("window must end with the latest message")
	}
	for i, m := range got {
		if m.Role == RoleTool && (i == 0 || len(got[i-1].ToolCalls) == 0) {
			t.Fatalf("tool message %d split from its assistant message", i)
		}
	}

	big := []*Message{
		{Role: RoleUser, Content: "old"},
		{Role: RoleUser, Content: string(make([]byte, maxContextBytes+1))},
	}
	if got := contextWindow(big); len(got) != 1 || got[0] != big[1] {
		t.Fatalf("oversized latest unit must be kept alone, got %d messages", len(got))
	}
}

// 单用户并发上限：达到上限后拒绝，归还后恢复，其它用户不受影响。
func TestAgentUsecase_UserRunCap(t *testing.T) {
	t.Parallel()
	uc := NewAgentUsecase(nil, nil, nil, nil, nil, nil, nil, nil, NewAgentConfig(nil), slog.Default())
	for i := range maxUserConcurrentRuns {
		if !uc.acquireUser("u1") {
			t.Fatalf("acquireUser #%d rejected below cap", i+1)
		}
	}
	if uc.acquireUser("u1") {
		t.Fatal("acquireUser must reject above per-user cap")
	}
	if !uc.acquireUser("u2") {
		t.Fatal("other users must not be affected")
	}
	uc.releaseUser("u1")
	if !uc.acquireUser("u1") {
		t.Fatal("acquireUser must succeed after release")
	}
	for range maxUserConcurrentRuns {
		uc.releaseUser("u1")
	}
	uc.releaseUser("u2")
	if len(uc.userRuns) != 0 {
		t.Fatalf("userRuns not drained: %v", uc.userRuns)
	}
}

// 有界加载（ListRecent）的截断点可能落在 assistant 调用块中间：首部遗留的
// 无主 tool 消息被丢弃，窗口不会以 tool 消息起始；且 historyLoadLimit 必须
// 高于发送窗口上限，保证有界加载不会比全量加载少发消息。
func TestContextWindow_TruncatedLoadStartsMidToolGroup(t *testing.T) {
	t.Parallel()
	if historyLoadLimit <= maxContextMessages {
		t.Fatalf("historyLoadLimit = %d must exceed maxContextMessages = %d", historyLoadLimit, maxContextMessages)
	}
	full := []*Message{
		{Role: RoleUser, Content: "q0"},
		{Role: RoleAssistant, ToolCalls: []ToolCallRef{{ID: "a", Name: "t"}, {ID: "b", Name: "t"}}},
		{Role: RoleTool, ToolCallID: "a", Content: "{}"},
		{Role: RoleTool, ToolCallID: "b", Content: "{}"},
		{Role: RoleAssistant, Content: "a0"},
		{Role: RoleUser, Content: "q1"},
		{Role: RoleAssistant, Content: "a1"},
	}
	// 模拟 LIMIT 截在调用块中间：只剩该块的 tool 结果，没有声明它的 assistant。
	got := contextWindow(full[2:])
	want := full[5:]
	if len(got) != len(want) {
		t.Fatalf("contextWindow() = %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d = (%s, %q), want (%s, %q)", i, got[i].Role, got[i].Content, want[i].Role, want[i].Content)
		}
	}
}
