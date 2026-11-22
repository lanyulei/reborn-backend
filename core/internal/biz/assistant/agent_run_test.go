package biz_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
)

const runUser = "018f3e1a-7b1c-7cc1-9d2a-000000000001"

var runCatalog = biz.ToolCatalog{
	{Name: "T_Read", Module: "system", Operation: "/t.v1.T/Read", Code: strings.Repeat("a", 32), HTTPMethod: http.MethodGet, PathTemplate: "/read", InputSchema: json.RawMessage(`{"type":"object"}`)},
	{Name: "T_Write", Module: "system", Operation: "/t.v1.T/Write", Code: strings.Repeat("b", 32), Write: true, HTTPMethod: http.MethodPost, PathTemplate: "/write", BodyAll: true, InputSchema: json.RawMessage(`{"type":"object"}`)},
}

// stubInvoker 回环执行器桩：记录调用并返回预置结果。
type stubInvoker struct {
	calls  []string
	inputs []string
	ctxErr []error
	result *biz.InvokeResult
	err    error
	before func()
}

func (s *stubInvoker) Invoke(ctx context.Context, spec biz.ToolSpec, input json.RawMessage, _ string) (*biz.InvokeResult, error) {
	if s.before != nil {
		s.before()
	}
	s.calls = append(s.calls, spec.Name)
	s.inputs = append(s.inputs, string(input))
	s.ctxErr = append(s.ctxErr, ctx.Err())
	return s.result, s.err
}

type stubActor struct{}

func (stubActor) DescribeActor(context.Context, string) (string, string, bool, error) {
	return "admin", "", true, nil
}
func (stubActor) EffectiveOperations(context.Context, string) ([]string, error) { return nil, nil }

type runHarness struct {
	store   *testfake.AssistantStore
	model   *testfake.ChatModel
	invoker *stubInvoker
	uc      *biz.AgentUsecase
	events  []biz.Event
}

func newRunHarness() *runHarness {
	h := &runHarness{store: testfake.NewAssistantStore(), model: testfake.NewChatModel(), invoker: &stubInvoker{result: &biz.InvokeResult{StatusCode: 200, Body: json.RawMessage(`{}`)}}}
	h.uc = biz.NewAgentUsecase(h.store.Conversations(), h.store.Messages(), h.store.ToolCalls(), h.model, h.invoker,
		stubActor{}, stubActor{}, runCatalog, nil, biz.AgentConfig{Enabled: true, Timeout: time.Minute, MaxSteps: 5, MaxConcurrentRuns: 2}, slog.Default())
	return h
}

func (h *runHarness) run(ctx context.Context, in biz.RunInput) error {
	in.UserID = runUser
	h.events = nil
	return h.uc.Run(ctx, in, func(ev biz.Event) error { h.events = append(h.events, ev); return nil })
}

func (h *runHarness) find(typ string) *biz.Event {
	for i := len(h.events) - 1; i >= 0; i-- {
		if h.events[i].Type == typ {
			return &h.events[i]
		}
	}
	return nil
}

func (h *runHarness) conversationID(t *testing.T) string {
	t.Helper()
	ev := h.find(biz.EventConversation)
	if ev == nil {
		t.Fatal("no conversation event")
	}
	return ev.Payload.(biz.ConversationPayload).ID
}

// lastToolMessage 最近一次模型请求里的最后一条 tool 消息。
func (h *runHarness) lastToolMessage(t *testing.T) *biz.Message {
	t.Helper()
	reqs := h.model.Requests()
	msgs := reqs[len(reqs)-1].Messages
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == biz.RoleTool {
			return msgs[i]
		}
	}
	t.Fatal("no tool message sent to model")
	return nil
}

// 超长输出先脱敏再截断：截断预览里不得带出敏感值。
func TestRun_MaskBeforeTruncate(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	big := `{"items":[{"token":"s3cr3t-value","pad":"` + strings.Repeat("x", 40<<10) + `"}]}`
	h.invoker.result = &biz.InvokeResult{StatusCode: 200, Body: json.RawMessage(big)}
	h.model.Call("c1", "T_Read", map[string]any{}).Reply("完成")
	if err := h.run(context.Background(), biz.RunInput{Content: "查"}); err != nil {
		t.Fatal(err)
	}
	content := h.lastToolMessage(t).Content
	if !strings.Contains(content, `"truncated":true`) || strings.Contains(content, "s3cr3t-value") {
		t.Fatalf("tool message leaks secret or is not truncated: %.300s", content)
	}
	out, _ := json.Marshal(h.find(biz.EventToolResult).Payload)
	if strings.Contains(string(out), "s3cr3t-value") {
		t.Fatal("tool_result event leaks secret")
	}
}

// 参数残缺（输出被截断）：不执行、写操作不进入确认，以失败回灌；历史里的参数规整为 "{}"。
func TestRun_InvalidArgumentsNotExecuted(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"T_Read", "T_Write"} {
		h := newRunHarness()
		h.model.Script(func(biz.ChatRequest) (*biz.Completion, error) {
			return &biz.Completion{FinishReason: "length", ToolCalls: []biz.ToolCallRef{{ID: "c1", Name: tool, Arguments: `{"name":"半截`}}}, nil
		}).Reply("好的")
		if err := h.run(context.Background(), biz.RunInput{Content: "做"}); err != nil {
			t.Fatal(err)
		}
		if len(h.invoker.calls) != 0 || h.find(biz.EventToolConfirmRequired) != nil {
			t.Fatalf("%s: broken arguments must not execute nor ask for confirmation", tool)
		}
		if res := h.find(biz.EventToolResult).Payload.(biz.ToolCallPayload); res.Status != biz.ToolStatusFailed {
			t.Fatalf("%s: tool_result = %+v", tool, res)
		}
		if done := h.find(biz.EventDone).Payload.(biz.DonePayload); done.Status != biz.DoneCompleted {
			t.Fatalf("%s: done = %+v", tool, done)
		}
		for _, m := range h.model.Requests()[1].Messages {
			for _, ref := range m.ToolCalls {
				if ref.Arguments != "{}" {
					t.Fatalf("%s: history keeps broken arguments %q", tool, ref.Arguments)
				}
			}
		}
	}
}

// 回复因长度上限被截断：追加说明并随消息落库。
func TestRun_LengthNotice(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	h.model.Script(func(biz.ChatRequest) (*biz.Completion, error) {
		return &biz.Completion{Content: "前半段", FinishReason: "length"}, nil
	})
	if err := h.run(context.Background(), biz.RunInput{Content: "说"}); err != nil {
		t.Fatal(err)
	}
	msgs, _ := h.store.Messages().List(context.Background(), h.conversationID(t))
	if last := msgs[len(msgs)-1]; !strings.HasPrefix(last.Content, "前半段") || !strings.Contains(last.Content, "截断") {
		t.Fatalf("assistant message = %q", last.Content)
	}
}

// 已确认的写操作：执行不随轮次 ctx 取消而中止；拿不到结果时如实记为"结果未知"。
func TestRun_WriteOutcomeUnknown(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	h.model.Call("c1", "T_Write", map[string]any{"name": "x"})
	if err := h.run(context.Background(), biz.RunInput{Content: "写"}); err != nil {
		t.Fatal(err)
	}
	convID := h.conversationID(t)
	callID := h.find(biz.EventToolConfirmRequired).Payload.(biz.ToolCallPayload).ID

	ctx, cancel := context.WithCancel(context.Background())
	h.invoker.before = cancel // 请求发出时客户端断开
	h.invoker.result, h.invoker.err = nil, errors.New("timeout")
	err := h.run(ctx, biz.RunInput{ConversationID: convID, Confirm: &biz.Confirmation{ToolCallID: callID, Approved: true}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if len(h.invoker.ctxErr) != 1 || h.invoker.ctxErr[0] != nil {
		t.Fatalf("write must run on a context detached from cancellation, ctx errors = %v", h.invoker.ctxErr)
	}
	audit := h.store.ToolCall(callID)
	if audit.Status != biz.ToolStatusFailed || !strings.Contains(audit.Error, "结果未知") {
		t.Fatalf("audit = %+v", audit)
	}
	// 历史完整：下一轮请求里该调用带着"结果未知"的 tool 消息。
	h.invoker.before = nil
	h.model.Reply("我先查一下")
	if err := h.run(context.Background(), biz.RunInput{ConversationID: convID, Content: "怎么样了"}); err != nil {
		t.Fatal(err)
	}
	if content := h.lastToolMessage(t).Content; !strings.Contains(content, "结果未知") {
		t.Fatalf("tool message = %s", content)
	}
}

// 轮次中途访问令牌失效（工具 401）：记失败后结束本轮，下发明确的错误而不是继续让模型空转。
func TestRun_SessionExpired(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	h.invoker.result = &biz.InvokeResult{StatusCode: http.StatusUnauthorized, Reason: "TOKEN_EXPIRED", Message: "token has expired"}
	h.model.Call("c1", "T_Read", map[string]any{}).Reply("不应到达")
	err := h.run(context.Background(), biz.RunInput{Content: "查"})
	if !errors.Is(err, biz.ErrSessionExpired) {
		t.Fatalf("Run() error = %v, want ErrSessionExpired", err)
	}
	ev := h.find(biz.EventError)
	if ev == nil || ev.Payload.(biz.ErrorPayload).Reason != "TOKEN_EXPIRED" {
		t.Fatalf("error event = %+v", ev)
	}
	if n := len(h.model.Requests()); n != 1 {
		t.Fatalf("model requests = %d, want 1", n)
	}
}

// 上一轮在"终态已写、tool 消息未写"之间中断：收尾按真实结果补写 tool 消息，不把成功改写成取消；
// 还停在执行中的写操作记为取消并注明结果未知。
func TestRun_SettlesInterruptedCallsByRealOutcome(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	h.model.Call("c1", "T_Write", map[string]any{"name": "x"})
	if err := h.run(context.Background(), biz.RunInput{Content: "写"}); err != nil {
		t.Fatal(err)
	}
	convID := h.conversationID(t)
	callID := h.find(biz.EventToolConfirmRequired).Payload.(biz.ToolCallPayload).ID
	// 模拟"终态已写、tool 消息未写"：审计记录直接置为成功。
	seeded := h.store.ToolCall(callID)
	seeded.Status, seeded.Output = biz.ToolStatusSucceeded, json.RawMessage(`{"id":"created-1"}`)
	h.store.SeedToolCall(seeded)
	h.model.Reply("已完成")
	if err := h.run(context.Background(), biz.RunInput{ConversationID: convID, Content: "结果呢"}); err != nil {
		t.Fatal(err)
	}
	if content := h.lastToolMessage(t).Content; !strings.Contains(content, "created-1") {
		t.Fatalf("tool message = %s, want the real output", content)
	}
	if got := h.store.ToolCall(callID).Status; got != biz.ToolStatusSucceeded {
		t.Fatalf("audit status = %s, want succeeded", got)
	}

	// 停在执行中的写操作：新会话里重现。
	h2 := newRunHarness()
	h2.model.Call("c1", "T_Write", map[string]any{"name": "x"})
	if err := h2.run(context.Background(), biz.RunInput{Content: "写"}); err != nil {
		t.Fatal(err)
	}
	conv2 := h2.conversationID(t)
	call2 := h2.find(biz.EventToolConfirmRequired).Payload.(biz.ToolCallPayload).ID
	stuck := h2.store.ToolCall(call2)
	stuck.Status = biz.ToolStatusApproved
	h2.store.SeedToolCall(stuck)
	h2.model.Reply("我先确认一下")
	if err := h2.run(context.Background(), biz.RunInput{ConversationID: conv2, Content: "结果呢"}); err != nil {
		t.Fatal(err)
	}
	audit := h2.store.ToolCall(call2)
	if audit.Status != biz.ToolStatusCancelled || !strings.Contains(audit.Error, "结果未知") {
		t.Fatalf("audit = %+v", audit)
	}
	if content := h2.lastToolMessage(t).Content; !strings.Contains(content, "结果未知") {
		t.Fatalf("tool message = %s", content)
	}
	if len(h2.invoker.calls) != 0 {
		t.Fatal("interrupted write must not be re-executed")
	}
}
