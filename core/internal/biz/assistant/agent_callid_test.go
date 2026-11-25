package biz_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

// toolCalls 本轮会话里的全部工具调用审计记录（按发起时间正序）。
func (h *runHarness) toolCalls(t *testing.T) []*biz.ToolCall {
	t.Helper()
	calls, err := h.store.ToolCalls().ListSince(context.Background(), h.conversationID(t), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return calls
}

// toolResultsOf 取某次模型请求窗口里跟在 assistant 调用块之后的 tool 消息：返回 (tool_call_id, 内容) 序列。
func toolResultsOf(msgs []*biz.Message, assistant int) [][2]string {
	var out [][2]string
	for i := assistant + 1; i < len(msgs) && msgs[i].Role == biz.RoleTool; i++ {
		out = append(out, [2]string{msgs[i].ToolCallID, msgs[i].Content})
	}
	return out
}

// lastAssistantIndex 窗口里最后一条带 tool_calls 的 assistant 消息下标。
func lastAssistantIndex(t *testing.T, msgs []*biz.Message) int {
	t.Helper()
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == biz.RoleAssistant && len(msgs[i].ToolCalls) > 0 {
			return i
		}
	}
	t.Fatal("no assistant message with tool calls in the model request")
	return -1
}

// 模型给出的调用 ID / 未知工具名超过库表列宽（varchar(128)）：截断后照常落库，整轮正常结束，不再因入库
// 失败而以「助手运行出错」白跑。
func TestRun_LongCallIDAndToolNameFitColumnWidth(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	longID := "call_" + strings.Repeat("i", 300)
	otherID := "call_" + strings.Repeat("j", 300)
	longName := "T_" + strings.Repeat("x", 300)
	h.model.Script(func(biz.ChatRequest) (*biz.Completion, error) {
		return &biz.Completion{ToolCalls: []biz.ToolCallRef{
			{ID: longID, Name: longName, Arguments: `{}`},
			{ID: otherID, Name: "T_Read", Arguments: `{"q":"ok"}`},
		}, FinishReason: "tool_calls"}, nil
	}).Reply("完成")
	if err := h.run(context.Background(), biz.RunInput{Content: "查"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if done := h.find(biz.EventDone).Payload.(biz.DonePayload); done.Status != biz.DoneCompleted {
		t.Fatalf("done = %+v, want completed", done)
	}

	calls := h.toolCalls(t)
	if len(calls) != 2 {
		t.Fatalf("audit records = %d, want 2", len(calls))
	}
	unknown, known := calls[0], calls[1]
	if unknown.ToolName != longName[:128] || unknown.CallID != longID[:128] {
		t.Errorf("unknown tool audit = name %d chars, call id %d chars; want both clipped to 128", len(unknown.ToolName), len(unknown.CallID))
	}
	if unknown.Status != biz.ToolStatusFailed || !strings.Contains(unknown.Error, "未知工具") {
		t.Errorf("unknown tool audit = %s %q, want failed", unknown.Status, unknown.Error)
	}
	if known.CallID != otherID[:128] || known.ToolName != "T_Read" || known.Status != biz.ToolStatusSucceeded {
		t.Errorf("known tool audit = call id %d chars, tool %q, status %s; want clipped id, succeeded", len(known.CallID), known.ToolName, known.Status)
	}
	// 失败原因里带的工具名同样不超列宽。
	if r := []rune(unknown.Error); len(r) > 128+len("未知工具或当前用户无权使用：") {
		t.Errorf("unknown tool error = %d chars, the tool name in it must be clipped", len(r))
	}

	// 历史与发给模型的消息一致：tool_call 与结果按 ID 一一配对。
	msgs, _ := h.store.Messages().List(context.Background(), h.conversationID(t))
	i := lastAssistantIndex(t, msgs)
	results := toolResultsOf(msgs, i)
	if len(results) != 2 {
		t.Fatalf("tool messages after the assistant call block = %d, want 2", len(results))
	}
	for k, ref := range msgs[i].ToolCalls {
		if results[k][0] != ref.ID {
			t.Errorf("tool message %d is for %q, tool_call %d is %q", k, results[k][0], k, ref.ID)
		}
	}
	reqs := h.model.Requests()
	if len(reqs) != 2 {
		t.Fatalf("model requests = %d, want 2", len(reqs))
	}
	window := reqs[1].Messages
	wi := lastAssistantIndex(t, window)
	for k, ref := range msgs[i].ToolCalls {
		if window[wi].ToolCalls[k].ID != ref.ID || window[wi].ToolCalls[k].Name != ref.Name {
			t.Errorf("tool_call %d sent to model = %+v, stored = %+v", k, window[wi].ToolCalls[k], ref)
		}
	}
}

// 同一条 assistant 消息里重复的 tool_call ID 重新生成：每个调用各有自己的 ID 与结果，回灌模型时不会两个
// 调用拿到同一份结果。
func TestRun_DuplicateCallIDsInOneMessageGetDistinctIDs(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	outputs := []string{`{"echo":1,"ok":true}`, `{"echo":2,"ok":true}`, `{"echo":3,"ok":true}`, `{"echo":4,"ok":true}`}
	h.invoker.before = func() {
		h.invoker.result = &biz.InvokeResult{StatusCode: 200, Body: json.RawMessage(outputs[len(h.invoker.calls)])}
	}
	h.model.Script(func(biz.ChatRequest) (*biz.Completion, error) {
		return &biz.Completion{ToolCalls: []biz.ToolCallRef{
			{ID: "call_1", Name: "T_Read", Arguments: `{"n":1}`},
			{ID: "call_1", Name: "T_Read", Arguments: `{"n":2}`},
			{ID: "call_1", Name: "T_Read", Arguments: `{"n":3}`},
			{ID: "", Name: "T_Read", Arguments: `{"n":4}`},
		}, FinishReason: "tool_calls"}, nil
	}).Reply("完成")
	if err := h.run(context.Background(), biz.RunInput{Content: "查"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !slices.Equal(h.invoker.inputs, []string{`{"n":1}`, `{"n":2}`, `{"n":3}`, `{"n":4}`}) {
		t.Fatalf("invoker inputs = %v", h.invoker.inputs)
	}

	calls := h.toolCalls(t)
	ids := make([]string, 0, len(calls))
	for _, tc := range calls {
		if tc.Status != biz.ToolStatusSucceeded {
			t.Errorf("audit %s = %s %q", tc.CallID, tc.Status, tc.Error)
		}
		ids = append(ids, tc.CallID)
	}
	if len(ids) != 4 || slices.Contains(ids, "") || len(slices.Compact(slices.Sorted(slices.Values(ids)))) != 4 {
		t.Fatalf("call ids = %q, want 4 distinct non-empty ids", ids)
	}

	// 回灌：assistant 消息的 4 个 tool_call 各自紧跟属于自己的结果（按发起顺序）。
	reqs := h.model.Requests()
	if len(reqs) != 2 {
		t.Fatalf("model requests = %d, want 2", len(reqs))
	}
	window := reqs[1].Messages
	wi := lastAssistantIndex(t, window)
	results := toolResultsOf(window, wi)
	if len(window[wi].ToolCalls) != 4 || len(results) != 4 {
		t.Fatalf("tool_calls = %d, tool messages = %d, want 4 / 4", len(window[wi].ToolCalls), len(results))
	}
	for k, ref := range window[wi].ToolCalls {
		if ref.ID != ids[k] || results[k][0] != ids[k] {
			t.Errorf("call %d: tool_call id %q, result id %q, audit id %q", k, ref.ID, results[k][0], ids[k])
		}
		if results[k][1] != outputs[k] {
			t.Errorf("call %d got result %s, want its own %s", k, results[k][1], outputs[k])
		}
	}
}
