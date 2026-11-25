package biz_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
)

// stallingModel 先吐出一段文字，然后一直等到 ctx 结束：模拟模型接口挂住直到单轮超时，或客户端中途断开。
type stallingModel struct{ *testfake.ChatModel }

func (m *stallingModel) Stream(ctx context.Context, _ biz.ChatRequest, onDelta func(biz.Delta) error) (*biz.Completion, error) {
	if err := onDelta(biz.Delta{Text: "先说一半"}); err != nil {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// useModel 换掉脚手架里的模型（仓储与执行器不变）：用来在同一份历史上继续下一轮。
func (h *runHarness) useModel(model biz.ChatModel, timeout time.Duration) {
	h.uc = biz.NewAgentUsecase(h.store.Conversations(), h.store.Messages(), h.store.ToolCalls(), model, h.invoker,
		stubActor{}, stubActor{}, runCatalog, nil, biz.AgentConfig{Enabled: true, Timeout: timeout, MaxSteps: 5, MaxConcurrentRuns: 2}, slog.Default())
}

// newStallingHarness 模型挂住、单轮上限很短的运行脚手架；h.model 仍是脚本桩，供后续轮次换回。
func newStallingHarness() *runHarness {
	h := newRunHarness()
	h.useModel(&stallingModel{ChatModel: h.model}, 200*time.Millisecond)
	return h
}

func eventTypes(events []biz.Event) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type)
	}
	return out
}

// continuesNormally 超时 / 被取消的轮次之后，同一会话换回脚本模型再跑一轮必须照常完成（没有留下悬空调用，
// 租约已经释放）。
func continuesNormally(t *testing.T, h *runHarness, conversationID string) {
	t.Helper()
	h.useModel(h.model, time.Minute)
	h.model.Reply("我在")
	if err := h.run(context.Background(), biz.RunInput{ConversationID: conversationID, Content: "继续"}); err != nil {
		t.Fatalf("next run error = %v", err)
	}
	if done := h.find(biz.EventDone).Payload.(biz.DonePayload); done.Status != biz.DoneCompleted {
		t.Fatalf("next run done = %+v, want completed", done)
	}
}

func lastDone(t *testing.T, events []biz.Event) biz.DonePayload {
	t.Helper()
	if len(events) == 0 || events[len(events)-1].Type != biz.EventDone {
		t.Fatalf("last event = %+v, want done", events[len(events)-1:])
	}
	return events[len(events)-1].Payload.(biz.DonePayload)
}

// 单轮达到时限：流不能在最后一个 delta 后悄悄结束，客户端要收到终止事件才能区分「回答完了」与「被截断」。
// 这是超时而不是业务错误，所以不下发 error 事件；Run 返回的仍是 ctx 的错误。
func TestRun_TimeoutEmitsDoneCancelled(t *testing.T) {
	t.Parallel()
	h := newStallingHarness()
	err := h.run(context.Background(), biz.RunInput{Content: "查"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	want := []string{biz.EventConversation, biz.EventTextDelta, biz.EventDone}
	if got := eventTypes(h.events); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if done := lastDone(t, h.events); done.Status != biz.DoneCancelled {
		t.Fatalf("done = %+v, want cancelled", done)
	}
	continuesNormally(t, h, h.conversationID(t))
}

// 时限来自上游（SSE 传输层的流时长上限 sseMaxDuration）时同样补发：被取消与被截断的区别在原因，不在谁定的期限。
func TestRun_ParentDeadlineEmitsDoneCancelled(t *testing.T) {
	t.Parallel()
	h := newStallingHarness()
	h.useModel(&stallingModel{ChatModel: h.model}, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := h.run(ctx, biz.RunInput{Content: "查"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded", err)
	}
	if got := eventTypes(h.events); !slices.Equal(got, []string{biz.EventConversation, biz.EventTextDelta, biz.EventDone}) {
		t.Fatalf("events = %v", got)
	}
	if done := lastDone(t, h.events); done.Status != biz.DoneCancelled {
		t.Fatalf("done = %+v, want cancelled", done)
	}
	continuesNormally(t, h, h.conversationID(t))
}

// 运行是被上游取消的（不是超时）：SSE 传输层只在某次写出已经失败之后才取消运行，客户端已经断开——
// 此时不再补发终止事件（没人收得到，只会让收尾白等一个写超时），也不发 error 事件；返回原始错误并照常收尾。
func TestRun_CancelledByTransportWritesNothingMore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		deltaErr error // 首个 delta 的写出是否就是那次失败的写出（空 = 它写成功了，之后心跳失败才取消）
	}{
		{name: "event write failed", deltaErr: errors.New("write tcp: connection reset by peer")},
		{name: "ping failed"},
	} {
		h := newStallingHarness()
		ctx, cancel := context.WithCancel(context.Background())
		var events []biz.Event
		var conversationID string
		err := h.uc.Run(ctx, biz.RunInput{UserID: runUser, Content: "查"}, func(ev biz.Event) error {
			events = append(events, ev)
			switch ev.Type {
			case biz.EventConversation:
				conversationID = ev.Payload.(biz.ConversationPayload).ID
			case biz.EventTextDelta:
				cancel() // 传输层在某次写出失败后取消运行
				return tc.deltaErr
			}
			return nil
		})
		cancel()
		if err == nil {
			t.Fatalf("%s: Run() error = nil, want the error that ended the run", tc.name)
		}
		// 只 attempted 了 conversation 与 text_delta 两个事件：取消之后没有再写。
		if got := eventTypes(events); !slices.Equal(got, []string{biz.EventConversation, biz.EventTextDelta}) {
			t.Errorf("%s: events = %v, want nothing written after the cancellation", tc.name, got)
		}
		continuesNormally(t, h, conversationID)
	}
}

// 连接还在、但运行以 Canceled 结束（不是 ctx 取消引起的）：客户端在等终止事件，照常补发 done。
func TestRun_CanceledErrorWithLiveStreamEmitsDone(t *testing.T) {
	t.Parallel()
	h := newRunHarness()
	h.model.Script(func(biz.ChatRequest) (*biz.Completion, error) { return nil, context.Canceled })
	err := h.run(context.Background(), biz.RunInput{Content: "查"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got := eventTypes(h.events); !slices.Equal(got, []string{biz.EventConversation, biz.EventDone}) {
		t.Fatalf("events = %v, want conversation then done (no error event)", got)
	}
	if done := lastDone(t, h.events); done.Status != biz.DoneCancelled {
		t.Fatalf("done = %+v, want cancelled", done)
	}
	continuesNormally(t, h, h.conversationID(t))
}

// 终止事件写不出去（连接刚断）：忽略写出失败，Run 仍返回原始的超时错误，轮次照常收尾。
func TestRun_DoneCancelledWriteFailureIsIgnored(t *testing.T) {
	t.Parallel()
	h := newStallingHarness()
	var writes int
	var conversationID string
	err := h.uc.Run(context.Background(), biz.RunInput{UserID: runUser, Content: "查"}, func(ev biz.Event) error {
		writes++
		if ev.Type == biz.EventConversation {
			conversationID = ev.Payload.(biz.ConversationPayload).ID
		}
		if ev.Type == biz.EventDone {
			return errors.New("client gone")
		}
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context.DeadlineExceeded (not the write error)", err)
	}
	if writes != 3 {
		t.Fatalf("events offered to the transport = %d, want 3", writes)
	}
	continuesNormally(t, h, conversationID)
}
