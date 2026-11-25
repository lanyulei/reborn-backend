package biz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
)

// rejectedStub 与 data 层的 rejectedError 同构：Unwrap 返回 ErrLLMRejected 哨兵本身。
type rejectedStub struct{ cause error }

func (e rejectedStub) Error() string { return ErrLLMRejected.Error() + ": " + e.cause.Error() }
func (e rejectedStub) Unwrap() error { return ErrLLMRejected }

// 无人值守运行的结束原因：模型「暂时不可用」（5xx / 限流 / 网络错误 / 空补全 / 空闲超时）必须与「拒绝了请求」
// 区分开——两者是同一个 reason 的 Kratos 错误，此前按 code + reason 判等，「不可用」全部被记成「拒绝」，
// 运维 Agent 因此不重试。
func TestHeadlessEndReason(t *testing.T) {
	t.Parallel()
	ok := context.Background()
	cases := []struct {
		name       string
		err        error
		ctx        context.Context
		reason     string
		rethrowNil bool
	}{
		{name: "completed", err: nil, ctx: ok, reason: "", rethrowNil: true},
		{name: "empty completion", err: fmt.Errorf("%w: empty completion", ErrLLMUnavailable), ctx: ok, reason: HeadlessLLMUnavailable, rethrowNil: true},
		{name: "5xx / network", err: fmt.Errorf("%w: %v", ErrLLMUnavailable, io.ErrUnexpectedEOF), ctx: ok, reason: HeadlessLLMUnavailable, rethrowNil: true},
		{name: "stream idle timeout", err: fmt.Errorf("%w: %v", ErrLLMUnavailable, context.DeadlineExceeded), ctx: ok, reason: HeadlessLLMUnavailable, rethrowNil: true},
		{name: "bare unavailable sentinel", err: ErrLLMUnavailable, ctx: ok, reason: HeadlessLLMUnavailable, rethrowNil: true},
		{name: "4xx rejected", err: rejectedStub{cause: errors.New("400 context length exceeded")}, ctx: ok, reason: HeadlessLLMRejected, rethrowNil: true},
		{name: "rejected, further wrapped", err: fmt.Errorf("stage: %w", rejectedStub{cause: io.EOF}), ctx: ok, reason: HeadlessLLMRejected, rethrowNil: true},
		{name: "bare rejected sentinel", err: ErrLLMRejected, ctx: ok, reason: HeadlessLLMRejected, rethrowNil: true},
		{name: "session expired", err: fmt.Errorf("call: %w", ErrSessionExpired), ctx: ok, reason: HeadlessSessionExpired, rethrowNil: true},
		{name: "canceled", err: context.Canceled, ctx: ok, reason: "", rethrowNil: false},
		{name: "deadline", err: context.DeadlineExceeded, ctx: ok, reason: HeadlessTimeout, rethrowNil: true},
		{name: "other", err: errors.New("boom"), ctx: ok, reason: "", rethrowNil: false},
	}
	for _, c := range cases {
		reason, rethrow := headlessEndReason(c.ctx, c.err)
		if reason != c.reason || (rethrow == nil) != c.rethrowNil {
			t.Errorf("%s: headlessEndReason() = (%q, %v), want (%q, rethrowNil=%v)", c.name, reason, rethrow, c.reason, c.rethrowNil)
		}
	}
	// 被包装的网络错误不会因为 errors.As 得到 net.Error 之类的中间类型而被误判成拒绝。
	var ne net.Error
	wrapped := fmt.Errorf("%w: %w", ErrLLMUnavailable, &net.OpError{Op: "read", Err: io.EOF})
	if !errors.As(wrapped, &ne) || isLLMRejected(wrapped) {
		t.Fatalf("a network failure must not be classified as rejected")
	}
}
