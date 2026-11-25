package service

import (
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 渠道解绑路由后被删：它保留期内的任务、未重放完的死信，对 biz 范围受限的操作者仍按渠道原来的 biz 判范围——
// 范围内可见、可重放。此前归属判定走的是「未删除渠道」查询，这些行对受限操作者恒为「渠道不存在」（404），
// 而不限用户一切正常。范围外的照旧拒绝。
func TestNotifyDeletedChannel_StillScopedByItsBizForRestrictedOperators(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	ctx := testCtx()
	if err := store.CreateChannel(ctx, &biz.Channel{ID: dispatchChannelID, BizID: "monitor", Type: "email", Name: "ops-mail", Enabled: true}); err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	base := time.Now().UTC().Add(-time.Hour)
	store.SeedDispatch(&biz.Dispatch{
		ID: "018f3e1a-0304-7000-8000-0000000000d1", EventID: dispatchEventID, ChannelID: dispatchChannelID,
		Recipient: "ops@example.com", Status: biz.DispatchStatusFailed, Attempts: 2, CreateTime: base,
	})
	store.SeedDeadLetter(&biz.DeadLetter{
		ID: "018f3e1a-0304-7000-8000-0000000000q1", DispatchID: "018f3e1a-0304-7000-8000-0000000000d1", EventID: dispatchEventID,
		ChannelID: dispatchChannelID, Recipient: "ops@example.com", Attempts: 6, ReplayState: biz.ReplayStateNone,
		CreateTime: base, UpdateTime: base,
	})
	if err := store.DeleteChannel(ctx, dispatchChannelID); err != nil {
		t.Fatalf("DeleteChannel() error = %v", err)
	}

	inScope := testfake.Scoped("monitor")
	dispatches := NewNotifyDispatchService(biz.NewDispatchUsecase(store), biz.NewChannelUsecase(store, store, store), inScope)
	letters := NewNotifyDeadLetterService(biz.NewDeadLetterUsecase(store), biz.NewChannelUsecase(store, store, store), inScope)
	if got, err := dispatches.GetDispatch(ctx, &notifyv1.GetDispatchRequest{Id: "018f3e1a-0304-7000-8000-0000000000d1"}); err != nil || got.GetDispatch().GetChannelId() != dispatchChannelID {
		t.Fatalf("GetDispatch() = %v, %v; want the dispatch of the deleted channel to stay visible in scope", got, err)
	}
	if got, err := letters.GetDeadLetter(ctx, &notifyv1.GetDeadLetterRequest{Id: "018f3e1a-0304-7000-8000-0000000000q1"}); err != nil || got.GetDeadLetter().GetReplayState() != biz.ReplayStateNone {
		t.Fatalf("GetDeadLetter() = %v, %v; want the dead letter of the deleted channel to stay visible in scope", got, err)
	}
	if got, err := letters.ReplayDeadLetter(ctx, &notifyv1.ReplayDeadLetterRequest{Id: "018f3e1a-0304-7000-8000-0000000000q1"}); err != nil || got.GetDeadLetter().GetReplayState() != biz.ReplayStatePending {
		t.Fatalf("ReplayDeadLetter() = %v, %v; want the dead letter of the deleted channel to be replayable in scope", got, err)
	}

	// 范围外的 biz：渠道已删也照样按原 biz 拒绝，不会因为「查不到渠道」而放行。
	outOfScope := testfake.Scoped("cmdb")
	if _, err := NewNotifyDispatchService(biz.NewDispatchUsecase(store), biz.NewChannelUsecase(store, store, store), outOfScope).
		GetDispatch(ctx, &notifyv1.GetDispatchRequest{Id: "018f3e1a-0304-7000-8000-0000000000d1"}); !kerrors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("GetDispatch() out of scope: error = %v, want ErrBizForbidden", err)
	}
}
