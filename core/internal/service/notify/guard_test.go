package service

import (
	"context"
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

// newScopedChannelService 构造受限（仅可管 monitor）的渠道 service 与共享 store。
func newScopedChannelService(scope *testfake.BizScopeStub) (*NotifyChannelService, *testfake.Store) {
	store := testfake.NewStore()
	return NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), scope), store
}

func emailReq(bizID, name string) *notifyv1.CreateChannelRequest {
	biz := bizID
	return &notifyv1.CreateChannelRequest{
		BizId: &biz, Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_EMAIL, Name: name,
		Detail: &notifyv1.CreateChannelRequest_Email{Email: &notifyv1.EmailChannelConfig{Host: "smtp", Port: 25, FromAddr: "a@b.c"}},
		Secret: strPtr("pw"),
	}
}

func strPtr(s string) *string { return &s }

// TestBizGuard_ScopedMatrix 受限用户矩阵：范围内 200、范围外 403、列表缺 biz 400、
// 不限用户零变化、无 claims 401。
func TestBizGuard_ScopedMatrix(t *testing.T) {
	t.Parallel()
	svc, store := newScopedChannelService(testfake.Scoped("monitor"))
	// 用不限 service 预置 default 与 monitor 各一渠道（模拟其他团队的配置）。
	admin := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), testfake.Unrestricted())
	def, err := admin.CreateChannel(testCtx(), emailReq("default", "mail-default"))
	if err != nil {
		t.Fatal(err)
	}
	mon, err := admin.CreateChannel(testCtx(), emailReq("monitor", "mail-monitor"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx()

	// Create：范围内放行、范围外 403、缺省 biz(default) 403。
	if _, err := svc.CreateChannel(ctx, emailReq("monitor", "mail-2")); err != nil {
		t.Fatalf("create in scope: %v", err)
	}
	if _, err := svc.CreateChannel(ctx, emailReq("ticket", "x")); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("create out of scope: %v, want ErrBizForbidden", err)
	}
	req := emailReq("", "y")
	req.BizId = nil
	if _, err := svc.CreateChannel(ctx, req); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("create default biz: %v, want ErrBizForbidden", err)
	}
	// Get/Update/Delete/Test：按存储行的 biz 判定；不存在仍 404。
	if _, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: mon.GetChannel().GetId()}); err != nil {
		t.Fatalf("get in scope: %v", err)
	}
	if _, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: def.GetChannel().GetId()}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("get out of scope: %v", err)
	}
	if _, err := svc.UpdateChannel(ctx, &notifyv1.UpdateChannelRequest{Id: def.GetChannel().GetId(), Name: strPtr("hijack")}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("update out of scope: %v", err)
	}
	if _, err := svc.DeleteChannel(ctx, &notifyv1.DeleteChannelRequest{Id: def.GetChannel().GetId()}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("delete out of scope: %v", err)
	}
	if _, err := svc.SendTestEvent(ctx, &notifyv1.SendTestEventRequest{Id: def.GetChannel().GetId()}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("test out of scope: %v", err)
	}
	if _, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: "018f3e1a-0000-7000-8000-000000000000"}); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("get missing: %v, want not found", err)
	}
	// List：缺 biz 400、越权 403、范围内只看到自己的。
	if _, err := svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10}); !errors.Is(err, biz.ErrBizRequired) {
		t.Fatalf("list without biz: %v, want ErrBizRequired", err)
	}
	if _, err := svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10, BizId: strPtr("default")}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("list other biz: %v, want ErrBizForbidden", err)
	}
	list, err := svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10, BizId: strPtr("monitor")})
	if err != nil || list.GetTotal() != 2 {
		t.Fatalf("list in scope = %v, %v; want 2", list.GetTotal(), err)
	}
	// 不限用户：列表不带 biz 看全部（现状不变）。
	all, err := admin.ListChannels(testCtx(), &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10})
	if err != nil || all.GetTotal() != 3 {
		t.Fatalf("unrestricted list = %v, %v; want 3", all.GetTotal(), err)
	}
	// 无身份：401。
	if _, err := svc.ListChannels(context.Background(), &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10}); errors.Code(err) != 401 {
		t.Fatalf("no claims: %v, want 401", err)
	}
}

// TestBizGuard_RuntimeTablesRequireChannel 受限用户的任务/明细/死信列表须带
// channel_id 且渠道在范围内；重放同理。
func TestBizGuard_RuntimeTablesRequireChannel(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	channels := biz.NewChannelUsecase(store, store, store)
	admin := NewNotifyChannelService(channels, testfake.Unrestricted())
	mon, _ := admin.CreateChannel(testCtx(), emailReq("monitor", "m"))
	def, _ := admin.CreateChannel(testCtx(), emailReq("default", "d"))
	scope := testfake.Scoped("monitor")
	dispatch := NewNotifyDispatchService(biz.NewDispatchUsecase(store), channels, scope)
	logs := NewNotifyDeliveryLogService(biz.NewDeliveryLogUsecase(store), channels, scope)
	dlq := NewNotifyDeadLetterService(biz.NewDeadLetterUsecase(store), channels, scope)
	ctx := testCtx()

	if _, err := dispatch.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{Page: 1, PageSize: 10}); !errors.Is(err, biz.ErrBizRequired) {
		t.Fatalf("dispatch list without channel: %v", err)
	}
	if _, err := dispatch.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{Page: 1, PageSize: 10, ChannelId: strPtr(def.GetChannel().GetId())}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("dispatch list other biz channel: %v", err)
	}
	if _, err := dispatch.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{Page: 1, PageSize: 10, ChannelId: strPtr(mon.GetChannel().GetId())}); err != nil {
		t.Fatalf("dispatch list in scope: %v", err)
	}
	if _, err := logs.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{Page: 1, PageSize: 10}); !errors.Is(err, biz.ErrBizRequired) {
		t.Fatalf("log list without channel: %v", err)
	}
	store.SeedDeadLetter(&biz.DeadLetter{
		ID: "018f3e1a-0000-7000-8000-00000000d001", DispatchID: "018f3e1a-0000-7000-8000-00000000d001",
		EventID: "018f3e1a-0000-7000-8000-00000000e001", ChannelID: def.GetChannel().GetId(), Recipient: "r", Attempts: 6, ReplayState: biz.ReplayStateNone,
	})
	if _, err := dlq.ReplayDeadLetter(ctx, &notifyv1.ReplayDeadLetterRequest{Id: "018f3e1a-0000-7000-8000-00000000d001"}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("replay other biz: %v, want ErrBizForbidden", err)
	}
	if _, err := dlq.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{Page: 1, PageSize: 10, ChannelId: strPtr(mon.GetChannel().GetId())}); err != nil {
		t.Fatalf("dlq list in scope: %v", err)
	}
}
