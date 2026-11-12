package service

import (
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newRouteService(t *testing.T) (*NotifyRouteService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	return NewNotifyRouteService(biz.NewRouteUsecase(store, store), biz.NewPreviewUsecase(store, store, store, store), testfake.Unrestricted()), store
}

// seedRouteChannel 预置一个钉钉渠道并返回其 ID。
func seedRouteChannel(t *testing.T, svc *NotifyChannelService) string {
	t.Helper()
	resp, err := svc.CreateChannel(testCtx(), &notifyv1.CreateChannelRequest{
		Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_DINGTALK,
		Name: "dingtalk-ops",
		Detail: &notifyv1.CreateChannelRequest_Robot{Robot: &notifyv1.RobotChannelConfig{
			AtMobiles: []string{"13800000000"},
		}},
		Secret: sptr("robot-token"),
	})
	if err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	return resp.GetChannel().GetId()
}

// newRouteSpec 返回合法告警路由规则载荷。
func newRouteSpec(channelID string) *notifyv1.RouteSpec {
	return &notifyv1.RouteSpec{
		EventPattern: "monitor.alert.*",
		SeverityMin:  i32ptr(2),
		Priority:     i32ptr(3),
		ChannelIds:   []string{channelID},
		Recipient: &notifyv1.RouteRecipient{
			Fixed: []string{"ops@example.com"},
		},
		AggregateWindowS:  i32ptr(60),
		SuppressThreshold: i32ptr(500),
		SuppressDurationM: i32ptr(30),
	}
}

// i32ptr int32 取址。
func i32ptr(v int32) *int32 { return &v }

func TestNotifyRouteService_CreateRoundtrip(t *testing.T) {
	t.Parallel()
	svc, store := newRouteService(t)
	channelSvc := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), biz.NewChannelHealthUsecase(store), testfake.Unrestricted())
	channelID := seedRouteChannel(t, channelSvc)
	ctx := testCtx()

	resp, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: newRouteSpec(channelID)})
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	route := resp.GetRoute()
	if route.GetId() == "" || route.GetBizId() != biz.DefaultBizID {
		t.Fatalf("CreateRoute() = %+v, want id and default biz", route)
	}
	// 显式调优项透传。
	if route.GetSeverityMin() != 2 || route.GetPriority() != 3 ||
		route.GetAggregateWindowS() != 60 || route.GetSuppressThreshold() != 500 ||
		route.GetSuppressDurationM() != 30 {
		t.Fatalf("CreateRoute() tuning = %+v", route)
	}
	if !route.GetEnabled() || route.GetRecipient().GetSubscription() {
		t.Fatalf("CreateRoute() enabled/recipient = %+v", route.GetRecipient())
	}
	if got := route.GetChannelIds(); len(got) != 1 || got[0] != channelID {
		t.Fatalf("CreateRoute() channels = %v", got)
	}

	// 缺省调优项走默认：severity 1 / priority 5 / 窗口 0 / 阈值 500 / 时长 30。
	bare := newRouteSpec(channelID)
	bare.SeverityMin, bare.Priority, bare.AggregateWindowS = nil, nil, nil
	bare.SuppressThreshold, bare.SuppressDurationM = nil, nil
	resp, err = svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: bare})
	if err != nil {
		t.Fatalf("CreateRoute(defaults) error = %v", err)
	}
	d := resp.GetRoute()
	if d.GetSeverityMin() != 1 || d.GetPriority() != 5 || d.GetAggregateWindowS() != 0 ||
		d.GetSuppressThreshold() != 500 || d.GetSuppressDurationM() != 30 {
		t.Fatalf("CreateRoute(defaults) tuning = %+v", d)
	}
}

func TestNotifyRouteService_CreateErrors(t *testing.T) {
	t.Parallel()
	svc, store := newRouteService(t)
	channelSvc := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), biz.NewChannelHealthUsecase(store), testfake.Unrestricted())
	channelID := seedRouteChannel(t, channelSvc)
	ctx := testCtx()

	// 引用不存在渠道。
	missing := newRouteSpec("018f3e1a-0301-7000-8000-0000000000ff")
	if _, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: missing}); !errors.Is(err, biz.ErrRouteInvalid) {
		t.Fatalf("CreateRoute(missing channel) error = %v, want ErrRouteInvalid", err)
	}

	// 收件策略为空：群机器人渠道以实例绑定群为隐含收件，允许留空（biz 按渠道类型校验）。
	bare := newRouteSpec(channelID)
	bare.Recipient = &notifyv1.RouteRecipient{}
	if _, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: bare}); err != nil {
		t.Fatalf("CreateRoute(robot, empty recipient) error = %v, want nil", err)
	}

	// 邮件渠道 + 空收件策略：无法解析收件地址，拒绝。
	mailResp, err := channelSvc.CreateChannel(ctx, newEmailCreate("email-route"))
	if err != nil {
		t.Fatalf("seed email channel: %v", err)
	}
	mail := newRouteSpec(mailResp.GetChannel().GetId())
	mail.Recipient = &notifyv1.RouteRecipient{}
	if _, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: mail}); !errors.Is(err, biz.ErrRouteInvalid) {
		t.Fatalf("CreateRoute(email, empty recipient) error = %v, want ErrRouteInvalid", err)
	}
}

func TestNotifyRouteService_UpdateRoute(t *testing.T) {
	t.Parallel()
	svc, store := newRouteService(t)
	channelSvc := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), biz.NewChannelHealthUsecase(store), testfake.Unrestricted())
	channelID := seedRouteChannel(t, channelSvc)
	ctx := testCtx()

	created, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: newRouteSpec(channelID)})
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	id := created.GetRoute().GetId()

	// spec 整组替换：换模式 + 关聚合 + 订阅收件。
	replaced := newRouteSpec(channelID)
	replaced.EventPattern = "permission.change"
	replaced.AggregateWindowS = i32ptr(0)
	replaced.Recipient = &notifyv1.RouteRecipient{Subscription: true}
	if _, err := svc.UpdateRoute(ctx, &notifyv1.UpdateRouteRequest{Id: id, Spec: replaced}); err != nil {
		t.Fatalf("UpdateRoute(spec) error = %v", err)
	}
	got, err := svc.GetRoute(ctx, &notifyv1.GetRouteRequest{Id: id})
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
	r := got.GetRoute()
	if r.GetEventPattern() != "permission.change" || r.GetAggregateWindowS() != 0 ||
		!r.GetRecipient().GetSubscription() || len(r.GetRecipient().GetFixed()) != 0 {
		t.Fatalf("UpdateRoute(spec) result = %+v", r)
	}
	// 整组替换语义：未显式传回的 severity 等字段重置为默认值。
	if r.GetSeverityMin() != 2 || r.GetPriority() != 3 {
		t.Fatalf("UpdateRoute(spec) kept tuning = %+v", r)
	}

	// 独立启停不动规则。
	disabled := false
	if _, err := svc.UpdateRoute(ctx, &notifyv1.UpdateRouteRequest{Id: id, Enabled: &disabled}); err != nil {
		t.Fatalf("UpdateRoute(enabled) error = %v", err)
	}
	got, _ = svc.GetRoute(ctx, &notifyv1.GetRouteRequest{Id: id})
	if got.GetRoute().GetEnabled() || got.GetRoute().GetEventPattern() != "permission.change" {
		t.Fatalf("UpdateRoute(enabled) result = %+v", got.GetRoute())
	}

	// 不存在路由。
	if _, err := svc.UpdateRoute(ctx, &notifyv1.UpdateRouteRequest{
		Id: missingChannelID, Enabled: &disabled,
	}); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("UpdateRoute(missing) error = %v, want ErrRouteNotFound", err)
	}
}

func TestNotifyRouteService_DeleteAndList(t *testing.T) {
	t.Parallel()
	svc, store := newRouteService(t)
	channelSvc := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), biz.NewChannelHealthUsecase(store), testfake.Unrestricted())
	channelID := seedRouteChannel(t, channelSvc)
	ctx := testCtx()

	for _, pattern := range []string{"monitor.alert.*", "permission.change"} {
		spec := newRouteSpec(channelID)
		spec.EventPattern = pattern
		if _, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{Spec: spec}); err != nil {
			t.Fatalf("CreateRoute(%q) error = %v", pattern, err)
		}
	}

	list, err := svc.ListRoutes(ctx, &notifyv1.ListRoutesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListRoutes() error = %v", err)
	}
	if list.GetTotal() != 2 || len(list.GetItems()) != 2 {
		t.Fatalf("ListRoutes() = %d items, total %d, want 2/2", len(list.GetItems()), list.GetTotal())
	}

	// keyword 模糊匹配 event_pattern。
	kw := "permission"
	list, err = svc.ListRoutes(ctx, &notifyv1.ListRoutesRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListRoutes(keyword) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetEventPattern() != "permission.change" {
		t.Fatalf("ListRoutes(keyword) = %+v", list.GetItems())
	}

	// 删除后不可见。
	if _, err := svc.DeleteRoute(ctx, &notifyv1.DeleteRouteRequest{Id: list.GetItems()[0].GetId()}); err != nil {
		t.Fatalf("DeleteRoute() error = %v", err)
	}
	if _, err := svc.GetRoute(ctx, &notifyv1.GetRouteRequest{Id: list.GetItems()[0].GetId()}); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("GetRoute(deleted) error = %v, want ErrRouteNotFound", err)
	}
}

// TestNotifyRouteService_PreviewRouting 路由试算：命中路由/渠道/模板解析与静默。
func TestNotifyRouteService_PreviewRouting(t *testing.T) {
	t.Parallel()
	svc, store := newRouteService(t)
	ctx := testCtx()
	chSvc := NewNotifyChannelService(biz.NewChannelUsecase(store, store, store).WithTestEventCooldown(0), biz.NewChannelHealthUsecase(store), testfake.Unrestricted())
	chID := seedRouteChannel(t, chSvc)
	created, err := svc.CreateRoute(ctx, &notifyv1.CreateRouteRequest{
		Spec: &notifyv1.RouteSpec{EventPattern: "monitor.*", ChannelIds: []string{chID}, Recipient: &notifyv1.RouteRecipient{}},
	})
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	resp, err := svc.PreviewRouting(ctx, &notifyv1.PreviewRoutingRequest{EventType: "monitor.alert.firing", Severity: 3})
	if err != nil {
		t.Fatalf("PreviewRouting() error = %v", err)
	}
	if resp.GetSilenced() || resp.GetMatched().GetId() != created.GetRoute().GetId() || len(resp.GetCandidates()) != 1 ||
		len(resp.GetChannels()) != 1 || resp.GetChannels()[0].GetId() != chID || resp.GetChannels()[0].GetTemplateId() != "" || resp.GetAggregate() {
		t.Fatalf("PreviewRouting() = %+v", resp)
	}
	if resp, err := svc.PreviewRouting(ctx, &notifyv1.PreviewRoutingRequest{EventType: "ticket.apply.created", Severity: 3}); err != nil || resp.GetMatched() != nil || len(resp.GetChannels()) != 0 {
		t.Fatalf("no-route preview = %+v, %v", resp, err)
	}
	if _, err := biz.NewSilenceUsecase(store).Create(ctx, &biz.Silence{BizID: biz.DefaultBizID, Enabled: true, SilenceRule: biz.SilenceRule{EventPattern: "monitor.*", SeverityMax: 4}}); err != nil {
		t.Fatal(err)
	}
	if resp, err := svc.PreviewRouting(ctx, &notifyv1.PreviewRoutingRequest{EventType: "monitor.alert.firing", Severity: 3}); err != nil || !resp.GetSilenced() || len(resp.GetSilenceIds()) != 1 {
		t.Fatalf("silenced preview = %+v, %v", resp, err)
	}
	// 受限用户：目标 biz 不在范围 → 403。
	scoped := NewNotifyRouteService(biz.NewRouteUsecase(store, store), biz.NewPreviewUsecase(store, store, store, store), testfake.Scoped("ops"))
	if _, err := scoped.PreviewRouting(ctx, &notifyv1.PreviewRoutingRequest{EventType: "monitor.alert.firing", Severity: 3}); !errors.Is(err, biz.ErrBizForbidden) {
		t.Fatalf("scoped preview error = %v, want ErrBizForbidden", err)
	}
}
