//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"github.com/go-kratos/kratos/v3/errors"
)

// 删除渠道与写入路由的并发窗口由数据层兜底（biz 层的检查都在事务之外）：
//   - 已软删的渠道不能再被路由引用（CreateRoute / UpdateRoute 在事务内锁渠道行后判定）；
//   - 仍被路由引用的渠道，即便绕过 biz 层直接调仓储，软删也会被拒。
func TestIntegration_RouteAndChannelDeleteGuardEachOther(t *testing.T) {
	d := newIntegrationNotifyData(t)
	ctx := context.Background()
	channels, routes := NewChannelRepo(d, newTestSecretBox(t)), NewRouteRepo(d)

	chLive := newChannelDO("018f3e1a-0301-7000-8000-000000000001", "live")
	chGone := newChannelDO("018f3e1a-0301-7000-8000-000000000002", "gone")
	for _, ch := range []*biz.Channel{chLive, chGone} {
		if err := channels.CreateChannel(ctx, ch); err != nil {
			t.Fatalf("CreateChannel(%s) error = %v", ch.Name, err)
		}
	}

	// 路由引用一个已软删的渠道：拒绝（软删后行仍在，锁行时能看到 delete_time）。
	if err := channels.DeleteChannel(ctx, chGone.ID); err != nil {
		t.Fatalf("DeleteChannel(unused) error = %v", err)
	}
	err := routes.CreateRoute(ctx, newRouteDO("018f3e1a-0302-7000-8000-000000000001", "monitor.alert.*", chLive.ID, chGone.ID))
	if !errors.Is(err, biz.ErrRouteInvalid) {
		t.Fatalf("CreateRoute referencing a deleted channel: error = %v, want ErrRouteInvalid", err)
	}

	// 引用在用渠道的路由可以建；此后该渠道不能再删（ErrChannelInUse），先解绑才能删。
	route := newRouteDO("018f3e1a-0302-7000-8000-000000000002", "monitor.alert.*", chLive.ID)
	if err := routes.CreateRoute(ctx, route); err != nil {
		t.Fatalf("CreateRoute(live channel) error = %v", err)
	}
	if err := channels.DeleteChannel(ctx, chLive.ID); !errors.Is(err, biz.ErrChannelInUse) {
		t.Fatalf("DeleteChannel(still referenced by a route): error = %v, want ErrChannelInUse", err)
	}
	if got, err := routes.GetRoute(ctx, route.ID); err != nil || got == nil {
		t.Fatalf("the refused delete must not touch the route: %v", err)
	}
	// 把路由改绑到别的（已删除的）渠道同样被拒；改绑为空渠道列表后，渠道可删。
	empty := newRouteDO(route.ID, "monitor.alert.*")
	empty.ChannelIDs = []string{} // 列是 NOT NULL：解绑须落空数组而不是 NULL
	if err := routes.UpdateRoute(ctx, route.ID, biz.RouteUpdate{Rule: &empty.RouteRule}); err != nil {
		t.Fatalf("UpdateRoute(unbind channel) error = %v", err)
	}
	still := newRouteDO(route.ID, "monitor.alert.*", chGone.ID)
	if err := routes.UpdateRoute(ctx, route.ID, biz.RouteUpdate{Rule: &still.RouteRule}); !errors.Is(err, biz.ErrRouteInvalid) {
		t.Fatalf("UpdateRoute rebinding to a deleted channel: error = %v, want ErrRouteInvalid", err)
	}
	if err := channels.DeleteChannel(ctx, chLive.ID); err != nil {
		t.Fatalf("DeleteChannel(after unbinding) error = %v", err)
	}
}
