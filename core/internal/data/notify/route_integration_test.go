//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/go-kratos/kratos/v3/errors"
)

// newRouteDO 返回带固定 ID 与审计时间的路由 DO（渠道绑定校验在 biz 层，
// 仓储不过问渠道存在性——此处引用任意合法 UUID）。
func newRouteDO(id, eventPattern string, channelIDs ...string) *biz.Route {
	return &biz.Route{
		ID:    id,
		BizID: biz.DefaultBizID,
		RouteRule: biz.RouteRule{
			EventPattern:      eventPattern,
			SeverityMin:       2,
			TagMatch:          map[string]string{"env": "prod"},
			Priority:          3,
			ChannelIDs:        channelIDs,
			Recipient:         biz.RouteRecipient{Fixed: []string{"ops@example.com"}},
			AggregateWindowS:  60,
			SuppressThreshold: 500,
			SuppressDurationM: 30,
		},
		Enabled:    true,
		CreateTime: time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime: time.Now().UTC().Truncate(time.Microsecond),
	}
}

// routeCfgVersion 读回 notify_route 的配置版本号。
func routeCfgVersion(t *testing.T, d *basedata.Data) int64 {
	t.Helper()
	var version int64
	if err := d.Pool.QueryRow(context.Background(),
		"SELECT version FROM notify_cfg_version WHERE table_name = 'notify_route'").Scan(&version); err != nil {
		t.Fatalf("read route cfg version: %v", err)
	}
	return version
}

func TestIntegration_RouteCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewRouteRepo(d)
	ctx := context.Background()

	chA := "018f3e1a-0301-7000-8000-000000000001"
	chB := "018f3e1a-0301-7000-8000-000000000002"
	alert := newRouteDO("018f3e1a-0302-7000-8000-000000000001", "monitor.alert.*", chA, chB)
	if err := repo.CreateRoute(ctx, alert); err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	if v := routeCfgVersion(t, d); v < 1 {
		t.Fatalf("cfg version after create = %d, want >= 1", v)
	}

	// 同 pattern 多条路由共存（设计即无唯一约束，priority 决胜）。
	tighter := newRouteDO("018f3e1a-0302-7000-8000-000000000002", "monitor.alert.*", chA)
	tighter.SeverityMin = 4
	if err := repo.CreateRoute(ctx, tighter); err != nil {
		t.Fatalf("CreateRoute(overlap) error = %v", err)
	}

	// 读路径全字段回填：jsonb（tag_match/recipient_policy）与 uuid[] 往返。
	got, err := repo.GetRoute(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
	if got.BizID != biz.DefaultBizID || got.EventPattern != "monitor.alert.*" ||
		got.SeverityMin != 2 || got.Priority != 3 || !got.Enabled {
		t.Fatalf("GetRoute() = %+v", got)
	}
	if len(got.TagMatch) != 1 || got.TagMatch["env"] != "prod" {
		t.Fatalf("GetRoute() tagMatch = %v", got.TagMatch)
	}
	if len(got.ChannelIDs) != 2 || got.ChannelIDs[0] != chA || got.ChannelIDs[1] != chB {
		t.Fatalf("GetRoute() channels = %v", got.ChannelIDs)
	}
	if len(got.Recipient.Fixed) != 1 || got.Recipient.Fixed[0] != "ops@example.com" {
		t.Fatalf("GetRoute() recipient = %+v", got.Recipient)
	}
	if got.AggregateWindowS != 60 || got.SuppressThreshold != 500 || got.SuppressDurationM != 30 {
		t.Fatalf("GetRoute() tuning = %d/%d/%d", got.AggregateWindowS, got.SuppressThreshold, got.SuppressDurationM)
	}
	if _, err := repo.GetRoute(ctx, missingChannelID); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("GetRoute(missing) error = %v, want ErrRouteNotFound", err)
	}

	// Rule 整组替换：tag_match 置 nil → 落 NULL；收件策略切换订阅；同事务 bump。
	vBefore := routeCfgVersion(t, d)
	if err := repo.UpdateRoute(ctx, alert.ID, biz.RouteUpdate{Rule: &biz.RouteRule{
		EventPattern:      "permission.change",
		SeverityMin:       1,
		Priority:          5,
		ChannelIDs:        []string{chB},
		Recipient:         biz.RouteRecipient{Subscription: true},
		AggregateWindowS:  0,
		SuppressThreshold: 500,
		SuppressDurationM: 30,
	}}); err != nil {
		t.Fatalf("UpdateRoute(rule) error = %v", err)
	}
	if vAfter := routeCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after update = %d, want %d (same-tx bump)", vAfter, vBefore+1)
	}
	got, err = repo.GetRoute(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
	if got.TagMatch != nil {
		t.Fatalf("UpdateRoute(rule) tagMatch = %v, want nil (cleared to NULL)", got.TagMatch)
	}
	if got.EventPattern != "permission.change" || len(got.ChannelIDs) != 1 || got.ChannelIDs[0] != chB {
		t.Fatalf("UpdateRoute(rule) result = %+v", got)
	}
	if !got.Recipient.Subscription || len(got.Recipient.Fixed) != 0 {
		t.Fatalf("UpdateRoute(rule) recipient = %+v", got.Recipient)
	}

	// 独立启停 + 不存在路由。
	disabled := false
	if err := repo.UpdateRoute(ctx, alert.ID, biz.RouteUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("UpdateRoute(enabled) error = %v", err)
	}
	if got, _ := repo.GetRoute(ctx, alert.ID); got.Enabled {
		t.Fatal("UpdateRoute(enabled) not applied")
	}
	if err := repo.UpdateRoute(ctx, missingChannelID, biz.RouteUpdate{Enabled: &disabled}); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("UpdateRoute(missing) error = %v, want ErrRouteNotFound", err)
	}

	// 列表：keyword（event_pattern 模糊）/ enabled 过滤与分页。
	items, total, err := repo.ListRoutes(ctx, biz.RouteFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListRoutes() error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListRoutes() = %d items, total %d, want 2/2", len(items), total)
	}
	items, total, err = repo.ListRoutes(ctx, biz.RouteFilter{Keyword: "permission"}, 1, 20)
	if err != nil {
		t.Fatalf("ListRoutes(keyword) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "permission.change" {
		t.Fatalf("ListRoutes(keyword) = %+v", items)
	}
	enabled := true
	items, total, err = repo.ListRoutes(ctx, biz.RouteFilter{Enabled: &enabled}, 1, 20)
	if err != nil {
		t.Fatalf("ListRoutes(enabled) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "monitor.alert.*" {
		t.Fatalf("ListRoutes(enabled) = %+v", items)
	}
	items, total, err = repo.ListRoutes(ctx, biz.RouteFilter{}, 2, 1)
	if err != nil {
		t.Fatalf("ListRoutes(paged) error = %v", err)
	}
	if total != 2 || len(items) != 1 {
		t.Fatalf("ListRoutes(paged) = %d items, total %d, want 1/2", len(items), total)
	}

	// 软删：读不可见、版本继续 bump、同 pattern 重建无冲突。
	vBefore = routeCfgVersion(t, d)
	if err := repo.DeleteRoute(ctx, alert.ID); err != nil {
		t.Fatalf("DeleteRoute() error = %v", err)
	}
	if vAfter := routeCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after delete = %d, want %d", vAfter, vBefore+1)
	}
	if _, err := repo.GetRoute(ctx, alert.ID); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("GetRoute(deleted) error = %v, want ErrRouteNotFound", err)
	}
	if err := repo.DeleteRoute(ctx, alert.ID); !errors.Is(err, biz.ErrRouteNotFound) {
		t.Fatalf("DeleteRoute(twice) error = %v, want ErrRouteNotFound", err)
	}
	rebuild := newRouteDO("018f3e1a-0302-7000-8000-000000000003", "permission.change", chA)
	if err := repo.CreateRoute(ctx, rebuild); err != nil {
		t.Fatalf("CreateRoute(rebuild same pattern) error = %v", err)
	}
}
