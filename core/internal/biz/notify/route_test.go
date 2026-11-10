package biz

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// routeRepoStub 路由仓储内存桩：承载用例断言所需的最小语义（软删不可见、
// Rule 整组替换、NotFound）。
type routeRepoStub struct {
	routes map[string]*Route
}

func newRouteRepoStub() *routeRepoStub {
	return &routeRepoStub{routes: make(map[string]*Route)}
}

func (s *routeRepoStub) CreateRoute(_ context.Context, route *Route) error {
	cp := *route
	cp.ChannelIDs = append([]string(nil), route.ChannelIDs...)
	cp.Recipient.Fixed = append([]string(nil), route.Recipient.Fixed...)
	s.routes[cp.ID] = &cp
	return nil
}

func (s *routeRepoStub) UpdateRoute(_ context.Context, id string, upd RouteUpdate) error {
	route, ok := s.routes[id]
	if !ok {
		return ErrRouteNotFound
	}
	if upd.Rule != nil {
		route.EventPattern = upd.Rule.EventPattern
		route.SeverityMin = upd.Rule.SeverityMin
		route.TagMatch = upd.Rule.TagMatch
		route.Priority = upd.Rule.Priority
		route.ChannelIDs = append([]string(nil), upd.Rule.ChannelIDs...)
		route.Recipient = upd.Rule.Recipient
		route.AggregateWindowS = upd.Rule.AggregateWindowS
		route.SuppressThreshold = upd.Rule.SuppressThreshold
		route.SuppressDurationM = upd.Rule.SuppressDurationM
	}
	if upd.Enabled != nil {
		route.Enabled = *upd.Enabled
	}
	route.UpdateTime = time.Now()
	return nil
}

func (s *routeRepoStub) DeleteRoute(_ context.Context, id string) error {
	if _, ok := s.routes[id]; !ok {
		return ErrRouteNotFound
	}
	delete(s.routes, id)
	return nil
}

func (s *routeRepoStub) GetRoute(_ context.Context, id string) (*Route, error) {
	route, ok := s.routes[id]
	if !ok {
		return nil, ErrRouteNotFound
	}
	cp := *route
	return &cp, nil
}

func (s *routeRepoStub) ListRoutes(_ context.Context, filter RouteFilter, page, pageSize int64) ([]*Route, int64, error) {
	var matched []*Route
	for _, route := range s.routes {
		if filter.Enabled != nil && route.Enabled != *filter.Enabled {
			continue
		}
		cp := *route
		matched = append(matched, &cp)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// seedChannel 预置一个渠道到渠道桩。
func seedChannel(t *testing.T, stub *channelRepoStub, id, bizID string) {
	t.Helper()
	if err := stub.CreateChannel(context.Background(), &Channel{
		ID: id, BizID: bizID, Type: ChannelTypeDingtalk, Name: "ch-" + id,
		Config: &ChannelConfig{Robot: &RobotConfig{}}, Enabled: true,
	}); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
}

// newRouteRule 返回合法告警路由规则入参（引用 seedChannel 预置的渠道）。
func newRouteRule(channelIDs ...string) *RouteRule {
	return &RouteRule{
		EventPattern:      "monitor.alert.*",
		SeverityMin:       2,
		Priority:          3,
		ChannelIDs:        channelIDs,
		Recipient:         RouteRecipient{Fixed: []string{"ops@example.com"}},
		AggregateWindowS:  60,
		SuppressThreshold: 500,
		SuppressDurationM: 30,
	}
}

// newRouteUC 组装路由用例（路由桩 + 渠道桩）。
func newRouteUC() (*RouteUsecase, *routeRepoStub, *channelRepoStub) {
	routes, channels := newRouteRepoStub(), newChannelRepoStub()
	return NewRouteUsecase(routes, channels), routes, channels
}

func TestRouteUsecase_Create(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)

	route := &Route{Enabled: true}
	route.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
	out, err := uc.Create(context.Background(), route)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(out.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", out.ID)
	}
	if out.BizID != DefaultBizID {
		t.Fatalf("Create() bizID = %q, want default", out.BizID)
	}
	if out.CreateTime.IsZero() || out.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}
	// TagMatch 深引用：入参 map 事后改动不影响落库对象。
	tagged := &Route{Enabled: true}
	tagged.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
	tagged.TagMatch = map[string]string{"env": "prod"}
	if _, err := uc.Create(context.Background(), tagged); err != nil {
		t.Fatalf("Create(tagged) error = %v", err)
	}
}

func TestRouteUsecase_CreatePatternInvalid(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)
	id := "018f3e1a-0301-7000-8000-000000000001"

	cases := []string{
		"",                   // 空
		"Monitor.Alert",      // 大写
		"monitor.*.firing",   // 中间通配（仅允许尾部）
		"monitor.alert.**",   // 双星
		"monitor.alert.*sub", // 星后缀非独立
		".alert",             // 前导点
		"monitor alert",      // 空格
	}
	for _, pattern := range cases {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule(id)
		route.EventPattern = pattern
		if _, err := uc.Create(context.Background(), route); !errors.Is(err, ErrRouteInvalid) {
			t.Errorf("Create(pattern %q) error = %v, want ErrRouteInvalid", pattern, err)
		}
	}
	// 合法形态：'*' / 精确 / 前缀通配。
	for _, pattern := range []string{"*", "monitor.alert.firing", "monitor.alert.*", "a", "monitor.*"} {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule(id)
		route.EventPattern = pattern
		if _, err := uc.Create(context.Background(), route); err != nil {
			t.Errorf("Create(pattern %q) error = %v, want nil", pattern, err)
		}
	}
}

func TestRouteUsecase_CreateBoundsInvalid(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)

	cases := []struct {
		name string
		mut  func(rule *RouteRule)
	}{
		{"severity 0", func(r *RouteRule) { r.SeverityMin = 0 }},
		{"severity 5", func(r *RouteRule) { r.SeverityMin = 5 }},
		{"priority 0", func(r *RouteRule) { r.Priority = 0 }},
		{"priority 10", func(r *RouteRule) { r.Priority = 10 }},
		{"window 10", func(r *RouteRule) { r.AggregateWindowS = 10 }},
		{"window 301", func(r *RouteRule) { r.AggregateWindowS = 301 }},
		{"threshold 0", func(r *RouteRule) { r.SuppressThreshold = 0 }},
		{"threshold 100001", func(r *RouteRule) { r.SuppressThreshold = 100001 }},
		{"duration 0", func(r *RouteRule) { r.SuppressDurationM = 0 }},
		{"duration 1441", func(r *RouteRule) { r.SuppressDurationM = 1441 }},
		{"no channels", func(r *RouteRule) { r.ChannelIDs = nil }},
		{"tag too many", func(r *RouteRule) {
			r.TagMatch = make(map[string]string, 17)
			for i := range 17 {
				r.TagMatch[string(rune('a'+i))] = "v"
			}
		}},
	}
	for _, tc := range cases {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
		tc.mut(&route.RouteRule)
		if _, err := uc.Create(context.Background(), route); !errors.Is(err, ErrRouteInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrRouteInvalid", tc.name, err)
		}
	}
	// 聚合窗口边界合法：0（不聚合）与 30/300。
	for _, w := range []int32{0, 30, 300} {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
		route.AggregateWindowS = w
		if _, err := uc.Create(context.Background(), route); err != nil {
			t.Errorf("Create(window %d) error = %v, want nil", w, err)
		}
	}
}

func TestRouteUsecase_CreateRecipientByChannelType(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	// 群机器人渠道（seedChannel 预置 dingtalk）：实例绑定群即隐含收件，收件策略可空。
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)
	robot := &Route{Enabled: true}
	robot.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
	robot.Recipient = RouteRecipient{}
	if _, err := uc.Create(context.Background(), robot); err != nil {
		t.Fatalf("Create(robot, empty recipient) error = %v, want nil", err)
	}

	// 邮件渠道：必须由固定收件人或订阅者提供地址。
	if err := channels.CreateChannel(context.Background(), &Channel{
		ID: "018f3e1a-0301-7000-8000-000000000002", BizID: DefaultBizID, Type: ChannelTypeEmail, Name: "mail",
		Config: &ChannelConfig{Email: &EmailConfig{Host: "smtp.example.com", Port: 465, FromAddr: "ops@example.com"}}, Enabled: true,
	}); err != nil {
		t.Fatalf("seed email channel: %v", err)
	}
	mail := &Route{Enabled: true}
	mail.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000002")
	mail.Recipient = RouteRecipient{}
	if _, err := uc.Create(context.Background(), mail); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Create(email, empty recipient) error = %v, want ErrRouteInvalid", err)
	}
	mail.Recipient = RouteRecipient{Subscription: true}
	if _, err := uc.Create(context.Background(), mail); err != nil {
		t.Fatalf("Create(email, subscription) error = %v, want nil", err)
	}
}

func TestRouteUsecase_CreateChannelBindingInvalid(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000002", "monitor")

	make := func(ids ...string) *Route {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule(ids...)
		return route
	}

	// 不存在的渠道。
	if _, err := uc.Create(context.Background(), make("018f3e1a-0301-7000-8000-0000000000ff")); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Create(missing channel) error = %v, want ErrRouteInvalid", err)
	}
	// 跨 biz 渠道。
	if _, err := uc.Create(context.Background(), make("018f3e1a-0301-7000-8000-000000000002")); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Create(cross-biz channel) error = %v, want ErrRouteInvalid", err)
	}
	// monitor biz 路由绑定 monitor 渠道合法（biz 隔离按路由维度）。
	mon := make("018f3e1a-0301-7000-8000-000000000002")
	mon.BizID = "monitor"
	if _, err := uc.Create(context.Background(), mon); err != nil {
		t.Fatalf("Create(monitor route) error = %v", err)
	}
	// 重复渠道 ID。
	if _, err := uc.Create(context.Background(), make(
		"018f3e1a-0301-7000-8000-000000000001",
		"018f3e1a-0301-7000-8000-000000000001")); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Create(duplicate channel) error = %v, want ErrRouteInvalid", err)
	}
	// 软删渠道视同不存在。
	if err := channels.DeleteChannel(context.Background(), "018f3e1a-0301-7000-8000-000000000001"); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if _, err := uc.Create(context.Background(), make("018f3e1a-0301-7000-8000-000000000001")); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Create(deleted channel) error = %v, want ErrRouteInvalid", err)
	}
}

func TestRouteUsecase_Update(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	ctx := context.Background()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)

	route := &Route{Enabled: true}
	route.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
	created, err := uc.Create(ctx, route)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Rule 整组替换。
	replaced := newRouteRule("018f3e1a-0301-7000-8000-000000000001")
	replaced.EventPattern = "permission.change"
	replaced.SeverityMin = 1
	replaced.AggregateWindowS = 0
	replaced.Recipient = RouteRecipient{Subscription: true}
	if err := uc.Update(ctx, created.ID, RouteUpdate{Rule: replaced}); err != nil {
		t.Fatalf("Update(rule) error = %v", err)
	}
	got, err := uc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.EventPattern != "permission.change" || got.AggregateWindowS != 0 || !got.Recipient.Subscription {
		t.Fatalf("Update(rule) result = %+v", got)
	}

	// 独立启停。
	disabled := false
	if err := uc.Update(ctx, created.ID, RouteUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("Update(enabled) error = %v", err)
	}
	if got, _ := uc.Get(ctx, created.ID); got.Enabled {
		t.Fatal("Update(enabled) not applied")
	}

	// 非法新规则拒绝（引用不存在渠道，按现路由 biz 校验）。
	bad := newRouteRule("018f3e1a-0301-7000-8000-0000000000ff")
	if err := uc.Update(ctx, created.ID, RouteUpdate{Rule: bad}); !errors.Is(err, ErrRouteInvalid) {
		t.Fatalf("Update(bad rule) error = %v, want ErrRouteInvalid", err)
	}
	// 不存在路由。
	if err := uc.Update(ctx, "missing-id", RouteUpdate{Rule: replaced}); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrRouteNotFound", err)
	}
}

func TestRouteUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	uc, _, channels := newRouteUC()
	ctx := context.Background()
	seedChannel(t, channels, "018f3e1a-0301-7000-8000-000000000001", DefaultBizID)

	patterns := []string{"monitor.alert.*", "permission.change", "*"}
	for _, pattern := range patterns {
		route := &Route{Enabled: true}
		route.RouteRule = *newRouteRule("018f3e1a-0301-7000-8000-000000000001")
		route.EventPattern = pattern
		if _, err := uc.Create(ctx, route); err != nil {
			t.Fatalf("Create(%q) error = %v", pattern, err)
		}
	}

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, RouteFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 3/3", len(items), total)
	}

	// enabled 过滤透传仓储：停用一条。
	disabled := false
	for _, item := range items {
		if item.EventPattern == "*" {
			if err := uc.Update(ctx, item.ID, RouteUpdate{Enabled: &disabled}); err != nil {
				t.Fatalf("Update() error = %v", err)
			}
		}
	}
	items, total, err = uc.List(ctx, RouteFilter{Enabled: &disabled}, 1, 20)
	if err != nil {
		t.Fatalf("List(enabled filter) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "*" {
		t.Fatalf("List(enabled filter) = %+v", items)
	}
}
