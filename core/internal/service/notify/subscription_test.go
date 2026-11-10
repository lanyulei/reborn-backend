package service

import (
	"context"
	"strings"
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/go-kratos/kratos/v3/errors"
	"github.com/golang-jwt/jwt/v5"
)

// adminResolverStub 固定返回给定管理员判定（代管校验桩）。
type adminResolverStub struct{ admin bool }

func (s adminResolverStub) IsAdminUser(context.Context, string) (bool, error) {
	return s.admin, nil
}

const (
	subOperatorID = "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b2001"
	subOtherID    = "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b2002"
)

// subClaimsContext 模拟鉴权中间件注入后的请求 ctx（操作者=subOperatorID）。
func subClaimsContext() context.Context {
	return jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: subOperatorID},
	})
}

func newSubscriptionService(t *testing.T, admin bool) *NotifySubscriptionService {
	t.Helper()
	return newSubscriptionServiceOn(testfake.NewStore(), admin)
}

// newSubscriptionServiceOn 在共享 store 上构造订阅 service（代管双方看同一数据）。
func newSubscriptionServiceOn(store *testfake.Store, admin bool) *NotifySubscriptionService {
	return NewNotifySubscriptionService(biz.NewSubscriptionUsecase(store), adminResolverStub{admin: admin})
}

// protoPrefs 返回合法偏好载荷（email 开关 + 钉钉目标群）。
func protoPrefs() map[string]*notifyv1.SubscriptionPref {
	return map[string]*notifyv1.SubscriptionPref{
		"email":    {Kind: &notifyv1.SubscriptionPref_Subscribed{Subscribed: true}},
		"dingtalk": {Kind: &notifyv1.SubscriptionPref_Target{Target: "$group:ops"}},
	}
}

func TestNotifySubscriptionService_UpsertRoundtrip(t *testing.T) {
	t.Parallel()
	svc := newSubscriptionService(t, false)
	ctx := subClaimsContext()

	// user_id 缺省=操作者本人；缺省项：biz=default、enabled=true。
	resp, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		EventPattern: "monitor.alert.*",
		ChannelPrefs: protoPrefs(),
	})
	if err != nil {
		t.Fatalf("UpsertSubscription() error = %v", err)
	}
	sub := resp.GetSubscription()
	if sub.GetId() == "" || sub.GetUserId() != subOperatorID || sub.GetBizId() != biz.DefaultBizID {
		t.Fatalf("UpsertSubscription() = %+v, want id/self-user/default biz", sub)
	}
	if !sub.GetEnabled() {
		t.Fatal("UpsertSubscription() enabled = false, want default true")
	}
	email := sub.GetChannelPrefs()["email"]
	if email == nil || !email.GetSubscribed() {
		t.Fatalf("UpsertSubscription() email pref = %+v", email)
	}
	dingtalk := sub.GetChannelPrefs()["dingtalk"]
	if dingtalk == nil || dingtalk.GetTarget() != "$group:ops" {
		t.Fatalf("UpsertSubscription() dingtalk pref = %+v", dingtalk)
	}

	// 同自然键再次保存 = 替换（同 id、偏好换向）。
	off := false
	resp2, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		EventPattern: "monitor.alert.*",
		ChannelPrefs: map[string]*notifyv1.SubscriptionPref{
			"wecom": {Kind: &notifyv1.SubscriptionPref_Subscribed{Subscribed: true}},
		},
		Enabled: &off,
	})
	if err != nil {
		t.Fatalf("UpsertSubscription(replace) error = %v", err)
	}
	re := resp2.GetSubscription()
	if re.GetId() != sub.GetId() || re.GetEnabled() {
		t.Fatalf("UpsertSubscription(replace) = %+v, want same id and disabled", re)
	}
	if len(re.GetChannelPrefs()) != 1 || re.GetChannelPrefs()["wecom"] == nil {
		t.Fatalf("UpsertSubscription(replace) prefs = %+v", re.GetChannelPrefs())
	}
}

func TestNotifySubscriptionService_UpsertScope(t *testing.T) {
	t.Parallel()
	ctx := subClaimsContext()

	// 非管理员显式操作本人：合法。
	svc := newSubscriptionService(t, false)
	if _, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		UserId:       sptr(subOperatorID),
		EventPattern: "monitor.alert.*",
		ChannelPrefs: protoPrefs(),
	}); err != nil {
		t.Fatalf("UpsertSubscription(self explicit) error = %v", err)
	}

	// 非管理员指定他人：403 代管拒绝。
	if _, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		UserId:       sptr(subOtherID),
		EventPattern: "monitor.alert.*",
		ChannelPrefs: protoPrefs(),
	}); !errors.Is(err, biz.ErrSubscriptionForbidden) {
		t.Fatalf("UpsertSubscription(other, non-admin) error = %v, want ErrSubscriptionForbidden", err)
	}

	// 管理员代管他人：合法且归属目标用户。
	adminSvc := newSubscriptionService(t, true)
	resp, err := adminSvc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		UserId:       sptr(subOtherID),
		EventPattern: "permission.change",
		ChannelPrefs: protoPrefs(),
	})
	if err != nil {
		t.Fatalf("UpsertSubscription(other, admin) error = %v", err)
	}
	if resp.GetSubscription().GetUserId() != subOtherID {
		t.Fatalf("UpsertSubscription(other, admin) userId = %q, want %q",
			resp.GetSubscription().GetUserId(), subOtherID)
	}

	// 非法偏好（webhook 不可订阅）：biz 哨兵拒绝。
	if _, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		EventPattern: "monitor.alert.*",
		ChannelPrefs: map[string]*notifyv1.SubscriptionPref{
			"webhook": {Kind: &notifyv1.SubscriptionPref_Subscribed{Subscribed: true}},
		},
	}); !errors.Is(err, biz.ErrSubscriptionInvalid) {
		t.Fatalf("UpsertSubscription(webhook) error = %v, want ErrSubscriptionInvalid", err)
	}
}

func TestNotifySubscriptionService_DeleteOwnership(t *testing.T) {
	t.Parallel()
	ctx := subClaimsContext()

	// 管理员/非管理员两个 service 共享同一数据源（代管双方看同一订阅）。
	store := testfake.NewStore()
	adminSvc := newSubscriptionServiceOn(store, true)
	svc := newSubscriptionServiceOn(store, false)

	// 以管理员代管他人建一条订阅。
	created, err := adminSvc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		UserId:       sptr(subOtherID),
		EventPattern: "monitor.alert.*",
		ChannelPrefs: protoPrefs(),
	})
	if err != nil {
		t.Fatalf("UpsertSubscription(admin) error = %v", err)
	}
	otherSubID := created.GetSubscription().GetId()

	// 非管理员删他人订阅：403。
	if _, err := svc.DeleteSubscription(ctx, &notifyv1.DeleteSubscriptionRequest{Id: otherSubID}); !errors.Is(err, biz.ErrSubscriptionForbidden) {
		t.Fatalf("DeleteSubscription(other, non-admin) error = %v, want ErrSubscriptionForbidden", err)
	}
	// 管理员删他人订阅：合法。
	if _, err := adminSvc.DeleteSubscription(ctx, &notifyv1.DeleteSubscriptionRequest{Id: otherSubID}); err != nil {
		t.Fatalf("DeleteSubscription(other, admin) error = %v", err)
	}
	// 再删：NotFound。
	if _, err := adminSvc.DeleteSubscription(ctx, &notifyv1.DeleteSubscriptionRequest{Id: otherSubID}); !errors.Is(err, biz.ErrSubscriptionNotFound) {
		t.Fatalf("DeleteSubscription(twice) error = %v, want ErrSubscriptionNotFound", err)
	}

	// 本人删自己的：合法。
	mine, err := svc.UpsertSubscription(ctx, &notifyv1.UpsertSubscriptionRequest{
		EventPattern: "*",
		ChannelPrefs: protoPrefs(),
	})
	if err != nil {
		t.Fatalf("UpsertSubscription(self) error = %v", err)
	}
	if _, err := svc.DeleteSubscription(ctx, &notifyv1.DeleteSubscriptionRequest{
		Id: mine.GetSubscription().GetId(),
	}); err != nil {
		t.Fatalf("DeleteSubscription(self) error = %v", err)
	}
}

func TestNotifySubscriptionService_ListScope(t *testing.T) {
	t.Parallel()
	ctx := subClaimsContext()
	svc := newSubscriptionService(t, true) // 建数据用管理员

	for _, tc := range []struct {
		user  string
		pref  string
		prefs map[string]*notifyv1.SubscriptionPref
	}{
		{subOperatorID, "monitor.alert.*", protoPrefs()},
		{subOperatorID, "permission.change", protoPrefs()},
		{subOtherID, "monitor.alert.*", protoPrefs()},
	} {
		req := &notifyv1.UpsertSubscriptionRequest{
			UserId:       sptr(tc.user),
			EventPattern: tc.pref,
			ChannelPrefs: tc.prefs,
		}
		if _, err := svc.UpsertSubscription(ctx, req); err != nil {
			t.Fatalf("UpsertSubscription(%s,%s) error = %v", tc.user, tc.pref, err)
		}
	}

	// 缺省=本人：只见自己的两条。
	list, err := svc.ListSubscriptions(ctx, &notifyv1.ListSubscriptionsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListSubscriptions(self) error = %v", err)
	}
	if list.GetTotal() != 2 || len(list.GetItems()) != 2 {
		t.Fatalf("ListSubscriptions(self) = %d items, total %d, want 2/2", len(list.GetItems()), list.GetTotal())
	}
	for _, item := range list.GetItems() {
		if item.GetUserId() != subOperatorID {
			t.Fatalf("ListSubscriptions(self) leaked user %q", item.GetUserId())
		}
	}

	// 非管理员指定他人：403。
	plain := newSubscriptionService(t, false)
	if _, err := plain.ListSubscriptions(ctx, &notifyv1.ListSubscriptionsRequest{
		Page: 1, PageSize: 10, UserId: sptr(subOtherID),
	}); !errors.Is(err, biz.ErrSubscriptionForbidden) {
		t.Fatalf("ListSubscriptions(other, non-admin) error = %v, want ErrSubscriptionForbidden", err)
	}

	// 管理员代管 + keyword / enabled 过滤。
	kw := "permission"
	list, err = svc.ListSubscriptions(ctx, &notifyv1.ListSubscriptionsRequest{
		Page: 1, PageSize: 10, UserId: sptr(subOperatorID), Keyword: &kw,
	})
	if err != nil {
		t.Fatalf("ListSubscriptions(keyword) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetEventPattern() != "permission.change" {
		t.Fatalf("ListSubscriptions(keyword) = %+v", list.GetItems())
	}
	off := false
	list, err = svc.ListSubscriptions(ctx, &notifyv1.ListSubscriptionsRequest{
		Page: 1, PageSize: 10, UserId: sptr(subOperatorID), Enabled: &off,
	})
	if err != nil {
		t.Fatalf("ListSubscriptions(enabled) error = %v", err)
	}
	if list.GetTotal() != 0 {
		t.Fatalf("ListSubscriptions(enabled) total = %d, want 0", list.GetTotal())
	}
}

// TestNotifySubscriptionService_UpsertRejectsEmptyPref oneof 未设置的偏好条目
// 不得静默丢弃（否则请求 200 但该渠道未保存）。
func TestNotifySubscriptionService_UpsertRejectsEmptyPref(t *testing.T) {
	t.Parallel()
	svc := newSubscriptionService(t, false)
	_, err := svc.UpsertSubscription(subClaimsContext(), &notifyv1.UpsertSubscriptionRequest{
		EventPattern: "monitor.alert.*",
		ChannelPrefs: map[string]*notifyv1.SubscriptionPref{
			"email":    {},
			"dingtalk": {Kind: &notifyv1.SubscriptionPref_Subscribed{Subscribed: true}},
		},
	})
	if !errors.Is(err, biz.ErrSubscriptionInvalid) {
		t.Fatalf("UpsertSubscription(empty pref) error = %v, want ErrSubscriptionInvalid", err)
	}
}

// TestNotifySubscriptionService_SelfIDCaseInsensitive 请求携带大写形态的本人
// UUID（string.uuid 校验放行）仍判为本人，不触发代管 403。
func TestNotifySubscriptionService_SelfIDCaseInsensitive(t *testing.T) {
	t.Parallel()
	svc := newSubscriptionService(t, false)
	upper := strings.ToUpper(subOperatorID)
	resp, err := svc.UpsertSubscription(subClaimsContext(), &notifyv1.UpsertSubscriptionRequest{
		UserId:       &upper,
		EventPattern: "monitor.alert.*",
		ChannelPrefs: protoPrefs(),
	})
	if err != nil {
		t.Fatalf("UpsertSubscription(upper-case self id) error = %v", err)
	}
	if resp.GetSubscription().GetUserId() != subOperatorID {
		t.Fatalf("user_id = %q, want canonical operator id", resp.GetSubscription().GetUserId())
	}
}
