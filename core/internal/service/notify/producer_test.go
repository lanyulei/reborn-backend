package service

import (
	"strings"
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newProducerService 组装生产者 service（testfake 全仓储内存实现）。
func newProducerService(t *testing.T) *NotifyProducerService {
	t.Helper()
	return NewNotifyProducerService(biz.NewProducerUsecase(testfake.NewStore()), testfake.Unrestricted())
}

// protoProducerSpec 返回合法生产者配置载荷（白名单两条 + 一小时后过期）。
func protoProducerSpec() *notifyv1.ProducerSpec {
	return &notifyv1.ProducerSpec{
		Name:           "monitor-engine",
		EventWhitelist: []string{"monitor.alert.*", "monitor.host.down"},
		ExpireTime:     timestamppb.New(time.Now().UTC().Add(time.Hour)),
	}
}

func TestNotifyProducerService_CreateRoundtrip(t *testing.T) {
	t.Parallel()
	svc := newProducerService(t)
	ctx := testCtx()

	// 缺省项：biz=default、enabled=true；token 明文仅本响应出现一次。
	resp, err := svc.CreateProducer(ctx, &notifyv1.CreateProducerRequest{Spec: protoProducerSpec()})
	if err != nil {
		t.Fatalf("CreateProducer() error = %v", err)
	}
	producer := resp.GetProducer()
	if producer.GetId() == "" || producer.GetBizId() != biz.DefaultBizID || !producer.GetEnabled() {
		t.Fatalf("CreateProducer() = %+v, want id/default biz/enabled", producer)
	}
	if len(producer.GetEventWhitelist()) != 2 ||
		producer.GetEventWhitelist()[0] != "monitor.alert.*" {
		t.Fatalf("CreateProducer() whitelist = %v", producer.GetEventWhitelist())
	}
	if producer.GetExpireTime() == nil || !producer.GetExpireTime().IsValid() {
		t.Fatalf("CreateProducer() expireTime = %v, want set", producer.GetExpireTime())
	}
	if token := resp.GetToken(); !strings.HasPrefix(token, "npt_") || len(token) != 47 {
		t.Fatalf("CreateProducer() token = %q, want npt_ + 43 base64url chars", token)
	}

	// 读路径恒不回显 token / 哈希（proto 实体无该字段，Get/List 复用同一转换）。
	got, err := svc.GetProducer(ctx, &notifyv1.GetProducerRequest{Id: producer.GetId()})
	if err != nil {
		t.Fatalf("GetProducer() error = %v", err)
	}
	if got.GetProducer().GetId() != producer.GetId() ||
		got.GetProducer().GetName() != "monitor-engine" {
		t.Fatalf("GetProducer() = %+v", got.GetProducer())
	}

	// 同 biz 重名：409 哨兵。
	if _, err := svc.CreateProducer(ctx, &notifyv1.CreateProducerRequest{
		Spec: &notifyv1.ProducerSpec{Name: "monitor-engine"},
	}); !errors.Is(err, biz.ErrProducerNameExists) {
		t.Fatalf("CreateProducer(dup name) error = %v, want ErrProducerNameExists", err)
	}

	// 非法配置（白名单条目带空格）：400 哨兵。
	if _, err := svc.CreateProducer(ctx, &notifyv1.CreateProducerRequest{
		Spec: &notifyv1.ProducerSpec{Name: "engine", EventWhitelist: []string{"monitor alert"}},
	}); !errors.Is(err, biz.ErrProducerInvalid) {
		t.Fatalf("CreateProducer(bad whitelist) error = %v, want ErrProducerInvalid", err)
	}
}

func TestNotifyProducerService_UpdateAndRotate(t *testing.T) {
	t.Parallel()
	svc := newProducerService(t)
	ctx := testCtx()

	created, err := svc.CreateProducer(ctx, &notifyv1.CreateProducerRequest{
		BizId: sptr("monitor"), Spec: protoProducerSpec(),
	})
	if err != nil {
		t.Fatalf("CreateProducer() error = %v", err)
	}
	id := created.GetProducer().GetId()

	// spec 整组替换：不传 expire_time 即清除；biz_id 恒不可改（实体透传库内值）。
	if _, err := svc.UpdateProducer(ctx, &notifyv1.UpdateProducerRequest{
		Id:   id,
		Spec: &notifyv1.ProducerSpec{Name: "monitor-engine-v2", EventWhitelist: []string{"monitor.host.down"}},
	}); err != nil {
		t.Fatalf("UpdateProducer(spec) error = %v", err)
	}
	got, err := svc.GetProducer(ctx, &notifyv1.GetProducerRequest{Id: id})
	if err != nil {
		t.Fatalf("GetProducer() error = %v", err)
	}
	p := got.GetProducer()
	if p.GetName() != "monitor-engine-v2" || len(p.GetEventWhitelist()) != 1 ||
		p.GetExpireTime() != nil || p.GetBizId() != "monitor" {
		t.Fatalf("UpdateProducer(spec) result = %+v", p)
	}

	// enabled 独立启停。
	off := false
	if _, err := svc.UpdateProducer(ctx, &notifyv1.UpdateProducerRequest{Id: id, Enabled: &off}); err != nil {
		t.Fatalf("UpdateProducer(enabled) error = %v", err)
	}
	if got, _ = svc.GetProducer(ctx, &notifyv1.GetProducerRequest{Id: id}); got.GetProducer().GetEnabled() {
		t.Fatal("UpdateProducer(enabled) not applied")
	}

	// 轮换：返回新明文（与初发不同）且实体刷新。
	rotated, err := svc.RotateProducerToken(ctx, &notifyv1.RotateProducerTokenRequest{Id: id})
	if err != nil {
		t.Fatalf("RotateProducerToken() error = %v", err)
	}
	if rotated.GetToken() == created.GetToken() || !strings.HasPrefix(rotated.GetToken(), "npt_") {
		t.Fatalf("RotateProducerToken() token = %q, want fresh npt_ token", rotated.GetToken())
	}
	// 轮换只换哈希：enabled 等其余字段保持（上一停用仍生效）。
	if rotated.GetProducer().GetId() != id || rotated.GetProducer().GetEnabled() {
		t.Fatalf("RotateProducerToken() producer = %+v", rotated.GetProducer())
	}
	// 不存在：404。
	if _, err := svc.RotateProducerToken(ctx, &notifyv1.RotateProducerTokenRequest{
		Id: "018f3e1a-0304-7000-8000-00000000dead",
	}); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("RotateProducerToken(missing) error = %v, want ErrProducerNotFound", err)
	}
}

func TestNotifyProducerService_DeleteAndList(t *testing.T) {
	t.Parallel()
	svc := newProducerService(t)
	ctx := testCtx()

	for _, tc := range []struct {
		name    string
		biz     *string
		enabled *bool
	}{
		{name: "monitor-engine"},
		{name: "monitor-reporter"},
		{name: "permission-engine", enabled: func() *bool { v := false; return &v }()},
	} {
		if _, err := svc.CreateProducer(ctx, &notifyv1.CreateProducerRequest{
			Spec: &notifyv1.ProducerSpec{Name: tc.name}, Enabled: tc.enabled,
		}); err != nil {
			t.Fatalf("CreateProducer(%q) error = %v", tc.name, err)
		}
	}

	// keyword / enabled 过滤 + 分页。
	kw := "monitor"
	list, err := svc.ListProducers(ctx, &notifyv1.ListProducersRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListProducers(keyword) error = %v", err)
	}
	if list.GetTotal() != 2 || len(list.GetItems()) != 2 {
		t.Fatalf("ListProducers(keyword) = %d items, total %d, want 2/2", len(list.GetItems()), list.GetTotal())
	}
	on := true
	list, err = svc.ListProducers(ctx, &notifyv1.ListProducersRequest{Page: 1, PageSize: 10, Enabled: &on})
	if err != nil {
		t.Fatalf("ListProducers(enabled) error = %v", err)
	}
	if list.GetTotal() != 2 {
		t.Fatalf("ListProducers(enabled) total = %d, want 2", list.GetTotal())
	}
	list, err = svc.ListProducers(ctx, &notifyv1.ListProducersRequest{Page: 2, PageSize: 2})
	if err != nil {
		t.Fatalf("ListProducers(paged) error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 1 {
		t.Fatalf("ListProducers(paged) = %d items, total %d, want 1/3", len(list.GetItems()), list.GetTotal())
	}

	// 删除：软删不可见、重复删 404。
	victim := list.GetItems()[0].GetId()
	if _, err := svc.DeleteProducer(ctx, &notifyv1.DeleteProducerRequest{Id: victim}); err != nil {
		t.Fatalf("DeleteProducer() error = %v", err)
	}
	if _, err := svc.GetProducer(ctx, &notifyv1.GetProducerRequest{Id: victim}); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("GetProducer(deleted) error = %v, want ErrProducerNotFound", err)
	}
	if _, err := svc.DeleteProducer(ctx, &notifyv1.DeleteProducerRequest{Id: victim}); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("DeleteProducer(twice) error = %v, want ErrProducerNotFound", err)
	}
}
