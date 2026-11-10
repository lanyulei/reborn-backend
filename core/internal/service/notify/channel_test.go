package service

import (
	"strings"
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func newChannelService(t *testing.T) (*NotifyChannelService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	return NewNotifyChannelService(biz.NewChannelUsecase(store, store, store), testfake.Unrestricted()), store
}

// sptr 返回字符串指针（proto optional 字段赋值辅助）。
func sptr(s string) *string { return &s }

// newEmailCreate 返回合法邮件渠道创建请求。
func newEmailCreate(name string) *notifyv1.CreateChannelRequest {
	return &notifyv1.CreateChannelRequest{
		Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_EMAIL,
		Name: name,
		Detail: &notifyv1.CreateChannelRequest_Email{Email: &notifyv1.EmailChannelConfig{
			Host: "smtp.example.com", Port: 465, Username: "ops", FromAddr: "ops@example.com",
		}},
		Secret: sptr("smtp-password"),
	}
}

func TestNotifyChannelService_CreateRoundtrip(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	resp, err := svc.CreateChannel(ctx, newEmailCreate("email-corp"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	ch := resp.GetChannel()
	if ch.GetId() == "" || ch.GetBizId() != biz.DefaultBizID {
		t.Fatalf("CreateChannel() = %+v, want id and default biz", ch)
	}
	if !ch.GetEnabled() {
		t.Fatal("CreateChannel() enabled default = false, want true")
	}
	if ch.GetDetail().(*notifyv1.NotifyChannel_Email).Email.GetHost() != "smtp.example.com" {
		t.Fatalf("CreateChannel() detail = %+v", ch.GetDetail())
	}

	// webhook 类型：secret 可选、headers/timeout 往返。
	whResp, err := svc.CreateChannel(ctx, &notifyv1.CreateChannelRequest{
		Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WEBHOOK,
		Name: "webhook-audit",
		Detail: &notifyv1.CreateChannelRequest_Webhook{Webhook: &notifyv1.WebhookChannelConfig{
			Url: "https://audit.example.com/hook", Method: "POST",
			Headers: map[string]string{"X-Source": "core"}, TimeoutS: proto.Int32(3),
		}},
		RateLimit: &notifyv1.NotifyRateLimit{Qps: 5, Burst: 10},
	})
	if err != nil {
		t.Fatalf("CreateChannel(webhook) error = %v", err)
	}
	wh := whResp.GetChannel()
	detail, ok := wh.GetDetail().(*notifyv1.NotifyChannel_Webhook)
	if !ok || detail.Webhook.GetHeaders()["X-Source"] != "core" || detail.Webhook.GetTimeoutS() != 3 {
		t.Fatalf("CreateChannel(webhook) detail = %+v", wh.GetDetail())
	}
	if wh.GetRateLimit().GetQps() != 5 || wh.GetRateLimit().GetBurst() != 10 {
		t.Fatalf("CreateChannel(webhook) rateLimit = %+v", wh.GetRateLimit())
	}
}

// TestNotifyChannelService_SecretNeverReturned 出参契约：任何读路径不回填
// secret（proto 实体本无该字段，锁定实体不含凭据的结构性保证）。
func TestNotifyChannelService_SecretNeverReturned(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	created, err := svc.CreateChannel(ctx, newEmailCreate("email-alert"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	id := created.GetChannel().GetId()
	if created.GetChannel().String() == "" || created.String() == "" {
		t.Fatal("response not serializable")
	}

	got, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: id})
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	list, err := svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	// NotifyChannel 实体无 secret 字段（编译期结构保证）；此处锁运行时
	// 序列化输出不含凭据明文。
	for _, payload := range []string{got.String(), list.String(), created.String()} {
		if strings.Contains(payload, "smtp-password") {
			t.Fatalf("response payload leaks secret: %s", payload)
		}
	}
}

func TestNotifyChannelService_UpdateChannel(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	created, err := svc.CreateChannel(ctx, newEmailCreate("email-corp"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	id := created.GetChannel().GetId()

	// 改名 + 停用 + 换配置（同 type）。
	name := "email-corp-2"
	enabled := false
	_, err = svc.UpdateChannel(ctx, &notifyv1.UpdateChannelRequest{
		Id:      id,
		Name:    &name,
		Enabled: &enabled,
		Detail: &notifyv1.UpdateChannelRequest_Email{Email: &notifyv1.EmailChannelConfig{
			Host: "smtp2.example.com", Port: 587, FromAddr: "noreply@example.com", Starttls: true,
		}},
	})
	if err != nil {
		t.Fatalf("UpdateChannel() error = %v", err)
	}
	got, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: id})
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	if got.GetChannel().GetName() != name || got.GetChannel().GetEnabled() {
		t.Fatalf("UpdateChannel() result = %+v", got.GetChannel())
	}
	email := got.GetChannel().GetDetail().(*notifyv1.NotifyChannel_Email).Email
	if email.GetHost() != "smtp2.example.com" || !email.GetStarttls() {
		t.Fatalf("UpdateChannel() detail = %+v", email)
	}

	// secret 不传 = 保留旧密文：内存仓储出参恒不回填 secret（对齐 data 层），
	// 密文保留/轮换的字节级断言在 data 层集成测试以原生 SQL 校验。

	// config 分支与 type 不匹配拒绝。
	_, err = svc.UpdateChannel(ctx, &notifyv1.UpdateChannelRequest{
		Id: id,
		Detail: &notifyv1.UpdateChannelRequest_Webhook{Webhook: &notifyv1.WebhookChannelConfig{
			Url: "https://example.com/hook", Method: "POST", TimeoutS: proto.Int32(3),
		}},
	})
	if !errors.Is(err, biz.ErrChannelConfigInvalid) {
		t.Fatalf("UpdateChannel(kind mismatch) error = %v, want ErrChannelConfigInvalid", err)
	}

	// 不存在渠道。
	_, err = svc.UpdateChannel(ctx, &notifyv1.UpdateChannelRequest{Id: missingChannelID, Name: &name})
	if !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("UpdateChannel(missing) error = %v, want ErrChannelNotFound", err)
	}
}

// missingChannelID 是不存在于存储中的合法 UUID。
const missingChannelID = "018f3e1a-ffff-7000-8000-000000000001"

func TestNotifyChannelService_DeleteChannel(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	created, err := svc.CreateChannel(ctx, newEmailCreate("email-old"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	id := created.GetChannel().GetId()

	if _, err := svc.DeleteChannel(ctx, &notifyv1.DeleteChannelRequest{Id: id}); err != nil {
		t.Fatalf("DeleteChannel() error = %v", err)
	}
	if _, err := svc.GetChannel(ctx, &notifyv1.GetChannelRequest{Id: id}); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("GetChannel(deleted) error = %v, want ErrChannelNotFound", err)
	}
	if _, err := svc.DeleteChannel(ctx, &notifyv1.DeleteChannelRequest{Id: id}); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("DeleteChannel(twice) error = %v, want ErrChannelNotFound", err)
	}

	// 软删后名称可复用。
	if _, err := svc.CreateChannel(ctx, newEmailCreate("email-old")); err != nil {
		t.Fatalf("CreateChannel(reuse name) error = %v", err)
	}
}

func TestNotifyChannelService_ListChannels(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	if _, err := svc.CreateChannel(ctx, newEmailCreate("email-corp")); err != nil {
		t.Fatalf("CreateChannel(email) error = %v", err)
	}
	if _, err := svc.CreateChannel(ctx, &notifyv1.CreateChannelRequest{
		Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_DINGTALK,
		Name: "dingtalk-ops",
		Detail: &notifyv1.CreateChannelRequest_Robot{Robot: &notifyv1.RobotChannelConfig{
			AtMobiles: []string{"13800000000"},
		}},
		Secret: sptr("robot-token"),
	}); err != nil {
		t.Fatalf("CreateChannel(robot) error = %v", err)
	}

	// 全量。
	list, err := svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	if list.GetTotal() != 2 || len(list.GetItems()) != 2 {
		t.Fatalf("ListChannels() = %d items, total %d, want 2/2", len(list.GetItems()), list.GetTotal())
	}

	// type 过滤（proto optional：显式指定才生效）。
	typ := notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_EMAIL
	list, err = svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10, Type: &typ})
	if err != nil {
		t.Fatalf("ListChannels(type) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetName() != "email-corp" {
		t.Fatalf("ListChannels(type) = %+v", list.GetItems())
	}

	// 关键字过滤。
	kw := "dingtalk"
	list, err = svc.ListChannels(ctx, &notifyv1.ListChannelsRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListChannels(keyword) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetType() != notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_DINGTALK {
		t.Fatalf("ListChannels(keyword) = %+v", list.GetItems())
	}
}

func TestNotifyChannelService_CreateChannelErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	// 重名冲突（同 biz）。
	if _, err := svc.CreateChannel(ctx, newEmailCreate("dup")); err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	if _, err := svc.CreateChannel(ctx, newEmailCreate("dup")); !errors.Is(err, biz.ErrChannelNameExists) {
		t.Fatalf("CreateChannel(dup) error = %v, want ErrChannelNameExists", err)
	}

	// config 分支与 type 不匹配。
	bad := newEmailCreate("mismatch")
	bad.Type = notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WEBHOOK
	if _, err := svc.CreateChannel(ctx, bad); !errors.Is(err, biz.ErrChannelConfigInvalid) {
		t.Fatalf("CreateChannel(mismatch) error = %v, want ErrChannelConfigInvalid", err)
	}

	// 必需 secret 缺失。
	noSecret := newEmailCreate("nosecret")
	noSecret.Secret = nil
	if _, err := svc.CreateChannel(ctx, noSecret); !errors.Is(err, biz.ErrChannelConfigInvalid) {
		t.Fatalf("CreateChannel(no secret) error = %v, want ErrChannelConfigInvalid", err)
	}
}

func TestNotifyChannelService_SendTestEvent(t *testing.T) {
	t.Parallel()
	svc, store := newChannelService(t)
	ctx := testCtx()

	created, err := svc.CreateChannel(ctx, newEmailCreate("email-corp"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	id := created.GetChannel().GetId()

	// 缺省 spec：响应事件 = notify.test / internal / received-pending，
	// 载荷 Struct 呈现缺省测试上下文。
	resp, err := svc.SendTestEvent(ctx, &notifyv1.SendTestEventRequest{Id: id})
	if err != nil {
		t.Fatalf("SendTestEvent() error = %v", err)
	}
	event := resp.GetEvent()
	if event.GetId() == "" || event.GetEventType() != biz.TestEventTypeDefault {
		t.Fatalf("SendTestEvent() event = %+v, want default type %q", event, biz.TestEventTypeDefault)
	}
	if event.GetSource() != biz.EventSourceInternal || event.GetStatus() != biz.EventStatusReceived {
		t.Fatalf("SendTestEvent() source/status = %q/%d", event.GetSource(), event.GetStatus())
	}
	if event.GetPayload().GetFields()["test"].GetBoolValue() != true {
		t.Fatalf("SendTestEvent() payload = %+v, want test context", event.GetPayload())
	}

	// 落库原语收到同一事件（testfake InsertEvent 与查询同一存储）。
	items, total, err := store.ListEvents(ctx, biz.EventFilter{EventType: biz.TestEventTypeDefault}, 1, 10)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != event.GetId() {
		t.Fatalf("ListEvents() = %d items, want inserted event %s", total, event.GetId())
	}

	// 自定义 spec 往返：payload Struct → 原始 JSON 字节 → 响应 Struct。
	payload, err := structpb.NewStruct(map[string]any{"order": "A-1"})
	if err != nil {
		t.Fatalf("NewStruct() error = %v", err)
	}
	custom, err := svc.SendTestEvent(ctx, &notifyv1.SendTestEventRequest{
		Id:         id,
		EventType:  sptr("monitor.alert.test"),
		Title:      sptr("自定义测试"),
		Recipients: []string{"ops@example.com"},
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("SendTestEvent(custom) error = %v", err)
	}
	got := custom.GetEvent()
	if got.GetEventType() != "monitor.alert.test" || got.GetTitle() != "自定义测试" {
		t.Fatalf("SendTestEvent(custom) type/title = %q/%q", got.GetEventType(), got.GetTitle())
	}
	if len(got.GetRecipients()) != 1 || got.GetRecipients()[0] != "ops@example.com" {
		t.Fatalf("SendTestEvent(custom) recipients = %v", got.GetRecipients())
	}
	if got.GetPayload().GetFields()["order"].GetStringValue() != "A-1" {
		t.Fatalf("SendTestEvent(custom) payload = %+v, want order A-1", got.GetPayload())
	}
}

func TestNotifyChannelService_SendTestEventErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	// 渠道不存在 → 404。
	_, err := svc.SendTestEvent(ctx, &notifyv1.SendTestEventRequest{Id: missingChannelID})
	if !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("SendTestEvent(missing) error = %v, want ErrChannelNotFound", err)
	}

	// 停用渠道 → 409。
	created, err := svc.CreateChannel(ctx, newEmailCreate("email-off"))
	if err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	enabled := false
	if _, err := svc.UpdateChannel(ctx, &notifyv1.UpdateChannelRequest{Id: created.GetChannel().GetId(), Enabled: &enabled}); err != nil {
		t.Fatalf("UpdateChannel(disable) error = %v", err)
	}
	if _, err := svc.SendTestEvent(ctx, &notifyv1.SendTestEventRequest{Id: created.GetChannel().GetId()}); !errors.Is(err, biz.ErrChannelDisabled) {
		t.Fatalf("SendTestEvent(disabled) error = %v, want ErrChannelDisabled", err)
	}
}

func TestNotifyChannelService_WebhookTimeoutDefault(t *testing.T) {
	t.Parallel()
	svc, _ := newChannelService(t)
	ctx := testCtx()

	// timeout_s 未指定：service 层补齐缺省值后落库并回显，管道侧总能拿到确定超时。
	resp, err := svc.CreateChannel(ctx, &notifyv1.CreateChannelRequest{
		Type: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WEBHOOK,
		Name: "webhook-default-timeout",
		Detail: &notifyv1.CreateChannelRequest_Webhook{Webhook: &notifyv1.WebhookChannelConfig{
			Url: "https://example.com/hook", Method: "POST",
		}},
	})
	if err != nil {
		t.Fatalf("CreateChannel(webhook without timeout) error = %v", err)
	}
	detail, ok := resp.GetChannel().GetDetail().(*notifyv1.NotifyChannel_Webhook)
	if !ok || detail.Webhook.GetTimeoutS() != biz.DefaultWebhookTimeoutS {
		t.Fatalf("CreateChannel(webhook without timeout) detail = %+v, want timeout %d", resp.GetChannel().GetDetail(), biz.DefaultWebhookTimeoutS)
	}
}
