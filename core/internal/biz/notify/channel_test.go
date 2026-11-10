package biz

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
)

// uuidV7Pattern 校验 UUIDv7：版本位为 7、variant 位为 10xx（与 system 域同式）。
var uuidV7Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// channelRepoStub 渠道仓储内存桩：只承载用例断言所需的最小语义
// （重名冲突、软删不可见、nil 字段不更新）。
type channelRepoStub struct {
	channels map[string]*Channel
	refs     map[string]int64 // channelID -> 引用它的路由数
}

func newChannelRepoStub() *channelRepoStub {
	return &channelRepoStub{channels: make(map[string]*Channel)}
}

func (s *channelRepoStub) CreateChannel(_ context.Context, ch *Channel) error {
	for _, existing := range s.channels {
		if existing.BizID == ch.BizID && existing.Name == ch.Name {
			return ErrChannelNameExists
		}
	}
	cp := *ch
	s.channels[cp.ID] = &cp
	return nil
}

func (s *channelRepoStub) UpdateChannel(_ context.Context, id string, upd ChannelUpdate) error {
	ch, ok := s.channels[id]
	if !ok {
		return ErrChannelNotFound
	}
	if upd.Name != nil {
		ch.Name = *upd.Name
	}
	if upd.Config != nil {
		ch.Config = upd.Config
	}
	if upd.RateLimit != nil {
		ch.RateLimit = *upd.RateLimit
	}
	if upd.Enabled != nil {
		ch.Enabled = *upd.Enabled
	}
	if upd.Secret != nil {
		ch.Secret = *upd.Secret
	}
	return nil
}

func (s *channelRepoStub) DeleteChannel(_ context.Context, id string) error {
	if _, ok := s.channels[id]; !ok {
		return ErrChannelNotFound
	}
	delete(s.channels, id)
	return nil
}

func (s *channelRepoStub) GetChannel(_ context.Context, id string) (*Channel, error) {
	ch, ok := s.channels[id]
	if !ok {
		return nil, ErrChannelNotFound
	}
	cp := *ch
	return &cp, nil
}

func (s *channelRepoStub) ListChannels(_ context.Context, filter ChannelFilter, page, pageSize int64) ([]*Channel, int64, error) {
	var matched []*Channel
	for _, ch := range s.channels {
		if filter.Type != nil && ch.Type != *filter.Type {
			continue
		}
		if filter.Enabled != nil && ch.Enabled != *filter.Enabled {
			continue
		}
		cp := *ch
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

// newEmailChannel 返回合法邮件渠道入参（Enabled 由 service 层解析 proto
// optional 默认值后传入，biz 层按值落库不重复默认）。
func newEmailChannel(name string) *Channel {
	return &Channel{
		Type:    ChannelTypeEmail,
		Name:    name,
		Config:  &ChannelConfig{Email: &EmailConfig{Host: "smtp.example.com", Port: 465, Username: "ops", FromAddr: "ops@example.com"}},
		Secret:  "smtp-password",
		Enabled: true,
	}
}

func TestChannelUsecase_Create(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)

	ch, err := uc.Create(context.Background(), newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(ch.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", ch.ID)
	}
	if ch.BizID != DefaultBizID {
		t.Fatalf("Create() bizID = %q, want default %q", ch.BizID, DefaultBizID)
	}
	if ch.CreateTime.IsZero() || ch.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}
	if !ch.Enabled {
		t.Fatal("Create() enabled passthrough = false, want true")
	}
	if ch.Secret != "" {
		t.Fatal("Create() output must not carry secret")
	}
}

func TestChannelUsecase_CreateBizIDExplicit(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)

	ch := newEmailChannel("email-alert")
	ch.BizID = "monitor"
	out, err := uc.Create(context.Background(), ch)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if out.BizID != "monitor" {
		t.Fatalf("Create() bizID = %q, want explicit monitor", out.BizID)
	}
}

func TestChannelUsecase_CreateDuplicateName(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	if _, err := uc.Create(ctx, newEmailChannel("email-corp")); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	_, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if !errors.Is(err, ErrChannelNameExists) {
		t.Fatalf("Create(duplicate name) error = %v, want ErrChannelNameExists", err)
	}
}

func TestChannelUsecase_CreateConfigKindMismatch(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)

	cases := []struct {
		name string
		ch   *Channel
	}{
		{
			name: "email type with webhook config",
			ch: &Channel{
				Type:   ChannelTypeEmail,
				Name:   "mixed",
				Config: &ChannelConfig{Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "POST", TimeoutS: 3}},
				Secret: "smtp-password",
			},
		},
		{
			name: "email type with no config",
			ch: &Channel{
				Type:   ChannelTypeEmail,
				Name:   "bare",
				Secret: "smtp-password",
			},
		},
		{
			name: "email type with two branches",
			ch: &Channel{
				Type: ChannelTypeEmail,
				Name: "double",
				Config: &ChannelConfig{
					Email:   &EmailConfig{Host: "smtp.example.com", Port: 465, FromAddr: "ops@example.com"},
					Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "POST", TimeoutS: 3},
				},
				Secret: "smtp-password",
			},
		},
		{
			name: "unknown type",
			ch: &Channel{
				Type:   "carrier-pigeon",
				Name:   "pigeon",
				Config: &ChannelConfig{Robot: &RobotConfig{}},
				Secret: "seed",
			},
		},
	}
	for _, tc := range cases {
		_, err := uc.Create(context.Background(), tc.ch)
		if !errors.Is(err, ErrChannelConfigInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrChannelConfigInvalid", tc.name, err)
		}
	}
}

func TestChannelUsecase_CreateSecretRequired(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	// email/群机器人缺 secret 拒绝。
	email := newEmailChannel("email-nosecret")
	email.Secret = ""
	if _, err := uc.Create(ctx, email); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Create(email without secret) error = %v, want ErrChannelConfigInvalid", err)
	}
	robot := &Channel{
		Type:   ChannelTypeDingtalk,
		Name:   "dingtalk-ops",
		Config: &ChannelConfig{Robot: &RobotConfig{AtMobiles: []string{"13800000000"}}},
		Secret: "",
	}
	if _, err := uc.Create(ctx, robot); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Create(robot without secret) error = %v, want ErrChannelConfigInvalid", err)
	}

	// webhook 的 HMAC 密钥可选。
	webhook := &Channel{
		Type:   ChannelTypeWebhook,
		Name:   "webhook-audit",
		Config: &ChannelConfig{Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "POST", TimeoutS: 3}},
	}
	out, err := uc.Create(ctx, webhook)
	if err != nil {
		t.Fatalf("Create(webhook without secret) error = %v", err)
	}
	if out.ID == "" {
		t.Fatal("Create(webhook without secret) not persisted")
	}
}

func TestChannelUsecase_Update(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	ch, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// 改名 + 停用 + 限流。
	name := "email-corp-2"
	enabled := false
	if err := uc.Update(ctx, ch.ID, ChannelUpdate{
		Name:      &name,
		Enabled:   &enabled,
		RateLimit: &RateLimit{QPS: 5, Burst: 10},
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	got, err := uc.Get(ctx, ch.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != name || got.Enabled || got.RateLimit.QPS != 5 || got.RateLimit.Burst != 10 {
		t.Fatalf("Update() result = %+v", got)
	}

	// config 替换须与现有 type 匹配（type 不可改）。
	bad := &ChannelConfig{Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "POST", TimeoutS: 3}}
	if err := uc.Update(ctx, ch.ID, ChannelUpdate{Config: bad}); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Update(config kind mismatch) error = %v, want ErrChannelConfigInvalid", err)
	}
	// 匹配的 config 正常替换。
	host := "smtp2.example.com"
	if err := uc.Update(ctx, ch.ID, ChannelUpdate{
		Config: &ChannelConfig{Email: &EmailConfig{Host: host, Port: 587, FromAddr: "noreply@example.com", StartTLS: true}},
	}); err != nil {
		t.Fatalf("Update(config) error = %v", err)
	}
	got, err = uc.Get(ctx, ch.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Config.Email.Host != host || !got.Config.Email.StartTLS {
		t.Fatalf("Update(config) result = %+v", got.Config.Email)
	}

	// 空字符串 secret 拒绝（轮换不允许传空）。
	empty := ""
	if err := uc.Update(ctx, ch.ID, ChannelUpdate{Secret: &empty}); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Update(empty secret) error = %v, want ErrChannelConfigInvalid", err)
	}

	// 不存在渠道。
	if err := uc.Update(ctx, "missing-id", ChannelUpdate{Name: &name}); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrChannelNotFound", err)
	}
}

func TestChannelUsecase_Delete(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	ch, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := uc.Delete(ctx, ch.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := uc.Delete(ctx, ch.ID); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("Delete(twice) error = %v, want ErrChannelNotFound", err)
	}
}

func TestChannelUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	for _, name := range []string{"email-corp", "email-alert", "webhook-audit"} {
		ch := newEmailChannel(name)
		if name == "webhook-audit" {
			ch.Type = ChannelTypeWebhook
			ch.Config = &ChannelConfig{Webhook: &WebhookConfig{URL: "https://example.com/hook", Method: "POST", TimeoutS: 3}}
		}
		if _, err := uc.Create(ctx, ch); err != nil {
			t.Fatalf("Create(%s) error = %v", name, err)
		}
	}

	// 非法分页参数规整为默认值（page=1/pageSize=20），不报错。
	items, total, err := uc.List(ctx, ChannelFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 3/3", len(items), total)
	}

	// type 过滤透传仓储。
	typ := ChannelTypeEmail
	items, total, err = uc.List(ctx, ChannelFilter{Type: &typ}, 1, 20)
	if err != nil {
		t.Fatalf("List(type filter) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("List(type filter) = %d items, total %d, want 2/2", len(items), total)
	}
}

// eventWriterStub 事件落库桩：记录 InsertEvent 调用供断言。
type eventWriterStub struct {
	inserted []*Event
}

func (s *eventWriterStub) InsertEvent(_ context.Context, event *Event) error {
	cp := *event
	cp.Payload = append([]byte(nil), event.Payload...)
	cp.Recipients = append([]string(nil), event.Recipients...)
	s.inserted = append(s.inserted, &cp)
	return nil
}

func TestChannelUsecase_SendTestEventDefaults(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	events := &eventWriterStub{}
	uc := NewChannelUsecase(stub, events, stub)
	ctx := context.Background()

	ch, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	event, err := uc.SendTestEvent(ctx, ch.ID, TestEventSpec{})
	if err != nil {
		t.Fatalf("SendTestEvent() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(event.ID) {
		t.Fatalf("SendTestEvent() id = %q, want UUIDv7", event.ID)
	}
	if event.EventType != TestEventTypeDefault {
		t.Fatalf("SendTestEvent() eventType = %q, want default %q", event.EventType, TestEventTypeDefault)
	}
	if event.Title != TestEventTitleDefault {
		t.Fatalf("SendTestEvent() title = %q, want default %q", event.Title, TestEventTitleDefault)
	}
	if event.Severity != TestEventSeverity {
		t.Fatalf("SendTestEvent() severity = %d, want %d", event.Severity, TestEventSeverity)
	}
	if event.Source != EventSourceInternal {
		t.Fatalf("SendTestEvent() source = %q, want internal", event.Source)
	}
	if event.Status != EventStatusReceived || event.PublishState != 0 {
		t.Fatalf("SendTestEvent() status/publishState = %d/%d, want received/pending", event.Status, event.PublishState)
	}
	if event.BizID != ch.BizID {
		t.Fatalf("SendTestEvent() bizID = %q, want channel biz %q", event.BizID, ch.BizID)
	}
	if event.CreateTime.IsZero() || !event.NextPublishTime.Equal(event.CreateTime) {
		t.Fatalf("SendTestEvent() times = %v/%v", event.CreateTime, event.NextPublishTime)
	}
	// 缺省载荷携带测试上下文（test 标记 + 目标实例）。
	if !strings.Contains(string(event.Payload), `"test":true`) || !strings.Contains(string(event.Payload), ch.ID) {
		t.Fatalf("SendTestEvent() payload = %s, want test context with channel id", event.Payload)
	}
	// 落库原语收到同一事件。
	if len(events.inserted) != 1 || events.inserted[0].ID != event.ID {
		t.Fatalf("SendTestEvent() inserted = %+v, want the returned event", events.inserted)
	}
}

func TestChannelUsecase_SendTestEventCustomSpec(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()

	ch, err := uc.Create(ctx, newEmailChannel("email-alert"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	spec := TestEventSpec{
		EventType:  "monitor.alert.test",
		Title:      "自定义测试",
		Recipients: []string{"ops@example.com"},
		Payload:    []byte(`{"order":"A-1"}`),
	}
	event, err := uc.SendTestEvent(ctx, ch.ID, spec)
	if err != nil {
		t.Fatalf("SendTestEvent() error = %v", err)
	}
	if event.EventType != spec.EventType || event.Title != spec.Title {
		t.Fatalf("SendTestEvent() type/title = %q/%q", event.EventType, event.Title)
	}
	if len(event.Recipients) != 1 || event.Recipients[0] != "ops@example.com" {
		t.Fatalf("SendTestEvent() recipients = %v", event.Recipients)
	}
	// 附加载荷与缺省测试上下文合并（proto 契约），test/channel_id 恒由系统填充。
	if string(event.Payload) != `{"channel_id":"`+ch.ID+`","order":"A-1","test":true}` {
		t.Fatalf("SendTestEvent() payload = %s, want merged with test context", event.Payload)
	}
}

// TestChannelUsecase_SendTestEventPayloadGuards 测试载荷约束：须为 JSON 对象、
// 与 NotificationWriter 同一 32KB 上限、系统键不可被覆盖、收件上限。
func TestChannelUsecase_SendTestEventPayloadGuards(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	writer := new(eventWriterStub)
	uc := NewChannelUsecase(stub, writer, stub)
	ctx := context.Background()
	ch, err := uc.Create(ctx, newEmailChannel("email-guard"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	for _, tc := range []struct {
		name string
		spec TestEventSpec
	}{
		{"array payload", TestEventSpec{Payload: []byte(`[1,2]`)}},
		{"scalar payload", TestEventSpec{Payload: []byte(`"x"`)}},
		{"null payload", TestEventSpec{Payload: []byte(`null`)}},
		{"broken json", TestEventSpec{Payload: []byte(`{oops`)}},
		{"oversized payload", TestEventSpec{Payload: []byte(`{"k":"` + strings.Repeat("x", 32*1024) + `"}`)}},
		{"too many recipients", TestEventSpec{Recipients: make([]string, 21)}},
	} {
		if _, err := uc.SendTestEvent(ctx, ch.ID, tc.spec); !errors.Is(err, ErrChannelConfigInvalid) {
			t.Errorf("SendTestEvent(%s) error = %v, want ErrChannelConfigInvalid", tc.name, err)
		}
	}
	if len(writer.inserted) != 0 {
		t.Fatalf("invalid specs must not insert, got %d", len(writer.inserted))
	}

	// 调用方试图覆盖 test/channel_id：以系统值为准。
	event, err := uc.SendTestEvent(ctx, ch.ID, TestEventSpec{Payload: []byte(`{"test":false,"channel_id":"spoof"}`)})
	if err != nil {
		t.Fatalf("SendTestEvent(override attempt) error = %v", err)
	}
	if string(event.Payload) != `{"channel_id":"`+ch.ID+`","test":true}` {
		t.Fatalf("SendTestEvent(override attempt) payload = %s", event.Payload)
	}
}

// TestChannelUsecase_WebhookHeadersRejectCredentials config.headers 明文落库且
// 可读回，凭据类请求头键名一律拒绝（鉴权走 secret/HMAC）；大小写不敏感。
func TestChannelUsecase_WebhookHeadersRejectCredentials(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()
	webhook := func(headers map[string]string) *Channel {
		return &Channel{
			Type: ChannelTypeWebhook, Name: "hook",
			Config: &ChannelConfig{Webhook: &WebhookConfig{
				URL: "https://hooks.example.com/x", Method: "POST", TimeoutS: 3, Headers: headers,
			}},
			Enabled: true,
		}
	}
	for _, key := range []string{"Authorization", "authorization", "X-API-Key", "Cookie", " Proxy-Authorization "} {
		if _, err := uc.Create(ctx, webhook(map[string]string{key: "v"})); !errors.Is(err, ErrChannelConfigInvalid) {
			t.Errorf("Create(header %q) error = %v, want ErrChannelConfigInvalid", key, err)
		}
	}
	created, err := uc.Create(ctx, webhook(map[string]string{"X-Trace-Id": "abc", "Content-Type": "application/json"}))
	if err != nil {
		t.Fatalf("Create(benign headers) error = %v", err)
	}
	// 更新同样经校验。
	if err := uc.Update(ctx, created.ID, ChannelUpdate{Config: webhook(map[string]string{"Authorization": "Bearer x"}).Config}); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Update(credential header) error = %v, want ErrChannelConfigInvalid", err)
	}
}

func TestChannelUsecase_SendTestEventInvalidEventType(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	events := &eventWriterStub{}
	uc := NewChannelUsecase(stub, events, stub)
	ctx := context.Background()

	ch, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// 大写 / 路由模式通配 / 空 exclamation 均拒绝（须为小写点分事件名）。
	for _, typ := range []string{"Monitor.Alert", "monitor.*", "monitor!"} {
		_, err := uc.SendTestEvent(ctx, ch.ID, TestEventSpec{EventType: typ})
		if !errors.Is(err, ErrChannelConfigInvalid) {
			t.Errorf("SendTestEvent(%q) error = %v, want ErrChannelConfigInvalid", typ, err)
		}
	}
	if len(events.inserted) != 0 {
		t.Fatalf("invalid spec must not insert, got %d events", len(events.inserted))
	}
}

func TestChannelUsecase_SendTestEventChannelState(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	events := &eventWriterStub{}
	uc := NewChannelUsecase(stub, events, stub)
	ctx := context.Background()

	// 渠道不存在 → 404。
	if _, err := uc.SendTestEvent(ctx, "missing-id", TestEventSpec{}); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("SendTestEvent(missing) error = %v, want ErrChannelNotFound", err)
	}

	ch, err := uc.Create(ctx, newEmailChannel("email-corp"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// 停用实例 → 409（路由本就跳过，测试无意义）。
	enabled := false
	if err := uc.Update(ctx, ch.ID, ChannelUpdate{Enabled: &enabled}); err != nil {
		t.Fatalf("Update(disable) error = %v", err)
	}
	if _, err := uc.SendTestEvent(ctx, ch.ID, TestEventSpec{}); !errors.Is(err, ErrChannelDisabled) {
		t.Fatalf("SendTestEvent(disabled) error = %v, want ErrChannelDisabled", err)
	}
	if len(events.inserted) != 0 {
		t.Fatalf("disabled channel must not insert, got %d events", len(events.inserted))
	}
}

// ListChannelsByIDs 批量查询桩：逐个委托 GetChannel，缺失 ID 跳过（对齐
// data 层语义）。
func (s *channelRepoStub) ListChannelsByIDs(ctx context.Context, ids []string) ([]*Channel, error) {
	out := make([]*Channel, 0, len(ids))
	for _, id := range ids {
		ch, err := s.GetChannel(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, ch)
	}
	return out, nil
}

// CountRoutesUsingChannel 引用计数桩：按 refs 字段返回。
func (s *channelRepoStub) CountRoutesUsingChannel(_ context.Context, channelID string) (int64, error) {
	return s.refs[channelID], nil
}

// TestChannelUsecase_DeleteReferencedByRoute 仍被未删除路由引用的渠道拒绝删除（409），
// 解绑后可删。
func TestChannelUsecase_DeleteReferencedByRoute(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	stub.refs = map[string]int64{}
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()
	ch, err := uc.Create(ctx, newEmailChannel("email-ref"))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	stub.refs[ch.ID] = 2
	if err := uc.Delete(ctx, ch.ID); !errors.Is(err, ErrChannelInUse) {
		t.Fatalf("Delete(referenced) error = %v, want ErrChannelInUse", err)
	}
	stub.refs[ch.ID] = 0
	if err := uc.Delete(ctx, ch.ID); err != nil {
		t.Fatalf("Delete(unreferenced) error = %v", err)
	}
}

// TestChannelUsecase_SignSecret 签名密钥仅 dingtalk/feishu 可带；出参不回填；
// 更新非法类型拒绝、清除（空串）放行。
func TestChannelUsecase_SignSecret(t *testing.T) {
	t.Parallel()
	stub := newChannelRepoStub()
	uc := NewChannelUsecase(stub, new(eventWriterStub), stub)
	ctx := context.Background()
	ding := &Channel{Type: ChannelTypeDingtalk, Name: "ding", Config: &ChannelConfig{Robot: &RobotConfig{}}, Secret: "tok", SignSecret: "SEC", Enabled: true}
	out, err := uc.Create(ctx, ding)
	if err != nil || out.SignSecret != "" || out.Secret != "" {
		t.Fatalf("Create(dingtalk+sign) = %+v, %v", out, err)
	}
	email := newEmailChannel("mail")
	email.SignSecret = "x"
	if _, err := uc.Create(ctx, email); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Create(email+sign) error = %v, want ErrChannelConfigInvalid", err)
	}
	email.SignSecret = ""
	mail, err := uc.Create(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	sign := "new"
	if err := uc.Update(ctx, mail.ID, ChannelUpdate{SignSecret: &sign}); !errors.Is(err, ErrChannelConfigInvalid) {
		t.Fatalf("Update(email sign) error = %v", err)
	}
	if err := uc.Update(ctx, out.ID, ChannelUpdate{SignSecret: &sign}); err != nil {
		t.Fatalf("Update(dingtalk sign) error = %v", err)
	}
	clear := ""
	if err := uc.Update(ctx, mail.ID, ChannelUpdate{SignSecret: &clear}); err != nil {
		t.Fatalf("Update(clear sign on email) error = %v, want nil (no-op clear allowed)", err)
	}
}
