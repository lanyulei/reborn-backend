//go:build integration

package data

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// missingChannelID 是不存在于库中的合法 UUID。
const missingChannelID = "018f3e1a-ffff-7000-8000-000000000001"

// newIntegrationNotifyData 构造真实连接池并截断 notify 八表；护栏与 system 域
// 相同：库名不含 "test" 直接失败（集成测试 TRUNCATE 业务表，禁止指向开发库）。
func newIntegrationNotifyData(t *testing.T) *basedata.Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	if cfg, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	} else if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"（集成测试会 TRUNCATE 业务表，禁止指向开发/生产库；请用 reborn_test）", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &conf.Data_Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := d.Pool.Exec(ctx, "TRUNCATE TABLE notify_channel, notify_template, notify_route, notify_silence, notify_subscription, notify_producer, notify_event, notify_dispatch, notify_delivery_log, notify_dlq, notify_cfg_version"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return d
}

// newTestSecretBox 返回固定主密钥的加密器（仅测试脚手架，非真实凭据）。
func newTestSecretBox(t *testing.T) *SecretBox {
	t.Helper()
	box, err := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: "integration-test-master-key"}})
	if err != nil {
		t.Fatalf("NewSecretBox() error = %v", err)
	}
	return box
}

// newChannelDO 返回带固定 ID 与审计时间的渠道 DO。
func newChannelDO(id, name string) *biz.Channel {
	return &biz.Channel{
		ID:         id,
		BizID:      biz.DefaultBizID,
		Type:       biz.ChannelTypeEmail,
		Name:       name,
		Config:     &biz.ChannelConfig{Email: &biz.EmailConfig{Host: "smtp.example.com", Port: 465, Username: "ops", FromAddr: "ops@example.com", StartTLS: true}},
		Secret:     "smtp-password",
		RateLimit:  biz.RateLimit{QPS: 5, Burst: 10},
		Enabled:    true,
		CreateTime: time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime: time.Now().UTC().Truncate(time.Microsecond),
	}
}

// rawSecret 原生 SQL 读回密文（bytea）——仓储自身永不查询该列，测试用直查
// 验证「不传=保留旧密文 / 传新值=轮换」的字节级行为。
func rawSecret(t *testing.T, d *basedata.Data, id string) []byte {
	t.Helper()
	var secret []byte
	if err := d.Pool.QueryRow(context.Background(),
		"SELECT secret FROM notify_channel WHERE id = $1", id).Scan(&secret); err != nil {
		t.Fatalf("read raw secret: %v", err)
	}
	return secret
}

// cfgVersion 读回当前配置版本号。
func cfgVersion(t *testing.T, d *basedata.Data) int64 {
	t.Helper()
	var version int64
	if err := d.Pool.QueryRow(context.Background(),
		"SELECT version FROM notify_cfg_version WHERE table_name = 'notify_channel'").Scan(&version); err != nil {
		t.Fatalf("read cfg version: %v", err)
	}
	return version
}

func TestIntegration_SecretBox(t *testing.T) {
	t.Parallel()

	// 未配置主密钥：构造成功（不阻断启动），Seal 快速失败。
	bare, err := NewSecretBox(&conf.Notify{})
	if err != nil {
		t.Fatalf("NewSecretBox(no key) error = %v", err)
	}
	if _, err := bare.Seal("x"); !errors.Is(err, biz.ErrChannelSecretUnavailable) {
		t.Fatalf("Seal(no key) error = %v, want ErrChannelSecretUnavailable", err)
	}

	// 正常加密：随机 nonce 两次密文不同；布局 = nonce(12) + 明文 + GCM tag(16)。
	box, err := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: "k1"}})
	if err != nil {
		t.Fatalf("NewSecretBox() error = %v", err)
	}
	plaintext := "smtp-password"
	ct1, err := box.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	ct2, _ := box.Seal(plaintext)
	if string(ct1) == string(ct2) {
		t.Fatal("Seal() ciphertexts identical, nonce reuse suspected")
	}
	// 独立重建 AES-256-GCM（key = sha256(主密钥)）验证派生一致性与 nonce||ct 布局。
	key := sha256.Sum256([]byte("k1"))
	block, _ := aes.NewCipher(key[:])
	aead, _ := cipher.NewGCM(block)
	if len(ct1) != aead.NonceSize()+len(plaintext)+16 {
		t.Fatalf("Seal() length = %d, want nonce+plaintext+tag", len(ct1))
	}
	nonce, body := ct1[:aead.NonceSize()], ct1[aead.NonceSize():]
	got, err := aead.Open(nil, nonce, body, nil)
	if err != nil {
		t.Fatalf("AES-GCM open: %v", err)
	}
	if string(got) != plaintext {
		t.Fatalf("open = %q, want %q", got, plaintext)
	}

	// 同主密钥的另一实例（跨进程场景）密文亦可被同一派生密钥解开。
	peer, err := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: "k1"}})
	if err != nil {
		t.Fatalf("NewSecretBox(peer) error = %v", err)
	}
	ct3, err := peer.Seal(plaintext)
	if err != nil {
		t.Fatalf("peer.Seal() error = %v", err)
	}
	if got, err := aead.Open(nil, ct3[:aead.NonceSize()], ct3[aead.NonceSize():], nil); err != nil || string(got) != plaintext {
		t.Fatalf("peer ciphertext open = %q, %v", got, err)
	}
}

func TestIntegration_ChannelCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	box := newTestSecretBox(t)
	repo := NewChannelRepo(d, box)
	ctx := context.Background()

	email := newChannelDO("018f3e1a-0101-7000-8000-000000000001", "email-corp")
	if err := repo.CreateChannel(ctx, email); err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	seedVersion := cfgVersion(t, d) // 种子行缺失亦自举
	if seedVersion < 1 {
		t.Fatalf("cfg version = %d, want >= 1", seedVersion)
	}

	// 重名冲突（同 biz 未删除行）映射哨兵错误。
	dup := newChannelDO("018f3e1a-0101-7000-8000-000000000002", "email-corp")
	if err := repo.CreateChannel(ctx, dup); !errors.Is(err, biz.ErrChannelNameExists) {
		t.Fatalf("CreateChannel(dup) error = %v, want ErrChannelNameExists", err)
	}
	// 不同 biz 同名放行（biz 隔离）。
	other := newChannelDO("018f3e1a-0101-7000-8000-000000000003", "email-corp")
	other.BizID = "monitor"
	if err := repo.CreateChannel(ctx, other); err != nil {
		t.Fatalf("CreateChannel(other biz) error = %v", err)
	}

	// webhook 无 secret 落 NULL；类型化配置（headers/timeout）往返。
	webhook := &biz.Channel{
		ID:         "018f3e1a-0101-7000-8000-000000000004",
		BizID:      biz.DefaultBizID,
		Type:       biz.ChannelTypeWebhook,
		Name:       "webhook-audit",
		Config:     &biz.ChannelConfig{Webhook: &biz.WebhookConfig{URL: "https://audit.example.com/hook", Method: "POST", Headers: map[string]string{"X-Source": "core"}, TimeoutS: 3}},
		Enabled:    true,
		CreateTime: time.Now().UTC(),
		UpdateTime: time.Now().UTC(),
	}
	if err := repo.CreateChannel(ctx, webhook); err != nil {
		t.Fatalf("CreateChannel(webhook) error = %v", err)
	}
	if raw := rawSecret(t, d, webhook.ID); raw != nil {
		t.Fatalf("webhook without secret stored %d bytes, want NULL", len(raw))
	}
	gotWebhook, err := repo.GetChannel(ctx, webhook.ID)
	if err != nil {
		t.Fatalf("GetChannel(webhook) error = %v", err)
	}
	if gotWebhook.Config.Webhook.Headers["X-Source"] != "core" || gotWebhook.Config.Webhook.TimeoutS != 3 {
		t.Fatalf("GetChannel(webhook) config = %+v", gotWebhook.Config.Webhook)
	}

	// 读路径回填标量字段且不携带 secret。
	got, err := repo.GetChannel(ctx, email.ID)
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	if got.BizID != biz.DefaultBizID || got.Type != biz.ChannelTypeEmail || !got.Enabled {
		t.Fatalf("GetChannel() = %+v", got)
	}
	if got.Config.Email.Host != "smtp.example.com" || !got.Config.Email.StartTLS {
		t.Fatalf("GetChannel() config = %+v", got.Config.Email)
	}
	if got.RateLimit.QPS != 5 || got.RateLimit.Burst != 10 {
		t.Fatalf("GetChannel() rateLimit = %+v", got.RateLimit)
	}
	if got.Secret != "" {
		t.Fatal("GetChannel() must not carry secret")
	}
	if _, err := repo.GetChannel(ctx, missingChannelID); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("GetChannel(missing) error = %v, want ErrChannelNotFound", err)
	}

	// 密文落库且非明文。
	sealed := rawSecret(t, d, email.ID)
	if len(sealed) == 0 || strings.Contains(string(sealed), "smtp-password") {
		t.Fatalf("stored secret = %q, want AES-256-GCM ciphertext", sealed)
	}

	// 更新：改名 + 停用 + 限流；secret 不传 → 密文字节不变。
	name, enabled := "email-corp-2", false
	vBefore := cfgVersion(t, d)
	if err := repo.UpdateChannel(ctx, email.ID, biz.ChannelUpdate{
		Name:      &name,
		Enabled:   &enabled,
		RateLimit: &biz.RateLimit{QPS: 1, Burst: 2},
	}); err != nil {
		t.Fatalf("UpdateChannel() error = %v", err)
	}
	if string(rawSecret(t, d, email.ID)) != string(sealed) {
		t.Fatal("UpdateChannel(without secret) changed ciphertext, want preserved")
	}
	if vAfter := cfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after update = %d, want %d (same-tx bump)", vAfter, vBefore+1)
	}
	got, err = repo.GetChannel(ctx, email.ID)
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	if got.Name != name || got.Enabled || got.RateLimit.QPS != 1 {
		t.Fatalf("UpdateChannel() result = %+v", got)
	}

	// secret 轮换：密文变化且仍是密文。
	rotated := "new-smtp-password"
	if err := repo.UpdateChannel(ctx, email.ID, biz.ChannelUpdate{Secret: &rotated}); err != nil {
		t.Fatalf("UpdateChannel(rotate secret) error = %v", err)
	}
	sealed2 := rawSecret(t, d, email.ID)
	if string(sealed2) == string(sealed) || strings.Contains(string(sealed2), rotated) {
		t.Fatal("secret rotation did not re-encrypt ciphertext")
	}

	// 更新不存在渠道。
	if err := repo.UpdateChannel(ctx, missingChannelID, biz.ChannelUpdate{Name: &name}); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("UpdateChannel(missing) error = %v, want ErrChannelNotFound", err)
	}

	// 列表：keyword（name/type 模糊）/type/enabled 过滤与分页。
	robot := &biz.Channel{
		ID:         "018f3e1a-0101-7000-8000-000000000005",
		BizID:      biz.DefaultBizID,
		Type:       biz.ChannelTypeDingtalk,
		Name:       "dingtalk-ops",
		Config:     &biz.ChannelConfig{Robot: &biz.RobotConfig{AtMobiles: []string{"13800000000"}}},
		Secret:     "robot-token",
		Enabled:    true,
		CreateTime: time.Now().UTC(),
		UpdateTime: time.Now().UTC(),
	}
	if err := repo.CreateChannel(ctx, robot); err != nil {
		t.Fatalf("CreateChannel(robot) error = %v", err)
	}

	items, total, err := repo.ListChannels(ctx, biz.ChannelFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	// 全量 = default biz 3 行 + monitor biz 1 行（v1 列表不做 biz 过滤，
	// 跨 biz 管理口径；biz 隔离体现在唯一约束与创建）。
	if total != 4 || len(items) != 4 {
		t.Fatalf("ListChannels() = %d items, total %d, want 4/4", len(items), total)
	}
	typ := biz.ChannelTypeEmail
	items, total, err = repo.ListChannels(ctx, biz.ChannelFilter{Type: &typ}, 1, 20)
	if err != nil {
		t.Fatalf("ListChannels(type) error = %v", err)
	}
	// default 与 monitor 两个 biz 各一封邮件渠道（列表不做 biz 过滤）。
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListChannels(type) = %d items, total %d, want 2/2", len(items), total)
	}
	for _, item := range items {
		if item.Type != biz.ChannelTypeEmail {
			t.Fatalf("ListChannels(type) leaked %q row", item.Type)
		}
	}
	items, total, err = repo.ListChannels(ctx, biz.ChannelFilter{Keyword: "dingtalk"}, 1, 20)
	if err != nil {
		t.Fatalf("ListChannels(keyword) error = %v", err)
	}
	if total != 1 || items[0].Type != biz.ChannelTypeDingtalk {
		t.Fatalf("ListChannels(keyword) = %d %+v", total, items)
	}
	items, total, err = repo.ListChannels(ctx, biz.ChannelFilter{}, 2, 3)
	if err != nil {
		t.Fatalf("ListChannels(paged) error = %v", err)
	}
	if total != 4 || len(items) != 1 {
		t.Fatalf("ListChannels(paged) = %d items, total %d, want 1/4", len(items), total)
	}

	// 软删：读不可见、名称可复用、版本继续 bump。
	vBefore = cfgVersion(t, d)
	if err := repo.DeleteChannel(ctx, email.ID); err != nil {
		t.Fatalf("DeleteChannel() error = %v", err)
	}
	if vAfter := cfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after delete = %d, want %d", vAfter, vBefore+1)
	}
	if _, err := repo.GetChannel(ctx, email.ID); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("GetChannel(deleted) error = %v, want ErrChannelNotFound", err)
	}
	if err := repo.DeleteChannel(ctx, email.ID); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("DeleteChannel(twice) error = %v, want ErrChannelNotFound", err)
	}
	reuse := newChannelDO("018f3e1a-0101-7000-8000-000000000006", "email-corp-2")
	if err := repo.CreateChannel(ctx, reuse); err != nil {
		t.Fatalf("CreateChannel(reuse name) error = %v", err)
	}
}

// TestIntegration_SendTestEvent 全链路（usecase + 渠道/事件仓储 + 真实 PG）：
// 核心对 notify_event 的唯一写路径——测试事件直插（§5.3 仅 INSERT）。断言
// 落库行可被 ListEvents 读回、可空列 NULL 语义、停用拒绝与运行表不 bump。
func TestIntegration_SendTestEvent(t *testing.T) {
	d := newIntegrationNotifyData(t)
	box := newTestSecretBox(t)
	channelRepo := NewChannelRepo(d, box)
	eventRepo := NewEventRepo(d)
	uc := biz.NewChannelUsecase(channelRepo, eventRepo, NewRouteRepo(d))
	ctx := context.Background()

	ch, err := uc.Create(ctx, &biz.Channel{
		Type:    biz.ChannelTypeEmail,
		Name:    "email-test",
		Config:  newChannelDO("", "").Config,
		Secret:  "smtp-password",
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// 缺省 spec：notify.test / internal / received-pending；dedup/group/
	// recipients 落 NULL（交路由解析），payload 落缺省测试上下文。
	event, err := uc.SendTestEvent(ctx, ch.ID, biz.TestEventSpec{})
	if err != nil {
		t.Fatalf("SendTestEvent() error = %v", err)
	}
	if event.EventType != biz.TestEventTypeDefault || event.Source != biz.EventSourceInternal ||
		event.Status != biz.EventStatusReceived || event.PublishState != 0 {
		t.Fatalf("SendTestEvent() event = %+v, want defaults", event)
	}

	// 落库行经查询路径读回（与管道 relay 所见同一张表）。
	items, total, err := eventRepo.ListEvents(ctx, biz.EventFilter{EventType: biz.TestEventTypeDefault}, 1, 10)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != event.ID {
		t.Fatalf("ListEvents() = %d items, want inserted event %s", total, event.ID)
	}
	got := items[0]
	if got.BizID != ch.BizID || got.Title != biz.TestEventTitleDefault || got.Severity != biz.TestEventSeverity {
		t.Fatalf("ListEvents() row = %+v, want channel biz/default title/severity", got)
	}
	if len(got.Recipients) != 0 || got.DedupKey != "" || got.GroupKey != "" {
		t.Fatalf("ListEvents() nullable cols = (%v, %q, %q), want empty", got.Recipients, got.DedupKey, got.GroupKey)
	}

	// 原生 SQL 断言可空列真实落 NULL（而非空串/空数组）。
	var dedupNull, groupNull, recipientsNull bool
	var payload []byte
	if err := d.Pool.QueryRow(ctx,
		"SELECT dedup_key IS NULL, group_key IS NULL, recipients IS NULL, payload FROM notify_event WHERE id = $1",
		event.ID).Scan(&dedupNull, &groupNull, &recipientsNull, &payload); err != nil {
		t.Fatalf("raw row: %v", err)
	}
	if !dedupNull || !groupNull || !recipientsNull {
		t.Fatalf("nullable cols NULL = (%v, %v, %v), want all true", dedupNull, groupNull, recipientsNull)
	}
	if payload == nil {
		t.Fatal("payload = NULL, want default test context")
	}

	// 自定义 spec：显式收件 jsonb 往返。
	_, err = uc.SendTestEvent(ctx, ch.ID, biz.TestEventSpec{
		EventType:  "monitor.alert.test",
		Title:      "自定义测试",
		Recipients: []string{"ops@example.com"},
		Payload:    []byte(`{"order":"A-1"}`),
	})
	if err != nil {
		t.Fatalf("SendTestEvent(custom) error = %v", err)
	}
	items, _, err = eventRepo.ListEvents(ctx, biz.EventFilter{EventType: "monitor.alert.test"}, 1, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("ListEvents(custom) = (%d items, %v)", len(items), err)
	}
	if len(items[0].Recipients) != 1 || items[0].Recipients[0] != "ops@example.com" {
		t.Fatalf("ListEvents(custom) recipients = %v, want jsonb roundtrip", items[0].Recipients)
	}

	// 停用实例拒绝（路由本就跳过，测试无意义）。
	enabled := false
	if err := uc.Update(ctx, ch.ID, biz.ChannelUpdate{Enabled: &enabled}); err != nil {
		t.Fatalf("Update(disable) error = %v", err)
	}
	if _, err := uc.SendTestEvent(ctx, ch.ID, biz.TestEventSpec{}); !errors.Is(err, biz.ErrChannelDisabled) {
		t.Fatalf("SendTestEvent(disabled) error = %v, want ErrChannelDisabled", err)
	}
	// 不存在渠道。
	if _, err := uc.SendTestEvent(ctx, "018f3e1a-ffff-7000-8000-0000000000f1", biz.TestEventSpec{}); !errors.Is(err, biz.ErrChannelNotFound) {
		t.Fatalf("SendTestEvent(missing) error = %v, want ErrChannelNotFound", err)
	}

	// 运行表写入不 bump 配置版本：notify_event 无 cfg_version 行。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_event'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_event rows, want 0（运行表不 bump）", n)
	}
}

// TestIntegration_ListChannelsByIDs 批量按 ID 查询：仅返回存在且未删除的渠道，
// 缺失/已删 ID 静默跳过，空输入返回空集。
func TestIntegration_ListChannelsByIDs(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewChannelRepo(d, newTestSecretBox(t))
	ctx := context.Background()

	a := newChannelDO("018f3e1a-0109-7000-8000-000000000001", "batch-a")
	b := newChannelDO("018f3e1a-0109-7000-8000-000000000002", "batch-b")
	for _, ch := range []*biz.Channel{a, b} {
		if err := repo.CreateChannel(ctx, ch); err != nil {
			t.Fatalf("CreateChannel(%s): %v", ch.Name, err)
		}
	}
	if err := repo.DeleteChannel(ctx, b.ID); err != nil {
		t.Fatalf("DeleteChannel(b): %v", err)
	}
	got, err := repo.ListChannelsByIDs(ctx, []string{a.ID, b.ID, "018f3e1a-0109-7000-8000-0000000000ff"})
	if err != nil {
		t.Fatalf("ListChannelsByIDs() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != a.ID || got[0].Secret != "" {
		t.Fatalf("ListChannelsByIDs() = %+v, want only channel a without secret", got)
	}
	if empty, err := repo.ListChannelsByIDs(ctx, nil); err != nil || len(empty) != 0 {
		t.Fatalf("ListChannelsByIDs(nil) = %v, %v; want empty", empty, err)
	}
}
