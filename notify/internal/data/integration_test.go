//go:build integration

package data

import (
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newIntegrationData 构造真实连接池；未配置 TEST_DATABASE_SOURCE 时跳过。护栏与
// core 一致：库名不含 "test" 直接失败（集成测试 TRUNCATE 通知域全部表及 system_user）。
func newIntegrationData(t *testing.T) *Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"（集成测试会 TRUNCATE 业务表，禁止指向开发库）", db)
	}
	d, cleanup, err := NewData(&conf.Data{Database: &conf.Data_Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE notify_channel, notify_template, notify_route, notify_silence, notify_subscription,
		notify_producer, notify_event, notify_dispatch, notify_delivery_log, notify_dlq, notify_dedup, notify_agg_group,
		notify_rate_bucket, notify_quota_counter, notify_channel_health, notify_cfg_version, notify_ingress_replay,
		system_permission, system_grant, system_user_role, "system_user" CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return d
}

const testMasterKey = "contract-master-key"

func testBox(t *testing.T) *SecretBox {
	t.Helper()
	box, err := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: testMasterKey}})
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// 固定 ID（uuid，hex 字符）。
const (
	idChanHook  = "018f3e1a-0b01-7000-8000-000000000001"
	idChanEmail = "018f3e1a-0b01-7000-8000-000000000002"
	idChanOff   = "018f3e1a-0b01-7000-8000-000000000003"
	idRoute     = "018f3e1a-0b02-7000-8000-000000000001"
	idRouteAgg  = "018f3e1a-0b02-7000-8000-000000000002"
	idTpl       = "018f3e1a-0b03-7000-8000-000000000001"
	idSilence   = "018f3e1a-0b04-7000-8000-000000000001"
	idProducer  = "018f3e1a-0b05-7000-8000-000000000001"
	idUser      = "018f3e1a-0b06-7000-8000-000000000001"
	idSub       = "018f3e1a-0b07-7000-8000-000000000001"
)

// seedConfig 预置一套最小配置：webhook 渠道（含 secret）、email 渠道、停用渠道、
// 直发路由（monitor.*）、聚合路由（host.down, 窗口 30s）、模板、生产者、订阅用户。
func seedConfig(t *testing.T, d *Data, webhookURL string) {
	t.Helper()
	ctx := context.Background()
	box := testBox(t)
	sealed, err := box.SealWithNonce("hook-secret", []byte("0123456789ab"))
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := d.Pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	exec(`INSERT INTO notify_channel (id, biz_id, type, name, config, secret, rate_limit, enabled) VALUES
		($1, 'default', 'webhook', 'hook', $2, $3, '{"qps":0,"burst":0}', true),
		($4, 'default', 'email', 'mail', '{"host":"127.0.0.1","port":2525,"username":"","from_addr":"noreply@example.com","starttls":false}', NULL, '{"qps":5,"burst":5}', true),
		($5, 'default', 'email', 'mail-off', '{"host":"x","port":25,"username":"","from_addr":"a@b.c","starttls":false}', NULL, '{"qps":0,"burst":0}', false)`,
		idChanHook, `{"url":"`+webhookURL+`","method":"POST","headers":{"X-Source":"core"},"timeout_s":2}`, sealed, idChanEmail, idChanOff)
	exec(`INSERT INTO notify_route (id, biz_id, event_pattern, severity_min, tag_match, priority, channel_ids, recipient_policy, aggregate_window_s, suppress_threshold, suppress_duration_m, enabled) VALUES
		($1, 'default', 'monitor.*', 1, NULL, 5, $2::uuid[], '{"fixed":["ops@example.com"],"subscription":true}', 0, 500, 30, true),
		($3, 'default', 'host.down', 1, NULL, 5, $4::uuid[], '{"fixed":["x"]}', 30, 2, 30, true)`,
		idRoute, []string{idChanHook, idChanEmail, idChanOff}, idRouteAgg, []string{idChanHook})
	exec(`INSERT INTO notify_template (id, biz_id, event_type, channel_type, subject, body, version, enabled) VALUES
		($1, 'default', '*', 'webhook', '[{{.BizID}}] {{.Title}}', 'sev={{.Severity}} count={{.Count}}', 2, true)`, idTpl)
	exec(`INSERT INTO notify_silence (id, biz_id, event_pattern, severity_max, time_window, enabled) VALUES
		($1, 'default', 'monitor.silent', 4, NULL, true)`, idSilence)
	exec(`INSERT INTO notify_producer (id, name, biz_id, event_whitelist, token_hash, expire_time, enabled) VALUES
		($1, 'engine', 'default', '{}', $2, NULL, true)`, idProducer, []byte("hash"))
	exec(`INSERT INTO "system_user" (id, username, password, nickname, email, status) VALUES ($1, 'alice', 'x', 'Alice', 'alice@example.com', 1)`, idUser)
	exec(`INSERT INTO notify_subscription (id, user_id, biz_id, event_pattern, channel_prefs, enabled) VALUES
		($1, $2, 'default', 'monitor.*', '{"email":true}', true)`, idSub, idUser)
	exec(`INSERT INTO notify_cfg_version (table_name, version) VALUES ('notify_channel', 3), ('notify_route', 1)`)
}

// seedEvent 直插一条 pending 事件（模拟 core NotificationWriter / ingress 落库）。
func seedEvent(t *testing.T, d *Data, id, eventType string, severity int32, groupKey string, recipients any) {
	t.Helper()
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO notify_event (id, biz_id, event_type, severity, title, payload, dedup_key, group_key, recipients, source, status, publish_state, publish_attempts, next_publish_time, create_time)
		 VALUES ($1, 'default', $2, $3, $4, '{"host":"web-1"}', NULL, $5, $6, 'internal', 1, 0, 0, now(), now())`,
		id, eventType, severity, "title-"+id, dbx.NilIfEmpty(groupKey), recipients); err != nil {
		t.Fatalf("seed event %s: %v", id, err)
	}
}

func newCache(t *testing.T, d *Data) *biz.ConfigCache {
	t.Helper()
	cache := biz.NewConfigCache(NewConfigRepo(d, testBox(t)), NewRateBucketRepo(d), testLogger())
	if err := cache.Load(context.Background()); err != nil {
		t.Fatalf("cache.Load: %v", err)
	}
	return cache
}

func count(t *testing.T, d *Data, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := d.Pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// TestIntegration_SecretBoxContract 用 core 侧真实代码生成的密文验证解密契约
// （core: NewSecretBox(key).Seal("smtp-pass-☃")，见 crypto.go）。
func TestIntegration_SecretBoxContract(t *testing.T) {
	t.Parallel()
	sealed, err := hex.DecodeString("ddfb5e98ad8fa78bb60f6d785e2986a3d7e480a0b016f82e6f5b450b7c4669ca8a337dc7626ed3aed2")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := testBox(t).Open(sealed)
	if err != nil || plain != "smtp-pass-☃" {
		t.Fatalf("Open(core ciphertext) = %q, %v", plain, err)
	}
	if _, err := (&SecretBox{}).Open(sealed); err == nil {
		t.Fatal("missing master key must fail")
	}
	if _, err := testBox(t).Open(sealed[:10]); err == nil {
		t.Fatal("truncated ciphertext must fail")
	}
}

func TestIntegration_ConfigSnapshot(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	repo := NewConfigRepo(d, testBox(t))
	ctx := context.Background()
	versions, err := repo.LoadVersions(ctx)
	if err != nil || versions["notify_channel"] != 3 || versions["notify_route"] != 1 {
		t.Fatalf("LoadVersions() = %v, %v", versions, err)
	}
	snap, err := repo.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	hook := snap.Channel(idChanHook)
	if hook == nil || hook.Webhook == nil || hook.Webhook.Headers["X-Source"] != "core" || hook.Secret != "hook-secret" || hook.SecretErr != nil {
		t.Fatalf("webhook channel = %+v", hook)
	}
	mail := snap.Channel(idChanEmail)
	if mail == nil || mail.Email == nil || mail.Email.FromAddr != "noreply@example.com" || mail.RateLimit.QPS != 5 || mail.Secret != "" {
		t.Fatalf("email channel = %+v", mail)
	}
	if off := snap.Channel(idChanOff); off == nil || off.Enabled {
		t.Fatalf("disabled channel must be present but disabled: %+v", off)
	}
	r := snap.MatchRoute("default", "monitor.alert.firing", 2, nil)
	if r == nil || r.ID != idRoute || len(r.ChannelIDs) != 3 || !r.Recipient.Subscription || r.Recipient.Fixed[0] != "ops@example.com" {
		t.Fatalf("route = %+v", r)
	}
	if agg := snap.MatchRoute("default", "host.down", 1, nil); agg == nil || agg.AggregateWindowS != 30 || agg.SuppressThreshold != 2 {
		t.Fatalf("agg route = %+v", agg)
	}
	if tpl := snap.Template("default", "monitor.alert.firing", "webhook"); tpl == nil || tpl.Version != 2 {
		t.Fatalf("template = %+v", tpl)
	}
	if !snap.Silenced("default", "monitor.silent", 3, time.Now()) {
		t.Fatal("silence not loaded")
	}
	if p := snap.Producers[idProducer]; p == nil || string(p.TokenHash) != "hash" || !p.Enabled {
		t.Fatalf("producer = %+v", p)
	}
	// 生产者密钥封存形态（core 写入）：用主密钥解封为 HMAC 密钥；主密钥不一致 → 仅该
	// 生产者不可鉴权（TokenHash 置空 + KeyErr）。
	sealedKey, err := testBox(t).Seal("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_producer SET token_hash = $2 WHERE id = $1", idProducer, notifycontract.WrapSealedProducerKey(sealedKey)); err != nil {
		t.Fatal(err)
	}
	sealedSnap, err := repo.LoadSnapshot(ctx)
	if err != nil || string(sealedSnap.Producers[idProducer].TokenHash) != "0123456789abcdef0123456789abcdef" || sealedSnap.Producers[idProducer].KeyErr != nil {
		t.Fatalf("sealed producer key: err=%v producer=%+v", err, sealedSnap.Producers[idProducer])
	}
	// 主密钥错误：secret 渠道标记不健康，其余正常（快照不整体失败）。
	badBox, _ := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: "wrong"}})
	snap2, err := NewConfigRepo(d, badBox).LoadSnapshot(ctx)
	if err != nil || snap2.Channel(idChanHook).SecretErr == nil || snap2.Channel(idChanEmail).SecretErr != nil {
		t.Fatalf("wrong key snapshot: err=%v hook=%+v", err, snap2.Channel(idChanHook))
	}
	if p := snap2.Producers[idProducer]; p == nil || p.KeyErr == nil || len(p.TokenHash) != 0 {
		t.Fatalf("wrong key: sealed producer must be unusable, got %+v", p)
	}
	// 令牌桶同步：qps=5 的 email 有桶，qps=0 无桶。
	if err := NewRateBucketRepo(d).SyncBuckets(ctx, []*biz.Channel{hook, mail}); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_rate_bucket") != 1 {
		t.Fatal("expected exactly one bucket row")
	}
}

// TestIntegration_SnapshotTenancyColumns 快照读取 0023 新列：生产者配额、渠道签名密钥（解密）。
func TestIntegration_SnapshotTenancyColumns(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	box := testBox(t)
	sign, _ := box.SealWithNonce("SIGN-SECRET", []byte("abcdefghijkl"))
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_producer SET qps_limit = 7, daily_limit = 700 WHERE id = $1", idProducer); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, `INSERT INTO notify_channel (id, biz_id, type, name, config, secret, sign_secret, rate_limit, enabled)
		VALUES ('018f3e1a-0b01-7000-8000-000000000009', 'default', 'feishu', 'fs', '{"at_mobiles":[]}', $1, $2, '{"qps":0,"burst":0}', true)`,
		mustSeal(t, box, "fkey"), sign); err != nil {
		t.Fatal(err)
	}
	snap, err := NewConfigRepo(d, box).LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p := snap.Producers[idProducer]; p == nil || p.QPSLimit != 7 || p.DailyLimit != 700 {
		t.Fatalf("producer quota = %+v", p)
	}
	fs := snap.Channel("018f3e1a-0b01-7000-8000-000000000009")
	if fs == nil || fs.Secret != "fkey" || fs.SignSecret != "SIGN-SECRET" || fs.SecretErr != nil {
		t.Fatalf("feishu channel = %+v", fs)
	}
}

func mustSeal(t *testing.T, box *SecretBox, plain string) []byte {
	t.Helper()
	out, err := box.SealWithNonce(plain, []byte("0123456789ab"))
	if err != nil {
		t.Fatal(err)
	}
	return out
}
