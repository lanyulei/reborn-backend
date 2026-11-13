//go:build integration

package data

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// TestIntegration_NotificationDedupGate core 直插去重门闩（真实 PG）：同 (biz, dedup_key)
// 二次直插返回首次事件且不落第二行；门闩随事务回滚释放；过期后重新接纳。
func TestIntegration_NotificationDedupGate(t *testing.T) {
	d := newIntegrationNotifyData(t)
	manager := basedata.NewTxManager(d)
	notifyUC := biznotify.NewNotificationUsecase(NewEventRepo(d), nil)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, "TRUNCATE TABLE notify_dedup"); err != nil {
		t.Fatal(err)
	}
	send := func() (*biznotify.Event, error) {
		var out *biznotify.Event
		err := manager.WithinTx(ctx, func(tx txrun.Tx) error {
			ev, err := notifyUC.NotifyEvent(ctx, tx, biznotify.Event{BizID: "core", EventType: "user.status", Title: "t", DedupKey: "user-42"})
			out = ev
			return err
		})
		return out, err
	}
	first, err := send()
	if err != nil || first.Deduplicated {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := send()
	if err != nil || !second.Deduplicated || second.ID != first.ID {
		t.Fatalf("second = %+v, %v; want dedup to %s", second, err, first.ID)
	}
	var events, gates int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM notify_event WHERE event_type = 'user.status'").Scan(&events)
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM notify_dedup").Scan(&gates)
	if events != 1 || gates != 1 {
		t.Fatalf("events=%d gates=%d, want 1/1", events, gates)
	}
	// 回滚释放：新键事务失败 → 门闩无该键。
	_ = manager.WithinTx(ctx, func(tx txrun.Tx) error {
		if _, err := notifyUC.NotifyEvent(ctx, tx, biznotify.Event{BizID: "core", EventType: "user.status", Title: "t", DedupKey: "rollback"}); err != nil {
			return err
		}
		return context.Canceled
	})
	var rolled int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM notify_dedup WHERE dedup_key = 'rollback'").Scan(&rolled)
	if rolled != 0 {
		t.Fatal("gate row must be released on rollback")
	}
	// 过期后重新接纳并原地替换。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dedup SET expire_time = now() - interval '1 second'")
	third, err := send()
	if err != nil || third.Deduplicated || third.ID == first.ID {
		t.Fatalf("after expiry = %+v, %v", third, err)
	}
}

// TestIntegration_TenancyColumns 0023 新列往返：角色 biz_scope、生产者配额、渠道 sign_secret
// （密文落库、读列集不含、清除落 NULL）。
func TestIntegration_TenancyColumns(t *testing.T) {
	d := newIntegrationNotifyData(t)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE system_permission, system_grant, system_user_role, system_role`); err != nil {
		t.Fatal(err)
	}
	roles := datasystem.NewRoleRepo(d)
	role := &bizsystem.Role{
		ID: "018f3e1a-2301-7000-8000-000000000001", Name: "监控管理员", Code: "mon_admin", Status: 1,
		BizScope: []string{"monitor", "ticket"}, CreateTime: time.Now().UTC(), UpdateTime: time.Now().UTC(),
	}
	if err := roles.CreateRole(ctx, role); err != nil {
		t.Fatal(err)
	}
	got, err := roles.GetRole(ctx, role.ID)
	if err != nil || !slices.Equal(got.BizScope, []string{"monitor", "ticket"}) {
		t.Fatalf("role scope = %v, %v", got, err)
	}
	empty := []string{}
	if err := roles.UpdateRole(ctx, role.ID, bizsystem.RoleUpdate{BizScope: &empty}); err != nil {
		t.Fatal(err)
	}
	got, _ = roles.GetRole(ctx, role.ID)
	if len(got.BizScope) != 0 {
		t.Fatalf("cleared scope = %v, want empty (unrestricted)", got.BizScope)
	}
	legacy := &bizsystem.Role{ID: "018f3e1a-2301-7000-8000-000000000002", Name: "旧角色", Code: "legacy", Status: 1, CreateTime: time.Now().UTC(), UpdateTime: time.Now().UTC()}
	if err := roles.CreateRole(ctx, legacy); err != nil {
		t.Fatalf("nil scope must store as empty array, not NULL: %v", err)
	}

	producers := NewProducerRepo(d, nil)
	p := newProducerDO("018f3e1a-2302-7000-8000-000000000001", "quota-engine")
	p.QPSLimit, p.DailyLimit = 20, 50000
	if err := producers.CreateProducer(ctx, p); err != nil {
		t.Fatal(err)
	}
	gp, err := producers.GetProducer(ctx, p.ID)
	if err != nil || gp.QPSLimit != 20 || gp.DailyLimit != 50000 {
		t.Fatalf("producer quota = %+v, %v", gp, err)
	}
	if err := producers.UpdateProducer(ctx, p.ID, biznotify.ProducerUpdate{Spec: &biznotify.ProducerSpec{Name: "quota-engine", QPSLimit: 5}}); err != nil {
		t.Fatal(err)
	}
	gp, _ = producers.GetProducer(ctx, p.ID)
	if gp.QPSLimit != 5 || gp.DailyLimit != 0 {
		t.Fatalf("producer quota after spec replace = %+v", gp)
	}

	box := newTestSecretBox(t)
	channels := NewChannelRepo(d, box)
	ding := &biznotify.Channel{
		ID: "018f3e1a-2303-7000-8000-000000000001", BizID: "default", Type: biznotify.ChannelTypeDingtalk, Name: "ding-sign",
		Config: &biznotify.ChannelConfig{Robot: &biznotify.RobotConfig{}}, Secret: "token", SignSecret: "SEC123", Enabled: true,
		CreateTime: time.Now().UTC(), UpdateTime: time.Now().UTC(),
	}
	if err := channels.CreateChannel(ctx, ding); err != nil {
		t.Fatal(err)
	}
	var sealed []byte
	_ = d.Pool.QueryRow(ctx, "SELECT sign_secret FROM notify_channel WHERE id = $1", ding.ID).Scan(&sealed)
	if plain := openForTest(t, sealed); plain != "SEC123" {
		t.Fatalf("sign_secret stored = %q", plain)
	}
	gc, _ := channels.GetChannel(ctx, ding.ID)
	if gc.SignSecret != "" || gc.Secret != "" {
		t.Fatal("secrets must never be read back")
	}
	clear := ""
	if err := channels.UpdateChannel(ctx, ding.ID, biznotify.ChannelUpdate{SignSecret: &clear}); err != nil {
		t.Fatal(err)
	}
	var cleared []byte
	_ = d.Pool.QueryRow(ctx, "SELECT sign_secret FROM notify_channel WHERE id = $1", ding.ID).Scan(&cleared)
	if cleared != nil {
		t.Fatal("empty sign_secret must clear the column to NULL")
	}
}

// testMasterKey 与 channel_integration_test.go 的 newTestSecretBox 同一主密钥。
const testMasterKey = "integration-test-master-key"

// openForTest 测试侧解密（core 生产代码刻意不提供 Open——secret 只写不读）：
// 与 SecretBox 同一布局 nonce(12)||ct、key = sha256(主密钥)。
func openForTest(t *testing.T, sealed []byte) string {
	t.Helper()
	key := sha256.Sum256([]byte(testMasterKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], nil)
	if err != nil {
		t.Fatalf("open sealed secret: %v", err)
	}
	return string(plain)
}
