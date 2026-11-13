//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/go-kratos/kratos/v3/errors"
)

// newProducerDO 返回带固定 ID 与审计时间的生产者 DO（token 哈希占位，
// 轮换断言用真实 SHA-256 比对）。
func newProducerDO(id, name string) *biz.Producer {
	return &biz.Producer{
		ID: id,
		ProducerSpec: biz.ProducerSpec{
			Name:           name,
			EventWhitelist: []string{"monitor.alert.*", "monitor.host.down"},
		},
		TokenHash:  []byte("placeholder-hash-32-bytes-xxxxxxxx"),
		Enabled:    true,
		CreateTime: time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime: time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestIntegration_ProducerCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewProducerRepo(d, nil) // 未配置主密钥：存量形态（裸哈希）往返
	ctx := context.Background()

	// 新建：text[] 白名单与 bytea 哈希往返；expire_time 缺省 NULL。
	monitor := newProducerDO("018f3e1a-0304-7000-8000-0000000000c1", "monitor-engine")
	if err := repo.CreateProducer(ctx, monitor); err != nil {
		t.Fatalf("CreateProducer() error = %v", err)
	}
	got, err := repo.GetProducer(ctx, monitor.ID)
	if err != nil {
		t.Fatalf("GetProducer() error = %v", err)
	}
	if len(got.EventWhitelist) != 2 || got.EventWhitelist[0] != "monitor.alert.*" ||
		got.EventWhitelist[1] != "monitor.host.down" {
		t.Fatalf("GetProducer() whitelist = %v", got.EventWhitelist)
	}
	if string(got.TokenHash) != string(monitor.TokenHash) {
		t.Fatalf("GetProducer() tokenHash = %v, want roundtrip", got.TokenHash)
	}
	if got.ExpireTime != nil {
		t.Fatalf("GetProducer() expireTime = %v, want nil", got.ExpireTime)
	}

	// 同 biz 重名 → 23505 映射 409。
	dup := newProducerDO("018f3e1a-0304-7000-8000-0000000000c2", "monitor-engine")
	if err := repo.CreateProducer(ctx, dup); !errors.Is(err, biz.ErrProducerNameExists) {
		t.Fatalf("CreateProducer(dup) error = %v, want ErrProducerNameExists", err)
	}
	// 跨 biz 同名：合法（唯一键含 biz 维度）。
	otherBiz := newProducerDO("018f3e1a-0304-7000-8000-0000000000c3", "monitor-engine")
	otherBiz.BizID = "ci"
	if err := repo.CreateProducer(ctx, otherBiz); err != nil {
		t.Fatalf("CreateProducer(other biz) error = %v", err)
	}

	// 空白名单：nil 归一为 '{}'（NOT NULL 约束下的空数组）。
	bare := newProducerDO("018f3e1a-0304-7000-8000-0000000000c4", "bare-engine")
	bare.EventWhitelist = nil
	if err := repo.CreateProducer(ctx, bare); err != nil {
		t.Fatalf("CreateProducer(nil whitelist) error = %v", err)
	}
	if got, _ = repo.GetProducer(ctx, bare.ID); len(got.EventWhitelist) != 0 {
		t.Fatalf("GetProducer(nil whitelist) = %v, want empty", got.EventWhitelist)
	}

	// Spec 整组替换：改名 + 设过期 + 白名单换组；enabled 独立启停。
	expire := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	disabled := false
	if err := repo.UpdateProducer(ctx, monitor.ID, biz.ProducerUpdate{
		Spec: &biz.ProducerSpec{
			Name:           "monitor-engine-v2",
			EventWhitelist: []string{"permission.change"},
			ExpireTime:     &expire,
		},
	}); err != nil {
		t.Fatalf("UpdateProducer(spec) error = %v", err)
	}
	if err := repo.UpdateProducer(ctx, monitor.ID, biz.ProducerUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("UpdateProducer(enabled) error = %v", err)
	}
	got, err = repo.GetProducer(ctx, monitor.ID)
	if err != nil {
		t.Fatalf("GetProducer(updated) error = %v", err)
	}
	if got.Name != "monitor-engine-v2" || len(got.EventWhitelist) != 1 ||
		got.EventWhitelist[0] != "permission.change" || got.Enabled {
		t.Fatalf("UpdateProducer() result = %+v", got)
	}
	if got.ExpireTime == nil || !got.ExpireTime.Equal(expire) {
		t.Fatalf("UpdateProducer() expireTime = %v, want %v", got.ExpireTime, expire)
	}
	// 改名撞同 biz 既有名 → 409。
	if err := repo.UpdateProducer(ctx, monitor.ID, biz.ProducerUpdate{
		Spec: &biz.ProducerSpec{Name: "bare-engine"},
	}); !errors.Is(err, biz.ErrProducerNameExists) {
		t.Fatalf("UpdateProducer(name conflict) error = %v, want ErrProducerNameExists", err)
	}

	// 轮换：哈希替换 + 权威行返回；不存在 404。
	newHash := []byte("rotated-hash-32-bytes-xxxxxxxxxxx")
	rotated, err := repo.UpdateProducerToken(ctx, monitor.ID, newHash)
	if err != nil {
		t.Fatalf("UpdateProducerToken() error = %v", err)
	}
	if string(rotated.TokenHash) != string(newHash) || rotated.Name != "monitor-engine-v2" {
		t.Fatalf("UpdateProducerToken() = %+v", rotated)
	}
	if got, _ = repo.GetProducer(ctx, monitor.ID); string(got.TokenHash) != string(newHash) {
		t.Fatal("UpdateProducerToken() hash not persisted")
	}
	if _, err := repo.UpdateProducerToken(ctx, missingChannelID, newHash); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("UpdateProducerToken(missing) error = %v, want ErrProducerNotFound", err)
	}

	// 列表：keyword（name 模糊）/ enabled / 分页。
	items, total, err := repo.ListProducers(ctx, biz.ProducerFilter{Keyword: "engine"}, 1, 20)
	if err != nil {
		t.Fatalf("ListProducers(keyword) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("ListProducers(keyword) = %d items, total %d, want 3/3", len(items), total)
	}
	on := true
	items, total, err = repo.ListProducers(ctx, biz.ProducerFilter{Enabled: &on}, 1, 20)
	if err != nil {
		t.Fatalf("ListProducers(enabled) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListProducers(enabled) = %d items, total %d, want 2/2", len(items), total)
	}
	items, total, err = repo.ListProducers(ctx, biz.ProducerFilter{}, 2, 2)
	if err != nil {
		t.Fatalf("ListProducers(paged) error = %v", err)
	}
	if total != 3 || len(items) != 1 {
		t.Fatalf("ListProducers(paged) = %d items, total %d, want 1/3", len(items), total)
	}

	// 软删：读不可见、重复删 404、名称可复用。
	if err := repo.DeleteProducer(ctx, monitor.ID); err != nil {
		t.Fatalf("DeleteProducer() error = %v", err)
	}
	if _, err := repo.GetProducer(ctx, monitor.ID); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("GetProducer(deleted) error = %v, want ErrProducerNotFound", err)
	}
	if err := repo.DeleteProducer(ctx, monitor.ID); !errors.Is(err, biz.ErrProducerNotFound) {
		t.Fatalf("DeleteProducer(twice) error = %v, want ErrProducerNotFound", err)
	}
	reborn := newProducerDO("018f3e1a-0304-7000-8000-0000000000c9", "monitor-engine-v2")
	if err := repo.CreateProducer(ctx, reborn); err != nil {
		t.Fatalf("CreateProducer(reuse name) error = %v", err)
	}

	// 生产者属管道 ingress 鉴权缓存集：每次写同事务 bump cfg_version
	// （与订阅的负断言相反；失败路径回滚不计数）。
	var version int64
	if err := d.Pool.QueryRow(ctx,
		"SELECT version FROM notify_cfg_version WHERE table_name = 'notify_producer'").Scan(&version); err != nil {
		t.Fatalf("read cfg_version: %v", err)
	}
	// 成功写操作：create×4（monitor/otherBiz/bare/reborn，dup 撞唯一回滚）
	// + update×2（spec/enabled，撞名回滚）+ rotate×1 + delete×1 = 8 次成功 bump。
	if version != 8 {
		t.Fatalf("cfg_version = %d, want 8（每次成功写均 bump）", version)
	}
}

// TestIntegration_ProducerKeySealed 配置主密钥后 HMAC 密钥封存落库（管道用同一主密钥
// 解封得到原密钥）；存量裸哈希行在任一次更新时换成封存形态，密钥不变。
func TestIntegration_ProducerKeySealed(t *testing.T) {
	d := newIntegrationNotifyData(t)
	ctx := context.Background()
	peer, err := secretbox.New("integration-test-master-key")
	if err != nil {
		t.Fatal(err)
	}
	storedKey := func(id string) []byte {
		t.Helper()
		var stored []byte
		if err := d.Pool.QueryRow(ctx, "SELECT token_hash FROM notify_producer WHERE id = $1", id).Scan(&stored); err != nil {
			t.Fatalf("read token_hash: %v", err)
		}
		return stored
	}
	open := func(id string) string {
		t.Helper()
		sealed, ok := notifycontract.SealedProducerKey(storedKey(id))
		if !ok {
			t.Fatalf("producer %s key is not sealed", id)
		}
		plain, err := peer.Open(sealed)
		if err != nil {
			t.Fatalf("open sealed key: %v", err)
		}
		return plain
	}

	// 存量行：未配置主密钥时创建 → 裸哈希。
	legacy := newProducerDO("018f3e1a-0304-7000-8000-0000000000d1", "legacy-engine")
	if err := NewProducerRepo(d, nil).CreateProducer(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if string(storedKey(legacy.ID)) != string(legacy.TokenHash) {
		t.Fatal("no master key: key must be stored in legacy form")
	}

	repo := NewProducerRepo(d, newTestSecretBox(t))
	fresh := newProducerDO("018f3e1a-0304-7000-8000-0000000000d2", "sealed-engine")
	if err := repo.CreateProducer(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	if open(fresh.ID) != string(fresh.TokenHash) {
		t.Fatal("CreateProducer() sealed key does not open to the HMAC key")
	}
	rotated := []byte("rotated-key-32-bytes-xxxxxxxxxxx")
	if _, err := repo.UpdateProducerToken(ctx, fresh.ID, rotated); err != nil {
		t.Fatal(err)
	}
	if open(fresh.ID) != string(rotated) {
		t.Fatal("UpdateProducerToken() sealed key does not open to the new HMAC key")
	}
	off := false
	if err := repo.UpdateProducer(ctx, legacy.ID, biz.ProducerUpdate{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	if open(legacy.ID) != string(legacy.TokenHash) {
		t.Fatal("UpdateProducer() must reseal the legacy key unchanged")
	}
}
