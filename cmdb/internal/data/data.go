// Package data 实现 biz 定义的仓储接口，是唯一允许 import 数据库驱动与出站 I/O 的层。
// 与 core 同库：元数据只读，资源/关系/历史/任务/运行/暂存读写（cmdb_sync_role）。
package data

import (
	"context"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/cmdb/internal/data/collector"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	NewSecretBox, wire.Bind(new(biz.CredentialOpener), new(*SecretBox)),
	NewMetaRepo, wire.Bind(new(biz.MetaRepo), new(*metaRepo)),
	NewSourceRepo, wire.Bind(new(biz.SourceRepo), new(*sourceRepo)),
	NewTaskRepo, wire.Bind(new(biz.TaskRepo), new(*taskRepo)),
	NewStagingRepo, wire.Bind(new(biz.StagingRepo), new(*stagingRepo)),
	NewReconcileRepo, wire.Bind(new(biz.ReconcileRepo), new(*reconcileRepo)),
	NewSchedulerRepo, wire.Bind(new(biz.SchedulerRepo), new(*schedulerRepo)),
	NewLocker, wire.Bind(new(loop.Locker), new(*locker)),
	wire.Bind(new(biz.Pinger), new(*Data)),
	NewNotifier, wire.Bind(new(biz.Notifier), new(*notifier)),
	NewMaintenanceRepo, wire.Bind(new(biz.MaintenanceRepo), new(*maintenanceRepo)),
	NewWebhookRepo, wire.Bind(new(biz.WebhookRepo), new(*webhookRepo)),
	collector.NewRegistry,
)

// dataPingTimeout 启动连通性检查超时。
const dataPingTimeout = 3 * time.Second

// Data 持有共享连接池。
type Data struct {
	Pool *pgxpool.Pool
}

// NewData 构造连接池并 Ping 验证；cleanup 释放连接。
func NewData(c *conf.Data) (*Data, func(), error) {
	pool, err := dbx.NewPool(c.GetDatabase().GetSource(), dbx.PoolOptions{
		MaxConns:    c.GetDatabase().GetMaxConns(),
		MinConns:    c.GetDatabase().GetMinConns(),
		MaxIdleTime: c.GetDatabase().GetMaxConnIdleTime().AsDuration(),
		PingTimeout: dataPingTimeout,
	})
	if err != nil {
		return nil, nil, err
	}
	return &Data{Pool: pool}, func() { pool.Close() }, nil
}

// Ping 健康检查（1s 超时）。
func (d *Data) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}

// SecretBox 采集源凭据解密器 = kit/secretbox.Box（与 core 写路径同一实现）；数据面只 Open。
type SecretBox struct {
	box *secretbox.Box
}

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = errors.New("credential master key unavailable")

// NewSecretBox 由配置构造（主密钥为空 → 带凭据的任务失败，不阻断启动）。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Open implements biz.CredentialOpener.
func (b *SecretBox) Open(ciphertext []byte) (string, error) {
	out, err := b.box.Open(ciphertext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", ErrMasterKeyUnavailable
	}
	return out, err
}

var _ loop.Locker = (*locker)(nil)

// locker PG advisory lock 分布式互斥（会话级锁绑定专用连接，连接断即失主）。
type locker struct {
	data *Data
}

// NewLocker new a locker.
func NewLocker(data *Data) *locker { return &locker{data: data} }

// TryLock implements loop.Locker.
func (l *locker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	return dbx.TryAdvisoryLock(ctx, l.data.Pool, key)
}
