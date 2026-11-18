// Package data 实现 biz 定义的仓储接口，是唯一允许 import 数据库驱动的层。
// 与 core 同库：配置表只读（数据源健康状态三列可写），运行表读写（monitor_engine_role）。
package data

import (
	"context"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	wire.Bind(new(biz.Pinger), new(*Data)),
	NewLocker, wire.Bind(new(loop.Locker), new(*locker)),
	NewSecretBox, wire.Bind(new(biz.SecretOpener), new(*SecretBox)),
	NewConfigRepo, wire.Bind(new(biz.ConfigRepo), new(*configRepo)), wire.Bind(new(biz.RuleRepo), new(*configRepo)),
	NewTaskRepo, wire.Bind(new(biz.TaskRepo), new(*taskRepo)),
	NewRoundRepo, wire.Bind(new(biz.RoundRepo), new(*roundRepo)),
	NewPlannerRepo, wire.Bind(new(biz.PlannerRepo), new(*plannerRepo)),
	NewMaintenanceRepo, wire.Bind(new(biz.PartitionRepo), new(*maintenanceRepo)), wire.Bind(new(biz.RetentionRepo), new(*maintenanceRepo)), wire.Bind(new(biz.InhibitRepo), new(*configRepo)),
	NewCommandRepo, wire.Bind(new(biz.CommandRepo), new(*commandRepo)),
	NewRecordRepo, wire.Bind(new(biz.RecordRepo), new(*recordRepo)),
	NewSelfmonRepo, wire.Bind(new(biz.WatchRepo), new(*selfmonRepo)), wire.Bind(new(biz.InstanceRepo), new(*selfmonRepo)),
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

// SecretBox 数据源凭据解密器 = kit/secretbox.Box（与 core 写路径同一实现）；引擎只 Open。
type SecretBox struct {
	box *secretbox.Box
}

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = errors.New("credential master key unavailable")

// NewSecretBox 由配置构造（主密钥为空 → 带凭据的数据源解密失败，不阻断启动）。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Open 解密 monitor_datasource.secret；主密钥未配置返回 ErrMasterKeyUnavailable。
// 明文只应驻留调用栈，不得写入日志与错误信息。
func (b *SecretBox) Open(ciphertext []byte) (string, error) {
	out, err := b.box.Open(ciphertext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", ErrMasterKeyUnavailable
	}
	return out, err
}

var (
	_ loop.Locker      = (*locker)(nil)
	_ biz.SecretOpener = (*SecretBox)(nil)
)

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
