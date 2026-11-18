// Package data 实现 biz 定义的仓储接口，是唯一允许 import 数据库驱动的层。
// 与 core 同库：凭据 / 项目 / 执行节点只读（执行节点的心跳列可写），执行记录与主机仅更新，
// 执行日志读写与清理（job_engine_role，见 core/migrations/0070_job_init）。
// 只有 engine 角色连接数据库：runner 角色下不建连接池，本包的仓储也不会被调用。
package data

import (
	"context"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	wire.Bind(new(biz.Pinger), new(*Data)),
	NewLocker, wire.Bind(new(loop.Locker), new(*locker)),
	NewSecretBox, wire.Bind(new(biz.SecretOpener), new(*SecretBox)),
	NewRunnerRepo, wire.Bind(new(biz.RunnerRepo), new(*runnerRepo)),
	NewExecutionRepo,
	wire.Bind(new(biz.QueueRepo), new(*executionRepo)),
	wire.Bind(new(biz.MaterialRepo), new(*executionRepo)),
	wire.Bind(new(biz.PayloadRepo), new(*executionRepo)),
	wire.Bind(new(biz.ProgressRepo), new(*executionRepo)),
	wire.Bind(new(biz.HousekeepRepo), new(*executionRepo)),
)

// dataPingTimeout 启动连通性检查超时。
const dataPingTimeout = 3 * time.Second

// Data 持有共享连接池；runner 角色（未启用 engine）下 Pool 为 nil。
type Data struct {
	Pool *pgxpool.Pool
}

// NewData 构造连接池并 Ping 验证；cleanup 释放连接。未启用 engine 角色时不连接数据库：
// 执行节点可以部署在够不到数据库的网络区域，只凭 runner 配置启动。
func NewData(roles conf.Roles, c *conf.Data) (*Data, func(), error) {
	if !roles.Has(conf.RoleEngine) {
		return &Data{}, func() {}, nil
	}
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

// Ping 健康检查（1s 超时）；未连接数据库（runner 角色）时恒为健康。
func (d *Data) Ping() error {
	if d.Pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}

// SecretBox 凭据解密器 = kit/secretbox.Box（与 core 写路径同一实现）；引擎只 Open。
type SecretBox struct {
	box *secretbox.Box
}

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = errors.New("credential master key unavailable")

// NewSecretBox 由配置构造（主密钥为空 → 解密失败，被领取的执行记为失败；不阻断启动）。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Open 解密 job_credential / job_project 的密文列；主密钥未配置返回 ErrMasterKeyUnavailable。
// 明文只应驻留调用栈与下发的任务里，不得写入日志与错误信息。
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
	_ biz.Pinger       = (*Data)(nil)
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
