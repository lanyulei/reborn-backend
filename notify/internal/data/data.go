// Package data 实现 biz 定义的仓储/骨干接口，是唯一允许 import 数据库驱动的层。
// 与 core 同库：配置表只读（notify_core_role 写），运行表读写（notify_pipe_role）。
package data

import (
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"
	"github.com/lanyulei/reborn/backend/notify/internal/data/channel"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	NewSecretBox,
	NewConfigRepo, wire.Bind(new(biz.ConfigRepo), new(*configRepo)),
	NewRateBucketRepo, wire.Bind(new(biz.RateBucketSyncer), new(*rateBucketRepo)), wire.Bind(new(biz.RateLimiter), new(*rateBucketRepo)),
	NewEventRepo, wire.Bind(new(biz.EventSource), new(*eventRepo)),
	NewDispatchRepo, wire.Bind(new(biz.DispatchRepo), new(*dispatchRepo)),
	NewAggRepo, wire.Bind(new(biz.AggRepo), new(*aggRepo)),
	NewDlqRepo, wire.Bind(new(biz.DlqRepo), new(*dlqRepo)),
	NewIngressRepo, wire.Bind(new(biz.IngressRepo), new(*ingressRepo)),
	NewMaintenanceRepo, wire.Bind(new(biz.MaintenanceRepo), new(*maintenanceRepo)),
	NewLocker, wire.Bind(new(biz.Locker), new(*locker)),
	NewHealthRepo, wire.Bind(new(biz.ChannelHealthRepo), new(*healthRepo)),
	wire.Bind(new(biz.BacklogRepo), new(*Data)),
	wire.Bind(new(biz.Pinger), new(*Data)),
	channel.NewMux,
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
	cleanup := func() { pool.Close() }
	return &Data{Pool: pool}, cleanup, nil
}
