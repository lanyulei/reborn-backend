// Package data 实现 biz 定义的仓储接口，是唯一允许 import 数据库/缓存驱动的层。
//
// 分域结构：各业务域的仓储实现在 internal/data/<domain>/ 子包（子包包名保持
// data）；本包持有跨域共享的连接池（Data）。因域子包需引用本包的 Data 类型，
// ProviderSet 不在此聚合各域（避免 import 环），由 cmd/server/wire.go 显式
// 列出各域 ProviderSet 装配。新增域时在 wire.go 追加一行。
package data

import (
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers：跨域共享的连接池与事务编排器（txrun
// 适配，供 biz 用例层跨域同事务组合业务写与通知事件直插）。
var ProviderSet = wire.NewSet(NewData, NewTxManager, wire.Bind(new(txrun.Manager), new(*TxManager)))

// dataPingTimeout 是启动阶段连通性检查超时；启动非请求链路，允许独立超时控制。
const dataPingTimeout = 3 * time.Second

// Data 持有数据层公共资源（PostgreSQL 连接池 + 上传文件目录）。
// 域子包（internal/data/<domain>）跨包引用，故字段导出；仅限 data 层内部使用，
// biz/service 不得触碰（分层契约见 AGENTS.md）。
type Data struct {
	Pool    *pgxpool.Pool
	LogoDir string
}

// NewData 构造数据层资源：解析 DSN、应用连接池参数并 Ping 验证连通性，
// 失败在装配阶段快速返回错误；cleanup 在进程退出时统一释放连接。
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
	logoDir := c.GetUpload().GetDir()
	if logoDir == "" {
		logoDir = "./uploads"
	}
	return &Data{Pool: pool, LogoDir: logoDir}, cleanup, nil
}
