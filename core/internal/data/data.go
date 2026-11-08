// Package data 实现 biz 定义的仓储接口，是唯一允许 import 数据库/缓存驱动的层。
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	NewUserRepo,
	NewRoleRepo,
	NewUserRoleRepo,
	NewRefreshTokenRepo,
	NewPermissionRepo,
	NewSystemConfigRepo,
	// 接口在消费侧（biz）定义，实现在本包绑定。
	wire.Bind(new(biz.UserReader), new(*userRepo)),
	wire.Bind(new(biz.UserWriter), new(*userRepo)),
	wire.Bind(new(biz.RoleReader), new(*roleRepo)),
	wire.Bind(new(biz.RoleWriter), new(*roleRepo)),
	wire.Bind(new(biz.UserRoleRepo), new(*userRoleRepo)),
	wire.Bind(new(biz.RefreshTokenReader), new(*refreshTokenRepo)),
	wire.Bind(new(biz.RefreshTokenWriter), new(*refreshTokenRepo)),
	wire.Bind(new(biz.PermissionPolicySource), new(*permissionRepo)),
	wire.Bind(new(biz.PermissionGrantReader), new(*permissionRepo)),
	wire.Bind(new(biz.PermissionAdminRepo), new(*permissionRepo)),
	wire.Bind(new(biz.SystemConfigRepo), new(*systemConfigRepo)),
)

// dataPingTimeout 是启动阶段连通性检查超时；启动非请求链路，允许独立超时控制。
const dataPingTimeout = 3 * time.Second

// Data 持有数据层公共资源（PostgreSQL 连接池 + 上传文件目录）。
type Data struct {
	pool    *pgxpool.Pool
	logoDir string
}

// NewData 构造数据层资源：解析 DSN、应用连接池参数并 Ping 验证连通性，
// 失败在装配阶段快速返回错误；cleanup 在进程退出时统一释放连接。
func NewData(c *conf.Data) (*Data, func(), error) {
	if c.GetDatabase().GetSource() == "" {
		return nil, nil, fmt.Errorf("data.database.source is required")
	}
	cfg, err := pgxpool.ParseConfig(c.GetDatabase().GetSource())
	if err != nil {
		return nil, nil, fmt.Errorf("parse database source: %w", err)
	}
	if n := c.GetDatabase().GetMaxConns(); n > 0 {
		cfg.MaxConns = int32(n)
	}
	if n := c.GetDatabase().GetMinConns(); n > 0 {
		cfg.MinConns = int32(n)
	}
	if d := c.GetDatabase().GetMaxConnIdleTime(); d != nil {
		cfg.MaxConnIdleTime = d.AsDuration()
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create database pool: %w", err)
	}
	cleanup := func() { pool.Close() }

	// 启动阶段连通性验证（非请求链路，独立超时上下文）。
	pingCtx, cancel := context.WithTimeout(context.Background(), dataPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("ping database: %w", err)
	}
	logoDir := c.GetUpload().GetDir()
	if logoDir == "" {
		logoDir = "./uploads"
	}
	return &Data{pool: pool, logoDir: logoDir}, cleanup, nil
}
