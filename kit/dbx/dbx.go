// Package dbx 收纳 data 层共享的 PostgreSQL 工具（pgx v5）：连接池构造、事务封装、
// LIKE 转义、可空实参归一、advisory lock。仅 data 层可 import（depguard 禁止
// biz/service 引用——驱动依赖只允许在 data）。
package dbx

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolOptions 连接池参数；零值取驱动默认。
type PoolOptions struct {
	MaxConns    int32
	MinConns    int32
	MaxIdleTime time.Duration
	PingTimeout time.Duration // 启动连通性检查超时，零值 3s
}

// NewPool 解析 DSN、应用参数并 Ping 验证；失败在装配阶段快速返回。
func NewPool(dsn string, opts PoolOptions) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, fmt.Errorf("database source is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database source: %w", err)
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.MinConns > 0 {
		cfg.MinConns = opts.MinConns
	}
	if opts.MaxIdleTime > 0 {
		cfg.MaxConnIdleTime = opts.MaxIdleTime
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	timeout := opts.PingTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// WithTx 在单个事务中执行 fn：fn 出错回滚并原样透传，成功则提交；
// Rollback 在已提交后返回 ErrTxClosed，统一忽略。
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	return WithTxOptions(ctx, pool, pgx.TxOptions{}, fn)
}

// WithTxOptions 带隔离级别/访问模式的事务封装。
func WithTxOptions(ctx context.Context, pool *pgxpool.Pool, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// LikeContains 构造 ILIKE 子串匹配实参（%keyword%），转义 LIKE 元字符 `\ % _`：
// 用户输入按字面匹配而非退化成全表通配；PG 缺省转义字符即反斜杠，无需 ESCAPE 子句。
func LikeContains(keyword string) string {
	return "%" + likeEscaper.Replace(keyword) + "%"
}

// NilIfEmpty 空串落 NULL。
func NilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// NilIfEmptyBytes 空字节落 NULL。
func NilIfEmptyBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// TextArray 归一化 text[] 实参：nil 切片会被 pgx 编码为 NULL（撞 NOT NULL），统一落空数组。
func TextArray(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}

// TryAdvisoryLock 非阻塞获取会话级 advisory lock：锁绑定专用连接（Acquire 后 Hijack
// 脱离连接池），release 解锁并关闭连接；连接断即锁释放（PG 会话结束自动释放，无需 TTL）。
// ok=false 表示他人持有。
func TryAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, key string) (release func(), ok bool, err error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire lock conn: %w", err)
	}
	id := LockID(key)
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	raw := conn.Hijack()
	release = func() {
		_, _ = raw.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", id)
		_ = raw.Close(context.WithoutCancel(ctx))
	}
	return release, true, nil
}

// LockID 把锁名哈希为 advisory lock 的 bigint 键。
func LockID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64()) //nolint:gosec // 锁键哈希，溢出翻转无害
}
