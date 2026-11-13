// Package dbx 收纳 data 层共享的 PostgreSQL 工具（pgx v5）：连接池构造、事务封装、
// LIKE 转义、可空实参归一、advisory lock。仅 data 层可 import（depguard 禁止
// biz/service 引用——驱动依赖只允许在 data）。
package dbx

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 连接与语句超时缺省值；DSN 中显式给出 connect_timeout / statement_timeout 时以 DSN 为准。
const (
	DefaultConnectTimeout   = 5 * time.Second
	DefaultStatementTimeout = 2 * time.Minute
)

// ExecNoTimeout 在独占连接上关闭 statement_timeout 后执行单条语句，结束后恢复
// 会话缺省值再归还连接池。用于确需长时间运行的维护语句（如并发建索引）；
// 调用方自行用 ctx 给出上界。语句在事务块外执行（CREATE INDEX CONCURRENTLY 的前提）。
func ExecNoTimeout(ctx context.Context, pool *pgxpool.Pool, stmt string, args ...any) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		return fmt.Errorf("disable statement_timeout: %w", err)
	}
	// 恢复用脱离取消的短时 ctx：语句被取消后也必须把连接还原，否则该连接带着
	// "无超时" 回到池里。恢复失败则销毁连接。
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(rctx, "RESET statement_timeout"); err != nil {
			_ = conn.Conn().Close(rctx)
		}
	}()
	_, err = conn.Exec(ctx, stmt, args...)
	return err
}

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
	// 超时缺省值（DSN 已显式设置时不覆盖）：
	//   - connect_timeout：库不可达时建连不无限期挂起；
	//   - statement_timeout：单条语句的服务端上界，防失控查询长期占住连接与锁。
	//     取值偏保守（远大于任何正常请求），长时 DDL 走 ExecNoTimeout 豁免；
	//     持有 advisory lock 的空闲连接不执行语句，不受影响。
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = DefaultConnectTimeout
	}
	if _, ok := cfg.ConnConfig.RuntimeParams["statement_timeout"]; !ok {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(DefaultStatementTimeout.Milliseconds(), 10)
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
// ok=false 表示他人持有。持锁连接不在池内、无人探活：ctx 携带 loop.LockProbe
// 时填入对该连接的 Ping，供 loop.Leader 每轮探测——连接已断而持有方不知情
// 会与接任者同时执行（脑裂）。
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
	if probe := loop.LockProbeFromContext(ctx); probe != nil {
		probe.Check = raw.Ping
	}
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
