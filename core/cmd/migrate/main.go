// Package main 数据库迁移命令：golang-migrate 驱动，脚本经 go:embed 打包，
// DSN 读取与服务同一份配置（configs/config.yaml + KRATOS_* 环境变量）。
// 只做装配与子命令分发，不含业务逻辑（AGENTS.md §1）。
//
//	go run ./cmd/migrate -conf configs up            # 应用全部未执行版本（可重复执行）
//	go run ./cmd/migrate -conf configs down          # 回退最近一个版本
//	go run ./cmd/migrate -conf configs version       # 当前版本与 dirty 状态
//	go run ./cmd/migrate -conf configs force 23      # 直接写入版本号（基线 / 修复 dirty），不执行 SQL
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/migrations"
	"github.com/lanyulei/reborn/backend/kit/bootstrap"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql 驱动名 "pgx"
)

// migrationsTable 版本表名（golang-migrate 缺省）。
const migrationsTable = "schema_migrations"

var flagconf string

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

// slogLogger 把 golang-migrate 的日志桥到 slog。
type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, v ...any) {
	s.l.Info(strings.TrimRight(fmt.Sprintf(format, v...), "\n"))
}
func (s slogLogger) Verbose() bool { return false }

func main() {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	// Ctrl-C / SIGTERM 取消等待迁移互斥锁；正在执行的迁移由数据库在连接断开时回滚并标记 dirty。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, flag.Args()); err != nil {
		logger.Error("migrate failed", "err", err)
		os.Exit(1)
	}
}

// migrateLockKey 迁移互斥锁的键：任意固定值，全部环境一致即可。
const migrateLockKey int64 = 0x72626e5f6d696772 // "rebn_migr"

// 等待互斥锁时的轮询间隔与解锁的时限。
const (
	lockPollInterval = time.Second
	unlockTimeout    = 5 * time.Second
)

// acquireLock 获取迁移互斥锁。README 约定迁移由 pg_advisory_lock 互斥，但 golang-migrate 只有 CLI
// 默认加锁，以库方式使用须自己持锁：部署 Job 双副本、或 CI 与人工同时执行时，两个进程会读到同一
// 版本并执行同一个文件（轻则后者因对象已存在失败并置 dirty，重则数据回填在两个事务里交错）。
// 锁是会话级、绑定在专用连接上：连接断开即释放，持锁进程崩溃不会留下死锁。另一个迁移在跑时每秒
// 重试并提示一次，ctx 取消即放弃。返回的 release 解锁并归还连接。
func acquireLock(ctx context.Context, db *sql.DB, logger *slog.Logger) (release func(), err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock connection: %w", err)
	}
	defer func() {
		if err != nil {
			logger.Error("close lock connection", "error", conn.Close())
		}
	}()
	for first := true; ; first = false {
		var got bool
		if err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", migrateLockKey).Scan(&got); err != nil {
			return nil, fmt.Errorf("acquire migrate lock: %w", err)
		}
		if got {
			return func() {
				uctx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
				defer cancel()
				if _, uerr := conn.ExecContext(uctx, "SELECT pg_advisory_unlock($1)", migrateLockKey); uerr != nil {
					logger.Error("release migrate lock", "error", uerr)
				}
				if cerr := conn.Close(); cerr != nil {
					logger.Error("close lock connection", "error", cerr)
				}
			}, nil
		}
		if first {
			logger.Warn("another migration is in progress, waiting for its lock")
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("acquire migrate lock: %w", ctx.Err())
		case <-time.After(lockPollInterval):
		}
	}
}

// run 执行一条迁移命令（version 只读，不持锁）。
func run(ctx context.Context, logger *slog.Logger, args []string) error {
	dsn, err := loadDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
	if cmd != "version" {
		// 写命令（含驱动初始化时建版本表）全部在互斥锁内执行；version 只读，不持锁。
		release, lerr := acquireLock(ctx, db, logger)
		if lerr != nil {
			return lerr
		}
		defer release()
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{MigrationsTable: migrationsTable})
	if err != nil {
		return fmt.Errorf("init database driver: %w", err)
	}
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("init migration source: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	m.Log = slogLogger{l: logger}
	defer func() {
		if serr, derr := m.Close(); serr != nil || derr != nil {
			logger.Error("close migrate", "source_err", serr, "database_err", derr)
		}
	}()

	switch cmd {
	case "up":
		err = m.Up()
	case "down":
		err = m.Steps(-1)
	case "version":
		return printVersion(logger, m)
	case "force":
		if len(args) < 2 {
			return errors.New("usage: migrate force <version>")
		}
		v, perr := strconv.Atoi(args[1])
		if perr != nil {
			return fmt.Errorf("invalid version %q: %w", args[1], perr)
		}
		err = m.Force(v)
	default:
		return fmt.Errorf("unknown command %q (up | down | version | force <version>)", cmd)
	}
	if errors.Is(err, migrate.ErrNoChange) {
		logger.Info("migrate: no change (up to date)")
		return nil
	}
	if err != nil {
		var dirty migrate.ErrDirty
		if errors.As(err, &dirty) {
			return fmt.Errorf("%w; 修复数据库后执行: make migrate-force VERSION=%d", err, dirty.Version)
		}
		return err
	}
	return printVersion(logger, m)
}

// printVersion 输出当前版本与 dirty 标记；空库输出 none。
func printVersion(logger *slog.Logger, m *migrate.Migrate) error {
	version, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		logger.Info("migrate: version none")
		return nil
	}
	if err != nil {
		return fmt.Errorf("read version: %w", err)
	}
	logger.Info("migrate: version", "version", version, "dirty", dirty)
	return nil
}

// loadDSN 读取 data.database.source（与 cmd/server 同一配置源：共享基线 + configs + KRATOS_ 环境变量）。
func loadDSN() (string, error) {
	var bc conf.Bootstrap
	if err := bootstrap.Load(flagconf, &bc); err != nil {
		return "", err
	}
	dsn := bc.GetData().GetDatabase().GetSource()
	if dsn == "" {
		return "", errors.New("data.database.source is empty")
	}
	return dsn, nil
}
