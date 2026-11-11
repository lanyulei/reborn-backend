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
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/migrations"

	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"
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
	if err := run(logger, flag.Args()); err != nil {
		logger.Error("migrate failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, args []string) error {
	dsn, err := loadDSN()
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
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

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
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

// loadDSN 读取 data.database.source（与 cmd/server 同一配置源与环境变量覆盖规则）。
func loadDSN() (string, error) {
	c := config.New(config.WithSource(file.NewSource(flagconf), env.NewSource("KRATOS")))
	defer func() { _ = c.Close() }()
	if err := c.Load(); err != nil {
		return "", fmt.Errorf("load config: %w", err)
	}
	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		return "", fmt.Errorf("scan config: %w", err)
	}
	dsn := bc.GetData().GetDatabase().GetSource()
	if dsn == "" {
		return "", errors.New("data.database.source is empty")
	}
	return dsn, nil
}
