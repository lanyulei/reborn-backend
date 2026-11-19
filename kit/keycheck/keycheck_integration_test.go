//go:build integration

package keycheck

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestRunAgainstDatabase 在测试库的临时 schema 里走完登记 / 比对 / 不一致 / 表缺失四条路径。
// 未配置 TEST_DATABASE_SOURCE 时跳过；库名须含 "test"。
func TestRunAgainstDatabase(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping integration test")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if !strings.Contains(cfg.Database, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", cfg.Database)
	}
	ctx := context.Background()
	admin, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	schema := fmt.Sprintf("keycheck_it_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	defer func() { _, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") }()
	scoped := dsn + " search_path=" + schema
	if strings.Contains(dsn, "://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		scoped = dsn + sep + "search_path=" + schema
	}
	status := func(owner bool, secret string) (string, error) {
		t.Helper()
		res, err := Run(ctx, scoped, owner, Key{Name: Master, Secret: secret})
		if err != nil {
			return "", err
		}
		return res[0].Status, nil
	}

	// 表不存在（迁移未执行）：跳过核对，不阻断启动。
	if got, err := status(true, "k1"); err != nil || got != StatusUnavailable {
		t.Fatalf("missing table: %q, %v; want unavailable", got, err)
	}
	if _, err := admin.Exec(ctx, "CREATE TABLE "+schema+".system_key_check (name varchar(64) PRIMARY KEY, fingerprint char(32) NOT NULL, create_time timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// core 尚未登记：其余服务无从比对。
	if got, err := status(false, "k1"); err != nil || got != StatusUnregistered {
		t.Fatalf("before registration: %q, %v; want unregistered", got, err)
	}
	if got, err := status(true, "k1"); err != nil || got != StatusRegistered {
		t.Fatalf("owner first start: %q, %v; want registered", got, err)
	}
	if got, err := status(true, "k1"); err != nil || got != StatusMatched {
		t.Fatalf("owner restart: %q, %v; want matched", got, err)
	}
	if got, err := status(false, "k1"); err != nil || got != StatusMatched {
		t.Fatalf("reader same key: %q, %v; want matched", got, err)
	}
	// 不一致：读者与第二个 core 实例都必须失败，且不得覆盖已登记的指纹。
	if _, err := status(false, "k2"); !errors.Is(err, ErrMismatch) {
		t.Fatalf("reader other key: %v; want ErrMismatch", err)
	}
	if _, err := status(true, "k2"); !errors.Is(err, ErrMismatch) {
		t.Fatalf("owner other key: %v; want ErrMismatch", err)
	}
	if got, err := status(false, "k1"); err != nil || got != StatusMatched {
		t.Fatalf("fingerprint must survive a mismatching owner: %q, %v", got, err)
	}
}
