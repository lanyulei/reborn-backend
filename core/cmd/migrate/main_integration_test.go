//go:build integration

package main

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// newTestDB 连接 TEST_DATABASE_SOURCE（库名须含 test）。本测试只调用咨询锁函数，不读写任何业务表。
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping integration tests")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// 迁移互斥锁是会话级、独占的：第二个调用方在第一个释放前拿不到锁，释放后可立即获得；
// ctx 取消时放弃等待并归还连接。
func TestAcquireLock_IsExclusive(t *testing.T) {
	db := newTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	release, err := acquireLock(ctx, db, logger)
	if err != nil {
		t.Fatalf("first acquireLock() error = %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer waitCancel()
	if _, err := acquireLock(waitCtx, db, logger); err == nil {
		t.Fatal("second acquireLock() must not succeed while the first holder has not released")
	}

	release()
	// 被取消的等待方归还连接后，锁必须可以被重新获取（没有泄漏的持锁连接）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		release2, err := acquireLock(ctx, db, logger)
		if err == nil {
			release2()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock must be re-acquirable after release: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
