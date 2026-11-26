package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	"github.com/lanyulei/reborn/backend/kit/loop"
)

// oncallPurgeStore 班次清理桩：每批返回固定的条数，记录语句与事务次数。
type oncallPurgeStore struct {
	mu    sync.Mutex
	per   int
	calls int
	txs   int
}

func (s *oncallPurgeStore) WithinTx(_ context.Context, fn func(tx bizoncall.Tx) error) error {
	s.mu.Lock()
	s.txs++
	s.mu.Unlock()
	return fn(oncallPurgeTx{store: s})
}

func (s *oncallPurgeStore) purgeCalls() (calls, txs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.txs
}

// oncallPurgeTx 只实现班次清理用到的事务方法；其余方法走嵌入的 nil 接口（调用即 panic，测试不会触及）。
type oncallPurgeTx struct {
	bizoncall.Tx
	store *oncallPurgeStore
}

func (t oncallPurgeTx) PurgeDeletedScheduleShifts(context.Context, time.Duration, int) (int, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	t.store.calls++
	return t.store.per, nil
}

// syncLogs 并发安全的日志汇聚：循环在自己的 goroutine 里写，测试 goroutine 读。
type syncLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLogs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func startOncallPurgeLoop(t *testing.T, store *oncallPurgeStore, retention time.Duration, logs *syncLogs) {
	t.Helper()
	uc := bizoncall.NewWorkerUsecase(store, nil, nil, nil, nil)
	logger := slog.New(slog.NewTextHandler(logs, nil))
	var purge *loop.Loop
	for _, s := range NewOncallLoops(uc, bizoncall.Config{DeletedShiftRetention: retention}, logger, nil).Servers() {
		if lp, ok := s.(*loop.Loop); ok && lp.Name() == "oncall-purge" {
			purge = lp
		}
	}
	if purge == nil {
		t.Fatal("the oncall-purge loop is not registered")
	}
	// 循环脱离传入 ctx 的取消（kit/loop），由 Stop 停止。
	go func() { _ = purge.Start(context.Background()) }()
	t.Cleanup(func() {
		if err := purge.Stop(context.Background()); err != nil {
			t.Errorf("stop the loop: %v", err)
		}
	})
}

// oncall-purge 循环启动即清一轮；清到行时记一条带表名与条数的日志；清到行也不立即再跑一轮——每轮按预算删一批，
// 积压按周期（1 小时）消化，不连续空转。
func TestOncallPurgeLoopIsPacedByInterval(t *testing.T) {
	t.Parallel()
	store := &oncallPurgeStore{per: 3}
	var logs syncLogs
	startOncallPurgeLoop(t, store, 12*time.Hour, &logs)

	deadline := time.Now().Add(3 * time.Second)
	for {
		calls, _ := store.purgeCalls()
		if calls > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	if calls, txs := store.purgeCalls(); calls != 1 || txs != 1 {
		t.Fatalf("the purge loop ran %d statements in %d transactions right after start, want exactly 1: a productive round must not trigger an immediate re-run", calls, txs)
	}
	if out := logs.String(); !strings.Contains(out, "table=oncall_shift") || !strings.Contains(out, "count=3") {
		t.Fatalf("purging rows must be logged with the table and the count, got:\n%s", out)
	}
}

// 保留期 0 = 不清理：循环照常注册，但整轮不发语句、不打清理日志。
func TestOncallPurgeLoopDisabledByZeroRetention(t *testing.T) {
	t.Parallel()
	store := &oncallPurgeStore{per: 3}
	var logs syncLogs
	startOncallPurgeLoop(t, store, 0, &logs)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if out := logs.String(); strings.Contains(out, "worker loop started") && strings.Contains(out, "loop=oncall-purge") || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if calls, txs := store.purgeCalls(); calls != 0 || txs != 0 {
		t.Fatalf("retention 0 still issued %d statements in %d transactions", calls, txs)
	}
	if strings.Contains(logs.String(), "table=oncall_shift") {
		t.Fatalf("nothing was purged but a purge log was emitted:\n%s", logs.String())
	}
}
