package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 已删除值班表的班次保留期：未设置取缺省（30 天），显式设置按配置值，显式 0（及负值）= 不清理。
func TestNewConfig_DeletedShiftRetention(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		c    *conf.Oncall
		want time.Duration
	}{
		{name: "nil config", c: nil, want: DefaultDeletedShiftRetention},
		{name: "unset", c: &conf.Oncall{}, want: DefaultDeletedShiftRetention},
		{name: "explicit zero disables", c: &conf.Oncall{DeletedScheduleShiftRetention: durationpb.New(0)}, want: 0},
		{name: "negative disables", c: &conf.Oncall{DeletedScheduleShiftRetention: durationpb.New(-time.Hour)}, want: 0},
		{name: "explicit value", c: &conf.Oncall{DeletedScheduleShiftRetention: durationpb.New(7 * 24 * time.Hour)}, want: 7 * 24 * time.Hour},
	}
	for _, tc := range cases {
		if got := NewConfig(tc.c).DeletedShiftRetention; got != tc.want {
			t.Errorf("%s: DeletedShiftRetention = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// 分批清理的停止条件：不足一批即停（已清完）、语句条数用尽即停、时间预算用尽即停、ctx 结束与出错原样返回，
// 返回值始终是已删除的条数。
func TestPurgeBatched_StopConditions(t *testing.T) {
	t.Parallel()
	const batch = 10
	full := func(context.Context) (int, error) { return batch, nil }
	o := loop.PurgeOptions{Batch: batch, MaxStatements: 4, Budget: time.Minute}
	scripted := func(results ...int) func(context.Context) (int, error) {
		return func(context.Context) (int, error) {
			if len(results) == 0 {
				t.Fatal("step called after it should have stopped")
			}
			n := results[0]
			results = results[1:]
			return n, nil
		}
	}
	if total, err := loop.PurgeBatched(context.Background(), o, scripted(batch, batch, 3)); err != nil || total != 23 {
		t.Fatalf("drained: total = %d err = %v, want 23 (stops on a partial batch)", total, err)
	}
	if total, err := loop.PurgeBatched(context.Background(), o, scripted(0)); err != nil || total != 0 {
		t.Fatalf("nothing to delete: total = %d err = %v, want 0 after one statement", total, err)
	}
	if total, err := loop.PurgeBatched(context.Background(), o, full); err != nil || total != batch*4 {
		t.Fatalf("statement Budget: total = %d err = %v, want %d", total, err, batch*4)
	}
	if total, err := loop.PurgeBatched(context.Background(), loop.PurgeOptions{Batch: batch, MaxStatements: 4, Budget: -time.Second}, full); err != nil || total != batch {
		t.Fatalf("time Budget: total = %d err = %v, want %d", total, err, batch)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if total, err := loop.PurgeBatched(ctx, o, full); !errors.Is(err, context.Canceled) || total != batch {
		t.Fatalf("ctx done: total = %d err = %v, want %d + context.Canceled", total, err, batch)
	}
	boom := errors.New("db down")
	if total, err := loop.PurgeBatched(context.Background(), o, func(context.Context) (int, error) { return batch, boom }); !errors.Is(err, boom) || total != batch {
		t.Fatalf("step error: total = %d err = %v, want %d + boom", total, err, batch)
	}
}

// purgeTx 只实现班次清理用到的事务方法；其余方法走嵌入的 nil 接口（调用即 panic，测试不会触及）。
// 按脚本返回每批的条数，脚本用尽后返回 err（nil 即 0 行）。
type purgeTx struct {
	Tx
	retentions []time.Duration
	limits     []int
	script     []int
	err        error
}

func (t *purgeTx) PurgeDeletedScheduleShifts(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	t.retentions = append(t.retentions, olderThan)
	t.limits = append(t.limits, limit)
	if len(t.script) == 0 {
		return 0, t.err
	}
	n := t.script[0]
	t.script = t.script[1:]
	return n, nil
}

type purgeStore struct {
	tx  *purgeTx
	txs int
}

func (s *purgeStore) WithinTx(_ context.Context, fn func(tx Tx) error) error {
	s.txs++
	return fn(s.tx)
}

// 班次清理用例：保留期 0 一条语句都不发；否则按配置的保留期与批大小经事务下发；积压时一轮至多发满语句条数、
// 每批一个事务；仓储出错时带上出错前已删的条数返回。
func TestWorkerPurgeDeletedScheduleShifts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	newTx := func(script ...int) *purgeTx { return &purgeTx{script: script} }

	store := &purgeStore{tx: newTx()}
	if n, err := NewWorkerUsecase(store, nil, nil, nil, nil).PurgeDeletedScheduleShifts(ctx, 0); err != nil || n != 0 || len(store.tx.retentions) != 0 || store.txs != 0 {
		t.Fatalf("retention 0: n = %d err = %v calls = %d txs = %d; want no statement at all", n, err, len(store.tx.retentions), store.txs)
	}

	tx := newTx(3)
	store = &purgeStore{tx: tx}
	if n, err := NewWorkerUsecase(store, nil, nil, nil, nil).PurgeDeletedScheduleShifts(ctx, NewConfig(nil).DeletedShiftRetention); err != nil || n != 3 || len(tx.retentions) != 1 {
		t.Fatalf("PurgeDeletedScheduleShifts() = %d, %v calls = %d; want 3 after one batch", n, err, len(tx.retentions))
	}
	if tx.retentions[0] != 30*24*time.Hour || tx.limits[0] != shiftPurgeBatch {
		t.Fatalf("PurgeDeletedScheduleShifts called with retention = %v limit = %d", tx.retentions[0], tx.limits[0])
	}

	// 积压：每批都删满时一轮至多发 shiftPurgeStatements 条，每批一个事务。
	tx = newTx()
	for range shiftPurgeStatements + 3 {
		tx.script = append(tx.script, shiftPurgeBatch)
	}
	store = &purgeStore{tx: tx}
	if n, err := NewWorkerUsecase(store, nil, nil, nil, nil).PurgeDeletedScheduleShifts(ctx, DefaultDeletedShiftRetention); err != nil || n != shiftPurgeBatch*shiftPurgeStatements ||
		len(tx.retentions) != shiftPurgeStatements || store.txs != shiftPurgeStatements {
		t.Fatalf("backlog: n = %d err = %v calls = %d txs = %d; want %d statements, one transaction each", n, err, len(tx.retentions), store.txs, shiftPurgeStatements)
	}

	// 仓储出错：返回错误，出错前已删的条数不丢。
	boom := errors.New("db down")
	tx = &purgeTx{script: []int{shiftPurgeBatch}, err: boom}
	store = &purgeStore{tx: tx}
	if n, err := NewWorkerUsecase(store, nil, nil, nil, nil).PurgeDeletedScheduleShifts(ctx, DefaultDeletedShiftRetention); !errors.Is(err, boom) || n != shiftPurgeBatch {
		t.Fatalf("repo error: n = %d err = %v; want %d + boom", n, err, shiftPurgeBatch)
	}
}
