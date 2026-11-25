//go:build integration

package data

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// 真实死锁：被测的多行更新与另一个事务按相反顺序要同样的行。让被测语句成为被回滚的一方——数据库在「谁先
// 查到死锁」的那个事务上回滚：每个会话都是自己开始等待满一个 deadlock_timeout 才做第一次检查，所以让另一个
// 事务在被测语句刚开始等待时就来要它的行——环在被测语句的第一次检查之前已经闭合，它先查到，被回滚。
// 这正是 NATS 骨干下 relay 落账与 sender 声明任务之间会出现的情形（见 conflict_retry.go）。
//
//	事务 A：锁住 X …………………………… 被测语句开始等待后，马上来更新 Q（被测语句持有）→ 等被测语句
//	被测语句 S：更新 [Q, X]——先锁到 Q，再等 X（A 持有）；与 A 互相等待 → 死锁，S 先查到，被回滚
//	A 随后拿到 Q；A 提交后 S 重做成功。

func TestIntegration_RetryConflictSurvivesRealDeadlock(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	const (
		rowQ = "deadlock-q" // 被测语句先锁到、事务 A 稍后要更新
		rowX = "deadlock-x" // 事务 A 先锁住，被测语句等它
	)
	// 物理顺序（插入顺序）与数组顺序一致：无论执行计划按哪一个走，被测语句都是先 Q、再 X。
	for _, name := range []string{rowQ, rowX} {
		if _, err := d.Pool.Exec(ctx, `INSERT INTO notify_cfg_version (table_name, version) VALUES ($1, 0)`, name); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	txA, err := d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback(ctx) }()
	if _, err := txA.Exec(ctx, `SELECT version FROM notify_cfg_version WHERE table_name = $1 FOR UPDATE`, rowX); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	type result struct {
		tag pgconn.CommandTag
		err error
	}
	statementDone := make(chan result, 1)
	go func() {
		tag, err := retryConflict(ctx, func() (pgconn.CommandTag, error) {
			calls.Add(1)
			return d.Pool.Exec(ctx, `UPDATE notify_cfg_version t SET version = version + 1
				FROM unnest($1::text[]) AS n(name) WHERE t.table_name = n.name`, []string{rowQ, rowX})
		})
		statementDone <- result{tag, err}
	}()
	// 被测语句：锁着 Q，在等 X（用 NOWAIT 试锁判断）。
	waitForLocks(t, d, ctx, []string{rowQ})
	aUpdateDone := make(chan error, 1)
	go func() {
		_, aerr := txA.Exec(ctx, `UPDATE notify_cfg_version SET version = version + 1 WHERE table_name = $1`, rowQ)
		aUpdateDone <- aerr
	}()
	// 被测语句被回滚后事务 A 拿到 Q（它自己不能是被回滚的那个，否则这个夹具没有逼出重做）；A 提交，
	// 被测语句的重做才能拿到 X。
	if err := <-aUpdateDone; err != nil {
		t.Fatalf("transaction A failed with %v: the fixture must make the statement under test the one rolled back", err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var got result
	select {
	case got = <-statementDone:
	case <-time.After(30 * time.Second):
		t.Fatal("statement under test did not finish after the deadlock")
	}
	if got.err != nil {
		t.Fatalf("retryConflict() error = %v; want the statement to succeed on its own after being rolled back", got.err)
	}
	if calls.Load() != 2 {
		t.Fatalf("statement under test ran %d times, want 2 (once before being rolled back, once on retry)", calls.Load())
	}
	if got.tag.RowsAffected() != 2 {
		t.Fatalf("statement under test updated %d rows, want 2", got.tag.RowsAffected())
	}
	if q, x := cfgVersion(t, d, rowQ), cfgVersion(t, d, rowX); q != 2 || x != 1 {
		t.Fatalf("versions = q:%d x:%d; want q 2 (once by each), x 1", q, x)
	}
}

// waitForLocks 等到 locked 里的行都被别的会话锁着。
func waitForLocks(t *testing.T, d *Data, ctx context.Context, locked []string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		ok := true
		for _, name := range locked {
			ok = ok && rowLockedByOthers(ctx, d, name)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rows %v never ended up locked by the statement under test", locked)
		}
	}
}

// rowLockedByOthers 这一行此刻是否被别的会话锁着：用 NOWAIT 试锁，锁不上（55P03）即有人在持有；
// 锁得上则语句结束即释放，不留痕迹。
func rowLockedByOthers(ctx context.Context, d *Data, name string) bool {
	_, err := d.Pool.Exec(ctx, `SELECT version FROM notify_cfg_version WHERE table_name = $1 FOR UPDATE NOWAIT`, name)
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "55P03"
}

// cfgVersion 一行的当前版本号。
func cfgVersion(t *testing.T, d *Data, name string) int64 {
	t.Helper()
	var v int64
	if err := d.Pool.QueryRow(context.Background(), `SELECT version FROM notify_cfg_version WHERE table_name = $1`, name).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}
