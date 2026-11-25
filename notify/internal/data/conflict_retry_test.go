package data

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgErr 构造一个带 SQLSTATE 的数据库错误。
func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: "ERROR: " + code + " (SQLSTATE " + code + ")"}
}

func TestIsConflict(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":            {nil, false},
		"plain":          {errors.New("boom"), false},
		"deadlock":       {pgErr(sqlstateDeadlockDetected), true},
		"serialization":  {pgErr(sqlstateSerializationFail), true},
		"wrapped":        {fmt.Errorf("exec: %w", pgErr(sqlstateDeadlockDetected)), true},
		"other pg error": {pgErr("23505"), false},
	} {
		if got := isConflict(tc.err); got != tc.want {
			t.Errorf("%s: isConflict() = %v, want %v", name, got, tc.want)
		}
	}
}

// 首次就成功：只执行一次。
func TestRetryConflict_FirstTrySucceeds(t *testing.T) {
	t.Parallel()
	calls := 0
	got, err := retryConflict(context.Background(), func() (string, error) {
		calls++
		return "ok", nil
	})
	if err != nil || got != "ok" || calls != 1 {
		t.Fatalf("retryConflict() = %q, %v after %d calls; want ok, nil, 1 call", got, err, calls)
	}
}

// 死锁 / 序列化失败会重做，直到成功；被包装过的也算。
func TestRetryConflict_RetriesUntilItSucceeds(t *testing.T) {
	t.Parallel()
	for name, code := range map[string]string{"deadlock": sqlstateDeadlockDetected, "serialization": sqlstateSerializationFail} {
		calls := 0
		got, err := retryConflict(context.Background(), func() (int, error) {
			calls++
			if calls < 3 {
				return 0, fmt.Errorf("mark published: %w", pgErr(code))
			}
			return calls, nil
		})
		if err != nil || got != 3 || calls != 3 {
			t.Errorf("%s: retryConflict() = %d, %v after %d calls; want 3, nil, 3 calls", name, got, err, calls)
		}
	}
}

// 其它错误不重做：原样返回，一次都不多执行。
func TestRetryConflict_DoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"plain":      errors.New("connection reset"),
		"unique":     pgErr("23505"),
		"cancelled":  context.Canceled,
		"query args": pgErr("42P02"),
	} {
		calls := 0
		if _, got := retryConflict(context.Background(), func() (struct{}, error) { calls++; return struct{}{}, err }); !errors.Is(got, err) || calls != 1 {
			t.Errorf("%s: retryConflict() error = %v (want the original) after %d calls (want 1)", name, got, calls)
		}
	}
}

// 一直撞上：重做到上限为止，返回最后一次的错误（不吞、不换成别的错误）。
func TestRetryConflict_GivesUpAfterLimit(t *testing.T) {
	t.Parallel()
	start := time.Now()
	calls := 0
	_, err := retryConflict(context.Background(), func() (struct{}, error) {
		calls++
		return struct{}{}, pgErr(sqlstateDeadlockDetected)
	})
	if !isConflict(err) || calls != conflictRetries+1 {
		t.Fatalf("retryConflict() error = %v after %d calls; want the conflict error after %d calls", err, calls, conflictRetries+1)
	}
	// 退避是 10 + 20 + 40ms；留出调度余量，只卡下限，避免机器快慢让测试不稳定。
	if waited := time.Since(start); waited < conflictBackoff*((1<<conflictRetries)-1) {
		t.Fatalf("retryConflict() returned after %v; the backoff between retries must be honoured", waited)
	}
}

// 等待期间 ctx 结束：立刻返回最后一次的错误，不继续重做、不返回成功。
func TestRetryConflict_StopsWhenContextEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan error, 1)
	go func() {
		_, err := retryConflict(ctx, func() (struct{}, error) {
			calls++
			cancel() // 第一次失败之后、退避等待之中取消
			return struct{}{}, pgErr(sqlstateDeadlockDetected)
		})
		done <- err
	}()
	select {
	case err := <-done:
		if !isConflict(err) || calls != 1 {
			t.Fatalf("retryConflict() error = %v after %d calls; want the conflict error after exactly 1 call", err, calls)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retryConflict() did not return after the context ended")
	}
}
