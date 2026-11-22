package biz

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// remindTx 只实现提醒批处理用到的事务方法；其余方法走嵌入的 nil 接口（调用即 panic，测试不会触及）。
// 记录执行过的语句（保存点）与置位过的班次。
type remindTx struct {
	Tx
	due    []*DueReminder
	stmts  []string
	masked map[string]int32
}

func (t *remindTx) Exec(_ context.Context, sql string, _ ...any) (int64, error) {
	t.stmts = append(t.stmts, sql)
	return 0, nil
}

func (t *remindTx) ListDueReminders(context.Context, time.Time, int) ([]*DueReminder, error) {
	return t.due, nil
}

func (t *remindTx) SetRemindedMask(_ context.Context, shiftID string, mask int32) error {
	t.masked[shiftID] = mask
	return nil
}

type remindStore struct{ tx *remindTx }

func (s remindStore) WithinTx(_ context.Context, fn func(tx Tx) error) error { return fn(s.tx) }

// failingWriter 对收件人含 failUser 的事件报错（模拟一行坏数据让通知校验不过）。
type failingWriter struct {
	failUser string
	written  []string
}

func (w *failingWriter) NotifyEvent(_ context.Context, _ txrun.Tx, e biznotify.Event) (*biznotify.Event, error) {
	if slices.Contains(e.Recipients, w.failUser) {
		return nil, errors.New("bad row")
	}
	w.written = append(w.written, e.Title)
	return &biznotify.Event{ID: "ev"}, nil
}

// 一批提醒里一行失败：回滚到保存点只作废它自己，其余照常提醒并置位；失败的不置位（下一轮再试）并以错误返回。
func TestRemindIsolatesABadRow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	shift := func(id, user string) *DueReminder {
		return &DueReminder{
			Shift:         &Shift{ID: id, ScheduleID: "s1", ScheduleName: "一线", GroupID: "g1", Role: RolePrimary, UserID: user, UserName: user, Start: now.Add(10 * time.Minute), End: now.Add(8 * time.Hour)},
			Timezone:      "UTC",
			RemindMinutes: []int32{15},
		}
	}
	tx := &remindTx{due: []*DueReminder{shift("sh-ok-1", "alice"), shift("sh-bad", "mallory"), shift("sh-ok-2", "bob")}, masked: map[string]int32{}}
	writer := &failingWriter{failUser: "mallory"}
	uc := NewWorkerUsecase(remindStore{tx}, nil, nil, nil, NewNotifier(writer))
	uc.now = func() time.Time { return now }

	sent, err := uc.Remind(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sh-bad") {
		t.Fatalf("Remind() error = %v, want the bad row reported", err)
	}
	if sent != 2 || len(writer.written) != 2 {
		t.Fatalf("sent = %d, written = %v; want the two good rows", sent, writer.written)
	}
	if tx.masked["sh-ok-1"] != 1 || tx.masked["sh-ok-2"] != 1 {
		t.Fatalf("good rows must be marked reminded: %v", tx.masked)
	}
	if _, ok := tx.masked["sh-bad"]; ok {
		t.Fatal("the failed row must not be marked (retried next round)")
	}
	// 每行一个保存点：好行 RELEASE，坏行 ROLLBACK TO。
	joined := strings.Join(tx.stmts, "\n")
	if strings.Count(joined, "SAVEPOINT oncall_row") != 3+2+1 || strings.Count(joined, "ROLLBACK TO SAVEPOINT") != 1 || strings.Count(joined, "RELEASE SAVEPOINT") != 2 {
		t.Fatalf("savepoint statements = %q", tx.stmts)
	}
}
