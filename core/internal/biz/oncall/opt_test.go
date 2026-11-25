package biz

import (
	"context"
	"slices"
	"testing"
	"time"
)

// digestTx 每日播报批处理用到的事务方法；班次经事务连接读（ListShiftsRange）并记录入参。
type digestTx struct {
	Tx
	due      []*Schedule
	shifts   []*Shift
	from, to time.Time
	digested []string
}

func (t *digestTx) Exec(context.Context, string, ...any) (int64, error) { return 0, nil }

func (t *digestTx) ListDueDigests(context.Context, time.Time, int) ([]*Schedule, error) {
	return t.due, nil
}

func (t *digestTx) ListShiftsRange(_ context.Context, from, to time.Time) ([]*Shift, error) {
	t.from, t.to = from, to
	return t.shifts, nil
}

func (t *digestTx) MarkDigested(_ context.Context, id string, _ time.Time) error {
	t.digested = append(t.digested, id)
	return nil
}

type digestStore struct{ tx *digestTx }

func (s digestStore) WithinTx(_ context.Context, fn func(tx Tx) error) error { return fn(s.tx) }

// 每日播报在持有值班表行锁的事务里读当前班次：必须走事务连接（Tx.ListShiftsRange），不能再经
// ShiftRepo 向连接池要第二条连接。这里 ShiftRepo 传 nil——一旦走回连接池路径即 panic；读取窗口、
// 主备判定与播报内容同改动前。
func TestDigestReadsShiftsOnTxConnection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	day := func(d int) time.Time { return time.Date(2026, 11, d, 0, 0, 0, 0, time.UTC) }
	tx := &digestTx{
		due: []*Schedule{{ID: "s1", Name: "一线", Timezone: "UTC"}, {ID: "s2", Name: "二线", Timezone: "UTC"}},
		shifts: []*Shift{
			{ID: "p", ScheduleID: "s1", Role: RolePrimary, UserName: "甲", Start: day(20), End: day(21)},
			{ID: "b", ScheduleID: "s1", Role: RoleBackup, UserName: "乙", Start: day(20), End: day(21)},
			// 别的值班表的班次不算在 s2 头上。
			{ID: "x", ScheduleID: "s9", Role: RolePrimary, UserName: "丙", Start: day(20), End: day(21)},
		},
	}
	writer := &captureWriter{}
	uc := NewWorkerUsecase(digestStore{tx}, nil, nil, nil, NewNotifier(writer))
	uc.now = func() time.Time { return now }

	sent, err := uc.Digest(context.Background())
	if err != nil || sent != 2 {
		t.Fatalf("Digest() = %d, %v, want 2, nil", sent, err)
	}
	if !tx.from.Equal(now) || !tx.to.Equal(now.Add(time.Second)) {
		t.Errorf("shift window = [%s, %s), want [now, now+1s)", tx.from, tx.to)
	}
	if !slices.Equal(tx.digested, []string{"s1", "s2"}) {
		t.Errorf("digested = %v, want [s1 s2]", tx.digested)
	}
	if len(writer.events) != 2 {
		t.Fatalf("events = %d, want 2", len(writer.events))
	}
	if e := writer.events[0]; e.Title != "今日值班：一线 · 甲" || e.Payload["primary"] != "甲" || e.Payload["backup"] != "乙" {
		t.Errorf("s1 digest = %q %v", e.Title, e.Payload)
	}
	if e := writer.events[1]; e.Title != "今日值班：二线 · 无人值班" {
		t.Errorf("s2 digest title = %q", e.Title)
	}
}

// 没有到点的值班表时不读班次（同改动前：直接返回）。
func TestDigestSkipsShiftReadWhenNothingDue(t *testing.T) {
	t.Parallel()
	tx := &digestTx{}
	uc := NewWorkerUsecase(digestStore{tx}, nil, nil, nil, NewNotifier(&captureWriter{}))
	if sent, err := uc.Digest(context.Background()); err != nil || sent != 0 {
		t.Fatalf("Digest() = %d, %v", sent, err)
	}
	if !tx.from.IsZero() {
		t.Error("shifts were read although no schedule was due")
	}
}

// scopedShifts 记录 ListRangeScoped 的入参；按 scope 过滤与否由 filter 决定（模拟数据库下推）。
type scopedShifts struct {
	ShiftRepo
	shifts   []*Shift
	filter   bool
	scope    ShiftScope
	from, to time.Time
	calls    int
}

func (r *scopedShifts) ListRangeScoped(_ context.Context, from, to time.Time, scope ShiftScope) ([]*Shift, error) {
	r.calls++
	r.scope, r.from, r.to = scope, from, to
	if !r.filter {
		return r.shifts, nil
	}
	out := make([]*Shift, 0, len(r.shifts))
	for _, sh := range r.shifts {
		if sh.ScheduleID == scope.ScheduleID {
			out = append(out, sh)
		}
	}
	return out, nil
}

// OnDuty 只取这张值班表的班次（谓词下推到 SQL），结果与全量读取后内存过滤一致：仓储按范围过滤
// 与否（filter）不影响结果——逐行判定保留。
func TestOnDutyPushesScheduleScopeDown(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	day := func(d int) time.Time { return time.Date(2026, 11, d, 0, 0, 0, 0, time.UTC) }
	shifts := []*Shift{
		{ScheduleID: "s1", Role: RolePrimary, UserID: "a", Start: day(20), End: day(21)},
		{ScheduleID: "s1", Role: RoleBackup, UserID: "b", Start: day(20), End: day(21)},
		{ScheduleID: "s2", Role: RolePrimary, UserID: "z", Start: day(20), End: day(21)},
	}
	for _, filter := range []bool{false, true} {
		env := &memEnv{sched: &Schedule{ID: "s1", Status: StatusEnabled}}
		repo := &scopedShifts{shifts: shifts, filter: filter}
		uc := NewDutyUsecase(&memSchedules{env: env}, repo)
		users, err := uc.OnDuty(context.Background(), "s1", at)
		if err != nil || !slices.Equal(users, []string{"a", "b"}) {
			t.Fatalf("filter=%v OnDuty() = %v, %v, want [a b]", filter, users, err)
		}
		if repo.scope != (ShiftScope{ScheduleID: "s1"}) || !repo.from.Equal(at) || !repo.to.Equal(at.Add(time.Second)) {
			t.Errorf("filter=%v query = %+v [%s, %s)", filter, repo.scope, repo.from, repo.to)
		}
	}
	// 停用的值班表不读班次。
	env := &memEnv{sched: &Schedule{ID: "s1", Status: StatusDisabled}}
	repo := &scopedShifts{shifts: shifts}
	if users, err := NewDutyUsecase(&memSchedules{env: env}, repo).OnDuty(context.Background(), "s1", at); err != nil || len(users) != 0 || repo.calls != 0 {
		t.Errorf("disabled schedule: users %v, err %v, calls %d", users, err, repo.calls)
	}
}

// touchShares 公共日历用到的地址仓储：记录 Touch 收到的 ctx 状态。
type touchShares struct {
	ShareRepo
	link        *ShareLink
	touched     int
	hasDeadline bool
	remaining   time.Duration
	ctxErr      error
}

func (r *touchShares) FindByHash(context.Context, []byte) (*ShareLink, error) { return r.link, nil }

func (r *touchShares) Touch(ctx context.Context, _ string) error {
	r.touched++
	deadline, ok := ctx.Deadline()
	r.hasDeadline, r.remaining, r.ctxErr = ok, time.Until(deadline), ctx.Err()
	return nil
}

type touchGroups struct{ GroupRepo }

func (touchGroups) Get(_ context.Context, id, _ string) (*Group, error) {
	return &Group{ID: id, Name: "运维"}, nil
}

// 公共日历的访问时间落账：不随请求取消（请求已断开也照记），但带上限——连接池排队时不拖住匿名接口。
func TestPublicCalendarTouchIsBounded(t *testing.T) {
	t.Parallel()
	shares := &touchShares{link: &ShareLink{ID: "l1", ScopeType: ScopeGroup, ScopeID: "g1"}}
	shifts := &scopedShifts{shifts: []*Shift{{ScheduleID: "s1", UserPhone: "138"}}}
	uc := NewShareUsecase(nil, touchGroups{}, nil, shifts, shares, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 请求方已断开
	from := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	cal, err := uc.Public(ctx, "token", "10.0.0.1", from, from.AddDate(0, 0, 7))
	if err != nil {
		t.Fatalf("Public() error = %v", err)
	}
	if cal.Title != "运维" || len(cal.Shifts) != 1 || cal.Shifts[0].UserPhone != "" {
		t.Errorf("calendar = %+v", cal)
	}
	if shares.touched != 1 {
		t.Fatalf("Touch called %d times, want 1", shares.touched)
	}
	if shares.ctxErr != nil {
		t.Errorf("touch ctx err = %v, want nil (must not follow request cancellation)", shares.ctxErr)
	}
	if !shares.hasDeadline || shares.remaining <= 0 || shares.remaining > shareTouchTimeout {
		t.Errorf("touch ctx deadline: has %v, remaining %s, want within %s", shares.hasDeadline, shares.remaining, shareTouchTimeout)
	}
}
