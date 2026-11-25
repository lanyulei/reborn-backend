package biz

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// memReminderGrace 交接时刻提醒的补发宽限（与 data 层 reminderGrace 同值，那边是 data 包的私有常量）。
const memReminderGrace = 10 * time.Minute

// memEnv 值班域用例的内存状态：排班、提醒、替班几个用例共用一份，经 memTx 读写。
type memEnv struct {
	sched   *Schedule
	members []*Member
	// shifts 按开始时间排序（fillSlots / avoidSameUser 的前置条件）。
	shifts []*Shift
	logs   []*ChangeLog
	// remaps UpdateRemindedMasks 被调用的次数（提醒提前量没变时不该被调用）。
	remaps int
}

// memTx 值班域事务的内存桩：只实现这几个用例用到的方法，其余走嵌入的 nil 接口（调用即 panic，
// 测试不会触及）。
type memTx struct {
	Tx
	env *memEnv
}

func (t *memTx) Exec(context.Context, string, ...any) (int64, error) { return 0, nil }

func (t *memTx) LockGroup(_ context.Context, id string) (*Group, error) {
	return &Group{ID: id}, nil
}

func (t *memTx) LockSchedule(_ context.Context, id string) (*Schedule, error) {
	if t.env.sched == nil || t.env.sched.ID != id {
		return nil, ErrScheduleNotFound
	}
	s := *t.env.sched
	return &s, nil
}

func (t *memTx) ListMembers(context.Context, string) ([]*Member, error) { return t.env.members, nil }

func (t *memTx) UpdateSchedule(_ context.Context, s *Schedule, _ string) error {
	t.env.sched = s
	return nil
}

func (t *memTx) SetGeneratedUntil(_ context.Context, _ string, until *time.Time) error {
	t.env.sched.GeneratedUntil = until
	return nil
}

func (t *memTx) shiftsFrom(scheduleID string, from time.Time) []*Shift {
	var out []*Shift
	for _, sh := range t.env.shifts {
		if sh.ScheduleID == scheduleID && sh.End.After(from) {
			out = append(out, sh)
		}
	}
	return out
}

func (t *memTx) ListShiftsFrom(_ context.Context, scheduleID string, from time.Time) ([]*Shift, error) {
	return t.shiftsFrom(scheduleID, from), nil
}

func (t *memTx) LockShiftsFrom(_ context.Context, scheduleID string, from time.Time) ([]*Shift, error) {
	return t.shiftsFrom(scheduleID, from), nil
}

func (t *memTx) UpdateRemindedMasks(_ context.Context, masks []ShiftMask) error {
	t.env.remaps++
	for _, m := range masks {
		for _, sh := range t.env.shifts {
			if sh.ID == m.ShiftID {
				sh.RemindedMask = m.Mask
			}
		}
	}
	return nil
}

func (t *memTx) DeleteFutureAutoShifts(_ context.Context, scheduleID string, now time.Time) error {
	t.env.shifts = slices.DeleteFunc(t.env.shifts, func(sh *Shift) bool {
		return sh.ScheduleID == scheduleID && sh.Source == SourceAuto && sh.Start.After(now)
	})
	return nil
}

func (t *memTx) InsertShifts(_ context.Context, shifts []*Shift, _ string) error {
	t.env.shifts = append(t.env.shifts, shifts...)
	slices.SortStableFunc(t.env.shifts, func(a, b *Shift) int { return a.Start.Compare(b.Start) })
	return nil
}

func (t *memTx) DeleteShift(_ context.Context, id string) error {
	t.env.shifts = slices.DeleteFunc(t.env.shifts, func(sh *Shift) bool { return sh.ID == id })
	return nil
}

func (t *memTx) InsertChangeLog(_ context.Context, l *ChangeLog) error {
	t.env.logs = append(t.env.logs, l)
	return nil
}

// ListDueReminders 与 data 层同口径的到期判定：启用、配置了提醒、班次开始时间落在
// (now-宽限, now+7 天]，且至少有一个提醒已到点而未发送。
func (t *memTx) ListDueReminders(_ context.Context, now time.Time, _ int) ([]*DueReminder, error) {
	s := t.env.sched
	var due []*DueReminder
	for _, sh := range t.shiftsFrom(s.ID, now) {
		if s.Status != StatusEnabled || len(s.RemindMinutes) == 0 || !sh.Start.After(now.Add(-memReminderGrace)) || sh.Start.After(now.Add(inspectWindow)) {
			continue
		}
		pending := false
		for i, m := range s.RemindMinutes {
			if sh.RemindedMask&(int32(1)<<i) == 0 && !sh.Start.After(now.Add(time.Duration(m)*time.Minute)) {
				pending = true
			}
		}
		if !pending {
			continue
		}
		continued := slices.ContainsFunc(t.env.shifts, func(p *Shift) bool {
			return p.ID != sh.ID && p.ScheduleID == sh.ScheduleID && p.Role == sh.Role && p.UserID == sh.UserID && p.End.Equal(sh.Start)
		})
		due = append(due, &DueReminder{Shift: sh, Timezone: s.Timezone, RemindMinutes: s.RemindMinutes, Continued: continued})
	}
	return due, nil
}

func (t *memTx) SetRemindedMask(_ context.Context, shiftID string, mask int32) error {
	for _, sh := range t.env.shifts {
		if sh.ID == shiftID {
			sh.RemindedMask = mask
		}
	}
	return nil
}

type memStore struct{ env *memEnv }

func (s *memStore) WithinTx(_ context.Context, fn func(tx Tx) error) error {
	return fn(&memTx{env: s.env})
}

// memSchedules 只实现 Get；List 等读方法这几个用例不会触及。
type memSchedules struct {
	ScheduleRepo
	env *memEnv
}

func (r *memSchedules) Get(context.Context, string) (*Schedule, error) {
	if r.env.sched == nil {
		return nil, ErrScheduleNotFound
	}
	s := *r.env.sched
	return &s, nil
}

// payloadIDs 事件载荷里反序列化出来的字符串数组。
func payloadIDs(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.(string))
	}
	return out
}

// capturedEvent 落库的通知事件（载荷解码后保留）。
type capturedEvent struct {
	Title      string
	Recipients []string
	Payload    map[string]any
}

// captureWriter 记录落库的通知事件。
type captureWriter struct{ events []capturedEvent }

func (w *captureWriter) NotifyEvent(_ context.Context, _ txrun.Tx, e biznotify.Event) (*biznotify.Event, error) {
	var payload map[string]any
	if err := json.Unmarshal(e.Payload, &payload); err != nil {
		return nil, err
	}
	w.events = append(w.events, capturedEvent{Title: e.Title, Recipients: e.Recipients, Payload: payload})
	return &biznotify.Event{ID: e.EventType}, nil
}

// newMemScheduleEnv 一张按天轮转的值班表（UTC，每天 0 点交接，名单 a → b → c）：此刻 2026-11-20
// 06:00，a 值班到明天 0 点；返回的 now 可拨动。
func newMemScheduleEnv(t *testing.T, remind []int32) (*memEnv, *ScheduleUsecase, *WorkerUsecase, *captureWriter, *time.Time) {
	t.Helper()
	day := func(d int) time.Time { return time.Date(2026, 11, d, 0, 0, 0, 0, time.UTC) }
	until := day(25)
	env := &memEnv{
		sched: &Schedule{
			ID: "s1", GroupID: "g1", Name: "一线", Timezone: "UTC", RotationUnit: UnitDay, RotationInterval: 1,
			AnchorTime: day(19), Status: StatusEnabled, HorizonDays: 7, RemindMinutes: remind, MemberIDs: []string{"a", "b", "c"},
			GeneratedUntil: &until,
		},
		members: []*Member{{UserID: "a", Available: true}, {UserID: "b", Available: true}, {UserID: "c", Available: true}},
		shifts: []*Shift{
			{ID: "d20", ScheduleID: "s1", GroupID: "g1", Role: RolePrimary, UserID: "a", Start: day(20), End: day(21), Source: SourceAuto},
			{ID: "d21", ScheduleID: "s1", GroupID: "g1", Role: RolePrimary, UserID: "b", Start: day(21), End: day(22), Source: SourceAuto},
			{ID: "d22", ScheduleID: "s1", GroupID: "g1", Role: RolePrimary, UserID: "c", Start: day(22), End: day(23), Source: SourceAuto},
			{ID: "d23", ScheduleID: "s1", GroupID: "g1", Role: RolePrimary, UserID: "a", Start: day(23), End: day(24), Source: SourceAuto},
		},
	}
	writer := &captureWriter{}
	notifier := NewNotifier(writer)
	schedUC := NewScheduleUsecase(&memStore{env}, nil, &memSchedules{env: env}, nil, notifier)
	worker := NewWorkerUsecase(&memStore{env}, nil, &memSchedules{env: env}, nil, notifier)
	now := day(20).Add(6 * time.Hour)
	schedUC.now, worker.now = func() time.Time { return now }, func() time.Time { return now }
	return env, schedUC, worker, writer, &now
}

func dailyConfigOf(env *memEnv, remind ...int32) ScheduleConfig {
	return ScheduleConfig{
		Name: env.sched.Name, Timezone: env.sched.Timezone, RotationUnit: UnitDay, RotationInterval: 1, AnchorTime: env.sched.AnchorTime,
		MemberIDs: env.sched.MemberIDs, BackupEnabled: false, HorizonDays: 7, RemindMinutes: remind,
	}
}

var memAdmin = Actor{UserID: "admin", IsAdmin: true}

// remind 把时钟拨到 at 后跑一轮提醒。
func remind(t *testing.T, worker *WorkerUsecase, now *time.Time, at time.Time) int {
	t.Helper()
	*now = at
	sent, err := worker.Remind(context.Background())
	if err != nil {
		t.Fatalf("Remind(%s) error = %v", at.Format(time.RFC3339), err)
	}
	return sent
}

// 提醒提前量固定为从大到小：同一组值的下标才有确定含义。
func TestApplyNormalizesRemindMinutes(t *testing.T) {
	t.Parallel()
	members := memberSet([]*Member{{UserID: "a"}, {UserID: "b"}})
	for _, tc := range []struct {
		in   []int32
		want []int32
	}{
		{[]int32{30, 1440}, []int32{1440, 30}},
		{[]int32{1440, 30}, []int32{1440, 30}},
		{[]int32{0, 60}, []int32{60, 0}},
		{[]int32{15}, []int32{15}},
		{nil, nil},
	} {
		var s Schedule
		if err := (ScheduleConfig{
			Name: "n", Timezone: "UTC", RotationUnit: UnitDay, RotationInterval: 1, AnchorTime: time.Now(),
			MemberIDs: []string{"a"}, HorizonDays: 7, RemindMinutes: tc.in,
		}).apply(&s, members); err != nil {
			t.Fatalf("apply(%v) error = %v", tc.in, err)
		}
		if !slices.Equal(s.RemindMinutes, tc.want) {
			t.Errorf("apply(%v).RemindMinutes = %v, want %v", tc.in, s.RemindMinutes, tc.want)
		}
	}
	// 不改动调用方的切片。
	in := []int32{30, 1440}
	if got := normalizeRemindMinutes(in); !slices.Equal(got, []int32{1440, 30}) || !slices.Equal(in, []int32{30, 1440}) {
		t.Fatalf("normalizeRemindMinutes() = %v and left the input as %v", got, in)
	}
}

// 已提醒标记按值对到新下标：对不上的丢弃，新出现的保持未发送。
func TestRemapRemindedMask(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		old, next  []int32
		mask, want int32
	}{
		{"去掉已发的那条：剩下的提醒变为未发", []int32{1440, 30}, []int32{30}, 0b01, 0b00},
		{"去掉已发的那条：另一条也已发则保留", []int32{1440, 30}, []int32{30}, 0b11, 0b01},
		{"调换顺序：已发的跟着值走", []int32{30, 1440}, []int32{1440, 30}, 0b01, 0b10},
		{"调换顺序：两条都已发", []int32{30, 1440}, []int32{1440, 30}, 0b11, 0b11},
		{"换值：只有值相同的提醒保留已发", []int32{60, 0}, []int32{30, 0}, 0b11, 0b10},
		{"清空提醒", []int32{60}, nil, 0b01, 0b00},
		{"原先没有提醒：旧标记不沿用到新提醒", nil, []int32{60}, 0b11, 0b00},
		{"未变", []int32{60, 0}, []int32{60, 0}, 0b10, 0b10},
	} {
		if got := remapRemindedMask(tc.old, tc.next, tc.mask); got != tc.want {
			t.Errorf("%s: remap(%v → %v, %02b) = %02b, want %02b", tc.name, tc.old, tc.next, tc.mask, got, tc.want)
		}
	}
}

// 报告里的失败场景一：提醒 [1440, 30]，明天那班的「提前一天」已发（mask=01）；管理员改成 [30]。
// 不改写的话第 0 位被当成「30 分钟已发」，30 分钟前的提醒永远不再发（漏提醒）。
func TestUpdateRemindMinutesDoesNotLoseReminder(t *testing.T) {
	t.Parallel()
	env, schedUC, worker, writer, now := newMemScheduleEnv(t, []int32{1440, 30})
	next := env.shifts[1] // 明天 0 点开始的一班
	next.RemindedMask = 0b01

	if _, err := schedUC.Update(context.Background(), memAdmin, "s1", dailyConfigOf(env, 30)); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if !slices.Equal(env.sched.RemindMinutes, []int32{30}) {
		t.Fatalf("stored remind minutes = %v", env.sched.RemindMinutes)
	}
	if next.RemindedMask != 0 {
		t.Fatalf("mask after update = %02b, want 0 (the 30-minute reminder was never sent)", next.RemindedMask)
	}
	if got := remind(t, worker, now, next.Start.Add(-31*time.Minute)); got != 0 {
		t.Fatalf("Remind(31 min before) = %d, want 0", got)
	}
	if got := remind(t, worker, now, next.Start.Add(-30*time.Minute)); got != 1 {
		t.Fatalf("Remind(30 min before) = %d, want 1", got)
	}
	if got := remind(t, worker, now, next.Start.Add(-29*time.Minute)); got != 0 {
		t.Fatalf("Remind(again) = %d, want 0", got)
	}
	if next.RemindedMask != 0b01 {
		t.Errorf("mask = %02b, want 01", next.RemindedMask)
	}
	if len(writer.events) != 1 || !slices.Equal(writer.events[0].Recipients, []string{"b"}) || writer.events[0].Payload["lead_minutes"] != float64(30) {
		t.Errorf("reminders = %+v", writer.events)
	}
	if env.remaps != 1 {
		t.Errorf("UpdateRemindedMasks called %d times, want 1", env.remaps)
	}
}

// 报告里的失败场景二：提醒 [30, 1440]，明天那班离开始还有 18 小时，「提前一天」已发（mask=10）；
// 管理员改成 [1440, 30]。不改写的话第 0 位被当成「1440 未发」且已到点——下一轮巡检立即再发一条；
// 第 1 位被当成「30 已发」，30 分钟前那条反而被吞掉。
func TestUpdateRemindMinutesDoesNotRepeatReminder(t *testing.T) {
	t.Parallel()
	env, schedUC, worker, writer, now := newMemScheduleEnv(t, []int32{30, 1440})
	next := env.shifts[1] // 明天 0 点开始的一班
	next.RemindedMask = 0b10

	if _, err := schedUC.Update(context.Background(), memAdmin, "s1", dailyConfigOf(env, 1440, 30)); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if next.RemindedMask != 0b01 {
		t.Fatalf("mask after update = %02b, want 01 (the day-ahead reminder stays sent under its new index)", next.RemindedMask)
	}
	// 改完立刻巡检、以及 30 分钟提醒到点之前：都不该发。
	for _, at := range []time.Time{*now, next.Start.Add(-2 * time.Hour), next.Start.Add(-31 * time.Minute)} {
		if got := remind(t, worker, now, at); got != 0 || len(writer.events) != 0 {
			t.Fatalf("Remind(%s) = %d events %d, want no reminder", at.Format(time.RFC3339), got, len(writer.events))
		}
	}
	if got := remind(t, worker, now, next.Start.Add(-30*time.Minute)); got != 1 {
		t.Fatalf("Remind(30 min before) = %d, want exactly one", got)
	}
	if got := remind(t, worker, now, next.Start.Add(-5*time.Minute)); got != 0 {
		t.Fatalf("Remind(again) = %d, want 0", got)
	}
	if next.RemindedMask != 0b11 {
		t.Errorf("mask = %02b, want 11", next.RemindedMask)
	}
	if len(writer.events) != 1 || !slices.Equal(writer.events[0].Recipients, []string{"b"}) || writer.events[0].Payload["lead_minutes"] != float64(30) {
		t.Errorf("reminders = %+v", writer.events)
	}
}

// 提醒提前量没变（提交顺序不同也算没变）不触碰已提醒标记；只改轮转规则重排时，同一人同一开始
// 时间的班次沿用标记，换了人的不沿用。
func TestUpdateKeepsMasksWhenRemindMinutesUnchanged(t *testing.T) {
	t.Parallel()
	env, schedUC, _, _, _ := newMemScheduleEnv(t, []int32{60, 0})
	env.shifts[1].RemindedMask, env.shifts[2].RemindedMask, env.shifts[3].RemindedMask = 0b10, 0b11, 0b01

	if _, err := schedUC.Update(context.Background(), memAdmin, "s1", dailyConfigOf(env, 0, 60)); err != nil {
		t.Fatalf("Update(same values, other order) error = %v", err)
	}
	if env.remaps != 0 || !slices.Equal(env.sched.RemindMinutes, []int32{60, 0}) ||
		env.shifts[1].RemindedMask != 0b10 || env.shifts[2].RemindedMask != 0b11 || env.shifts[3].RemindedMask != 0b01 {
		t.Fatalf("masks touched: remind %v, remaps %d, masks %d %d %d",
			env.sched.RemindMinutes, env.remaps, env.shifts[1].RemindedMask, env.shifts[2].RemindedMask, env.shifts[3].RemindedMask)
	}

	// 改名单（a 与 c 对调）触发重排：21 日由 a 接手、23 日由 b 接手，都不沿用原值班人的标记；
	// 22 日仍是 c，沿用。
	cfg := dailyConfigOf(env, 60, 0)
	cfg.MemberIDs = []string{"c", "b", "a"}
	if _, err := schedUC.Update(context.Background(), memAdmin, "s1", cfg); err != nil {
		t.Fatalf("Update(rotation) error = %v", err)
	}
	if env.remaps != 0 {
		t.Fatalf("rotation-only change must not remap masks, got %d calls", env.remaps)
	}
	byStart := map[string]*Shift{}
	for _, sh := range env.shifts {
		byStart[sh.Start.Format(time.RFC3339)] = sh
	}
	for day, want := range map[string]struct {
		user string
		mask int32
	}{"2026-11-21T00:00:00Z": {"a", 0}, "2026-11-22T00:00:00Z": {"c", 0b11}, "2026-11-23T00:00:00Z": {"b", 0}} {
		if sh := byStart[day]; sh == nil || sh.UserID != want.user || sh.RemindedMask != want.mask {
			t.Errorf("shift of %s = %+v, want user %s mask %02b", day, sh, want.user, want.mask)
		}
	}
}

// 替班被改派给他人：原替班人此刻本该值班，却既不是原值班人也不是新替班人——同样收到替班通知，
// 变更记录里也记下他。
func TestCreateOverrideNotifiesDisplacedSubstitute(t *testing.T) {
	t.Parallel()
	env, schedUC, _, writer, _ := newMemScheduleEnv(t, []int32{60})
	// 21 日：c 替 b 值班。
	env.shifts[1] = &Shift{
		ID: "ov-c", ScheduleID: "s1", GroupID: "g1", Role: RolePrimary, UserID: "c", OriginUserID: "b",
		Start: env.shifts[1].Start, End: env.shifts[1].End, Source: SourceOverride,
	}
	day := env.shifts[1].Start

	created, err := schedUC.CreateOverride(context.Background(), memAdmin, OverrideInput{
		ScheduleID: "s1", Role: RolePrimary, UserID: "a", Start: day, End: day.Add(24 * time.Hour), Remark: "改派",
	})
	if err != nil {
		t.Fatalf("CreateOverride() error = %v", err)
	}
	if len(created) != 1 || created[0].UserID != "a" || created[0].OriginUserID != "b" {
		t.Fatalf("created = %+v", created)
	}
	if len(writer.events) != 1 {
		t.Fatalf("events = %+v, want exactly one shift.changed event", writer.events)
	}
	e := writer.events[0]
	if !slices.Equal(e.Recipients, []string{"a", "b", "c"}) || !slices.Equal(payloadIDs(e.Payload["displaced_user_ids"]), []string{"c"}) ||
		!slices.Equal(payloadIDs(e.Payload["origin_user_ids"]), []string{"b"}) {
		t.Errorf("event = %+v, want recipients [a b c] with c recorded as displaced", e)
	}
	if len(env.logs) != 1 || env.logs[0].Detail["displaced_user_ids"].([]string)[0] != "c" || len(env.logs[0].Detail["displaced_user_ids"].([]string)) != 1 {
		t.Errorf("change log = %+v", env.logs)
	}

	// 再改回给原替班人 c：被替下的是 a（上一轮的替班人），原值班人仍是 b；同样有人要被通知到。
	writer.events = nil
	if _, err := schedUC.CreateOverride(context.Background(), memAdmin, OverrideInput{
		ScheduleID: "s1", Role: RolePrimary, UserID: "c", Start: day, End: day.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("CreateOverride(back to c) error = %v", err)
	}
	if len(writer.events) != 1 || !slices.Equal(writer.events[0].Recipients, []string{"c", "b", "a"}) ||
		!slices.Equal(payloadIDs(writer.events[0].Payload["displaced_user_ids"]), []string{"a"}) {
		t.Errorf("event = %+v, want a recorded as displaced", writer.events)
	}
}

// 提醒提前量与轮转规则一起改：先按值改写标记、再重排——重排后同一人同一开始时间的班次沿用的是
// 已经对到新下标的标记，而不是旧下标的原值。
func TestUpdateRemapsMasksBeforeReschedule(t *testing.T) {
	t.Parallel()
	env, schedUC, _, _, _ := newMemScheduleEnv(t, []int32{30, 1440})
	env.shifts[1].RemindedMask, env.shifts[2].RemindedMask = 0b10, 0b10 // 「提前一天」已发（旧下标 1）

	cfg := dailyConfigOf(env, 1440, 30)
	cfg.MemberIDs = []string{"c", "b", "a"} // 21 日换成 a，22 日仍是 c
	if _, err := schedUC.Update(context.Background(), memAdmin, "s1", cfg); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	for _, sh := range env.shifts {
		switch sh.Start.Day() {
		case 21:
			if sh.UserID != "a" || sh.RemindedMask != 0 {
				t.Errorf("21st = %s mask %02b, want a with nothing sent", sh.UserID, sh.RemindedMask)
			}
		case 22:
			if sh.UserID != "c" || sh.RemindedMask != 0b01 {
				t.Errorf("22nd = %s mask %02b, want c with the day-ahead reminder kept under index 0", sh.UserID, sh.RemindedMask)
			}
		}
	}
}
