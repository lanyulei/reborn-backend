package biz

import (
	"slices"
	"testing"
	"time"
)

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %s: %v", name, err)
	}
	return loc
}

func ts(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return v
}

func TestRotationPlanWeekly(t *testing.T) {
	t.Parallel()
	sh := mustLoc(t, "Asia/Shanghai")
	r := Rotation{Loc: sh, StepDays: 7, Anchor: ts(t, "2026-11-16T10:00:00+08:00"), Offset: 1, Members: []string{"a", "b", "c"}, Backup: true}
	// from 落在第 1 个周期中途：首段在 from 处截断，末段保持完整周期。
	from, to := ts(t, "2026-11-25T00:00:00+08:00"), ts(t, "2026-12-08T00:00:00+08:00")
	slots, until := r.Plan(from, to)
	want := []Slot{
		{RolePrimary, "c", from, ts(t, "2026-11-30T10:00:00+08:00")},
		{RoleBackup, "a", from, ts(t, "2026-11-30T10:00:00+08:00")},
		{RolePrimary, "a", ts(t, "2026-11-30T10:00:00+08:00"), ts(t, "2026-12-07T10:00:00+08:00")},
		{RoleBackup, "b", ts(t, "2026-11-30T10:00:00+08:00"), ts(t, "2026-12-07T10:00:00+08:00")},
		{RolePrimary, "b", ts(t, "2026-12-07T10:00:00+08:00"), ts(t, "2026-12-14T10:00:00+08:00")},
		{RoleBackup, "c", ts(t, "2026-12-07T10:00:00+08:00"), ts(t, "2026-12-14T10:00:00+08:00")},
	}
	if len(slots) != len(want) {
		t.Fatalf("got %d slots, want %d: %+v", len(slots), len(want), slots)
	}
	for i, w := range want {
		g := slots[i]
		if g.Role != w.Role || g.UserID != w.UserID || !g.Start.Equal(w.Start) || !g.End.Equal(w.End) {
			t.Errorf("slot %d = %+v, want %+v", i, g, w)
		}
	}
	if !until.Equal(ts(t, "2026-12-14T10:00:00+08:00")) {
		t.Errorf("until = %s", until)
	}
}

// 夏令时切换时交接时刻不漂移：纽约 2026-03-08 进入夏令时，每日 09:00 本地时间交接。
func TestRotationPlanKeepsLocalTimeAcrossDST(t *testing.T) {
	t.Parallel()
	ny := mustLoc(t, "America/New_York")
	r := Rotation{Loc: ny, StepDays: 1, Anchor: time.Date(2026, 3, 6, 9, 0, 0, 0, ny), Members: []string{"a", "b"}}
	slots, _ := r.Plan(time.Date(2026, 3, 6, 9, 0, 0, 0, ny), time.Date(2026, 3, 10, 9, 0, 0, 0, ny))
	if len(slots) != 4 {
		t.Fatalf("got %d slots: %+v", len(slots), slots)
	}
	for i, s := range slots {
		if h := s.Start.In(ny).Hour(); h != 9 {
			t.Errorf("slot %d starts at local hour %d, want 9", i, h)
		}
	}
	// 跨越切换日的那一班只有 23 小时。
	if d := slots[1].End.Sub(slots[1].Start); d != 23*time.Hour {
		t.Errorf("DST day shift lasts %s, want 23h", d)
	}
	// 切换日之后查询：周期序号仍然正确（不因少一小时而错位）。
	after, _ := r.Plan(time.Date(2026, 3, 20, 8, 59, 0, 0, ny), time.Date(2026, 3, 20, 9, 1, 0, 0, ny))
	if len(after) != 2 || after[0].UserID != "b" || after[1].UserID != "a" {
		t.Errorf("after DST got %+v, want b (day 13) then a (day 14)", after)
	}
}

func TestRotationPlanEdges(t *testing.T) {
	t.Parallel()
	r := Rotation{Loc: time.UTC, StepDays: 1, Anchor: ts(t, "2026-11-20T00:00:00Z"), Members: []string{"a"}}
	// 起点在未来：只覆盖起点之后。
	slots, _ := r.Plan(ts(t, "2026-11-18T00:00:00Z"), ts(t, "2026-11-21T00:00:00Z"))
	if len(slots) != 1 || !slots[0].Start.Equal(r.Anchor) {
		t.Errorf("future anchor: %+v", slots)
	}
	// 范围整体早于起点 / 名单为空：无安排，until 为范围终点。
	if s, until := r.Plan(ts(t, "2026-11-01T00:00:00Z"), ts(t, "2026-11-02T00:00:00Z")); len(s) != 0 || !until.Equal(ts(t, "2026-11-02T00:00:00Z")) {
		t.Errorf("before anchor: %+v until %s", s, until)
	}
	r.Members = nil
	if s, _ := r.Plan(ts(t, "2026-11-20T00:00:00Z"), ts(t, "2026-11-22T00:00:00Z")); len(s) != 0 {
		t.Errorf("empty roster: %+v", s)
	}
}

func TestFillSlotsSubtractsExisting(t *testing.T) {
	t.Parallel()
	slot := Slot{RolePrimary, "a", ts(t, "2026-11-20T00:00:00Z"), ts(t, "2026-11-21T00:00:00Z")}
	existing := []*Shift{
		{Role: RolePrimary, Start: ts(t, "2026-11-19T20:00:00Z"), End: ts(t, "2026-11-20T02:00:00Z")}, // 进行中的旧班次
		{Role: RoleBackup, Start: ts(t, "2026-11-20T00:00:00Z"), End: ts(t, "2026-11-21T00:00:00Z")},  // 其它角色不占用
		{Role: RolePrimary, Start: ts(t, "2026-11-20T08:00:00Z"), End: ts(t, "2026-11-20T12:00:00Z")}, // 替班
	}
	got := fillSlots([]Slot{slot}, existing)
	if len(got) != 2 ||
		!got[0].Start.Equal(ts(t, "2026-11-20T02:00:00Z")) || !got[0].End.Equal(ts(t, "2026-11-20T08:00:00Z")) ||
		!got[1].Start.Equal(ts(t, "2026-11-20T12:00:00Z")) || !got[1].End.Equal(ts(t, "2026-11-21T00:00:00Z")) {
		t.Errorf("fill = %+v", got)
	}
	// 完全被占用：不产生片段。
	full := []*Shift{{Role: RolePrimary, Start: slot.Start, End: slot.End}}
	if got := fillSlots([]Slot{slot}, full); len(got) != 0 {
		t.Errorf("fully covered: %+v", got)
	}
}

func TestPlanOverride(t *testing.T) {
	t.Parallel()
	sched := &Schedule{ID: "s", GroupID: "g"}
	d := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	base := func() []*Shift {
		return []*Shift{
			{ID: "1", Role: RolePrimary, UserID: "a", Start: d(20, 0), End: d(21, 0), Source: SourceAuto, RemindedMask: 3},
			{ID: "b1", Role: RoleBackup, UserID: "b", Start: d(20, 0), End: d(21, 0), Source: SourceAuto},
			{ID: "2", Role: RolePrimary, UserID: "b", Start: d(21, 0), End: d(22, 0), Source: SourceAuto},
		}
	}

	t.Run("班次中段替班切成三段", func(t *testing.T) {
		t.Parallel()
		p := planOverride(sched, base(), RolePrimary, "c", d(20, 8), d(20, 12), "看病")
		if len(p.Deletes) != 0 || len(p.Updates) != 1 || len(p.Inserts) != 2 || len(p.Created) != 1 {
			t.Fatalf("plan = %+v", p)
		}
		if u := p.Updates[0]; u.ID != "1" || !u.Start.Equal(d(20, 0)) || !u.End.Equal(d(20, 8)) {
			t.Errorf("head = %+v", u)
		}
		tail := p.Inserts[0]
		if tail.UserID != "a" || tail.Source != SourceAuto || !tail.Start.Equal(d(20, 12)) || !tail.End.Equal(d(21, 0)) || tail.RemindedMask != 3 {
			t.Errorf("tail = %+v", tail)
		}
		c := p.Created[0]
		if c.UserID != "c" || c.OriginUserID != "a" || c.Source != SourceOverride || c.Remark != "看病" || !c.Start.Equal(d(20, 8)) || !c.End.Equal(d(20, 12)) {
			t.Errorf("override = %+v", c)
		}
		if len(p.Origins) != 1 || p.Origins[0] != "a" {
			t.Errorf("origins = %v", p.Origins)
		}
	})

	t.Run("跨越两个班次按原班次边界拆分", func(t *testing.T) {
		t.Parallel()
		p := planOverride(sched, base(), RolePrimary, "c", d(20, 12), d(21, 6), "")
		if len(p.Created) != 2 || p.Created[0].OriginUserID != "a" || p.Created[1].OriginUserID != "b" ||
			!p.Created[0].End.Equal(d(21, 0)) || !p.Created[1].Start.Equal(d(21, 0)) {
			t.Fatalf("created = %+v %+v", p.Created[0], p.Created[1])
		}
		if len(p.Updates) != 2 || !p.Updates[0].End.Equal(d(20, 12)) || !p.Updates[1].Start.Equal(d(21, 6)) {
			t.Errorf("updates = %+v", p.Updates)
		}
	})

	t.Run("整班替换删除原班次", func(t *testing.T) {
		t.Parallel()
		p := planOverride(sched, base(), RolePrimary, "c", d(20, 0), d(21, 0), "")
		if len(p.Deletes) != 1 || p.Deletes[0] != "1" || len(p.Updates) != 0 || len(p.Inserts) != 1 {
			t.Errorf("plan = %+v", p)
		}
	})

	t.Run("替班人本来就在值班则无变化", func(t *testing.T) {
		t.Parallel()
		p := planOverride(sched, base(), RolePrimary, "a", d(20, 2), d(20, 6), "")
		if len(p.Created) != 0 || len(p.Deletes) != 0 || len(p.Updates) != 0 {
			t.Errorf("plan = %+v", p)
		}
	})

	t.Run("再次替班保留最初的原值班人", func(t *testing.T) {
		t.Parallel()
		existing := []*Shift{{ID: "o", Role: RolePrimary, UserID: "c", OriginUserID: "a", Start: d(20, 0), End: d(21, 0), Source: SourceOverride}}
		p := planOverride(sched, existing, RolePrimary, "d", d(20, 0), d(21, 0), "")
		if len(p.Created) != 1 || p.Created[0].OriginUserID != "a" {
			t.Errorf("created = %+v", p.Created)
		}
		// 换回原值班人：不再记录原值班人（自己替自己没有意义）。
		p = planOverride(sched, existing, RolePrimary, "a", d(20, 0), d(21, 0), "")
		if len(p.Created) != 1 || p.Created[0].OriginUserID != "" || len(p.Origins) != 0 {
			t.Errorf("back to origin = %+v origins %v", p.Created, p.Origins)
		}
	})

	t.Run("空档由替班人填补", func(t *testing.T) {
		t.Parallel()
		existing := []*Shift{{ID: "1", Role: RolePrimary, UserID: "a", Start: d(20, 0), End: d(20, 6), Source: SourceAuto}}
		p := planOverride(sched, existing, RolePrimary, "c", d(20, 4), d(20, 10), "")
		if len(p.Created) != 2 || p.Created[0].OriginUserID != "a" || p.Created[1].OriginUserID != "" ||
			!p.Created[1].Start.Equal(d(20, 6)) || !p.Created[1].End.Equal(d(20, 10)) {
			t.Errorf("created = %+v", p.Created)
		}
	})
}

func TestOnHandover(t *testing.T) {
	t.Parallel()
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	// 轮转起点：上海时间周一 10:00。
	s := &Schedule{Timezone: "Asia/Shanghai", AnchorTime: time.Date(2026, 11, 16, 2, 0, 0, 0, time.UTC)}
	for _, tc := range []struct {
		at   time.Time
		want bool
	}{
		{time.Date(2026, 11, 20, 2, 0, 0, 0, time.UTC), true},   // 另一天的 10:00
		{time.Date(2026, 11, 10, 2, 0, 0, 0, time.UTC), true},   // 起点之前的 10:00
		{time.Date(2026, 11, 20, 2, 30, 0, 0, time.UTC), false}, // 10:30
		{time.Date(2026, 11, 19, 16, 0, 0, 0, time.UTC), false}, // 0 点
		{time.Date(2026, 11, 20, 2, 0, 1, 0, time.UTC), false},  // 差 1 秒
	} {
		if got := onHandover(s, loc, tc.at); got != tc.want {
			t.Errorf("onHandover(%s) = %v, want %v", tc.at.Format(time.RFC3339), got, tc.want)
		}
	}
}

func TestAvoidSameUser(t *testing.T) {
	t.Parallel()
	d := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	members := []string{"a", "b", "c"}
	// 20 日：c 替班做主值班（保留）；自动排班想让 c 做 19~22 日的备值班。
	existing := []*Shift{{Role: RolePrimary, UserID: "c", Start: d(20, 2), End: d(21, 2), Source: SourceOverride}}
	got := avoidSameUser([]Slot{{Role: RoleBackup, UserID: "c", Start: d(19, 2), End: d(22, 2)}}, existing, members)
	want := []Slot{
		{Role: RoleBackup, UserID: "c", Start: d(19, 2), End: d(20, 2)},
		{Role: RoleBackup, UserID: "a", Start: d(20, 2), End: d(21, 2)}, // 冲突的一天换成名单里的下一位
		{Role: RoleBackup, UserID: "c", Start: d(21, 2), End: d(22, 2)},
	}
	if !slices.Equal(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	// 同角色、不同人的已有班次不影响；名单只有一人时无人可换。
	same := []Slot{{Role: RolePrimary, UserID: "c", Start: d(19, 2), End: d(22, 2)}}
	if got := avoidSameUser(same, existing, members); !slices.Equal(got, same) {
		t.Errorf("same-role shift should not split: %+v", got)
	}
	one := []Slot{{Role: RoleBackup, UserID: "c", Start: d(19, 2), End: d(22, 2)}}
	if got := avoidSameUser(one, existing, []string{"c"}); !slices.Equal(got, one) {
		t.Errorf("single-member rotation should be left as is: %+v", got)
	}
}

// 替班被改派给他人：被替下的原替班人记入 Displaced（他此刻本该值班，需要被通知到），原值班人
// 仍是最初的那个；只截短、没被换掉的部分不算。
func TestPlanOverrideDisplacesSubstitute(t *testing.T) {
	t.Parallel()
	sched := &Schedule{ID: "s", GroupID: "g"}
	d := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	existing := []*Shift{{ID: "o", Role: RolePrimary, UserID: "y", OriginUserID: "x", Start: d(20, 0), End: d(21, 0), Source: SourceOverride}}

	p := planOverride(sched, existing, RolePrimary, "z", d(20, 0), d(21, 0), "")
	if len(p.Deletes) != 1 || len(p.Created) != 1 || p.Created[0].OriginUserID != "x" {
		t.Fatalf("plan = %+v", p)
	}
	if !slices.Equal(p.Displaced, []string{"y"}) || !slices.Equal(p.Origins, []string{"x"}) {
		t.Errorf("displaced = %v, origins = %v; want [y], [x]", p.Displaced, p.Origins)
	}
	// 只替半天：y 的替班被截短，他仍要知道另外半天不归他了。
	existing = []*Shift{{ID: "o", Role: RolePrimary, UserID: "y", OriginUserID: "x", Start: d(20, 0), End: d(21, 0), Source: SourceOverride}}
	p = planOverride(sched, existing, RolePrimary, "z", d(20, 0), d(20, 12), "")
	if !slices.Equal(p.Displaced, []string{"y"}) || len(p.Updates) != 1 || len(p.Deletes) != 0 {
		t.Errorf("half day: %+v", p)
	}
	// 被覆盖的是自动班次、或替班人本来就在值班：没有改派谁。
	existing = []*Shift{{ID: "1", Role: RolePrimary, UserID: "a", Start: d(20, 0), End: d(21, 0), Source: SourceAuto}}
	if p = planOverride(sched, existing, RolePrimary, "c", d(20, 8), d(20, 12), ""); len(p.Displaced) != 0 {
		t.Errorf("auto shift: displaced = %v", p.Displaced)
	}
	if p = planOverride(sched, existing, RolePrimary, "a", d(20, 8), d(20, 12), ""); len(p.Displaced) != 0 || len(p.Created) != 0 {
		t.Errorf("same substitute: %+v", p)
	}
}

// 夏令时拨快当天被跳过的本地时刻（纽约 2026-03-08 没有 02:30）：班次边界落在归一化后的 01:30 EST。
// 按钟面比对会把这个真实边界判成「不在交接时刻上」，这一天的替班就创建不了。
func TestOnHandoverDSTNonexistentLocalTime(t *testing.T) {
	t.Parallel()
	ny := mustLoc(t, "America/New_York")
	anchor := time.Date(2026, 1, 1, 2, 30, 0, 0, ny)
	s := &Schedule{AnchorTime: anchor}
	rot := Rotation{Loc: ny, StepDays: 1, Anchor: anchor}
	gapDay := rot.boundary(66) // 2026-03-08
	if want := time.Date(2026, 3, 8, 6, 30, 0, 0, time.UTC); !gapDay.Equal(want) {
		t.Fatalf("boundary on the spring-forward day = %s, want %s", gapDay.UTC(), want)
	}
	for _, tc := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"拨快当天的真实边界", gapDay, true},
		{"前一天", rot.boundary(65), true},
		{"后一天", rot.boundary(67), true},
		{"拨快后的 03:30（钟面上不是交接时刻）", gapDay.Add(time.Hour), false},
		{"当天 00:30", gapDay.Add(-90 * time.Minute), false},
		{"差一秒", gapDay.Add(time.Second), false},
	} {
		if got := onHandover(s, ny, tc.at); got != tc.want {
			t.Errorf("%s: onHandover(%s) = %v, want %v", tc.name, tc.at.UTC(), got, tc.want)
		}
	}
	// 周轮转也按天判定：交接时刻落在星期的中间那天同样可以替班（替整数天，不是替整周）。
	weekly := &Schedule{AnchorTime: anchor, RotationUnit: UnitWeek, RotationInterval: 1}
	if !onHandover(weekly, ny, rot.boundary(70)) {
		t.Error("a mid-week handover time must still be accepted for a weekly rotation")
	}
}
