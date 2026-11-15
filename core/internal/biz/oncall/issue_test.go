package biz

import (
	"testing"
	"time"
)

func TestDetectIssues(t *testing.T) {
	t.Parallel()
	d := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	until := d(30, 0)
	sched := func(id string) *Schedule {
		return &Schedule{ID: id, Name: id, GroupID: "g", Status: StatusEnabled, RotationUnit: UnitDay, RotationInterval: 1, AnchorTime: d(1, 0), GeneratedUntil: &until}
	}
	shift := func(schedule string, role int32, user string, from, to time.Time) *Shift {
		return &Shift{ScheduleID: schedule, ScheduleName: schedule, GroupID: "g", Role: role, UserID: user, UserName: user, UserAvailable: true, Start: from, End: to}
	}
	types := func(issues []*Issue) map[int32][]*Issue {
		m := map[int32][]*Issue{}
		for _, is := range issues {
			m[is.Type] = append(m[is.Type], is)
		}
		return m
	}

	t.Run("无人值班", func(t *testing.T) {
		t.Parallel()
		got := types(detectIssues(detectInput{
			Schedules: []*Schedule{sched("s1")},
			Shifts:    []*Shift{shift("s1", RolePrimary, "a", d(10, 0), d(11, 0)), shift("s1", RolePrimary, "b", d(11, 6), d(12, 0))},
			From:      d(10, 0), To: d(12, 0),
		}))
		gaps := got[IssueGap]
		if len(gaps) != 1 || !gaps[0].Start.Equal(d(11, 0)) || !gaps[0].End.Equal(d(11, 6)) {
			t.Errorf("gaps = %+v", gaps)
		}
	})

	t.Run("已排班范围之外与停用的值班表不算空档", func(t *testing.T) {
		t.Parallel()
		short := sched("s1")
		gen := d(11, 0)
		short.GeneratedUntil = &gen
		off := sched("s2")
		off.Status = StatusDisabled
		got := detectIssues(detectInput{
			Schedules: []*Schedule{short, off},
			Shifts:    []*Shift{shift("s1", RolePrimary, "a", d(10, 0), d(11, 0))},
			From:      d(10, 0), To: d(15, 0),
		})
		if len(got) != 0 {
			t.Errorf("issues = %+v", got)
		}
	})

	t.Run("值班人不可用与主备同人", func(t *testing.T) {
		t.Parallel()
		p := shift("s1", RolePrimary, "a", d(10, 0), d(11, 0))
		p.UserAvailable = false
		b := shift("s1", RoleBackup, "a", d(10, 0), d(11, 0))
		b.UserAvailable = false
		got := types(detectIssues(detectInput{Schedules: []*Schedule{sched("s1")}, Shifts: []*Shift{p, b}, From: d(10, 0), To: d(11, 0)}))
		// 同一人主备两个班次的"不可用"合并成一条。
		if len(got[IssueUserUnavailable]) != 1 {
			t.Errorf("unavailable = %+v", got[IssueUserUnavailable])
		}
		if len(got[IssueSamePerson]) != 1 {
			t.Errorf("same person = %+v", got[IssueSamePerson])
		}
	})

	t.Run("跨表撞班两张表各报一条", func(t *testing.T) {
		t.Parallel()
		got := types(detectIssues(detectInput{
			Schedules: []*Schedule{sched("s1"), sched("s2")},
			Shifts: []*Shift{
				shift("s1", RolePrimary, "a", d(10, 0), d(11, 0)),
				shift("s2", RolePrimary, "a", d(10, 12), d(11, 12)),
				shift("s2", RolePrimary, "b", d(10, 0), d(10, 12)),
				shift("s1", RolePrimary, "c", d(11, 0), d(11, 12)),
			},
			From: d(10, 0), To: d(11, 12),
		}))
		clash := got[IssueClash]
		if len(clash) != 2 || !clash[0].Start.Equal(d(10, 12)) || !clash[0].End.Equal(d(11, 0)) {
			t.Fatalf("clash = %+v", clash)
		}
		if clash[0].ScheduleID == clash[1].ScheduleID || clash[0].OtherScheduleName == clash[0].ScheduleName {
			t.Errorf("clash sides = %+v %+v", clash[0], clash[1])
		}
	})

	t.Run("一人同时排在多张表上按班次聚合而不是按对", func(t *testing.T) {
		t.Parallel()
		var schedules []*Schedule
		var shifts []*Shift
		for _, id := range []string{"s1", "s2", "s3", "s4"} {
			schedules = append(schedules, sched(id))
			shifts = append(shifts, shift(id, RolePrimary, "a", d(10, 0), d(11, 0)))
		}
		clash := types(detectIssues(detectInput{Schedules: schedules, Shifts: shifts, From: d(10, 0), To: d(11, 0)}))[IssueClash]
		if len(clash) != 4 {
			t.Fatalf("got %d clash issues, want 4 (one per schedule)", len(clash))
		}
		for _, is := range clash {
			if is.OtherScheduleCount != 3 || is.OtherScheduleName == is.ScheduleName {
				t.Errorf("clash = %+v, want 3 other schedules", is)
			}
		}
	})

	t.Run("连续值班过长", func(t *testing.T) {
		t.Parallel()
		// 按天轮转：同一人连续 3 天（> 2 个周期）才报；恰好 2 天不报。
		three := []*Shift{
			shift("s1", RolePrimary, "a", d(10, 0), d(11, 0)), shift("s1", RolePrimary, "a", d(11, 0), d(12, 0)),
			shift("s1", RolePrimary, "a", d(12, 0), d(13, 0)), shift("s1", RolePrimary, "b", d(13, 0), d(15, 0)),
		}
		got := types(detectIssues(detectInput{Schedules: []*Schedule{sched("s1")}, Shifts: three, From: d(12, 0), To: d(15, 0)}))
		streak := got[IssueLongStreak]
		if len(streak) != 1 || streak[0].UserID != "a" || !streak[0].Start.Equal(d(10, 0)) || !streak[0].End.Equal(d(13, 0)) {
			t.Errorf("streak = %+v", streak)
		}
	})
}

func TestPublicLimiter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	l := newPublicLimiter(func() time.Time { return now })
	for range publicFailurePerMinute {
		if !l.allow("1.1.1.1") {
			t.Fatal("blocked too early")
		}
		l.fail("1.1.1.1")
	}
	if l.allow("1.1.1.1") {
		t.Error("should be blocked after too many invalid tokens")
	}
	if !l.allow("2.2.2.2") {
		t.Error("other IPs must not be affected")
	}
	now = now.Add(time.Minute)
	if !l.allow("1.1.1.1") {
		t.Error("should recover in the next window")
	}
}
