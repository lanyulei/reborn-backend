package biz

import (
	"cmp"
	"slices"
	"time"
)

// 排班异常类型（与 proto ShiftIssue.type 一致）。
const (
	IssueGap             int32 = 1 // 无人值班
	IssueUserUnavailable int32 = 2 // 值班人不可用
	IssueSamePerson      int32 = 3 // 主备同人
	IssueClash           int32 = 4 // 跨表撞班
	IssueLongStreak      int32 = 5 // 连续值班过长
)

// longStreakPeriods 同一人连续担任主值班超过该周期数即判为过长。
const longStreakPeriods = 2

// maxIssues 单次返回的异常条数上限（按开始时间取最早的）：异常本应是少数，数据异常
// （如同一人被排进几十张值班表）时不让响应体无界增长。
const maxIssues = 300

// capIssues 截断到 maxIssues 条（入参已按开始时间排序）。
func capIssues(issues []*Issue) []*Issue {
	if len(issues) > maxIssues {
		return issues[:maxIssues]
	}
	return issues
}

// Issue 一条排班异常。
type Issue struct {
	Type         int32
	ScheduleID   string
	ScheduleName string
	GroupID      string
	UserID       string
	UserName     string
	Start        time.Time
	End          time.Time
	// OtherScheduleName 跨表撞班时的对方值班表（多张时为其中一张）；OtherScheduleCount 为对方表数。
	OtherScheduleName  string
	OtherScheduleCount int32
}

// detectInput 异常检测输入：Shifts 须覆盖 [From-回看, To+回看) 且按开始时间排序
// （回看窗口用于识别跨越查询边界的连续值班），只报告与 [From, To) 相交的异常。
type detectInput struct {
	Schedules []*Schedule
	Shifts    []*Shift
	From      time.Time
	To        time.Time
}

// detectIssues 检测五类排班异常；只检查启用的值班表。结果按开始时间、类型排序。
func detectIssues(in detectInput) []*Issue {
	if !in.From.Before(in.To) {
		return nil
	}
	active := make(map[string]*Schedule, len(in.Schedules))
	for _, s := range in.Schedules {
		if s.Status == StatusEnabled {
			active[s.ID] = s
		}
	}
	bySchedule := make(map[string][]*Shift, len(active))
	primaryByUser := make(map[string][]*Shift)
	for _, sh := range in.Shifts {
		if _, ok := active[sh.ScheduleID]; !ok {
			continue
		}
		bySchedule[sh.ScheduleID] = append(bySchedule[sh.ScheduleID], sh)
		if sh.Role == RolePrimary {
			primaryByUser[sh.UserID] = append(primaryByUser[sh.UserID], sh)
		}
	}
	var issues []*Issue
	report := func(typ int32, s *Schedule, sh *Shift, start, end time.Time, other string) {
		if !start.Before(in.To) || !end.After(in.From) {
			return
		}
		is := &Issue{Type: typ, ScheduleID: s.ID, ScheduleName: s.Name, GroupID: s.GroupID, Start: start, End: end, OtherScheduleName: other}
		if sh != nil {
			is.UserID, is.UserName = sh.UserID, sh.UserName
		}
		issues = append(issues, is)
	}

	for _, s := range in.Schedules {
		if _, ok := active[s.ID]; !ok {
			continue
		}
		shifts := bySchedule[s.ID]

		// 1 无人值班：已排班范围内主值班未覆盖的时间。
		from, to := maxTime(in.From, s.AnchorTime), in.To
		if s.GeneratedUntil != nil {
			to = minTime(to, *s.GeneratedUntil)
		}
		cursor := from
		for _, sh := range shifts {
			if sh.Role != RolePrimary || !sh.End.After(cursor) {
				continue
			}
			if !sh.Start.Before(to) {
				break
			}
			if sh.Start.After(cursor) {
				report(IssueGap, s, nil, cursor, sh.Start, "")
			}
			cursor = sh.End
		}
		if cursor.Before(to) {
			report(IssueGap, s, nil, cursor, to, "")
		}

		// 2 值班人不可用；5 连续值班过长——都按"同一人首尾相接的班次"合并后判定。
		period := time.Duration(rotationStepDays(s)) * 24 * time.Hour
		for _, role := range []int32{RolePrimary, RoleBackup} {
			var run []*Shift
			flush := func() {
				if len(run) == 0 {
					return
				}
				first, last := run[0], run[len(run)-1]
				if !first.UserAvailable {
					report(IssueUserUnavailable, s, first, maxTime(first.Start, in.From), last.End, "")
				}
				if role == RolePrimary && last.End.Sub(first.Start) > longStreakPeriods*period {
					report(IssueLongStreak, s, first, first.Start, last.End, "")
				}
				run = run[:0]
			}
			for _, sh := range shifts {
				if sh.Role != role {
					continue
				}
				if n := len(run); n > 0 && (run[n-1].UserID != sh.UserID || !run[n-1].End.Equal(sh.Start)) {
					flush()
				}
				run = append(run, sh)
			}
			flush()
		}

		// 3 主备同人：同一时间主值班与备值班是同一个人。
		for _, p := range shifts {
			if p.Role != RolePrimary {
				continue
			}
			for _, b := range shifts {
				if b.Role != RoleBackup || b.UserID != p.UserID {
					continue
				}
				if start, end := maxTime(p.Start, b.Start), minTime(p.End, b.End); start.Before(end) {
					report(IssueSamePerson, s, p, start, end, "")
				}
			}
		}
	}

	// 4 跨表撞班：同一人同一时间在多张值班表担任主值班。按班次聚合——每个撞班的班次
	// 只报一条（记下首张对方值班表与对方表数），而不是每对值班表各报一条：一个人同时
	// 排在 N 张表上时，按对报告是 N² 量级。
	type clash struct {
		start, end time.Time
		others     []string
	}
	for _, list := range primaryByUser {
		hits := make(map[*Shift]*clash)
		note := func(sh, other *Shift, start, end time.Time) {
			c := hits[sh]
			if c == nil {
				c = &clash{start: start, end: end}
				hits[sh] = c
			}
			c.start, c.end = minTime(c.start, start), maxTime(c.end, end)
			if !slices.Contains(c.others, other.ScheduleName) {
				c.others = append(c.others, other.ScheduleName)
			}
		}
		for i, a := range list {
			for _, b := range list[i+1:] {
				if !b.Start.Before(a.End) {
					break
				}
				if a.ScheduleID == b.ScheduleID {
					continue
				}
				if start, end := maxTime(a.Start, b.Start), minTime(a.End, b.End); start.Before(end) {
					note(a, b, start, end)
					note(b, a, start, end)
				}
			}
		}
		for _, sh := range list {
			if c := hits[sh]; c != nil {
				before := len(issues)
				report(IssueClash, active[sh.ScheduleID], sh, c.start, c.end, c.others[0])
				if len(issues) > before {
					issues[before].OtherScheduleCount = int32(len(c.others)) //nolint:gosec // 值班表数量有界
				}
			}
		}
	}

	slices.SortStableFunc(issues, func(a, b *Issue) int {
		return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Type, b.Type), cmp.Compare(a.ScheduleID, b.ScheduleID))
	})
	return mergeIssues(issues)
}

// mergeIssues 合并同类、同表、同人且首尾相接或重叠的异常（如跨多个班次的主备同人）。
func mergeIssues(issues []*Issue) []*Issue {
	out := make([]*Issue, 0, len(issues))
	last := make(map[string]*Issue, len(issues))
	for _, is := range issues {
		key := string(rune('0'+is.Type)) + "|" + is.ScheduleID + "|" + is.UserID + "|" + is.OtherScheduleName
		if prev, ok := last[key]; ok && !is.Start.After(prev.End) {
			prev.End = maxTime(prev.End, is.End)
			prev.OtherScheduleCount = max(prev.OtherScheduleCount, is.OtherScheduleCount)
			continue
		}
		last[key] = is
		out = append(out, is)
	}
	return out
}

// rotationStepDays 一个轮转周期的天数。
func rotationStepDays(s *Schedule) int {
	if s.RotationUnit == UnitWeek {
		return int(s.RotationInterval) * 7
	}
	return int(s.RotationInterval)
}

// issueLookback 异常检测需要向查询范围两侧多读的时间：连续值班判定要看到完整的
// 2 个周期以上（取全部值班表中最长周期）。
func issueLookback(schedules []*Schedule) time.Duration {
	days := 1
	for _, s := range schedules {
		days = max(days, rotationStepDays(s))
	}
	return time.Duration(longStreakPeriods*days+1) * 24 * time.Hour
}
