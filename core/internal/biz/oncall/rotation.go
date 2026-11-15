package biz

import (
	"slices"
	"time"
)

// Rotation 轮转规则：从 Anchor 起每 StepDays 天交接一次，第 k 个周期的主值班为
// Members[(Offset+k) mod n]，备值班为其下一位。周期边界按 Loc 的本地时间逐日累加，
// 夏令时切换时交接时刻不漂移。排班只覆盖 Anchor 之后。
type Rotation struct {
	Loc      *time.Location
	StepDays int
	Anchor   time.Time
	Offset   int
	Members  []string
	Backup   bool
}

// Slot 一段待排的值班时间。
type Slot struct {
	Role   int32
	UserID string
	Start  time.Time
	End    time.Time
}

// Period 一个轮转周期的名义时长（用于连续值班判定与查询回看窗口）。
func (r Rotation) Period() time.Duration {
	return time.Duration(r.StepDays) * 24 * time.Hour
}

// boundary 第 k 个周期的起点。
func (r Rotation) boundary(k int) time.Time {
	a := r.Anchor.In(r.Loc)
	return time.Date(a.Year(), a.Month(), a.Day()+k*r.StepDays, a.Hour(), a.Minute(), a.Second(), 0, r.Loc)
}

// index 返回包含 t 的周期序号（t 不早于 Anchor）。先按名义时长估算，再按真实边界
// 校正（夏令时使个别周期多/少一小时）。
func (r Rotation) index(t time.Time) int {
	k := max(int(t.Sub(r.Anchor)/r.Period()), 0)
	for k > 0 && r.boundary(k).After(t) {
		k--
	}
	for !r.boundary(k + 1).After(t) {
		k++
	}
	return k
}

// Plan 计算与 [from, to) 相交的全部周期的值班安排：首个周期在 from 处截断（不产生
// 已过去的班次），末个周期保持完整。until 为已覆盖到的时刻（末个周期终点；无可排
// 周期时为 to）。名单为空时不产生任何安排。
func (r Rotation) Plan(from, to time.Time) (slots []Slot, until time.Time) {
	n := len(r.Members)
	if from.Before(r.Anchor) {
		from = r.Anchor
	}
	if n == 0 || r.StepDays <= 0 || !from.Before(to) {
		return nil, to
	}
	until = to
	for k := r.index(from); ; k++ {
		start, end := r.boundary(k), r.boundary(k+1)
		if !start.Before(to) {
			break
		}
		if start.Before(from) {
			start = from
		}
		p := (r.Offset + k) % n
		slots = append(slots, Slot{Role: RolePrimary, UserID: r.Members[p], Start: start, End: end})
		if r.Backup {
			slots = append(slots, Slot{Role: RoleBackup, UserID: r.Members[(p+1)%n], Start: start, End: end})
		}
		until = end
	}
	return slots, until
}

// fillSlots 从理想安排中减去已存在班次占用的时间（同角色），返回可直接写入的剩余片段。
// existing 须按开始时间排序。首次发布、每日补排、改配置后重排、撤销替班后恢复共用本函数。
func fillSlots(slots []Slot, existing []*Shift) []Slot {
	out := make([]Slot, 0, len(slots))
	for _, s := range slots {
		cursor := s.Start
		for _, e := range existing {
			if e.Role != s.Role || !e.End.After(cursor) {
				continue
			}
			if !e.Start.Before(s.End) {
				break
			}
			if e.Start.After(cursor) {
				out = append(out, Slot{Role: s.Role, UserID: s.UserID, Start: cursor, End: e.Start})
			}
			cursor = e.End
		}
		if cursor.Before(s.End) {
			out = append(out, Slot{Role: s.Role, UserID: s.UserID, Start: cursor, End: s.End})
		}
	}
	return out
}

// avoidSameUser 保证主备值班人不是同一个人：待写入的片段若与已存在的另一角色班次
// （保留下来的替班、进行中的班次）是同一个人，冲突的那一段改由轮转名单中的下一位承担，
// 其余部分不变。existing 须按开始时间排序；名单不足两人时无人可换，原样返回。
func avoidSameUser(slots []Slot, existing []*Shift, members []string) []Slot {
	if len(members) < 2 {
		return slots
	}
	out := make([]Slot, 0, len(slots))
	for _, s := range slots {
		next := members[(slices.Index(members, s.UserID)+1)%len(members)]
		cursor := s.Start
		for _, e := range existing {
			if e.Role == s.Role || e.UserID != s.UserID || !e.End.After(cursor) {
				continue
			}
			if !e.Start.Before(s.End) {
				break
			}
			if e.Start.After(cursor) {
				out = append(out, Slot{Role: s.Role, UserID: s.UserID, Start: cursor, End: e.Start})
			}
			to := minTime(e.End, s.End)
			out = append(out, Slot{Role: s.Role, UserID: next, Start: maxTime(cursor, e.Start), End: to})
			cursor = to
		}
		if cursor.Before(s.End) {
			out = append(out, Slot{Role: s.Role, UserID: s.UserID, Start: cursor, End: s.End})
		}
	}
	return out
}

// overridePlan 一次替班对班次表的改动：先 Deletes/Updates（缩短或删除被覆盖的班次），
// 再 Inserts（被切开班次的后半段 + 替班班次），顺序不可颠倒（排他约束逐语句校验）。
type overridePlan struct {
	Deletes []string
	Updates []*Shift
	Inserts []*Shift
	// Created 新生成的替班班次（Inserts 的子集）。
	Created []*Shift
	// Origins 被替下的原值班人（去重）。
	Origins []string
}

// planOverride 计算把 [start, end) 内 role 的值班人换成 userID 所需的改动。existing 为
// 该值班表与区间相交的班次（按开始时间排序）。被覆盖的班次按边界切开；替班班次按原
// 班次边界拆分并各自记录原值班人；区间内无人值班的空档直接由替班人填补；原本就是
// 替班人的片段保持不动。没有任何实际变化时 Created 为空。
func planOverride(sched *Schedule, existing []*Shift, role int32, userID string, start, end time.Time, remark string) overridePlan {
	var plan overridePlan
	add := func(from, to time.Time, origin string) {
		if !from.Before(to) {
			return
		}
		if origin == userID {
			origin = ""
		}
		// 与上一段首尾相接且原值班人相同则合并，避免产生无意义的碎片。
		if n := len(plan.Created); n > 0 && plan.Created[n-1].End.Equal(from) && plan.Created[n-1].OriginUserID == origin {
			plan.Created[n-1].End = to
			return
		}
		sh := &Shift{
			ScheduleID: sched.ID, GroupID: sched.GroupID, Role: role, UserID: userID,
			Start: from, End: to, Source: SourceOverride, OriginUserID: origin, Remark: remark,
		}
		plan.Created = append(plan.Created, sh)
		if origin != "" && !slices.Contains(plan.Origins, origin) {
			plan.Origins = append(plan.Origins, origin)
		}
	}
	cursor := start
	var tails []*Shift
	for _, e := range existing {
		if e.Role != role || !e.End.After(start) || !e.Start.Before(end) {
			continue
		}
		add(cursor, e.Start, "")
		from, to := maxTime(e.Start, start), minTime(e.End, end)
		cursor = to
		if e.UserID == userID {
			continue
		}
		origin := e.UserID
		if e.Source == SourceOverride {
			origin = e.OriginUserID
		}
		add(from, to, origin)
		head, tail := e.Start.Before(from), to.Before(e.End)
		switch {
		case head && tail:
			rest := *e
			rest.ID, rest.Start = "", to
			tails = append(tails, &rest)
			cut := *e
			cut.End = from
			plan.Updates = append(plan.Updates, &cut)
		case head:
			cut := *e
			cut.End = from
			plan.Updates = append(plan.Updates, &cut)
		case tail:
			cut := *e
			cut.Start = to
			plan.Updates = append(plan.Updates, &cut)
		default:
			plan.Deletes = append(plan.Deletes, e.ID)
		}
	}
	add(cursor, end, "")
	plan.Inserts = append(tails, plan.Created...)
	return plan
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
