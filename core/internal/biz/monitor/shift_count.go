package biz

import (
	"context"
	"time"
)

const (
	// shiftCountMaxRange 班次告警数的时间范围上限（值班日历至多看两个月）。
	shiftCountMaxRange = 62 * 24 * time.Hour
	// maxShiftCounts 一次返回的班次数上限。
	maxShiftCounts = 500
)

// ShiftAlertCount 一个班次及其告警数。
type ShiftAlertCount struct {
	ShiftSlot
	AlertCount int32
}

// ShiftWindow 统计窗口：用户 + [Start, End)。
type ShiftWindow struct {
	UserID string
	Start  time.Time
	End    time.Time
}

// ShiftAlertCounter 班次告警数读仓储。
type ShiftAlertCounter interface {
	// CountNotified 各窗口内通知到该用户的告警事件数（monitor_notify_log 的接收方含该用户、通知时间在窗口
	// 内；同一事件只计一次），返回值与 windows 逐项对应。一条语句完成。
	CountNotified(ctx context.Context, windows []ShiftWindow) ([]int32, error)
}

// ShiftCountUsecase 值班日历上的班次告警数（只读）。
//
// 本接口免接口授权（值班日历页的用户不一定有监控的菜单），访问控制在这里：调用方须是可用用户，并持有
// 「查询告警事件」或「查询值班班次」两项接口权限之一。
//
// 统计范围按值班表，不按调用方的监控数据范围裁剪：返回的只是每个班次的一个数字，不含任何事件内容；
// 值班域也没有按值班表的可见性控制。这一点与 proto 注释里「只统计调用方数据范围内的事件」不同
// （负责方的取定）。
type ShiftCountUsecase struct {
	perms   Permissions
	oncall  OncallAccess
	dir     Directory
	shifts  Shifts
	counter ShiftAlertCounter
}

// NewShiftCountUsecase new a ShiftCountUsecase.
func NewShiftCountUsecase(perms Permissions, oncall OncallAccess, dir Directory, shifts Shifts, counter ShiftAlertCounter) *ShiftCountUsecase {
	return &ShiftCountUsecase{perms: perms, oncall: oncall, dir: dir, shifts: shifts, counter: counter}
}

// allowed 调用方是否可用用户且持有两项接口权限之一。
func (uc *ShiftCountUsecase) allowed(ctx context.Context, userID string) (bool, error) {
	active, err := uc.dir.ActiveUserIDs(ctx, []string{userID})
	if err != nil || len(active) != 1 {
		return false, err
	}
	ok, err := uc.perms.CanReadAlerts(ctx, userID)
	if err != nil || ok {
		return ok, err
	}
	return uc.oncall.CanViewOncallCalendar(ctx, userID)
}

// List 值班表在 [start, end) 内各班次的告警数：班次与范围的交集内通知到当班人的告警事件数。值班表
// 不存在返回空列表。
func (uc *ShiftCountUsecase) List(ctx context.Context, userID, scheduleID string, start, end time.Time) ([]*ShiftAlertCount, error) {
	ok, err := uc.allowed(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrForbidden
	}
	if !start.Before(end) || end.Sub(start) > shiftCountMaxRange {
		return nil, detail(ErrTimeRangeInvalid, "start_time must be before end_time and the range must not exceed 62 days")
	}
	slots, exists, err := uc.shifts.Shifts(ctx, scheduleID, start, end, maxShiftCounts)
	if err != nil {
		return nil, err
	}
	out := make([]*ShiftAlertCount, 0, len(slots))
	if !exists || len(slots) == 0 {
		return out, nil
	}
	slots = slots[:min(len(slots), maxShiftCounts)]
	windows := make([]ShiftWindow, 0, len(slots))
	for _, s := range slots {
		w := ShiftWindow{UserID: s.UserID, Start: s.Start, End: s.End}
		if w.Start.Before(start) {
			w.Start = start
		}
		if w.End.After(end) {
			w.End = end
		}
		windows = append(windows, w)
	}
	counts, err := uc.counter.CountNotified(ctx, windows)
	if err != nil {
		return nil, err
	}
	for i, s := range slots {
		item := &ShiftAlertCount{ShiftSlot: s}
		if i < len(counts) {
			item.AlertCount = counts[i]
		}
		out = append(out, item)
	}
	return out, nil
}
