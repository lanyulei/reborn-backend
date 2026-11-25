package biz

import (
	"context"
	"errors"
	"time"
)

// DutyUsecase 向其他域回答「某张值班表此刻谁在值班」（流程引擎的值班人规则用）。只读、
// 不做权限判定：调用方是系统内部的流程流转，不是某个登录用户。
type DutyUsecase struct {
	schedules ScheduleRepo
	shifts    ShiftRepo
}

// NewDutyUsecase new a DutyUsecase.
func NewDutyUsecase(schedules ScheduleRepo, shifts ShiftRepo) *DutyUsecase {
	return &DutyUsecase{schedules: schedules, shifts: shifts}
}

// OnDuty 值班表 scheduleID 在 at 时刻的值班人：主值班在前、备值班在后（替班的取替班人）。
// 值班表不存在、已停用或那一刻没有排班返回空，不报错——由调用方按「没有处理人」处理。
func (uc *DutyUsecase) OnDuty(ctx context.Context, scheduleID string, at time.Time) ([]string, error) {
	s, err := uc.schedules.Get(ctx, scheduleID)
	if errors.Is(err, ErrScheduleNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if s.Status != StatusEnabled {
		return nil, nil
	}
	// 班次区间左闭右开：取覆盖 at 的那一班（交班时刻归新的一班）。
	// 只取这张值班表的班次（谓词下推）；scheduleID 已经过上面的 Get，下方的逐行判定原样保留。
	shifts, err := uc.shifts.ListRangeScoped(ctx, at, at.Add(time.Second), ShiftScope{ScheduleID: scheduleID})
	if err != nil {
		return nil, err
	}
	var primary, backup string
	for _, sh := range shifts {
		if sh.ScheduleID != scheduleID || sh.Start.After(at) || !sh.End.After(at) {
			continue
		}
		if sh.Role == RolePrimary {
			primary = sh.UserID
		} else {
			backup = sh.UserID
		}
	}
	users := make([]string, 0, 2)
	for _, id := range []string{primary, backup} {
		if id != "" {
			users = append(users, id)
		}
	}
	return users, nil
}
