package biz

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// 后台任务批量与窗口参数。
const (
	extendBatch   = 200
	reminderBatch = 200
	digestBatch   = 50
	// inspectWindow 巡检向前看的时间。
	inspectWindow = 7 * 24 * time.Hour
)

// WorkerUsecase 值班后台任务：补排班、上班提醒、每日播报、异常巡检。多实例并发安全：
// 补排班靠值班表行锁 + 幂等的"只补不删"；提醒与播报靠 SKIP LOCKED 行锁与同事务置位；
// 巡检通知靠 notify 去重键。因此无需选主。
type WorkerUsecase struct {
	store     Store
	groups    GroupRepo
	schedules ScheduleRepo
	shifts    ShiftRepo
	notify    *Notifier
	now       func() time.Time
}

// NewWorkerUsecase new a WorkerUsecase.
func NewWorkerUsecase(store Store, groups GroupRepo, schedules ScheduleRepo, shifts ShiftRepo, notify *Notifier) *WorkerUsecase {
	return &WorkerUsecase{store: store, groups: groups, schedules: schedules, shifts: shifts, notify: notify, now: clock}
}

// Extend 给已排班范围不足提前天数的值班表补齐班次；单张表失败不影响其它表。
func (uc *WorkerUsecase) Extend(ctx context.Context) (int, error) {
	ids, err := uc.schedules.ListDueExtend(ctx, extendBatch)
	if err != nil {
		return 0, err
	}
	var errs []error
	done := 0
	for _, id := range ids {
		err := uc.store.WithinTx(ctx, func(tx Tx) error {
			s, err := tx.LockSchedule(ctx, id)
			if err != nil {
				return err
			}
			return generate(ctx, tx, s, uc.now(), false, "")
		})
		switch {
		case err == nil:
			done++
		case errors.Is(err, ErrScheduleNotFound): // 期间被删除
		default:
			errs = append(errs, fmt.Errorf("extend schedule %s: %w", id, err))
		}
	}
	return done, errors.Join(errs...)
}

// Remind 发送到点的上班提醒。同一班次同时有多个提醒到点（如临近开始才创建的替班）时只
// 发提前量最小的一条；与上一班次首尾相接的同人班次视为连续值班，不重复提醒。
func (uc *WorkerUsecase) Remind(ctx context.Context) (int, error) {
	sent := 0
	err := uc.store.WithinTx(ctx, func(tx Tx) error {
		now := uc.now()
		due, err := tx.ListDueReminders(ctx, now, reminderBatch)
		if err != nil {
			return err
		}
		for _, d := range due {
			mask, lead, fire := d.Shift.RemindedMask, int32(-1), false
			for i, m := range d.RemindMinutes {
				bit := int32(1) << i
				if mask&bit != 0 || now.Before(d.Shift.Start.Add(-time.Duration(m)*time.Minute)) {
					continue
				}
				mask |= bit
				fire = true
				if lead < 0 || m < lead {
					lead = m
				}
			}
			if !fire {
				continue
			}
			if !d.Continued {
				if _, err := uc.notify.shiftRemind(ctx, tx, d, lead, now, false); err != nil {
					return err
				}
				sent++
			}
			if err := tx.SetRemindedMask(ctx, d.Shift.ID, mask); err != nil {
				return err
			}
		}
		return nil
	})
	return sent, err
}

// Digest 发送到点的每日播报（按各值班表时区判断；当日只播报一次）。
func (uc *WorkerUsecase) Digest(ctx context.Context) (int, error) {
	sent := 0
	err := uc.store.WithinTx(ctx, func(tx Tx) error {
		now := uc.now()
		due, err := tx.ListDueDigests(ctx, now, digestBatch)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			return nil
		}
		current, err := uc.shifts.ListRange(ctx, now, now.Add(time.Second))
		if err != nil {
			return err
		}
		for _, s := range due {
			var primary, backup *Shift
			for _, sh := range current {
				if sh.ScheduleID != s.ID || sh.Start.After(now) {
					continue
				}
				if sh.Role == RolePrimary {
					primary = sh
				} else {
					backup = sh
				}
			}
			if _, err := uc.notify.dailyDigest(ctx, tx, s, primary, backup, now, false); err != nil {
				return err
			}
			if err := tx.MarkDigested(ctx, s.ID, now); err != nil {
				return err
			}
			sent++
		}
		return nil
	})
	return sent, err
}

// Inspect 巡检未来 7 天的排班异常并按分组通知管理员（同一分组每天至多一次）；
// 「连续值班过长」不在通知之列。
func (uc *WorkerUsecase) Inspect(ctx context.Context) (int, error) {
	schedules, err := uc.schedules.List(ctx, "")
	if err != nil {
		return 0, err
	}
	if len(schedules) == 0 {
		return 0, nil
	}
	now := uc.now()
	look := issueLookback(schedules)
	shifts, err := uc.shifts.ListRange(ctx, now.Add(-look), now.Add(inspectWindow+look))
	if err != nil {
		return 0, err
	}
	issues := detectIssues(detectInput{Schedules: schedules, Shifts: shifts, From: now, To: now.Add(inspectWindow)})
	timezones := make(map[string]string, len(schedules))
	for _, s := range schedules {
		timezones[s.ID] = s.Timezone
	}
	byGroup := map[string][]*Issue{}
	var order []string
	for _, is := range issues {
		// 连续值班过长只在日历里提示，不通知管理员（轮转人少时属常态，通知只会成为噪音）。
		if is.Type == IssueLongStreak {
			continue
		}
		if _, ok := byGroup[is.GroupID]; !ok {
			order = append(order, is.GroupID)
		}
		byGroup[is.GroupID] = append(byGroup[is.GroupID], is)
	}
	var errs []error
	for _, groupID := range order {
		admins, err := uc.groups.ListAdminIDs(ctx, groupID)
		if err == nil {
			err = uc.store.WithinTx(ctx, func(tx Tx) error {
				return uc.notify.issueDetected(ctx, tx, groupID, byGroup[groupID], timezones, admins, now)
			})
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("notify issues of group %s: %w", groupID, err))
		}
	}
	return len(order), errors.Join(errs...)
}
