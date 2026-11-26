package biz

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
)

// 后台任务批量与窗口参数。
const (
	extendBatch   = 200
	reminderBatch = 200
	digestBatch   = 50
	// inspectWindow 巡检向前看的时间。
	inspectWindow = 7 * 24 * time.Hour
)

// 已删除值班表的班次清理的分批参数（一轮 = oncall-purge 循环的一次迭代）。
const (
	// shiftPurgeBatch 每条 DELETE 至多删除的班次数：单条语句只锁一小段，不积压 WAL。
	shiftPurgeBatch = 1000
	// shiftPurgeStatements 一轮至多发出的 DELETE 条数，是时间预算之外的第二道闸（语句意外变慢时兜底）。
	shiftPurgeStatements = 100
	// shiftPurgeBudget 一轮用于班次清理的时间预算：删满一批且预算没用完才发下一条。首次启用（或长期停机后）
	// 历史积压可能很大，每轮（每小时）只在这么多时间里分批删，剩下的留给后续轮次，不在一轮里猛删；稳态下第一条
	// 语句就不足一批，预算用不上。
	shiftPurgeBudget = 5 * time.Second
)

// shiftPurgeOpts 班次清理一轮的预算，取值见上。
var shiftPurgeOpts = loop.PurgeOptions{Batch: shiftPurgeBatch, MaxStatements: shiftPurgeStatements, Budget: shiftPurgeBudget}

// WorkerUsecase 值班后台任务：补排班、上班提醒、每日播报、异常巡检、已删除值班表的班次清理。多实例并发安全：
// 补排班靠值班表行锁 + 幂等的"只补不删"；提醒与播报靠 SKIP LOCKED 行锁与同事务置位；
// 巡检通知靠 notify 去重键；班次清理是幂等的 DELETE（后到者删到 0 行）。因此无需选主。
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

// perRow 在批处理事务内用保存点隔离一行的处理：fn 出错时回滚到保存点，这一行的写入作废、事务其余部分
// 不受影响（PostgreSQL 里一条语句出错后整个事务进入中止态，不回滚到保存点的话后面的行也都做不成）。
// 返回 rowErr（这一行的失败，调用方记下继续）与 fatal（保存点语句本身失败：事务已不可用，整批放弃）。
func perRow(ctx context.Context, tx Tx, fn func() error) (rowErr, fatal error) {
	if _, err := tx.Exec(ctx, "SAVEPOINT oncall_row"); err != nil {
		return nil, fmt.Errorf("savepoint: %w", err)
	}
	if err := fn(); err != nil {
		if _, rb := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT oncall_row"); rb != nil {
			return nil, fmt.Errorf("rollback to savepoint after %w: %w", err, rb)
		}
		return err, nil
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT oncall_row"); err != nil {
		return nil, fmt.Errorf("release savepoint: %w", err)
	}
	return nil, nil
}

// Remind 发送到点的上班提醒。同一班次同时有多个提醒到点（如临近开始才创建的替班）时只
// 发提前量最小的一条；与上一班次首尾相接的同人班次视为连续值班，不重复提醒。
// 一行失败（如通知事件校验不过）只作废这一行，其余照常提交；失败的行下一轮再试，并以错误返回。
func (uc *WorkerUsecase) Remind(ctx context.Context) (int, error) {
	sent := 0
	var errs []error
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
			rowErr, fatal := perRow(ctx, tx, func() error {
				if !d.Continued {
					if _, err := uc.notify.shiftRemind(ctx, tx, d, lead, now, false); err != nil {
						return err
					}
				}
				return tx.SetRemindedMask(ctx, d.Shift.ID, mask)
			})
			if fatal != nil {
				return fatal
			}
			if rowErr != nil {
				errs = append(errs, fmt.Errorf("remind shift %s: %w", d.Shift.ID, rowErr))
				continue
			}
			if !d.Continued {
				sent++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return sent, errors.Join(errs...)
}

// Digest 发送到点的每日播报（按各值班表时区判断；当日只播报一次）。一张表失败只作废它自己（见 perRow）。
func (uc *WorkerUsecase) Digest(ctx context.Context) (int, error) {
	sent := 0
	var errs []error
	err := uc.store.WithinTx(ctx, func(tx Tx) error {
		now := uc.now()
		due, err := tx.ListDueDigests(ctx, now, digestBatch)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			return nil
		}
		// 经事务连接读：此时事务已持有到期值班表的行锁，再向连接池要第二条连接的话，连接池打满时
		// 各实例 / 各任务会带着行锁互相等连接。查询与 ShiftRepo.ListRange 是同一条语句。
		current, err := tx.ListShiftsRange(ctx, now, now.Add(time.Second))
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
			rowErr, fatal := perRow(ctx, tx, func() error {
				if _, err := uc.notify.dailyDigest(ctx, tx, s, primary, backup, now, false); err != nil {
					return err
				}
				return tx.MarkDigested(ctx, s.ID, now)
			})
			if fatal != nil {
				return fatal
			}
			if rowErr != nil {
				errs = append(errs, fmt.Errorf("digest schedule %s: %w", s.ID, rowErr))
				continue
			}
			sent++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return sent, errors.Join(errs...)
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
	issues := detectIssues(detectInput{Schedules: schedules, Shifts: shifts, From: now, To: now.Add(inspectWindow), Now: now})
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

// PurgeDeletedScheduleShifts 清理所属值班表已删除超过 retention 的班次（retention ≤ 0 = 不清理，不发语句），
// 返回本轮清理的条数。分批：删满一批且预算没用完才发下一条，不足一批 / 条数用尽 / 预算用尽即停，没删完的留给下一轮。
//
// 可删的依据：删除值班表时未开始的班次已当场清除、进行中的已在删除时刻截断，留下的是纯历史班次；值班表没有恢复
// 功能；班次的全部读取路径都只连接未删除的值班表（见 data 层 shiftJoins），这些行不再被任何查询或后台任务读到。
func (uc *WorkerUsecase) PurgeDeletedScheduleShifts(ctx context.Context, retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	return loop.PurgeBatched(ctx, shiftPurgeOpts, func(ctx context.Context) (int, error) {
		var n int
		err := uc.store.WithinTx(ctx, func(tx Tx) error {
			var err error
			if n, err = tx.PurgeDeletedScheduleShifts(ctx, retention, shiftPurgeOpts.Batch); err != nil {
				return err
			}
			return nil
		})
		return n, err
	})
}
