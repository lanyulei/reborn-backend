package biz

import (
	"context"
	"log/slog"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// CalendarStore 工作日历快照的写入（monitor_calendar，恒一行）。
type CalendarStore interface {
	// Save 写入快照；snap 为 nil 写空对象（= 没有日历）。内容与已保存的不同才写，并同事务把
	// monitor_cfg_version 的 monitor_calendar 行 +1；返回是否写入。
	Save(ctx context.Context, snap *mc.CalendarSnapshot) (changed bool, err error)
}

// CalendarSyncUsecase 把流程域的工作日历物化到 monitor_calendar（后台循环 monitor-calendar）：规则生效
// 时间的「仅工作时间 / 仅非工作时间」由评估引擎按这份快照判定。多实例各自运行也安全：写入是「内容不同
// 才更新」的单条语句，相同的内容不会重复写、不会重复抬版本号。
type CalendarSyncUsecase struct {
	src   CalendarSource
	store CalendarStore
	log   *slog.Logger
}

// NewCalendarSyncUsecase new a CalendarSyncUsecase.
func NewCalendarSyncUsecase(src CalendarSource, store CalendarStore, logger *slog.Logger) *CalendarSyncUsecase {
	return &CalendarSyncUsecase{src: src, store: store, log: logger}
}

// Run 同步一次。流程域没有日历、或日历通不过 monitorcontract 的校验时写空对象（引擎按「没有日历」处理）。
func (uc *CalendarSyncUsecase) Run(ctx context.Context) (int, error) {
	snap, ok, err := uc.src.WorkCalendar(ctx)
	if err != nil {
		return 0, err
	}
	var save *mc.CalendarSnapshot
	if ok {
		if _, err := snap.Compile(); err == nil {
			save = &snap
		} else {
			uc.log.WarnContext(ctx, "monitor calendar sync: the workflow calendar is not usable, storing an empty snapshot", "error", err)
		}
	}
	changed, err := uc.store.Save(ctx, save)
	if err != nil {
		return 0, err
	}
	if changed {
		uc.log.InfoContext(ctx, "monitor calendar snapshot updated", "empty", save == nil)
	}
	return 0, nil
}
