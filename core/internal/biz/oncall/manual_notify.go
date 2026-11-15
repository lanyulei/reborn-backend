package biz

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// 手动发送值班通知的参数。
const (
	// NotifyKindRemind / NotifyKindDigest 通知类型：上班提醒 / 每日播报。
	NotifyKindRemind int32 = 1
	NotifyKindDigest int32 = 2
	// NotifyTargetCurrent / NotifyTargetNext 上班提醒针对的班次：当前 / 下一个。
	NotifyTargetCurrent int32 = 1
	NotifyTargetNext    int32 = 2

	// ActionScheduleNotify 变更记录操作码：手动发送值班通知。
	ActionScheduleNotify = "schedule.notify"
	// manualNotifyInterval 同一张值班表、同一种通知两次手动发送的最小间隔（防连点、防刷）。
	manualNotifyInterval = time.Minute
	// maxNotifyResultEvents 一次查询投递结果的事件数上限。
	maxNotifyResultEvents = 10
)

// ErrNotifyThrottled 手动发送过于频繁（429）；reason 与全局限流中间件同口径。
var ErrNotifyThrottled = errors.New(429, "RATELIMIT", "this notification was sent less than a minute ago, please try again later")

// ManualNotifyInput 手动发送值班通知的入参。
type ManualNotifyInput struct {
	ScheduleID string
	Kind       int32
	Target     int32
}

// ManualNotifyResult 手动发送的结果：事件 ID 用于查询投递结果；Recipients 为上班提醒的
// 收件班次（每日播报的收件人由路由决定，为空）。
type ManualNotifyResult struct {
	EventIDs   []string
	Recipients []*Shift
}

// NotifyDelivery 一次投递（渠道 → 收件地址）。
type NotifyDelivery struct {
	ChannelName string
	ChannelType string
	Recipient   string
	Status      int32
	Error       string
}

// NotifyResult 一个通知事件的投递结果。
type NotifyResult struct {
	EventID    string
	EventType  string
	Status     int32
	Deliveries []*NotifyDelivery
}

// SendNotification 手动发送值班通知：自动通知没有发出时由分组管理员补发。内容、事件类型、
// 路由与自动通知完全一致；不改动自动提醒的已提醒标记，也不占用当日播报——宁可重复，
// 不让手动操作造成自动通知漏发。同一张值班表、同一种通知每分钟至多一次，并写入变更记录。
func (uc *ScheduleUsecase) SendNotification(ctx context.Context, actor Actor, in ManualNotifyInput) (*ManualNotifyResult, error) {
	cur, err := uc.schedules.Get(ctx, in.ScheduleID)
	if err != nil {
		return nil, err
	}
	out := &ManualNotifyResult{}
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, cur.GroupID); err != nil {
			return err
		}
		// 值班表行锁：并发的手动发送串行化，间隔校验不会被同时到达的请求绕过。
		s, err := tx.LockSchedule(ctx, in.ScheduleID)
		if err != nil {
			return err
		}
		if s.Status != StatusEnabled {
			return detail(ErrScheduleInvalid, "schedule is disabled")
		}
		eventType := EventShiftRemind
		if in.Kind == NotifyKindDigest {
			eventType = EventDailyDigest
		} else if in.Kind != NotifyKindRemind {
			return detail(ErrScheduleInvalid, "unknown notification kind %d", in.Kind)
		}
		now := uc.now()
		recent, err := tx.HasRecentManualNotify(ctx, s.ID, eventType, now.Add(-manualNotifyInterval))
		if err != nil {
			return err
		}
		if recent {
			return ErrNotifyThrottled
		}
		shifts, err := tx.ListShiftsFrom(ctx, s.ID, now)
		if err != nil {
			return err
		}
		logDetail := map[string]any{"kind": in.Kind}
		summary := "手动发送每日播报：" + s.Name
		if in.Kind == NotifyKindDigest {
			primary, backup := pickShift(shifts, RolePrimary, NotifyTargetCurrent, now), pickShift(shifts, RoleBackup, NotifyTargetCurrent, now)
			// 读路径才带分组名；播报正文需要它。
			s.GroupName = cur.GroupName
			if out.EventIDs, err = uc.notify.dailyDigest(ctx, tx, s, primary, backup, now, true); err != nil {
				return err
			}
		} else {
			target := in.Target
			if target != NotifyTargetNext {
				target = NotifyTargetCurrent
			}
			var userIDs []string
			for _, role := range []int32{RolePrimary, RoleBackup} {
				sh := pickShift(shifts, role, target, now)
				// 不可用（停用 / 离职）的值班人不发：发了也到不了，还会让结果里多一条失败。
				if sh == nil || !sh.UserAvailable {
					continue
				}
				lead := int32(0)
				if sh.Start.After(now) {
					lead = int32(min(sh.Start.Sub(now)/time.Minute, 7*24*60)) //nolint:gosec // 已封顶 7 天
				}
				sh.ScheduleName = s.Name
				ids, err := uc.notify.shiftRemind(ctx, tx, &DueReminder{Shift: sh, Timezone: s.Timezone}, lead, now, true)
				if err != nil {
					return err
				}
				out.EventIDs = append(out.EventIDs, ids...)
				out.Recipients = append(out.Recipients, sh)
				userIDs = append(userIDs, sh.UserID)
			}
			if len(out.Recipients) == 0 {
				if target == NotifyTargetNext {
					return detail(ErrScheduleInvalid, "there is no upcoming shift with an available person to remind")
				}
				return detail(ErrScheduleInvalid, "nobody available is on duty right now")
			}
			summary = "手动发送上班提醒：" + s.Name
			logDetail["target"], logDetail["user_ids"] = target, userIDs
		}
		logDetail["event_ids"] = out.EventIDs
		return writeLog(ctx, tx, actor, s.GroupID, s.ID, ActionScheduleNotify, summary, logDetail)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// pickShift 在按开始时间排序、均未结束的班次里取某角色的当前班次（已开始）或下一个班次
// （最早的未开始班次）；没有返回 nil。
func pickShift(shifts []*Shift, role, target int32, now time.Time) *Shift {
	for _, sh := range shifts {
		if sh.Role != role {
			continue
		}
		started := !sh.Start.After(now)
		if (target == NotifyTargetCurrent) == started {
			return sh
		}
	}
	return nil
}

// NotificationResults 查询手动发送的值班通知的投递结果。只有分组管理员（或超级管理员）
// 可查，且只返回属于该值班表的值班事件——事件 ID 由调用方传入，不能借此读取其它通知。
func (uc *ScheduleUsecase) NotificationResults(ctx context.Context, actor Actor, scheduleID string, eventIDs []string) ([]*NotifyResult, error) {
	if len(eventIDs) == 0 || len(eventIDs) > maxNotifyResultEvents {
		return nil, detail(ErrScheduleInvalid, "between 1 and %d event ids are required", maxNotifyResultEvents)
	}
	s, err := uc.schedules.Get(ctx, scheduleID)
	if err != nil {
		return nil, err
	}
	g, err := uc.groups.Get(ctx, s.GroupID, actor.UserID)
	if err != nil {
		return nil, err
	}
	if !CanManage(actor, g) {
		return nil, ErrForbidden
	}
	return uc.schedules.ListNotifyResults(ctx, s.ID, eventIDs)
}

// 值班通知的类别（配置状态检查用）。
const (
	// NotifyClassPersonal 个人通知：上班提醒 / 替班变更 / 排班异常（走同一条规则）。
	NotifyClassPersonal int32 = 1
	// NotifyClassDigest 每日播报。
	NotifyClassDigest int32 = 2
)

// NotifyRouteStatus 一类值班通知在通知规则里的配置情况。
type NotifyRouteStatus struct {
	Class     int32
	EventType string
	// RouteName 命中的规则；没有命中为空。
	RouteName string
	// ChannelCount 规则上已启用的渠道数；0 = 发不出去。
	ChannelCount int32
}

// NotificationStatus 值班通知是否配通：各类通知按代表性的事件类型在通知规则里试算一次
// （只读）。只回答「有没有规则、规则上有没有可用渠道」，不涉及规则内容与渠道凭据。
func (uc *ScheduleUsecase) NotificationStatus(ctx context.Context) ([]*NotifyRouteStatus, error) {
	out := []*NotifyRouteStatus{
		{Class: NotifyClassPersonal, EventType: EventShiftRemind},
		{Class: NotifyClassDigest, EventType: EventDailyDigest},
	}
	for _, st := range out {
		name, channels, err := uc.schedules.MatchNotifyRoute(ctx, st.EventType, severityInfo)
		if err != nil {
			return nil, err
		}
		st.RouteName, st.ChannelCount = name, channels
	}
	return out, nil
}
