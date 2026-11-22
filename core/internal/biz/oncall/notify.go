package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 值班通知事件：经 NotificationWriter 在业务事务内直插 notify_event（业务提交 = 事件
// 必然落库），由通知管道按 notify 管理面的路由/模板投递。事件落在缺省业务命名空间
// （default，界面上「业务组」留空），路由须建在同一命名空间下并覆盖对应事件类型
// （如 oncall.*）；未配置路由时事件落库后标记为无匹配路由，不影响业务。
const (
	// NotifyBiz 值班事件的业务命名空间：缺省命名空间，与迁移 0055 预置的路由、模板一致。
	NotifyBiz = biznotify.DefaultBizID

	EventShiftRemind   = "oncall.shift.remind"
	EventShiftChanged  = "oncall.shift.changed"
	EventDailyDigest   = "oncall.daily.digest"
	EventIssueDetected = "oncall.issue.detected"

	// maxRecipients 单个事件的显式收件上限（与 notify 入参约束一致），超出则拆成多个事件。
	maxRecipients = 20
	// maxEventTitleBytes 通知事件标题的字节上限（biz/notify maxEventTitleLen：按 len() 即字节计）。
	maxEventTitleBytes = 255
	// severityInfo / severityWarning 事件级别。
	severityInfo    int32 = 1
	severityWarning int32 = 2
	// payloadManual 载荷标记：分组管理员手动发送（数据层据此做发送间隔限制）。
	payloadManual = "manual"
	// localTimeLayout 通知正文里的本地时间格式。
	localTimeLayout = "2006-01-02 15:04"
)

// Notifier 值班通知编排：把业务语义转成通知事件；收件人为用户 ID（管道解析为邮箱）。
type Notifier struct {
	writer biznotify.NotificationWriter
}

// NewNotifier new a Notifier.
func NewNotifier(writer biznotify.NotificationWriter) *Notifier {
	return &Notifier{writer: writer}
}

// emit 直插一个事件；收件人超过上限时拆分（dedupKey 非空时各分片带序号）。
// recipients 为 nil 表示交由路由策略决定收件人；为空切片表示无人可通知，直接跳过。
func (n *Notifier) emit(ctx context.Context, tx txrun.Tx, eventType string, severity int32, title, groupKey, dedupKey string, payload map[string]any, recipients []string) error {
	_, err := n.emitIDs(ctx, tx, eventType, severity, title, groupKey, dedupKey, payload, recipients)
	return err
}

// emitIDs 同 emit，并返回落库事件的 ID（手动发送后据此查询投递结果）。
func (n *Notifier) emitIDs(ctx context.Context, tx txrun.Tx, eventType string, severity int32, title, groupKey, dedupKey string, payload map[string]any, recipients []string) ([]string, error) {
	if recipients != nil && len(recipients) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	title = truncateBytes(title, maxEventTitleBytes) // 通知域按字节限长（title ≤ 255 字节），按字节截且不切开字符
	chunks := [][]string{nil}
	if recipients != nil {
		chunks = slices.Collect(slices.Chunk(recipients, maxRecipients))
	}
	ids := make([]string, 0, len(chunks))
	for i, chunk := range chunks {
		key := dedupKey
		if key != "" && i > 0 {
			key = fmt.Sprintf("%s.%d", dedupKey, i)
		}
		ev, err := n.writer.NotifyEvent(ctx, tx, biznotify.Event{
			BizID: NotifyBiz, EventType: eventType, Severity: severity, Title: title,
			Payload: raw, GroupKey: groupKey, DedupKey: key, Recipients: chunk,
		})
		if err != nil {
			return nil, err
		}
		ids = append(ids, ev.ID)
	}
	return ids, nil
}

// localTime 按值班表时区格式化时间（时区非法时退回 UTC）。
func localTime(tz string, t time.Time) string {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	return t.In(loc).Format(localTimeLayout)
}

// shiftChange 一次替班或撤销替班。
type shiftChange struct {
	Role    int32
	UserID  string
	Origins []string
	Start   time.Time
	End     time.Time
	Remark  string
	Revoked bool
}

// shiftChanged 替班或撤销替班：通知原值班人与替班人。
func (n *Notifier) shiftChanged(ctx context.Context, tx txrun.Tx, s *Schedule, c shiftChange) error {
	title := "值班替班：" + s.Name
	if c.Revoked {
		title = "替班已撤销：" + s.Name
	}
	recipients := []string{c.UserID}
	for _, id := range c.Origins {
		if !slices.Contains(recipients, id) {
			recipients = append(recipients, id)
		}
	}
	return n.emit(ctx, tx, EventShiftChanged, severityInfo, title, "oncall.schedule."+s.ID, "", map[string]any{
		"schedule_id": s.ID, "schedule_name": s.Name, "group_id": s.GroupID, "role": RoleLabel(c.Role),
		"user_id": c.UserID, "origin_user_ids": c.Origins, "revoked": c.Revoked, "remark": c.Remark,
		"start_time": localTime(s.Timezone, c.Start), "end_time": localTime(s.Timezone, c.End), "timezone": s.Timezone,
	}, recipients)
}

// shiftRemind 上班提醒：通知班次的值班人。started 标明提醒发出时班次是否已经开始
// （交接时刻的提醒，或随值班表创建即刻开始的班次）。manual 为分组管理员手动补发
// （内容与自动提醒一致，仅在载荷里留痕）。返回落库事件的 ID。
func (n *Notifier) shiftRemind(ctx context.Context, tx txrun.Tx, d *DueReminder, leadMinutes int32, now time.Time, manual bool) ([]string, error) {
	sh := d.Shift
	payload := map[string]any{
		"schedule_id": sh.ScheduleID, "schedule_name": sh.ScheduleName, "group_id": sh.GroupID, "role": RoleLabel(sh.Role),
		"user_id": sh.UserID, "lead_minutes": leadMinutes, "started": !now.Before(sh.Start),
		"start_time": localTime(d.Timezone, sh.Start), "end_time": localTime(d.Timezone, sh.End), "timezone": d.Timezone,
	}
	if manual {
		payload[payloadManual] = true
	}
	return n.emitIDs(ctx, tx, EventShiftRemind, severityInfo, "值班提醒："+sh.ScheduleName+"（"+RoleLabel(sh.Role)+"）", "oncall.schedule."+sh.ScheduleID, "", payload, []string{sh.UserID})
}

// dailyDigest 每日播报：收件人交由路由决定（通常是群机器人渠道）。manual 同 shiftRemind。
func (n *Notifier) dailyDigest(ctx context.Context, tx txrun.Tx, s *Schedule, primary, backup *Shift, now time.Time, manual bool) ([]string, error) {
	name := func(sh *Shift) string {
		if sh == nil {
			return ""
		}
		return sh.UserName
	}
	title := "今日值班：" + s.Name + " · " + name(primary)
	if primary == nil {
		title = "今日值班：" + s.Name + " · 无人值班"
	}
	payload := map[string]any{
		"schedule_id": s.ID, "schedule_name": s.Name, "group_id": s.GroupID, "group_name": s.GroupName,
		"date": localTime(s.Timezone, now)[:10], "primary": name(primary), "backup": name(backup), "timezone": s.Timezone,
	}
	if primary != nil {
		payload["handover_time"] = localTime(s.Timezone, primary.End)
	}
	if manual {
		payload[payloadManual] = true
	}
	return n.emitIDs(ctx, tx, EventDailyDigest, severityInfo, title, "", "", payload, nil)
}

// issueDetected 巡检发现排班异常：通知分组管理员；同一分组每天只通知一次（去重键含日期）。
// timezones 为 值班表 ID → 时区：异常时间按所属值班表的时区展示。
func (n *Notifier) issueDetected(ctx context.Context, tx txrun.Tx, groupID string, issues []*Issue, timezones map[string]string, admins []string, now time.Time) error {
	const maxListed = 20
	list := make([]map[string]any, 0, min(len(issues), maxListed))
	for _, is := range issues[:min(len(issues), maxListed)] {
		tz := timezones[is.ScheduleID]
		list = append(list, map[string]any{
			"type": IssueLabel(is.Type), "schedule_name": is.ScheduleName, "user_name": is.UserName,
			"start_time": localTime(tz, is.Start), "end_time": localTime(tz, is.End), "timezone": tz,
			"other_schedule_name": is.OtherScheduleName, "other_schedule_count": is.OtherScheduleCount,
		})
	}
	var recipients []string
	if len(admins) > 0 {
		recipients = admins
	}
	return n.emit(ctx, tx, EventIssueDetected, severityWarning, fmt.Sprintf("值班排班异常：未来 7 天发现 %d 项", len(issues)),
		"oncall.issue."+groupID, "oncall.issue."+groupID+"."+now.UTC().Format("20060102"),
		map[string]any{"group_id": groupID, "count": len(issues), "issues": list}, recipients)
}

// IssueLabel 异常类型中文名。
func IssueLabel(typ int32) string {
	switch typ {
	case IssueGap:
		return "无人值班"
	case IssueUserUnavailable:
		return "值班人不可用"
	case IssueSamePerson:
		return "主备同人"
	case IssueClash:
		return "跨表撞班"
	case IssueLongStreak:
		return "连续值班过长"
	}
	return "未知异常"
}

// RoleLabel 角色中文名。
func RoleLabel(role int32) string {
	if role == RoleBackup {
		return "备值班"
	}
	return "主值班"
}
