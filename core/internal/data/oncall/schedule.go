package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

var (
	_ biz.ScheduleRepo = (*scheduleRepo)(nil)
	_ biz.ShiftRepo    = (*shiftRepo)(nil)
)

// scheduleSelectColumns 值班表查询列序（别名 sc）；轮转名单按 sort 聚合为数组。
const scheduleSelectColumns = `sc.id::text, sc.group_id::text,
	COALESCE((SELECT g.name FROM oncall_group g WHERE g.id = sc.group_id), ''),
	sc.name, sc.description, sc.timezone, sc.rotation_unit, sc.rotation_interval, sc.anchor_time, sc.anchor_offset,
	sc.backup_enabled, sc.horizon_days, sc.remind_minutes, COALESCE(to_char(sc.digest_time, 'HH24:MI'), ''),
	sc.status, sc.generated_until,
	ARRAY(SELECT sm.user_id::text FROM oncall_schedule_member sm WHERE sm.schedule_id = sc.id ORDER BY sm.sort),
	sc.create_time, sc.update_time`

func scanSchedule(scan func(dest ...any) error) (*biz.Schedule, error) {
	var s biz.Schedule
	if err := scan(&s.ID, &s.GroupID, &s.GroupName, &s.Name, &s.Description, &s.Timezone, &s.RotationUnit, &s.RotationInterval,
		&s.AnchorTime, &s.AnchorOffset, &s.BackupEnabled, &s.HorizonDays, &s.RemindMinutes, &s.DigestTime,
		&s.Status, &s.GeneratedUntil, &s.MemberIDs, &s.CreateTime, &s.UpdateTime); err != nil {
		return nil, err
	}
	return &s, nil
}

// collectSchedules 读取结果集并补齐轮转名单的用户展示信息（一次批量查询）。
func collectSchedules(ctx context.Context, q querier, rows pgx.Rows) ([]*biz.Schedule, error) {
	items := make([]*biz.Schedule, 0, 8)
	var ids []string
	for rows.Next() {
		s, err := scanSchedule(rows.Scan)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan oncall schedule: %w", err)
		}
		items = append(items, s)
		ids = append(ids, s.MemberIDs...)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read oncall schedules: %w", err)
	}
	users, err := lookupUsers(ctx, q, ids)
	if err != nil {
		return nil, err
	}
	for _, s := range items {
		s.Members = make([]*biz.User, 0, len(s.MemberIDs))
		for _, id := range s.MemberIDs {
			u := users[id]
			if u == nil {
				u = &biz.User{ID: id} // 用户已被删除：保留占位，标记不可用
			}
			s.Members = append(s.Members, u)
		}
	}
	return items, nil
}

type scheduleRepo struct {
	data *basedata.Data
}

// NewScheduleRepo new a scheduleRepo.
func NewScheduleRepo(data *basedata.Data) *scheduleRepo { return &scheduleRepo{data: data} }

// List implements biz.ScheduleRepo.
func (r *scheduleRepo) List(ctx context.Context, groupID string) ([]*biz.Schedule, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+scheduleSelectColumns+` FROM oncall_schedule sc
		 WHERE sc.delete_time IS NULL AND ($1 = '' OR sc.group_id = NULLIF($1, '')::uuid)
		 ORDER BY sc.group_id, sc.name, sc.id`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list oncall schedules: %w", err)
	}
	return collectSchedules(ctx, r.data.Pool, rows)
}

// Get implements biz.ScheduleRepo.
func (r *scheduleRepo) Get(ctx context.Context, id string) (*biz.Schedule, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+scheduleSelectColumns+" FROM oncall_schedule sc WHERE sc.id = $1 AND sc.delete_time IS NULL", id)
	if err != nil {
		return nil, fmt.Errorf("get oncall schedule: %w", err)
	}
	items, err := collectSchedules(ctx, r.data.Pool, rows)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, biz.ErrScheduleNotFound
	}
	return items[0], nil
}

// ListDueExtend implements biz.ScheduleRepo。留 1 小时余量：刚补排过的值班表不会在
// 下一轮立刻再次到期。
func (r *scheduleRepo) ListDueExtend(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text FROM oncall_schedule
		 WHERE delete_time IS NULL AND status = $1
		   AND (generated_until IS NULL OR generated_until < now() + horizon_days * interval '1 day' - interval '1 hour')
		 ORDER BY generated_until NULLS FIRST LIMIT $2`, biz.StatusEnabled, limit)
	if err != nil {
		return nil, fmt.Errorf("list oncall schedules due for extension: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan oncall schedules due for extension: %w", err)
	}
	return ids, nil
}

// ListNotifyResults implements biz.ScheduleRepo。事件限定为值班事件的命名空间下、载荷里的
// schedule_id 等于该值班表的值班事件；create_time 下界兼作分区剪枝。
func (r *scheduleRepo) ListNotifyResults(ctx context.Context, scheduleID string, eventIDs []string) ([]*biz.NotifyResult, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, event_type, status FROM notify_event
		 WHERE id = ANY($1::uuid[]) AND create_time > now() - interval '1 day'
		   AND biz_id = $3 AND event_type LIKE 'oncall.%' AND payload->>'schedule_id' = $2
		 ORDER BY create_time, id`, eventIDs, scheduleID, biz.NotifyBiz)
	if err != nil {
		return nil, fmt.Errorf("list oncall notify events: %w", err)
	}
	results, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.NotifyResult, error) {
		res := &biz.NotifyResult{}
		return res, row.Scan(&res.EventID, &res.EventType, &res.Status)
	})
	if err != nil {
		return nil, fmt.Errorf("scan oncall notify events: %w", err)
	}
	if len(results) == 0 {
		return results, nil
	}
	index := make(map[string]*biz.NotifyResult, len(results))
	ids := make([]string, 0, len(results))
	for _, res := range results {
		index[res.EventID] = res
		ids = append(ids, res.EventID)
	}
	rows, err = r.data.Pool.Query(ctx,
		`SELECT d.event_id::text, COALESCE(c.name, ''), COALESCE(c.type, ''), d.recipient, d.status, COALESCE(d.last_error, '')
		 FROM notify_dispatch d LEFT JOIN notify_channel c ON c.id = d.channel_id
		 WHERE d.event_id = ANY($1::uuid[]) AND d.create_time > now() - interval '1 day'
		 ORDER BY d.create_time, d.id`, ids)
	if err != nil {
		return nil, fmt.Errorf("list oncall notify dispatches: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		d := &biz.NotifyDelivery{}
		if err := rows.Scan(&eventID, &d.ChannelName, &d.ChannelType, &d.Recipient, &d.Status, &d.Error); err != nil {
			return nil, fmt.Errorf("scan oncall notify dispatch: %w", err)
		}
		if res := index[eventID]; res != nil {
			res.Deliveries = append(res.Deliveries, d)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate oncall notify dispatches: %w", err)
	}
	return results, nil
}

// MatchNotifyRoute implements biz.ScheduleRepo。只读通知规则与渠道两张配置表；模式匹配与
// 具体度排序用 kit/notifycontract 的同一实现，与管道选路一致。
func (r *scheduleRepo) MatchNotifyRoute(ctx context.Context, eventType string, severity int32) (string, int32, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT name, event_pattern, priority,
		        (SELECT count(*) FROM notify_channel c
		          WHERE c.id = ANY(rt.channel_ids) AND c.enabled AND c.delete_time IS NULL)::int
		 FROM notify_route rt
		 WHERE rt.biz_id = $1 AND rt.enabled AND rt.delete_time IS NULL AND rt.severity_min <= $2
		   AND (rt.tag_match IS NULL OR rt.tag_match = '{}'::jsonb)
		 ORDER BY rt.priority, rt.id`, biz.NotifyBiz, severity)
	if err != nil {
		return "", 0, fmt.Errorf("list notify routes for oncall: %w", err)
	}
	defer rows.Close()
	var (
		found        bool
		bestName     string
		bestChannels int32
		bestPriority int32
		bestSpecific int
	)
	for rows.Next() {
		var name, pattern string
		var priority, channels int32
		if err := rows.Scan(&name, &pattern, &priority, &channels); err != nil {
			return "", 0, fmt.Errorf("scan notify route for oncall: %w", err)
		}
		if !notifycontract.MatchPattern(pattern, eventType) {
			continue
		}
		specific := notifycontract.PatternSpecificity(pattern)
		// 行已按优先级排序：只在同一优先级内用「更具体的模式」替换。
		if !found || (priority == bestPriority && specific < bestSpecific) {
			found, bestName, bestChannels, bestPriority, bestSpecific = true, name, channels, priority, specific
		}
		if priority > bestPriority {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return "", 0, fmt.Errorf("iterate notify routes for oncall: %w", err)
	}
	return bestName, bestChannels, nil
}

// shiftSelectColumns 班次查询列序（别名 sh，须连同 shiftJoins 使用）。
const shiftSelectColumns = `sh.id::text, sh.schedule_id::text, sc.name, sh.group_id::text, sh.role, sh.user_id::text,
	COALESCE(NULLIF(u.nickname, ''), u.username, ''), ` + userAvailableExpr + `, COALESCE(u.phone, ''),
	sh.start_time, sh.end_time, sh.source, COALESCE(sh.origin_user_id::text, ''),
	COALESCE(NULLIF(ou.nickname, ''), ou.username, ''), sh.remark, sh.reminded_mask`

// shiftJoins 只读取未删除值班表的班次；用户信息为只读投影。
const shiftJoins = ` FROM oncall_shift sh
	JOIN oncall_schedule sc ON sc.id = sh.schedule_id AND sc.delete_time IS NULL
	LEFT JOIN "system_user" u ON u.id = sh.user_id
	LEFT JOIN "system_user" ou ON ou.id = sh.origin_user_id`

func scanShift(scan func(dest ...any) error, extra ...any) (*biz.Shift, error) {
	var s biz.Shift
	dest := append([]any{
		&s.ID, &s.ScheduleID, &s.ScheduleName, &s.GroupID, &s.Role, &s.UserID, &s.UserName, &s.UserAvailable, &s.UserPhone,
		&s.Start, &s.End, &s.Source, &s.OriginUserID, &s.OriginUserName, &s.Remark, &s.RemindedMask,
	}, extra...)
	if err := scan(dest...); err != nil {
		return nil, err
	}
	return &s, nil
}

func collectShifts(rows pgx.Rows) ([]*biz.Shift, error) {
	defer rows.Close()
	items := make([]*biz.Shift, 0, 64)
	for rows.Next() {
		s, err := scanShift(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan oncall shift: %w", err)
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

type shiftRepo struct {
	data *basedata.Data
}

// NewShiftRepo new a shiftRepo.
func NewShiftRepo(data *basedata.Data) *shiftRepo { return &shiftRepo{data: data} }

// ListRange implements biz.ShiftRepo.
func (r *shiftRepo) ListRange(ctx context.Context, from, to time.Time) ([]*biz.Shift, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+shiftSelectColumns+shiftJoins+` WHERE sh.start_time < $2 AND sh.end_time > $1
		 ORDER BY sh.start_time, sh.schedule_id, sh.role`, from, to)
	if err != nil {
		return nil, fmt.Errorf("list oncall shifts: %w", err)
	}
	return collectShifts(rows)
}

// Get implements biz.ShiftRepo.
func (r *shiftRepo) Get(ctx context.Context, id string) (*biz.Shift, error) {
	return getShift(ctx, r.data.Pool, id)
}

func getShift(ctx context.Context, q querier, id string) (*biz.Shift, error) {
	s, err := scanShift(q.QueryRow(ctx, "SELECT "+shiftSelectColumns+shiftJoins+" WHERE sh.id = $1", id).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrShiftNotFound
		}
		return nil, fmt.Errorf("get oncall shift: %w", err)
	}
	return s, nil
}
