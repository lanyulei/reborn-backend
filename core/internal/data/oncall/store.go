package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.Store = (*store)(nil)
	_ biz.Tx    = (*txn)(nil)
)

// reminderGrace 交接时刻提醒（提前量 0）的补发宽限：后台任务停顿不超过该时长仍会补发。
const reminderGrace = 10 * time.Minute

type store struct {
	data *basedata.Data
}

// NewStore new a store.
func NewStore(data *basedata.Data) *store { return &store{data: data} }

// WithinTx implements biz.Store.
func (s *store) WithinTx(ctx context.Context, fn func(tx biz.Tx) error) error {
	return dbx.WithTx(ctx, s.data.Pool, func(tx pgx.Tx) error {
		return fn(&txn{tx: tx})
	})
}

// txn 是 biz.Tx 的 pgx 实现；同时满足 txrun.Tx，通知事件可在同一事务内直插。
type txn struct {
	tx pgx.Tx
}

// Exec implements txrun.Tx.
func (t *txn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// QueryRow implements txrun.Tx.
func (t *txn) QueryRow(ctx context.Context, sql string, args ...any) txrun.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

// exec 执行写语句并统一包装错误。
func (t *txn) exec(ctx context.Context, what, sql string, args ...any) (int64, error) {
	n, err := t.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return n, nil
}

// LockGroup implements biz.Tx.
func (t *txn) LockGroup(ctx context.Context, id string) (*biz.Group, error) {
	var g biz.Group
	err := t.tx.QueryRow(ctx,
		"SELECT id::text, name, description, create_time, update_time FROM oncall_group WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).
		Scan(&g.ID, &g.Name, &g.Description, &g.CreateTime, &g.UpdateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrGroupNotFound
		}
		return nil, fmt.Errorf("lock oncall group: %w", err)
	}
	return &g, nil
}

// MemberRole implements biz.Tx.
func (t *txn) MemberRole(ctx context.Context, groupID, userID string) (int32, error) {
	var role int32
	err := t.tx.QueryRow(ctx, "SELECT role FROM oncall_group_member WHERE group_id = $1 AND user_id = $2", groupID, userID).Scan(&role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get oncall member role: %w", err)
	}
	return role, nil
}

// InsertGroup implements biz.Tx.
func (t *txn) InsertGroup(ctx context.Context, g *biz.Group, actor string) error {
	_, err := t.tx.Exec(ctx,
		"INSERT INTO oncall_group (id, name, description, create_user, update_user) VALUES ($1, $2, $3, $4, $4)",
		g.ID, g.Name, g.Description, dbx.NilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "uk_oncall_group_name") {
			return biz.ErrGroupNameExists
		}
		return fmt.Errorf("insert oncall group: %w", err)
	}
	return nil
}

// UpdateGroup implements biz.Tx.
func (t *txn) UpdateGroup(ctx context.Context, id, name, description, actor string) error {
	_, err := t.tx.Exec(ctx,
		"UPDATE oncall_group SET name = $2, description = $3, update_user = $4, update_time = now() WHERE id = $1 AND delete_time IS NULL",
		id, name, description, dbx.NilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "uk_oncall_group_name") {
			return biz.ErrGroupNameExists
		}
		return fmt.Errorf("update oncall group: %w", err)
	}
	return nil
}

// DeleteGroup implements biz.Tx.
func (t *txn) DeleteGroup(ctx context.Context, id, actor string) error {
	if _, err := t.exec(ctx, "delete oncall group members", "DELETE FROM oncall_group_member WHERE group_id = $1", id); err != nil {
		return err
	}
	_, err := t.exec(ctx, "delete oncall group",
		"UPDATE oncall_group SET delete_time = now(), delete_user = $2 WHERE id = $1 AND delete_time IS NULL", id, dbx.NilIfEmpty(actor))
	return err
}

// CountSchedules implements biz.Tx.
func (t *txn) CountSchedules(ctx context.Context, groupID string) (int, error) {
	var n int
	if err := t.tx.QueryRow(ctx, "SELECT count(*) FROM oncall_schedule WHERE group_id = $1 AND delete_time IS NULL", groupID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count oncall schedules: %w", err)
	}
	return n, nil
}

// ListMembers implements biz.Tx.
func (t *txn) ListMembers(ctx context.Context, groupID string) ([]*biz.Member, error) {
	return listMembers(ctx, t.tx, groupID)
}

// InsertMembers implements biz.Tx.
func (t *txn) InsertMembers(ctx context.Context, groupID string, userIDs []string, role int32, actor string) (int, error) {
	ids := make([]string, len(userIDs))
	for i := range userIDs {
		id, err := idgen.NewID()
		if err != nil {
			return 0, fmt.Errorf("generate member id: %w", err)
		}
		ids[i] = id
	}
	n, err := t.exec(ctx, "insert oncall group members",
		`INSERT INTO oncall_group_member (id, group_id, user_id, role, create_user)
		 SELECT x.id, $1, x.user_id, $4, $5 FROM unnest($2::uuid[], $3::uuid[]) AS x(id, user_id)
		 ON CONFLICT (group_id, user_id) DO NOTHING`, groupID, ids, userIDs, role, dbx.NilIfEmpty(actor))
	return int(n), err
}

// UpdateMemberRole implements biz.Tx.
func (t *txn) UpdateMemberRole(ctx context.Context, groupID, userID string, role int32) error {
	_, err := t.exec(ctx, "update oncall member role",
		"UPDATE oncall_group_member SET role = $3 WHERE group_id = $1 AND user_id = $2", groupID, userID, role)
	return err
}

// DeleteMember implements biz.Tx.
func (t *txn) DeleteMember(ctx context.Context, groupID, userID string) error {
	_, err := t.exec(ctx, "delete oncall member", "DELETE FROM oncall_group_member WHERE group_id = $1 AND user_id = $2", groupID, userID)
	return err
}

// HasPendingOverrides implements biz.Tx。结束时间以数据库时钟为准（与提醒任务判定班次是否
// 未结束是同一个时钟）。
func (t *txn) HasPendingOverrides(ctx context.Context, groupID, userID string) (bool, error) {
	var found bool
	err := t.tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM oncall_shift
		 WHERE group_id = $1::uuid AND user_id = $2::uuid AND source = $3::smallint AND end_time > now())`,
		groupID, userID, biz.SourceOverride).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("check pending oncall overrides: %w", err)
	}
	return found, nil
}

// LockSchedule implements biz.Tx.
func (t *txn) LockSchedule(ctx context.Context, id string) (*biz.Schedule, error) {
	s, err := scanSchedule(t.tx.QueryRow(ctx,
		"SELECT "+scheduleSelectColumns+" FROM oncall_schedule sc WHERE sc.id = $1 AND sc.delete_time IS NULL FOR UPDATE OF sc", id).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrScheduleNotFound
		}
		return nil, fmt.Errorf("lock oncall schedule: %w", err)
	}
	return s, nil
}

// replaceScheduleMembers 整体替换轮转名单。
func (t *txn) replaceScheduleMembers(ctx context.Context, s *biz.Schedule, actor string) error {
	if _, err := t.exec(ctx, "clear oncall schedule members", "DELETE FROM oncall_schedule_member WHERE schedule_id = $1", s.ID); err != nil {
		return err
	}
	if len(s.MemberIDs) == 0 {
		return nil
	}
	ids := make([]string, len(s.MemberIDs))
	for i := range s.MemberIDs {
		id, err := idgen.NewID()
		if err != nil {
			return fmt.Errorf("generate schedule member id: %w", err)
		}
		ids[i] = id
	}
	_, err := t.exec(ctx, "insert oncall schedule members",
		`INSERT INTO oncall_schedule_member (id, schedule_id, user_id, sort, create_user)
		 SELECT x.id, $1, x.user_id, x.ord - 1, $4 FROM unnest($2::uuid[], $3::uuid[]) WITH ORDINALITY AS x(id, user_id, ord)`,
		s.ID, ids, s.MemberIDs, dbx.NilIfEmpty(actor))
	return err
}

// remindMinutes 归一化 integer[] 实参（nil 切片会被编码为 NULL，撞 NOT NULL）。
func remindMinutes(m []int32) []int32 {
	if m == nil {
		return []int32{}
	}
	return m
}

// InsertSchedule implements biz.Tx.
func (t *txn) InsertSchedule(ctx context.Context, s *biz.Schedule, actor string) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO oncall_schedule (id, group_id, name, description, timezone, rotation_unit, rotation_interval, anchor_time,
		        anchor_offset, backup_enabled, horizon_days, remind_minutes, digest_time, status, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, '')::time, $14, $15, $15)`,
		s.ID, s.GroupID, s.Name, s.Description, s.Timezone, s.RotationUnit, s.RotationInterval, s.AnchorTime,
		s.AnchorOffset, s.BackupEnabled, s.HorizonDays, remindMinutes(s.RemindMinutes), s.DigestTime, s.Status, dbx.NilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "uk_oncall_schedule_name") {
			return biz.ErrScheduleNameExists
		}
		return fmt.Errorf("insert oncall schedule: %w", err)
	}
	return t.replaceScheduleMembers(ctx, s, actor)
}

// UpdateSchedule implements biz.Tx。播报时刻变化时清空 last_digest_date，新时刻当日仍可播报。
func (t *txn) UpdateSchedule(ctx context.Context, s *biz.Schedule, actor string) error {
	_, err := t.tx.Exec(ctx,
		`UPDATE oncall_schedule SET name = $2, description = $3, timezone = $4, rotation_unit = $5, rotation_interval = $6,
		        anchor_time = $7, anchor_offset = $8, backup_enabled = $9, horizon_days = $10, remind_minutes = $11,
		        last_digest_date = CASE WHEN digest_time IS NOT DISTINCT FROM NULLIF($12, '')::time THEN last_digest_date END,
		        digest_time = NULLIF($12, '')::time, update_user = $13, update_time = now()
		 WHERE id = $1 AND delete_time IS NULL`,
		s.ID, s.Name, s.Description, s.Timezone, s.RotationUnit, s.RotationInterval, s.AnchorTime, s.AnchorOffset,
		s.BackupEnabled, s.HorizonDays, remindMinutes(s.RemindMinutes), s.DigestTime, dbx.NilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "uk_oncall_schedule_name") {
			return biz.ErrScheduleNameExists
		}
		return fmt.Errorf("update oncall schedule: %w", err)
	}
	return t.replaceScheduleMembers(ctx, s, actor)
}

// SetScheduleStatus implements biz.Tx.
func (t *txn) SetScheduleStatus(ctx context.Context, id string, status int32, actor string) error {
	_, err := t.exec(ctx, "set oncall schedule status",
		"UPDATE oncall_schedule SET status = $2, update_user = $3, update_time = now() WHERE id = $1 AND delete_time IS NULL",
		id, status, dbx.NilIfEmpty(actor))
	return err
}

// SetGeneratedUntil implements biz.Tx.
func (t *txn) SetGeneratedUntil(ctx context.Context, id string, until *time.Time) error {
	_, err := t.exec(ctx, "set oncall schedule generated_until", "UPDATE oncall_schedule SET generated_until = $2 WHERE id = $1", id, until)
	return err
}

// DeleteSchedule implements biz.Tx.
func (t *txn) DeleteSchedule(ctx context.Context, id, actor string) error {
	if _, err := t.exec(ctx, "clear oncall schedule members", "DELETE FROM oncall_schedule_member WHERE schedule_id = $1", id); err != nil {
		return err
	}
	_, err := t.exec(ctx, "delete oncall schedule",
		"UPDATE oncall_schedule SET delete_time = now(), delete_user = $2 WHERE id = $1 AND delete_time IS NULL", id, dbx.NilIfEmpty(actor))
	return err
}

// shiftsFromSQL 值班表中结束时间晚于 from 的班次（ListShiftsFrom / LockShiftsFrom 共用）。
const shiftsFromSQL = "SELECT " + shiftSelectColumns + shiftJoins + " WHERE sh.schedule_id = $1 AND sh.end_time > $2 ORDER BY sh.start_time, sh.role"

// ListShiftsRange implements biz.Tx：与 shiftRepo.ListRange 同一条语句，走事务连接。
func (t *txn) ListShiftsRange(ctx context.Context, from, to time.Time) ([]*biz.Shift, error) {
	rows, err := t.tx.Query(ctx, shiftsRangeSQL, from, to)
	if err != nil {
		return nil, fmt.Errorf("list oncall shifts in tx: %w", err)
	}
	return collectShifts(rows)
}

// ListShiftsFrom implements biz.Tx.
func (t *txn) ListShiftsFrom(ctx context.Context, scheduleID string, from time.Time) ([]*biz.Shift, error) {
	rows, err := t.tx.Query(ctx, shiftsFromSQL, scheduleID, from)
	if err != nil {
		return nil, fmt.Errorf("list oncall shifts in tx: %w", err)
	}
	return collectShifts(rows)
}

// LockShiftsFrom implements biz.Tx：行锁读取（只锁班次行；用户信息是只读投影）。提醒任务对班次行
// 用 SKIP LOCKED，与本读取互斥，之后按读到的标记改写不会覆盖掉它刚置的位。
func (t *txn) LockShiftsFrom(ctx context.Context, scheduleID string, from time.Time) ([]*biz.Shift, error) {
	rows, err := t.tx.Query(ctx, shiftsFromSQL+" FOR UPDATE OF sh", scheduleID, from)
	if err != nil {
		return nil, fmt.Errorf("lock oncall shifts in tx: %w", err)
	}
	return collectShifts(rows)
}

// HasRecentManualNotify implements biz.Tx。在持有值班表行锁的事务内查询，并发的手动
// 发送互相可见；create_time 下界兼作分区剪枝。
func (t *txn) HasRecentManualNotify(ctx context.Context, scheduleID, eventType string, since time.Time) (bool, error) {
	var found bool
	err := t.tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM notify_event
		 WHERE create_time > $3 AND biz_id = $4 AND event_type = $2
		   AND payload->>'schedule_id' = $1 AND payload->>'manual' = 'true')`, scheduleID, eventType, since, biz.NotifyBiz).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("check recent manual oncall notification: %w", err)
	}
	return found, nil
}

// GetShift implements biz.Tx.
func (t *txn) GetShift(ctx context.Context, id string) (*biz.Shift, error) {
	return getShift(ctx, t.tx, id)
}

// DeleteFutureAutoShifts implements biz.Tx.
func (t *txn) DeleteFutureAutoShifts(ctx context.Context, scheduleID string, now time.Time) error {
	_, err := t.exec(ctx, "delete future auto oncall shifts",
		"DELETE FROM oncall_shift WHERE schedule_id = $1 AND start_time > $2 AND source = $3", scheduleID, now, biz.SourceAuto)
	return err
}

// DeleteShiftsFrom implements biz.Tx.
func (t *txn) DeleteShiftsFrom(ctx context.Context, scheduleID string, from time.Time) error {
	_, err := t.exec(ctx, "delete oncall shifts", "DELETE FROM oncall_shift WHERE schedule_id = $1 AND start_time >= $2", scheduleID, from)
	return err
}

// TruncateShiftsAt implements biz.Tx.
func (t *txn) TruncateShiftsAt(ctx context.Context, scheduleID string, at time.Time) error {
	_, err := t.exec(ctx, "truncate oncall shifts",
		"UPDATE oncall_shift SET end_time = $2, update_time = now() WHERE schedule_id = $1 AND start_time < $2 AND end_time > $2", scheduleID, at)
	return err
}

// InsertShifts implements biz.Tx：单语句批量写入（一次补排可达数百行）。
func (t *txn) InsertShifts(ctx context.Context, shifts []*biz.Shift, actor string) error {
	if len(shifts) == 0 {
		return nil
	}
	n := len(shifts)
	ids, users, origins := make([]string, n), make([]string, n), make([]*string, n)
	roles, sources, masks := make([]int32, n), make([]int32, n), make([]int32, n)
	starts, ends, remarks := make([]time.Time, n), make([]time.Time, n), make([]string, n)
	for i, s := range shifts {
		ids[i], users[i], roles[i], sources[i], masks[i] = s.ID, s.UserID, s.Role, s.Source, s.RemindedMask
		starts[i], ends[i], remarks[i] = s.Start, s.End, s.Remark
		if s.OriginUserID != "" {
			origins[i] = &s.OriginUserID
		}
	}
	_, err := t.tx.Exec(ctx,
		`INSERT INTO oncall_shift (id, schedule_id, group_id, role, user_id, start_time, end_time, source, origin_user_id,
		        remark, reminded_mask, create_user, update_user)
		 SELECT x.id, $1, $2, x.role, x.user_id, x.start_time, x.end_time, x.source, x.origin_user_id, x.remark, x.mask, $12, $12
		 FROM unnest($3::uuid[], $4::smallint[], $5::uuid[], $6::timestamptz[], $7::timestamptz[], $8::smallint[],
		             $9::uuid[], $10::text[], $11::smallint[])
		      AS x(id, role, user_id, start_time, end_time, source, origin_user_id, remark, mask)`,
		shifts[0].ScheduleID, shifts[0].GroupID, ids, roles, users, starts, ends, sources, origins, remarks, masks, dbx.NilIfEmpty(actor))
	if err != nil {
		if isExclusionViolation(err) {
			return biz.ErrShiftConflict
		}
		return fmt.Errorf("insert oncall shifts: %w", err)
	}
	return nil
}

// UpdateShiftRange implements biz.Tx.
func (t *txn) UpdateShiftRange(ctx context.Context, id string, start, end time.Time, actor string) error {
	_, err := t.tx.Exec(ctx,
		"UPDATE oncall_shift SET start_time = $2, end_time = $3, update_user = $4, update_time = now() WHERE id = $1",
		id, start, end, dbx.NilIfEmpty(actor))
	if err != nil {
		if isExclusionViolation(err) {
			return biz.ErrShiftConflict
		}
		return fmt.Errorf("update oncall shift range: %w", err)
	}
	return nil
}

// DeleteShift implements biz.Tx.
func (t *txn) DeleteShift(ctx context.Context, id string) error {
	_, err := t.exec(ctx, "delete oncall shift", "DELETE FROM oncall_shift WHERE id = $1", id)
	return err
}

// InsertShareLink implements biz.Tx.
func (t *txn) InsertShareLink(ctx context.Context, l *biz.ShareLink, actor string) error {
	_, err := t.exec(ctx, "insert oncall share link",
		`INSERT INTO oncall_share_link (id, scope_type, scope_id, name, token_hash, token_sealed, show_phone, expire_time, create_time, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
		l.ID, l.ScopeType, l.ScopeID, l.Name, l.TokenHash, l.TokenSealed, l.ShowPhone, l.ExpireTime, l.CreateTime, dbx.NilIfEmpty(actor))
	return err
}

// UpdateShareLinkToken implements biz.Tx：换哈希即旧令牌失效；访问时间一并清空。
func (t *txn) UpdateShareLinkToken(ctx context.Context, id string, hash, sealed []byte, actor string) error {
	n, err := t.exec(ctx, "reset oncall share link",
		`UPDATE oncall_share_link SET token_hash = $2, token_sealed = $3, last_access_time = NULL, update_user = $4, update_time = now()
		 WHERE id = $1 AND delete_time IS NULL`, id, hash, sealed, dbx.NilIfEmpty(actor))
	if err == nil && n == 0 {
		return biz.ErrShareLinkNotFound
	}
	return err
}

// DeleteShareLink implements biz.Tx.
func (t *txn) DeleteShareLink(ctx context.Context, id, actor string) error {
	n, err := t.exec(ctx, "delete oncall share link",
		"UPDATE oncall_share_link SET delete_time = now(), delete_user = $2 WHERE id = $1 AND delete_time IS NULL", id, dbx.NilIfEmpty(actor))
	if err == nil && n == 0 {
		return biz.ErrShareLinkNotFound
	}
	return err
}

// DeleteShareLinksByScope implements biz.Tx.
func (t *txn) DeleteShareLinksByScope(ctx context.Context, scopeType int32, scopeID, actor string) error {
	_, err := t.exec(ctx, "delete oncall share links by scope",
		`UPDATE oncall_share_link SET delete_time = now(), delete_user = $3
		 WHERE scope_type = $1 AND scope_id = $2 AND delete_time IS NULL`, scopeType, scopeID, dbx.NilIfEmpty(actor))
	return err
}

// CountShareLinks implements biz.Tx.
func (t *txn) CountShareLinks(ctx context.Context, scopeType int32, scopeID string) (int, error) {
	var n int
	err := t.tx.QueryRow(ctx,
		"SELECT count(*) FROM oncall_share_link WHERE scope_type = $1 AND scope_id = $2 AND delete_time IS NULL", scopeType, scopeID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count oncall share links: %w", err)
	}
	return n, nil
}

// InsertChangeLog implements biz.Tx.
func (t *txn) InsertChangeLog(ctx context.Context, l *biz.ChangeLog) error {
	detail := l.Detail
	if detail == nil {
		detail = map[string]any{}
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal oncall change log detail: %w", err)
	}
	_, err = t.exec(ctx, "insert oncall change log",
		"INSERT INTO oncall_change_log (id, group_id, schedule_id, action, summary, detail, create_user) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		l.ID, l.GroupID, dbx.NilIfEmpty(l.ScheduleID), l.Action, l.Summary, raw, dbx.NilIfEmpty(l.OperatorID))
	return err
}

// ListDueReminders implements biz.Tx：start_time 落在 (now-宽限, now+7 天] 且至少有一个
// 提醒已到点而未发送的班次；行锁 SKIP LOCKED 让多实例各取各的。
func (t *txn) ListDueReminders(ctx context.Context, now time.Time, limit int) ([]*biz.DueReminder, error) {
	rows, err := t.tx.Query(ctx,
		"SELECT "+shiftSelectColumns+`, sc.timezone, sc.remind_minutes,
		        EXISTS (SELECT 1 FROM oncall_shift p WHERE p.schedule_id = sh.schedule_id AND p.role = sh.role
		                AND p.user_id = sh.user_id AND p.end_time = sh.start_time)`+shiftJoins+`
		 WHERE sc.status = $3 AND cardinality(sc.remind_minutes) > 0
		   AND sh.start_time > $1::timestamptz - $4::interval AND sh.start_time <= $1::timestamptz + interval '7 days'
		   AND EXISTS (SELECT 1 FROM unnest(sc.remind_minutes) WITH ORDINALITY AS r(m, i)
		               WHERE sh.reminded_mask & (1 << (r.i - 1)::int) = 0
		                 AND sh.start_time <= $1::timestamptz + r.m * interval '1 minute')
		 ORDER BY sh.start_time LIMIT $2 FOR UPDATE OF sh SKIP LOCKED`,
		now, limit, biz.StatusEnabled, reminderGrace)
	if err != nil {
		return nil, fmt.Errorf("list due oncall reminders: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.DueReminder, 0, 16)
	for rows.Next() {
		var d biz.DueReminder
		sh, err := scanShift(rows.Scan, &d.Timezone, &d.RemindMinutes, &d.Continued)
		if err != nil {
			return nil, fmt.Errorf("scan due oncall reminder: %w", err)
		}
		d.Shift = sh
		items = append(items, &d)
	}
	return items, rows.Err()
}

// SetRemindedMask implements biz.Tx.
func (t *txn) SetRemindedMask(ctx context.Context, shiftID string, mask int32) error {
	_, err := t.exec(ctx, "set oncall shift reminded mask", "UPDATE oncall_shift SET reminded_mask = $2 WHERE id = $1", shiftID, mask)
	return err
}

// UpdateRemindedMasks implements biz.Tx：单语句批量覆盖（修改提醒提前量时按值重映射已提醒标记）。
func (t *txn) UpdateRemindedMasks(ctx context.Context, masks []biz.ShiftMask) error {
	if len(masks) == 0 {
		return nil
	}
	ids, values := make([]string, len(masks)), make([]int32, len(masks))
	for i, m := range masks {
		ids[i], values[i] = m.ShiftID, m.Mask
	}
	_, err := t.exec(ctx, "remap oncall shift reminded masks",
		`UPDATE oncall_shift sh SET reminded_mask = x.mask
		 FROM unnest($1::uuid[], $2::smallint[]) AS x(id, mask)
		 WHERE sh.id = x.id`, ids, values)
	return err
}

// purgeDeletedScheduleShiftsSQL 已删除值班表的班次的保留期清理（PurgeDeletedScheduleShifts 用；单独成常量供集成测试
// 在真实库上核对执行计划）。$1 保留期，$2 批大小；时间取数据库时钟，与值班表 delete_time 的写入同一时钟源。
//
// 按值班表取行（迁移 0090 的 idx_oncall_shift_schedule）：从已删除超过保留期的值班表出发，经 schedule_id 索引取班次，
// 取满一批即停。子查询不排序：按删除时间排序的话，每批都得先把所有已删除值班表的班次取出来排好序才能取前 N 条，
// 积压越大每批越慢；这些行反正都要删，先后次序没有意义。
//
// 不需要在被删的行上重检条件：值班表没有恢复功能（delete_time 只会从 NULL 变成非 NULL），而班次的全部写路径都
// 先锁一张未删除的值班表，已删除值班表的班次不会再被改写。
const purgeDeletedScheduleShiftsSQL = `DELETE FROM oncall_shift WHERE id IN (
	SELECT sh.id FROM oncall_shift sh
	JOIN oncall_schedule sc ON sc.id = sh.schedule_id
	WHERE sc.delete_time IS NOT NULL AND sc.delete_time < now() - $1::interval
	LIMIT $2)`

// PurgeDeletedScheduleShifts implements biz.Tx.PurgeDeletedScheduleShifts。语句幂等：多实例同时清理时后到者删到 0 行。
func (t *txn) PurgeDeletedScheduleShifts(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	n, err := t.exec(ctx, "purge shifts of deleted oncall schedules", purgeDeletedScheduleShiftsSQL, olderThan, limit)
	return int(n), err
}

// ListDueDigests implements biz.Tx：本地时刻在播报时刻之后 30 分钟内且当日未播报的启用
// 值班表（窗口用于避免当天晚些时候才设置播报时刻时立刻补发）。
func (t *txn) ListDueDigests(ctx context.Context, now time.Time, limit int) ([]*biz.Schedule, error) {
	rows, err := t.tx.Query(ctx,
		"SELECT "+scheduleSelectColumns+` FROM oncall_schedule sc
		 WHERE sc.delete_time IS NULL AND sc.status = $3 AND sc.digest_time IS NOT NULL
		   AND (sc.last_digest_date IS NULL OR sc.last_digest_date < ($1::timestamptz AT TIME ZONE sc.timezone)::date)
		   AND ($1::timestamptz AT TIME ZONE sc.timezone) - (($1::timestamptz AT TIME ZONE sc.timezone)::date + sc.digest_time)
		       BETWEEN interval '0' AND interval '30 minutes'
		 ORDER BY sc.id LIMIT $2 FOR UPDATE OF sc SKIP LOCKED`, now, limit, biz.StatusEnabled)
	if err != nil {
		return nil, fmt.Errorf("list due oncall digests: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Schedule, 0, 8)
	for rows.Next() {
		s, err := scanSchedule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan due oncall digest: %w", err)
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

// MarkDigested implements biz.Tx.
func (t *txn) MarkDigested(ctx context.Context, scheduleID string, now time.Time) error {
	_, err := t.exec(ctx, "mark oncall schedule digested",
		"UPDATE oncall_schedule SET last_digest_date = ($2::timestamptz AT TIME ZONE timezone)::date WHERE id = $1", scheduleID, now)
	return err
}
