package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.SilenceRepo = (*silenceRepo)(nil)

// silenceRepo 静默仓储 PostgreSQL 实现；每次写操作同事务 bump
// notify_cfg_version('notify_silence')（管道 10s 轮询刷新静默缓存）。
// time_window 以 jsonb 落库（结构即 biz 层 json 标签），NULL=全程静默。
type silenceRepo struct {
	data *basedata.Data
}

// NewSilenceRepo new a silenceRepo.
func NewSilenceRepo(data *basedata.Data) *silenceRepo {
	return &silenceRepo{data: data}
}

// silenceSelectColumns 是静默查询列序，与 scanSilence 对应。
const silenceSelectColumns = "id, biz_id, event_pattern, severity_max, time_window, enabled, create_time, update_time"

// scanSilence 按列序扫描一行静默数据并解码为 DO（time_window 经字节中转
// 反序列化）。
func scanSilence(scan func(dest ...any) error) (*biz.Silence, error) {
	var (
		id, bizID, eventPattern string
		severityMax             int32
		windowRaw               []byte
		enabled                 bool
		createTime, updateTime  time.Time
	)
	if err := scan(&id, &bizID, &eventPattern, &severityMax, &windowRaw,
		&enabled, &createTime, &updateTime); err != nil {
		return nil, err
	}
	silence := &biz.Silence{
		ID:      id,
		BizID:   bizID,
		Enabled: enabled,
		SilenceRule: biz.SilenceRule{
			EventPattern: eventPattern,
			SeverityMax:  severityMax,
		},
		CreateTime: createTime,
		UpdateTime: updateTime,
	}
	if len(windowRaw) > 0 {
		var window biz.SilenceWindow
		if err := json.Unmarshal(windowRaw, &window); err != nil {
			return nil, fmt.Errorf("decode time_window: %w", err)
		}
		silence.Window = &window
	}
	return silence, nil
}

// encodeSilenceWindow 编码时间窗；nil 落 NULL（全程静默）。
func encodeSilenceWindow(w *biz.SilenceWindow) any {
	if w == nil {
		return nil
	}
	raw, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("encode time_window: %w", err)
	}
	return raw
}

// CreateSilence implements biz.SilenceRepo：落库并同事务 bump 版本。
func (r *silenceRepo) CreateSilence(ctx context.Context, silence *biz.Silence) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_silence
			     (id, biz_id, event_pattern, severity_max, time_window, enabled, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			silence.ID, silence.BizID, silence.EventPattern, silence.SeverityMax,
			encodeSilenceWindow(silence.Window), silence.Enabled,
			silence.CreateTime, silence.UpdateTime); err != nil {
			return fmt.Errorf("insert silence: %w", err)
		}
		return bumpCfgVersion(ctx, tx, "notify_silence")
	})
}

// UpdateSilence implements biz.SilenceRepo：Rule 整组替换规则字段或 Enabled
// 启停；update_time 恒刷新。
func (r *silenceRepo) UpdateSilence(ctx context.Context, id string, upd biz.SilenceUpdate) error {
	sets := make([]string, 0, 4)
	args := make([]any, 0, 5)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Rule != nil {
		add("event_pattern", upd.Rule.EventPattern)
		add("severity_max", upd.Rule.SeverityMax)
		add("time_window", encodeSilenceWindow(upd.Rule.Window))
	}
	if upd.Enabled != nil {
		add("enabled", *upd.Enabled)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	query := "UPDATE notify_silence SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("update silence: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrSilenceNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_silence")
	})
}

// DeleteSilence implements biz.SilenceRepo：软删并同事务 bump 版本。
func (r *silenceRepo) DeleteSilence(ctx context.Context, id string) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE notify_silence SET delete_time = now(), update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL", id)
		if err != nil {
			return fmt.Errorf("soft delete silence: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrSilenceNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_silence")
	})
}

// GetSilence implements biz.SilenceRepo。
func (r *silenceRepo) GetSilence(ctx context.Context, id string) (*biz.Silence, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+silenceSelectColumns+" FROM notify_silence WHERE id = $1 AND delete_time IS NULL", id)
	silence, err := scanSilence(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrSilenceNotFound
		}
		return nil, fmt.Errorf("get silence: %w", err)
	}
	return silence, nil
}

// ListSilences implements biz.SilenceRepo：条件过滤 + 分页，两条参数化查询。
func (r *silenceRepo) ListSilences(ctx context.Context, filter biz.SilenceFilter, page, pageSize int64) ([]*biz.Silence, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 2)
	if filter.BizID != "" {
		args = append(args, filter.BizID)
		where = append(where, "biz_id = $"+strconv.Itoa(len(args)))
	}
	if filter.Keyword != "" {
		args = append(args, basedata.LikeContains(filter.Keyword))
		where = append(where, "event_pattern ILIKE $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_silence WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count silences: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + silenceSelectColumns + " FROM notify_silence WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list silences: %w", err)
	}
	defer rows.Close()

	silences := make([]*biz.Silence, 0, pageSize)
	for rows.Next() {
		silence, err := scanSilence(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan silence: %w", err)
		}
		silences = append(silences, silence)
	}
	return silences, total, rows.Err()
}
