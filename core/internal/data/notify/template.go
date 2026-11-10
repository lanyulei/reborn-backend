package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var _ biz.TemplateRepo = (*templateRepo)(nil)

// templateRepo 模板仓储 PostgreSQL 实现；每次写操作同事务 bump
// notify_cfg_version('notify_template')（管道 10s 轮询刷新配置缓存），
// 更新恒 version+1（dispatch 记录 (template_id, version) 可复现历史通知）。
type templateRepo struct {
	data *basedata.Data
}

// NewTemplateRepo new a templateRepo.
func NewTemplateRepo(data *basedata.Data) *templateRepo {
	return &templateRepo{data: data}
}

// mapTemplateUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误。
func mapTemplateUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		if pgErr.ConstraintName == "uk_notify_template_spec" {
			return biz.ErrTemplateSpecExists
		}
	}
	return err
}

// templateSelectColumns 是模板查询列序，与 scanTemplate 对应。
const templateSelectColumns = "id, biz_id, event_type, channel_type, subject, body, version, enabled, create_time, update_time"

// scanTemplate 按列序扫描一行模板数据并解码为 DO。
func scanTemplate(scan func(dest ...any) error) (*biz.Template, error) {
	var (
		id, bizID, eventType, channelType string
		subject, body                     string
		version                           int32
		enabled                           bool
		createTime, updateTime            time.Time
	)
	if err := scan(&id, &bizID, &eventType, &channelType, &subject, &body,
		&version, &enabled, &createTime, &updateTime); err != nil {
		return nil, err
	}
	return &biz.Template{
		ID:          id,
		BizID:       bizID,
		EventType:   eventType,
		ChannelType: channelType,
		Subject:     subject,
		Body:        body,
		Version:     version,
		Enabled:     enabled,
		CreateTime:  createTime,
		UpdateTime:  updateTime,
	}, nil
}

// CreateTemplate implements biz.TemplateRepo：落库并同事务 bump 版本。
func (r *templateRepo) CreateTemplate(ctx context.Context, tpl *biz.Template) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_template
			     (id, biz_id, event_type, channel_type, subject, body, version, enabled, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			tpl.ID, tpl.BizID, tpl.EventType, tpl.ChannelType, tpl.Subject, tpl.Body,
			tpl.Version, tpl.Enabled, tpl.CreateTime, tpl.UpdateTime); err != nil {
			return mapTemplateUniqueViolation(err)
		}
		return bumpCfgVersion(ctx, tx, "notify_template")
	})
}

// UpdateTemplate implements biz.TemplateRepo：动态 SET 只允许静态列名白名单，
// 任何更新恒 version = version + 1（模板版本化，历史通知可复现）。
func (r *templateRepo) UpdateTemplate(ctx context.Context, id string, upd biz.TemplateUpdate) error {
	sets := make([]string, 0, 5)
	args := make([]any, 0, 6)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.EventType != nil {
		add("event_type", *upd.EventType)
	}
	if upd.ChannelType != nil {
		add("channel_type", *upd.ChannelType)
	}
	if upd.Subject != nil {
		add("subject", *upd.Subject)
	}
	if upd.Body != nil {
		add("body", *upd.Body)
	}
	if upd.Enabled != nil {
		add("enabled", *upd.Enabled)
	}
	if len(sets) == 0 {
		return nil
	}
	sets = append(sets, "version = version + 1")

	args = append(args, id)
	query := "UPDATE notify_template SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return mapTemplateUniqueViolation(err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrTemplateNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_template")
	})
}

// DeleteTemplate implements biz.TemplateRepo：软删并同事务 bump 版本。
func (r *templateRepo) DeleteTemplate(ctx context.Context, id string) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE notify_template SET delete_time = now(), update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL", id)
		if err != nil {
			return fmt.Errorf("soft delete template: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrTemplateNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_template")
	})
}

// GetTemplate implements biz.TemplateRepo。
func (r *templateRepo) GetTemplate(ctx context.Context, id string) (*biz.Template, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+templateSelectColumns+" FROM notify_template WHERE id = $1 AND delete_time IS NULL", id)
	tpl, err := scanTemplate(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrTemplateNotFound
		}
		return nil, fmt.Errorf("get template: %w", err)
	}
	return tpl, nil
}

// ListTemplates implements biz.TemplateRepo：条件过滤 + 分页，两条参数化查询。
func (r *templateRepo) ListTemplates(ctx context.Context, filter biz.TemplateFilter, page, pageSize int64) ([]*biz.Template, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 5)
	if filter.BizID != "" {
		args = append(args, filter.BizID)
		where = append(where, "biz_id = $"+strconv.Itoa(len(args)))
	}
	if filter.Keyword != "" {
		args = append(args, basedata.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(event_type ILIKE $"+n+" OR channel_type ILIKE $"+n+" OR subject ILIKE $"+n+")")
	}
	if filter.EventType != nil {
		args = append(args, *filter.EventType)
		where = append(where, "event_type = $"+strconv.Itoa(len(args)))
	}
	if filter.ChannelType != nil {
		args = append(args, *filter.ChannelType)
		where = append(where, "channel_type = $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_template WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count templates: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + templateSelectColumns + " FROM notify_template WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	templates := make([]*biz.Template, 0, pageSize)
	for rows.Next() {
		tpl, err := scanTemplate(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan template: %w", err)
		}
		templates = append(templates, tpl)
	}
	return templates, total, rows.Err()
}
