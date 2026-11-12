package data

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/dbx"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

var _ biz.HistoryRepo = (*historyRepo)(nil)

// insertHistories 在事务内追加历史（资源/关系写操作同事务调用）。
func insertHistories(ctx context.Context, tx pgx.Tx, items []*biz.History) error {
	for _, h := range items {
		if h == nil {
			continue
		}
		diff, err := json.Marshal(h.Diff)
		if err != nil {
			return fmt.Errorf("marshal history diff: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			h.ID, h.CIID, h.Action, diff, h.Operator, h.Source, h.RequestID, h.CreateTime); err != nil {
			return fmt.Errorf("insert cmdb history: %w", err)
		}
	}
	return nil
}

type historyRepo struct {
	data *basedata.Data
}

// NewHistoryRepo new a historyRepo.
func NewHistoryRepo(data *basedata.Data) *historyRepo { return &historyRepo{data: data} }

// ListByCI implements biz.HistoryRepo（时间倒序）。
func (r *historyRepo) ListByCI(ctx context.Context, ciID string, page, pageSize int64) ([]*biz.History, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci_history WHERE ci_id = $1", ciID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb history: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, ci_id, action, diff, operator, source, request_id, create_time FROM cmdb_ci_history
		 WHERE ci_id = $1 ORDER BY create_time DESC, id DESC LIMIT $2 OFFSET $3`, ciID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb history: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.History, 0, pageSize)
	for rows.Next() {
		var h biz.History
		var diff []byte
		if err := rows.Scan(&h.ID, &h.CIID, &h.Action, &diff, &h.Operator, &h.Source, &h.RequestID, &h.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan cmdb history: %w", err)
		}
		if len(diff) > 0 {
			if err := json.Unmarshal(diff, &h.Diff); err != nil {
				return nil, 0, fmt.Errorf("decode cmdb history diff: %w", err)
			}
		}
		items = append(items, &h)
	}
	return items, total, rows.Err()
}

// List implements biz.HistoryRepo：全局审计，join 资源取名称/类型/环境（已删除资源仍可见）。
func (r *historyRepo) List(ctx context.Context, filter biz.HistoryFilter, page, pageSize int64) ([]*biz.History, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 8)
	add := func(cond string, value any) {
		args = append(args, value)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if len(filter.ModelCodes) > 0 {
		add("c.model_code = ANY(?)", dbx.TextArray(filter.ModelCodes))
	}
	if len(filter.Envs) > 0 {
		add("c.env = ANY(?)", dbx.TextArray(filter.Envs))
	}
	if filter.ModelCode != "" {
		add("c.model_code = ?", filter.ModelCode)
	}
	if filter.Action != "" {
		add("h.action = ?", filter.Action)
	}
	if filter.Operator != "" {
		add("h.operator = ?", filter.Operator)
	}
	if filter.Keyword != "" {
		add("c.name ILIKE ?", dbx.LikeContains(filter.Keyword))
	}
	if filter.From != nil {
		add("h.create_time >= ?", *filter.From)
	}
	if filter.To != nil {
		add("h.create_time <= ?", *filter.To)
	}
	from := " FROM cmdb_ci_history h JOIN cmdb_ci c ON c.id = h.ci_id WHERE " + strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+from, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb history: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT h.id, h.ci_id, c.name, c.model_code, h.action, h.diff, h.operator, h.source, h.request_id, h.create_time"+from+
			" ORDER BY h.create_time DESC, h.id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb history: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.History, 0, pageSize)
	for rows.Next() {
		var h biz.History
		var diff []byte
		if err := rows.Scan(&h.ID, &h.CIID, &h.CIName, &h.ModelCode, &h.Action, &diff, &h.Operator, &h.Source, &h.RequestID, &h.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan cmdb history: %w", err)
		}
		if len(diff) > 0 {
			if err := json.Unmarshal(diff, &h.Diff); err != nil {
				return nil, 0, fmt.Errorf("decode history diff: %w", err)
			}
		}
		items = append(items, &h)
	}
	return items, total, rows.Err()
}
