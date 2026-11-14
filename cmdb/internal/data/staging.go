package data

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.StagingRepo = (*stagingRepo)(nil)

type stagingRepo struct {
	data *Data
}

// NewStagingRepo new a stagingRepo.
func NewStagingRepo(data *Data) *stagingRepo { return &stagingRepo{data: data} }

// Append implements biz.StagingRepo（批量 upsert，一事务）。
func (r *stagingRepo) Append(ctx context.Context, runID string, rows []*biz.Resource) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, res := range rows {
		attrs, err := json.Marshal(res.Attrs)
		if err != nil {
			return fmt.Errorf("marshal attrs: %w", err)
		}
		rels := res.Relations
		if rels == nil {
			rels = []cmdbcontract.StagingRelation{}
		}
		relations, err := json.Marshal(rels)
		if err != nil {
			return fmt.Errorf("marshal relations: %w", err)
		}
		batch.Queue(
			`INSERT INTO cmdb_sync_staging (run_id, model_code, external_id, name, attrs, relations) VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (run_id, model_code, external_id) DO UPDATE SET name = EXCLUDED.name, attrs = EXCLUDED.attrs, relations = EXCLUDED.relations`,
			runID, res.ModelCode, res.ExternalID, res.Name, attrs, relations)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		results := tx.SendBatch(ctx, batch)
		defer func() { _ = results.Close() }()
		for range rows {
			if _, err := results.Exec(); err != nil {
				return fmt.Errorf("insert staging: %w", err)
			}
		}
		return nil
	})
}

// RefreshStats implements biz.StagingRepo。暂存表归数据面角色所有（migrations 0049），
// 故数据面账号可以 ANALYZE；账号无权时 PostgreSQL 只给出 WARNING 并跳过，不报错。
// 单独一条语句（不在对账事务内）：只在分析期间持有表级 ShareUpdateExclusive 锁。
func (r *stagingRepo) RefreshStats(ctx context.Context) error {
	if _, err := r.data.Pool.Exec(ctx, "ANALYZE cmdb_sync_staging"); err != nil {
		return fmt.Errorf("analyze staging: %w", err)
	}
	return nil
}

// Models implements biz.StagingRepo。
func (r *stagingRepo) Models(ctx context.Context, runID string) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT model_code FROM cmdb_sync_staging WHERE run_id = $1 GROUP BY model_code ORDER BY min(ctid)", runID)
	if err != nil {
		return nil, fmt.Errorf("staging models: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 4)
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		out = append(out, code)
	}
	return out, rows.Err()
}

// Count implements biz.StagingRepo。
func (r *stagingRepo) Count(ctx context.Context, runID string) (int32, error) {
	var n int32
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_sync_staging WHERE run_id = $1", runID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count staging: %w", err)
	}
	return n, nil
}

// Clear implements biz.StagingRepo。
func (r *stagingRepo) Clear(ctx context.Context, runID string) error {
	if _, err := r.data.Pool.Exec(ctx, "DELETE FROM cmdb_sync_staging WHERE run_id = $1", runID); err != nil {
		return fmt.Errorf("clear staging: %w", err)
	}
	return nil
}
