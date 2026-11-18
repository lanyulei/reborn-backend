package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.RecordRepo = (*recordRepo)(nil)

// recordRepo 记录规则任务：领取 / 归还是 taskRepo 在 monitor_record_task 上的实例（与评估任务同一份语句），
// 这里只多出规则读取与结果写回。
type recordRepo struct {
	*taskRepo
}

// NewRecordRepo new a recordRepo.
func NewRecordRepo(data *Data) *recordRepo {
	return &recordRepo{taskRepo: &taskRepo{data: data, table: recordTasks}}
}

// LoadRecordRule implements biz.RecordRepo.
func (r *recordRepo) LoadRecordRule(ctx context.Context, id string) (*biz.RecordRule, time.Time, error) {
	var (
		rule      = biz.RecordRule{ID: id}
		now       time.Time
		labels    []byte
		intervalS int32
	)
	err := r.data.Pool.QueryRow(ctx,
		`SELECT name, datasource_id::text, expr, interval_s, labels, enabled AND delete_time IS NULL, now()
		 FROM monitor_record_rule WHERE id = $1`, id).
		Scan(&rule.Name, &rule.DatasourceID, &rule.Expr, &intervalS, &labels, &rule.Active, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, time.Time{}, biz.ErrRuleNotFound
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("load record rule: %w", err)
	}
	if err := json.Unmarshal(labels, &rule.Labels); err != nil {
		return nil, time.Time{}, fmt.Errorf("decode record rule labels: %w", err)
	}
	rule.Interval = time.Duration(intervalS) * time.Second
	return &rule, now, nil
}

// Finish implements biz.RecordRepo：围栏在写回语句上（影响 0 行 = 租约已失或任务已被规划器删除）。
func (r *recordRepo) Finish(ctx context.Context, task *biz.Task, res *biz.RecordResult) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE monitor_record_task
		 SET next_eval_time = now() + $3::interval, lease_owner = NULL, lease_until = NULL, last_eval_time = now(),
		     last_status = $4, last_error = $5, last_duration_ms = $6, last_series = $7
		 WHERE record_rule_id = $1 AND lease_owner = $2`,
		task.RuleID, task.Token, res.Interval, res.Status, res.Error, res.Duration.Milliseconds(), res.Series)
	if err != nil {
		return fmt.Errorf("write back record task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return biz.ErrLeaseLost
	}
	return nil
}
