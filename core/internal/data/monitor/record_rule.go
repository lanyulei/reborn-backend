package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RecordRuleReader       = (*recordRuleRepo)(nil)
	_ biz.RecordRuleWriter       = (*recordRuleRepo)(nil)
	_ biz.DatasourceWriteTargets = (*datasourceRepo)(nil)
)

// recordRuleColumns 记录规则查询列序（表别名 r，任务表别名 t：LEFT JOIN，引擎尚未物化任务时各列为 NULL）。
// monitor_record_task 归评估引擎写，core 只读。
const recordRuleColumns = `r.id::text, r.group_id::text, r.name, r.description, r.datasource_id::text, r.expr, r.interval_s, r.labels,
	r.enabled, r.version, r.create_time, r.update_time, COALESCE(r.update_user::text, ''),
	t.record_rule_id IS NOT NULL, COALESCE(t.last_status, 0), t.last_eval_time, COALESCE(t.last_error, ''),
	COALESCE(t.last_duration_ms, 0), COALESCE(t.last_series, 0), t.next_eval_time`

// recordRuleFrom 记录规则与其任务行。
const recordRuleFrom = ` FROM monitor_record_rule r LEFT JOIN monitor_record_task t ON t.record_rule_id = r.id WHERE r.delete_time IS NULL`

func scanRecordRule(scan func(dest ...any) error) (*biz.RecordRule, error) {
	var r biz.RecordRule
	s := &r.Spec
	var labels []byte
	var hasTask bool
	var task biz.RecordTask
	if err := scan(&r.ID, &s.GroupID, &s.Name, &s.Description, &s.DatasourceID, &s.Expr, &s.IntervalS, &labels,
		&s.Enabled, &r.Version, &r.CreateTime, &r.UpdateTime, &r.UpdateUser,
		&hasTask, &task.LastStatus, &task.LastEvalTime, &task.LastError, &task.LastDurationMS, &task.LastSeries, &task.NextEvalTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(labels, &s.Labels, "record rule labels"); err != nil {
		return nil, err
	}
	if hasTask {
		r.Task = &task
	}
	return &r, nil
}

type recordRuleRepo struct {
	data *basedata.Data
}

// NewRecordRuleRepo new a recordRuleRepo.
func NewRecordRuleRepo(data *basedata.Data) *recordRuleRepo { return &recordRuleRepo{data: data} }

// List implements biz.RecordRuleReader。执行状态按展示口径过滤：停用的规则与没有任务行的规则都算 0。
func (r *recordRuleRepo) List(ctx context.Context, filter biz.RecordRuleFilter, page, pageSize int64) ([]*biz.RecordRule, int64, error) {
	var a sqlArgs
	where := groupCond(&a, "r.group_id", filter.Groups)
	if filter.DatasourceID != "" {
		where += " AND r.datasource_id = " + a.add(filter.DatasourceID)
	}
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (r.name ILIKE " + p + " OR r.description ILIKE " + p + " OR r.expr ILIKE " + p + ")"
	}
	if filter.Enabled != nil {
		where += " AND r.enabled = " + a.add(*filter.Enabled)
	}
	if filter.LastStatus != nil {
		where += " AND (CASE WHEN r.enabled THEN COALESCE(t.last_status, 0) ELSE 0 END) = " + a.add(*filter.LastStatus)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+recordRuleFrom+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor record rules: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+recordRuleColumns+recordRuleFrom+where+
		" ORDER BY r.update_time DESC, r.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list monitor record rules: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RecordRule, 0, min(pageSize, 256))
	for rows.Next() {
		rule, err := scanRecordRule(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan monitor record rule: %w", err)
		}
		items = append(items, rule)
	}
	return items, total, rows.Err()
}

// Get implements biz.RecordRuleReader.
func (r *recordRuleRepo) Get(ctx context.Context, id string) (*biz.RecordRule, error) {
	rule, err := scanRecordRule(r.data.Pool.QueryRow(ctx, "SELECT "+recordRuleColumns+recordRuleFrom+" AND r.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRecordRuleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor record rule: %w", err)
	}
	return rule, nil
}

// FindByName implements biz.RecordRuleReader（走唯一索引 uk_monitor_record_rule_name）。
func (r *recordRuleRepo) FindByName(ctx context.Context, groupID, datasourceID, name string) (string, error) {
	var id string
	err := r.data.Pool.QueryRow(ctx,
		"SELECT id::text FROM monitor_record_rule WHERE group_id = $1 AND datasource_id = $2 AND name = $3 AND delete_time IS NULL",
		groupID, datasourceID, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("find monitor record rule by name: %w", err)
	}
	return id, nil
}

func recordRuleWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_monitor_record_rule_name") {
		return biz.ErrRecordRuleNameExists
	}
	return fmt.Errorf("%s monitor record rule: %w", what, err)
}

// lockRecordRefs 写记录规则前确认分组与数据源仍然存在并加共享锁（与它们的删除互斥）。数据源行的共享锁
// 同时挡住「清除 Remote Write 地址」的并发更新（那边持行锁检查是否还有启用的记录规则），所以在锁内再
// 确认一次：启用的规则要求数据源此刻配置了写入地址。
func lockRecordRefs(ctx context.Context, tx pgx.Tx, s *biz.RecordRuleSpec) error {
	if err := lockGroup(ctx, tx, s.GroupID); err != nil {
		return err
	}
	var writable bool
	err := tx.QueryRow(ctx,
		"SELECT remote_write_url <> '' FROM monitor_datasource WHERE id = $1 AND delete_time IS NULL FOR SHARE", s.DatasourceID).Scan(&writable)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.Detail(biz.ErrRecordRuleInvalid, "the selected datasource no longer exists")
	}
	if err != nil {
		return fmt.Errorf("lock monitor datasource: %w", err)
	}
	if s.Enabled && !writable {
		return biz.ErrRecordRuleNoWriteURL
	}
	return nil
}

func encodeRecordLabels(labels map[string]string) ([]byte, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	raw, err := json.Marshal(labels)
	if err != nil {
		return nil, fmt.Errorf("encode record rule labels: %w", err)
	}
	return raw, nil
}

// Create implements biz.RecordRuleWriter.
func (r *recordRuleRepo) Create(ctx context.Context, rule *biz.RecordRule, actor string) error {
	s := &rule.Spec
	labels, err := encodeRecordLabels(s.Labels)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRecordRefs(ctx, tx, s); err != nil {
			return err
		}
		err := tx.QueryRow(ctx,
			`INSERT INTO monitor_record_rule (id, group_id, name, description, datasource_id, expr, interval_s, labels, enabled, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10) RETURNING version`,
			rule.ID, s.GroupID, s.Name, s.Description, s.DatasourceID, s.Expr, s.IntervalS, labels, s.Enabled, dbx.NilIfEmpty(actor)).
			Scan(&rule.Version)
		if err != nil {
			return recordRuleWriteErr(err, "insert")
		}
		return nil
	})
}

// Update implements biz.RecordRuleWriter。乐观锁：version 不一致时不写入。
func (r *recordRuleRepo) Update(ctx context.Context, rule *biz.RecordRule, expectVersion int32, actor string) error {
	s := &rule.Spec
	labels, err := encodeRecordLabels(s.Labels)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRecordRefs(ctx, tx, s); err != nil {
			return err
		}
		err := tx.QueryRow(ctx,
			`UPDATE monitor_record_rule SET group_id = $3, name = $4, description = $5, datasource_id = $6, expr = $7, interval_s = $8,
			   labels = $9, enabled = $10, version = version + 1, update_time = now(), update_user = $11
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL RETURNING version`,
			rule.ID, expectVersion, s.GroupID, s.Name, s.Description, s.DatasourceID, s.Expr, s.IntervalS, labels, s.Enabled, dbx.NilIfEmpty(actor)).
			Scan(&rule.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return missingOrConflict(ctx, tx, tableRecordRule, rule.ID, biz.ErrRecordRuleNotFound)
		}
		if err != nil {
			return recordRuleWriteErr(err, "update")
		}
		return nil
	})
}

// Delete implements biz.RecordRuleWriter.
func (r *recordRuleRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return softDelete(ctx, tx, tableRecordRule, id, expectVersion, actor, biz.ErrRecordRuleNotFound)
	})
}

// WriteTargets implements biz.DatasourceWriteTargets.
func (r *datasourceRepo) WriteTargets(ctx context.Context, ids []string) (map[string]biz.DatasourceWriteTarget, error) {
	out := make(map[string]biz.DatasourceWriteTarget, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, name, enabled, remote_write_url <> '' FROM monitor_datasource WHERE id = ANY($1::uuid[]) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor datasource write targets: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var t biz.DatasourceWriteTarget
		if err := rows.Scan(&id, &t.Name, &t.Enabled, &t.HasWriteURL); err != nil {
			return nil, fmt.Errorf("scan monitor datasource write target: %w", err)
		}
		out[id] = t
	}
	return out, rows.Err()
}

// recordRulesUsing 引用该数据源的未删除记录规则（走 idx_monitor_record_rule_datasource）：至多 inUseListed 个
// 名称与总数。enabledOnly 时只算启用的。调用方已持有数据源行的行锁。
func recordRulesUsing(ctx context.Context, tx pgx.Tx, datasourceID string, enabledOnly bool) ([]string, int, error) {
	rows, err := tx.Query(ctx,
		`SELECT name, count(*) OVER () FROM monitor_record_rule
		 WHERE datasource_id = $1 AND delete_time IS NULL AND (enabled OR NOT $2) ORDER BY name, id LIMIT $3`,
		datasourceID, enabledOnly, inUseListed)
	if err != nil {
		return nil, 0, fmt.Errorf("list record rules using monitor datasource: %w", err)
	}
	defer rows.Close()
	var names []string
	var total int
	for rows.Next() {
		var name string
		if err := rows.Scan(&name, &total); err != nil {
			return nil, 0, fmt.Errorf("scan record rule using monitor datasource: %w", err)
		}
		names = append(names, name)
	}
	return names, total, rows.Err()
}
