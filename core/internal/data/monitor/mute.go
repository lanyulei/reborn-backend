package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.MuteReader         = (*muteRepo)(nil)
	_ biz.MuteWriter         = (*muteRepo)(nil)
	_ biz.SubscriptionReader = (*subscriptionRepo)(nil)
	_ biz.SubscriptionWriter = (*subscriptionRepo)(nil)
)

// muteColumns 屏蔽规则查询列序（表别名 m）。smallint[] 读成 int[]，与领域模型的 []int32 对齐。
const muteColumns = `m.id::text, COALESCE(m.group_id::text, ''), m.name, m.reason, m.rule_ids::text[], m.datasource_ids::text[],
	m.severities::int[], m.matchers, m.mode, m.time_type, m.start_time, m.end_time, m.periodic, m.enabled, m.version,
	m.create_time, m.update_time, COALESCE(m.create_user::text, ''), COALESCE(m.update_user::text, '')`

func scanMute(scan func(dest ...any) error) (*biz.Mute, error) {
	var m biz.Mute
	s := &m.Spec
	var matchers, periodic []byte
	if err := scan(&m.ID, &s.GroupID, &s.Name, &s.Reason, &s.RuleIDs, &s.DatasourceIDs, &s.Severities, &matchers, &s.Mode,
		&s.TimeType, &s.StartTime, &s.EndTime, &periodic, &s.Enabled, &m.Version, &m.CreateTime, &m.UpdateTime, &m.CreateUser, &m.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(matchers, &s.Matchers, "mute matchers"); err != nil {
		return nil, err
	}
	if len(periodic) > 0 {
		s.Periodic = &mc.Effective{}
		if err := decodeJSON(periodic, s.Periodic, "mute periodic"); err != nil {
			return nil, err
		}
	}
	return &m, nil
}

type muteRepo struct {
	data *basedata.Data
}

// NewMuteRepo new a muteRepo.
func NewMuteRepo(data *basedata.Data) *muteRepo { return &muteRepo{data: data} }

// List implements biz.MuteReader.
func (r *muteRepo) List(ctx context.Context, filter biz.MuteFilter, page, pageSize int64) ([]*biz.Mute, int64, error) {
	var a sqlArgs
	where := "m.delete_time IS NULL"
	switch {
	case filter.Groups.All && !filter.IncludeGlobal:
		where += " AND m.group_id IS NOT NULL"
	case filter.Groups.All:
	case filter.IncludeGlobal:
		where += " AND (m.group_id IS NULL OR m.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[]))"
	default:
		where += " AND m.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[])"
	}
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (m.name ILIKE " + p + " OR m.reason ILIKE " + p + ")"
	}
	if filter.Enabled != nil {
		where += " AND m.enabled = " + a.add(*filter.Enabled)
	}
	if filter.Mode != nil {
		where += " AND m.mode = " + a.add(*filter.Mode)
	}
	switch {
	case filter.State == biz.MuteStateDisabled:
		where += " AND NOT m.enabled"
	case filter.State == biz.MuteStateExpired:
		where += " AND m.enabled AND m.end_time IS NOT NULL AND m.end_time <= now()"
	case filter.Live:
		where += " AND m.enabled AND (m.end_time IS NULL OR m.end_time > now())"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_mute m WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor mutes: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+muteColumns+" FROM monitor_mute m WHERE "+where+
		" ORDER BY m.create_time DESC, m.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list monitor mutes: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Mute, 0, min(pageSize, 256))
	for rows.Next() {
		m, err := scanMute(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan monitor mute: %w", err)
		}
		items = append(items, m)
	}
	return items, total, rows.Err()
}

// Get implements biz.MuteReader.
func (r *muteRepo) Get(ctx context.Context, id string) (*biz.Mute, error) {
	m, err := scanMute(r.data.Pool.QueryRow(ctx, "SELECT "+muteColumns+" FROM monitor_mute m WHERE m.id = $1 AND m.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrMuteNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor mute: %w", err)
	}
	return m, nil
}

// Names implements biz.MuteReader（含已删除的：历史事件仍要展示命中的屏蔽规则）。
func (r *muteRepo) Names(ctx context.Context, ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, name FROM monitor_mute WHERE id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor mute names: %w", err)
	}
	return collectNames(rows, "monitor mute name")
}

// muteArgs 屏蔽规则写入的列实参（顺序：group_id, name, reason, rule_ids, datasource_ids, severities,
// matchers, mode, time_type, start_time, end_time, periodic, enabled）。
func muteArgs(s *biz.MuteSpec) ([]any, error) {
	matchers := s.Matchers
	if matchers == nil {
		matchers = []mc.Matcher{}
	}
	rawMatchers, err := json.Marshal(matchers)
	if err != nil {
		return nil, fmt.Errorf("encode mute matchers: %w", err)
	}
	var periodic any
	if s.Periodic != nil {
		raw, err := json.Marshal(s.Periodic)
		if err != nil {
			return nil, fmt.Errorf("encode mute periodic: %w", err)
		}
		periodic = raw
	}
	return []any{
		dbx.NilIfEmpty(s.GroupID), s.Name, s.Reason, dbx.TextArray(s.RuleIDs), dbx.TextArray(s.DatasourceIDs), int32Array(s.Severities),
		rawMatchers, s.Mode, s.TimeType, s.StartTime, s.EndTime, periodic, s.Enabled,
	}, nil
}

// Create implements biz.MuteWriter.
func (r *muteRepo) Create(ctx context.Context, m *biz.Mute, actor string) error {
	args, err := muteArgs(&m.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if m.Spec.GroupID != "" {
			if err := lockGroup(ctx, tx, m.Spec.GroupID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO monitor_mute (id, create_user, update_user, group_id, name, reason, rule_ids, datasource_ids, severities,
			   matchers, mode, time_type, start_time, end_time, periodic, enabled)
			 VALUES ($1, $2, $2, $3, $4, $5, $6::uuid[], $7::uuid[], $8::int[]::smallint[], $9, $10, $11, $12, $13, $14, $15)`,
			append([]any{m.ID, dbx.NilIfEmpty(actor)}, args...)...); err != nil {
			return fmt.Errorf("insert monitor mute: %w", err)
		}
		return bumpCfg(ctx, tx, mc.CfgMute)
	})
}

// Update implements biz.MuteWriter.
func (r *muteRepo) Update(ctx context.Context, m *biz.Mute, expectVersion int32, actor string) error {
	args, err := muteArgs(&m.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if m.Spec.GroupID != "" {
			if err := lockGroup(ctx, tx, m.Spec.GroupID); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_mute SET group_id = $4, name = $5, reason = $6, rule_ids = $7::uuid[], datasource_ids = $8::uuid[],
			   severities = $9::int[]::smallint[], matchers = $10, mode = $11, time_type = $12, start_time = $13, end_time = $14,
			   periodic = $15, enabled = $16, version = version + 1, update_time = now(), update_user = $3
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
			append([]any{m.ID, expectVersion, dbx.NilIfEmpty(actor)}, args...)...)
		if err != nil {
			return fmt.Errorf("update monitor mute: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return missingOrConflict(ctx, tx, tableMute, m.ID, biz.ErrMuteNotFound)
		}
		return bumpCfg(ctx, tx, mc.CfgMute)
	})
}

// softDelete 带乐观锁的软删（表名为代码内常量）。
func softDelete(ctx context.Context, tx pgx.Tx, table, id string, expectVersion int32, actor string, notFound error) error {
	tag, err := tx.Exec(ctx,
		"UPDATE "+table+" SET delete_time = now(), delete_user = $3, version = version + 1 WHERE id = $1 AND version = $2 AND delete_time IS NULL",
		id, expectVersion, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete %s: %w", table, err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, tx, table, id, notFound)
	}
	return nil
}

// Delete implements biz.MuteWriter.
func (r *muteRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := softDelete(ctx, tx, tableMute, id, expectVersion, actor, biz.ErrMuteNotFound); err != nil {
			return err
		}
		return bumpCfg(ctx, tx, mc.CfgMute)
	})
}

// subscriptionColumns 订阅规则查询列序（表别名 s）。
const subscriptionColumns = `s.id::text, s.name, s.owner_id::text, s.group_ids::text[], s.rule_ids::text[], s.datasource_ids::text[],
	s.severities::int[], s.on_firing, s.on_resolved, s.matchers, s.receivers, s.follow_repeat, s.enabled, s.version,
	s.create_time, s.update_time`

func scanSubscription(scan func(dest ...any) error) (*biz.Subscription, error) {
	var sub biz.Subscription
	s := &sub.Spec
	var matchers, receivers []byte
	if err := scan(&sub.ID, &s.Name, &sub.OwnerID, &s.GroupIDs, &s.RuleIDs, &s.DatasourceIDs, &s.Severities, &s.OnFiring, &s.OnResolved,
		&matchers, &receivers, &s.FollowRepeat, &s.Enabled, &sub.Version, &sub.CreateTime, &sub.UpdateTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(matchers, &s.Matchers, "subscription matchers"); err != nil {
		return nil, err
	}
	if err := decodeJSON(receivers, &s.Receivers, "subscription receivers"); err != nil {
		return nil, err
	}
	return &sub, nil
}

type subscriptionRepo struct {
	data *basedata.Data
}

// NewSubscriptionRepo new a subscriptionRepo.
func NewSubscriptionRepo(data *basedata.Data) *subscriptionRepo { return &subscriptionRepo{data: data} }

func (r *subscriptionRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.Subscription, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+subscriptionColumns+" FROM monitor_subscription s WHERE s.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.Subscription, 0, 16)
	for rows.Next() {
		sub, err := scanSubscription(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, sub)
	}
	return items, rows.Err()
}

// List implements biz.SubscriptionReader.
func (r *subscriptionRepo) List(ctx context.Context, filter biz.SubscriptionFilter, page, pageSize int64) ([]*biz.Subscription, int64, error) {
	var a sqlArgs
	where := ""
	switch {
	case filter.OwnerID != "":
		where += " AND s.owner_id = " + a.add(filter.OwnerID)
	case !filter.Groups.All:
		// 自己的订阅恒可见；别人的要求订阅分组与可读分组有交集。
		where += " AND (s.owner_id = " + a.add(filter.ViewerID) + " OR s.group_ids && " + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[])"
	}
	if len(filter.CoverGroupIDs) > 0 {
		where += " AND s.group_ids && " + a.add(filter.CoverGroupIDs) + "::uuid[]"
	}
	if filter.Keyword != "" {
		where += " AND s.name ILIKE " + a.add(dbx.LikeContains(filter.Keyword))
	}
	if filter.Enabled != nil {
		where += " AND s.enabled = " + a.add(*filter.Enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_subscription s WHERE s.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor subscriptions: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, "list monitor subscriptions", where+" ORDER BY s.create_time DESC, s.id LIMIT "+limit+" OFFSET "+offset, a.args)
	return items, total, err
}

// Get implements biz.SubscriptionReader.
func (r *subscriptionRepo) Get(ctx context.Context, id string) (*biz.Subscription, error) {
	sub, err := scanSubscription(r.data.Pool.QueryRow(ctx,
		"SELECT "+subscriptionColumns+" FROM monitor_subscription s WHERE s.id = $1 AND s.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrSubscriptionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor subscription: %w", err)
	}
	return sub, nil
}

// candidateWhere 派发预筛的条件与实参（$1 分组、$2 规则、$3 数据源、$4 级别、$5 是否只要跟随重复通知的）。
// 分组条件走 idx_monitor_subscription_groups（GIN）。
func candidateWhere(c biz.SubscriptionCandidate) (string, []any) {
	event := "s.on_firing"
	if c.Resolved {
		event = "s.on_resolved"
	}
	return ` AND s.enabled AND ` + event + ` AND (s.follow_repeat OR NOT $5)
		  AND s.group_ids && $1::uuid[]
		  AND (cardinality(s.rule_ids) = 0 OR s.rule_ids @> ARRAY[$2::uuid])
		  AND (cardinality(s.datasource_ids) = 0 OR s.datasource_ids @> ARRAY[$3::uuid])
		  AND s.severities @> ARRAY[$4::int]::smallint[]`,
		[]any{dbx.TextArray(c.GroupIDs), c.RuleID, c.DatasourceID, c.Severity, c.RepeatOnly}
}

// ListCandidates implements biz.SubscriptionReader。标签匹配不在 SQL 里做（正则 / in 语义由
// monitorcontract 的匹配器统一实现）。命中数达到 limit 时再数一次总数——只在可能被截断时才付这次计数。
func (r *subscriptionRepo) ListCandidates(ctx context.Context, c biz.SubscriptionCandidate, limit int) ([]*biz.Subscription, int, error) {
	where, args := candidateWhere(c)
	items, err := r.query(ctx, "list monitor subscription candidates", where+" ORDER BY s.id LIMIT $6", append(args, limit))
	if err != nil || len(items) < limit {
		return items, len(items), err
	}
	var total int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_subscription s WHERE s.delete_time IS NULL"+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor subscription candidates: %w", err)
	}
	return items, total, nil
}

// subscriptionArgs 订阅规则写入的列实参（顺序：name, group_ids, rule_ids, datasource_ids, severities,
// on_firing, on_resolved, matchers, receivers, follow_repeat, enabled）。
func subscriptionArgs(s *biz.SubscriptionSpec) ([]any, error) {
	matchers := s.Matchers
	if matchers == nil {
		matchers = []mc.Matcher{}
	}
	rawMatchers, err := json.Marshal(matchers)
	if err != nil {
		return nil, fmt.Errorf("encode subscription matchers: %w", err)
	}
	receivers, err := json.Marshal(s.Receivers)
	if err != nil {
		return nil, fmt.Errorf("encode subscription receivers: %w", err)
	}
	return []any{
		s.Name, dbx.TextArray(s.GroupIDs), dbx.TextArray(s.RuleIDs), dbx.TextArray(s.DatasourceIDs), int32Array(s.Severities),
		s.OnFiring, s.OnResolved, rawMatchers, receivers, s.FollowRepeat, s.Enabled,
	}, nil
}

// Create implements biz.SubscriptionWriter。订阅只有 core 读，不 bump 配置版本。订阅的分组加共享锁：
// 与分组删除（删除前检查是否仍被订阅引用）互斥。
func (r *subscriptionRepo) Create(ctx context.Context, s *biz.Subscription) error {
	args, err := subscriptionArgs(&s.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockGroups(ctx, tx, s.Spec.GroupIDs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO monitor_subscription (id, owner_id, create_user, update_user, name, group_ids, rule_ids, datasource_ids, severities,
			   on_firing, on_resolved, matchers, receivers, follow_repeat, enabled)
			 VALUES ($1, $2, $2, $2, $3, $4::uuid[], $5::uuid[], $6::uuid[], $7::int[]::smallint[], $8, $9, $10, $11, $12, $13)`,
			append([]any{s.ID, s.OwnerID}, args...)...); err != nil {
			return fmt.Errorf("insert monitor subscription: %w", err)
		}
		return nil
	})
}

// Update implements biz.SubscriptionWriter。
func (r *subscriptionRepo) Update(ctx context.Context, s *biz.Subscription, expectVersion int32, actor string) error {
	args, err := subscriptionArgs(&s.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockGroups(ctx, tx, s.Spec.GroupIDs); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_subscription SET name = $4, group_ids = $5::uuid[], rule_ids = $6::uuid[], datasource_ids = $7::uuid[],
			   severities = $8::int[]::smallint[], on_firing = $9, on_resolved = $10, matchers = $11, receivers = $12,
			   follow_repeat = $13, enabled = $14, version = version + 1, update_time = now(), update_user = $3
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
			append([]any{s.ID, expectVersion, dbx.NilIfEmpty(actor)}, args...)...)
		if err != nil {
			return fmt.Errorf("update monitor subscription: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return missingOrConflict(ctx, tx, tableSubscription, s.ID, biz.ErrSubscriptionNotFound)
		}
		return nil
	})
}

// Delete implements biz.SubscriptionWriter.
func (r *subscriptionRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return softDelete(ctx, tx, tableSubscription, id, expectVersion, actor, biz.ErrSubscriptionNotFound)
	})
}
