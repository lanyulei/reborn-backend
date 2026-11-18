package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RuleReader        = (*ruleRepo)(nil)
	_ biz.RuleLookup        = (*ruleRepo)(nil)
	_ biz.RuleWriter        = (*ruleRepo)(nil)
	_ biz.RuleVersionReader = (*ruleVersionRepo)(nil)
)

// ruleColumns 规则查询列序（表别名 r）。
const ruleColumns = `r.id::text, r.group_id::text, r.name, r.description, r.enabled, r.labels, r.datasource_scope,
	r.datasource_ids::text[], r.queries, r.level_inhibit, r.eval_interval_s, r.for_s, r.recover_s, r.nodata_keep_s,
	r.effective, r.annotations, r.recovery_expr, r.notify, r.notify_recovery, r.repeat_interval_m, r.max_notify,
	r.group_key_labels, r.version, r.spec_time, r.create_time, r.update_time, COALESCE(r.update_user::text, ''),
	r.ci_match, r.resource_scope, r.relabel, r.drop_expr, r.topology_inhibit, COALESCE(r.escalation_id::text, ''), r.actions`

// decodeJSON 解码 jsonb 列；what 用于错误说明。
func decodeJSON(raw []byte, dst any, what string) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode %s: %w", what, err)
	}
	return nil
}

func scanRule(scan func(dest ...any) error) (*biz.Rule, error) {
	var r biz.Rule
	s := &r.Spec
	var labels, queries, effective, annotations, notify, ciMatch, scope, relabel, actions []byte
	if err := scan(&r.ID, &s.GroupID, &s.Name, &s.Description, &s.Enabled, &labels, &s.DatasourceScope,
		&s.DatasourceIDs, &queries, &s.LevelInhibit, &s.EvalIntervalS, &s.ForS, &s.RecoverS, &s.NodataKeepS,
		&effective, &annotations, &s.RecoveryExpr, &notify, &s.NotifyRecovery, &s.RepeatIntervalM, &s.MaxNotify,
		&s.GroupKeyLabels, &r.Version, &r.SpecTime, &r.CreateTime, &r.UpdateTime, &r.UpdateUser,
		&ciMatch, &scope, &relabel, &s.DropExpr, &s.TopologyInhibit, &s.EscalationID, &actions); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		raw  []byte
		dst  any
		what string
	}{
		{labels, &s.Labels, "rule labels"},
		{queries, &s.Queries, "rule queries"},
		{effective, &s.Effective, "rule effective"},
		{annotations, &s.Annotations, "rule annotations"},
		{notify, &s.Notify, "rule notify"},
		{ciMatch, &s.CIMatch, "rule ci_match"},
		{scope, &s.ResourceScope, "rule resource_scope"},
		{relabel, &s.Relabel, "rule relabel"},
		{actions, &s.Actions, "rule actions"},
	} {
		if err := decodeJSON(f.raw, f.dst, f.what); err != nil {
			return nil, err
		}
	}
	return &r, nil
}

// ruleJSON 规则的 jsonb 列实参。
type ruleJSON struct {
	labels, queries, effective, annotations, notify, snapshot []byte
	// 第二、三期的列。
	ciMatch, scope, relabel, actions []byte
}

// p2Args 第二、三期各列的实参，顺序：ci_match, resource_scope, relabel, drop_expr, topology_inhibit,
// escalation_id, actions（Create / Update 的列清单末尾按这个顺序排）。
func (j *ruleJSON) p2Args(s *biz.RuleSpec) []any {
	return []any{j.ciMatch, j.scope, j.relabel, s.DropExpr, s.TopologyInhibit, dbx.NilIfEmpty(s.EscalationID), j.actions}
}

func encodeRule(spec *biz.RuleSpec) (ruleJSON, error) {
	var out ruleJSON
	for _, f := range []struct {
		dst *[]byte
		src any
	}{
		{&out.labels, spec.Labels},
		{&out.queries, spec.Queries},
		{&out.effective, spec.Effective},
		{&out.annotations, spec.Annotations},
		{&out.notify, spec.Notify},
		{&out.ciMatch, spec.CIMatch},
		{&out.scope, spec.ResourceScope},
		{&out.relabel, relabelOrEmpty(spec.Relabel)},
		{&out.actions, spec.Actions},
		{&out.snapshot, spec},
	} {
		raw, err := json.Marshal(f.src)
		if err != nil {
			return out, fmt.Errorf("encode rule: %w", err)
		}
		*f.dst = raw
	}
	return out, nil
}

// relabelOrEmpty nil 切片落空数组（列有 jsonb_typeof = 'array' 的 CHECK，不能写 null）。
func relabelOrEmpty(steps []mc.RelabelStep) []mc.RelabelStep {
	if steps == nil {
		return []mc.RelabelStep{}
	}
	return steps
}

type ruleRepo struct {
	data *basedata.Data
}

// NewRuleRepo new a ruleRepo.
func NewRuleRepo(data *basedata.Data) *ruleRepo { return &ruleRepo{data: data} }

func (r *ruleRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.Rule, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ruleColumns+" FROM monitor_rule r WHERE r.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.Rule, 0, 32)
	for rows.Next() {
		rule, err := scanRule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, rule)
	}
	return items, rows.Err()
}

// List implements biz.RuleReader.
func (r *ruleRepo) List(ctx context.Context, filter biz.RuleFilter, page, pageSize int64) ([]*biz.Rule, int64, error) {
	var a sqlArgs
	where := groupCond(&a, "r.group_id", filter.Groups)
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (r.name ILIKE " + p + " OR r.description ILIKE " + p + " OR r.queries::text ILIKE " + p + ")"
	}
	if filter.Severity != nil {
		where += " AND r.queries @> jsonb_build_array(jsonb_build_object('severity', " + a.add(*filter.Severity) + "::int))"
	}
	if filter.DatasourceID != "" {
		where += fmt.Sprintf(" AND (r.datasource_scope = %d OR r.datasource_ids @> ARRAY[%s::uuid])", mc.DatasourceScopeAll, a.add(filter.DatasourceID))
	}
	if filter.Enabled != nil {
		where += " AND r.enabled = " + a.add(*filter.Enabled)
	}
	if filter.Firing != nil {
		where += " AND " + a.add(*filter.Firing) + fmt.Sprintf(" = EXISTS (SELECT 1 FROM monitor_alert al WHERE al.rule_id = r.id AND al.status = %d)", mc.AlertFiring)
	}
	if filter.LastStatus != nil {
		where += " AND EXISTS (SELECT 1 FROM monitor_eval_task t WHERE t.rule_id = r.id AND t.last_status = " + a.add(*filter.LastStatus) + ")"
	}
	if filter.EscalationID != "" {
		// 走 idx_monitor_rule_escalation。
		where += " AND r.escalation_id = " + a.add(filter.EscalationID) + "::uuid"
	}
	if len(filter.Labels) > 0 {
		raw, err := json.Marshal(filter.Labels)
		if err != nil {
			return nil, 0, fmt.Errorf("encode label filter: %w", err)
		}
		where += " AND r.labels @> " + a.add(raw) + "::jsonb"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_rule r WHERE r.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor rules: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, "list monitor rules", where+" ORDER BY r.update_time DESC, r.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	return items, total, err
}

// Get implements biz.RuleReader.
func (r *ruleRepo) Get(ctx context.Context, id string) (*biz.Rule, error) {
	rule, err := scanRule(r.data.Pool.QueryRow(ctx,
		"SELECT "+ruleColumns+" FROM monitor_rule r WHERE r.id = $1 AND r.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRuleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor rule: %w", err)
	}
	return rule, nil
}

// ListByIDs implements biz.RuleReader.
func (r *ruleRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Rule, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return r.query(ctx, "list monitor rules by ids", " AND r.id = ANY($1::uuid[])", ids)
}

// Names implements biz.RuleLookup.
func (r *ruleRepo) Names(ctx context.Context, ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, name FROM monitor_rule WHERE id = ANY($1::uuid[]) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor rule names: %w", err)
	}
	return collectNames(rows, "monitor rule name")
}

// FindByNames implements biz.RuleLookup.
func (r *ruleRepo) FindByNames(ctx context.Context, groupID string, names []string) (map[string]string, error) {
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT name, id::text FROM monitor_rule WHERE group_id = $1 AND name = ANY($2::text[]) AND delete_time IS NULL", groupID, names)
	if err != nil {
		return nil, fmt.Errorf("find monitor rules by names: %w", err)
	}
	return collectNames(rows, "monitor rule by name")
}

// ListByGroups implements biz.RuleLookup.
func (r *ruleRepo) ListByGroups(ctx context.Context, groups biz.GroupFilter, limit int) ([]*biz.Rule, error) {
	if groups.Empty() {
		return nil, nil
	}
	var a sqlArgs
	where := groupCond(&a, "r.group_id", groups)
	return r.query(ctx, "list monitor rules by groups", where+" ORDER BY r.group_id, r.name, r.id LIMIT "+a.add(limit), a.args...)
}

func ruleWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_monitor_rule_name") {
		return biz.ErrRuleNameExists
	}
	return fmt.Errorf("%s monitor rule: %w", what, err)
}

// insertVersion 追加一条版本快照（与规则写入同事务）。
func insertVersion(ctx context.Context, tx pgx.Tx, ruleID string, version int32, snapshot []byte, change biz.RuleChange) error {
	id, err := idgen.NewID()
	if err != nil {
		return fmt.Errorf("generate rule version id: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO monitor_rule_version (id, rule_id, version, snapshot, action, summary, operator) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		id, ruleID, version, snapshot, change.Action, change.Summary, dbx.NilIfEmpty(change.Operator)); err != nil {
		return fmt.Errorf("insert monitor rule version: %w", err)
	}
	return nil
}

// lockRefs 写规则前确认所属分组与指定的数据源仍然存在，并加共享锁（与它们的删除互斥）。
func lockRefs(ctx context.Context, tx pgx.Tx, ruleID string, spec *biz.RuleSpec) error {
	if err := lockGroup(ctx, tx, spec.GroupID); err != nil {
		return err
	}
	if err := lockDatasources(ctx, tx, spec.DatasourceIDs); err != nil {
		return err
	}
	return lockEscalation(ctx, tx, ruleID, spec.EscalationID)
}

// lockEscalation 给规则引用的升级策略加共享锁，与策略的删除（行锁 + 事务内统计引用数）互斥：删除在先时
// 这里读不到策略，删除在后时它会等本事务提交再数到这条规则。策略已不存在时，只有规则原本就引用着它
// （引用关系不设外键，策略删除前的校验保证不了历史数据）才放行，新引用一律拒绝。
func lockEscalation(ctx context.Context, tx pgx.Tx, ruleID, escalationID string) error {
	if escalationID == "" {
		return nil
	}
	var kept bool
	err := tx.QueryRow(ctx, "SELECT true FROM monitor_escalation WHERE id = $1 AND delete_time IS NULL FOR SHARE", escalationID).Scan(&kept)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock monitor escalation: %w", err)
	}
	if err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM monitor_rule WHERE id = $1 AND escalation_id = $2 AND delete_time IS NULL)", ruleID, escalationID).Scan(&kept); err != nil {
		return fmt.Errorf("check monitor rule escalation: %w", err)
	}
	if !kept {
		return biz.ErrRuleEscalationMissing
	}
	return nil
}

// Create implements biz.RuleWriter.
func (r *ruleRepo) Create(ctx context.Context, rule *biz.Rule, change biz.RuleChange) error {
	s := &rule.Spec
	enc, err := encodeRule(s)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRefs(ctx, tx, rule.ID, s); err != nil {
			return err
		}
		err := tx.QueryRow(ctx,
			`INSERT INTO monitor_rule (id, group_id, name, description, enabled, labels, datasource_scope, datasource_ids, queries,
			   level_inhibit, eval_interval_s, for_s, recover_s, nodata_keep_s, effective, annotations, recovery_expr, notify,
			   notify_recovery, repeat_interval_m, max_notify, group_key_labels, create_user, update_user,
			   ci_match, resource_scope, relabel, drop_expr, topology_inhibit, escalation_id, actions)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::uuid[], $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $23,
			   $24, $25, $26, $27, $28, $29, $30)
			 RETURNING version, spec_time, create_time, update_time`,
			append([]any{
				rule.ID, s.GroupID, s.Name, s.Description, s.Enabled, enc.labels, s.DatasourceScope, dbx.TextArray(s.DatasourceIDs), enc.queries,
				s.LevelInhibit, s.EvalIntervalS, s.ForS, s.RecoverS, s.NodataKeepS, enc.effective, enc.annotations, s.RecoveryExpr, enc.notify,
				s.NotifyRecovery, s.RepeatIntervalM, s.MaxNotify, dbx.TextArray(s.GroupKeyLabels), dbx.NilIfEmpty(change.Operator),
			}, enc.p2Args(s)...)...).
			Scan(&rule.Version, &rule.SpecTime, &rule.CreateTime, &rule.UpdateTime)
		if err != nil {
			return ruleWriteErr(err, "insert")
		}
		return insertVersion(ctx, tx, rule.ID, rule.Version, enc.snapshot, change)
	})
}

// Update implements biz.RuleWriter。乐观锁：version 不一致时不写入、不留版本。
func (r *ruleRepo) Update(ctx context.Context, rule *biz.Rule, expectVersion int32, change biz.RuleChange) error {
	s := &rule.Spec
	enc, err := encodeRule(s)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockRefs(ctx, tx, rule.ID, s); err != nil {
			return err
		}
		err := tx.QueryRow(ctx,
			`UPDATE monitor_rule SET group_id = $3, name = $4, description = $5, enabled = $6, labels = $7, datasource_scope = $8,
			   datasource_ids = $9::uuid[], queries = $10, level_inhibit = $11, eval_interval_s = $12, for_s = $13, recover_s = $14,
			   nodata_keep_s = $15, effective = $16, annotations = $17, recovery_expr = $18, notify = $19, notify_recovery = $20,
			   repeat_interval_m = $21, max_notify = $22, group_key_labels = $23,
			   spec_time = CASE WHEN $24 THEN now() ELSE spec_time END,
			   version = version + 1, update_time = now(), update_user = $25,
			   ci_match = $26, resource_scope = $27, relabel = $28, drop_expr = $29, topology_inhibit = $30,
			   escalation_id = $31, actions = $32
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL
			 RETURNING version, spec_time, update_time`,
			append([]any{
				rule.ID, expectVersion, s.GroupID, s.Name, s.Description, s.Enabled, enc.labels, s.DatasourceScope,
				dbx.TextArray(s.DatasourceIDs), enc.queries, s.LevelInhibit, s.EvalIntervalS, s.ForS, s.RecoverS,
				s.NodataKeepS, enc.effective, enc.annotations, s.RecoveryExpr, enc.notify, s.NotifyRecovery,
				s.RepeatIntervalM, s.MaxNotify, dbx.TextArray(s.GroupKeyLabels), change.BumpSpecTime, dbx.NilIfEmpty(change.Operator),
			}, enc.p2Args(s)...)...).
			Scan(&rule.Version, &rule.SpecTime, &rule.UpdateTime)
		if errors.Is(err, pgx.ErrNoRows) {
			return missingOrConflict(ctx, tx, tableRule, rule.ID, biz.ErrRuleNotFound)
		}
		if err != nil {
			return ruleWriteErr(err, "update")
		}
		return insertVersion(ctx, tx, rule.ID, rule.Version, enc.snapshot, change)
	})
}

// Delete implements biz.RuleWriter。软删并留一条 delete 版本（快照为删除前的内容）。
func (r *ruleRepo) Delete(ctx context.Context, rule *biz.Rule, change biz.RuleChange) error {
	enc, err := encodeRule(&rule.Spec)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var version int32
		err := tx.QueryRow(ctx,
			`UPDATE monitor_rule SET delete_time = now(), delete_user = $3, version = version + 1, update_time = now()
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL RETURNING version`,
			rule.ID, rule.Version, dbx.NilIfEmpty(change.Operator)).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return missingOrConflict(ctx, tx, tableRule, rule.ID, biz.ErrRuleNotFound)
		}
		if err != nil {
			return fmt.Errorf("delete monitor rule: %w", err)
		}
		return insertVersion(ctx, tx, rule.ID, version, enc.snapshot, change)
	})
}

type ruleVersionRepo struct {
	data *basedata.Data
}

// NewRuleVersionRepo new a ruleVersionRepo.
func NewRuleVersionRepo(data *basedata.Data) *ruleVersionRepo { return &ruleVersionRepo{data: data} }

// List implements biz.RuleVersionReader.
func (r *ruleVersionRepo) List(ctx context.Context, ruleID string, page, pageSize int64) ([]*biz.RuleVersion, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_rule_version WHERE rule_id = $1", ruleID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor rule versions: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT rule_id::text, version, action, summary, COALESCE(operator::text, ''), create_time
		 FROM monitor_rule_version WHERE rule_id = $1 ORDER BY version DESC LIMIT $2 OFFSET $3`, ruleID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list monitor rule versions: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RuleVersion, 0, pageSize)
	for rows.Next() {
		var v biz.RuleVersion
		if err := rows.Scan(&v.RuleID, &v.Version, &v.Action, &v.Summary, &v.OperatorID, &v.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan monitor rule version: %w", err)
		}
		items = append(items, &v)
	}
	return items, total, rows.Err()
}

// Get implements biz.RuleVersionReader.
func (r *ruleVersionRepo) Get(ctx context.Context, ruleID string, version int32) (*biz.RuleVersion, error) {
	var v biz.RuleVersion
	var snapshot []byte
	err := r.data.Pool.QueryRow(ctx,
		`SELECT rule_id::text, version, action, summary, COALESCE(operator::text, ''), create_time, snapshot
		 FROM monitor_rule_version WHERE rule_id = $1 AND version = $2`, ruleID, version).
		Scan(&v.RuleID, &v.Version, &v.Action, &v.Summary, &v.OperatorID, &v.CreateTime, &snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRuleVersionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor rule version: %w", err)
	}
	v.Spec = &biz.RuleSpec{}
	if err := decodeJSON(snapshot, v.Spec, "rule snapshot"); err != nil {
		return nil, err
	}
	return &v, nil
}
