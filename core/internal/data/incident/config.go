package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RuleRepo     = (*ruleRepo)(nil)
	_ biz.BusinessRepo = (*businessRepo)(nil)
	_ biz.PlaybookRepo = (*playbookRepo)(nil)
	_ biz.SourceRepo   = (*sourceRepo)(nil)
	_ biz.SettingRepo  = (*settingRepo)(nil)
)

// missingOrConflict 乐观锁更新没有命中任何行时区分原因。
func missingOrConflict(ctx context.Context, q basedata.Queryer, table, id string, notFound error) error {
	var exists bool
	if err := q.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+table+" WHERE id = $1 AND delete_time IS NULL)", id).Scan(&exists); err != nil {
		return fmt.Errorf("check %s exists: %w", table, err)
	}
	if !exists {
		return notFound
	}
	return biz.ErrVersionConflict
}

// softDelete 带乐观锁的软删（表名为代码内常量）。
func (r *ruleRepo) softDelete(ctx context.Context, table, id string, expectVersion int32, actor string, notFound error) error {
	return softDeleteRow(ctx, r.data, table, id, expectVersion, actor, notFound)
}

func softDeleteRow(ctx context.Context, data *basedata.Data, table, id string, expectVersion int32, actor string, notFound error) error {
	tag, err := data.Pool.Exec(ctx,
		"UPDATE "+table+" SET delete_time = now(), delete_user = $3, version = version + 1 WHERE id = $1 AND version = $2 AND delete_time IS NULL",
		id, expectVersion, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete %s: %w", table, err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, data.Pool, table, id, notFound)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 触发规则
// ---------------------------------------------------------------------------

type ruleRepo struct {
	data *basedata.Data
}

// NewRuleRepo new a ruleRepo.
func NewRuleRepo(data *basedata.Data) *ruleRepo { return &ruleRepo{data: data} }

const ruleColumns = "r.id::text, r.spec, r.version, r.create_time, r.update_time, COALESCE(r.create_user::text, ''), COALESCE(r.update_user::text, '')"

func scanRule(scan func(dest ...any) error) (*biz.Rule, error) {
	var ru biz.Rule
	var spec []byte
	if err := scan(&ru.ID, &spec, &ru.Version, &ru.CreateTime, &ru.UpdateTime, &ru.CreateUser, &ru.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(spec, &ru.Spec, "incident rule spec"); err != nil {
		return nil, err
	}
	return &ru, nil
}

func (r *ruleRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.Rule, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ruleColumns+" FROM incident_rule r WHERE r.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Rule, 0, 16)
	for rows.Next() {
		ru, err := scanRule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, ru)
	}
	return out, rows.Err()
}

// List implements biz.RuleRepo.
func (r *ruleRepo) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*biz.Rule, int64, error) {
	var a sqlArgs
	where := ""
	if keyword != "" {
		p := a.add(dbx.LikeContains(keyword))
		where += " AND (r.name ILIKE " + p + " OR r.description ILIKE " + p + ")"
	}
	if enabled != nil {
		where += " AND r.enabled = " + a.add(*enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_rule r WHERE r.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident rules: %w", err)
	}
	items, err := r.query(ctx, "list incident rules", where+" ORDER BY r.sort, r.create_time LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args)
	return items, total, err
}

// ListEnabled implements biz.RuleRepo.
func (r *ruleRepo) ListEnabled(ctx context.Context) ([]*biz.Rule, error) {
	return r.query(ctx, "list enabled incident rules", " AND r.enabled ORDER BY r.sort, r.create_time", nil)
}

// Get implements biz.RuleRepo.
func (r *ruleRepo) Get(ctx context.Context, id string) (*biz.Rule, error) {
	ru, err := scanRule(r.data.Pool.QueryRow(ctx, "SELECT "+ruleColumns+" FROM incident_rule r WHERE r.id = $1 AND r.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRuleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident rule: %w", err)
	}
	return ru, nil
}

// ListByIDs implements biz.RuleRepo（含已删除：历史故障仍要显示规则名）。
func (r *ruleRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Rule, error) {
	if len(ids) == 0 {
		return []*biz.Rule{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ruleColumns+" FROM incident_rule r WHERE r.id = ANY($1::uuid[])", textArray(ids))
	if err != nil {
		return nil, fmt.Errorf("list incident rules by ids: %w", err)
	}
	defer rows.Close()
	var out []*biz.Rule
	for rows.Next() {
		ru, err := scanRule(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, ru)
	}
	return out, rows.Err()
}

// Create implements biz.RuleRepo.
func (r *ruleRepo) Create(ctx context.Context, ru *biz.Rule, actor string) error {
	spec, err := encodeJSON(ru.Spec, "incident rule spec")
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO incident_rule (id, name, description, sort, enabled, spec, create_user, update_user) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		ru.ID, ru.Spec.Name, ru.Spec.Description, ru.Spec.Sort, ru.Spec.Enabled, spec, nilIfEmpty(actor)); err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrRuleNameExists
		}
		return fmt.Errorf("insert incident rule: %w", err)
	}
	return nil
}

// Update implements biz.RuleRepo.
func (r *ruleRepo) Update(ctx context.Context, ru *biz.Rule, expectVersion int32, actor string) error {
	spec, err := encodeJSON(ru.Spec, "incident rule spec")
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE incident_rule SET name = $3, description = $4, sort = $5, enabled = $6, spec = $7, version = version + 1, update_time = now(), update_user = $8
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`, ru.ID, expectVersion, ru.Spec.Name, ru.Spec.Description, ru.Spec.Sort, ru.Spec.Enabled, spec, nilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrRuleNameExists
		}
		return fmt.Errorf("update incident rule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, r.data.Pool, "incident_rule", ru.ID, biz.ErrRuleNotFound)
	}
	return nil
}

// Delete implements biz.RuleRepo.
func (r *ruleRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return r.softDelete(ctx, "incident_rule", id, expectVersion, actor, biz.ErrRuleNotFound)
}

// CountPlaybookRefs implements biz.RuleRepo.
func (r *ruleRepo) CountPlaybookRefs(ctx context.Context, playbookID string) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_rule WHERE delete_time IS NULL AND spec->'action'->>'playbook_id' = $1", playbookID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count rule playbook refs: %w", err)
	}
	return n, nil
}

// CountBusinessRefs implements biz.RuleRepo.
func (r *ruleRepo) CountBusinessRefs(ctx context.Context, businessID string) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_rule WHERE delete_time IS NULL AND spec->'incident'->>'business_id' = $1", businessID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count rule business refs: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// 业务字典
// ---------------------------------------------------------------------------

type businessRepo struct {
	data *basedata.Data
}

// NewBusinessRepo new a businessRepo.
func NewBusinessRepo(data *basedata.Data) *businessRepo { return &businessRepo{data: data} }

const businessColumns = `b.id::text, b.code, b.name, b.description, COALESCE(b.owner_id::text, ''), COALESCE(b.schedule_id::text, ''), b.groups,
	COALESCE(b.ci_id::text, ''), b.label_values, b.sort, b.enabled, b.version, b.create_time, b.update_time, COALESCE(b.create_user::text, ''), COALESCE(b.update_user::text, '')`

func scanBusiness(scan func(dest ...any) error) (*biz.Business, error) {
	var b biz.Business
	if err := scan(&b.ID, &b.Code, &b.Name, &b.Description, &b.OwnerID, &b.ScheduleID, &b.Groups, &b.CIID, &b.LabelValues, &b.Sort, &b.Enabled,
		&b.Version, &b.CreateTime, &b.UpdateTime, &b.CreateUser, &b.UpdateUser); err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *businessRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.Business, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+businessColumns+" FROM incident_business b WHERE b.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Business, 0, 16)
	for rows.Next() {
		b, err := scanBusiness(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// List implements biz.BusinessRepo.
func (r *businessRepo) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*biz.Business, int64, error) {
	var a sqlArgs
	where := ""
	if keyword != "" {
		p := a.add(dbx.LikeContains(keyword))
		where += " AND (b.name ILIKE " + p + " OR b.code ILIKE " + p + ")"
	}
	if enabled != nil {
		where += " AND b.enabled = " + a.add(*enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_business b WHERE b.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident businesses: %w", err)
	}
	items, err := r.query(ctx, "list incident businesses", where+" ORDER BY b.sort, b.name LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args)
	return items, total, err
}

// ListAll implements biz.BusinessRepo.
func (r *businessRepo) ListAll(ctx context.Context) ([]*biz.Business, error) {
	return r.query(ctx, "list all incident businesses", " ORDER BY b.sort, b.name LIMIT 1000", nil)
}

// Get implements biz.BusinessRepo（含已删除：历史故障仍要显示业务名）。
func (r *businessRepo) Get(ctx context.Context, id string) (*biz.Business, error) {
	b, err := scanBusiness(r.data.Pool.QueryRow(ctx, "SELECT "+businessColumns+" FROM incident_business b WHERE b.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrBusinessNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident business: %w", err)
	}
	return b, nil
}

// ListByIDs implements biz.BusinessRepo（含已删除，与 Get 同口径）。
func (r *businessRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Business, error) {
	if len(ids) == 0 {
		return []*biz.Business{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+businessColumns+" FROM incident_business b WHERE b.id = ANY($1::uuid[])", textArray(ids))
	if err != nil {
		return nil, fmt.Errorf("list incident businesses by ids: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Business, 0, len(ids))
	for rows.Next() {
		b, err := scanBusiness(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("list incident businesses by ids: scan: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// GetByCode implements biz.BusinessRepo.
func (r *businessRepo) GetByCode(ctx context.Context, code string) (*biz.Business, error) {
	b, err := scanBusiness(r.data.Pool.QueryRow(ctx, "SELECT "+businessColumns+" FROM incident_business b WHERE b.code = $1 AND b.delete_time IS NULL", code).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrBusinessNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident business by code: %w", err)
	}
	return b, nil
}

// Create implements biz.BusinessRepo.
func (r *businessRepo) Create(ctx context.Context, b *biz.Business, actor string) error {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO incident_business (id, code, name, description, owner_id, schedule_id, groups, ci_id, label_values, sort, enabled, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9::text[], $10, $11, $12, $12)`,
		b.ID, b.Code, b.Name, b.Description, nilIfEmpty(b.OwnerID), nilIfEmpty(b.ScheduleID), textArray(b.Groups), nilIfEmpty(b.CIID), textArray(b.LabelValues), b.Sort, b.Enabled, nilIfEmpty(actor)); err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrBusinessCodeExists
		}
		return fmt.Errorf("insert incident business: %w", err)
	}
	return nil
}

// Update implements biz.BusinessRepo.
func (r *businessRepo) Update(ctx context.Context, b *biz.Business, expectVersion int32, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE incident_business SET name = $3, description = $4, owner_id = $5, schedule_id = $6, groups = $7::text[], ci_id = $8, label_values = $9::text[], sort = $10, enabled = $11,
		   version = version + 1, update_time = now(), update_user = $12
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
		b.ID, expectVersion, b.Name, b.Description, nilIfEmpty(b.OwnerID), nilIfEmpty(b.ScheduleID), textArray(b.Groups), nilIfEmpty(b.CIID), textArray(b.LabelValues), b.Sort, b.Enabled, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("update incident business: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, r.data.Pool, "incident_business", b.ID, biz.ErrBusinessNotFound)
	}
	return nil
}

// Delete implements biz.BusinessRepo.
func (r *businessRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return softDeleteRow(ctx, r.data, "incident_business", id, expectVersion, actor, biz.ErrBusinessNotFound)
}

// InUse implements biz.BusinessRepo：被活跃 / 已恢复的故障引用的数量（已关闭的不算）。
func (r *businessRepo) InUse(ctx context.Context, id string) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_record WHERE business_id = $1 AND status <= 4", id).Scan(&n); err != nil {
		return 0, fmt.Errorf("count business refs: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// 预案
// ---------------------------------------------------------------------------

type playbookRepo struct {
	data *basedata.Data
}

// NewPlaybookRepo new a playbookRepo.
func NewPlaybookRepo(data *basedata.Data) *playbookRepo { return &playbookRepo{data: data} }

const playbookColumns = `p.id::text, p.name, p.description, p.doc, p.steps, p.match, COALESCE(p.agent_user_id::text, ''), p.enabled, p.version,
	p.create_time, p.update_time, COALESCE(p.create_user::text, ''), COALESCE(p.update_user::text, '')`

func scanPlaybook(scan func(dest ...any) error) (*biz.Playbook, error) {
	var pb biz.Playbook
	s := &pb.Spec
	var steps, match []byte
	if err := scan(&pb.ID, &s.Name, &s.Description, &s.Doc, &steps, &match, &s.AgentUserID, &s.Enabled, &pb.Version, &pb.CreateTime, &pb.UpdateTime, &pb.CreateUser, &pb.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(steps, &s.Steps, "playbook steps"); err != nil {
		return nil, err
	}
	if err := decodeJSON(match, &s.Match, "playbook match"); err != nil {
		return nil, err
	}
	return &pb, nil
}

func (r *playbookRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.Playbook, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+playbookColumns+" FROM incident_playbook p WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Playbook, 0, 16)
	for rows.Next() {
		pb, err := scanPlaybook(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, pb)
	}
	return out, rows.Err()
}

// List implements biz.PlaybookRepo.
func (r *playbookRepo) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*biz.Playbook, int64, error) {
	var a sqlArgs
	where := "p.delete_time IS NULL"
	if keyword != "" {
		p := a.add(dbx.LikeContains(keyword))
		where += " AND (p.name ILIKE " + p + " OR p.description ILIKE " + p + ")"
	}
	if enabled != nil {
		where += " AND p.enabled = " + a.add(*enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_playbook p WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident playbooks: %w", err)
	}
	items, err := r.query(ctx, "list incident playbooks", where+" ORDER BY p.name LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args)
	return items, total, err
}

// ListEnabled implements biz.PlaybookRepo.
func (r *playbookRepo) ListEnabled(ctx context.Context) ([]*biz.Playbook, error) {
	return r.query(ctx, "list enabled incident playbooks", "p.delete_time IS NULL AND p.enabled ORDER BY p.name LIMIT 500", nil)
}

// Get implements biz.PlaybookRepo.
func (r *playbookRepo) Get(ctx context.Context, id string) (*biz.Playbook, error) {
	pb, err := scanPlaybook(r.data.Pool.QueryRow(ctx, "SELECT "+playbookColumns+" FROM incident_playbook p WHERE p.id = $1 AND p.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrPlaybookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident playbook: %w", err)
	}
	return pb, nil
}

// ListByIDs implements biz.PlaybookRepo（含已删除）。
func (r *playbookRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Playbook, error) {
	if len(ids) == 0 {
		return []*biz.Playbook{}, nil
	}
	return r.query(ctx, "list incident playbooks by ids", "p.id = ANY($1::uuid[])", []any{textArray(ids)})
}

// Create implements biz.PlaybookRepo.
func (r *playbookRepo) Create(ctx context.Context, pb *biz.Playbook, actor string) error {
	steps, err := encodeJSON(nonNilSteps(pb.Spec.Steps), "playbook steps")
	if err != nil {
		return err
	}
	match, err := encodeJSON(pb.Spec.Match, "playbook match")
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO incident_playbook (id, name, description, doc, steps, match, agent_user_id, enabled, create_user, update_user) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
		pb.ID, pb.Spec.Name, pb.Spec.Description, pb.Spec.Doc, steps, match, nilIfEmpty(pb.Spec.AgentUserID), pb.Spec.Enabled, nilIfEmpty(actor)); err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrPlaybookNameExists
		}
		return fmt.Errorf("insert incident playbook: %w", err)
	}
	return nil
}

func nonNilSteps(steps []biz.PlaybookStep) []biz.PlaybookStep {
	if steps == nil {
		return []biz.PlaybookStep{}
	}
	return steps
}

// Update implements biz.PlaybookRepo.
func (r *playbookRepo) Update(ctx context.Context, pb *biz.Playbook, expectVersion int32, actor string) error {
	steps, err := encodeJSON(nonNilSteps(pb.Spec.Steps), "playbook steps")
	if err != nil {
		return err
	}
	match, err := encodeJSON(pb.Spec.Match, "playbook match")
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE incident_playbook SET name = $3, description = $4, doc = $5, steps = $6, match = $7, agent_user_id = $8, enabled = $9, version = version + 1, update_time = now(), update_user = $10
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
		pb.ID, expectVersion, pb.Spec.Name, pb.Spec.Description, pb.Spec.Doc, steps, match, nilIfEmpty(pb.Spec.AgentUserID), pb.Spec.Enabled, nilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrPlaybookNameExists
		}
		return fmt.Errorf("update incident playbook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, r.data.Pool, "incident_playbook", pb.ID, biz.ErrPlaybookNotFound)
	}
	return nil
}

// Delete implements biz.PlaybookRepo.
func (r *playbookRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return softDeleteRow(ctx, r.data, "incident_playbook", id, expectVersion, actor, biz.ErrPlaybookNotFound)
}

// ---------------------------------------------------------------------------
// 接入源
// ---------------------------------------------------------------------------

type sourceRepo struct {
	data *basedata.Data
}

// NewSourceRepo new a sourceRepo.
func NewSourceRepo(data *basedata.Data) *sourceRepo { return &sourceRepo{data: data} }

const sourceColumns = `s.id::text, s.name, s.description, s.token_hash, s.token_prefix, s.default_severity, COALESCE(s.business_id::text, ''), s.groups, s.allowed_ips,
	s.enabled, s.last_used_time, s.version, s.create_time, s.update_time`

func scanSource(scan func(dest ...any) error) (*biz.Source, error) {
	var s biz.Source
	if err := scan(&s.ID, &s.Name, &s.Description, &s.TokenHash, &s.TokenPrefix, &s.DefaultSeverity, &s.BusinessID, &s.Groups, &s.AllowedIPs,
		&s.Enabled, &s.LastUsedTime, &s.Version, &s.CreateTime, &s.UpdateTime); err != nil {
		return nil, err
	}
	return &s, nil
}

// List implements biz.SourceRepo.
func (r *sourceRepo) List(ctx context.Context) ([]*biz.Source, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+sourceColumns+" FROM incident_source s WHERE s.delete_time IS NULL ORDER BY s.name")
	if err != nil {
		return nil, fmt.Errorf("list incident sources: %w", err)
	}
	defer rows.Close()
	var out []*biz.Source
	for rows.Next() {
		s, err := scanSource(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan incident source: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Get implements biz.SourceRepo（含已删除：历史故障仍要显示来源名）。
func (r *sourceRepo) Get(ctx context.Context, id string) (*biz.Source, error) {
	s, err := scanSource(r.data.Pool.QueryRow(ctx, "SELECT "+sourceColumns+" FROM incident_source s WHERE s.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrSourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident source: %w", err)
	}
	return s, nil
}

// FindByHash implements biz.SourceRepo.
func (r *sourceRepo) FindByHash(ctx context.Context, hash []byte) (*biz.Source, error) {
	s, err := scanSource(r.data.Pool.QueryRow(ctx, "SELECT "+sourceColumns+" FROM incident_source s WHERE s.token_hash = $1 AND s.delete_time IS NULL", hash).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrSourceUnauthorized
	}
	if err != nil {
		return nil, fmt.Errorf("find incident source by token: %w", err)
	}
	return s, nil
}

// Create implements biz.SourceRepo.
func (r *sourceRepo) Create(ctx context.Context, s *biz.Source, actor string) error {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO incident_source (id, name, description, token_hash, token_prefix, default_severity, business_id, groups, allowed_ips, enabled, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text[], $9::text[], $10, $11, $11)`,
		s.ID, s.Name, s.Description, s.TokenHash, s.TokenPrefix, s.DefaultSeverity, nilIfEmpty(s.BusinessID), textArray(s.Groups), textArray(s.AllowedIPs), s.Enabled, nilIfEmpty(actor)); err != nil {
		if isUniqueViolation(err, "uk_incident_source_name") {
			return biz.ErrSourceNameExists
		}
		return fmt.Errorf("insert incident source: %w", err)
	}
	return nil
}

// Update implements biz.SourceRepo.
func (r *sourceRepo) Update(ctx context.Context, s *biz.Source, expectVersion int32, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE incident_source SET name = $3, description = $4, default_severity = $5, business_id = $6, groups = $7::text[], allowed_ips = $8::text[], enabled = $9,
		   version = version + 1, update_time = now(), update_user = $10
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
		s.ID, expectVersion, s.Name, s.Description, s.DefaultSeverity, nilIfEmpty(s.BusinessID), textArray(s.Groups), textArray(s.AllowedIPs), s.Enabled, nilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "uk_incident_source_name") {
			return biz.ErrSourceNameExists
		}
		return fmt.Errorf("update incident source: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, r.data.Pool, "incident_source", s.ID, biz.ErrSourceNotFound)
	}
	return nil
}

// SetToken implements biz.SourceRepo.
func (r *sourceRepo) SetToken(ctx context.Context, id string, hash []byte, prefix, actor string) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_source SET token_hash = $2, token_prefix = $3, version = version + 1, update_time = now(), update_user = $4 WHERE id = $1 AND delete_time IS NULL",
		id, hash, prefix, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("set incident source token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSourceNotFound
	}
	return nil
}

// Delete implements biz.SourceRepo（软删，令牌哈希置为随机不可匹配值以作废）。
func (r *sourceRepo) Delete(ctx context.Context, id string, actor string) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_source SET delete_time = now(), delete_user = $2, enabled = false, token_hash = gen_random_bytes(32), version = version + 1 WHERE id = $1 AND delete_time IS NULL", id, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete incident source: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSourceNotFound
	}
	return nil
}

// Touch implements biz.SourceRepo.
func (r *sourceRepo) Touch(ctx context.Context, id string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE incident_source SET last_used_time = now() WHERE id = $1 AND (last_used_time IS NULL OR last_used_time < now() - interval '1 minute')", id); err != nil {
		return fmt.Errorf("touch incident source: %w", err)
	}
	return nil
}

// CountBusinessRefs implements biz.SourceRepo.
func (r *sourceRepo) CountBusinessRefs(ctx context.Context, businessID string) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_source WHERE delete_time IS NULL AND business_id = $1", businessID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count source business refs: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// 单例设置
// ---------------------------------------------------------------------------

type settingRepo struct {
	data *basedata.Data
}

// NewSettingRepo new a settingRepo.
func NewSettingRepo(data *basedata.Data) *settingRepo { return &settingRepo{data: data} }

// Get implements biz.SettingRepo.
func (r *settingRepo) Get(ctx context.Context) (*biz.Settings, error) {
	var raw []byte
	var version int32
	err := r.data.Pool.QueryRow(ctx, "SELECT spec, version FROM incident_setting WHERE id = 1").Scan(&raw, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return &biz.Settings{Version: 0}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get incident settings: %w", err)
	}
	s := &biz.Settings{}
	if err := decodeJSON(raw, s, "incident settings"); err != nil {
		return nil, err
	}
	s.Version = version
	return s, nil
}

// Save implements biz.SettingRepo.
func (r *settingRepo) Save(ctx context.Context, s *biz.Settings, expectVersion int32, actor string) error {
	raw, err := encodeJSON(s, "incident settings")
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_setting SET spec = $1, version = version + 1, update_time = now(), update_user = $3 WHERE id = 1 AND version = $2", raw, expectVersion, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("save incident settings: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrVersionConflict
	}
	return nil
}

var _ = time.Now
