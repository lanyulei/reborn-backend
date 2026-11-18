package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxRiskRules 规则列表的条数上限。
const maxRiskRules = 500

var (
	_ biz.ScriptRepo   = (*scriptRepo)(nil)
	_ biz.RiskRuleRepo = (*riskRuleRepo)(nil)
	_ biz.PolicyRepo   = (*policyRepo)(nil)
	_ biz.SecretOpener = (*TokenOpener)(nil)
)

// TokenOpener 解密触发器调用令牌（校验流程 Webhook 的 HMAC 签名用）。与 SecretSealer 分开：
// 作业凭据（登录主机的私钥 / 密码）在 core 里没有解密入口，这个类型只被触发器用例使用。
type TokenOpener struct {
	box *secretbox.Box
}

// NewTokenOpener 由启动配置构造；主密钥未配置时 Open 返回 ErrSecretUnavailable。
func NewTokenOpener(c *conf.Notify) (*TokenOpener, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &TokenOpener{box: box}, nil
}

// Open implements biz.SecretOpener.
func (o *TokenOpener) Open(sealed []byte) (string, error) {
	out, err := o.box.Open(sealed)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", biz.ErrSecretUnavailable
	}
	return out, err
}

// softDelete 软删一行配置（table 为代码内常量）；不存在返回 notFound。
func softDelete(ctx context.Context, pool *pgxpool.Pool, table, id, actor string, notFound error) error {
	tag, err := pool.Exec(ctx, "UPDATE "+table+" SET delete_time = now(), delete_user = $2, version = version + 1 WHERE id = $1 AND delete_time IS NULL",
		id, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete %s: %w", table, err)
	}
	if tag.RowsAffected() == 0 {
		return notFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// 脚本库
// ---------------------------------------------------------------------------

type scriptRepo struct {
	data *basedata.Data
}

// NewScriptRepo new a scriptRepo.
func NewScriptRepo(data *basedata.Data) *scriptRepo { return &scriptRepo{data: data} }

// List implements biz.ScriptRepo（不读脚本内容）。
func (r *scriptRepo) List(ctx context.Context, filter biz.ScriptFilter, page, pageSize int64) ([]*biz.Script, int64, error) {
	var a sqlArgs
	where := "delete_time IS NULL"
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (name ILIKE " + k + " OR description ILIKE " + k + ")"
	}
	if filter.Interpreter != "" {
		where += " AND interpreter = " + a.add(filter.Interpreter)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_script WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job scripts: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, name, interpreter, octet_length(content), description, version, create_time, update_time, COALESCE(update_user::text, '')
		 FROM job_script WHERE `+where+" ORDER BY update_time DESC, id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job scripts: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Script, 0, pageSize)
	for rows.Next() {
		var s biz.Script
		if err := rows.Scan(&s.ID, &s.Name, &s.Interpreter, &s.Size, &s.Description, &s.Version, &s.CreateTime, &s.UpdateTime, &s.UpdateUser); err != nil {
			return nil, 0, fmt.Errorf("scan job script: %w", err)
		}
		items = append(items, &s)
	}
	return items, total, rows.Err()
}

// Get implements biz.ScriptRepo.
func (r *scriptRepo) Get(ctx context.Context, id string) (*biz.Script, error) {
	var s biz.Script
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id::text, name, interpreter, content, octet_length(content), description, version, create_time, update_time, COALESCE(update_user::text, '')
		 FROM job_script WHERE id = $1 AND delete_time IS NULL`, id).
		Scan(&s.ID, &s.Name, &s.Interpreter, &s.Content, &s.Size, &s.Description, &s.Version, &s.CreateTime, &s.UpdateTime, &s.UpdateUser)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrScriptNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job script: %w", err)
	}
	return &s, nil
}

// Write implements biz.ScriptRepo.
func (r *scriptRepo) Write(ctx context.Context, op biz.WriteOp, s *biz.Script, expectVersion int32, actor string) error {
	wrap := func(err error, what string) error {
		if isUniqueViolation(err, "uk_job_script_name") {
			return biz.ErrScriptNameExists
		}
		return fmt.Errorf("%s job script: %w", what, err)
	}
	switch op {
	case biz.OpCreate:
		if _, err := r.data.Pool.Exec(ctx,
			`INSERT INTO job_script (id, name, interpreter, content, description, create_user, update_user) VALUES ($1, $2, $3, $4, $5, $6, $6)`,
			s.ID, s.Name, s.Interpreter, s.Content, s.Description, dbx.NilIfEmpty(actor)); err != nil {
			return wrap(err, "insert")
		}
		return nil
	case biz.OpUpdate:
		return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
			if err := lockVersion(ctx, tx, "job_script", s.ID, expectVersion, biz.ErrScriptNotFound); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE job_script SET name = $2, interpreter = $3, content = $4, description = $5,
				   version = version + 1, update_time = now(), update_user = $6 WHERE id = $1`,
				s.ID, s.Name, s.Interpreter, s.Content, s.Description, dbx.NilIfEmpty(actor)); err != nil {
				return wrap(err, "update")
			}
			return nil
		})
	}
	return softDelete(ctx, r.data.Pool, "job_script", s.ID, actor, biz.ErrScriptNotFound)
}

// ---------------------------------------------------------------------------
// 高危命令规则
// ---------------------------------------------------------------------------

type riskRuleRepo struct {
	data *basedata.Data
}

// NewRiskRuleRepo new a riskRuleRepo.
func NewRiskRuleRepo(data *basedata.Data) *riskRuleRepo { return &riskRuleRepo{data: data} }

const riskRuleColumns = "id::text, name, pattern, action, enabled, description, version, update_time, COALESCE(update_user::text, '')"

func scanRiskRule(scan func(dest ...any) error) (*biz.RiskRule, error) {
	var r biz.RiskRule
	if err := scan(&r.ID, &r.Name, &r.Pattern, &r.Action, &r.Enabled, &r.Description, &r.Version, &r.UpdateTime, &r.UpdateUser); err != nil {
		return nil, err
	}
	return &r, nil
}

// List implements biz.RiskRuleRepo.
func (r *riskRuleRepo) List(ctx context.Context) ([]*biz.RiskRule, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+riskRuleColumns+" FROM job_risk_rule WHERE delete_time IS NULL ORDER BY action, name, id LIMIT $1", maxRiskRules)
	if err != nil {
		return nil, fmt.Errorf("list job risk rules: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RiskRule, 0, 16)
	for rows.Next() {
		rule, err := scanRiskRule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan job risk rule: %w", err)
		}
		items = append(items, rule)
	}
	return items, rows.Err()
}

// Get implements biz.RiskRuleRepo.
func (r *riskRuleRepo) Get(ctx context.Context, id string) (*biz.RiskRule, error) {
	rule, err := scanRiskRule(r.data.Pool.QueryRow(ctx, "SELECT "+riskRuleColumns+" FROM job_risk_rule WHERE id = $1 AND delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRiskRuleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job risk rule: %w", err)
	}
	return rule, nil
}

// Write implements biz.RiskRuleRepo.
func (r *riskRuleRepo) Write(ctx context.Context, op biz.WriteOp, rule *biz.RiskRule, expectVersion int32, actor string) error {
	wrap := func(err error, what string) error {
		if isUniqueViolation(err, "uk_job_risk_rule_name") {
			return biz.ErrRiskRuleNameExists
		}
		return fmt.Errorf("%s job risk rule: %w", what, err)
	}
	switch op {
	case biz.OpCreate:
		if _, err := r.data.Pool.Exec(ctx,
			`INSERT INTO job_risk_rule (id, name, pattern, action, enabled, description, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			rule.ID, rule.Name, rule.Pattern, rule.Action, rule.Enabled, rule.Description, dbx.NilIfEmpty(actor)); err != nil {
			return wrap(err, "insert")
		}
		return nil
	case biz.OpUpdate:
		return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
			if err := lockVersion(ctx, tx, "job_risk_rule", rule.ID, expectVersion, biz.ErrRiskRuleNotFound); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE job_risk_rule SET name = $2, pattern = $3, action = $4, enabled = $5, description = $6,
				   version = version + 1, update_time = now(), update_user = $7 WHERE id = $1`,
				rule.ID, rule.Name, rule.Pattern, rule.Action, rule.Enabled, rule.Description, dbx.NilIfEmpty(actor)); err != nil {
				return wrap(err, "update")
			}
			return nil
		})
	}
	return softDelete(ctx, r.data.Pool, "job_risk_rule", rule.ID, actor, biz.ErrRiskRuleNotFound)
}

// ---------------------------------------------------------------------------
// 全局策略
// ---------------------------------------------------------------------------

type policyRepo struct {
	data *basedata.Data
}

// NewPolicyRepo new a policyRepo.
func NewPolicyRepo(data *basedata.Data) *policyRepo { return &policyRepo{data: data} }

// Get implements biz.PolicyRepo。
func (r *policyRepo) Get(ctx context.Context) (*biz.Policy, error) {
	var p biz.Policy
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT protected_envs, approval_workflow_key, adhoc_approval, max_hosts, version, update_time, COALESCE(update_user::text, '')
		 FROM job_policy WHERE id = 1`).
		Scan(&p.ProtectedEnvs, &p.ApprovalWorkflowKey, &p.AdhocApproval, &p.MaxHosts, &p.Version, &p.UpdateTime, &p.UpdateUser)
	if err != nil {
		return nil, fmt.Errorf("get job policy: %w", err)
	}
	return &p, nil
}

// Update implements biz.PolicyRepo.
func (r *policyRepo) Update(ctx context.Context, p *biz.Policy, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_policy SET protected_envs = $1, approval_workflow_key = $2, adhoc_approval = $3, max_hosts = $4,
		   version = version + 1, update_time = now(), update_user = $5
		 WHERE id = 1 AND version = $6`,
		dbx.TextArray(p.ProtectedEnvs), p.ApprovalWorkflowKey, p.AdhocApproval, p.MaxHosts, dbx.NilIfEmpty(actor), p.Version)
	if err != nil {
		return fmt.Errorf("update job policy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrVersionConflict
	}
	return nil
}
