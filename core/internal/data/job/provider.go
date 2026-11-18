// Package data 作业中心域仓储实现（pgx + PostgreSQL，手写参数化 SQL）。
//
// 表的读写分工（迁移 0070 / 0075 以列级 GRANT 强制）：配置表（凭据 / 项目 / 模板 / 执行节点）由 core 读写；
// 执行记录由 core 建单，之后只改取消与结束相关的列（排队中的执行由 core 直接取消），其余状态由执行
// 引擎推进；主机快照只建不改；日志只读。执行节点的心跳列（agent_version / hostname / capacity /
// running / last_seen）归引擎，本包不写。
//
// 密文列（job_credential.secret / become_secret、job_project.secret）本包只写入、从不读出：
// 读取语句里只出现 `IS NOT NULL`。时间列一律用数据库 now()。
package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
)

// ProviderSet is 作业中心域 data providers。
var ProviderSet = wire.NewSet(
	NewCredentialRepo, NewProjectRepo, NewTemplateRepo, NewExecutionRepo, NewRunnerRepo, NewSecretSealer,
	NewScriptRepo, NewRiskRuleRepo, NewPolicyRepo, NewScheduleRepo, NewTriggerRepo, NewFireRepo, NewStatsRepo, NewTokenOpener,
	wire.Bind(new(biz.ScriptRepo), new(*scriptRepo)),
	wire.Bind(new(biz.RiskRuleRepo), new(*riskRuleRepo)),
	wire.Bind(new(biz.PolicyRepo), new(*policyRepo)),
	wire.Bind(new(biz.ScheduleRepo), new(*scheduleRepo)),
	wire.Bind(new(biz.ScheduleQueue), new(*scheduleRepo)),
	wire.Bind(new(biz.TriggerRepo), new(*triggerRepo)),
	wire.Bind(new(biz.TriggerSecrets), new(*triggerRepo)),
	wire.Bind(new(biz.TriggerNames), new(*triggerRepo)),
	wire.Bind(new(biz.FireRepo), new(*fireRepo)),
	wire.Bind(new(biz.SignalQueue), new(*fireRepo)),
	wire.Bind(new(biz.StatsReader), new(*statsRepo)),
	wire.Bind(new(biz.NotifyQueue), new(*statsRepo)),
	wire.Bind(new(biz.Retention), new(*statsRepo)),
	wire.Bind(new(biz.SecretOpener), new(*TokenOpener)),
	wire.Bind(new(biz.CredentialReader), new(*credentialRepo)),
	wire.Bind(new(biz.CredentialWriter), new(*credentialRepo)),
	wire.Bind(new(biz.ProjectReader), new(*projectRepo)),
	wire.Bind(new(biz.ProjectWriter), new(*projectRepo)),
	wire.Bind(new(biz.TemplateReader), new(*templateRepo)),
	wire.Bind(new(biz.TemplateWriter), new(*templateRepo)),
	wire.Bind(new(biz.ExecutionReader), new(*executionRepo)),
	wire.Bind(new(biz.ExecutionLogReader), new(*executionRepo)),
	wire.Bind(new(biz.ExecutionWriter), new(*executionRepo)),
	wire.Bind(new(biz.RunnerReader), new(*runnerRepo)),
	wire.Bind(new(biz.RunnerWriter), new(*runnerRepo)),
	wire.Bind(new(biz.RunnerTokens), new(*runnerRepo)),
	wire.Bind(new(biz.SecretSealer), new(*SecretSealer)),
)

// maxOptions 下拉选项列表的条数上限。
const maxOptions = 500

// inUseListed 删除被拒时列出的引用模板名个数。
const inUseListed = 5

// SecretSealer 作业凭据加密：kit/secretbox（AES-256-GCM），主密钥与通知渠道凭据相同
// （KRATOS_NOTIFY_MASTER_KEY）。只暴露 Seal——core 不解密作业凭据，解密只发生在执行引擎。
type SecretSealer struct {
	box *secretbox.Box
}

var _ biz.SecretSealer = (*SecretSealer)(nil)

// NewSecretSealer 由启动配置构造；主密钥未配置时构造成功，Seal 返回 ErrSecretUnavailable。
func NewSecretSealer(c *conf.Notify) (*SecretSealer, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretSealer{box: box}, nil
}

// Seal implements biz.SecretSealer.
func (s *SecretSealer) Seal(plaintext string) ([]byte, error) {
	out, err := s.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return out, err
}

// isUniqueViolation 报告是否唯一约束冲突（constraint 为空不区分约束）。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// sqlArgs 动态条件的实参收集器：add 追加一个实参并返回其占位符（$n）。
type sqlArgs struct {
	args []any
}

func (a *sqlArgs) add(v any) string {
	a.args = append(a.args, v)
	return fmt.Sprintf("$%d", len(a.args))
}

// lockVersion 行锁一行配置并校验乐观锁版本（table 为代码内常量，不接受外部输入）。
func lockVersion(ctx context.Context, tx pgx.Tx, table, id string, expect int32, notFound error) error {
	var version int32
	err := tx.QueryRow(ctx, "SELECT version FROM "+table+" WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound
	}
	if err != nil {
		return fmt.Errorf("lock %s: %w", table, err)
	}
	if version != expect {
		return biz.ErrVersionConflict
	}
	return nil
}

// lockForDelete 行锁待删除的行；不存在返回 notFound。
func lockForDelete(ctx context.Context, tx pgx.Tx, table, id string, notFound error) error {
	var locked string
	err := tx.QueryRow(ctx, "SELECT id::text FROM "+table+" WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound
	}
	if err != nil {
		return fmt.Errorf("lock %s: %w", table, err)
	}
	return nil
}

// templatesUsing 列出引用某个项目 / 凭据的未删除模板名（至多 inUseListed 个）与总数。
// column 为代码内常量（project_id / credential_id）。调用方已行锁被引用的行：模板保存时对它加共享锁，
// 两者互斥，这里查到「无引用」之后不会再冒出新的引用。
func templatesUsing(ctx context.Context, tx pgx.Tx, column, id string) ([]string, int, error) {
	rows, err := tx.Query(ctx,
		"SELECT name, count(*) OVER () FROM job_template WHERE "+column+" = $1 AND delete_time IS NULL ORDER BY name LIMIT $2", id, inUseListed)
	if err != nil {
		return nil, 0, fmt.Errorf("list job templates by %s: %w", column, err)
	}
	defer rows.Close()
	var names []string
	var total int
	for rows.Next() {
		var name string
		if err := rows.Scan(&name, &total); err != nil {
			return nil, 0, fmt.Errorf("scan job template by %s: %w", column, err)
		}
		names = append(names, name)
	}
	return names, total, rows.Err()
}

// collectNames 读取 (id, name) 两列的结果集。
func collectNames(rows pgx.Rows, what string) (map[string]string, error) {
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan %s: %w", what, err)
		}
		out[id] = name
	}
	return out, rows.Err()
}
