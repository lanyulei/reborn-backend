// Package data 容器平台域仓储实现（pgx + PostgreSQL，手写参数化 SQL）。
//
// 表的读写分工（迁移 0072 以 GRANT 强制）：配置表（集群 / Agent 令牌 / 角色数据范围 / 配置版本）与
// 终端票据由 core 读写；运行表（实例、集群运行状态、终端会话）只读；审计只追加（集群与授权的配置
// 变更）；通知发件箱只改派发状态相关的列。
//
// 凭据密文列（k8s_cluster.credential）本包只写入、从不读出：读取语句里只出现 `IS NOT NULL`。
// 配置表的每次写入都在同一事务内 bump k8s_cfg_version（数据面据此刷新）并追加审计。时间列一律用数据库 now()。
package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/k8splane"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ProviderSet is 容器平台域 data providers。
var ProviderSet = wire.NewSet(
	NewClusterRepo, NewScopeRepo, NewAuditRepo, NewTicketRepo, NewOutboxRepo, NewSecretSealer, NewInstanceResolver, k8splane.New,
	NewHelmRepo, wire.Bind(new(biz.HelmRepoStore), new(*helmRepo)), wire.Bind(new(biz.HelmRepoDeleter), new(*helmRepo)),
	wire.Bind(new(biz.ClusterReader), new(*clusterRepo)),
	wire.Bind(new(biz.ClusterWriter), new(*clusterRepo)),
	wire.Bind(new(biz.AgentTokenRepo), new(*clusterRepo)),
	wire.Bind(new(biz.ScopeRepo), new(*scopeRepo)),
	wire.Bind(new(biz.AuditReader), new(*auditRepo)),
	wire.Bind(new(biz.TicketRepo), new(*ticketRepo)),
	wire.Bind(new(biz.OutboxRepo), new(*outboxRepo)),
	wire.Bind(new(biz.SecretSealer), new(*SecretSealer)),
	wire.Bind(new(k8splane.Resolver), new(*instanceResolver)),
	wire.Bind(new(biz.Plane), new(*k8splane.Client)),
	NewChangeRepo, NewSettingRepo, wire.Bind(new(biz.ChangeStore), new(*changeRepo)), wire.Bind(new(biz.ChangeReader), new(*changeRepo)),
	wire.Bind(new(biz.ChangeFlow), new(*changeRepo)), wire.Bind(new(biz.ChangeQueue), new(*changeRepo)), wire.Bind(new(biz.ChangeRefresher), new(*changeRepo)),
	wire.Bind(new(biz.SettingRepo), new(*settingRepo)),
	NewInsightRepo, wire.Bind(new(biz.InsightRepo), new(*insightRepo)), wire.Bind(new(biz.ClusterBriefs), new(*insightRepo)),
	wire.Bind(new(biz.GrantSource), new(*insightRepo)),
	NewGovOptions, NewGovRepo, wire.Bind(new(biz.KubeTokenRepo), new(*govRepo)), wire.Bind(new(biz.KubeTokenAuthRepo), new(*govRepo)),
	NewInspectionRepo, wire.Bind(new(biz.InspectionRepo), new(*inspectionRepo)),
	NewExecRecordRepo, wire.Bind(new(biz.ExecRecordRepo), new(*execRecordRepo)),
	wire.Bind(new(biz.PlaneRouter), new(*k8splane.Client)),
)

// SecretSealer 集群凭据加密：kit/secretbox（AES-256-GCM），主密钥与通知渠道凭据相同
// （KRATOS_NOTIFY_MASTER_KEY）。只暴露 Seal——core 不解密集群凭据，解密只发生在数据面。
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

// isUniqueViolation 报告是否为指定唯一约束的冲突。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == constraint
}

// sqlArgs 动态条件的实参收集器：add 追加一个实参并返回其占位符（$n）。
type sqlArgs struct {
	args []any
}

func (a *sqlArgs) add(v any) string {
	a.args = append(a.args, v)
	return fmt.Sprintf("$%d", len(a.args))
}

// bumpCfg 同事务把配置版本 +1（name 为 kit/k8scontract 的 Cfg* 常量）。
func bumpCfg(ctx context.Context, tx pgx.Tx, name string) error {
	if _, err := tx.Exec(ctx, "UPDATE k8s_cfg_version SET version = version + 1, update_time = now() WHERE name = $1", name); err != nil {
		return fmt.Errorf("bump k8s cfg version %s: %w", name, err)
	}
	return nil
}

// insertAudit 同事务追加一条 core 侧的审计（集群与授权的配置变更）。a 为 nil 不写。
// clusterID / clusterName / name 可空；detail 落 patch 列，调用方保证不含凭据与令牌。
func insertAudit(ctx context.Context, tx pgx.Tx, a *biz.AuditEntry, clusterID, clusterName, name string) error {
	if a == nil {
		return nil
	}
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	var patch any
	if len(a.Detail) > 0 {
		patch = []byte(a.Detail)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO k8s_audit (id, user_id, username, client_ip, request_id, cluster_id, cluster_name, resource, name, action, result, patch)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		id, dbx.NilIfEmpty(a.UserID), a.Username, a.ClientIP, a.RequestID, dbx.NilIfEmpty(clusterID), clusterName,
		a.Resource, name, a.Action, kc.AuditOK, patch); err != nil {
		return fmt.Errorf("insert k8s audit: %w", err)
	}
	return nil
}
