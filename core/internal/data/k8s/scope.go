package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var _ biz.ScopeRepo = (*scopeRepo)(nil)

type scopeRepo struct {
	data *basedata.Data
}

// NewScopeRepo new a scopeRepo.
func NewScopeRepo(data *basedata.Data) *scopeRepo { return &scopeRepo{data: data} }

// Get implements biz.ScopeRepo.
func (r *scopeRepo) Get(ctx context.Context, roleID string) ([]*biz.RoleScope, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT s.cluster_id::text, c.name, s.namespace, s.level
		 FROM k8s_role_scope s JOIN k8s_cluster c ON c.id = s.cluster_id AND c.delete_time IS NULL
		 WHERE s.role_id = $1 ORDER BY c.name, s.namespace`, roleID)
	if err != nil {
		return nil, fmt.Errorf("list k8s role scopes: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RoleScope, 0, 8)
	for rows.Next() {
		var s biz.RoleScope
		if err := rows.Scan(&s.ClusterID, &s.ClusterName, &s.Namespace, &s.Level); err != nil {
			return nil, fmt.Errorf("scan k8s role scope: %w", err)
		}
		items = append(items, &s)
	}
	return items, rows.Err()
}

// Replace implements biz.ScopeRepo。先对引用的集群加共享锁（与集群删除互斥），再整组替换。
func (r *scopeRepo) Replace(ctx context.Context, roleID string, items []*biz.RoleScope, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	clusterIDs := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, it := range items {
		if _, ok := seen[it.ClusterID]; !ok {
			seen[it.ClusterID] = struct{}{}
			clusterIDs = append(clusterIDs, it.ClusterID)
		}
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if len(clusterIDs) > 0 {
			rows, err := tx.Query(ctx,
				"SELECT id::text FROM k8s_cluster WHERE id = ANY($1::uuid[]) AND delete_time IS NULL FOR SHARE", clusterIDs)
			if err != nil {
				return fmt.Errorf("lock k8s clusters: %w", err)
			}
			found := 0
			for rows.Next() {
				found++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("lock k8s clusters: %w", err)
			}
			if found != len(clusterIDs) {
				return biz.ScopeInvalid("some clusters do not exist or have been removed")
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM k8s_role_scope WHERE role_id = $1", roleID); err != nil {
			return fmt.Errorf("delete k8s role scopes: %w", err)
		}
		for _, it := range items {
			if _, err := tx.Exec(ctx,
				"INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level, update_user) VALUES ($1, $2, $3, $4, $5)",
				roleID, it.ClusterID, it.Namespace, it.Level, dbx.NilIfEmpty(actor)); err != nil {
				return fmt.Errorf("insert k8s role scope: %w", err)
			}
		}
		if err := bumpCfg(ctx, tx, kc.CfgRoleScope); err != nil {
			return err
		}
		return insertAudit(ctx, tx, audit, "", "", roleID)
	})
}
