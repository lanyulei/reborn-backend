package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ClusterReader  = (*clusterRepo)(nil)
	_ biz.ClusterWriter  = (*clusterRepo)(nil)
	_ biz.AgentTokenRepo = (*clusterRepo)(nil)
)

// clusterSelect 集群查询：左连运行状态；凭据只读出「是否已保存」。$1 恒为数据范围的配置版本名，
// $2 恒为长期令牌类型。
const clusterSelect = `SELECT c.id::text, c.name, c.display_name, c.env, c.access_mode, c.agent_location, c.api_server,
	c.proxy_url, c.ca_cert, c.exec_disabled, c.description, c.enabled, c.credential IS NOT NULL,
	c.version, c.create_time, c.update_time, COALESCE(c.datasource_id::text, ''), c.event_reasons,
	COALESCE(c.approval_template_id::text, ''),
	COALESCE(r.status, 0), r.status_since, COALESCE(r.k8s_version, ''), COALESCE(r.node_total, 0), COALESCE(r.node_ready, 0),
	COALESCE(r.agent_version, ''), r.agent_last_seen, COALESCE(r.last_error, ''), r.probe_time, COALESCE(r.scope_error, ''),
	COALESCE(r.scope_version, 0) >= COALESCE((SELECT version FROM k8s_cfg_version WHERE name = $1), 0),
	(SELECT count(*) FROM k8s_agent_token t WHERE t.cluster_id = c.id AND t.kind = $2)
	FROM k8s_cluster c LEFT JOIN k8s_cluster_runtime r ON r.cluster_id = c.id`

func scanCluster(scan func(dest ...any) error) (*biz.Cluster, error) {
	var c biz.Cluster
	if err := scan(&c.ID, &c.Name, &c.DisplayName, &c.Env, &c.AccessMode, &c.AgentLocation, &c.APIServer,
		&c.ProxyURL, &c.CACert, &c.ExecDisabled, &c.Description, &c.Enabled, &c.HasCredential,
		&c.Version, &c.CreateTime, &c.UpdateTime, &c.DatasourceID, &c.EventReasons, &c.ApprovalTemplateID,
		&c.Runtime.Status, &c.Runtime.StatusSince, &c.Runtime.K8sVersion, &c.Runtime.NodeTotal, &c.Runtime.NodeReady,
		&c.Runtime.AgentVersion, &c.Runtime.AgentLastSeen, &c.Runtime.LastError, &c.Runtime.ProbeTime, &c.Runtime.ScopeError,
		&c.Runtime.ScopeSynced, &c.AgentTokens); err != nil {
		return nil, err
	}
	return &c, nil
}

type clusterRepo struct {
	data *basedata.Data
}

// NewClusterRepo new a clusterRepo.
func NewClusterRepo(data *basedata.Data) *clusterRepo { return &clusterRepo{data: data} }

// List implements biz.ClusterReader.
func (r *clusterRepo) List(ctx context.Context, filter biz.ClusterFilter, page, pageSize int64) ([]*biz.Cluster, int64, error) {
	a := sqlArgs{args: []any{kc.CfgRoleScope, kc.TokenLongTerm}}
	where := " WHERE c.delete_time IS NULL"
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (c.name ILIKE " + k + " OR c.display_name ILIKE " + k + ")"
	}
	if filter.Env != nil {
		where += " AND c.env = " + a.add(*filter.Env)
	}
	if filter.Status != nil {
		where += " AND COALESCE(r.status, 0) = " + a.add(*filter.Status)
	}
	var total int64
	// count 复用同一组实参（$1 / $2 以恒真条件占位，保持占位符编号一致）。
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM k8s_cluster c LEFT JOIN k8s_cluster_runtime r ON r.cluster_id = c.id"+where+
			" AND $1::text IS NOT NULL AND $2::int IS NOT NULL", a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s clusters: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, clusterSelect+where+" ORDER BY c.env, c.name LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s clusters: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Cluster, 0, pageSize)
	for rows.Next() {
		item, err := scanCluster(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan k8s cluster: %w", err)
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// Get implements biz.ClusterReader.
func (r *clusterRepo) Get(ctx context.Context, id string) (*biz.Cluster, error) {
	item, err := scanCluster(r.data.Pool.QueryRow(ctx,
		clusterSelect+" WHERE c.id = $3 AND c.delete_time IS NULL", kc.CfgRoleScope, kc.TokenLongTerm, id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrClusterNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s cluster: %w", err)
	}
	return item, nil
}

// Options implements biz.ClusterReader.
func (r *clusterRepo) Options(ctx context.Context, all bool, roleIDs []string) ([]*biz.ClusterOption, error) {
	const cols = `c.id::text, c.name, c.display_name, c.env, COALESCE(r.status, 0), COALESCE(r.last_error, ''),
		c.exec_disabled, COALESCE(r.k8s_version, ''), c.approval_template_id IS NOT NULL`
	var rows pgx.Rows
	var err error
	if all {
		rows, err = r.data.Pool.Query(ctx, "SELECT "+cols+`, $1::int
			FROM k8s_cluster c LEFT JOIN k8s_cluster_runtime r ON r.cluster_id = c.id
			WHERE c.delete_time IS NULL AND c.enabled ORDER BY c.env, c.name`, kc.LevelClusterAdmin)
	} else {
		rows, err = r.data.Pool.Query(ctx, "SELECT "+cols+`, s.level
			FROM k8s_cluster c
			JOIN (SELECT cluster_id, max(level)::int AS level FROM k8s_role_scope WHERE role_id = ANY($1::uuid[]) GROUP BY cluster_id) s
			  ON s.cluster_id = c.id
			LEFT JOIN k8s_cluster_runtime r ON r.cluster_id = c.id
			WHERE c.delete_time IS NULL AND c.enabled ORDER BY c.env, c.name`, dbx.TextArray(roleIDs))
	}
	if err != nil {
		return nil, fmt.Errorf("list k8s cluster options: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ClusterOption, 0, 16)
	for rows.Next() {
		var o biz.ClusterOption
		if err := rows.Scan(&o.ID, &o.Name, &o.DisplayName, &o.Env, &o.Status, &o.LastError, &o.ExecDisabled, &o.K8sVersion, &o.ApprovalRequired, &o.Level); err != nil {
			return nil, fmt.Errorf("scan k8s cluster option: %w", err)
		}
		items = append(items, &o)
	}
	return items, rows.Err()
}

// Create implements biz.ClusterWriter.
func (r *clusterRepo) Create(ctx context.Context, c *biz.Cluster, credential []byte, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_cluster (id, name, display_name, env, access_mode, agent_location, api_server, proxy_url, ca_cert,
			   credential, exec_disabled, description, enabled, create_user, update_user, datasource_id, event_reasons,
			   approval_template_id)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $15, $16, $17)`,
			c.ID, c.Name, c.DisplayName, c.Env, c.AccessMode, c.AgentLocation, c.APIServer, c.ProxyURL, c.CACert,
			dbx.NilIfEmptyBytes(credential), c.ExecDisabled, c.Description, c.Enabled, dbx.NilIfEmpty(actor),
			dbx.NilIfEmpty(c.DatasourceID), dbx.TextArray(c.EventReasons), dbx.NilIfEmpty(c.ApprovalTemplateID)); err != nil {
			if isUniqueViolation(err, "uk_k8s_cluster_name") {
				return biz.ErrClusterNameExists
			}
			return fmt.Errorf("insert k8s cluster: %w", err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO k8s_cluster_runtime (cluster_id) VALUES ($1) ON CONFLICT DO NOTHING", c.ID); err != nil {
			return fmt.Errorf("insert k8s cluster runtime: %w", err)
		}
		if err := bumpCfg(ctx, tx, kc.CfgCluster); err != nil {
			return err
		}
		return insertAudit(ctx, tx, audit, c.ID, c.Name, c.Name)
	})
}

// lockCluster 行锁未删除的集群并返回其名称与版本；不存在返回 ErrClusterNotFound。
func lockCluster(ctx context.Context, tx pgx.Tx, id, mode string) (name string, version int32, err error) {
	err = tx.QueryRow(ctx, "SELECT name, version FROM k8s_cluster WHERE id = $1 AND delete_time IS NULL FOR "+mode, id).Scan(&name, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, biz.ErrClusterNotFound
	}
	if err != nil {
		return "", 0, fmt.Errorf("lock k8s cluster: %w", err)
	}
	return name, version, nil
}

// Update implements biz.ClusterWriter。名称与接入方式不在更新列内；Agent 方式恒清空凭据。
func (r *clusterRepo) Update(ctx context.Context, c *biz.Cluster, credential []byte, expectVersion int32, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		name, version, err := lockCluster(ctx, tx, c.ID, "UPDATE")
		if err != nil {
			return err
		}
		if version != expectVersion {
			return biz.ErrVersionConflict
		}
		if _, err := tx.Exec(ctx,
			`UPDATE k8s_cluster SET display_name = $2, env = $3, agent_location = $4, api_server = $5, proxy_url = $6, ca_cert = $7,
			   credential = CASE WHEN access_mode = $8 THEN NULL ELSE COALESCE($9, credential) END,
			   exec_disabled = $10, description = $11, enabled = $12, datasource_id = $14, event_reasons = $15,
			   approval_template_id = $16, version = version + 1, update_time = now(), update_user = $13
			 WHERE id = $1`,
			c.ID, c.DisplayName, c.Env, c.AgentLocation, c.APIServer, c.ProxyURL, c.CACert, kc.AccessAgent,
			dbx.NilIfEmptyBytes(credential), c.ExecDisabled, c.Description, c.Enabled, dbx.NilIfEmpty(actor),
			dbx.NilIfEmpty(c.DatasourceID), dbx.TextArray(c.EventReasons), dbx.NilIfEmpty(c.ApprovalTemplateID)); err != nil {
			return fmt.Errorf("update k8s cluster: %w", err)
		}
		if err := bumpCfg(ctx, tx, kc.CfgCluster); err != nil {
			return err
		}
		return insertAudit(ctx, tx, audit, c.ID, name, name)
	})
}

// Delete implements biz.ClusterWriter。软删并清除凭据；数据范围与 Agent 令牌物理删除。
func (r *clusterRepo) Delete(ctx context.Context, id string, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		name, _, err := lockCluster(ctx, tx, id, "UPDATE")
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE k8s_cluster SET delete_time = now(), delete_user = $2, credential = NULL, version = version + 1 WHERE id = $1`,
			id, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("delete k8s cluster: %w", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM k8s_role_scope WHERE cluster_id = $1", id); err != nil {
			return fmt.Errorf("delete k8s role scopes of cluster: %w", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM k8s_agent_token WHERE cluster_id = $1", id); err != nil {
			return fmt.Errorf("delete k8s agent tokens of cluster: %w", err)
		}
		for _, cfg := range []string{kc.CfgCluster, kc.CfgRoleScope, kc.CfgAgentToken} {
			if err := bumpCfg(ctx, tx, cfg); err != nil {
				return err
			}
		}
		return insertAudit(ctx, tx, audit, id, name, name)
	})
}

// ReplaceRegister implements biz.AgentTokenRepo。
func (r *clusterRepo) ReplaceRegister(ctx context.Context, clusterID, tokenID string, hash []byte, expire time.Time, audit *biz.AuditEntry) error {
	actor := ""
	if audit != nil {
		actor = audit.UserID
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		name, _, err := lockCluster(ctx, tx, clusterID, "UPDATE")
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM k8s_agent_token WHERE cluster_id = $1 AND kind = $2", clusterID, kc.TokenRegister); err != nil {
			return fmt.Errorf("delete k8s register tokens: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_agent_token (id, cluster_id, kind, token_hash, expire_time, create_user) VALUES ($1, $2, $3, $4, $5, $6)`,
			tokenID, clusterID, kc.TokenRegister, hash, expire, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("insert k8s register token: %w", err)
		}
		if err := bumpCfg(ctx, tx, kc.CfgAgentToken); err != nil {
			return err
		}
		return insertAudit(ctx, tx, audit, clusterID, name, name)
	})
}

// RevokeAll implements biz.AgentTokenRepo。
func (r *clusterRepo) RevokeAll(ctx context.Context, clusterID string, audit *biz.AuditEntry) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		name, _, err := lockCluster(ctx, tx, clusterID, "UPDATE")
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM k8s_agent_token WHERE cluster_id = $1", clusterID); err != nil {
			return fmt.Errorf("delete k8s agent tokens: %w", err)
		}
		if err := bumpCfg(ctx, tx, kc.CfgAgentToken); err != nil {
			return err
		}
		return insertAudit(ctx, tx, audit, clusterID, name, name)
	})
}
