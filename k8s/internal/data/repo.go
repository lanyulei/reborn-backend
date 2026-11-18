package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.InstanceRepo  = (*registryRepo)(nil)
	_ biz.OwnershipRepo = (*registryRepo)(nil)
	_ biz.ClusterRepo   = (*clusterRepo)(nil)
	_ biz.RuntimeRepo   = (*runtimeRepo)(nil)
	_ biz.OutboxRepo    = (*runtimeRepo)(nil)
	_ biz.AuditRepo     = (*auditRepo)(nil)
	_ biz.ExecRepo      = (*auditRepo)(nil)
)

// registryRepo 实例登记与集群归属。时间一律取数据库时钟：各实例的本地时钟不参与租约判定。
type registryRepo struct{ data *Data }

// NewRegistryRepo new a registryRepo.
func NewRegistryRepo(data *Data) *registryRepo { return &registryRepo{data: data} }

// Heartbeat implements biz.InstanceRepo.
func (r *registryRepo) Heartbeat(ctx context.Context, id, addr string, clusters int) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_instance (id, addr, clusters) VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET addr = EXCLUDED.addr, clusters = EXCLUDED.clusters, heartbeat_time = now()`,
		id, addr, clusters)
	return err
}

// Remove implements biz.InstanceRepo.
func (r *registryRepo) Remove(ctx context.Context, id string) error {
	_, err := r.data.Pool.Exec(ctx, "DELETE FROM k8s_instance WHERE id = $1", id)
	return err
}

// Renew implements biz.OwnershipRepo。只续租直连集群：Agent 集群的归属由持有隧道的实例经
// agentRepo.Attach 维护。
func (r *registryRepo) Renew(ctx context.Context, instance string, lease time.Duration) ([]string, error) {
	// 已停用 / 删除的集群：放弃归属（它们不会再被任何实例领取）。
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_cluster_runtime r SET owner_instance = '', lease_until = NULL
		 WHERE r.owner_instance = $1
		   AND NOT EXISTS (SELECT 1 FROM k8s_cluster c WHERE c.id = r.cluster_id AND c.enabled AND c.delete_time IS NULL)`,
		instance); err != nil {
		return nil, fmt.Errorf("drop disabled clusters: %w", err)
	}
	// 让出超过公平份额的集群（每轮至多 2 个，平缓迁移）：数据库抖动后先恢复的实例会把所有集群都领走，
	// 之后没有这一步就永远不会还回去。只在确有别的存活实例比份额空闲时才让，避免让出去没人接。
	// 存活 = 心跳在一个租约之内；本实例恒算存活（心跳可能恰好落后）。
	if _, err := r.data.Pool.Exec(ctx,
		`WITH live AS (
		     SELECT id, clusters FROM k8s_instance WHERE id <> $1 AND heartbeat_time > now() - make_interval(secs => $2)
		 ), share AS (
		     SELECT ceil((SELECT count(*) FROM k8s_cluster WHERE enabled AND delete_time IS NULL AND access_mode <> 1)::numeric
		                 / ((SELECT count(*) FROM live) + 1))::int AS n
		 ), excess AS (
		     SELECT rr.cluster_id FROM k8s_cluster_runtime rr JOIN k8s_cluster cc ON cc.id = rr.cluster_id
		     WHERE rr.owner_instance = $1 AND cc.access_mode <> 1
		       AND EXISTS (SELECT 1 FROM live WHERE live.clusters < (SELECT n FROM share))
		     ORDER BY rr.cluster_id DESC OFFSET (SELECT n FROM share) LIMIT 2
		 )
		 UPDATE k8s_cluster_runtime r SET owner_instance = '', lease_until = NULL
		 FROM excess WHERE r.cluster_id = excess.cluster_id AND r.owner_instance = $1`,
		instance, lease.Seconds()); err != nil {
		return nil, fmt.Errorf("shed excess clusters: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE k8s_cluster_runtime SET lease_until = now() + make_interval(secs => $2)
		 WHERE owner_instance = $1
		   AND cluster_id IN (SELECT id FROM k8s_cluster WHERE access_mode <> 1)
		 RETURNING cluster_id::text`, instance, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("renew cluster leases: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Claim implements biz.OwnershipRepo。领取数量受「公平份额」限制：已启用集群数 ÷ 存活实例数（向上取整）
// 减去本实例已持有的数量，使集群在实例间大致均衡；实例下线后份额自动变大，由其余实例接管。
func (r *registryRepo) Claim(ctx context.Context, instance string, lease time.Duration, limit int) ([]string, error) {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_cluster_runtime (cluster_id)
		 SELECT c.id FROM k8s_cluster c WHERE c.enabled AND c.delete_time IS NULL
		 ON CONFLICT (cluster_id) DO NOTHING`); err != nil {
		return nil, fmt.Errorf("ensure runtime rows: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`WITH share AS (
		     SELECT ceil((SELECT count(*) FROM k8s_cluster WHERE enabled AND delete_time IS NULL AND access_mode <> 1)::numeric
		                 / ((SELECT count(*) FROM k8s_instance WHERE id <> $1 AND heartbeat_time > now() - make_interval(secs => $2)) + 1))::int
		            - (SELECT count(*) FROM k8s_cluster_runtime rr JOIN k8s_cluster cc ON cc.id = rr.cluster_id
		               WHERE rr.owner_instance = $1 AND rr.lease_until >= now() AND cc.access_mode <> 1)::int AS n
		 ), pick AS (
		     SELECT r.cluster_id FROM k8s_cluster_runtime r
		     JOIN k8s_cluster c ON c.id = r.cluster_id
		     WHERE c.enabled AND c.delete_time IS NULL AND c.access_mode <> 1
		       AND (r.owner_instance = '' OR r.lease_until IS NULL OR r.lease_until < now())
		     ORDER BY r.cluster_id
		     LIMIT greatest(least($3, (SELECT n FROM share)), 0)
		     FOR UPDATE OF r SKIP LOCKED
		 )
		 UPDATE k8s_cluster_runtime r SET owner_instance = $1, lease_until = now() + make_interval(secs => $2)
		 FROM pick WHERE r.cluster_id = pick.cluster_id RETURNING r.cluster_id::text`,
		instance, lease.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim clusters: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Release implements biz.OwnershipRepo.
func (r *registryRepo) Release(ctx context.Context, instance string) error {
	_, err := r.data.Pool.Exec(ctx,
		"UPDATE k8s_cluster_runtime SET owner_instance = '', lease_until = NULL WHERE owner_instance = $1", instance)
	return err
}

// clusterRepo 配置表只读访问。
type clusterRepo struct{ data *Data }

// NewClusterRepo new a clusterRepo.
func NewClusterRepo(data *Data) *clusterRepo { return &clusterRepo{data: data} }

const clusterColumns = `id::text, name, display_name, env, access_mode, api_server, proxy_url, ca_cert, credential, exec_disabled, version, update_time`

func scanCluster(row pgx.Row) (*biz.Cluster, error) {
	c := &biz.Cluster{}
	err := row.Scan(&c.ID, &c.Name, &c.DisplayName, &c.Env, &c.AccessMode, &c.APIServer, &c.ProxyURL, &c.CACert,
		&c.Credential, &c.ExecDisabled, &c.Version, &c.UpdateTime)
	return c, err
}

// ListEnabled implements biz.ClusterRepo.
func (r *clusterRepo) ListEnabled(ctx context.Context) ([]*biz.Cluster, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+clusterColumns+" FROM k8s_cluster WHERE enabled AND delete_time IS NULL")
	if err != nil {
		return nil, fmt.Errorf("list clusters: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Cluster, error) { return scanCluster(row) })
}

// Credential 读取已保存的凭据密文（测试连接时沿用）；集群不存在返回 (nil, nil)。
func (r *clusterRepo) Credential(ctx context.Context, clusterID string) ([]byte, kube.SavedEndpoint, error) {
	var (
		cred []byte
		ep   kube.SavedEndpoint
	)
	err := r.data.Pool.QueryRow(ctx, "SELECT credential, api_server, proxy_url, ca_cert FROM k8s_cluster WHERE id = $1 AND delete_time IS NULL", clusterID).
		Scan(&cred, &ep.APIServer, &ep.ProxyURL, &ep.CACert)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ep, nil
	}
	return cred, ep, err
}

// Versions implements biz.ClusterRepo.
func (r *clusterRepo) Versions(ctx context.Context) (map[string]int64, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT name, version FROM k8s_cfg_version")
	if err != nil {
		return nil, fmt.Errorf("read config versions: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, rows.Err()
}

// Scopes implements biz.ClusterRepo.
func (r *clusterRepo) Scopes(ctx context.Context, clusterID string) ([]biz.Scope, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT role_id::text, namespace, level FROM k8s_role_scope WHERE cluster_id = $1", clusterID)
	if err != nil {
		return nil, fmt.Errorf("read role scopes: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.Scope, error) {
		var s biz.Scope
		err := row.Scan(&s.RoleID, &s.Namespace, &s.Level)
		return s, err
	})
}

// runtimeRepo 集群运行状态与通知发件箱。
type runtimeRepo struct{ data *Data }

// NewRuntimeRepo new a runtimeRepo.
func NewRuntimeRepo(data *Data) *runtimeRepo { return &runtimeRepo{data: data} }

// errNotOwner 运行行不归本实例（归属已转移）：结果丢弃。
var errNotOwner = errors.New("cluster is not owned by this instance")

// SaveProbe implements biz.RuntimeRepo。以 owner_instance 围栏：归属已转移的实例写不进去。
// 失败时保留上一次成功读到的版本与节点数。
func (r *runtimeRepo) SaveProbe(ctx context.Context, instance string, rec biz.ProbeRecord) (int32, time.Time, error) {
	var prev int32
	var since time.Time
	ok := rec.Error == ""
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE k8s_cluster_runtime r SET
		     status = $3,
		     status_since = CASE WHEN r.status <> $3 THEN now() ELSE r.status_since END,
		     k8s_version = CASE WHEN $4 THEN $5 ELSE r.k8s_version END,
		     node_total = CASE WHEN $4 THEN $6 ELSE r.node_total END,
		     node_ready = CASE WHEN $4 THEN $7 ELSE r.node_ready END,
		     last_error = $8,
		     probe_time = now()
		 FROM (SELECT cluster_id, status AS prev FROM k8s_cluster_runtime
		       WHERE cluster_id = $1 AND owner_instance = $2 FOR UPDATE) o
		 WHERE r.cluster_id = o.cluster_id
		 RETURNING o.prev, r.status_since`,
		rec.ClusterID, instance, rec.Status, ok, rec.Info.Version, rec.Info.NodeTotal, rec.Info.NodeReady, rec.Error).
		Scan(&prev, &since)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, errNotOwner
	}
	return prev, since, err
}

// SaveScope implements biz.RuntimeRepo。version = 0 表示本次未成功：只更新错误，不前移已同步版本。
func (r *runtimeRepo) SaveScope(ctx context.Context, instance, clusterID string, version int64, errMsg string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_cluster_runtime SET scope_version = CASE WHEN $3 > 0 THEN $3 ELSE scope_version END, scope_error = $4
		 WHERE cluster_id = $1 AND owner_instance = $2`, clusterID, instance, version, errMsg)
	return err
}

// Enqueue implements biz.OutboxRepo.
func (r *runtimeRepo) Enqueue(ctx context.Context, ev biz.OutboxEvent) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return err
	}
	_, err = r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_notify_outbox (id, cluster_id, event_type, severity, dedup_key, group_key, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (dedup_key) DO NOTHING`,
		id, ev.ClusterID, ev.EventType, ev.Severity, ev.DedupKey, ev.GroupKey, payload)
	return err
}

// auditRepo 操作审计与终端会话记录。
type auditRepo struct{ data *Data }

// NewAuditRepo new an auditRepo.
func NewAuditRepo(data *Data) *auditRepo { return &auditRepo{data: data} }

// auditWriteTimeout 单次审计写入的上限：审计不应无限期拖住请求的收尾。
const auditWriteTimeout = 5 * time.Second

// Insert implements biz.AuditRepo.
func (r *auditRepo) Insert(ctx context.Context, a *biz.AuditRecord) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	// 调用方（biz.Auditor）给整批事件设了总时限；这里再给单条设上限，取两者中较早的。
	ctx, cancel := context.WithTimeout(ctx, auditWriteTimeout)
	defer cancel()
	_, err = r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_audit (id, user_id, username, client_ip, request_id, cluster_id, cluster_name, namespace,
		     api_group, api_version, resource, kind, name, action, result, error, before_obj, after_obj, patch,
		     truncated, emergency, change_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
		id, dbx.NilIfEmpty(a.Identity.UserID), clip(a.Identity.Username, 64), clip(a.Identity.ClientIP, 64),
		clip(a.Identity.RequestID, 64), a.ClusterID, a.ClusterName, clip(a.Ref.Namespace, 63),
		clip(a.Ref.Group, 128), clip(a.Ref.Version, 32), clip(a.Ref.Resource, 128), clip(a.Kind, 128), clip(a.Ref.Name, 253),
		a.Action, a.Result, clip(a.Error, 512), jsonOrNil(a.Before), jsonOrNil(a.After), jsonOrNil(a.Patch),
		a.Truncated, a.Identity.Emergency, dbx.NilIfEmpty(a.Identity.ChangeID))
	return err
}

func jsonOrNil(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

// clip 截到至多 n 字节（不切断多字节字符、去掉 PostgreSQL 拒收的字节）。
func clip(s string, n int) string { return biz.Truncate(s, n) }

// Start implements biz.ExecRepo.
func (r *auditRepo) Start(ctx context.Context, s *biz.ExecSession) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_exec_session (id, user_id, username, client_ip, cluster_id, cluster_name, namespace, pod, container, kind)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		s.ID, s.Identity.UserID, clip(s.Identity.Username, 64), clip(s.Identity.ClientIP, 64), s.ClusterID, s.ClusterName,
		s.Params.Namespace, s.Params.Pod, s.Params.Container, s.Params.Kind)
	return err
}

// Finish implements biz.ExecRepo.
func (r *auditRepo) Finish(ctx context.Context, id, reason string, bytesIn, bytesOut int64) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_exec_session SET end_time = now(), end_reason = $2, bytes_in = $3, bytes_out = $4
		 WHERE id = $1 AND end_time IS NULL`, id, reason, bytesIn, bytesOut)
	return err
}
