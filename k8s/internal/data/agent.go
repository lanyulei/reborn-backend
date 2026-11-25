package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.AgentTokenRepo   = (*agentRepo)(nil)
	_ biz.AgentRuntimeRepo = (*agentRepo)(nil)
)

// agentRepo Agent 令牌与 Agent 集群的归属。
type agentRepo struct{ data *Data }

// NewAgentRepo new an agentRepo.
func NewAgentRepo(data *Data) *agentRepo { return &agentRepo{data: data} }

// dummyHash 令牌不存在时仍做一次等长比较，让「不存在」与「密钥不对」耗时一致。
var dummyHash = k8scontract.HashSecret("reborn-agent-dummy-secret")

// tokenQuery 取令牌及其集群；只认已启用、未删除、Agent 接入的集群上未过期的令牌。
const tokenQuery = `SELECT t.cluster_id::text, c.name, t.token_hash
	FROM k8s_agent_token t JOIN k8s_cluster c ON c.id = t.cluster_id
	WHERE t.id = $1 AND t.kind = $2 AND (t.expire_time IS NULL OR t.expire_time > now())
	  AND c.enabled AND c.delete_time IS NULL AND c.access_mode = 1`

// registerLock 串行化同一个注册令牌的换发（事务级咨询锁，键里带上令牌 ID）。
const registerLock = "SELECT pg_advisory_xact_lock(hashtextextended('k8s:agent-register:' || $1, 0))"

// Exchange implements biz.AgentTokenRepo。同一个注册令牌的换发被串行化：并发提交同一个令牌时，
// 后到者等前者提交之后再读，读不到该行。
//
// 串行化用咨询锁而不是对令牌行 FOR UPDATE：行锁（FOR UPDATE / FOR SHARE 一类）需要表的 UPDATE 权限，
// 而数据面角色对 k8s_agent_token 只有 SELECT / INSERT / DELETE（迁移 0072）——以受限账号部署时整条查询
// 被数据库拒绝（42501），Agent 永远注册不上；本地开发沿用 postgres 账号，看不出这个问题。咨询锁不需要任何表权限。
// 锁顺序恒为「令牌 → 集群」（集群锁见下），并发换发之间不会互等。
func (r *agentRepo) Exchange(ctx context.Context, tokenID, secret string) (biz.AgentGrant, error) {
	grant := biz.AgentGrant{}
	newID, err := idgen.NewID()
	if err != nil {
		return grant, err
	}
	token, hash, err := k8scontract.NewToken(newID)
	if err != nil {
		return grant, err
	}
	err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, lerr := tx.Exec(ctx, registerLock, tokenID); lerr != nil {
			return fmt.Errorf("lock register token: %w", lerr)
		}
		var stored []byte
		qerr := tx.QueryRow(ctx, tokenQuery, tokenID, k8scontract.TokenRegister).
			Scan(&grant.ClusterID, &grant.ClusterName, &stored)
		if errors.Is(qerr, pgx.ErrNoRows) {
			k8scontract.SecretMatches(secret, dummyHash)
			return biz.ErrAgentToken
		}
		if qerr != nil {
			return fmt.Errorf("read register token: %w", qerr)
		}
		if !k8scontract.SecretMatches(secret, stored) {
			return biz.ErrAgentToken
		}
		// 串行化同一集群的令牌换发（事务级咨询锁）：否则两个并发的换发各自裁剪时看不到对方刚插入的行，
		// 长期令牌会超过上限。数据面对集群行没有读以外的一切权限，所以不用 FOR UPDATE 锁集群行。
		if _, lerr := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('k8s:agent-token:' || $1, 0))", grant.ClusterID); lerr != nil {
			return fmt.Errorf("lock cluster tokens: %w", lerr)
		}
		// 消费必须真的删到那一行。咨询锁只在换发之间互斥，控制面的吊销 / 重置注册令牌、删除集群与过期的
		// 清理都不走这把锁：它们在「读出令牌」之后删掉这一行时，这里的 DELETE 会先等对方提交、再删到 0 行。
		// 不拦的话就等于拿一枚已经不存在的注册令牌换发出了长期令牌（例如给一个正在被删除的集群留下一枚
		// 永不过期的孤儿令牌）。
		tag, derr := tx.Exec(ctx, "DELETE FROM k8s_agent_token WHERE id = $1", tokenID)
		if derr != nil {
			return fmt.Errorf("consume register token: %w", derr)
		}
		if tag.RowsAffected() != 1 {
			return biz.ErrAgentToken
		}
		if _, ierr := tx.Exec(ctx,
			"INSERT INTO k8s_agent_token (id, cluster_id, kind, token_hash) VALUES ($1, $2, $3, $4)",
			newID, grant.ClusterID, k8scontract.TokenLongTerm, hash); ierr != nil {
			return fmt.Errorf("issue long-term token: %w", ierr)
		}
		// 超过上限删最旧的（重新安装的 Agent 顶替旧的；被删令牌对应的隧道由吊销检查断开）。
		if _, terr := tx.Exec(ctx,
			`DELETE FROM k8s_agent_token WHERE id IN (
			     SELECT id FROM k8s_agent_token WHERE cluster_id = $1 AND kind = $2
			     ORDER BY create_time DESC, id DESC OFFSET $3)`,
			grant.ClusterID, k8scontract.TokenLongTerm, k8scontract.MaxLongTermTokens); terr != nil {
			return fmt.Errorf("trim long-term tokens: %w", terr)
		}
		return nil
	})
	if err != nil {
		return biz.AgentGrant{}, err
	}
	grant.TokenID, grant.Token = newID, token
	return grant, nil
}

// Verify implements biz.AgentTokenRepo.
func (r *agentRepo) Verify(ctx context.Context, tokenID, secret string) (biz.AgentGrant, error) {
	grant := biz.AgentGrant{TokenID: tokenID}
	var (
		stored  []byte
		enabled bool
	)
	// 不在查询里过滤 enabled：停用的集群要与「令牌无效」区分开（密钥比对通过之后才区分）。
	err := r.data.Pool.QueryRow(ctx,
		`SELECT t.cluster_id::text, c.name, t.token_hash, c.enabled
		 FROM k8s_agent_token t JOIN k8s_cluster c ON c.id = t.cluster_id
		 WHERE t.id = $1 AND t.kind = $2 AND (t.expire_time IS NULL OR t.expire_time > now())
		   AND c.delete_time IS NULL AND c.access_mode = 1`, tokenID, k8scontract.TokenLongTerm).
		Scan(&grant.ClusterID, &grant.ClusterName, &stored, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		k8scontract.SecretMatches(secret, dummyHash)
		return biz.AgentGrant{}, biz.ErrAgentToken
	}
	if err != nil {
		return biz.AgentGrant{}, fmt.Errorf("read agent token: %w", err)
	}
	if !k8scontract.SecretMatches(secret, stored) {
		return biz.AgentGrant{}, biz.ErrAgentToken
	}
	if !enabled {
		return biz.AgentGrant{}, biz.ErrAgentClusterDisabled
	}
	return grant, nil
}

// Alive implements biz.AgentTokenRepo.
func (r *agentRepo) Alive(ctx context.Context, tokenIDs []string) (map[string]struct{}, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT t.id::text FROM k8s_agent_token t JOIN k8s_cluster c ON c.id = t.cluster_id
		 WHERE t.id = ANY($1::uuid[]) AND t.kind = $2 AND c.enabled AND c.delete_time IS NULL AND c.access_mode = 1`,
		tokenIDs, k8scontract.TokenLongTerm)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out, nil
}

// Attach implements biz.AgentRuntimeRepo。可领取的条件：无主、已是本实例、租约过期，或原主实例已无心跳
// （数据面实例异常退出后，重连到其他实例的 Agent 不必等租约到期）。
func (r *agentRepo) Attach(ctx context.Context, instance, clusterID, agentVersion string, lease time.Duration) (bool, error) {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_cluster_runtime (cluster_id)
		 SELECT c.id FROM k8s_cluster c WHERE c.id = $1 AND c.enabled AND c.delete_time IS NULL
		 ON CONFLICT (cluster_id) DO NOTHING`, clusterID); err != nil {
		return false, fmt.Errorf("ensure runtime row: %w", err)
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_cluster_runtime r SET
		     agent_connect_time = CASE WHEN r.owner_instance = $2 AND r.agent_connect_time IS NOT NULL
		                               THEN r.agent_connect_time ELSE now() END,
		     owner_instance = $2,
		     lease_until = now() + make_interval(secs => $4),
		     agent_version = $3,
		     agent_last_seen = now()
		 FROM k8s_cluster c
		 WHERE c.id = r.cluster_id AND r.cluster_id = $1
		   AND c.enabled AND c.delete_time IS NULL AND c.access_mode = 1
		   AND (r.owner_instance = '' OR r.owner_instance = $2 OR r.lease_until IS NULL OR r.lease_until < now()
		        OR NOT EXISTS (SELECT 1 FROM k8s_instance i
		                       WHERE i.id = r.owner_instance AND i.heartbeat_time > now() - make_interval(secs => $4)))`,
		clusterID, instance, clip(agentVersion, 32), lease.Seconds())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Detach implements biz.AgentRuntimeRepo.
func (r *agentRepo) Detach(ctx context.Context, instance, clusterID string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_cluster_runtime SET owner_instance = '', lease_until = NULL, agent_connect_time = NULL, agent_last_seen = now()
		 WHERE cluster_id = $1 AND owner_instance = $2`, clusterID, instance)
	return err
}

// SweepOffline implements biz.AgentRuntimeRepo。条件更新自身原子：多实例并发时每个集群只被一个实例翻转。
// 从未连接过的集群也标为不可达（原因可见），但不算「由在线翻转」。
func (r *agentRepo) SweepOffline(ctx context.Context, grace time.Duration) ([]biz.OfflineAgent, error) {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_cluster_runtime (cluster_id)
		 SELECT c.id FROM k8s_cluster c WHERE c.enabled AND c.delete_time IS NULL AND c.access_mode = 1
		 ON CONFLICT (cluster_id) DO NOTHING`); err != nil {
		return nil, fmt.Errorf("ensure runtime rows: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE k8s_cluster_runtime r SET status = $2, status_since = now(), last_error = $3, probe_time = now(),
		     owner_instance = '', lease_until = NULL
		 FROM k8s_cluster c, (SELECT rt.cluster_id, rt.status AS prev FROM k8s_cluster_runtime rt
		                      JOIN k8s_cluster ac ON ac.id = rt.cluster_id AND ac.access_mode = 1
		                      FOR UPDATE OF rt SKIP LOCKED) o
		 WHERE c.id = r.cluster_id AND o.cluster_id = r.cluster_id
		   AND c.enabled AND c.delete_time IS NULL AND c.access_mode = 1
		   AND r.status <> $2
		   AND (r.owner_instance = '' OR r.lease_until IS NULL OR r.lease_until < now())
		   AND coalesce(r.agent_last_seen, c.create_time) < now() - make_interval(secs => $1)
		 RETURNING c.id::text, c.name, c.display_name, c.env, o.prev, r.status_since`,
		grace.Seconds(), k8scontract.ClusterUnreachable, biz.AgentOfflineMessage)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.OfflineAgent, error) {
		var o biz.OfflineAgent
		var prev int32
		err := row.Scan(&o.Cluster.ID, &o.Cluster.Name, &o.Cluster.DisplayName, &o.Cluster.Env, &prev, &o.Since)
		o.WasOnline = prev == k8scontract.ClusterOnline
		return o, err
	})
}
