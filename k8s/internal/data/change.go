package data

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var _ biz.ChangeRepo = (*changeRepo)(nil)

// changeRepo 变更单的领取与回写（k8s_change；数据面只有执行状态相关列的 UPDATE 权限）。
type changeRepo struct {
	data *Data
}

// NewChangeRepo new a changeRepo.
func NewChangeRepo(data *Data) *changeRepo { return &changeRepo{data: data} }

// Claim implements biz.ChangeRepo. 时间一律用数据库时钟。
func (r *changeRepo) Claim(ctx context.Context, instance string, clusterIDs []string, lease time.Duration) (*biz.Change, error) {
	var ch biz.Change
	var requester []byte
	// 租约过期且已经开始向集群提交的变更单：结果未知，不重跑，直接记为失败。
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET status = $1, message = $2, finish_time = now(), lease_owner = '', lease_until = NULL
		 WHERE cluster_id = ANY($3::uuid[]) AND status = $4 AND lease_until < now() AND result->>'started' = 'true'`,
		k8scontract.ChangeFailed, interruptedMessage, clusterIDs, k8scontract.ChangeExecuting); err != nil {
		return nil, err
	}
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE k8s_change SET status = $1, lease_owner = $2, lease_until = now() + make_interval(secs => $3), attempts = attempts + 1
		 WHERE id = (
		     SELECT id FROM k8s_change
		     WHERE cluster_id = ANY($4::uuid[]) AND kind IN (1, 2) AND exec IS NOT NULL AND requester IS NOT NULL
		       AND expire_time > now()
		       AND (status = $5 OR (status = $1 AND lease_until < now() AND COALESCE(result->>'started', '') <> 'true'))
		     ORDER BY approve_time, id LIMIT 1 FOR UPDATE SKIP LOCKED)
		 RETURNING id::text, cluster_id::text, kind, exec, requester, client_ip`,
		k8scontract.ChangeExecuting, instance, lease.Seconds(), clusterIDs, k8scontract.ChangeApproved).
		Scan(&ch.ID, &ch.ClusterID, &ch.Kind, &ch.Exec, &requester, &ch.ClientIP)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(requester, &ch.Requester); err != nil {
		return nil, err
	}
	return &ch, nil
}

// Finish implements biz.ChangeRepo.
func (r *changeRepo) Finish(ctx context.Context, instance, id string, status int32, message string, result k8scontract.ChangeResult) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET status = $1, message = $2, result = $3, finish_time = now(), lease_owner = '', lease_until = NULL
		 WHERE id = $4 AND status = $5 AND lease_owner = $6`,
		status, message, raw, id, k8scontract.ChangeExecuting, instance)
	return err
}

// Release implements biz.ChangeRepo.
func (r *changeRepo) Release(ctx context.Context, instance, id, message string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET status = $1, message = $2, lease_owner = '', lease_until = NULL
		 WHERE id = $3 AND status = $4 AND lease_owner = $5`,
		k8scontract.ChangeApproved, message, id, k8scontract.ChangeExecuting, instance)
	return err
}

// interruptedMessage 执行器在开始提交之后失联的变更单的结果说明。
const interruptedMessage = "the executor stopped after it started changing the cluster; the change may be partly applied — verify the resources and resubmit if needed"

// MarkStarted implements biz.ChangeRepo.
func (r *changeRepo) MarkStarted(ctx context.Context, instance, id string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_change SET result = jsonb_set(COALESCE(result, '{}'::jsonb), '{started}', 'true'::jsonb)
		 WHERE id = $1 AND status = $2 AND lease_owner = $3`, id, k8scontract.ChangeExecuting, instance)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("the change is no longer leased to this instance")
	}
	return nil
}
