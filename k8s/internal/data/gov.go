package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

// Record implements biz.ExecRepo：追加一块终端录制并更新会话的录制标记（同一事务）。
func (r *auditRepo) Record(ctx context.Context, id string, seq int, data []byte, truncated bool) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if len(data) > 0 {
			if _, err := tx.Exec(ctx,
				`INSERT INTO k8s_exec_record (session_id, seq, data) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, id, seq, data); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx,
			`UPDATE k8s_exec_session SET recorded = recorded OR $2, record_truncated = record_truncated OR $3 WHERE id = $1`,
			id, len(data) > 0, truncated)
		return err
	})
}

// inspectionRepo 巡检运行的领取与结果写入（k8s_inspection_run / k8s_inspection_finding）。
type inspectionRepo struct {
	data *Data
}

// NewInspectionRepo new an inspectionRepo.
func NewInspectionRepo(d *Data) *inspectionRepo { return &inspectionRepo{data: d} }

var _ biz.InspectionRepo = (*inspectionRepo)(nil)

// staleRunning 执行中的运行超过此时长视为被中断（实例崩溃），允许重新领取。
const staleRunning = 30 * time.Minute

// Claim implements biz.InspectionRepo.
func (r *inspectionRepo) Claim(ctx context.Context, clusterIDs []string, interval time.Duration) (*biz.InspectionRun, error) {
	var run *biz.InspectionRun
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var got biz.InspectionRun
		err := tx.QueryRow(ctx,
			`UPDATE k8s_inspection_run SET status = $2, start_time = now()
			 WHERE id = (
			     SELECT id FROM k8s_inspection_run
			     WHERE cluster_id = ANY($1::uuid[])
			       AND (status = $3 OR (status = $2 AND start_time < now() - make_interval(secs => $4)))
			     ORDER BY create_time LIMIT 1 FOR UPDATE SKIP LOCKED)
			 RETURNING id::text, cluster_id::text, trigger`,
			clusterIDs, k8scontract.InspectRunning, k8scontract.InspectPending, staleRunning.Seconds()).
			Scan(&got.ID, &got.ClusterID, &got.Trigger)
		if err == nil {
			run = &got
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("claim inspection run: %w", err)
		}
		if interval <= 0 {
			return nil
		}
		// 到期的集群：最近一次运行（任何状态）早于 interval，或从未巡检。取最久未巡检的一个。
		var clusterID string
		err = tx.QueryRow(ctx,
			`SELECT c.id::text FROM k8s_cluster c
			 LEFT JOIN LATERAL (SELECT max(create_time) AS last FROM k8s_inspection_run WHERE cluster_id = c.id) l ON true
			 WHERE c.id = ANY($1::uuid[]) AND c.delete_time IS NULL AND c.enabled
			   AND (l.last IS NULL OR l.last < now() - make_interval(secs => $2))
			 ORDER BY l.last NULLS FIRST LIMIT 1`, clusterIDs, interval.Seconds()).Scan(&clusterID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("find due inspection: %w", err)
		}
		id, err := idgen.NewID()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_inspection_run (id, cluster_id, trigger, status, start_time) VALUES ($1, $2, $3, $4, now())`,
			id, clusterID, k8scontract.InspectScheduled, k8scontract.InspectRunning); err != nil {
			return fmt.Errorf("create scheduled inspection: %w", err)
		}
		run = &biz.InspectionRun{ID: id, ClusterID: clusterID, Trigger: k8scontract.InspectScheduled}
		return nil
	})
	return run, err
}

// Finish implements biz.InspectionRepo.
func (r *inspectionRepo) Finish(ctx context.Context, run *biz.InspectionRun, clusterName string, res *biz.InspectionResult, score int, errMsg string, keep int) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		status, summary := k8scontract.InspectDone, []byte("{}")
		if errMsg != "" {
			status = k8scontract.InspectFailed
		}
		if res != nil {
			raw, err := json.Marshal(res.Summary)
			if err != nil {
				return err
			}
			summary = raw
			rows := make([][]any, 0, len(res.Findings))
			for _, f := range res.Findings {
				id, err := idgen.NewID()
				if err != nil {
					return err
				}
				rows = append(rows, []any{id, run.ID, f.Check, f.Severity, clip(f.Namespace, 63), clip(f.Kind, 128), clip(f.Name, 253), clip(f.Message, 512)})
			}
			if len(rows) > 0 {
				if _, err := tx.CopyFrom(ctx, pgx.Identifier{"k8s_inspection_finding"},
					[]string{"id", "run_id", "check_id", "severity", "namespace", "kind", "name", "message"}, pgx.CopyFromRows(rows)); err != nil {
					return fmt.Errorf("write inspection findings: %w", err)
				}
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE k8s_inspection_run SET status = $2, score = $3, summary = $4, error = $5, cluster_name = $6, finish_time = now() WHERE id = $1`,
			run.ID, status, score, summary, clip(errMsg, 512), clip(clusterName, 63)); err != nil {
			return fmt.Errorf("finish inspection run: %w", err)
		}
		// 只保留最近 keep 次（发现项随运行级联删除）。
		_, err := tx.Exec(ctx,
			`DELETE FROM k8s_inspection_run WHERE cluster_id = $1 AND id IN (
			     SELECT id FROM k8s_inspection_run WHERE cluster_id = $1 ORDER BY create_time DESC OFFSET $2)`,
			run.ClusterID, keep)
		return err
	})
}
