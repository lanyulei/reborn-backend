package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.KubeTokenRepo     = (*govRepo)(nil)
	_ biz.KubeTokenAuthRepo = (*govRepo)(nil)
	_ biz.InspectionRepo    = (*inspectionRepo)(nil)
	_ biz.ExecRecordRepo    = (*execRecordRepo)(nil)
)

// NewGovOptions 治理类配置（k8s.kubeconfig）。
func NewGovOptions(c *conf.K8S) biz.GovOptions {
	return biz.GovOptions{ServerURL: c.GetKubeconfig().GetServerUrl(), MaxTTL: c.GetKubeconfig().GetMaxTtl().AsDuration()}
}

// govRepo 限时 kubeconfig 令牌（k8s_kube_token）。
type govRepo struct {
	data *basedata.Data
}

// NewGovRepo new a govRepo.
func NewGovRepo(data *basedata.Data) *govRepo { return &govRepo{data: data} }

// Create implements biz.KubeTokenRepo。计数与插入在同一事务，并以用户为键加事务级咨询锁，
// 并发签发不会突破上限。
func (r *govRepo) Create(ctx context.Context, t *biz.KubeToken, hash []byte, limit int, audit *biz.AuditEntry) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "k8s_kube_token:"+t.UserID); err != nil {
			return fmt.Errorf("lock kube tokens: %w", err)
		}
		var n int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM k8s_kube_token WHERE user_id = $1 AND revoke_time IS NULL AND expire_time > now()", t.UserID).Scan(&n); err != nil {
			return fmt.Errorf("count kube tokens: %w", err)
		}
		if n >= limit {
			return biz.ErrKubeTokenLimit
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_kube_token (id, user_id, username, cluster_id, token_hash, note, expire_time, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			t.ID, t.UserID, t.Username, t.ClusterID, hash, t.Note, t.ExpireTime, t.CreateTime); err != nil {
			return fmt.Errorf("insert kube token: %w", err)
		}
		return insertAudit(ctx, tx, audit, t.ClusterID, t.ClusterName, t.ID)
	})
}

// List implements biz.KubeTokenRepo.
func (r *govRepo) List(ctx context.Context, f biz.KubeTokenFilter, page, pageSize int64) ([]*biz.KubeToken, int64, error) {
	var a sqlArgs
	where := []string{"true"}
	if f.UserID != "" {
		where = append(where, "t.user_id = "+a.add(f.UserID))
	}
	if f.ClusterID != "" {
		where = append(where, "t.cluster_id = "+a.add(f.ClusterID))
	}
	if f.ActiveOnly {
		where = append(where, "t.revoke_time IS NULL AND t.expire_time > now()")
	}
	cond := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_kube_token t WHERE "+cond, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count kube tokens: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT t.id::text, t.user_id::text, t.username, t.cluster_id::text, coalesce(c.name, ''), t.note, t.create_time, t.expire_time,
		        t.last_used_time, t.last_used_ip, t.revoke_time
		 FROM k8s_kube_token t LEFT JOIN k8s_cluster c ON c.id = t.cluster_id
		 WHERE `+cond+` ORDER BY t.create_time DESC LIMIT `+a.add(pageSize)+` OFFSET `+a.add((page-1)*pageSize), a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list kube tokens: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.KubeToken, error) {
		var t biz.KubeToken
		err := row.Scan(&t.ID, &t.UserID, &t.Username, &t.ClusterID, &t.ClusterName, &t.Note, &t.CreateTime, &t.ExpireTime,
			&t.LastUsedTime, &t.LastUsedIP, &t.RevokeTime)
		return &t, err
	})
	return items, total, err
}

// Revoke implements biz.KubeTokenRepo.
func (r *govRepo) Revoke(ctx context.Context, id, ownerID, by string, audit *biz.AuditEntry) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var a sqlArgs
		cond := "revoke_time IS NULL AND expire_time > now()"
		if id != "" {
			cond += " AND id = " + a.add(id)
		}
		if ownerID != "" {
			cond += " AND user_id = " + a.add(ownerID)
		}
		if id == "" && ownerID == "" {
			return errors.New("revoke kube token: id or owner is required")
		}
		tag, err := tx.Exec(ctx, "UPDATE k8s_kube_token SET revoke_time = now(), revoke_user = "+a.add(by)+" WHERE "+cond, a.args...)
		if err != nil {
			return fmt.Errorf("revoke kube token: %w", err)
		}
		if n = int(tag.RowsAffected()); n == 0 {
			return nil
		}
		return insertAudit(ctx, tx, audit, "", "", id)
	})
	return n, err
}

// Lookup implements biz.KubeTokenAuthRepo.
func (r *govRepo) Lookup(ctx context.Context, id string) (*biz.KubeTokenAuth, error) {
	var a biz.KubeTokenAuth
	err := r.data.Pool.QueryRow(ctx,
		`SELECT t.id::text, t.user_id::text, t.cluster_id::text, t.token_hash, t.expire_time, t.revoke_time IS NOT NULL,
		        c.enabled AND c.delete_time IS NULL, c.approval_template_id IS NOT NULL
		 FROM k8s_kube_token t JOIN k8s_cluster c ON c.id = t.cluster_id WHERE t.id = $1`, id).
		Scan(&a.ID, &a.UserID, &a.ClusterID, &a.Hash, &a.ExpireTime, &a.Revoked, &a.ClusterEnabled, &a.ApprovalRequired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup kube token: %w", err)
	}
	return &a, nil
}

// Touch implements biz.KubeTokenAuthRepo.
func (r *govRepo) Touch(ctx context.Context, id, ip string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := r.data.Pool.Exec(ctx, "UPDATE k8s_kube_token SET last_used_time = now(), last_used_ip = $2 WHERE id = $1", id, ip)
	return err
}

// inspectionRepo 巡检记录（k8s_inspection_run / k8s_inspection_finding）。
type inspectionRepo struct {
	data *basedata.Data
}

// NewInspectionRepo new an inspectionRepo.
func NewInspectionRepo(data *basedata.Data) *inspectionRepo { return &inspectionRepo{data: data} }

const inspectionColumns = `id::text, cluster_id::text, cluster_name, trigger, status, score, summary, error, create_time, finish_time`

func scanInspection(row pgx.Row) (*biz.InspectionRun, error) {
	var run biz.InspectionRun
	var summary []byte
	if err := row.Scan(&run.ID, &run.ClusterID, &run.ClusterName, &run.Trigger, &run.Status, &run.Score, &summary, &run.Error,
		&run.CreateTime, &run.FinishTime); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(summary, &run.Summary)
	return &run, nil
}

// List implements biz.InspectionRepo.
func (r *inspectionRepo) List(ctx context.Context, clusterID string, page, pageSize int64) ([]*biz.InspectionRun, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_inspection_run WHERE cluster_id = $1", clusterID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count inspections: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+inspectionColumns+" FROM k8s_inspection_run WHERE cluster_id = $1 ORDER BY create_time DESC LIMIT $2 OFFSET $3",
		clusterID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list inspections: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.InspectionRun, error) { return scanInspection(row) })
	return items, total, err
}

// Get implements biz.InspectionRepo。limit ≤ 0 时不读发现项。
func (r *inspectionRepo) Get(ctx context.Context, id string, namespaces []string, limit int) (*biz.InspectionRun, []*biz.InspectionFinding, error) {
	run, err := scanInspection(r.data.Pool.QueryRow(ctx, "SELECT "+inspectionColumns+" FROM k8s_inspection_run WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, biz.ErrInspectionNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get inspection: %w", err)
	}
	if limit <= 0 {
		return run, nil, nil
	}
	var a sqlArgs
	cond := "run_id = " + a.add(id)
	if namespaces != nil {
		cond += " AND (namespace = '' OR namespace = ANY(" + a.add(namespaces) + "::text[]))"
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT check_id, severity, namespace, kind, name, message FROM k8s_inspection_finding WHERE "+cond+
			" ORDER BY severity DESC, check_id, namespace, name LIMIT "+a.add(limit), a.args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list inspection findings: %w", err)
	}
	findings, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.InspectionFinding, error) {
		var f biz.InspectionFinding
		err := row.Scan(&f.Check, &f.Severity, &f.Namespace, &f.Kind, &f.Name, &f.Message)
		return &f, err
	})
	return run, findings, err
}

// Create implements biz.InspectionRepo。同一集群的「没有在途巡检才插入」由事务级咨询锁串行化。
func (r *inspectionRepo) Create(ctx context.Context, run *biz.InspectionRun, userID string, audit *biz.AuditEntry) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "k8s_inspection:"+run.ClusterID); err != nil {
			return fmt.Errorf("lock inspection: %w", err)
		}
		var busy bool
		// 执行中超过 30 分钟的视为被中断，不阻塞新的手动巡检（数据面会重新领取它）。
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM k8s_inspection_run WHERE cluster_id = $1
			     AND (status = $2 OR (status = $3 AND start_time > now() - interval '30 minutes')))`,
			run.ClusterID, kc.InspectPending, kc.InspectRunning).Scan(&busy); err != nil {
			return fmt.Errorf("check inspection: %w", err)
		}
		if busy {
			return biz.ErrInspectionBusy
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_inspection_run (id, cluster_id, cluster_name, trigger, status, create_user, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			run.ID, run.ClusterID, run.ClusterName, run.Trigger, run.Status, dbx.NilIfEmpty(userID), run.CreateTime); err != nil {
			return fmt.Errorf("insert inspection: %w", err)
		}
		return insertAudit(ctx, tx, audit, run.ClusterID, run.ClusterName, run.ID)
	})
}

// execRecordRepo 终端录制读取（k8s_exec_record，数据面写入）。
type execRecordRepo struct {
	data *basedata.Data
}

// NewExecRecordRepo new an execRecordRepo.
func NewExecRecordRepo(data *basedata.Data) *execRecordRepo { return &execRecordRepo{data: data} }

// maxRecordChunks 单个会话读取的录制块上限（8MiB 上限 / 256KiB 一块 ≈ 32 块，留足余量）。
const maxRecordChunks = 4096

// Recording implements biz.ExecRecordRepo.
func (r *execRecordRepo) Recording(ctx context.Context, sessionID string) (*biz.ExecRecording, error) {
	out := &biz.ExecRecording{Session: &biz.ExecSession{}}
	s := out.Session
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id::text, user_id::text, username, cluster_id::text, cluster_name, namespace, pod, container, kind, start_time, end_time,
		        recorded, record_truncated
		 FROM k8s_exec_session WHERE id = $1`, sessionID).
		Scan(&s.ID, &s.UserID, &s.Username, &s.ClusterID, &s.ClusterName, &s.Namespace, &s.Pod, &s.Container, &s.Kind, &s.StartTime, &s.EndTime,
			&s.Recorded, &out.Truncated)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !s.Recorded) {
		return nil, biz.ErrExecRecordNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get exec session: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT data FROM k8s_exec_record WHERE session_id = $1 ORDER BY seq LIMIT $2", sessionID, maxRecordChunks)
	if err != nil {
		return nil, fmt.Errorf("list exec record: %w", err)
	}
	out.Chunks, err = pgx.CollectRows(rows, pgx.RowTo[[]byte])
	return out, err
}

// AuditReplay implements biz.ExecRecordRepo.
func (r *execRecordRepo) AuditReplay(ctx context.Context, audit *biz.AuditEntry, s *biz.ExecSession) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return insertAudit(ctx, tx, audit, s.ClusterID, s.ClusterName, s.ID)
	})
}
