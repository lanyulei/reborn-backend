package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.AuditReader = (*auditRepo)(nil)

// auditColumns 审计列表列序（不含对象内容）。
const auditColumns = `id::text, create_time, COALESCE(user_id::text, ''), username, client_ip, COALESCE(cluster_id::text, ''),
	cluster_name, namespace, api_group, api_version, resource, kind, name, action, result, error, emergency,
	COALESCE(change_id::text, ''), truncated`

type auditRepo struct {
	data *basedata.Data
}

// NewAuditRepo new an auditRepo.
func NewAuditRepo(data *basedata.Data) *auditRepo { return &auditRepo{data: data} }

func auditDest(a *biz.Audit) []any {
	return []any{
		&a.ID, &a.CreateTime, &a.UserID, &a.Username, &a.ClientIP, &a.ClusterID, &a.ClusterName, &a.Namespace,
		&a.APIGroup, &a.APIVersion, &a.Resource, &a.Kind, &a.Name, &a.Action, &a.Result, &a.Error, &a.Emergency, &a.ChangeID, &a.Truncated,
	}
}

// List implements biz.AuditReader。时间窗口恒有（用例已规整），分区裁剪生效。
func (r *auditRepo) List(ctx context.Context, f biz.AuditFilter, page, pageSize int64) ([]*biz.Audit, int64, error) {
	var a sqlArgs
	where := "create_time >= " + a.add(f.Start) + " AND create_time <= " + a.add(f.End)
	where += objectScopeWhere(&a, f.Access, "cluster_id", "namespace")
	if f.ClusterID != "" {
		where += " AND cluster_id = " + a.add(f.ClusterID)
	}
	if f.Namespace != "" {
		where += " AND namespace = " + a.add(f.Namespace)
	}
	if f.Resource != "" {
		where += " AND resource = " + a.add(f.Resource)
	}
	if f.Name != "" {
		where += " AND name = " + a.add(f.Name)
	}
	if f.Action != "" {
		where += " AND action = " + a.add(f.Action)
	}
	if f.UserID != "" {
		where += " AND user_id = " + a.add(f.UserID)
	}
	if f.Result != nil {
		where += " AND result = " + a.add(*f.Result)
	}
	if f.Keyword != "" {
		k := a.add(dbx.LikeContains(f.Keyword))
		where += " AND (name ILIKE " + k + " OR username ILIKE " + k + ")"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_audit WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s audits: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+auditColumns+" FROM k8s_audit WHERE "+where+
		" ORDER BY create_time DESC, id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s audits: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Audit, 0, pageSize)
	for rows.Next() {
		var item biz.Audit
		if err := rows.Scan(auditDest(&item)...); err != nil {
			return nil, 0, fmt.Errorf("scan k8s audit: %w", err)
		}
		items = append(items, &item)
	}
	return items, total, rows.Err()
}

// Get implements biz.AuditReader。
func (r *auditRepo) Get(ctx context.Context, id string) (*biz.Audit, error) {
	var item biz.Audit
	var before, after, patch []byte
	dest := append(auditDest(&item), &before, &after, &patch)
	err := r.data.Pool.QueryRow(ctx,
		"SELECT "+auditColumns+", before_obj, after_obj, patch FROM k8s_audit WHERE id = $1 LIMIT 1", id).Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrAuditNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s audit: %w", err)
	}
	item.BeforeObj, item.AfterObj, item.Patch = before, after, patch
	return &item, nil
}

// ListExecSessions implements biz.AuditReader。
func (r *auditRepo) ListExecSessions(ctx context.Context, f biz.ExecSessionFilter, page, pageSize int64) ([]*biz.ExecSession, int64, error) {
	var a sqlArgs
	where := "start_time >= " + a.add(f.Start) + " AND start_time <= " + a.add(f.End)
	where += objectScopeWhere(&a, f.Access, "cluster_id", "namespace")
	if f.ClusterID != "" {
		where += " AND cluster_id = " + a.add(f.ClusterID)
	}
	if f.UserID != "" {
		where += " AND user_id = " + a.add(f.UserID)
	}
	if f.Keyword != "" {
		k := a.add(dbx.LikeContains(f.Keyword))
		where += " AND (pod ILIKE " + k + " OR username ILIKE " + k + ")"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_exec_session WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s exec sessions: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, user_id::text, username, client_ip, cluster_id::text, cluster_name, namespace, pod, container, kind,
		   start_time, end_time, end_reason, bytes_in, bytes_out, recorded
		 FROM k8s_exec_session WHERE `+where+" ORDER BY start_time DESC, id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s exec sessions: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ExecSession, 0, pageSize)
	for rows.Next() {
		var s biz.ExecSession
		if err := rows.Scan(&s.ID, &s.UserID, &s.Username, &s.ClientIP, &s.ClusterID, &s.ClusterName, &s.Namespace, &s.Pod, &s.Container,
			&s.Kind, &s.StartTime, &s.EndTime, &s.EndReason, &s.BytesIn, &s.BytesOut, &s.Recorded); err != nil {
			return nil, 0, fmt.Errorf("scan k8s exec session: %w", err)
		}
		items = append(items, &s)
	}
	return items, total, rows.Err()
}
