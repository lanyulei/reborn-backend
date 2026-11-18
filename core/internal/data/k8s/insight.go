package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.InsightRepo   = (*insightRepo)(nil)
	_ biz.ClusterBriefs = (*insightRepo)(nil)
)

// insightRepo 可观测部分的只读查询：Warning 事件（k8s_event，数据面写入）、总览摘要
// （k8s_cluster_runtime.summary）、角色数据范围展开。
type insightRepo struct {
	data *basedata.Data
}

// NewInsightRepo new an insightRepo.
func NewInsightRepo(data *basedata.Data) *insightRepo { return &insightRepo{data: data} }

// Grants implements biz.InsightRepo.
func (r *insightRepo) Grants(ctx context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT s.cluster_id::text, c.name, s.namespace, max(s.level)::int
		 FROM k8s_role_scope s JOIN k8s_cluster c ON c.id = s.cluster_id AND c.delete_time IS NULL AND c.enabled
		 WHERE s.role_id = ANY($1::uuid[]) GROUP BY s.cluster_id, c.name, s.namespace`, dbx.TextArray(roleIDs))
	if err != nil {
		return nil, fmt.Errorf("list k8s grants: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.RoleScope, error) {
		var s biz.RoleScope
		err := row.Scan(&s.ClusterID, &s.ClusterName, &s.Namespace, &s.Level)
		return &s, err
	})
}

// accessWhere 把数据范围翻译成事件表的过滤条件。集群级对象的事件（namespace 为空，如节点事件）
// 对在该集群上有任意授权的人可见——他们本来就能在界面上看到节点。
func accessWhere(a *sqlArgs, access biz.Access) string {
	if access.All {
		return ""
	}
	var any, allNS, pairC, pairN []string
	for id, ca := range access.Clusters {
		any = append(any, id)
		if ca.AllNamespaces {
			allNS = append(allNS, id)
			continue
		}
		for ns := range ca.Namespaces {
			pairC, pairN = append(pairC, id), append(pairN, ns)
		}
	}
	return fmt.Sprintf(` AND (e.cluster_id = ANY(%s::uuid[]) OR (e.namespace = '' AND e.cluster_id = ANY(%s::uuid[]))
		OR (e.cluster_id, e.namespace) IN (SELECT * FROM unnest(%s::uuid[], %s::text[])))`,
		a.add(dbx.TextArray(allNS)), a.add(dbx.TextArray(any)), a.add(dbx.TextArray(pairC)), a.add(dbx.TextArray(pairN)))
}

// ListEvents implements biz.InsightRepo.
func (r *insightRepo) ListEvents(ctx context.Context, f biz.EventFilter, access biz.Access, page, pageSize int64) ([]*biz.Event, int64, error) {
	a := &sqlArgs{}
	var w strings.Builder
	w.WriteString(" WHERE e.last_time >= " + a.add(f.Start) + " AND e.last_time <= " + a.add(f.End))
	w.WriteString(accessWhere(a, access))
	for col, v := range map[string]*string{"e.cluster_id::text": f.ClusterID, "e.namespace": f.Namespace, "e.kind": f.Kind, "e.name": f.Name, "e.reason": f.Reason} {
		if v != nil {
			w.WriteString(" AND " + col + " = " + a.add(*v))
		}
	}
	if f.Keyword != "" {
		p := a.add(dbx.LikeContains(f.Keyword))
		w.WriteString(" AND (e.name ILIKE " + p + " OR e.message ILIKE " + p + ")")
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM k8s_event e"+w.String(), a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count k8s events: %w", err)
	}
	q := `SELECT e.id::text, e.cluster_id::text, e.cluster_name, e.namespace, e.kind, e.name, e.reason, e.message, e.source,
		e.count, e.first_time, e.last_time FROM k8s_event e` + w.String() +
		" ORDER BY e.last_time DESC, e.id DESC LIMIT " + a.add(pageSize) + " OFFSET " + a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, q, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list k8s events: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Event, error) {
		var e biz.Event
		err := row.Scan(&e.ID, &e.ClusterID, &e.ClusterName, &e.Namespace, &e.Kind, &e.Name, &e.Reason, &e.Message, &e.Source,
			&e.Count, &e.FirstTime, &e.LastTime)
		return &e, err
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan k8s events: %w", err)
	}
	return items, total, nil
}

// Summaries implements biz.InsightRepo.
func (r *insightRepo) Summaries(ctx context.Context, clusterIDs []string) (map[string]*biz.ClusterSummaryRow, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT c.id::text, COALESCE(r.summary::text, '{}'), r.summary_time, c.datasource_id IS NOT NULL
		 FROM k8s_cluster c LEFT JOIN k8s_cluster_runtime r ON r.cluster_id = c.id
		 WHERE c.id = ANY($1::uuid[]) AND c.delete_time IS NULL`, dbx.TextArray(clusterIDs))
	if err != nil {
		return nil, fmt.Errorf("list k8s cluster summaries: %w", err)
	}
	defer rows.Close()
	out := map[string]*biz.ClusterSummaryRow{}
	for rows.Next() {
		var id, summary string
		var at *time.Time
		row := &biz.ClusterSummaryRow{}
		if err := rows.Scan(&id, &summary, &at, &row.HasDatasource); err != nil {
			return nil, err
		}
		row.Summary, row.SummaryTime = []byte(summary), at
		out[id] = row
	}
	return out, rows.Err()
}

// Brief implements biz.ClusterBriefs.
func (r *insightRepo) Brief(ctx context.Context, id string) (*biz.ClusterBrief, error) {
	c := &biz.ClusterBrief{ID: id}
	err := r.data.Pool.QueryRow(ctx,
		"SELECT name, enabled, COALESCE(datasource_id::text, '') FROM k8s_cluster WHERE id = $1 AND delete_time IS NULL", id).
		Scan(&c.Name, &c.Enabled, &c.DatasourceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrClusterNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get k8s cluster brief: %w", err)
	}
	return c, nil
}

// objectScopeWhere 把数据范围翻译成「对象内容」类表（审计、变更单、终端会话）的过滤条件，与
// biz.Access.AllowsObject 同口径：有「全部命名空间」授权的集群全部可见；否则只有授权的命名空间，
// 集群级对象（namespace 为空）与不属于任何集群的行不可见。
func objectScopeWhere(a *sqlArgs, access biz.Access, clusterCol, nsCol string) string {
	if access.All {
		return ""
	}
	var allNS, pairC, pairN []string
	for id, ca := range access.Clusters {
		if ca.AllNamespaces {
			allNS = append(allNS, id)
			continue
		}
		for ns := range ca.Namespaces {
			pairC, pairN = append(pairC, id), append(pairN, ns)
		}
	}
	return fmt.Sprintf(` AND (%[1]s = ANY(%[3]s::uuid[]) OR (%[2]s <> '' AND (%[1]s, %[2]s) IN (SELECT * FROM unnest(%[4]s::uuid[], %[5]s::text[]))))`,
		clusterCol, nsCol, a.add(dbx.TextArray(allNS)), a.add(dbx.TextArray(pairC)), a.add(dbx.TextArray(pairN)))
}
