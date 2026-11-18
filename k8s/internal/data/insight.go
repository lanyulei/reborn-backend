package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

// insightRepo 总览摘要与 Warning 事件（k8s_cluster_runtime.summary、k8s_event）。
type insightRepo struct{ data *Data }

// NewInsightRepo new an insightRepo.
func NewInsightRepo(data *Data) *insightRepo { return &insightRepo{data: data} }

var (
	_ biz.SummaryRepo = (*insightRepo)(nil)
	_ biz.EventRepo   = (*insightRepo)(nil)
)

// SaveSummary implements biz.SummaryRepo（以 owner_instance 围栏）。
func (r *insightRepo) SaveSummary(ctx context.Context, instance, clusterID string, s k8scontract.ClusterSummary) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = r.data.Pool.Exec(ctx,
		"UPDATE k8s_cluster_runtime SET summary = $3, summary_time = now() WHERE cluster_id = $1 AND owner_instance = $2",
		clusterID, instance, string(raw))
	return err
}

// Reasons implements biz.EventRepo.
func (r *insightRepo) Reasons(ctx context.Context) (map[string][]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, event_reasons FROM k8s_cluster WHERE enabled AND delete_time IS NULL AND cardinality(event_reasons) > 0")
	if err != nil {
		return nil, fmt.Errorf("read event reasons: %w", err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var id string
		var reasons []string
		if err := rows.Scan(&id, &reasons); err != nil {
			return nil, err
		}
		out[id] = reasons
	}
	return out, rows.Err()
}

// Upsert implements biz.EventRepo。
func (r *insightRepo) Upsert(ctx context.Context, rec biz.EventRecord, first bool) (string, int64, error) {
	id, err := idgen.NewID()
	if err != nil {
		return "", 0, err
	}
	var count int64
	err = r.data.Pool.QueryRow(ctx,
		`INSERT INTO k8s_event (id, cluster_id, cluster_name, namespace, kind, name, reason, message, source, count, first_time, last_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 ON CONFLICT (cluster_id, namespace, kind, name, reason) DO UPDATE SET
		     cluster_name = EXCLUDED.cluster_name,
		     message = CASE WHEN EXCLUDED.last_time >= k8s_event.last_time THEN EXCLUDED.message ELSE k8s_event.message END,
		     source = EXCLUDED.source,
		     count = CASE WHEN $13 THEN GREATEST(k8s_event.count, EXCLUDED.count) ELSE k8s_event.count + $14 END,
		     first_time = LEAST(k8s_event.first_time, EXCLUDED.first_time),
		     last_time = GREATEST(k8s_event.last_time, EXCLUDED.last_time)
		 RETURNING id::text, count`,
		id, rec.ClusterID, rec.ClusterName, clip(rec.Namespace, 63), clip(rec.Kind, 128), clip(rec.Name, 253), clip(rec.Reason, 128),
		clipRunes(rec.Message, 1024), clip(rec.Source, 128), max(rec.Absolute, 1), rec.FirstTime, rec.LastTime, first, rec.Delta).
		Scan(&id, &count)
	return id, count, err
}

func clipRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// ClaimNotify implements biz.EventRepo：通知门闩与发件箱写入在同一事务，多实例并发时至多一个成功。
func (r *insightRepo) ClaimNotify(ctx context.Context, id string, window time.Duration, build func(at time.Time) biz.OutboxEvent) (bool, error) {
	claimed := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var at time.Time
		err := tx.QueryRow(ctx,
			`UPDATE k8s_event SET notify_time = now()
			 WHERE id = $1 AND (notify_time IS NULL OR notify_time < now() - make_interval(secs => $2)) RETURNING notify_time`,
			id, window.Seconds()).Scan(&at)
		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		ev := build(at)
		payload, err := json.Marshal(ev.Payload)
		if err != nil {
			return err
		}
		oid, err := idgen.NewID()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO k8s_notify_outbox (id, cluster_id, event_type, severity, dedup_key, group_key, payload)
			 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (dedup_key) DO NOTHING`,
			oid, ev.ClusterID, ev.EventType, ev.Severity, clip(ev.DedupKey, 255), clip(ev.GroupKey, 255), payload); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claimed, err
}

// PurgeEvents 删除最近发生时间早于保留期的 Warning 事件（维护任务调用）。
func (r *insightRepo) PurgeEvents(ctx context.Context, retention time.Duration) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM k8s_event WHERE last_time < now() - make_interval(secs => $1)", retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("purge warning events: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
