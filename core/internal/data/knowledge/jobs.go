package data

import (
	"context"
	"errors"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.LinkRepo      = (*linkRepo)(nil)
	_ biz.IngestRepo    = (*ingestRepo)(nil)
	_ biz.IndexRepo     = (*indexRepo)(nil)
	_ biz.SearchLogRepo = (*searchLogRepo)(nil)
)

// ---------- 关联 ----------

type linkRepo struct {
	data *basedata.Data
}

// NewLinkRepo new a linkRepo.
func NewLinkRepo(data *basedata.Data) *linkRepo { return &linkRepo{data: data} }

// Create implements biz.LinkRepo.
func (r *linkRepo) Create(ctx context.Context, l *biz.Link) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO knowledge_link (id, document_id, target_type, target_id, target_name, create_user, create_time) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		l.ID, l.DocumentID, l.TargetType, l.TargetID, l.TargetName, l.CreateUser, l.CreateTime)
	if isUniqueViolation(err, "uq_knowledge_link") {
		return biz.ErrLinkExists
	}
	if err != nil {
		return wrap("insert knowledge link", err)
	}
	return nil
}

// Delete implements biz.LinkRepo.
func (r *linkRepo) Delete(ctx context.Context, documentID, linkID string) error {
	tag, err := r.data.Pool.Exec(ctx, `DELETE FROM knowledge_link WHERE id=$1 AND document_id=$2`, linkID, documentID)
	if err != nil {
		return wrap("delete knowledge link", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrLinkNotFound
	}
	return nil
}

// ListByDocument implements biz.LinkRepo.
func (r *linkRepo) ListByDocument(ctx context.Context, documentID string) ([]*biz.Link, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, document_id, target_type, target_id, target_name, create_user, create_time FROM knowledge_link WHERE document_id=$1 ORDER BY create_time, id`, documentID)
	if err != nil {
		return nil, wrap("list knowledge links", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Link, error) {
		var l biz.Link
		return &l, row.Scan(&l.ID, &l.DocumentID, &l.TargetType, &l.TargetID, &l.TargetName, &l.CreateUser, &l.CreateTime)
	})
	if err != nil {
		return nil, wrap("scan knowledge link", err)
	}
	return items, nil
}

// ListByTarget implements biz.LinkRepo.
func (r *linkRepo) ListByTarget(ctx context.Context, targetType, targetID string, unrestricted bool, spaceIDs []string) ([]*biz.LinkedDocument, error) {
	var a sqlArgs
	where := "l.target_type = " + a.add(targetType) + " AND l.target_id = " + a.add(targetID) + " AND d.delete_time IS NULL AND d.status = 2 AND s.delete_time IS NULL"
	if !unrestricted {
		where += " AND d.space_id = ANY(" + a.add(dbx.TextArray(spaceIDs)) + "::uuid[])"
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT l.id, d.id, d.title, s.name, COALESCE(NULLIF(d.summary, ''), d.ai_summary), d.review_at FROM knowledge_link l
		 JOIN knowledge_document d ON d.id = l.document_id JOIN knowledge_space s ON s.id = d.space_id WHERE `+where+` ORDER BY l.create_time DESC LIMIT 50`, a.args...)
	if err != nil {
		return nil, wrap("list knowledge links by target", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.LinkedDocument, error) {
		var d biz.LinkedDocument
		return &d, row.Scan(&d.LinkID, &d.DocumentID, &d.Title, &d.SpaceName, &d.Summary, &d.ReviewAt)
	})
	if err != nil {
		return nil, wrap("scan knowledge linked document", err)
	}
	return items, nil
}

// ---------- 导入任务 ----------

type ingestRepo struct {
	data *basedata.Data
}

// NewIngestRepo new an ingestRepo.
func NewIngestRepo(data *basedata.Data) *ingestRepo { return &ingestRepo{data: data} }

const ingestColumns = `id, space_id, document_id, file_id, filename, title, tags, publish, status, error, chunk_count, attempts, create_user, create_time, update_time`

func scanIngest(scan func(dest ...any) error) (*biz.IngestJob, error) {
	var j biz.IngestJob
	var status int16
	if err := scan(&j.ID, &j.SpaceID, &j.DocumentID, &j.FileID, &j.Filename, &j.Title, &j.Tags, &j.Publish, &status, &j.Error, &j.ChunkCount,
		&j.Attempts, &j.CreateUser, &j.CreateTime, &j.UpdateTime); err != nil {
		return nil, err
	}
	j.Status = int32(status)
	if j.Tags == nil {
		j.Tags = []string{}
	}
	return &j, nil
}

// Create implements biz.IngestRepo.
func (r *ingestRepo) Create(ctx context.Context, j *biz.IngestJob) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO knowledge_ingest_job (`+ingestColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		j.ID, j.SpaceID, nullStr(j.DocumentID), j.FileID, j.Filename, j.Title, dbx.TextArray(j.Tags), j.Publish, j.Status, j.Error, j.ChunkCount,
		j.Attempts, j.CreateUser, j.CreateTime, j.UpdateTime)
	if err != nil {
		return wrap("insert knowledge ingest job", err)
	}
	return nil
}

// Get implements biz.IngestRepo.
func (r *ingestRepo) Get(ctx context.Context, id string) (*biz.IngestJob, error) {
	j, err := scanIngest(r.data.Pool.QueryRow(ctx, `SELECT `+ingestColumns+` FROM knowledge_ingest_job WHERE id=$1`, id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrIngestNotFound
	}
	if err != nil {
		return nil, wrap("get knowledge ingest job", err)
	}
	return j, nil
}

// List implements biz.IngestRepo.
func (r *ingestRepo) List(ctx context.Context, f biz.IngestFilter, page, pageSize int64) ([]*biz.IngestJob, int64, error) {
	var a sqlArgs
	where := "TRUE"
	if !f.Unrestricted {
		where += " AND space_id = ANY(" + a.add(dbx.TextArray(f.SpaceIDs)) + "::uuid[])"
	}
	if f.SpaceID != "" {
		where += " AND space_id = " + a.add(f.SpaceID)
	}
	if f.Status != nil {
		where += " AND status = " + a.add(*f.Status)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM knowledge_ingest_job WHERE `+where, a.args...).Scan(&total); err != nil {
		return nil, 0, wrap("count knowledge ingest jobs", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, `SELECT `+ingestColumns+` FROM knowledge_ingest_job WHERE `+where+` ORDER BY create_time DESC, id LIMIT `+limit+` OFFSET `+offset, a.args...)
	if err != nil {
		return nil, 0, wrap("list knowledge ingest jobs", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.IngestJob, error) { return scanIngest(row.Scan) })
	if err != nil {
		return nil, 0, wrap("scan knowledge ingest job", err)
	}
	return items, total, nil
}

// Requeue implements biz.IngestRepo.
func (r *ingestRepo) Requeue(ctx context.Context, id string) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx, `UPDATE knowledge_ingest_job SET status=1, attempts=0, error='', lease_until=NULL, update_time=now() WHERE id=$1 AND status=4`, id)
	if err != nil {
		return false, wrap("requeue knowledge ingest job", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Claim implements biz.IngestRepo.
func (r *ingestRepo) Claim(ctx context.Context, lease time.Duration, limit int) ([]*biz.IngestJob, error) {
	rows, err := r.data.Pool.Query(ctx,
		`WITH picked AS (
		   SELECT id FROM knowledge_ingest_job WHERE status = 1 OR (status = 2 AND lease_until < now())
		   ORDER BY create_time LIMIT $1 FOR UPDATE SKIP LOCKED)
		 UPDATE knowledge_ingest_job j SET status = 2, attempts = j.attempts + 1, lease_until = now() + make_interval(secs => $2), update_time = now()
		 FROM picked WHERE j.id = picked.id RETURNING `+qualify(ingestColumns, "j."), limit, lease.Seconds())
	if err != nil {
		return nil, wrap("claim knowledge ingest jobs", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.IngestJob, error) { return scanIngest(row.Scan) })
	if err != nil {
		return nil, wrap("scan knowledge ingest job", err)
	}
	return items, nil
}

// Finish implements biz.IngestRepo。status 为处理中时保留领取时的租约（租约到期才会被再次领取，充当重试退避），其余状态清空。
func (r *ingestRepo) Finish(ctx context.Context, id string, status int32, documentID *string, chunkCount int32, errMsg string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE knowledge_ingest_job SET status=$2::int, document_id=COALESCE($3, document_id), chunk_count=$4, error=$5,
		   lease_until=CASE WHEN $2::int = 2 THEN lease_until ELSE NULL END, update_time=now() WHERE id=$1`,
		id, status, nullStr(documentID), chunkCount, errMsg)
	if err != nil {
		return wrap("finish knowledge ingest job", err)
	}
	return nil
}

// SetDocumentTx implements biz.IngestRepo.
func (r *ingestRepo) SetDocumentTx(ctx context.Context, tx txrun.Tx, id, documentID string) error {
	if _, err := tx.Exec(ctx, `UPDATE knowledge_ingest_job SET document_id=$2, update_time=now() WHERE id=$1`, id, documentID); err != nil {
		return wrap("bind knowledge ingest job document", err)
	}
	return nil
}

// purgeFinishedIngestSQL 已结束的导入任务的保留期清理（PurgeFinished 用；单独成常量供集成测试在真实库上核对
// 执行计划）。$1 保留期，$2 批大小；时间取数据库时钟，与 update_time 的写入（领取 / 落账均写 now()）同一时钟源。
//
// 子查询的取行条件与迁移 0090 的部分索引 idx_knowledge_ingest_job_finished 的谓词逐字相同：部分索引只有在查询
// 条件能推出索引谓词时才会用得上；按 update_time 有序取满一批即停，积压再大每批也只扫一批的量。
//
// 外层 DELETE 把终态与时间条件再写一遍：READ COMMITTED 下，子查询取行之后、行锁到手之前，别的会话若恰好改写了
// 这条任务（重试一条失败任务会把它置回排队），DELETE 等到行锁后会按改写后的行重检外层条件，不再满足就跳过。
// 只按主键删的话，用户刚重试的任务会被这次清理顺手删掉。
const purgeFinishedIngestSQL = `DELETE FROM knowledge_ingest_job j WHERE j.id IN (
	   SELECT id FROM knowledge_ingest_job WHERE status IN (3, 4) AND update_time < now() - $1::interval ORDER BY update_time LIMIT $2)
	AND j.status IN (3, 4) AND j.update_time < now() - $1::interval`

// PurgeFinished implements biz.IngestRepo.PurgeFinished。语句幂等：多实例同时清理时后到者删到 0 行。
func (r *ingestRepo) PurgeFinished(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, purgeFinishedIngestSQL, olderThan, limit)
	if err != nil {
		return 0, wrap("purge finished knowledge ingest jobs", err)
	}
	return int(tag.RowsAffected()), nil
}

// ---------- 索引任务 ----------

type indexRepo struct {
	data *basedata.Data
}

// NewIndexRepo new an indexRepo.
func NewIndexRepo(data *basedata.Data) *indexRepo { return &indexRepo{data: data} }

const indexUpsert = `INSERT INTO knowledge_index_job (document_id, version, status, attempts, error, lease_until, next_run_time, create_time, update_time)
 VALUES ($1, $2, 1, 0, '', NULL, now(), now(), now())
 ON CONFLICT (document_id) DO UPDATE SET version = EXCLUDED.version, status = 1, attempts = 0, error = '', lease_until = NULL, next_run_time = now(), update_time = now()`

// UpsertTx implements biz.IndexRepo.
func (r *indexRepo) UpsertTx(ctx context.Context, tx txrun.Tx, documentID string, version int32) error {
	if _, err := tx.Exec(ctx, indexUpsert, documentID, version); err != nil {
		return wrap("upsert knowledge index job", err)
	}
	return nil
}

// Upsert implements biz.IndexRepo.
func (r *indexRepo) Upsert(ctx context.Context, documentID string, version int32) error {
	if _, err := r.data.Pool.Exec(ctx, indexUpsert, documentID, version); err != nil {
		return wrap("upsert knowledge index job", err)
	}
	return nil
}

// DeleteTx implements biz.IndexRepo.
func (r *indexRepo) DeleteTx(ctx context.Context, tx txrun.Tx, documentID string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM knowledge_index_job WHERE document_id=$1`, documentID); err != nil {
		return wrap("delete knowledge index job", err)
	}
	return nil
}

// Claim implements biz.IndexRepo.
func (r *indexRepo) Claim(ctx context.Context, lease time.Duration, limit int) ([]*biz.IndexJob, error) {
	rows, err := r.data.Pool.Query(ctx,
		`WITH picked AS (
		   SELECT document_id FROM knowledge_index_job
		   WHERE (status = 1 AND next_run_time <= now()) OR (status = 2 AND lease_until < now())
		   ORDER BY next_run_time LIMIT $1 FOR UPDATE SKIP LOCKED)
		 UPDATE knowledge_index_job j SET status = 2, attempts = j.attempts + 1, lease_until = now() + make_interval(secs => $2), update_time = now()
		 FROM picked WHERE j.document_id = picked.document_id RETURNING j.document_id, j.version, j.status, j.attempts, j.error, j.next_run_time`,
		limit, lease.Seconds())
	if err != nil {
		return nil, wrap("claim knowledge index jobs", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.IndexJob, error) {
		var j biz.IndexJob
		var status int16
		if err := row.Scan(&j.DocumentID, &j.Version, &status, &j.Attempts, &j.Error, &j.NextRunTime); err != nil {
			return nil, err
		}
		j.Status = int32(status)
		return &j, nil
	})
	if err != nil {
		return nil, wrap("scan knowledge index job", err)
	}
	return items, nil
}

// Finish implements biz.IndexRepo：成功删除任务行（任务版本仍是当前版本时）；其余状态回写。
func (r *indexRepo) Finish(ctx context.Context, j *biz.IndexJob, status int32, errMsg string, nextRun time.Time) error {
	var err error
	if status == biz.JobSucceeded {
		_, err = r.data.Pool.Exec(ctx, `DELETE FROM knowledge_index_job WHERE document_id=$1 AND version=$2 AND status=2`, j.DocumentID, j.Version)
	} else {
		_, err = r.data.Pool.Exec(ctx,
			`UPDATE knowledge_index_job SET status=$3, error=$4, lease_until=NULL, next_run_time=$5, update_time=now() WHERE document_id=$1 AND version=$2 AND status=2`,
			j.DocumentID, j.Version, status, errMsg, nextRun)
	}
	if err != nil {
		return wrap("finish knowledge index job", err)
	}
	return nil
}

// Counts implements biz.IndexRepo.
func (r *indexRepo) Counts(ctx context.Context) (pending, failed int64, err error) {
	err = r.data.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status IN (1, 2)), count(*) FILTER (WHERE status = 4) FROM knowledge_index_job`).Scan(&pending, &failed)
	if err != nil {
		return 0, 0, wrap("count knowledge index jobs", err)
	}
	return pending, failed, nil
}

// ---------- 检索日志 ----------

type searchLogRepo struct {
	data *basedata.Data
}

// NewSearchLogRepo new a searchLogRepo.
func NewSearchLogRepo(data *basedata.Data) *searchLogRepo { return &searchLogRepo{data: data} }

// Create implements biz.SearchLogRepo.
func (r *searchLogRepo) Create(ctx context.Context, l *biz.SearchLog) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO knowledge_search_log (id, user_id, source, query, top_k, hit_count, degraded, duration_ms, chunk_ids, feedback, create_time)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::uuid[],$10,$11)`,
		l.ID, l.UserID, l.Source, l.Query, l.TopK, l.HitCount, l.Degraded, l.DurationMS, dbx.TextArray(l.ChunkIDs), l.Feedback, l.CreateTime)
	if err != nil {
		return wrap("insert knowledge search log", err)
	}
	return nil
}

// SetFeedback implements biz.SearchLogRepo.
func (r *searchLogRepo) SetFeedback(ctx context.Context, id, userID string, feedback int32) error {
	tag, err := r.data.Pool.Exec(ctx, `UPDATE knowledge_search_log SET feedback=$3 WHERE id=$1 AND user_id=$2`, id, userID, feedback)
	if err != nil {
		return wrap("set knowledge search feedback", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSearchLogNotFound
	}
	return nil
}

// List implements biz.SearchLogRepo.
func (r *searchLogRepo) List(ctx context.Context, f biz.SearchLogFilter, page, pageSize int64) ([]*biz.SearchLog, int64, error) {
	var a sqlArgs
	where := "TRUE"
	if f.Source != "" {
		where += " AND source = " + a.add(f.Source)
	}
	if f.NoResult {
		where += " AND hit_count = 0"
	}
	if f.Keyword != "" {
		where += " AND query ILIKE " + a.add(dbx.LikeContains(f.Keyword))
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM knowledge_search_log WHERE `+where, a.args...).Scan(&total); err != nil {
		return nil, 0, wrap("count knowledge search logs", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, user_id, source, query, top_k, hit_count, degraded, duration_ms, feedback, create_time FROM knowledge_search_log WHERE `+where+
			` ORDER BY create_time DESC, id LIMIT `+limit+` OFFSET `+offset, a.args...)
	if err != nil {
		return nil, 0, wrap("list knowledge search logs", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.SearchLog, error) {
		var l biz.SearchLog
		var fb int16
		if err := row.Scan(&l.ID, &l.UserID, &l.Source, &l.Query, &l.TopK, &l.HitCount, &l.Degraded, &l.DurationMS, &fb, &l.CreateTime); err != nil {
			return nil, err
		}
		l.Feedback = int32(fb)
		return &l, nil
	})
	if err != nil {
		return nil, 0, wrap("scan knowledge search log", err)
	}
	return items, total, nil
}

// Stats implements biz.SearchLogRepo.
func (r *searchLogRepo) Stats(ctx context.Context, since time.Time) (biz.SearchStats, error) {
	var s biz.SearchStats
	err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE hit_count = 0), count(*) FILTER (WHERE feedback = 1), count(*) FILTER (WHERE feedback = 2)
		 FROM knowledge_search_log WHERE create_time >= $1`, since).Scan(&s.SearchCount, &s.NoResultCount, &s.HelpfulCount, &s.UnhelpfulCount)
	if err != nil {
		return s, wrap("knowledge search stats", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT query, count(*) FROM knowledge_search_log WHERE create_time >= $1 AND hit_count = 0 GROUP BY query ORDER BY 2 DESC, 1 LIMIT 10`, since)
	if err != nil {
		return s, wrap("knowledge no-result queries", err)
	}
	s.TopNoResult, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.QueryCount, error) {
		var q biz.QueryCount
		return q, row.Scan(&q.Query, &q.Count)
	})
	if err != nil {
		return s, wrap("scan knowledge no-result query", err)
	}
	return s, nil
}

// Purge implements biz.SearchLogRepo.
func (r *searchLogRepo) Purge(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM knowledge_search_log WHERE id IN (SELECT id FROM knowledge_search_log WHERE create_time < $1 ORDER BY create_time LIMIT $2)`, before, limit)
	if err != nil {
		return 0, wrap("purge knowledge search logs", err)
	}
	return int(tag.RowsAffected()), nil
}

// qualify 给逗号分隔的列清单加表别名前缀。
func qualify(columns, prefix string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = prefix + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}
