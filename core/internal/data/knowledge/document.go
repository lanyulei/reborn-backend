package data

import (
	"context"
	"errors"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.DocumentRepo = (*documentRepo)(nil)
	_ biz.VersionRepo  = (*versionRepo)(nil)
)

// documentRepo 知识文档仓储。
type documentRepo struct {
	data *basedata.Data
}

// NewDocumentRepo new a documentRepo.
func NewDocumentRepo(data *basedata.Data) *documentRepo {
	return &documentRepo{data: data}
}

// docListColumns 列表列（不含正文；草稿只带有无标记）。
const docListColumns = `d.id, d.space_id, d.slug, d.title, d.status, d.review_state, d.review_note, d.summary, d.ai_summary, d.tags, d.source,
 d.source_ref, d.owner_id, d.review_at, d.review_notify_time, d.version, d.index_status, d.create_user, d.update_user, d.create_time, d.update_time,
 d.publish_time, (d.draft_content IS NOT NULL OR d.draft_title IS NOT NULL)`

func scanDocumentList(scan func(dest ...any) error) (*biz.Document, error) {
	var d biz.Document
	var status, review, index int16
	if err := scan(&d.ID, &d.SpaceID, &d.Slug, &d.Title, &status, &review, &d.ReviewNote, &d.Summary, &d.AISummary, &d.Tags, &d.Source,
		&d.SourceRef, &d.OwnerID, &d.ReviewAt, &d.ReviewNotifyTime, &d.Version, &index, &d.CreateUser, &d.UpdateUser, &d.CreateTime, &d.UpdateTime,
		&d.PublishTime, &d.Draft); err != nil {
		return nil, err
	}
	d.Status, d.ReviewState, d.IndexStatus = int32(status), int32(review), int32(index)
	if d.Tags == nil {
		d.Tags = []string{}
	}
	return &d, nil
}

// Create implements biz.DocumentRepo.
func (r *documentRepo) Create(ctx context.Context, d *biz.Document) error {
	_, err := execFrom(ctx, r.data).Exec(ctx,
		`INSERT INTO knowledge_document (id, space_id, slug, title, status, review_state, review_note, content, draft_content, draft_title, summary,
		 ai_summary, tags, source, source_ref, owner_id, review_at, version, index_status, create_user, update_user, create_time, update_time, publish_time)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
		d.ID, d.SpaceID, d.Slug, d.Title, d.Status, d.ReviewState, d.ReviewNote, d.Content, nullStr(d.DraftContent), nullStr(d.DraftTitle), d.Summary,
		d.AISummary, dbx.TextArray(d.Tags), d.Source, d.SourceRef, d.OwnerID, nullTime(d.ReviewAt), d.Version, d.IndexStatus, d.CreateUser, d.UpdateUser,
		d.CreateTime, d.UpdateTime, nullTime(d.PublishTime))
	if isUniqueViolation(err, "uq_knowledge_document_slug") {
		return biz.ErrSlugExists
	}
	if err != nil {
		return wrap("insert knowledge document", err)
	}
	return nil
}

// Update implements biz.DocumentRepo：乐观锁，只命中 version 仍为 expectedVersion 的行；标签、索引状态与复审提醒时间
// 由 SetTags / SetIndexStatus / MarkReviewNotified · ResetReviewNotified 专用写入，这里不触碰，避免用过期快照覆盖后台任务
// （AI 标签、向量化、复审巡检）的结果。
func (r *documentRepo) Update(ctx context.Context, d *biz.Document, expectedVersion int32) error {
	tag, err := execFrom(ctx, r.data).Exec(ctx,
		`UPDATE knowledge_document SET slug=$2, title=$3, status=$4, review_state=$5, review_note=$6, content=$7, draft_content=$8, draft_title=$9,
		 summary=$10, owner_id=$11, review_at=$12, version=$13, update_user=$14, update_time=$15, publish_time=$16
		 WHERE id=$1 AND delete_time IS NULL AND version=$17`,
		d.ID, d.Slug, d.Title, d.Status, d.ReviewState, d.ReviewNote, d.Content, nullStr(d.DraftContent), nullStr(d.DraftTitle), d.Summary,
		d.OwnerID, nullTime(d.ReviewAt), d.Version, d.UpdateUser, d.UpdateTime, nullTime(d.PublishTime), expectedVersion)
	if isUniqueViolation(err, "uq_knowledge_document_slug") {
		return biz.ErrSlugExists
	}
	if err != nil {
		return wrap("update knowledge document", err)
	}
	if tag.RowsAffected() == 0 {
		// 行不存在（已删）或版本已变：先区分，前者按不存在、后者按并发冲突。
		var exists bool
		if err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM knowledge_document WHERE id=$1 AND delete_time IS NULL)`, d.ID).Scan(&exists); err != nil {
			return wrap("update knowledge document", err)
		}
		if exists {
			return biz.ErrDocumentConflict
		}
		return biz.ErrDocumentNotFound
	}
	return nil
}

// SetTags implements biz.DocumentRepo.
func (r *documentRepo) SetTags(ctx context.Context, id string, tags []string) error {
	tag, err := execFrom(ctx, r.data).Exec(ctx, `UPDATE knowledge_document SET tags=$2 WHERE id=$1 AND delete_time IS NULL`, id, dbx.TextArray(tags))
	if err != nil {
		return wrap("set knowledge document tags", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrDocumentNotFound
	}
	return nil
}

// Get implements biz.DocumentRepo（含正文与草稿）。
func (r *documentRepo) Get(ctx context.Context, id string) (*biz.Document, error) {
	row := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT `+docListColumns+`, d.content, d.draft_content, d.draft_title FROM knowledge_document d WHERE d.id=$1 AND d.delete_time IS NULL`, id)
	var d biz.Document
	var status, review, index int16
	var draftContent, draftTitle *string
	err := row.Scan(&d.ID, &d.SpaceID, &d.Slug, &d.Title, &status, &review, &d.ReviewNote, &d.Summary, &d.AISummary, &d.Tags, &d.Source,
		&d.SourceRef, &d.OwnerID, &d.ReviewAt, &d.ReviewNotifyTime, &d.Version, &index, &d.CreateUser, &d.UpdateUser, &d.CreateTime, &d.UpdateTime,
		&d.PublishTime, &d.Draft, &d.Content, &draftContent, &draftTitle)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrDocumentNotFound
	}
	if err != nil {
		return nil, wrap("get knowledge document", err)
	}
	d.Status, d.ReviewState, d.IndexStatus = int32(status), int32(review), int32(index)
	// 详情带草稿正文本身：有无草稿以指针为准，列表标记在此不生效（否则发布清掉草稿后标记仍为真）。
	d.DraftContent, d.DraftTitle, d.Draft = draftContent, draftTitle, false
	if d.Tags == nil {
		d.Tags = []string{}
	}
	return &d, nil
}

// documentWhere 组装列表过滤条件。
func documentWhere(a *sqlArgs, f biz.DocumentFilter, now time.Time) string {
	where := "d.delete_time IS NULL"
	if !f.Unrestricted {
		where += " AND d.space_id = ANY(" + a.add(dbx.TextArray(f.SpaceIDs)) + "::uuid[])"
	}
	if f.SpaceID != "" {
		where += " AND d.space_id = " + a.add(f.SpaceID)
	}
	if f.Status != nil {
		where += " AND d.status = " + a.add(*f.Status)
	}
	if f.Keyword != "" {
		where += " AND d.title ILIKE " + a.add(dbx.LikeContains(f.Keyword))
	}
	if f.Tag != "" {
		where += " AND " + a.add(f.Tag) + " = ANY(d.tags)"
	}
	if f.OwnerID != "" {
		where += " AND d.owner_id = " + a.add(f.OwnerID)
	}
	if f.ReviewPending {
		where += " AND d.review_state = 1"
	}
	if f.ReviewOverdue {
		where += " AND d.status = 2 AND d.review_at IS NOT NULL AND d.review_at < " + a.add(now)
	}
	if f.ReviewDueBefore != nil {
		where += " AND d.status = 2 AND d.review_at IS NOT NULL AND d.review_at <= " + a.add(*f.ReviewDueBefore)
	}
	if f.ReviewNoticePending {
		where += " AND (d.review_notify_time IS NULL OR (d.review_at <= " + a.add(now) + " AND d.review_notify_time < d.review_at))"
	}
	if f.HideDrafts {
		where += " AND (d.status <> 1 OR d.owner_id = " + a.add(f.DraftOwnerID) + " OR d.space_id = ANY(" + a.add(dbx.TextArray(f.DraftSpaceIDs)) + "::uuid[]))"
	}
	return where
}

// List implements biz.DocumentRepo.
func (r *documentRepo) List(ctx context.Context, f biz.DocumentFilter, page, pageSize int64) ([]*biz.Document, int64, error) {
	var a sqlArgs
	where := documentWhere(&a, f, time.Now())
	var total int64
	if err := r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM knowledge_document d WHERE `+where, a.args...).Scan(&total); err != nil {
		return nil, 0, wrap("count knowledge documents", err)
	}
	order := "d.update_time DESC, d.id"
	if f.OrderByPublish {
		order = "d.publish_time DESC NULLS LAST, d.id"
	}
	if f.ReviewDueBefore != nil {
		order = "d.review_at ASC, d.id"
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, `SELECT `+docListColumns+` FROM knowledge_document d WHERE `+where+` ORDER BY `+order+` LIMIT `+limit+` OFFSET `+offset, a.args...)
	if err != nil {
		return nil, 0, wrap("list knowledge documents", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Document, error) { return scanDocumentList(row.Scan) })
	if err != nil {
		return nil, 0, wrap("scan knowledge document", err)
	}
	return items, total, nil
}

// SoftDelete implements biz.DocumentRepo.
func (r *documentRepo) SoftDelete(ctx context.Context, id string, at time.Time) error {
	tag, err := execFrom(ctx, r.data).Exec(ctx, `UPDATE knowledge_document SET delete_time=$2 WHERE id=$1 AND delete_time IS NULL`, id, at)
	if err != nil {
		return wrap("delete knowledge document", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrDocumentNotFound
	}
	return nil
}

// MarkReviewNotified implements biz.DocumentRepo.
func (r *documentRepo) MarkReviewNotified(ctx context.Context, id string, at time.Time) error {
	if _, err := r.data.Pool.Exec(ctx, `UPDATE knowledge_document SET review_notify_time=$2 WHERE id=$1`, id, at); err != nil {
		return wrap("mark knowledge review notified", err)
	}
	return nil
}

// ResetReviewNotified implements biz.DocumentRepo.
func (r *documentRepo) ResetReviewNotified(ctx context.Context, id string) error {
	if _, err := execFrom(ctx, r.data).Exec(ctx, `UPDATE knowledge_document SET review_notify_time=NULL WHERE id=$1`, id); err != nil {
		return wrap("reset knowledge review notified", err)
	}
	return nil
}

// SetAISummary implements biz.DocumentRepo：标签只在现有标签为空时写入。
func (r *documentRepo) SetAISummary(ctx context.Context, id, summary string, tags []string) error {
	_, err := r.data.Pool.Exec(ctx,
		`UPDATE knowledge_document SET ai_summary=$2, tags = CASE WHEN cardinality(tags) = 0 AND cardinality($3::text[]) > 0 THEN $3::text[] ELSE tags END
		 WHERE id=$1 AND delete_time IS NULL`, id, summary, dbx.TextArray(tags))
	if err != nil {
		return wrap("set knowledge ai summary", err)
	}
	return nil
}

// SetIndexStatus implements biz.DocumentRepo.
func (r *documentRepo) SetIndexStatus(ctx context.Context, id string, status int32) error {
	if _, err := execFrom(ctx, r.data).Exec(ctx, `UPDATE knowledge_document SET index_status=$2 WHERE id=$1 AND delete_time IS NULL`, id, status); err != nil {
		return wrap("set knowledge index status", err)
	}
	return nil
}

// SetIndexStatusIfVersion implements biz.DocumentRepo：只命中仍是已发布且版本未变的行。
func (r *documentRepo) SetIndexStatusIfVersion(ctx context.Context, id string, status, version int32) error {
	if _, err := r.data.Pool.Exec(ctx, `UPDATE knowledge_document SET index_status=$2 WHERE id=$1 AND delete_time IS NULL AND status=2 AND version=$3`, id, status, version); err != nil {
		return wrap("set knowledge index status", err)
	}
	return nil
}

// PublishedIDs implements biz.DocumentRepo.
func (r *documentRepo) PublishedIDs(ctx context.Context, spaceID string) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx, `SELECT id FROM knowledge_document WHERE space_id=$1 AND status=2 AND delete_time IS NULL ORDER BY id`, spaceID)
	if err != nil {
		return nil, wrap("list published knowledge documents", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, wrap("scan knowledge document id", err)
	}
	return ids, nil
}

// CountVisible implements biz.DocumentRepo.
func (r *documentRepo) CountVisible(ctx context.Context, unrestricted bool, spaceIDs []string) (int64, error) {
	var a sqlArgs
	where := "delete_time IS NULL AND status = 2"
	if !unrestricted {
		where += " AND space_id = ANY(" + a.add(dbx.TextArray(spaceIDs)) + "::uuid[])"
	}
	var n int64
	if err := r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM knowledge_document WHERE `+where, a.args...).Scan(&n); err != nil {
		return 0, wrap("count visible knowledge documents", err)
	}
	return n, nil
}

// Stats implements biz.DocumentRepo.
func (r *documentRepo) Stats(ctx context.Context, now time.Time) (biz.DocumentStats, error) {
	var s biz.DocumentStats
	err := r.data.Pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM knowledge_space WHERE delete_time IS NULL),
		        count(*) FILTER (WHERE status = 2), count(*) FILTER (WHERE status = 1),
		        count(*) FILTER (WHERE status = 2 AND review_at IS NOT NULL AND review_at < $1),
		        count(*) FILTER (WHERE review_state = 1)
		 FROM knowledge_document WHERE delete_time IS NULL`, now).
		Scan(&s.SpaceCount, &s.PublishedCount, &s.DraftCount, &s.ReviewOverdueCount, &s.PendingReviewCount)
	if err != nil {
		return s, wrap("knowledge document stats", err)
	}
	return s, nil
}

// PurgeDeleted implements biz.DocumentRepo：物理删除软删超期的文档及其版本、关联（分块已在软删时删除）。
func (r *documentRepo) PurgeDeleted(ctx context.Context, before time.Time, limit int) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id FROM knowledge_document WHERE delete_time IS NOT NULL AND delete_time < $1 ORDER BY delete_time LIMIT $2 FOR UPDATE SKIP LOCKED`, before, limit)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, stmt := range []string{
			`DELETE FROM knowledge_version WHERE document_id = ANY($1::uuid[])`,
			`DELETE FROM knowledge_link WHERE document_id = ANY($1::uuid[])`,
			`DELETE FROM knowledge_chunk WHERE document_id = ANY($1::uuid[])`,
			`DELETE FROM knowledge_index_job WHERE document_id = ANY($1::uuid[])`,
			`DELETE FROM knowledge_document WHERE id = ANY($1::uuid[])`,
		} {
			if _, err := tx.Exec(ctx, stmt, ids); err != nil {
				return err
			}
		}
		n = len(ids)
		return nil
	})
	if err != nil {
		return 0, wrap("purge knowledge documents", err)
	}
	return n, nil
}

// versionRepo 版本仓储。
type versionRepo struct {
	data *basedata.Data
}

// NewVersionRepo new a versionRepo.
func NewVersionRepo(data *basedata.Data) *versionRepo {
	return &versionRepo{data: data}
}

// CreateTx implements biz.VersionRepo：同一文档同一版本号重复落库即并发发布，按文档冲突返回。
func (r *versionRepo) CreateTx(ctx context.Context, tx txrun.Tx, v *biz.Version) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO knowledge_version (id, document_id, version, title, content, note, publish_user, create_time) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		v.ID, v.DocumentID, v.Version, v.Title, v.Content, v.Note, v.PublishUser, v.CreateTime)
	if isUniqueViolation(err, "uq_knowledge_version") {
		return biz.ErrDocumentConflict
	}
	if err != nil {
		return wrap("insert knowledge version", err)
	}
	return nil
}

// List implements biz.VersionRepo.
func (r *versionRepo) List(ctx context.Context, documentID string) ([]*biz.Version, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, document_id, version, title, note, publish_user, create_time FROM knowledge_version WHERE document_id=$1 ORDER BY version DESC`, documentID)
	if err != nil {
		return nil, wrap("list knowledge versions", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Version, error) {
		var v biz.Version
		return &v, row.Scan(&v.ID, &v.DocumentID, &v.Version, &v.Title, &v.Note, &v.PublishUser, &v.CreateTime)
	})
	if err != nil {
		return nil, wrap("scan knowledge version", err)
	}
	return items, nil
}

// Get implements biz.VersionRepo.
func (r *versionRepo) Get(ctx context.Context, documentID string, version int32) (*biz.Version, error) {
	var v biz.Version
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, document_id, version, title, content, note, publish_user, create_time FROM knowledge_version WHERE document_id=$1 AND version=$2`,
		documentID, version).Scan(&v.ID, &v.DocumentID, &v.Version, &v.Title, &v.Content, &v.Note, &v.PublishUser, &v.CreateTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrVersionNotFound
	}
	if err != nil {
		return nil, wrap("get knowledge version", err)
	}
	return &v, nil
}
