package data

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.ChunkRepo = (*chunkRepo)(nil)

// chunkRepo 分块仓储：全文（tsvector）与向量（pgvector，可选）检索。
type chunkRepo struct {
	data   *basedata.Data
	vector bool
}

// NewChunkRepo new a chunkRepo；启动期探测向量列。
func NewChunkRepo(data *basedata.Data, logger *slog.Logger) *chunkRepo {
	return &chunkRepo{data: data, vector: probeVectorColumn(data, logger)}
}

// VectorAvailable implements biz.ChunkRepo.
func (r *chunkRepo) VectorAvailable() bool { return r.vector }

// errTxUnsupported 事务句柄不是本 data 包签发的（测试桩）。
var errTxUnsupported = errors.New("knowledge: transaction handle does not expose pgx.Tx")

// ReplaceTx implements biz.ChunkRepo：删旧（带回内容摘要 → 向量）→ 插新（摘要相同的复用向量）。
func (r *chunkRepo) ReplaceTx(ctx context.Context, tx txrun.Tx, documentID string, chunks []*biz.Chunk) error {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return errTxUnsupported
	}
	type reuse struct {
		model string
		vec   string
	}
	reusable := map[string]reuse{}
	if r.vector {
		rows, err := ptx.Query(ctx,
			`DELETE FROM knowledge_chunk WHERE document_id=$1 RETURNING content_sha256, embedding_model, embedding::text`, documentID)
		if err != nil {
			return wrap("delete knowledge chunks", err)
		}
		for rows.Next() {
			var sha string
			var model, vec *string
			if err := rows.Scan(&sha, &model, &vec); err != nil {
				rows.Close()
				return wrap("scan knowledge chunk", err)
			}
			if model != nil && vec != nil {
				reusable[sha] = reuse{model: *model, vec: *vec}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return wrap("delete knowledge chunks", err)
		}
	} else if _, err := ptx.Exec(ctx, `DELETE FROM knowledge_chunk WHERE document_id=$1`, documentID); err != nil {
		return wrap("delete knowledge chunks", err)
	}
	if len(chunks) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, c := range chunks {
		if ru, ok := reusable[c.SHA256]; ok && r.vector {
			b.Queue(`INSERT INTO knowledge_chunk (id, document_id, space_id, version, seq, heading_path, content, tsv, content_sha256, embedding_model, token_count, embedding)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,to_tsvector('simple',$8),$9,$10,$11,$12::halfvec)`,
				c.ID, c.DocumentID, c.SpaceID, c.Version, c.Seq, c.HeadingPath, c.Content, c.Tokens, c.SHA256, ru.model, c.TokenCount, ru.vec)
			continue
		}
		b.Queue(`INSERT INTO knowledge_chunk (id, document_id, space_id, version, seq, heading_path, content, tsv, content_sha256, token_count)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,to_tsvector('simple',$8),$9,$10)`,
			c.ID, c.DocumentID, c.SpaceID, c.Version, c.Seq, c.HeadingPath, c.Content, c.Tokens, c.SHA256, c.TokenCount)
	}
	if err := ptx.SendBatch(ctx, b).Close(); err != nil {
		return wrap("insert knowledge chunks", err)
	}
	return nil
}

// DeleteTx implements biz.ChunkRepo.
func (r *chunkRepo) DeleteTx(ctx context.Context, tx txrun.Tx, documentID string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM knowledge_chunk WHERE document_id=$1`, documentID); err != nil {
		return wrap("delete knowledge chunks", err)
	}
	return nil
}

// ListUnembedded implements biz.ChunkRepo.
func (r *chunkRepo) ListUnembedded(ctx context.Context, documentID, model string) ([]*biz.Chunk, error) {
	if !r.vector {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, seq, heading_path, content FROM knowledge_chunk WHERE document_id=$1 AND (embedding IS NULL OR embedding_model IS DISTINCT FROM $2) ORDER BY seq`,
		documentID, model)
	if err != nil {
		return nil, wrap("list unembedded knowledge chunks", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Chunk, error) {
		c := biz.Chunk{DocumentID: documentID}
		return &c, row.Scan(&c.ID, &c.Seq, &c.HeadingPath, &c.Content)
	})
	if err != nil {
		return nil, wrap("scan knowledge chunk", err)
	}
	return items, nil
}

// SetEmbeddings implements biz.ChunkRepo.
func (r *chunkRepo) SetEmbeddings(ctx context.Context, items []biz.EmbeddingUpdate) error {
	if !r.vector || len(items) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, it := range items {
		b.Queue(`UPDATE knowledge_chunk SET embedding=$2::halfvec, embedding_model=$3 WHERE id=$1`, it.ChunkID, vectorLiteral(it.Vector), it.Model)
	}
	if err := r.data.Pool.SendBatch(ctx, b).Close(); err != nil {
		return wrap("set knowledge embeddings", err)
	}
	return nil
}

// hitColumns 召回列：分块 + 文档标题 / 复审时间 + 空间名。
const hitColumns = `c.id, c.document_id, c.space_id, c.seq, c.heading_path, c.content, d.title, s.name, d.review_at`

// hitFrom 召回连接：只取已发布、未删除文档，未删除空间。
const hitFrom = ` FROM knowledge_chunk c
 JOIN knowledge_document d ON d.id = c.document_id AND d.delete_time IS NULL AND d.status = 2
 JOIN knowledge_space s ON s.id = c.space_id AND s.delete_time IS NULL`

// scopeWhere 空间 / 标签过滤。
func scopeWhere(a *sqlArgs, unrestricted bool, spaceIDs, tags []string) string {
	where := ""
	if !unrestricted {
		where += " AND c.space_id = ANY(" + a.add(dbx.TextArray(spaceIDs)) + "::uuid[])"
	}
	if len(tags) > 0 {
		where += " AND d.tags && " + a.add(dbx.TextArray(tags)) + "::text[]"
	}
	return where
}

func scanHit(row pgx.CollectableRow) (*biz.ChunkHit, error) {
	var h biz.ChunkHit
	return &h, row.Scan(&h.ChunkID, &h.DocumentID, &h.SpaceID, &h.Seq, &h.HeadingPath, &h.Content, &h.Title, &h.SpaceName, &h.ReviewAt, &h.Score)
}

// FullText implements biz.ChunkRepo.
func (r *chunkRepo) FullText(ctx context.Context, tsquery string, unrestricted bool, spaceIDs, tags []string, limit int) ([]*biz.ChunkHit, error) {
	var a sqlArgs
	q := a.add(tsquery)
	where := "c.tsv @@ q" + scopeWhere(&a, unrestricted, spaceIDs, tags)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT `+hitColumns+`, ts_rank_cd(c.tsv, q) AS score`+hitFrom+`, to_tsquery('simple', `+q+`) q WHERE `+where+
			` ORDER BY score DESC, c.id LIMIT `+a.add(limit), a.args...)
	if err != nil {
		return nil, wrap("full-text search knowledge", err)
	}
	items, err := pgx.CollectRows(rows, scanHit)
	if err != nil {
		return nil, wrap("scan knowledge hit", err)
	}
	return items, nil
}

// Vector implements biz.ChunkRepo：余弦距离，HNSW ef_search 临时调高以兼顾过滤后的召回。
func (r *chunkRepo) Vector(ctx context.Context, vector []float32, unrestricted bool, spaceIDs, tags []string, limit int) ([]*biz.ChunkHit, error) {
	if !r.vector {
		return nil, nil
	}
	var a sqlArgs
	v := a.add(vectorLiteral(vector))
	where := "c.embedding IS NOT NULL" + scopeWhere(&a, unrestricted, spaceIDs, tags)
	var items []*biz.ChunkHit
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('hnsw.ef_search', '100', true)`); err != nil {
			return err
		}
		// ORDER BY 只保留距离表达式：追加 c.id 作并列 tie-break 会让规划器放弃 HNSW 索引改为全表扫描 + 排序
		//（索引只能提供按距离的顺序）。近似检索本就不保证并列项的稳定顺序，由上层融合排序兜底。
		rows, err := tx.Query(ctx,
			`SELECT `+hitColumns+`, 1 - (c.embedding <=> `+v+`::halfvec) AS score`+hitFrom+` WHERE `+where+
				` ORDER BY c.embedding <=> `+v+`::halfvec LIMIT `+a.add(limit), a.args...)
		if err != nil {
			return err
		}
		items, err = pgx.CollectRows(rows, scanHit)
		return err
	})
	if err != nil {
		return nil, wrap("vector search knowledge", err)
	}
	return items, nil
}

// Titles implements biz.ChunkRepo：标题包含或 trgm 相似。
func (r *chunkRepo) Titles(ctx context.Context, query string, unrestricted bool, spaceIDs, tags []string, limit int) ([]*biz.DocHit, error) {
	var a sqlArgs
	q := a.add(query)
	like := a.add(dbx.LikeContains(query))
	where := "d.delete_time IS NULL AND d.status = 2 AND s.delete_time IS NULL AND (d.title ILIKE " + like + " OR d.title % " + q + ")"
	if !unrestricted {
		where += " AND d.space_id = ANY(" + a.add(dbx.TextArray(spaceIDs)) + "::uuid[])"
	}
	if len(tags) > 0 {
		where += " AND d.tags && " + a.add(dbx.TextArray(tags)) + "::text[]"
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT d.id, GREATEST(similarity(d.title, `+q+`), CASE WHEN d.title ILIKE `+like+` THEN 1 ELSE 0 END) AS score
		 FROM knowledge_document d JOIN knowledge_space s ON s.id = d.space_id WHERE `+where+` ORDER BY score DESC, d.id LIMIT `+a.add(limit), a.args...)
	if err != nil {
		return nil, wrap("title search knowledge", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.DocHit, error) {
		var h biz.DocHit
		return &h, row.Scan(&h.DocumentID, &h.Score)
	})
	if err != nil {
		return nil, wrap("scan knowledge title hit", err)
	}
	return items, nil
}

// Sections implements biz.ChunkRepo.
func (r *chunkRepo) Sections(ctx context.Context, keys []biz.SectionKey, maxChars int) (map[biz.SectionKey]string, error) {
	out := make(map[biz.SectionKey]string, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	want := make(map[biz.SectionKey]struct{}, len(keys))
	ids := make([]string, 0, len(keys))
	for _, k := range keys {
		want[k] = struct{}{}
		ids = append(ids, k.DocumentID)
	}
	rows, err := r.data.Pool.Query(ctx, `SELECT document_id, heading_path, content FROM knowledge_chunk WHERE document_id = ANY($1::uuid[]) ORDER BY document_id, seq`, dbx.TextArray(ids))
	if err != nil {
		return nil, wrap("load knowledge sections", err)
	}
	defer rows.Close()
	parts := map[biz.SectionKey][]string{}
	for rows.Next() {
		var k biz.SectionKey
		var content string
		if err := rows.Scan(&k.DocumentID, &k.HeadingPath, &content); err != nil {
			return nil, wrap("scan knowledge section", err)
		}
		if _, ok := want[k]; ok {
			parts[k] = append(parts[k], content)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("load knowledge sections", err)
	}
	for k, ps := range parts {
		text := strings.Join(ps, "\n\n")
		if rs := []rune(text); len(rs) > maxChars {
			text = string(rs[:maxChars]) + "…"
		}
		out[k] = text
	}
	return out, nil
}

// Counts implements biz.ChunkRepo.
func (r *chunkRepo) Counts(ctx context.Context) (total, embedded int64, err error) {
	if r.vector {
		err = r.data.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE embedding IS NOT NULL) FROM knowledge_chunk`).Scan(&total, &embedded)
	} else {
		err = r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM knowledge_chunk`).Scan(&total)
	}
	if err != nil {
		return 0, 0, wrap("count knowledge chunks", err)
	}
	return total, embedded, nil
}
