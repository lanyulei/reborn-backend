//go:build integration

package data

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/zhtext"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	itOwner  = "018f3e1a-00a0-7000-8000-000000000001"
	itReader = "018f3e1a-00a0-7000-8000-000000000002"
	itRole   = "018f3e1a-00a0-7000-8000-00000000000a"
)

type itRepos struct {
	data    *basedata.Data
	txm     *basedata.TxManager
	spaces  *spaceRepo
	docs    *documentRepo
	vers    *versionRepo
	chunks  *chunkRepo
	links   *linkRepo
	ingest  *ingestRepo
	index   *indexRepo
	logs    *searchLogRepo
	cleanup func()
}

// newIT 构造真实连接池并清空知识库表；库名须含 "test"。
func newIT(t *testing.T) *itRepos {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	if cfg, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	} else if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}, Upload: &conf.Data_Upload{Dir: t.TempDir()}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE knowledge_search_log, knowledge_index_job, knowledge_ingest_job, knowledge_link, knowledge_chunk,
		knowledge_version, knowledge_document, knowledge_space_acl, knowledge_space`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return &itRepos{
		data: d, txm: basedata.NewTxManager(d), spaces: NewSpaceRepo(d), docs: NewDocumentRepo(d), vers: NewVersionRepo(d),
		chunks: NewChunkRepo(d, slog.New(slog.NewTextHandler(io.Discard, nil))), links: NewLinkRepo(d), ingest: NewIngestRepo(d), index: NewIndexRepo(d), logs: NewSearchLogRepo(d),
	}
}

func mustID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (r *itRepos) newSpace(t *testing.T, code string, visibility int32) *biz.Space {
	t.Helper()
	now := time.Now()
	s := &biz.Space{
		ID: mustID(t), Code: code, Name: "空间 " + code, Type: "general", Visibility: visibility, OwnerID: itOwner, EmbeddingEnabled: true,
		CreateUser: itOwner, CreateTime: now, UpdateTime: now,
	}
	if err := r.spaces.Create(context.Background(), s); err != nil {
		t.Fatalf("create space: %v", err)
	}
	return s
}

func (r *itRepos) newDoc(t *testing.T, space *biz.Space, slug, title, content string) *biz.Document {
	t.Helper()
	now := time.Now()
	d := &biz.Document{
		ID: mustID(t), SpaceID: space.ID, Slug: slug, Title: title, Status: biz.DocStatusDraft, Content: content, Tags: []string{"nginx"},
		Source: biz.SourceManual, OwnerID: itOwner, CreateUser: itOwner, UpdateUser: itOwner, CreateTime: now, UpdateTime: now,
	}
	if err := r.docs.Create(context.Background(), d); err != nil {
		t.Fatalf("create document: %v", err)
	}
	return d
}

func TestSpaceRepo(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	pub := r.newSpace(t, "pub", biz.VisibilityPublic)
	priv := r.newSpace(t, "priv", biz.VisibilityRestricted)
	if err := r.spaces.Create(ctx, &biz.Space{ID: mustID(t), Code: "pub", Name: "dup", Type: "general", Visibility: 1, OwnerID: itOwner, CreateUser: itOwner}); !errors.Is(err, biz.ErrSpaceCodeExists) {
		t.Fatalf("duplicate code error = %v", err)
	}
	if err := r.spaces.ReplaceAcl(ctx, priv.ID, []*biz.AclEntry{
		{SubjectType: biz.SubjectUser, SubjectID: itReader, Level: biz.LevelRead},
		{SubjectType: biz.SubjectRole, SubjectID: itRole, Level: biz.LevelWrite},
	}, itOwner); err != nil {
		t.Fatalf("replace acl: %v", err)
	}
	acl, err := r.spaces.ListAcl(ctx, priv.ID)
	if err != nil || len(acl) != 2 {
		t.Fatalf("list acl = %v, %v", acl, err)
	}
	// 负责人：两空间皆管理。
	lv, err := r.spaces.Levels(ctx, itOwner, nil)
	if err != nil || lv[pub.ID] != biz.LevelManage || lv[priv.ID] != biz.LevelManage {
		t.Fatalf("owner levels = %v, %v", lv, err)
	}
	// 普通用户：公开空间读；受限空间经用户条目读，经角色条目写（取最大）。
	lv, err = r.spaces.Levels(ctx, itReader, nil)
	if err != nil || lv[pub.ID] != biz.LevelRead || lv[priv.ID] != biz.LevelRead {
		t.Fatalf("reader levels = %v, %v", lv, err)
	}
	lv, err = r.spaces.Levels(ctx, itReader, []string{itRole})
	if err != nil || lv[priv.ID] != biz.LevelWrite {
		t.Fatalf("reader+role levels = %v, %v", lv, err)
	}
	// 陌生人：只有公开空间。
	lv, err = r.spaces.Levels(ctx, mustID(t), nil)
	if err != nil || len(lv) != 1 || lv[pub.ID] != biz.LevelRead {
		t.Fatalf("stranger levels = %v, %v", lv, err)
	}
	// 有文档的空间不能删。
	r.newDoc(t, pub, "a", "A", "body")
	if err := r.spaces.SoftDelete(ctx, pub.ID, time.Now()); !errors.Is(err, biz.ErrSpaceNotEmpty) {
		t.Fatalf("delete non-empty = %v", err)
	}
	if err := r.spaces.SoftDelete(ctx, priv.ID, time.Now()); err != nil {
		t.Fatalf("delete empty = %v", err)
	}
	if _, err := r.spaces.Get(ctx, priv.ID); !errors.Is(err, biz.ErrSpaceNotFound) {
		t.Fatalf("get deleted = %v", err)
	}
	counts, err := r.spaces.DocumentCounts(ctx)
	if err != nil || counts[pub.ID] != 0 { // 草稿不计
		t.Fatalf("counts = %v, %v", counts, err)
	}
}

func TestDocumentChunkSearchFlow(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	space := r.newSpace(t, "ops", biz.VisibilityPublic)
	content := "# Nginx 排障\n\n## 502\n\n当 nginx 返回 502 时，先检查上游连接池是否耗尽。\n\n## 504\n\n504 多为上游超时。\n"
	doc := r.newDoc(t, space, "nginx", "Nginx 排障手册", content)
	if err := r.docs.Create(ctx, &biz.Document{ID: mustID(t), SpaceID: space.ID, Slug: "nginx", Title: "dup", Status: 1, OwnerID: itOwner, CreateUser: itOwner, UpdateUser: itOwner, Tags: []string{}}); !errors.Is(err, biz.ErrSlugExists) {
		t.Fatalf("duplicate slug error = %v", err)
	}

	// 发布：版本 + 文档 + 分块 + 索引任务同事务。
	chunks := biz.Split(doc.Title, content)
	for _, c := range chunks {
		c.ID, c.DocumentID, c.SpaceID, c.Version = mustID(t), doc.ID, space.ID, 1
	}
	now := time.Now()
	err := r.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := r.vers.CreateTx(ctx, tx, &biz.Version{ID: mustID(t), DocumentID: doc.ID, Version: 1, Title: doc.Title, Content: content, PublishUser: itOwner, CreateTime: now}); err != nil {
			return err
		}
		doc.Status, doc.Version, doc.PublishTime, doc.IndexStatus = biz.DocStatusPublished, 1, &now, biz.IndexPending
		if err := r.docs.Update(txrun.NewContext(ctx, tx), doc, 0); err != nil {
			return err
		}
		if err := r.docs.SetIndexStatus(txrun.NewContext(ctx, tx), doc.ID, biz.IndexPending); err != nil {
			return err
		}
		if err := r.chunks.ReplaceTx(ctx, tx, doc.ID, chunks); err != nil {
			return err
		}
		return r.index.UpsertTx(ctx, tx, doc.ID, 1)
	})
	if err != nil {
		t.Fatalf("publish tx: %v", err)
	}
	got, err := r.docs.Get(ctx, doc.ID)
	if err != nil || got.Status != biz.DocStatusPublished || got.Version != 1 || got.Content != content || got.HasDraft() || got.IndexStatus != biz.IndexPending {
		t.Fatalf("get after publish = %+v, %v", got, err)
	}
	// 乐观锁：带过期版本号的更新不命中，返回冲突；版本号重复落库同样按冲突。
	if err := r.docs.Update(ctx, got, 0); !errors.Is(err, biz.ErrDocumentConflict) {
		t.Fatalf("stale update error = %v, want ErrDocumentConflict", err)
	}
	err = r.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		return r.vers.CreateTx(ctx, tx, &biz.Version{ID: mustID(t), DocumentID: doc.ID, Version: 1, Title: doc.Title, Content: content, PublishUser: itOwner, CreateTime: now})
	})
	if !errors.Is(err, biz.ErrDocumentConflict) {
		t.Fatalf("duplicate version error = %v, want ErrDocumentConflict", err)
	}
	// 列表（不含正文）带草稿标记。
	draft := "改动"
	got.DraftContent = &draft
	if err := r.docs.Update(ctx, got, got.Version); err != nil {
		t.Fatal(err)
	}
	items, total, err := r.docs.List(ctx, biz.DocumentFilter{Unrestricted: true, Tag: "nginx"}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 || !items[0].HasDraft() || items[0].Content != "" {
		t.Fatalf("list = %+v total=%d err=%v", items, total, err)
	}
	items, _, err = r.docs.List(ctx, biz.DocumentFilter{SpaceIDs: []string{mustID(t)}}, 1, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("list outside scope = %v, %v", items, err)
	}

	// 全文召回：查询与索引同一分词，二元组命中；范围过滤生效。
	tsq := zhtext.Query("连接池耗尽 502")
	hits, err := r.chunks.FullText(ctx, tsq, false, []string{space.ID}, nil, 10)
	if err != nil || len(hits) == 0 {
		t.Fatalf("fulltext = %v, %v", hits, err)
	}
	if hits[0].Title != doc.Title || hits[0].SpaceName != space.Name || !strings.Contains(hits[0].Content, "连接池") {
		t.Fatalf("top hit = %+v", hits[0])
	}
	if hits, err = r.chunks.FullText(ctx, tsq, false, []string{mustID(t)}, nil, 10); err != nil || len(hits) != 0 {
		t.Fatalf("fulltext outside scope = %v, %v", hits, err)
	}
	if hits, err = r.chunks.FullText(ctx, tsq, true, nil, []string{"other"}, 10); err != nil || len(hits) != 0 {
		t.Fatalf("fulltext tag filter = %v, %v", hits, err)
	}
	titles, err := r.chunks.Titles(ctx, "nginx", true, nil, nil, 10)
	if err != nil || len(titles) != 1 || titles[0].DocumentID != doc.ID {
		t.Fatalf("titles = %v, %v", titles, err)
	}
	sections, err := r.chunks.Sections(ctx, []biz.SectionKey{{DocumentID: doc.ID, HeadingPath: "Nginx 排障 > 502"}}, 2000)
	if err != nil || !strings.Contains(sections[biz.SectionKey{DocumentID: doc.ID, HeadingPath: "Nginx 排障 > 502"}], "连接池") {
		t.Fatalf("sections = %v, %v", sections, err)
	}
	wantEmbedded := int64(0)
	if r.chunks.VectorAvailable() {
		pending, err := r.chunks.ListUnembedded(ctx, doc.ID, "m")
		if err != nil || len(pending) != len(chunks) {
			t.Fatalf("unembedded = %d, %v", len(pending), err)
		}
		// 写入单位向量：第 i 块的向量只在第 i 维为 1，查询向量指向「连接池」所在块 → 它必须排第一。
		target := -1
		updates := make([]biz.EmbeddingUpdate, 0, len(pending))
		for i, c := range pending {
			vec := make([]float32, biz.EmbeddingDimensions)
			vec[i] = 1
			if strings.Contains(c.Content, "连接池") {
				target = i
			}
			updates = append(updates, biz.EmbeddingUpdate{ChunkID: c.ID, Model: "m", Vector: vec})
		}
		if target < 0 {
			t.Fatal("no chunk contains the target text")
		}
		if err := r.chunks.SetEmbeddings(ctx, updates); err != nil {
			t.Fatalf("set embeddings: %v", err)
		}
		if left, err := r.chunks.ListUnembedded(ctx, doc.ID, "m"); err != nil || len(left) != 0 {
			t.Fatalf("still unembedded = %d, %v", len(left), err)
		}
		if left, err := r.chunks.ListUnembedded(ctx, doc.ID, "other-model"); err != nil || len(left) != len(chunks) {
			t.Fatalf("model change must re-embed everything: %d, %v", len(left), err)
		}
		q := make([]float32, biz.EmbeddingDimensions)
		q[target] = 1
		vh, err := r.chunks.Vector(ctx, q, false, []string{space.ID}, nil, 3)
		if err != nil || len(vh) == 0 || !strings.Contains(vh[0].Content, "连接池") || vh[0].Score < 0.99 {
			t.Fatalf("vector hits = %+v, %v", vh, err)
		}
		if vh, err = r.chunks.Vector(ctx, q, false, []string{mustID(t)}, nil, 3); err != nil || len(vh) != 0 {
			t.Fatalf("vector outside scope = %v, %v", vh, err)
		}
		// 重发布同样内容：摘要相同的分块复用向量，不需要重新向量化。
		again := biz.Split(doc.Title, content)
		for _, c := range again {
			c.ID, c.DocumentID, c.SpaceID, c.Version = mustID(t), doc.ID, space.ID, 2
		}
		if err := r.txm.WithinTx(ctx, func(tx txrun.Tx) error { return r.chunks.ReplaceTx(ctx, tx, doc.ID, again) }); err != nil {
			t.Fatalf("replace chunks: %v", err)
		}
		if left, err := r.chunks.ListUnembedded(ctx, doc.ID, "m"); err != nil || len(left) != 0 {
			t.Fatalf("vectors must be reused after republish: %d left, %v", len(left), err)
		}
		wantEmbedded = int64(len(chunks))
	} else if _, err := r.chunks.Vector(ctx, nil, true, nil, nil, 5); err != nil {
		t.Fatalf("vector without pgvector must be a no-op: %v", err)
	}
	total2, embedded, err := r.chunks.Counts(ctx)
	if err != nil || total2 != int64(len(chunks)) || embedded != wantEmbedded {
		t.Fatalf("counts = %d/%d, %v", total2, embedded, err)
	}

	// 版本。
	vs, err := r.vers.List(ctx, doc.ID)
	if err != nil || len(vs) != 1 || vs[0].Content != "" {
		t.Fatalf("versions = %v, %v", vs, err)
	}
	if v, err := r.vers.Get(ctx, doc.ID, 1); err != nil || v.Content != content {
		t.Fatalf("version 1 = %v, %v", v, err)
	}
	if _, err := r.vers.Get(ctx, doc.ID, 9); !errors.Is(err, biz.ErrVersionNotFound) {
		t.Fatalf("missing version = %v", err)
	}

	// 索引任务：领取 → 完成删除；重排保留。
	jobs, err := r.index.Claim(ctx, time.Minute, 5)
	if err != nil || len(jobs) != 1 || jobs[0].Version != 1 || jobs[0].Attempts != 1 {
		t.Fatalf("claim index = %+v, %v", jobs, err)
	}
	if again, err := r.index.Claim(ctx, time.Minute, 5); err != nil || len(again) != 0 {
		t.Fatalf("claim leased job again = %v, %v", again, err)
	}
	if err := r.index.Finish(ctx, jobs[0], biz.JobQueued, "retry", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if p, f, err := r.index.Counts(ctx); err != nil || p != 1 || f != 0 {
		t.Fatalf("index counts = %d/%d, %v", p, f, err)
	}
	jobs, err = r.index.Claim(ctx, time.Minute, 5)
	if err != nil || len(jobs) != 1 || jobs[0].Attempts != 2 {
		t.Fatalf("reclaim = %+v, %v", jobs, err)
	}
	if err := r.index.Finish(ctx, jobs[0], biz.JobSucceeded, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if p, _, err := r.index.Counts(ctx); err != nil || p != 0 {
		t.Fatalf("index pending after success = %d, %v", p, err)
	}

	// 关联。
	l := &biz.Link{ID: mustID(t), DocumentID: doc.ID, TargetType: "monitor_rule", TargetID: "rule-1", TargetName: "CPU 高", CreateUser: itOwner, CreateTime: time.Now()}
	if err := r.links.Create(ctx, l); err != nil {
		t.Fatal(err)
	}
	if err := r.links.Create(ctx, &biz.Link{ID: mustID(t), DocumentID: doc.ID, TargetType: "monitor_rule", TargetID: "rule-1", CreateUser: itOwner}); !errors.Is(err, biz.ErrLinkExists) {
		t.Fatalf("duplicate link = %v", err)
	}
	linked, err := r.links.ListByTarget(ctx, "monitor_rule", "rule-1", false, []string{space.ID})
	if err != nil || len(linked) != 1 || linked[0].Title != doc.Title {
		t.Fatalf("by target = %v, %v", linked, err)
	}
	if err := r.links.Delete(ctx, doc.ID, l.ID); err != nil {
		t.Fatal(err)
	}
	if err := r.links.Delete(ctx, doc.ID, l.ID); !errors.Is(err, biz.ErrLinkNotFound) {
		t.Fatalf("delete twice = %v", err)
	}

	// 检索日志。
	log := &biz.SearchLog{ID: mustID(t), UserID: itReader, Source: biz.SearchSourceWeb, Query: "502", TopK: 5, HitCount: 0, ChunkIDs: []string{}, CreateTime: time.Now()}
	if err := r.logs.Create(ctx, log); err != nil {
		t.Fatal(err)
	}
	if err := r.logs.SetFeedback(ctx, log.ID, itOwner, biz.FeedbackHelpful); !errors.Is(err, biz.ErrSearchLogNotFound) {
		t.Fatalf("feedback by stranger = %v", err)
	}
	if err := r.logs.SetFeedback(ctx, log.ID, itReader, biz.FeedbackUnhelpful); err != nil {
		t.Fatal(err)
	}
	st, err := r.logs.Stats(ctx, time.Now().Add(-time.Hour))
	if err != nil || st.SearchCount != 1 || st.NoResultCount != 1 || st.UnhelpfulCount != 1 || len(st.TopNoResult) != 1 || st.TopNoResult[0].Query != "502" {
		t.Fatalf("stats = %+v, %v", st, err)
	}
	if n, err := r.logs.Purge(ctx, time.Now().Add(time.Hour), 10); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}

	// 软删 + 物理清理。
	if err := r.docs.SoftDelete(ctx, doc.ID, time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.docs.Get(ctx, doc.ID); !errors.Is(err, biz.ErrDocumentNotFound) {
		t.Fatalf("get deleted = %v", err)
	}
	if n, err := r.docs.PurgeDeleted(ctx, time.Now().Add(-24*time.Hour), 10); err != nil || n != 1 {
		t.Fatalf("purge deleted = %d, %v", n, err)
	}
	if vs, err := r.vers.List(ctx, doc.ID); err != nil || len(vs) != 0 {
		t.Fatalf("versions after purge = %v, %v", vs, err)
	}
}

func TestIngestRepo(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	space := r.newSpace(t, "imp", biz.VisibilityPublic)
	now := time.Now()
	job := &biz.IngestJob{
		ID: mustID(t), SpaceID: space.ID, FileID: mustID(t), Filename: "a.md", Title: "a", Tags: []string{}, Status: biz.JobQueued,
		CreateUser: itOwner, CreateTime: now, UpdateTime: now,
	}
	if err := r.ingest.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	claimed, err := r.ingest.Claim(ctx, time.Minute, 5)
	if err != nil || len(claimed) != 1 || claimed[0].Status != biz.JobRunning || claimed[0].Attempts != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	if again, err := r.ingest.Claim(ctx, time.Minute, 5); err != nil || len(again) != 0 {
		t.Fatalf("claim leased = %v, %v", again, err)
	}
	if err := r.ingest.Finish(ctx, job.ID, biz.JobFailed, nil, 0, "boom"); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.ingest.Requeue(ctx, job.ID); err != nil || !ok {
		t.Fatalf("requeue = %v, %v", ok, err)
	}
	if ok, err := r.ingest.Requeue(ctx, job.ID); err != nil || ok {
		t.Fatalf("requeue queued job = %v, %v", ok, err)
	}
	docID := mustID(t)
	if err := r.ingest.Finish(ctx, job.ID, biz.JobSucceeded, &docID, 3, ""); err != nil {
		t.Fatal(err)
	}
	got, err := r.ingest.Get(ctx, job.ID)
	if err != nil || got.Status != biz.JobSucceeded || got.DocumentID == nil || *got.DocumentID != docID || got.ChunkCount != 3 {
		t.Fatalf("get = %+v, %v", got, err)
	}
	items, total, err := r.ingest.List(ctx, biz.IngestFilter{SpaceIDs: []string{space.ID}}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("list = %v/%d, %v", items, total, err)
	}
	if _, err := r.ingest.Get(ctx, mustID(t)); !errors.Is(err, biz.ErrIngestNotFound) {
		t.Fatalf("missing = %v", err)
	}
}
