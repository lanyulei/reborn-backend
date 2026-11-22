package biz

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// fakeDocumentRepo 内存文档仓储：复刻 data 层的乐观锁（version 不等于期望值即冲突）与列表过滤口径。
type fakeDocumentRepo struct {
	// beforeSetIndex 索引状态围栏写入前的钩子（模拟任务处理期间文档又发布了新版本）。
	beforeSetIndex func()
	docs           map[string]*Document
	// beforeUpdate 在 Update 比较版本前调用，用来模拟并发写入者。
	beforeUpdate func()
	indexStatus  map[string]int32
	tags         map[string][]string
}

func newFakeDocumentRepo() *fakeDocumentRepo {
	return &fakeDocumentRepo{docs: map[string]*Document{}, indexStatus: map[string]int32{}, tags: map[string][]string{}}
}

func (f *fakeDocumentRepo) Create(_ context.Context, d *Document) error {
	for _, e := range f.docs {
		if e.SpaceID == d.SpaceID && e.Slug == d.Slug {
			return ErrSlugExists
		}
	}
	cp := *d
	f.docs[d.ID] = &cp
	return nil
}

func (f *fakeDocumentRepo) Update(_ context.Context, d *Document, expectedVersion int32) error {
	if f.beforeUpdate != nil {
		f.beforeUpdate()
	}
	cur, ok := f.docs[d.ID]
	if !ok {
		return ErrDocumentNotFound
	}
	if cur.Version != expectedVersion {
		return ErrDocumentConflict
	}
	cp := *d
	cp.Tags, cp.IndexStatus = cur.Tags, cur.IndexStatus // 标签与索引状态不经 Update 写入。
	f.docs[d.ID] = &cp
	return nil
}

func (f *fakeDocumentRepo) SetTags(_ context.Context, id string, tags []string) error {
	d, ok := f.docs[id]
	if !ok {
		return ErrDocumentNotFound
	}
	d.Tags = slices.Clone(tags)
	f.tags[id] = slices.Clone(tags)
	return nil
}

func (f *fakeDocumentRepo) Get(_ context.Context, id string) (*Document, error) {
	d, ok := f.docs[id]
	if !ok {
		return nil, ErrDocumentNotFound
	}
	cp := *d
	return &cp, nil
}

func (f *fakeDocumentRepo) List(_ context.Context, fl DocumentFilter, _, _ int64) ([]*Document, int64, error) {
	out := []*Document{}
	for _, d := range f.docs {
		if !fl.Unrestricted && !slices.Contains(fl.SpaceIDs, d.SpaceID) {
			continue
		}
		if fl.Status != nil && d.Status != *fl.Status {
			continue
		}
		if fl.HideDrafts && d.Status == DocStatusDraft && d.OwnerID != fl.DraftOwnerID && !slices.Contains(fl.DraftSpaceIDs, d.SpaceID) {
			continue
		}
		cp := *d
		cp.Content = ""
		out = append(out, &cp)
	}
	return out, int64(len(out)), nil
}

func (f *fakeDocumentRepo) SoftDelete(_ context.Context, id string, _ time.Time) error {
	delete(f.docs, id)
	return nil
}

func (f *fakeDocumentRepo) MarkReviewNotified(context.Context, string, time.Time) error { return nil }

func (f *fakeDocumentRepo) SetAISummary(context.Context, string, string, []string) error { return nil }

func (f *fakeDocumentRepo) SetIndexStatus(_ context.Context, id string, status int32) error {
	if d, ok := f.docs[id]; ok {
		d.IndexStatus = status
	}
	f.indexStatus[id] = status
	return nil
}

// SetIndexStatusIfVersion 复刻 data 层围栏：仍是已发布且版本未变才写。
func (f *fakeDocumentRepo) SetIndexStatusIfVersion(ctx context.Context, id string, status, version int32) error {
	if f.beforeSetIndex != nil {
		f.beforeSetIndex()
	}
	d, ok := f.docs[id]
	if !ok || d.Status != DocStatusPublished || d.Version != version {
		return nil
	}
	return f.SetIndexStatus(ctx, id, status)
}

func (f *fakeDocumentRepo) ResetReviewNotified(_ context.Context, id string) error {
	if d, ok := f.docs[id]; ok {
		d.ReviewNotifyTime = nil
	}
	return nil
}

func (f *fakeDocumentRepo) PublishedIDs(context.Context, string) ([]string, error) { return nil, nil }

func (f *fakeDocumentRepo) CountVisible(context.Context, bool, []string) (int64, error) {
	return 0, nil
}

func (f *fakeDocumentRepo) Stats(context.Context, time.Time) (DocumentStats, error) {
	return DocumentStats{}, nil
}

func (f *fakeDocumentRepo) PurgeDeleted(context.Context, time.Time, int) (int, error) { return 0, nil }

type fakeVersionRepo struct{ versions []*Version }

func (f *fakeVersionRepo) CreateTx(_ context.Context, _ txrun.Tx, v *Version) error {
	for _, e := range f.versions {
		if e.DocumentID == v.DocumentID && e.Version == v.Version {
			return ErrDocumentConflict
		}
	}
	f.versions = append(f.versions, v)
	return nil
}

func (f *fakeVersionRepo) List(context.Context, string) ([]*Version, error) { return f.versions, nil }

func (f *fakeVersionRepo) Get(context.Context, string, int32) (*Version, error) {
	return nil, ErrVersionNotFound
}

type fakeChunkRepo struct{ unembedded []*Chunk }

func (fakeChunkRepo) VectorAvailable() bool                                       { return true }
func (fakeChunkRepo) ReplaceTx(context.Context, txrun.Tx, string, []*Chunk) error { return nil }
func (fakeChunkRepo) DeleteTx(context.Context, txrun.Tx, string) error            { return nil }
func (f fakeChunkRepo) ListUnembedded(context.Context, string, string) ([]*Chunk, error) {
	return f.unembedded, nil
}
func (fakeChunkRepo) SetEmbeddings(context.Context, []EmbeddingUpdate) error { return nil }
func (fakeChunkRepo) FullText(context.Context, string, bool, []string, []string, int) ([]*ChunkHit, error) {
	return nil, nil
}

func (fakeChunkRepo) Vector(context.Context, []float32, bool, []string, []string, int) ([]*ChunkHit, error) {
	return nil, nil
}

func (fakeChunkRepo) Titles(context.Context, string, bool, []string, []string, int) ([]*DocHit, error) {
	return nil, nil
}

func (fakeChunkRepo) Sections(context.Context, []SectionKey, int) (map[SectionKey]string, error) {
	return nil, nil
}
func (fakeChunkRepo) Counts(context.Context) (int64, int64, error) { return 0, 0, nil }

type fakeLinkRepo struct{}

func (fakeLinkRepo) Create(context.Context, *Link) error                     { return nil }
func (fakeLinkRepo) Delete(context.Context, string, string) error            { return nil }
func (fakeLinkRepo) ListByDocument(context.Context, string) ([]*Link, error) { return nil, nil }
func (fakeLinkRepo) ListByTarget(context.Context, string, string, bool, []string) ([]*LinkedDocument, error) {
	return nil, nil
}

type fakeIndexRepo struct{}

func (fakeIndexRepo) UpsertTx(context.Context, txrun.Tx, string, int32) error { return nil }
func (fakeIndexRepo) Upsert(context.Context, string, int32) error             { return nil }
func (fakeIndexRepo) DeleteTx(context.Context, txrun.Tx, string) error        { return nil }
func (fakeIndexRepo) Claim(context.Context, time.Duration, int) ([]*IndexJob, error) {
	return nil, nil
}

func (fakeIndexRepo) Finish(context.Context, *IndexJob, int32, string, time.Time) error { return nil }

func (fakeIndexRepo) Counts(context.Context) (int64, int64, error) { return 0, 0, nil }

type fakeTxm struct{}

func (fakeTxm) WithinTx(_ context.Context, fn func(tx txrun.Tx) error) error { return fn(nil) }

// fakeEmbedder 按 ctx 状态返回：ctx 已取消返回 ctx.Err()，否则返回 fail。
type fakeEmbedder struct{ fail error }

func (fakeEmbedder) Enabled() bool { return true }
func (fakeEmbedder) Model() string { return "fake" }
func (e fakeEmbedder) Embed(ctx context.Context, _ []string) ([][]float32, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, e.fail
}

func newDocumentUsecaseForTest() (*DocumentUsecase, *fakeDocumentRepo) {
	spaces, _ := newSpaceUsecaseForTest()
	docs := newFakeDocumentRepo()
	uc := NewDocumentUsecase(spaces, nil, docs, &fakeVersionRepo{}, fakeChunkRepo{}, fakeLinkRepo{}, fakeIndexRepo{}, nil, fakeTxm{}, nil, fakeDirectory{})
	return uc, docs
}

func TestUpdateConflictOnConcurrentWrite(t *testing.T) {
	t.Parallel()
	uc, repo := newDocumentUsecaseForTest()
	ctx := context.Background()
	doc, err := uc.Create(ctx, tOwner, CreateDocumentInput{SpaceID: tPub, Title: "草稿", Content: "正文"})
	if err != nil {
		t.Fatal(err)
	}
	// 读到快照之后、写回之前，另一方把文档发布了（version 0 → 1）：带过期版本号的写回必须被拒绝。
	repo.beforeUpdate = func() { repo.docs[doc.ID].Version = 1 }
	title := "改标题"
	if _, err := uc.Update(ctx, tOwner, doc.ID, DocumentPatch{Title: &title}); !errors.Is(err, ErrDocumentConflict) {
		t.Fatalf("stale update error = %v, want ErrDocumentConflict", err)
	}
	if repo.docs[doc.ID].Title != "草稿" {
		t.Fatalf("stale snapshot must not be written, title = %q", repo.docs[doc.ID].Title)
	}
}

func TestPublishPassesPreviousVersionAndSetsIndexStatus(t *testing.T) {
	t.Parallel()
	uc, repo := newDocumentUsecaseForTest()
	ctx := context.Background()
	doc, err := uc.Create(ctx, tOwner, CreateDocumentInput{SpaceID: tPub, Title: "手册", Content: "# 手册\n\n正文", Publish: true})
	if err != nil {
		t.Fatalf("create+publish: %v", err)
	}
	stored := repo.docs[doc.ID]
	if stored.Status != DocStatusPublished || stored.Version != 1 {
		t.Fatalf("stored = status %d version %d, want published v1", stored.Status, stored.Version)
	}
	// 索引状态经专用写入落库（通用 Update 不再携带该列）。
	if repo.indexStatus[doc.ID] != IndexPending {
		t.Fatalf("index status = %d, want pending", repo.indexStatus[doc.ID])
	}
	// 再发布一次：有草稿时期望版本为 1，落 v2。
	content := "# 手册\n\n改过的正文"
	if _, err := uc.Update(ctx, tOwner, doc.ID, DocumentPatch{Content: &content}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := uc.Publish(ctx, tOwner, doc.ID, ""); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if repo.docs[doc.ID].Version != 2 || repo.docs[doc.ID].Content != content {
		t.Fatalf("after second publish = v%d %q", repo.docs[doc.ID].Version, repo.docs[doc.ID].Content)
	}
}

func TestUpdateTagsGoThroughDedicatedSetter(t *testing.T) {
	t.Parallel()
	uc, repo := newDocumentUsecaseForTest()
	ctx := context.Background()
	doc, err := uc.Create(ctx, tOwner, CreateDocumentInput{SpaceID: tPub, Title: "标签", Content: "正文"})
	if err != nil {
		t.Fatal(err)
	}
	// 后台 AI 标签先落库；用户只改摘要时不能把标签清掉。
	repo.docs[doc.ID].Tags = []string{"ai"}
	summary := "摘要"
	if _, err := uc.Update(ctx, tOwner, doc.ID, DocumentPatch{Summary: &summary}); err != nil {
		t.Fatal(err)
	}
	if got := repo.docs[doc.ID].Tags; len(got) != 1 || got[0] != "ai" {
		t.Fatalf("tags clobbered by metadata update: %v", got)
	}
	tags := []string{"nginx", " nginx ", "排障"}
	if _, err := uc.Update(ctx, tOwner, doc.ID, DocumentPatch{Tags: &tags}); err != nil {
		t.Fatal(err)
	}
	if got := repo.tags[doc.ID]; len(got) != 2 || got[0] != "nginx" || got[1] != "排障" {
		t.Fatalf("SetTags = %v", got)
	}
}

func TestDraftVisibility(t *testing.T) {
	t.Parallel()
	uc, repo := newDocumentUsecaseForTest()
	ctx := context.Background()
	// 公开空间：tOwner 负责人（管理级别），tReader 只读。
	draft, err := uc.Create(ctx, tOwner, CreateDocumentInput{SpaceID: tPub, Title: "草稿", Content: "半成品"})
	if err != nil {
		t.Fatal(err)
	}
	published, err := uc.Create(ctx, tOwner, CreateDocumentInput{SpaceID: tPub, Title: "已发布", Content: "正文", Publish: true})
	if err != nil {
		t.Fatal(err)
	}
	// 只读成员看别人的草稿：按不存在处理，不暴露存在性。
	if _, err := uc.Get(ctx, tReader, draft.ID, ""); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("reader get draft = %v, want ErrDocumentNotFound", err)
	}
	if _, err := uc.Get(ctx, tReader, published.ID, ""); err != nil {
		t.Fatalf("reader get published = %v", err)
	}
	// 负责人与写权限 / 超管可见。
	if _, err := uc.Get(ctx, tOwner, draft.ID, ""); err != nil {
		t.Fatalf("owner get draft = %v", err)
	}
	if _, err := uc.Get(ctx, tAdmin, draft.ID, ""); err != nil {
		t.Fatalf("admin get draft = %v", err)
	}
	// 只读成员自己的草稿可见（负责人）。
	repo.docs[draft.ID].OwnerID = tReader
	if _, err := uc.Get(ctx, tReader, draft.ID, ""); err != nil {
		t.Fatalf("reader get own draft = %v", err)
	}
	repo.docs[draft.ID].OwnerID = tOwner

	// 列表：只读成员看不到别人的草稿，负责人与超管都能看到。
	items, total, err := uc.List(ctx, tReader, DocumentFilter{}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != published.ID {
		t.Fatalf("reader list = %d items total=%d err=%v", len(items), total, err)
	}
	if items, _, err := uc.List(ctx, tOwner, DocumentFilter{}, 1, 10); err != nil || len(items) != 2 {
		t.Fatalf("owner list = %d items, %v", len(items), err)
	}
	if items, _, err := uc.List(ctx, tAdmin, DocumentFilter{}, 1, 10); err != nil || len(items) != 2 {
		t.Fatalf("admin list = %d items, %v", len(items), err)
	}
}

func TestEmbedGuardIgnoresCallerCancellation(t *testing.T) {
	t.Parallel()
	upstream := errors.New("embedding upstream down")

	// 检索路径：调用方取消不计失败；服务自身出错才计。
	guard := &EmbedGuard{now: time.Now}
	search := &SearchUsecase{embedder: fakeEmbedder{fail: upstream}, guard: guard, now: time.Now}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := search.embedQuery(canceled, "q"); !errors.Is(err, context.Canceled) {
		t.Fatalf("embedQuery canceled = %v", err)
	}
	if guard.failures != 0 {
		t.Fatalf("canceled call counted as failure: %d", guard.failures)
	}
	if _, err := search.embedQuery(context.Background(), "q"); !errors.Is(err, upstream) {
		t.Fatalf("embedQuery upstream = %v", err)
	}
	if guard.failures != 1 {
		t.Fatalf("upstream failure not counted: %d", guard.failures)
	}

	// 索引路径同样。
	guard = &EmbedGuard{now: time.Now}
	index := &IndexUsecase{
		embedder: fakeEmbedder{fail: upstream}, guard: guard, chunks: fakeChunkRepo{unembedded: []*Chunk{{ID: "c1", Content: "x"}}},
		cfg: Config{BatchSize: defaultBatchSize}, logger: slog.Default(), now: time.Now,
	}
	doc := &Document{ID: "d1", Title: "t"}
	if err := index.embed(canceled, doc); !errors.Is(err, context.Canceled) {
		t.Fatalf("embed canceled = %v", err)
	}
	if guard.failures != 0 {
		t.Fatalf("canceled index call counted as failure: %d", guard.failures)
	}
	if err := index.embed(context.Background(), doc); !errors.Is(err, upstream) {
		t.Fatalf("embed upstream = %v", err)
	}
	if guard.failures != 1 {
		t.Fatalf("upstream index failure not counted: %d", guard.failures)
	}
}

// 索引任务回写时文档已发布了新版本：旧任务不能把新版本「排队中」的索引状态改写成已完成；版本未变时正常写入。
func TestIndexDoneFencedOnDocumentVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, spaceRepo := newSpaceUsecaseForTest()
	repo := newFakeDocumentRepo()
	repo.docs["d1"] = &Document{ID: "d1", SpaceID: tPub, Title: "t", Status: DocStatusPublished, Version: 1, IndexStatus: IndexPending}
	repo.indexStatus["d1"] = IndexPending
	index := &IndexUsecase{
		docs: repo, spaces: spaceRepo, chunks: fakeChunkRepo{}, index: fakeIndexRepo{}, embedder: fakeEmbedder{}, guard: &EmbedGuard{now: time.Now},
		cfg: Config{IndexConcurrency: 1, BatchSize: defaultBatchSize}, logger: slog.Default(), now: time.Now,
	}
	// Get 之后、回写之前又发布了 v2（新任务已排队）。
	repo.beforeSetIndex = func() { repo.docs["d1"].Version = 2 }
	index.process(ctx, &IndexJob{DocumentID: "d1", Version: 1, Status: JobRunning})
	if repo.indexStatus["d1"] != IndexPending {
		t.Fatalf("index status = %d after stale job, want pending kept", repo.indexStatus["d1"])
	}
	repo.beforeSetIndex = nil
	index.process(ctx, &IndexJob{DocumentID: "d1", Version: 2, Status: JobRunning})
	if repo.indexStatus["d1"] != IndexDone {
		t.Fatalf("index status = %d after current job, want done", repo.indexStatus["d1"])
	}
}
