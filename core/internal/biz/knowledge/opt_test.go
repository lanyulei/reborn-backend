package biz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

// countingSpaceRepo 记录 List / Levels 的调用次数。
type countingSpaceRepo struct {
	*fakeSpaceRepo
	lists, names, levels int
	nameIDs              []string
}

func (r *countingSpaceRepo) Names(ctx context.Context, ids []string) (map[string]string, error) {
	r.names++
	r.nameIDs = ids
	return r.fakeSpaceRepo.Names(ctx, ids)
}

func (r *countingSpaceRepo) List(ctx context.Context, keyword, typ string) ([]*Space, error) {
	r.lists++
	return r.fakeSpaceRepo.List(ctx, keyword, typ)
}

func (r *countingSpaceRepo) Levels(ctx context.Context, userID string, roleIDs []string) (map[string]int32, error) {
	r.levels++
	return r.fakeSpaceRepo.Levels(ctx, userID, roleIDs)
}

// countingRoles / countingDirectory 记录调用次数。
type countingRoles struct{ calls int }

func (r *countingRoles) EnabledRoleIDs(ctx context.Context, userID string) (bool, []string, error) {
	r.calls++
	return fakeRoles{}.EnabledRoleIDs(ctx, userID)
}

type countingDirectory struct{ calls int }

func (d *countingDirectory) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	d.calls++
	return fakeDirectory{}.UserNames(ctx, ids)
}

// hitChunkRepo 全文召回固定返回一条命中。
type hitChunkRepo struct {
	fakeChunkRepo
	spaceIDs []string
}

func (r *hitChunkRepo) FullText(_ context.Context, _ string, _ bool, spaceIDs, _ []string, _ int) ([]*ChunkHit, error) {
	r.spaceIDs = spaceIDs
	return []*ChunkHit{{ChunkID: "c1", DocumentID: "d1", Title: "手册", SpaceID: tPub, SpaceName: "公开", Content: "重启步骤"}}, nil
}

type memSearchLogs struct {
	SearchLogRepo
	logs []*SearchLog
}

func (m *memSearchLogs) Create(_ context.Context, l *SearchLog) error {
	m.logs = append(m.logs, l)
	return nil
}

type optEnv struct {
	docs   *DocumentUsecase
	repo   *fakeDocumentRepo
	spaces *countingSpaceRepo
	roles  *countingRoles
	dir    *countingDirectory
	chunks *hitChunkRepo
	logs   *memSearchLogs
}

func newOptEnv() *optEnv {
	_, base := newSpaceUsecaseForTest()
	e := &optEnv{
		repo: newFakeDocumentRepo(), spaces: &countingSpaceRepo{fakeSpaceRepo: base}, roles: &countingRoles{},
		dir: &countingDirectory{}, chunks: &hitChunkRepo{}, logs: &memSearchLogs{},
	}
	spaces := NewSpaceUsecase(e.spaces, e.roles, e.dir, fakeRoleDir{})
	search := NewSearchUsecase(spaces, e.chunks, e.logs, fakeEmbedder{}, NewEmbedGuard(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.docs = NewDocumentUsecase(spaces, search, e.repo, &fakeVersionRepo{}, e.chunks, fakeLinkRepo{}, fakeIndexRepo{}, nil, fakeTxm{}, nil, e.dir)
	return e
}

// 首页概览的四个列表合并后装饰一次：每篇文档补上的展示字段（空间名、负责人名、级别、草稿隐藏）与
// 逐个列表装饰时相同，而「查空间名」「查负责人名」各只发生一次（且不再列全部空间）。
func TestGetOverviewDecoratesOnce(t *testing.T) {
	t.Parallel()
	e := newOptEnv()
	draft := "草稿正文"
	e.repo.docs["d1"] = &Document{ID: "d1", SpaceID: tPub, OwnerID: tOwner, Status: DocStatusPublished, Title: "公开手册", DraftContent: &draft}
	e.repo.docs["d2"] = &Document{ID: "d2", SpaceID: tPriv, OwnerID: tReader, Status: DocStatusPublished, Title: "受限手册", DraftContent: &draft}

	// tReader：公开空间只读、受限空间经角色有写权限。
	out, err := e.docs.GetOverview(context.Background(), tReader)
	if err != nil {
		t.Fatalf("GetOverview() error = %v", err)
	}
	if out.SpaceCount != 2 || len(out.Recent) != 2 || len(out.Mine) != 2 || len(out.ReviewDue) != 2 {
		t.Fatalf("overview = spaces %d, recent %d, mine %d, due %d", out.SpaceCount, len(out.Recent), len(out.Mine), len(out.ReviewDue))
	}
	want := map[string]struct {
		space, owner string
		level        int32
		draftVisible bool
	}{
		"d1": {"公开", "user-01", LevelRead, false},
		"d2": {"受限", "user-02", LevelWrite, true},
	}
	total := 0
	for name, list := range map[string][]*Document{"recent": out.Recent, "mine": out.Mine, "pending": out.PendingReview, "due": out.ReviewDue} {
		for _, d := range list {
			total++
			w := want[d.ID]
			if d.SpaceName != w.space || d.OwnerName != w.owner || d.Level != w.level || (d.DraftContent != nil) != w.draftVisible {
				t.Errorf("%s/%s decorated as space %q owner %q level %d draft %v, want %+v",
					name, d.ID, d.SpaceName, d.OwnerName, d.Level, d.DraftContent != nil, w)
			}
		}
	}
	if total != 6 {
		t.Fatalf("decorated documents = %d, want 6", total)
	}
	// 不再列全部空间；空间名按涉及的空间 ID（去重）查一次，负责人名查一次。
	if e.spaces.lists != 0 || e.spaces.names != 1 || e.dir.calls != 1 || len(e.spaces.nameIDs) != 2 {
		t.Errorf("space lists = %d, name lookups = %d (%v), user-name lookups = %d", e.spaces.lists, e.spaces.names, e.spaces.nameIDs, e.dir.calls)
	}

	// 一篇可见文档都没有：与改动前一样不装饰（不列空间、不查名字）。
	empty := newOptEnv()
	if _, err := empty.docs.GetOverview(context.Background(), tReader); err != nil {
		t.Fatal(err)
	}
	if empty.spaces.lists != 0 || empty.spaces.names != 0 || empty.dir.calls != 0 {
		t.Errorf("empty overview: space lists = %d, user-name lookups = %d, want 0", empty.spaces.lists, empty.dir.calls)
	}
}

// 相关文档：显式关联之后的语义检索沿用同一请求里已解析的权限视图——权限只解析一次，检索范围、
// 命中与检索日志（来源 related）同改动前；直接调用 Search 的路径仍自行解析。
func TestListRelatedResolvesAccessOnce(t *testing.T) {
	t.Parallel()
	e := newOptEnv()
	ctx := context.Background()
	got, err := e.docs.ListRelated(ctx, tReader, "incident", "i-1", " 重启 ", 5)
	if err != nil {
		t.Fatalf("ListRelated() error = %v", err)
	}
	if len(got) != 1 || got[0].DocumentID != "d1" || got[0].Linked || got[0].SpaceName != "公开" {
		t.Fatalf("related = %+v", got)
	}
	if e.roles.calls != 1 || e.spaces.levels != 1 {
		t.Errorf("access resolved: roles %d, levels %d, want 1 each", e.roles.calls, e.spaces.levels)
	}
	if len(e.chunks.spaceIDs) != 2 {
		t.Errorf("search scoped to %v, want both visible spaces", e.chunks.spaceIDs)
	}
	if len(e.logs.logs) != 1 {
		t.Fatalf("search logs = %d, want 1", len(e.logs.logs))
	}
	if l := e.logs.logs[0]; l.UserID != tReader || l.Source != SearchSourceRelated || l.Query != "重启" || l.TopK != 5 || l.HitCount != 1 {
		t.Errorf("search log = %+v", l)
	}

	// 直接检索：行为不变（自行解析一次权限；空查询在解析之前就被拒绝）。
	res, err := e.docs.search.Search(ctx, tReader, SearchSourceWeb, SearchInput{Query: "重启"})
	if err != nil || len(res.Hits) != 1 || e.roles.calls != 2 {
		t.Fatalf("Search() hits = %v, err %v, role lookups %d", res, err, e.roles.calls)
	}
	if _, err := e.docs.search.Search(ctx, tReader, SearchSourceWeb, SearchInput{Query: "  "}); !errors.Is(err, ErrInvalidArgument) || e.roles.calls != 2 {
		t.Fatalf("Search(blank) err = %v, role lookups %d", err, e.roles.calls)
	}
}
