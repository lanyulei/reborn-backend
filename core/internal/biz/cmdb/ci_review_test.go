package biz

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 采集源删除后不会再有同步写 owner=sync 的字段：其下资源须允许人工维护这些字段（此前永久只读，
// 只能逐台解除托管）；采集源仍在时照旧拒绝。导入试运行走同一判定。
func TestUpdate_SyncOwnedFieldAfterSourceDeleted(t *testing.T) {
	t.Parallel()
	ctx, actor := context.Background(), Actor{UserID: "u"}
	newCase := func(sourceDeleted bool) (*CIUsecase, *ciFixture, *CI) {
		f := newCIFixture()
		f.fields.byModel["host"] = append(f.fields.byModel["host"], &Field{Code: "sn", Type: FieldTypeString, Owner: FieldOwnerSync})
		host := f.seedHost()
		f.cis.items[host.ID].SourceDeleted = sourceDeleted
		return f.usecase(), f, host
	}
	upd := CIUpdate{Attrs: map[string]any{"sn": "SN-1"}}

	uc, f, host := newCase(false)
	if _, err := uc.Update(ctx, actor, Scope{Unrestricted: true}, host.ID, host.Version, upd); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("live source: sync-owned field must stay read-only, got %v", err)
	}
	if len(f.cis.updated) != 0 {
		t.Fatal("rejected update must not write")
	}
	res, err := uc.Import(ctx, actor, Scope{Unrestricted: true}, "host", []ImportRow{{Name: host.Name, Attrs: upd.Attrs}}, true)
	if err != nil || res.Failed != 1 {
		t.Fatalf("live source: dry-run import must report the row as failed, got %+v err=%v", res, err)
	}

	uc, f, host = newCase(true)
	res, err = uc.Import(ctx, actor, Scope{Unrestricted: true}, "host", []ImportRow{{Name: host.Name, Attrs: upd.Attrs}}, true)
	if err != nil || res.Updated != 1 {
		t.Fatalf("deleted source: dry-run import must preview an update, got %+v err=%v", res, err)
	}
	if _, err := uc.Update(ctx, actor, Scope{Unrestricted: true}, host.ID, host.Version, upd); err != nil {
		t.Fatalf("deleted source: sync-owned field must be editable, got %v", err)
	}
	if len(f.cis.updated) != 1 || f.cis.updated[0].Attrs["sn"] != "SN-1" {
		t.Fatalf("update must be written: %+v", f.cis.updated)
	}
	// 对账锚点的守卫（rejectExternalIDChange）不随采集源删除放开：它由主控另行定义，这里只锁住现状。
	changed := "i-other"
	if _, err := uc.Update(ctx, actor, Scope{Unrestricted: true}, host.ID, 2, CIUpdate{ExternalID: &changed}); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("external id guard must be unchanged, got %v", err)
	}
}

func importActions(res *ImportResult) []string {
	out := make([]string, 0, len(res.Results))
	for _, r := range res.Results {
		out = append(out, r.Action)
	}
	return out
}

// 同批重名行：正式导入逐行提交，第 2 行起走更新链路；试运行此前按「库中不存在」把每行都判成
// create（缺必填的第 3 行还会报错），与正式结果不符。两种模式的动作必须逐行一致。
func TestImport_DryRunMatchesRealForDuplicateNames(t *testing.T) {
	t.Parallel()
	ctx, actor, scope := context.Background(), Actor{UserID: "u"}, Scope{Unrestricted: true}
	rows := []ImportRow{
		{Name: "n1", Attrs: map[string]any{"ip": "10.0.0.5"}},
		{Name: "n1", Attrs: map[string]any{"ip": "10.0.0.5"}},  // 与第 1 行相同 → skip
		{Name: "n1", Attrs: map[string]any{"owner_note": "x"}}, // 不带必填 ip：更新链路下合法 → update
		{Name: "n1", Attrs: map[string]any{"owner_note": "x"}}, // 在第 3 行之上再预演 → skip
		{Name: "n2", Attrs: map[string]any{"owner_note": "x"}}, // 新名称缺必填 → error
	}
	want := []string{ImportActionCreate, ImportActionSkip, ImportActionUpdate, ImportActionSkip, ImportActionError}

	f := newCIFixture()
	dry, err := f.usecase().Import(ctx, actor, scope, "host", rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := importActions(dry); !slices.Equal(got, want) {
		t.Fatalf("dry-run actions = %v, want %v", got, want)
	}
	if len(f.cis.created) != 0 || len(f.cis.updated) != 0 {
		t.Fatal("dry run must not write")
	}

	f = newCIFixture()
	live, err := f.usecase().Import(ctx, actor, scope, "host", rows, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := importActions(live); !slices.Equal(got, want) {
		t.Fatalf("real import actions = %v, want %v (dry run must match)", got, want)
	}
	if dry.Created != live.Created || dry.Updated != live.Updated || dry.Skipped != live.Skipped || dry.Failed != live.Failed {
		t.Fatalf("summary mismatch: dry=%+v live=%+v", dry, live)
	}
}

// 关系已存在：正式导入 Link 返回 0、行保持 skip；试运行此前仍计数并把 skip 改报 update。
func TestImport_DryRunDoesNotCountExistingRelations(t *testing.T) {
	t.Parallel()
	ctx, actor, scope := context.Background(), Actor{UserID: "u"}, Scope{Unrestricted: true}
	rows := []ImportRow{{Name: "h1", Relations: []ImportRelation{{Type: "host_runs_app", Direction: DirectionOutgoing, TargetName: "a1"}}}}

	f := newCIFixture()
	host, app := f.seedHost(), f.seedApp("prod")
	f.rels.exists["host_runs_app|"+host.ID+"|"+app.ID] = true
	res, err := f.usecase().Import(ctx, actor, scope, "host", rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if r := res.Results[0]; r.Action != ImportActionSkip || r.Message != "" || res.Skipped != 1 {
		t.Fatalf("existing relation must not be counted: %+v", r)
	}

	// 关系尚不存在：照旧计数并报 update。
	f = newCIFixture()
	f.seedHost()
	f.seedApp("prod")
	res, err = f.usecase().Import(ctx, actor, scope, "host", rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if r := res.Results[0]; r.Action != ImportActionUpdate || r.Message != "relations linked: 1" {
		t.Fatalf("new relation must be counted: %+v", r)
	}
}

// guardedModelRepo 带删除前置检查能力（ModelDeleteGuard）的类型仓储桩。
type guardedModelRepo struct {
	*memModelRepo
	deletedCIs int64
	refs       []string
	softDelete []string
}

func (r *guardedModelRepo) SoftDelete(_ context.Context, code string, _ time.Time) error {
	r.softDelete = append(r.softDelete, code)
	return nil
}

func (r *guardedModelRepo) DeletedCICount(context.Context, string) (int64, error) {
	return r.deletedCIs, nil
}

func (r *guardedModelRepo) RefFields(context.Context, string) ([]string, error) { return r.refs, nil }

// plainModelRepo 不带删除前置检查能力的仓储桩（只有 SoftDelete）。
type plainModelRepo struct {
	*memModelRepo
	softDelete []string
}

func (r *plainModelRepo) SoftDelete(_ context.Context, code string, _ time.Time) error {
	r.softDelete = append(r.softDelete, code)
	return nil
}

// 删除类型的前置检查：回收站里仍有该类型资源、或被其它类型的 ref 字段引用 → ErrModelInUse 且不删；
// 无引用时与从前一样只调用 SoftDelete；既有的三项拒绝（内置 / 仍有资源 / 被关系类型引用）不变。
func TestDeleteModel_Guards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	newRepo := func() *guardedModelRepo {
		return &guardedModelRepo{memModelRepo: &memModelRepo{models: map[string]*Model{
			"rack":    {Code: "rack", Name: "机柜", Status: ModelStatusEnabled},
			"host":    {Code: "host", Name: "主机", Status: ModelStatusEnabled},
			"builtin": {Code: "builtin", Builtin: true},
			"busy":    {Code: "busy", CICount: 3},
		}}}
	}
	relTypes := &memRelTypeRepo{types: []*RelationType{{Code: "host_runs_app", SrcModel: "host", DstModel: "app"}}}
	usecase := func(models ModelRepo) *ModelUsecase {
		return NewModelUsecase(models, nil, relTypes, nil, nil, nil, nil, nil, nil)
	}
	message := func(err error) string { return kerrors.FromError(err).Message }

	// 无引用：正常删除，只走 SoftDelete。
	repo := newRepo()
	if err := usecase(repo).DeleteModel(ctx, "rack"); err != nil {
		t.Fatalf("unreferenced model must be deleted, got %v", err)
	}
	if !slices.Equal(repo.softDelete, []string{"rack"}) {
		t.Fatalf("SoftDelete calls = %v", repo.softDelete)
	}

	// 回收站里仍有资源。
	repo = newRepo()
	repo.deletedCIs = 2
	err := usecase(repo).DeleteModel(ctx, "rack")
	if !errors.Is(err, ErrModelInUse) || !strings.Contains(message(err), "2 deleted resources") || len(repo.softDelete) != 0 {
		t.Fatalf("recycle bin must block deletion: err=%v deleted=%v", err, repo.softDelete)
	}

	// 被其它类型的 ref 字段引用。
	repo = newRepo()
	repo.refs = []string{"host.rack", "switch.rack"}
	err = usecase(repo).DeleteModel(ctx, "rack")
	if !errors.Is(err, ErrModelInUse) || !strings.Contains(message(err), "host.rack, switch.rack") || len(repo.softDelete) != 0 {
		t.Fatalf("ref fields must block deletion: err=%v deleted=%v", err, repo.softDelete)
	}

	// 既有拒绝不变（先于新检查）。
	repo = newRepo()
	for code, want := range map[string]string{"builtin": "builtin model", "busy": "3 resources", "host": "referenced by relation type host_runs_app"} {
		if err := usecase(repo).DeleteModel(ctx, code); !errors.Is(err, ErrModelInUse) || message(err) != want {
			t.Errorf("DeleteModel(%s) = %v, want ErrModelInUse %q", code, err, want)
		}
	}
	if err := usecase(repo).DeleteModel(ctx, "nope"); !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("unknown model error = %v", err)
	}
	if len(repo.softDelete) != 0 {
		t.Fatalf("rejected deletions must not write: %v", repo.softDelete)
	}

	// 仓储不具备检查能力（内存桩）：行为与补检查之前一致。
	plain := &plainModelRepo{memModelRepo: newRepo().memModelRepo}
	if err := usecase(plain).DeleteModel(ctx, "rack"); err != nil || !slices.Equal(plain.softDelete, []string{"rack"}) {
		t.Fatalf("repo without the guard capability: err=%v deleted=%v", err, plain.softDelete)
	}
}

// conflictSourceRepo / conflictRunRepo 冲突清单用例的最小桩。
type conflictSourceRepo struct {
	SyncSourceRepo
	env string
}

func (r conflictSourceRepo) Get(_ context.Context, id string) (*SyncSource, error) {
	if id != testIDRoot {
		return nil, ErrSourceNotFound
	}
	return &SyncSource{ID: id, Env: r.env}, nil
}

type conflictRunRepo struct {
	SyncRunRepo
	items     []*SyncConflict
	capped    map[string]bool // 类型 → 是否封顶
	gotModels [][]string      // 每次按范围查询收到的类型集合
}

func (r *conflictRunRepo) ListConflicts(context.Context, string, int64, int64) ([]*SyncConflict, int64, error) {
	return r.items, int64(len(r.items)), nil
}

func (r *conflictRunRepo) ConflictsCapped(context.Context, string) (bool, error) {
	for _, c := range r.capped {
		if c {
			return true, nil
		}
	}
	return false, nil
}

// scopedConflictRunRepo 在 conflictRunRepo 之上补齐 ScopedConflictRepo 能力。
type scopedConflictRunRepo struct{ *conflictRunRepo }

func (r scopedConflictRunRepo) ListConflictsIn(_ context.Context, _ string, modelCodes []string, _, _ int64) ([]*SyncConflict, int64, error) {
	r.gotModels = append(r.gotModels, modelCodes)
	var out []*SyncConflict
	for _, c := range r.items {
		if slices.Contains(modelCodes, c.ModelCode) {
			out = append(out, c)
		}
	}
	return out, int64(len(out)), nil
}

func (r scopedConflictRunRepo) ConflictsCappedIn(_ context.Context, _ string, modelCodes []string) (bool, error) {
	for _, code := range modelCodes {
		if r.capped[code] {
			return true, nil
		}
	}
	return false, nil
}

// 冲突清单带着采集到的资源名称与外部标识：受限调用者只看得到可见类型的冲突，total 与「是否封顶」
// 同口径；看不到采集源所在环境时整份清单不可见；不限的调用者与从前一致。
func TestListConflicts_Scope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	newRuns := func() *conflictRunRepo {
		return &conflictRunRepo{
			items:  []*SyncConflict{{ModelCode: "host", Name: "h-x"}, {ModelCode: "switch", Name: "sw-x"}},
			capped: map[string]bool{"switch": true},
		}
	}
	type result struct {
		names  []string
		total  int64
		capped bool
	}
	run := func(t *testing.T, uc *SyncUsecase, scope Scope) result {
		t.Helper()
		items, total, err := uc.ListConflicts(ctx, scope, testIDRoot, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		capped, err := uc.ConflictsCapped(ctx, scope, testIDRoot)
		if err != nil {
			t.Fatal(err)
		}
		res := result{total: total, capped: capped}
		for _, c := range items {
			res.names = append(res.names, c.Name)
		}
		return res
	}
	scoped := func(env string) (*SyncUsecase, *conflictRunRepo) {
		runs := newRuns()
		return NewSyncUsecase(conflictSourceRepo{env: env}, nil, scopedConflictRunRepo{runs}, nil), runs
	}

	uc, runs := scoped("prod")
	if got := run(t, uc, Scope{Unrestricted: true}); got.total != 2 || !got.capped || len(runs.gotModels) != 0 {
		t.Fatalf("unrestricted = %+v scopedCalls=%v", got, runs.gotModels)
	}
	// 类型受限：只见 host；switch 的封顶与它无关。
	if got := run(t, uc, scopeHostProd()); got.total != 1 || !slices.Equal(got.names, []string{"h-x"}) || got.capped {
		t.Fatalf("model-restricted = %+v", got)
	}
	if len(runs.gotModels) != 1 || !slices.Equal(runs.gotModels[0], []string{"host"}) {
		t.Fatalf("visible models must be pushed down: %v", runs.gotModels)
	}
	// 类型不限 + 仅 prod：清单完整（不走按类型过滤的查询）。
	uc, runs = scoped("prod")
	if got := run(t, uc, Scope{ModelUnrestricted: true, Envs: map[string]struct{}{"prod": {}}}); got.total != 2 || !got.capped || len(runs.gotModels) != 0 {
		t.Fatalf("model-unrestricted = %+v scopedCalls=%v", got, runs.gotModels)
	}
	// 采集源的环境不可见（含未分环境的采集源对环境受限者）：整份清单不可见。
	for _, env := range []string{"dev", ""} {
		uc, _ = scoped(env)
		if got := run(t, uc, scopeHostProd()); got.total != 0 || len(got.names) != 0 || got.capped {
			t.Fatalf("source env %q out of scope = %+v", env, got)
		}
	}
	// 仓储不具备按类型过滤的能力：受限调用者得到空清单，而不是全量。
	uc = NewSyncUsecase(conflictSourceRepo{env: "prod"}, nil, newRuns(), nil)
	if got := run(t, uc, scopeHostProd()); got.total != 0 || len(got.names) != 0 || got.capped {
		t.Fatalf("repo without the scoped capability must not leak: %+v", got)
	}
	if got := run(t, uc, Scope{Unrestricted: true}); got.total != 2 || !got.capped {
		t.Fatalf("unrestricted on a plain repo = %+v", got)
	}
	// 采集源不存在：沿用既有错误。
	if _, _, err := uc.ListConflicts(ctx, scopeHostProd(), testIDSwitch, 1, 10); !errors.Is(err, ErrSourceNotFound) {
		t.Fatalf("unknown source error = %v", err)
	}
}
