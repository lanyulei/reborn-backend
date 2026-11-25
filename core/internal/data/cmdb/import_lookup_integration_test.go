//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 导入的批量读取（真实 PG）：
//   - GetByNames 与 GetByName 逐名称等价（同一组列、同样不含已删除、同样按类型限定）；
//   - 在真实仓储上端到端跑一遍导入（试运行 + 正式）：不再有逐行的名称查询，名称与关系目标是常数条批量
//     查询；试运行与正式导入的逐行结论一致；落库状态符合预期。

// stmtRecorder 记录连接池上执行过的语句文本（pgx QueryTracer），测试里按片段统计。
type stmtRecorder struct {
	mu   sync.Mutex
	sqls []string
}

func (r *stmtRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	r.sqls = append(r.sqls, data.SQL)
	r.mu.Unlock()
	return ctx
}

func (r *stmtRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *stmtRecorder) reset() {
	r.mu.Lock()
	r.sqls = nil
	r.mu.Unlock()
}

// total 记录到的语句总数。
func (r *stmtRecorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sqls)
}

// count 含指定片段的语句数。
func (r *stmtRecorder) count(fragment string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.sqls {
		if strings.Contains(s, fragment) {
			n++
		}
	}
	return n
}

const (
	stmtGetByName   = "c.model_code = $1 AND c.name = $2 AND c.delete_time IS NULL"
	stmtGetByNames  = "c.name = ANY($2::text[])"
	stmtRelationGet = "FROM cmdb_relation_type t WHERE t.code = $1 AND t.delete_time IS NULL"
)

// newRecordingCmdbData 带语句记录的连接池（连接串不落日志、不进错误信息）。
func newRecordingCmdbData(tb testing.TB) (*basedata.Data, *stmtRecorder) {
	tb.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		tb.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		tb.Fatalf("parse TEST_DATABASE_SOURCE failed (%T)", err)
	}
	if db := cfg.ConnConfig.Database; !containsTest(db) {
		tb.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	rec := &stmtRecorder{}
	cfg.ConnConfig.Tracer = rec
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("create recording pool failed (%T)", err)
	}
	tb.Cleanup(pool.Close)
	return &basedata.Data{Pool: pool}, rec
}

// sameCI 两次读取的资源逐字段一致（时间按时刻比较）。
func sameCI(a, b *biz.CI) bool {
	return a.ID == b.ID && a.ModelCode == b.ModelCode && a.ModelName == b.ModelName && a.Name == b.Name && a.Status == b.Status &&
		a.Env == b.Env && a.Owner == b.Owner && slices.Equal(a.Tags, b.Tags) && a.Source == b.Source && a.ExternalID == b.ExternalID &&
		fmt.Sprint(a.Attrs) == fmt.Sprint(b.Attrs) && a.SourceName == b.SourceName && a.SourceDeleted == b.SourceDeleted &&
		a.OwnerName == b.OwnerName && a.Stale == b.Stale && a.Version == b.Version && a.CreateUser == b.CreateUser && a.UpdateUser == b.UpdateUser &&
		a.CreateTime.Equal(b.CreateTime) && a.UpdateTime.Equal(b.UpdateTime) && a.DeleteTime == nil && b.DeleteTime == nil
}

// GetByNames 与 GetByName 对每个名称给出同一行；同名不同类型互不串；已删除、不存在的名称不在结果里。
func TestIntegration_CmdbGetByNamesMatchesGetByName(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	cis := NewCIRepo(d)
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_model (code, name, category) VALUES ('il_host', '主机', 'compute'), ('il_app', '应用', 'other')`); err != nil {
		t.Fatal(err)
	}
	// 同一批名称在两个类型下各有一条；外加一条已删除的与几条特殊名称。
	names := []string{"web-1", "web-2", "中文 名称", "it's", `100%_ok`, "UPPER-case", "x"}
	now := time.Now()
	for _, model := range []string{"il_host", "il_app"} {
		for _, name := range names {
			ci := &biz.CI{
				ID: mustID(t), ModelCode: model, Name: name, Status: biz.CIStatusActive, Env: "prod", Tags: []string{"t"},
				Source: biz.SourceManual, Attrs: map[string]any{"ip": "10.0.0.1", "n": 3}, Version: 1, CreateUser: mustID(t), CreateTime: now, UpdateTime: now,
			}
			if err := cis.Create(ctx, ci, hist(t, ci.ID, "create")); err != nil {
				t.Fatalf("create %s/%s: %v", model, name, err)
			}
		}
	}
	deleted := &biz.CI{ID: mustID(t), ModelCode: "il_host", Name: "gone", Status: biz.CIStatusActive, Tags: []string{}, Source: biz.SourceManual, Attrs: map[string]any{}, Version: 1, CreateTime: now, UpdateTime: now}
	if err := cis.Create(ctx, deleted, hist(t, deleted.ID, "create")); err != nil {
		t.Fatal(err)
	}
	if err := cis.SoftDelete(ctx, deleted.ID, time.Now(), hist(t, deleted.ID, "delete")); err != nil {
		t.Fatal(err)
	}

	absent := []string{"gone", "nope", "", "web-1 "}
	for _, model := range []string{"il_host", "il_app"} {
		for _, set := range [][]string{
			append(append([]string{}, names...), absent...), // 全部 + 不存在 / 已删除
			{"web-2", "web-2", "中文 名称"},                     // 重复名称
			{"UPPER-case"},
			{"nope"},
			nil,
		} {
			items, err := cis.GetByNames(ctx, model, set)
			if err != nil {
				t.Fatalf("GetByNames(%s, %v): %v", model, set, err)
			}
			if len(items) > len(set) {
				t.Fatalf("GetByNames(%s) returned %d rows for %d names", model, len(items), len(set))
			}
			seen := map[string]int{}
			for _, ci := range items {
				seen[ci.Name]++
				if ci.ModelCode != model {
					t.Fatalf("GetByNames(%s) returned a %s resource", model, ci.ModelCode)
				}
			}
			for name, n := range seen {
				if n != 1 {
					t.Fatalf("GetByNames(%s) returned %q %d times", model, name, n)
				}
			}
			for _, name := range set {
				want, wantErr := cis.GetByName(ctx, model, name)
				if _, ok := seen[name]; ok != (wantErr == nil) {
					t.Fatalf("GetByNames(%s, %q): present=%v, GetByName err=%v", model, name, ok, wantErr)
				}
				if wantErr == nil && !sameCI(want, items[slices.IndexFunc(items, func(c *biz.CI) bool { return c.Name == name })]) {
					t.Fatalf("GetByNames(%s, %q) row differs from GetByName:\n %+v\n %+v", model, name, items[0], want)
				}
				if wantErr != nil && !errors.Is(wantErr, biz.ErrCINotFound) {
					t.Fatalf("GetByName(%s, %q): %v", model, name, wantErr)
				}
			}
		}
	}
}

// hiddenBatchLookup 只暴露 CIRepo：仓储「不具备」批量按名称读取的能力，导入退回逐行 GetByName
// （改写前每行一次名称查询的路径；改写前另外还有写入后的回读）。
type hiddenBatchLookup struct{ biz.CIRepo }

// importFX 真实仓储上的导入夹具：类型 ib_host（ip 必填、note）、ib_app；关系 ib_runs（一对多）、ib_peer。
type importFX struct {
	d        *basedata.Data
	rec      *stmtRecorder
	existing string // 预置的主机 ib-exist
	app1     string // 预置的应用 ib-app-1（已与 ib-exist 关联）
}

// newImportFX 清空 cmdb 表后搭建夹具；rec 记录连接池上的语句。
func newImportFX(t *testing.T) *importFX {
	t.Helper()
	newIntegrationCmdbData(t) // 清空 cmdb 表
	d, rec := newRecordingCmdbData(t)
	ctx := context.Background()
	models, fieldRepo, relTypes := NewModelRepo(d), NewFieldRepo(d), NewRelationTypeRepo(d)
	rels, cis := NewRelationRepo(d), NewCIRepo(d)
	now := time.Now()
	for _, m := range []string{"ib_host", "ib_app"} {
		if err := models.Create(ctx, &biz.Model{Code: m, Name: m, Category: "compute", Status: 1, CreateTime: now, UpdateTime: now}); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []*biz.Field{
		{ID: mustID(t), ModelCode: "ib_host", Code: "ip", Name: "IP", Type: biz.FieldTypeIP, Required: true, Owner: biz.FieldOwnerBoth, ListVisible: true, Sort: 1, CreateTime: now, UpdateTime: now},
		{ID: mustID(t), ModelCode: "ib_host", Code: "note", Name: "备注", Type: biz.FieldTypeString, Owner: biz.FieldOwnerManual, CreateTime: now, UpdateTime: now},
	} {
		if err := fieldRepo.Create(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	for _, rt := range []*biz.RelationType{
		{Code: "ib_runs", Name: "运行", ForwardName: "运行的应用", BackwardName: "运行于", SrcModel: "ib_host", DstModel: "ib_app", Cardinality: biz.CardinalityOneToMany, OnDelete: biz.OnDeleteRestrict, CreateTime: now, UpdateTime: now},
		{Code: "ib_peer", Name: "互备", ForwardName: "互备于", BackwardName: "被互备", SrcModel: "ib_host", DstModel: "ib_host", Cardinality: biz.CardinalityManyToMany, OnDelete: biz.OnDeleteRestrict, CreateTime: now, UpdateTime: now},
	} {
		if err := relTypes.Create(ctx, rt); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(model, name, env string) string {
		ci := &biz.CI{
			ID: mustID(t), ModelCode: model, Name: name, Status: biz.CIStatusActive, Env: env, Tags: []string{}, Source: biz.SourceManual,
			Attrs: map[string]any{}, Version: 1, CreateUser: mustID(t), CreateTime: now, UpdateTime: now,
		}
		if model == "ib_host" {
			ci.Attrs["ip"] = "10.0.0.1"
		}
		if err := cis.Create(ctx, ci, hist(t, ci.ID, "create")); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		return ci.ID
	}
	f := &importFX{d: d, rec: rec}
	f.existing = seed("ib_host", "ib-exist", "prod")
	f.app1 = seed("ib_app", "ib-app-1", "prod")
	seed("ib_app", "ib-app-2", "dev")
	if err := rels.Create(ctx, &biz.Relation{ID: mustID(t), TypeCode: "ib_runs", SrcID: f.existing, DstID: f.app1, Source: biz.SourceManual, CreateTime: now},
		[]*biz.History{hist(t, f.existing, "link"), hist(t, f.app1, "link")}); err != nil {
		t.Fatal(err)
	}
	return f
}

// usecase 组装导入用例；ciRepo 可换成隐藏了批量能力的包装。
func (f *importFX) usecase(ciRepo biz.CIRepo) *biz.CIUsecase {
	fieldRepo := NewFieldRepo(f.d)
	return biz.NewCIUsecase(NewModelRepo(f.d), fieldRepo, NewRelationTypeRepo(f.d), ciRepo, NewRelationRepo(f.d), NewHistoryRepo(f.d),
		biz.NewRelationUsecase(NewRelationTypeRepo(f.d), ciRepo, NewRelationRepo(f.d), fieldRepo), nil)
}

// importFXActor 导入的操作者（资源的 create_user / update_user 是 uuid 列）。
func importFXActor(t testing.TB) biz.Actor { return biz.Actor{UserID: mustID(t)} }

// importFXRows 一批 70 行：更新 / 跳过 / 关系已存在、同批重名、前向引用、缺失的对端、方向不符、非法取值，
// 外加 60 行纯新建。
func importFXRows() []biz.ImportRow {
	ops := "ops"
	rows := []biz.ImportRow{
		{Name: "ib-exist", Attrs: map[string]any{"note": "changed"}},                                                                                                            // update
		{Name: "ib-exist", Attrs: map[string]any{"ip": "10.0.0.1"}},                                                                                                             // skip：与库中一致
		{Name: "ib-exist", Relations: []biz.ImportRelation{{Type: "ib_runs", Direction: biz.DirectionOutgoing, TargetName: "ib-app-1"}}},                                        // 关系已存在：保持 skip
		{Name: "ib-fwd", Attrs: map[string]any{"ip": "10.0.1.1"}, Relations: []biz.ImportRelation{{Type: "ib_peer", Direction: biz.DirectionOutgoing, TargetName: "ib-later"}}}, // 引用本批后面才创建的资源
		{Name: "ib-dup", Attrs: map[string]any{"ip": "10.0.1.2"}},                                                                                                               // create
		{Name: "ib-dup", Owner: &ops}, // update：本批新建的同名行
		{Name: "ib-later", Attrs: map[string]any{"ip": "10.0.1.3"}, Relations: []biz.ImportRelation{{Type: "ib_runs", Direction: biz.DirectionOutgoing, TargetName: "ib-app-2"}}},
		{Name: "ib-bad", Attrs: map[string]any{"ip": "bad"}}, // error
		{Name: "ib-missing", Attrs: map[string]any{"ip": "10.0.1.4"}, Relations: []biz.ImportRelation{{Type: "ib_runs", Direction: biz.DirectionOutgoing, TargetName: "no-such-app"}}},
		{Name: "ib-typed", Attrs: map[string]any{"ip": "10.0.1.5"}, Relations: []biz.ImportRelation{{Type: "ib_runs", Direction: biz.DirectionIncoming, TargetName: "ib-app-1"}}}, // 方向不符
	}
	for i := 0; i < 60; i++ {
		rows = append(rows, biz.ImportRow{Name: fmt.Sprintf("ib-bulk-%03d", i), Attrs: map[string]any{"ip": fmt.Sprintf("10.2.%d.%d", i/256, i%256+1)}})
	}
	return rows
}

var importFXWant = []struct{ action, message string }{
	{biz.ImportActionUpdate, ""},
	{biz.ImportActionSkip, ""},
	{biz.ImportActionSkip, ""},
	{biz.ImportActionCreate, "relations linked: 1"},
	{biz.ImportActionCreate, ""},
	{biz.ImportActionUpdate, ""},
	{biz.ImportActionCreate, "relations linked: 1"},
	{biz.ImportActionError, "ip"},
	{biz.ImportActionCreate, "relation failed: "},
	{biz.ImportActionCreate, "relation failed: "},
}

// 真实仓储上的端到端导入（批量读取）：试运行不落库且逐行结论与正式导入一致；不再有逐行的名称查询；
// 落库状态符合预期；受限范围下逐行口径不变。
func TestIntegration_CmdbImportBatchLookups(t *testing.T) {
	f := newImportFX(t)
	ctx := context.Background()
	rows := importFXRows()
	uc := f.usecase(NewCIRepo(f.d))

	// 试运行：不落库；逐行结论必须与正式导入一致。
	f.rec.reset()
	dry, err := uc.Import(ctx, importFXActor(t), biz.Scope{Unrestricted: true}, "ib_host", rows, true)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Created != 65 || dry.Updated != 2 || dry.Skipped != 2 || dry.Failed != 1 {
		t.Fatalf("dry-run summary = created %d updated %d skipped %d failed %d", dry.Created, dry.Updated, dry.Skipped, dry.Failed)
	}
	for i, r := range dry.Results {
		// 试运行不落库：只有已存在资源的行带着它的 ID（与改写前一致），将要新建的行没有。
		wantID := ""
		if rows[i].Name == "ib-exist" {
			wantID = f.existing
		}
		if r.CIID != wantID {
			t.Fatalf("dry-run row %d reports resource id %q, want %q", i, r.CIID, wantID)
		}
		if i < len(importFXWant) && (r.Action != importFXWant[i].action || !strings.Contains(r.Message, importFXWant[i].message)) {
			t.Fatalf("dry-run row %d = %s %q, want %s containing %q", i, r.Action, r.Message, importFXWant[i].action, importFXWant[i].message)
		}
	}
	if n := f.rec.count(stmtGetByName); n != 0 {
		t.Fatalf("dry-run issued %d per-row name lookups (GetByName)", n)
	}
	if n := f.rec.count(stmtGetByNames); n < 1 || n > 4 {
		t.Fatalf("dry-run issued %d name batch lookups; expected a constant number", n)
	}
	var stored int
	if err := f.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci WHERE name LIKE 'ib-%'").Scan(&stored); err != nil || stored != 3 {
		t.Fatalf("dry-run must not persist: stored=%d err=%v", stored, err)
	}

	// 正式导入。
	f.rec.reset()
	real, err := uc.Import(ctx, importFXActor(t), biz.Scope{Unrestricted: true}, "ib_host", rows, false)
	if err != nil {
		t.Fatal(err)
	}
	if real.Created != dry.Created || real.Updated != dry.Updated || real.Skipped != dry.Skipped || real.Failed != dry.Failed {
		t.Fatalf("real summary = created %d updated %d skipped %d failed %d, want the dry-run's", real.Created, real.Updated, real.Skipped, real.Failed)
	}
	byName := map[string]string{}
	for i := range rows {
		d, r := dry.Results[i], real.Results[i]
		if d.Action != r.Action || d.Message != r.Message {
			t.Fatalf("row %d: dry-run %s %q, real %s %q", i, d.Action, d.Message, r.Action, r.Message)
		}
		byName[rows[i].Name] = r.CIID
	}
	if byName["ib-exist"] != f.existing {
		t.Fatalf("row 1 must point at the existing resource: %s vs %s", byName["ib-exist"], f.existing)
	}
	for _, name := range []string{"ib-fwd", "ib-dup", "ib-later", "ib-typed", "ib-bulk-000", "ib-bulk-059"} {
		if byName[name] == "" {
			t.Fatalf("created row %q must report its resource id", name)
		}
	}
	if n := f.rec.count(stmtGetByName); n != 0 {
		t.Fatalf("real import issued %d per-row name lookups (GetByName)", n)
	}
	if n := f.rec.count(stmtGetByNames); n < 1 || n > 4 {
		t.Fatalf("real import issued %d name batch lookups; expected a constant number", n)
	}
	t.Logf("real import of %d rows: %d statements total, %d batch name lookups, %d per-row name lookups, %d relation type lookups, %d insert statements",
		len(rows), f.rec.total(), f.rec.count(stmtGetByNames), f.rec.count(stmtGetByName), f.rec.count(stmtRelationGet), f.rec.count("INSERT INTO cmdb_ci ("))

	// 落库状态。
	var version int64
	if err := f.d.Pool.QueryRow(ctx, "SELECT version FROM cmdb_ci WHERE id = $1", f.existing).Scan(&version); err != nil || version != 2 {
		t.Fatalf("existing resource must be updated exactly once (version 2): version=%d err=%v", version, err)
	}
	if err := f.d.Pool.QueryRow(ctx, "SELECT version FROM cmdb_ci WHERE id = $1", byName["ib-dup"]).Scan(&version); err != nil || version != 2 {
		t.Fatalf("resource created then updated in the same batch must be at version 2: version=%d err=%v", version, err)
	}
	for _, r := range real.Results {
		var n int
		if r.Action == biz.ImportActionError {
			if err := f.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci WHERE name = $1", r.Name).Scan(&n); err != nil || n != 0 {
				t.Fatalf("failed row %q must not be stored: n=%d err=%v", r.Name, n, err)
			}
			continue
		}
		if err := f.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci WHERE id = $1 AND name = $2 AND delete_time IS NULL", r.CIID, r.Name).Scan(&n); err != nil || n != 1 {
			t.Fatalf("row %q must be stored under the reported id: n=%d err=%v", r.Name, n, err)
		}
	}
	var linked int
	if err := f.d.Pool.QueryRow(ctx, `SELECT count(*) FROM cmdb_relation r JOIN cmdb_ci s ON s.id = r.src_id WHERE s.name IN ('ib-fwd', 'ib-later')`).Scan(&linked); err != nil || linked != 2 {
		t.Fatalf("the two newly linked relations must exist: linked=%d err=%v", linked, err)
	}

	// 受限范围：范围内的行照常，范围外的对端按不存在处理（逐行口径不变）。
	prod, dev := "prod", "dev"
	scoped := []biz.ImportRow{
		{Name: "ib-scoped", Env: &prod, Attrs: map[string]any{"ip": "10.3.0.1"}},
		{Name: "ib-scoped-dev", Env: &dev, Attrs: map[string]any{"ip": "10.3.0.2"}},
		{Name: "ib-exist", Relations: []biz.ImportRelation{{Type: "ib_runs", Direction: biz.DirectionOutgoing, TargetName: "ib-app-2"}}},
	}
	scope := biz.Scope{Models: map[string]struct{}{"ib_host": {}, "ib_app": {}}, Envs: map[string]struct{}{"prod": {}}}
	res, err := uc.Import(ctx, importFXActor(t), scope, "ib_host", scoped, true)
	if err != nil || res.Created != 1 || res.Failed != 1 || res.Skipped != 1 ||
		!strings.Contains(res.Results[1].Message, "CMDB_SCOPE_FORBIDDEN") || !strings.Contains(res.Results[2].Message, `ib-app-2" not found`) {
		t.Fatalf("scoped dry-run = %+v err=%v", res, err)
	}
}

// 同一批行在真实库上的语句数：批量读取 vs 退回逐行名称查询（仓储不具备批量能力）。后者即改写前
// 「每行一次 GetByName、每条关系再查一次对端」的路径——改写前另外还有写入后的回读。结论逐行一致。
func TestIntegration_CmdbImportBatchLookupsStatementCount(t *testing.T) {
	ctx := context.Background()
	rows := importFXRows()
	type run struct {
		results                                           *biz.ImportResult
		dryStatements, dryGetByName                       int
		statements, getByName, getByNames, relTypeLookups int
	}
	execute := func(ciRepo func(d *basedata.Data) biz.CIRepo) run {
		f := newImportFX(t)
		uc := f.usecase(ciRepo(f.d))
		f.rec.reset()
		if _, err := uc.Import(ctx, importFXActor(t), biz.Scope{Unrestricted: true}, "ib_host", rows, true); err != nil {
			t.Fatal(err)
		}
		out := run{dryStatements: f.rec.total(), dryGetByName: f.rec.count(stmtGetByName)}
		f.rec.reset()
		var err error
		if out.results, err = uc.Import(ctx, importFXActor(t), biz.Scope{Unrestricted: true}, "ib_host", rows, false); err != nil {
			t.Fatal(err)
		}
		out.statements, out.getByName, out.getByNames, out.relTypeLookups = f.rec.total(), f.rec.count(stmtGetByName), f.rec.count(stmtGetByNames), f.rec.count(stmtRelationGet)
		return out
	}
	batched := execute(func(d *basedata.Data) biz.CIRepo { return NewCIRepo(d) })
	perRow := execute(func(d *basedata.Data) biz.CIRepo { return hiddenBatchLookup{NewCIRepo(d)} })
	if a, b := batched.results, perRow.results; a.Created != b.Created || a.Updated != b.Updated || a.Skipped != b.Skipped || a.Failed != b.Failed {
		t.Fatalf("batched and per-row lookups must reach the same summary: created/updated/skipped/failed %d/%d/%d/%d vs %d/%d/%d/%d",
			a.Created, a.Updated, a.Skipped, a.Failed, b.Created, b.Updated, b.Skipped, b.Failed)
	}
	for i := range rows {
		if a, b := batched.results.Results[i], perRow.results.Results[i]; a.Action != b.Action || a.Message != b.Message {
			t.Fatalf("row %d: batched %s %q, per-row %s %q", i, a.Action, a.Message, b.Action, b.Message)
		}
	}
	// 逐行路径：每行一次 GetByName（本批新建过的同名行除外），每条走到对端解析的关系声明再查一次；
	// 批量路径：0。
	if perRow.getByName < len(rows) || batched.getByName != 0 {
		t.Fatalf("per-row name lookups: per-row path %d (at least %d), batched path %d", perRow.getByName, len(rows), batched.getByName)
	}
	if batched.getByNames < 1 || batched.getByNames > 4 {
		t.Fatalf("batched path issued %d batch name lookups; expected a constant number", batched.getByNames)
	}
	if batched.statements >= perRow.statements {
		t.Fatalf("batched path must issue fewer statements: %d vs %d", batched.statements, perRow.statements)
	}
	t.Logf("dry-run of %d rows: batched %d statements, per-row lookups %d statements (%d GetByName)",
		len(rows), batched.dryStatements, perRow.dryStatements, perRow.dryGetByName)
	t.Logf("real import of %d rows: batched %d statements (%d batch name lookups, %d GetByName, %d relation type lookups), per-row lookups %d statements (%d GetByName, %d relation type lookups)",
		len(rows), batched.statements, batched.getByNames, batched.getByName, batched.relTypeLookups, perRow.statements, perRow.getByName, perRow.relTypeLookups)
}
