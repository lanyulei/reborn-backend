//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 退役闭包（真实 PG）：一条语句取边（stmtRetireEdges）后重放的结论，必须与逐类型遍历在每一种图形状上
// 逐项一致。逐类型遍历用的是生产实现自己的退化路径（仓储不具备批量取边能力时走 RelationTypeRepo.List +
// ListRelated，即改写前的算法），通过隐藏 RelationRepo 的 RetireGraphRepo 能力来触发。
// 另外断言闭包计算的语句数不随图规模增长，以及正式 Transition 落库的状态与历史符合预期。

// stmtCounter 连接池上的语句计数（pgx QueryTracer）；edgeStmts 只数闭包取边那一条语句。
type stmtCounter struct {
	total, edgeStmts atomic.Int64
}

func (c *stmtCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	c.total.Add(1)
	if strings.Contains(data.SQL, "WITH RECURSIVE walk(id) AS (") {
		c.edgeStmts.Add(1)
	}
	return ctx
}

func (c *stmtCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (c *stmtCounter) reset() { c.total.Store(0); c.edgeStmts.Store(0) }

func (c *stmtCounter) snapshot() (total, edgeStmts int) {
	return int(c.total.Load()), int(c.edgeStmts.Load())
}

// newCountingCmdbData 同一测试库上另建一个带语句计数的连接池（连接串不落日志、不进错误信息）。
func newCountingCmdbData(tb testing.TB) (*basedata.Data, *stmtCounter) {
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
	counter := &stmtCounter{}
	cfg.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("create counting pool failed (%T)", err)
	}
	tb.Cleanup(pool.Close)
	return &basedata.Data{Pool: pool}, counter
}

// ---- 夹具 ----

// retireFXSchema 一套模型与关系类型：级联 / restrict / 通配源端 / 已删除类型；create_time 交错，
// 同一资源下各类型的处理顺序必须是 (create_time, code)——编码靠前但创建较晚的 rc_a_any / rc_p_block 排在后面。
const retireFXSchema = `INSERT INTO cmdb_model (code, name, category) VALUES
  ('rc_host', '主机', 'compute'), ('rc_vm', '虚拟机', 'compute'), ('rc_disk', '磁盘', 'storage'), ('rc_app', '应用', 'other'), ('rc_sw', '交换机', 'network');
INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete, create_time) VALUES
  ('rc_a_runs', '运行', '运行的虚拟机', '运行于', 'rc_host', 'rc_vm', 'one_to_many', 'cascade_retire', '2026-01-01 10:00:05+00'),
  ('rc_b_has', '挂载', '挂载的磁盘', '挂载于', 'rc_vm', 'rc_disk', 'many_to_many', 'cascade_retire', '2026-01-01 10:00:05+00'),
  ('rc_m_parent', '父子', '子虚拟机', '父虚拟机', 'rc_vm', 'rc_vm', 'many_to_many', 'cascade_retire', '2026-01-01 10:00:05+00'),
  ('rc_n_uses', '使用', '使用的应用', '被使用于', 'rc_vm', 'rc_app', 'many_to_many', 'restrict', '2026-01-01 10:00:05+00'),
  ('rc_uplink', '上联', '上联交换机', '下联主机', 'rc_host', 'rc_sw', 'many_to_many', 'restrict', '2026-01-01 10:00:05+00'),
  ('rc_a_block', '前置阻塞', '阻塞交换机', '阻塞主机', 'rc_host', 'rc_sw', 'many_to_many', 'restrict', '2026-01-01 10:00:05+00'),
  ('rc_a_any', '关联应用', '关联的应用', '被关联于', '*', 'rc_app', 'many_to_many', 'cascade_retire', '2026-01-01 10:00:10+00'),
  ('rc_p_block', '后置阻塞', '后置交换机', '后置主机', 'rc_host', 'rc_sw', 'many_to_many', 'restrict', '2026-01-01 10:00:10+00'),
  ('rc_old', '旧挂载', '旧磁盘', '旧挂载于', 'rc_vm', 'rc_disk', 'many_to_many', 'cascade_retire', '2026-01-01 10:00:10+00'),
  ('rc_gone', '已删除类型', 'x', 'x', 'rc_host', 'rc_vm', 'many_to_many', 'restrict', '2026-01-01 10:00:10+00');
UPDATE cmdb_relation_type SET delete_time = now() WHERE code IN ('rc_old', 'rc_gone')`

type retireFXCI struct {
	key, model, name, status, env string
	deleted                       bool
}

type retireFXEdge struct{ typ, src, dst string }

// retireFX 夹具：cis 为已插入的资源（含逻辑名），ids 为逻辑名 → 资源 ID。
type retireFX struct {
	d   *basedata.Data
	ids map[string]string
	cis []retireFXCI
}

func newRetireFX(tb testing.TB, d *basedata.Data) *retireFX {
	tb.Helper()
	if _, err := d.Pool.Exec(context.Background(), retireFXSchema); err != nil {
		tb.Fatalf("fixture schema: %v", err)
	}
	return &retireFX{d: d, ids: map[string]string{}}
}

func (f *retireFX) insertCIs(tb testing.TB, cis []retireFXCI) {
	tb.Helper()
	ids := make([]string, 0, len(cis))
	models := make([]string, 0, len(cis))
	names := make([]string, 0, len(cis))
	statuses := make([]string, 0, len(cis))
	envs := make([]string, 0, len(cis))
	deleted := make([]*time.Time, 0, len(cis))
	now := time.Now()
	for _, c := range cis {
		if c.status == "" {
			c.status = biz.CIStatusActive
		}
		if _, dup := f.ids[c.key]; dup {
			tb.Fatalf("duplicate fixture key %q", c.key)
		}
		id := mustID(tb)
		f.ids[c.key] = id
		f.cis = append(f.cis, c)
		ids, models, names = append(ids, id), append(models, c.model), append(names, c.name)
		statuses, envs = append(statuses, c.status), append(envs, c.env)
		if c.deleted {
			deleted = append(deleted, &now)
		} else {
			deleted = append(deleted, nil)
		}
	}
	if _, err := f.d.Pool.Exec(context.Background(),
		`INSERT INTO cmdb_ci (id, model_code, name, status, env, delete_time)
		 SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])`,
		dbx.TextArray(ids), dbx.TextArray(models), dbx.TextArray(names), dbx.TextArray(statuses), dbx.TextArray(envs), deleted); err != nil {
		tb.Fatalf("insert fixture cis: %v", err)
	}
}

func (f *retireFX) insertEdges(tb testing.TB, edges []retireFXEdge) {
	tb.Helper()
	ids := make([]string, 0, len(edges))
	types := make([]string, 0, len(edges))
	srcs := make([]string, 0, len(edges))
	dsts := make([]string, 0, len(edges))
	for _, e := range edges {
		src, dst := f.ids[e.src], f.ids[e.dst]
		if src == "" || dst == "" {
			tb.Fatalf("edge %s: %s -> %s references an unknown fixture ci", e.typ, e.src, e.dst)
		}
		ids, types, srcs, dsts = append(ids, mustID(tb)), append(types, e.typ), append(srcs, src), append(dsts, dst)
	}
	if _, err := f.d.Pool.Exec(context.Background(),
		`INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) SELECT * FROM unnest($1::uuid[], $2::text[], $3::uuid[], $4::uuid[])`,
		dbx.TextArray(ids), dbx.TextArray(types), dbx.TextArray(srcs), dbx.TextArray(dsts)); err != nil {
		tb.Fatalf("insert fixture relations: %v", err)
	}
}

func (f *retireFX) idsOf(keys []string) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, f.ids[k])
	}
	return out
}

// allIDs 夹具里全部资源的 ID。
func (f *retireFX) allIDs() []string {
	keys := make([]string, 0, len(f.cis))
	for _, c := range f.cis {
		keys = append(keys, c.key)
	}
	return f.idsOf(keys)
}

// buildRetireFXShapes 各种图形状各一份（互不相连的子图）：
//
//	root —— 级联 ——> va、vb、vc（活跃）、vr0（已退役：跳过且不展开）、vm-gone（已删除：不可见）
//	va —— 级联 ——> d-shared（菱形：vb 也指向它）；va —— 通配源端类型 ——> aa ——> ab ——> aa（成环）
//	vb —— 级联 ——> c01 → c02 → … → c08（深链）、dd（已删除）、d-old（关系类型已删除）
//	vc —— 级联 ——> vbar（已退役：其下的 d-under-bar 不受影响）、dx（已删除）
//
// 以及一批只用于「预期报错 / 小闭包」的根：root2（深层 restrict）、root3（已退役的 restrict 对端仍计数）、
// root4（范围外的对端）、root5 / root6（类型顺序决定哪种拒绝原因胜出）、root7（已删除的关系类型）、
// root8（150 个对端：跨页，顺序由数据库排序决定）。
func buildRetireFXShapes(tb testing.TB, d *basedata.Data) *retireFX {
	tb.Helper()
	f := newRetireFX(tb, d)
	host, vm, disk, app, sw := "rc_host", "rc_vm", "rc_disk", "rc_app", "rc_sw"
	ci := func(key, model, name, status, env string) retireFXCI {
		return retireFXCI{key: key, model: model, name: name, status: status, env: env}
	}
	f.insertCIs(tb, []retireFXCI{
		ci("root", host, "rc-root", "", "prod"), ci("vr0", vm, "vr0", biz.CIStatusRetired, "prod"), ci("d-under-r0", disk, "d-under-r0", "", "prod"),
		ci("va", vm, "va", "", "prod"), ci("vb", vm, "vb", "", "prod"), ci("vc", vm, "vc", "", "prod"),
		ci("d-shared", disk, "d-shared", "", "prod"),
		{key: "dd", model: disk, name: "dd", env: "prod", deleted: true},
		ci("aa", app, "aa", "", "prod"), ci("ab", app, "ab", "", "prod"), ci("d-old", disk, "d-old", "", "prod"),
		ci("vbar", vm, "vbar", biz.CIStatusRetired, "prod"), ci("d-under-bar", disk, "d-under-bar", "", "prod"),
		{key: "dx", model: disk, name: "dx", env: "prod", deleted: true},
		{key: "vm-gone", model: vm, name: "vm-gone", env: "prod", deleted: true},
		ci("root2", host, "rc-root2", "", "prod"), ci("e-vm", vm, "rc-e-vm", "", "prod"), ci("e-app", app, "rc-e-app", "", "prod"),
		ci("root3", host, "rc-root3", "", "prod"), ci("e-sw", sw, "rc-e-sw", biz.CIStatusRetired, "prod"),
		ci("root4", host, "rc-root4", "", "prod"), ci("o-vm", vm, "rc-o-vm", "", "dev"),
		ci("root5", host, "rc-root5", "", "prod"), ci("x-sw", sw, "rc-x-sw", "", "prod"), ci("z-vm", vm, "rc-z-vm", "", "dev"),
		ci("root6", host, "rc-root6", "", "prod"), ci("x2-sw", sw, "rc-x2-sw", "", "prod"), ci("z2-vm", vm, "rc-z2-vm", "", "dev"),
		ci("root7", host, "rc-root7", "", "prod"), ci("g-vm", vm, "rc-g-vm", "", "prod"),
		ci("root8", host, "rc-root8", "", "prod"),
	})
	chain := make([]string, 0, 8)
	for i := 1; i <= 8; i++ {
		chain = append(chain, fmt.Sprintf("c%02d", i))
	}
	chainCIs := make([]retireFXCI, 0, len(chain))
	for _, key := range chain {
		chainCIs = append(chainCIs, ci(key, vm, "rc-"+key, "", "prod"))
	}
	// 星形：名称大小写混排（同一类型内名称唯一：uq_cmdb_ci_model_name），顺序完全由数据库的排序规则决定
	// （与字节序不同），两条路径用的是同一个 ORDER BY。
	star := make([]string, 0, 150)
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("p%03d", i)
		star = append(star, key)
		chainCIs = append(chainCIs, ci(key, vm, fmt.Sprintf("rc-%c%s%03d", 'A'+rune((i*7)%26), []string{"-", "_", " "}[i%3], i), "", "prod"))
	}
	f.insertCIs(tb, chainCIs)
	edges := []retireFXEdge{
		{"rc_a_runs", "root", "va"},
		{"rc_a_runs", "root", "vb"},
		{"rc_a_runs", "root", "vc"},
		{"rc_a_runs", "root", "vr0"},
		{"rc_a_runs", "root", "vm-gone"},
		{"rc_b_has", "va", "d-shared"},
		{"rc_b_has", "vb", "d-shared"},
		{"rc_b_has", "vb", "dd"},
		{"rc_b_has", "vr0", "d-under-r0"},
		{"rc_b_has", "vc", "dx"},
		{"rc_b_has", "vbar", "d-under-bar"},
		{"rc_old", "vb", "d-old"},
		{"rc_a_any", "va", "aa"},
		{"rc_a_any", "aa", "ab"},
		{"rc_a_any", "ab", "aa"},
		{"rc_m_parent", "vb", chain[0]},
		{"rc_m_parent", "vc", "vbar"},
		{"rc_a_runs", "root2", "e-vm"},
		{"rc_n_uses", "e-vm", "e-app"},
		{"rc_uplink", "root3", "e-sw"},
		{"rc_a_runs", "root4", "o-vm"},
		{"rc_a_block", "root5", "x-sw"},
		{"rc_a_runs", "root5", "z-vm"},
		{"rc_a_runs", "root6", "z2-vm"},
		{"rc_p_block", "root6", "x2-sw"},
		{"rc_gone", "root7", "g-vm"},
	}
	for i := 1; i < len(chain); i++ {
		edges = append(edges, retireFXEdge{"rc_m_parent", chain[i-1], chain[i]})
	}
	for _, key := range star {
		edges = append(edges, retireFXEdge{"rc_a_runs", "root8", key})
	}
	f.insertEdges(tb, edges)
	return f
}

// ---- 两条路径的闭包（只算不写） ----

// recordingCIs CIRepo 包装：Update 只记录顺序不落库。嵌入的是接口类型，UpdateBatch 不会被带进来，
// updateAll 因而逐条、自后向前调用 Update——记录到的顺序即「依赖在前、根在后」。
type recordingCIs struct {
	biz.CIRepo
	updated []string
}

func (r *recordingCIs) Update(_ context.Context, ci *biz.CI, _ int64, _ *biz.History) error {
	r.updated = append(r.updated, ci.ID)
	return nil
}

// perTypeRelations 只暴露 RelationRepo：仓储「不具备」批量取边能力，退役闭包走逐类型遍历（改写前的算法）。
type perTypeRelations struct{ biz.RelationRepo }

// retireOnly 只算闭包不落库地跑一次退役：返回闭包内的资源 ID（顺序即退役顺序）与错误。
func retireOnly(ctx context.Context, d *basedata.Data, relations biz.RelationRepo, actor biz.Actor, rootID string, scope biz.Scope) ([]string, error) {
	cis := NewCIRepo(d)
	rec := &recordingCIs{CIRepo: cis}
	uc := biz.NewCIUsecase(NewModelRepo(d), NewFieldRepo(d), NewRelationTypeRepo(d), rec, relations, NewHistoryRepo(d), nil, nil)
	cur, err := cis.Get(ctx, rootID)
	if err != nil {
		return nil, err
	}
	if _, err := uc.Transition(ctx, actor, scope, rootID, cur.Version, biz.CIStatusRetired); err != nil {
		return nil, err
	}
	if n := len(rec.updated); n > 0 {
		out := rec.updated[:n-1] // 记录顺序 = 闭包倒序 + 根
		slices.Reverse(out)
		return out, nil
	}
	return nil, errors.New("retire wrote nothing")
}

func sameError(a, b error) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || (errors.Is(a, b) && errors.Is(b, a) && kerrors.FromError(a).Message == kerrors.FromError(b).Message)
}

// ciState 夹具里每个资源的「状态 / 版本 / 历史条数」，用于核对哪些资源被改动。
type ciState map[string]string

func snapshotCIs(ctx context.Context, tb testing.TB, d *basedata.Data, ids []string) ciState {
	tb.Helper()
	rows, err := d.Pool.Query(ctx,
		`SELECT c.id::text, c.status || '/' || c.version::text || '/' ||
		        (SELECT count(*) FROM cmdb_ci_history h WHERE h.ci_id = c.id)::text
		 FROM cmdb_ci c WHERE c.id = ANY($1::uuid[])`, dbx.TextArray(ids))
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	out := ciState{}
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			tb.Fatal(err)
		}
		out[id] = state
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	if len(out) != len(ids) {
		tb.Fatalf("snapshot has %d of %d cis", len(out), len(ids))
	}
	return out
}

var (
	retireUnrestricted = biz.Scope{Unrestricted: true}
	// retireHostVMProd 只可见 host / vm 且仅 prod。
	retireHostVMProd = biz.Scope{Models: map[string]struct{}{"rc_host": {}, "rc_vm": {}}, Envs: map[string]struct{}{"prod": {}}}
	// retireAllModelsProd 类型不限、仅 prod。
	retireAllModelsProd = biz.Scope{ModelUnrestricted: true, Envs: map[string]struct{}{"prod": {}}}
)

type retireScenario struct {
	name    string
	root    string
	scope   biz.Scope
	want    []string // 闭包内的资源（逻辑名，顺序即退役顺序）；预期报错时为 nil
	wantErr error
	wantMsg string
}

func TestIntegration_CmdbRetireClosureMatchesPerTypeWalk(t *testing.T) {
	newIntegrationCmdbData(t) // 清空 cmdb 表
	d, counter := newCountingCmdbData(t)
	ctx := context.Background()
	f := buildRetireFXShapes(t, d)
	allIDs := f.allIDs()
	actor := biz.Actor{UserID: mustID(t)} // 操作者须是用户 ID（uuid 列）
	byID := map[string]string{}
	for key, id := range f.ids {
		byID[id] = key
	}
	// 星形的预期顺序 = 数据库的 (name, id) 排序（取边语句与 ListRelated 用的是同一个 ORDER BY）。
	rows, err := d.Pool.Query(ctx,
		`SELECT c.id::text FROM cmdb_relation r JOIN cmdb_ci c ON c.id = r.dst_id
		 WHERE r.type_code = 'rc_a_runs' AND r.src_id = $1 AND c.delete_time IS NULL ORDER BY c.name, c.id`, f.ids["root8"])
	if err != nil {
		t.Fatal(err)
	}
	var starWant []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		starWant = append(starWant, byID[id])
	}
	rows.Close()
	if len(starWant) != 150 {
		t.Fatalf("star fixture has %d peers, want 150", len(starWant))
	}

	scenarios := []retireScenario{
		// —— 预期报错：整次退役被拒绝、不产生任何写入（先跑）——
		{"deep restrict rejects", "root2", retireUnrestricted, nil, biz.ErrCIInUse, "1 resources linked via rc_n_uses"},
		{"retired restrict peer still counts", "root3", retireUnrestricted, nil, biz.ErrCIInUse, "1 resources linked via rc_uplink"},
		{"out-of-scope peer", "root4", retireAllModelsProd, nil, biz.ErrScopeForbidden, ""},
		{"earlier restrict type wins over scope", "root5", retireAllModelsProd, nil, biz.ErrCIInUse, "1 resources linked via rc_a_block"},
		{"same root, unrestricted", "root5", retireUnrestricted, nil, biz.ErrCIInUse, "1 resources linked via rc_a_block"},
		{"earlier cascade type: scope wins", "root6", retireAllModelsProd, nil, biz.ErrScopeForbidden, ""},
		{"later restrict type still rejects", "root6", retireUnrestricted, nil, biz.ErrCIInUse, "1 resources linked via rc_p_block"},
		{"app out of scope", "root", retireHostVMProd, nil, biz.ErrScopeForbidden, ""},
		// —— 预期成功（各根的子图互不相干；后跑，会真正落库）——
		{
			"comprehensive", "root", retireUnrestricted,
			[]string{"va", "vb", "vc", "d-shared", "aa", "c01", "ab", "c02", "c03", "c04", "c05", "c06", "c07", "c08"},
			nil, "",
		},
		{"in scope", "root4", retireUnrestricted, []string{"o-vm"}, nil, ""},
		{"deleted relation type is ignored", "root7", retireUnrestricted, []string{}, nil, ""},
		{"star of 150 keeps db order", "root8", retireUnrestricted, starWant, nil, ""},
	}
	// base 按场景类别（报错 / 成功）记住一条语句取边的路径首次得到的语句数：同类场景之后的语句数必须相同。
	base := map[bool]int{}
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			rootID := f.ids[s.root]
			// 逐类型遍历（改写前的算法）与一条语句取边后的重放，各自只算闭包。
			counter.reset()
			oldDeps, oldErr := retireOnly(ctx, d, perTypeRelations{NewRelationRepo(d)}, actor, rootID, s.scope)
			oldTotal, _ := counter.snapshot()
			counter.reset()
			newDeps, newErr := retireOnly(ctx, d, NewRelationRepo(d), actor, rootID, s.scope)
			newTotal, edgeStmts := counter.snapshot()
			if !sameError(oldErr, newErr) || !slices.Equal(oldDeps, newDeps) {
				t.Fatalf("batched path diverges from the per-type walk\n per-type: %v err=%v\n batched:  %v err=%v", oldDeps, oldErr, newDeps, newErr)
			}
			var gotErr error
			if s.wantErr != nil {
				if newErr == nil || !errors.Is(newErr, s.wantErr) {
					t.Fatalf("error = %v, want %v", newErr, s.wantErr)
				}
				if s.wantMsg != "" && kerrors.FromError(newErr).Message != s.wantMsg {
					t.Fatalf("error message = %q, want %q", kerrors.FromError(newErr).Message, s.wantMsg)
				}
				gotErr = newErr
			} else {
				if newErr != nil {
					t.Fatalf("unexpected error: %v", newErr)
				}
				if want := f.idsOf(s.want); !slices.Equal(newDeps, want) {
					var gotKeys []string
					for _, id := range newDeps {
						gotKeys = append(gotKeys, byID[id])
					}
					t.Fatalf("closure = %v, want %v", gotKeys, s.want)
				}
			}
			// 语句数：取边恰好一条；整次闭包计算的语句数与图规模无关（同类场景的语句数相同——成功场景比
			// 报错场景多末尾的回读与脱敏），逐类型遍历则随「资源 × 适用类型」增长。
			if edgeStmts != 1 {
				t.Fatalf("batched path issued %d edge statements, want 1", edgeStmts)
			}
			if rejected := gotErr != nil; base[rejected] == 0 {
				base[rejected] = newTotal
			} else if newTotal != base[rejected] {
				t.Fatalf("batched path took %d statements here but %d in an earlier scenario of the same kind: the count must not depend on the closure", newTotal, base[rejected])
			}
			if n := len(newDeps); n >= 150 && oldTotal <= n {
				t.Fatalf("per-type walk took %d statements for a %d-node closure; it is expected to scale with nodes", oldTotal, n)
			}
			t.Logf("closure=%d: per-type walk %d statements, batched path %d statements", len(newDeps), oldTotal, newTotal)

			// 正式退役（真实仓储：一条语句取边 → 同事务批量更新 + 历史）。
			before := snapshotCIs(ctx, t, d, allIDs)
			cis, rels := NewCIRepo(d), NewRelationRepo(d)
			uc := biz.NewCIUsecase(NewModelRepo(d), NewFieldRepo(d), NewRelationTypeRepo(d), cis, rels, NewHistoryRepo(d), nil, nil)
			cur, err := cis.Get(ctx, rootID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = uc.Transition(ctx, actor, s.scope, rootID, cur.Version, biz.CIStatusRetired)
			if !sameError(err, gotErr) {
				t.Fatalf("Transition error = %v, closure error = %v", err, gotErr)
			}
			after := snapshotCIs(ctx, t, d, allIDs)
			changed := make([]string, 0, len(newDeps)+1)
			for id, state := range after {
				if before[id] != state {
					changed = append(changed, byID[id])
				}
			}
			if gotErr != nil {
				if len(changed) != 0 {
					t.Fatalf("a rejected retire must not change anything, changed: %v", changed)
				}
				return
			}
			slices.Sort(changed)
			wantChanged := append(slices.Clone(s.want), s.root)
			slices.Sort(wantChanged)
			if !slices.Equal(changed, wantChanged) {
				t.Fatalf("changed cis = %v, want %v", changed, wantChanged)
			}
			for _, id := range append(slices.Clone(newDeps), rootID) {
				if state := after[id]; state != biz.CIStatusRetired+"/2/1" {
					t.Fatalf("%s must be retired with version 2 and one transition history, got %q", byID[id], state)
				}
			}
		})
	}
}

// seedRetireStar 基准图（n 须为偶数，共 n 个资源、n-1 条边）：根 → n/2 台虚拟机（级联），前 n/2-1 台虚拟机
// 各 → 1 块磁盘（级联）。虚拟机作为源端另适用 3 个没有边的关系类型（含 1 个 restrict），磁盘另适用通配源端的 1 个。
func seedRetireStar(tb testing.TB, d *basedata.Data, n int) (rootID string, want int) {
	tb.Helper()
	f := newRetireFX(tb, d)
	host, vm, disk := "rc_host", "rc_vm", "rc_disk"
	cis := []retireFXCI{{key: "root", model: host, name: "bench-root", env: "prod"}}
	edges := make([]retireFXEdge, 0, n-1)
	for i := 0; i < n/2; i++ {
		vmKey := fmt.Sprintf("vm%04d", i)
		cis = append(cis, retireFXCI{key: vmKey, model: vm, name: "bench-" + vmKey, env: "prod"})
		edges = append(edges, retireFXEdge{"rc_a_runs", "root", vmKey})
		if i < n/2-1 {
			diskKey := fmt.Sprintf("disk%04d", i)
			cis = append(cis, retireFXCI{key: diskKey, model: disk, name: "bench-" + diskKey, env: "prod"})
			edges = append(edges, retireFXEdge{"rc_b_has", vmKey, diskKey})
		}
	}
	f.insertCIs(tb, cis)
	f.insertEdges(tb, edges)
	return f.ids["root"], n - 1
}

// 基准（真实 PG，固定次数运行）：同一张图上比较逐类型遍历（改写前的算法）与一条语句取边后重放的耗时与
// 语句数。只算闭包不落库，所以可重复。不断言性能阈值。
func BenchmarkIntegration_CmdbRetireClosure(b *testing.B) {
	for _, n := range []int{100, 1000} {
		newIntegrationCmdbData(b) // 清空 cmdb 表
		d, counter := newCountingCmdbData(b)
		rootID, want := seedRetireStar(b, d, n)
		for _, path := range []string{"per-type", "batched"} {
			b.Run(fmt.Sprintf("nodes=%d/%s", n, path), func(b *testing.B) {
				ctx := context.Background()
				rels := biz.RelationRepo(NewRelationRepo(d))
				if path == "per-type" {
					rels = perTypeRelations{NewRelationRepo(d)}
				}
				cis := NewCIRepo(d)
				rec := &recordingCIs{CIRepo: cis}
				uc := biz.NewCIUsecase(NewModelRepo(d), NewFieldRepo(d), NewRelationTypeRepo(d), rec, rels, NewHistoryRepo(d), nil, nil)
				cur, err := cis.Get(ctx, rootID)
				if err != nil {
					b.Fatal(err)
				}
				actor := biz.Actor{UserID: mustID(b)}
				counter.reset() // 只数计时区间内的语句
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rec.updated = nil
					if _, err := uc.Transition(ctx, actor, retireUnrestricted, rootID, cur.Version, biz.CIStatusRetired); err != nil {
						b.Fatal(err)
					}
					if len(rec.updated) != want+1 {
						b.Fatalf("closure = %d, want %d (fixture clobbered by a concurrent test?)", len(rec.updated)-1, want)
					}
				}
				b.StopTimer()
				total, edgeStmts := counter.snapshot()
				b.ReportMetric(float64(total)/float64(b.N), "stmts/op")
				b.ReportMetric(float64(edgeStmts)/float64(b.N), "edgestmts/op")
			})
		}
	}
}
