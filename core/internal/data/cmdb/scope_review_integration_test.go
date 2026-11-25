//go:build integration

package data

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

// 按数据范围过滤的关联统计 / 拓扑 / 概览关联数（真实 SQL）：范围外的对端不计数、不占节点预算。
func TestIntegration_CmdbScopedRelationQueries(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	rels, overview := NewRelationRepo(d), NewOverviewRepo(d)
	for _, q := range []string{
		`INSERT INTO cmdb_model (code, name, category) VALUES ('sr_host', '主机', 'compute'), ('sr_sw', '交换机', 'network')`,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality) VALUES ('sr_uplink', '上联', '上联交换机', '下联主机', 'sr_host', 'sr_sw', 'many_to_many')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	host, swProd, swDev := mustID(t), mustID(t), mustID(t)
	for _, row := range [][]any{
		{host, "sr_host", "h1", "prod"},
		{swProd, "sr_sw", "sw-b-prod", "prod"},
		{swDev, "sr_sw", "sw-a-dev", "dev"}, // 名称排在前：不按范围过滤时先占预算
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name, env) VALUES ($1, $2, $3, $4)", row...); err != nil {
			t.Fatal(err)
		}
	}
	for _, dst := range []string{swProd, swDev} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) VALUES ($1, 'sr_uplink', $2, $3)", mustID(t), host, dst); err != nil {
			t.Fatal(err)
		}
	}
	out := biz.RelationKey{TypeCode: "sr_uplink", Direction: biz.DirectionOutgoing}
	in := biz.RelationKey{TypeCode: "sr_uplink", Direction: biz.DirectionIncoming}

	if counts, err := rels.CountByCI(ctx, host); err != nil || counts[out] != 2 {
		t.Fatalf("CountByCI = %v err=%v", counts, err)
	}
	for _, c := range []struct {
		name         string
		ciID         string
		models, envs []string
		key          biz.RelationKey
		want         int64
	}{
		{"model + env", host, []string{"sr_sw"}, []string{"prod"}, out, 1},
		{"env only", host, nil, []string{"dev"}, out, 1},
		{"model only", host, []string{"sr_sw"}, nil, out, 2},
		{"both unrestricted dims", host, nil, nil, out, 2},
		{"model out of scope", host, []string{"sr_host"}, nil, out, 0},
		{"empty set sees nothing", host, []string{}, nil, out, 0},
		{"incoming side", swProd, []string{"sr_host"}, []string{"prod"}, in, 1},
		{"incoming, peer env out of scope", swProd, nil, []string{"dev"}, in, 0},
	} {
		counts, err := rels.CountByCIIn(ctx, c.ciID, c.models, c.envs)
		if err != nil || counts[c.key] != c.want {
			t.Errorf("%s: CountByCIIn = %v err=%v, want %d", c.name, counts, err, c.want)
		}
	}

	// 拓扑：预算 2（根 + 1 个邻居）。不按范围过滤时 dev 交换机先占预算并截断；按范围过滤后
	// 预算留给 prod 交换机，且没有截断。
	nodes, _, truncated, err := rels.Neighbors(ctx, host, "", 1, 2)
	if err != nil || len(nodes) != 2 || nodes[1].ID != swDev || !truncated {
		t.Fatalf("unscoped neighbors = %+v trunc=%v err=%v", nodes, truncated, err)
	}
	nodes, edges, truncated, err := rels.NeighborsIn(ctx, host, "", 1, 2, nil, []string{"prod"})
	if err != nil || len(nodes) != 2 || nodes[0].ID != host || nodes[1].ID != swProd || len(edges) != 1 || truncated {
		t.Fatalf("scoped neighbors = %+v edges=%d trunc=%v err=%v", nodes, len(edges), truncated, err)
	}
	// 根节点不受范围限制（调用方已校验）；范围内没有邻居时只剩根。
	nodes, edges, truncated, err = rels.NeighborsIn(ctx, host, "", 2, 10, []string{"sr_host"}, []string{"dev"})
	if err != nil || len(nodes) != 1 || len(edges) != 0 || truncated {
		t.Fatalf("no in-scope neighbors = %+v edges=%d trunc=%v err=%v", nodes, len(edges), truncated, err)
	}

	// 概览关联数：受限时两端都须在范围内；不限时与原查询一致。
	for _, c := range []struct {
		name   string
		filter biz.OverviewFilter
		want   int64
	}{
		{"unrestricted", biz.OverviewFilter{}, 2},
		{"prod only", biz.OverviewFilter{Envs: []string{"prod"}}, 1},
		{"host only: every edge points out of scope", biz.OverviewFilter{ModelCodes: []string{"sr_host"}}, 0},
		{"both models, dev only: src is out of scope", biz.OverviewFilter{ModelCodes: []string{"sr_host", "sr_sw"}, Envs: []string{"dev"}}, 0},
	} {
		c.filter.Since = time.Now().AddDate(0, 0, -7)
		stats, err := overview.Stats(ctx, c.filter)
		if err != nil || stats.RelationCount != c.want {
			t.Errorf("%s: RelationCount = %+v err=%v, want %d", c.name, stats, err, c.want)
		}
	}
}

// 冲突清单按可见类型过滤（清单、total、封顶判定同口径）。
func TestIntegration_CmdbConflictScope(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	runs := NewSyncRunRepo(d)
	sourceID := mustID(t)
	source := cmdbcontract.SourceOf(sourceID)
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_model (code, name, category) VALUES ('cs_host', '主机', 'compute'), ('cs_vm', '虚拟机', 'compute')`); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]any{
		{source, "cs_host", "e-1", "host-1"},
		{source, "cs_vm", "e-2", "vm-1"},
		{source, "cs_vm", "e-3", "vm-2"},
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_sync_conflict (source, model_code, external_id, name, reason) VALUES ($1, $2, $3, $4, 'name taken')", row...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_sync_conflict_stat (source, model_code, total) VALUES ($1, 'cs_host', 1), ($1, 'cs_vm', $2)", source, cmdbcontract.ConflictStoreLimit+1); err != nil {
		t.Fatal(err)
	}
	names := func(items []*biz.SyncConflict) []string {
		out := make([]string, 0, len(items))
		for _, c := range items {
			out = append(out, c.Name)
		}
		slices.Sort(out)
		return out
	}
	if items, total, err := runs.ListConflicts(ctx, sourceID, 1, 10); err != nil || total != 3 || len(items) != 3 {
		t.Fatalf("ListConflicts = %d total=%d err=%v", len(items), total, err)
	}
	items, total, err := runs.ListConflictsIn(ctx, sourceID, []string{"cs_host"}, 1, 10)
	if err != nil || total != 1 || !slices.Equal(names(items), []string{"host-1"}) || items[0].ModelName != "主机" {
		t.Fatalf("ListConflictsIn(host) = %v total=%d err=%v", names(items), total, err)
	}
	// 分页的 LIMIT / OFFSET 占位符在范围参数之后。
	items, total, err = runs.ListConflictsIn(ctx, sourceID, []string{"cs_vm"}, 2, 1)
	if err != nil || total != 2 || len(items) != 1 {
		t.Fatalf("ListConflictsIn(vm, page 2) = %v total=%d err=%v", names(items), total, err)
	}
	if items, total, err := runs.ListConflictsIn(ctx, sourceID, []string{}, 1, 10); err != nil || total != 0 || len(items) != 0 {
		t.Fatalf("empty model set must see nothing: %d total=%d err=%v", len(items), total, err)
	}
	if capped, err := runs.ConflictsCapped(ctx, sourceID); err != nil || !capped {
		t.Fatalf("ConflictsCapped = %v err=%v", capped, err)
	}
	if capped, err := runs.ConflictsCappedIn(ctx, sourceID, []string{"cs_host"}); err != nil || capped {
		t.Fatalf("ConflictsCappedIn(host) = %v err=%v", capped, err)
	}
	if capped, err := runs.ConflictsCappedIn(ctx, sourceID, []string{"cs_vm"}); err != nil || !capped {
		t.Fatalf("ConflictsCappedIn(vm) = %v err=%v", capped, err)
	}
}

// 采集源更新不回写调用方读到的 next_run_time：调度器推进的值保留；启用且为空时置为库内 now()；
// 缩短周期收敛到 now() + 新周期，放宽周期不动；乐观锁照旧。
func TestIntegration_CmdbSourceUpdateKeepsSchedule(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	sources := NewSyncSourceRepo(d)
	now := time.Now()
	src := &biz.SyncSource{
		ID: mustID(t), Name: "it-sched", Provider: "aliyun", Config: []byte(`{"region_id":"r","access_key_id":"k"}`), Env: "prod",
		IntervalSeconds: 7 * 86400, GracePeriodSeconds: 0, Enabled: false, LastStatus: "never", Version: 1, CreateTime: now, UpdateTime: now,
	}
	if err := sources.Create(ctx, src, []byte("ct")); err != nil {
		t.Fatalf("create source: %v", err)
	}
	// secondsUntilNext 库内时钟下距计划时间的秒数；ok = false 表示计划时间为空。
	secondsUntilNext := func() (float64, bool) {
		t.Helper()
		var secs *float64
		if err := d.Pool.QueryRow(ctx, "SELECT extract(epoch FROM next_run_time - now())::float8 FROM cmdb_sync_source WHERE id = $1", src.ID).Scan(&secs); err != nil {
			t.Fatal(err)
		}
		if secs == nil {
			return 0, false
		}
		return *secs, true
	}
	update := func(mutate func(*biz.SyncSource)) {
		t.Helper()
		cur, err := sources.Get(ctx, src.ID)
		if err != nil {
			t.Fatal(err)
		}
		version := cur.Version
		mutate(cur)
		cur.UpdateTime = time.Now()
		if err := sources.Update(ctx, cur, version, nil); err != nil {
			t.Fatalf("update source: %v", err)
		}
	}

	// 停用状态下改名：仍无计划时间。
	update(func(s *biz.SyncSource) { s.Name = "it-sched-2" })
	if _, ok := secondsUntilNext(); ok {
		t.Fatal("a disabled source must stay unscheduled")
	}
	// 启用：计划时间置为库内 now()（调用方给的值不参与）。
	update(func(s *biz.SyncSource) {
		far := time.Now().Add(30 * 24 * time.Hour)
		s.Enabled, s.NextRunTime = true, &far
	})
	if secs, ok := secondsUntilNext(); !ok || secs > 5 || secs < -60 {
		t.Fatalf("enabling must schedule the source now, got %v (set=%v)", secs, ok)
	}
	// 调度器把计划时间推进到 7 天后（不升 version）；随后用旧读数保存配置，不得把它拉回去。
	stale, err := sources.Get(ctx, src.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET next_run_time = now() + interval '7 days' WHERE id = $1", src.ID); err != nil {
		t.Fatal(err)
	}
	stale.Name, stale.UpdateTime = "it-sched-3", time.Now()
	if err := sources.Update(ctx, stale, stale.Version, nil); err != nil {
		t.Fatalf("update with a stale next_run_time: %v", err)
	}
	if secs, ok := secondsUntilNext(); !ok || secs < 6.9*86400 {
		t.Fatalf("the scheduler's next_run_time must be kept, got %v", secs)
	}
	// 乐观锁照旧：旧版本被拒。
	if err := sources.Update(ctx, stale, stale.Version, nil); !errors.Is(err, biz.ErrSourceVersionConflict) {
		t.Fatalf("stale version error = %v", err)
	}
	// 缩短周期到 1 小时：计划时间收敛到 1 小时内。
	update(func(s *biz.SyncSource) { s.IntervalSeconds = 3600 })
	if secs, ok := secondsUntilNext(); !ok || secs > 3605 || secs < 3500 {
		t.Fatalf("shortening the interval must pull next_run_time in, got %v", secs)
	}
	// 放宽周期：不动原有计划。
	update(func(s *biz.SyncSource) { s.IntervalSeconds = 86400 })
	if secs, ok := secondsUntilNext(); !ok || secs > 3605 {
		t.Fatalf("widening the interval must not push next_run_time out, got %v", secs)
	}
	// 停用再启用：已有计划时间保留（不重置为现在）。
	update(func(s *biz.SyncSource) { s.Enabled = false })
	update(func(s *biz.SyncSource) { s.Enabled = true })
	if secs, ok := secondsUntilNext(); !ok || secs < 3000 {
		t.Fatalf("re-enabling must keep an existing next_run_time, got %v", secs)
	}
}

// 删除类型的前置检查查询：回收站资源数、其它类型指向它的未废弃 ref 字段。
func TestIntegration_CmdbModelDeleteGuard(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	models := NewModelRepo(d)
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_model (code, name, category) VALUES ('md_rack', '机柜', 'compute'), ('md_host', '主机', 'compute'), ('md_gone', '已删', 'compute')`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_model SET delete_time = now() WHERE code = 'md_gone'"); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]any{
		{mustID(t), "md_host", "rack", "ref", "md_rack", false},
		{mustID(t), "md_host", "old_rack", "ref", "md_rack", true}, // 已废弃：不算引用
		{mustID(t), "md_rack", "parent", "ref", "md_rack", false},  // 自引用：不阻塞删除自己
		{mustID(t), "md_gone", "rack", "ref", "md_rack", false},    // 所属类型已删除：不算引用
		{mustID(t), "md_host", "note", "string", "", false},
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_model_field (id, model_code, code, name, type, ref_model, deprecated) VALUES ($1, $2, $3, $3, $4, $5, $6)", row...); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := models.RefFields(ctx, "md_rack")
	if err != nil || !slices.Equal(refs, []string{"md_host.rack"}) {
		t.Fatalf("RefFields(md_rack) = %v err=%v", refs, err)
	}
	if refs, err := models.RefFields(ctx, "md_host"); err != nil || len(refs) != 0 {
		t.Fatalf("RefFields(md_host) = %v err=%v", refs, err)
	}

	live, gone := mustID(t), mustID(t)
	for _, row := range [][]any{{live, "md_rack", "r-live"}, {gone, "md_rack", "r-gone"}} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name) VALUES ($1, $2, $3)", row...); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := models.DeletedCICount(ctx, "md_rack"); err != nil || n != 0 {
		t.Fatalf("DeletedCICount before delete = %d err=%v", n, err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_ci SET delete_time = now() WHERE id = $1", gone); err != nil {
		t.Fatal(err)
	}
	if n, err := models.DeletedCICount(ctx, "md_rack"); err != nil || n != 1 {
		t.Fatalf("DeletedCICount = %d err=%v", n, err)
	}
	// 类型查询列序未变：未删除资源数照旧。
	if m, err := models.Get(ctx, "md_rack"); err != nil || m.CICount != 1 {
		t.Fatalf("Get(md_rack) = %+v err=%v", m, err)
	}
}
