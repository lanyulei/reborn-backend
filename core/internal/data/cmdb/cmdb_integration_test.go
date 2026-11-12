//go:build integration

package data

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newIntegrationCmdbData 连接 TEST_DATABASE_SOURCE（库名须含 test）并清空 cmdb 表。
func newIntegrationCmdbData(t *testing.T) *basedata.Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !containsTest(db) {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &conf.Data_Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, "TRUNCATE TABLE cmdb_sync_staging, cmdb_sync_run, cmdb_sync_task, cmdb_sync_source, cmdb_relation, cmdb_ci_history, cmdb_ci, cmdb_model_field, cmdb_relation_type, cmdb_model_view, cmdb_role_scope, cmdb_model CASCADE"); err != nil {
		t.Fatalf("truncate cmdb tables: %v", err)
	}
	return d
}

func containsTest(s string) bool {
	for i := 0; i+4 <= len(s); i++ {
		if s[i:i+4] == "test" {
			return true
		}
	}
	return false
}

func mustID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func hist(t *testing.T, ciID, action string) *biz.History {
	t.Helper()
	return &biz.History{ID: mustID(t), CIID: ciID, Action: action, Diff: map[string]any{"after": map[string]any{"tags": []string{"x"}}}, Operator: "tester", Source: "manual", CreateTime: time.Now()}
}

// 真实 PG 上串联六个仓储：建表语义（唯一键、外键、乐观锁、jsonb/text[]）与 SQL 正确性。
func TestIntegration_CmdbRepos(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	models, fields, relTypes := NewModelRepo(d), NewFieldRepo(d), NewRelationTypeRepo(d)
	cis, rels, hists := NewCIRepo(d), NewRelationRepo(d), NewHistoryRepo(d)
	now := time.Now()

	// 模型与字段。
	for _, code := range []string{"it_host", "it_part"} {
		if err := models.Create(ctx, &biz.Model{Code: code, Name: code, Category: "compute", Status: 1, CreateTime: now, UpdateTime: now}); err != nil {
			t.Fatalf("create model: %v", err)
		}
	}
	if err := models.Create(ctx, &biz.Model{Code: "it_host", Name: "dup", Category: "compute", Status: 1, CreateTime: now, UpdateTime: now}); !errors.Is(err, biz.ErrModelCodeExists) {
		t.Fatalf("duplicate model error = %v", err)
	}
	maxLen := int32(64)
	ipField := &biz.Field{ID: mustID(t), ModelCode: "it_host", Code: "ip", Name: "IP", Type: biz.FieldTypeIP, Required: true, Unique: true, Owner: "manual", ListVisible: true, Sort: 1, CreateTime: now, UpdateTime: now}
	kindField := &biz.Field{ID: mustID(t), ModelCode: "it_part", Code: "kind", Name: "Kind", Type: biz.FieldTypeEnum, EnumValues: []string{"a", "b"}, Rule: biz.FieldRule{MaxLen: &maxLen}, Owner: "manual", CreateTime: now, UpdateTime: now}
	for _, f := range []*biz.Field{ipField, kindField} {
		if err := fields.Create(ctx, f); err != nil {
			t.Fatalf("create field: %v", err)
		}
	}
	got, err := fields.Get(ctx, "it_part", kindField.ID)
	if err != nil || len(got.EnumValues) != 2 || got.Rule.MaxLen == nil || *got.Rule.MaxLen != 64 {
		t.Fatalf("field jsonb roundtrip: %+v err=%v", got, err)
	}
	if err := fields.Create(ctx, &biz.Field{ID: mustID(t), ModelCode: "it_host", Code: "ip", Name: "x", Type: "string", Owner: "manual", CreateTime: now, UpdateTime: now}); !errors.Is(err, biz.ErrFieldCodeExists) {
		t.Fatalf("duplicate field error = %v", err)
	}
	if err := relTypes.Create(ctx, &biz.RelationType{Code: "it_has_part", Name: "n", ForwardName: "f", BackwardName: "b", SrcModel: "it_host", DstModel: "it_part", Cardinality: biz.CardinalityOneToMany, OnDelete: biz.OnDeleteRestrict, CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatalf("create relation type: %v", err)
	}
	applicable, err := relTypes.List(ctx, "it_part")
	if err != nil || len(applicable) != 1 {
		t.Fatalf("applicable relation types = %d err=%v", len(applicable), err)
	}

	// 资源：jsonb attrs、text[] tags、唯一键、乐观锁。
	host := &biz.CI{ID: mustID(t), ModelCode: "it_host", Name: "h1", Status: "active", Env: "prod", Tags: []string{"web", "db"}, Source: "manual", UniqueKey: "ip=10.0.0.1", Attrs: map[string]any{"ip": "10.0.0.1", "cores": int64(8)}, Version: 1, CreateUser: mustID(t), CreateTime: now, UpdateTime: now}
	if err := cis.Create(ctx, host, hist(t, host.ID, "create")); err != nil {
		t.Fatalf("create ci: %v", err)
	}
	dup := *host
	dup.ID, dup.Name = mustID(t), "h2"
	if err := cis.Create(ctx, &dup, hist(t, dup.ID, "create")); !errors.Is(err, biz.ErrCIDuplicate) {
		t.Fatalf("duplicate unique key error = %v", err)
	}
	part := &biz.CI{ID: mustID(t), ModelCode: "it_part", Name: "p1", Status: "active", Tags: []string{}, Source: "manual", Attrs: map[string]any{"kind": "a"}, Version: 1, CreateTime: now, UpdateTime: now}
	if err := cis.Create(ctx, part, hist(t, part.ID, "create")); err != nil {
		t.Fatalf("create part: %v", err)
	}
	loaded, err := cis.Get(ctx, host.ID)
	if err != nil || loaded.ModelName != "it_host" || loaded.Attrs["cores"] != float64(8) || len(loaded.Tags) != 2 {
		t.Fatalf("get ci = %+v err=%v", loaded, err)
	}
	items, total, err := cis.List(ctx, biz.CIFilter{ModelCode: "it_host", Tag: "web", Attrs: map[string]any{"ip": "10.0.0.1"}}, 1, 10)
	if err != nil || total != 1 || items[0].ID != host.ID {
		t.Fatalf("list by tag+attrs: total=%d err=%v", total, err)
	}
	if _, total, _ := cis.List(ctx, biz.CIFilter{Keyword: "p"}, 1, 10); total != 1 {
		t.Fatalf("list by keyword total=%d", total)
	}
	updated := *loaded
	updated.Version, updated.Owner, updated.Attrs = 2, "ops", map[string]any{"ip": "10.0.0.1", "cores": int64(16)}
	if err := cis.Update(ctx, &updated, 1, hist(t, host.ID, "update")); err != nil {
		t.Fatalf("update ci: %v", err)
	}
	if err := cis.Update(ctx, &updated, 1, hist(t, host.ID, "update")); !errors.Is(err, biz.ErrCIVersionConflict) {
		t.Fatalf("stale version error = %v", err)
	}
	if exists, _ := cis.ExistsInModel(ctx, "it_part", part.ID); !exists {
		t.Fatal("ExistsInModel should be true")
	}

	// 关系：建立 / 统计 / 关联列表 / 迁移 / 解除；外键保护软删。
	rel := &biz.Relation{ID: mustID(t), TypeCode: "it_has_part", SrcID: host.ID, DstID: part.ID, Source: "manual", CreateTime: now}
	if err := rels.Create(ctx, rel, []*biz.History{hist(t, host.ID, "link"), hist(t, part.ID, "link")}); err != nil {
		t.Fatalf("create relation: %v", err)
	}
	if err := rels.Create(ctx, &biz.Relation{ID: mustID(t), TypeCode: "it_has_part", SrcID: host.ID, DstID: part.ID, CreateTime: now}, nil); !errors.Is(err, biz.ErrRelationExists) {
		t.Fatalf("duplicate relation error = %v", err)
	}
	counts, err := rels.CountByCI(ctx, part.ID)
	if err != nil || counts[biz.RelationKey{TypeCode: "it_has_part", Direction: biz.DirectionIncoming}] != 1 {
		t.Fatalf("count by ci = %v err=%v", counts, err)
	}
	if n, _ := rels.CountSide(ctx, "it_has_part", biz.DirectionIncoming, part.ID); n != 1 {
		t.Fatalf("count side = %d", n)
	}
	related, total, err := rels.ListRelated(ctx, host.ID, "it_has_part", biz.DirectionOutgoing, "p", 1, 10)
	if err != nil || total != 1 || related[0].CI.ID != part.ID || related[0].CI.Attrs["kind"] != "a" {
		t.Fatalf("list related = %+v total=%d err=%v", related, total, err)
	}
	host2 := &biz.CI{ID: mustID(t), ModelCode: "it_host", Name: "h2", Status: "active", Tags: []string{}, Source: "manual", UniqueKey: "ip=10.0.0.2", Attrs: map[string]any{"ip": "10.0.0.2"}, Version: 1, CreateTime: now, UpdateTime: now}
	if err := cis.Create(ctx, host2, hist(t, host2.ID, "create")); err != nil {
		t.Fatal(err)
	}
	next := &biz.Relation{ID: mustID(t), TypeCode: "it_has_part", SrcID: host2.ID, DstID: part.ID, Source: "manual", CreateTime: now}
	if err := rels.Move(ctx, "it_has_part", rel, next, []*biz.History{hist(t, part.ID, "unlink"), hist(t, part.ID, "link")}); err != nil {
		t.Fatalf("move: %v", err)
	}
	if _, total, _ := rels.ListRelated(ctx, host2.ID, "it_has_part", biz.DirectionOutgoing, "", 1, 10); total != 1 {
		t.Fatalf("after move total = %d", total)
	}
	if err := rels.Delete(ctx, "it_has_part", host.ID, part.ID, nil); !errors.Is(err, biz.ErrRelationNotFound) {
		t.Fatalf("delete moved relation error = %v", err)
	}
	if err := rels.Delete(ctx, "it_has_part", host2.ID, part.ID, []*biz.History{hist(t, part.ID, "unlink")}); err != nil {
		t.Fatalf("delete relation: %v", err)
	}

	// 历史：倒序分页，diff jsonb 往返。
	items2, total, err := hists.ListByCI(ctx, part.ID, 1, 2)
	if err != nil || total != 5 || len(items2) != 2 || items2[0].Action != "unlink" {
		t.Fatalf("history = %+v total=%d err=%v", items2, total, err)
	}
	if raw, _ := json.Marshal(items2[0].Diff); len(raw) == 0 {
		t.Fatal("diff must roundtrip")
	}

	// 软删与计数。
	if err := cis.SoftDelete(ctx, part.ID, time.Now(), hist(t, part.ID, "delete")); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := cis.Get(ctx, part.ID); !errors.Is(err, biz.ErrCINotFound) {
		t.Fatalf("deleted ci get error = %v", err)
	}
	m, err := models.Get(ctx, "it_host")
	if err != nil || m.CICount != 2 {
		t.Fatalf("model ci count = %d err=%v", m.CICount, err)
	}
	rt, err := relTypes.Get(ctx, "it_has_part")
	if err != nil || rt.RelationCount != 0 {
		t.Fatalf("relation count = %d err=%v", rt.RelationCount, err)
	}
	if err := relTypes.SoftDelete(ctx, "it_has_part", time.Now()); err != nil {
		t.Fatalf("soft delete relation type: %v", err)
	}
	if err := fields.Deprecate(ctx, "it_host", ipField.ID); err != nil {
		t.Fatalf("deprecate field: %v", err)
	}
	if err := models.Update(ctx, "it_part", biz.ModelUpdate{Status: ptr(int32(2))}); err != nil {
		t.Fatalf("update model: %v", err)
	}
	if err := models.SoftDelete(ctx, "it_part", time.Now()); err != nil {
		t.Fatalf("soft delete model: %v", err)
	}

	// 布局与范围仓储：upsert 幂等、jsonb/text[] 往返、未配置返回 nil。
	views, scopes := NewViewRepo(d), NewScopeRepo(d)
	if v, err := views.Get(ctx, "it_host"); err != nil || v != nil {
		t.Fatalf("unconfigured view = %v err=%v", v, err)
	}
	sections := []biz.ViewSection{{Type: "attrs", Title: "基本", Fields: []string{"ip"}}, {Type: "relation", RelationType: "it_has_part", Direction: "outgoing", Columns: []string{"kind"}, Collapsed: true}}
	for i := 0; i < 2; i++ {
		if err := views.Upsert(ctx, &biz.ModelView{ModelCode: "it_host", Sections: sections, UpdateUser: mustID(t), UpdateTime: time.Now()}); err != nil {
			t.Fatalf("upsert view: %v", err)
		}
	}
	if v, err := views.Get(ctx, "it_host"); err != nil || len(v.Sections) != 2 || !v.Sections[1].Collapsed || v.Sections[0].Title != "基本" {
		t.Fatalf("view roundtrip = %+v err=%v", v, err)
	}
	if err := views.Upsert(ctx, &biz.ModelView{ModelCode: "it_missing", Sections: nil, UpdateTime: time.Now()}); !errors.Is(err, biz.ErrModelNotFound) {
		t.Fatalf("view for missing model error = %v", err)
	}
	roleID := mustID(t)
	for i := 0; i < 2; i++ {
		if err := scopes.Upsert(ctx, &biz.RoleScope{RoleID: roleID, ModelCodes: []string{"it_host"}, Envs: []string{"prod", "test"}, UpdateTime: time.Now()}); err != nil {
			t.Fatalf("upsert scope: %v", err)
		}
	}
	byRole, err := scopes.ListByRoles(ctx, []string{roleID, mustID(t)})
	if err != nil || len(byRole) != 1 || len(byRole[0].Envs) != 2 || byRole[0].ModelCodes[0] != "it_host" {
		t.Fatalf("scopes by role = %+v err=%v", byRole, err)
	}
	if all, err := scopes.ListAll(ctx); err != nil || len(all) != 1 {
		t.Fatalf("all scopes = %d err=%v", len(all), err)
	}
	if none, err := scopes.ListByRoles(ctx, nil); err != nil || len(none) != 0 {
		t.Fatalf("empty role list = %v err=%v", none, err)
	}
}

func ptr[T any](v T) *T { return &v }

// 同步控制面仓储：唯一名、凭据只写、乐观锁、未完成任务唯一、运行记录倒序。
func TestIntegration_CmdbSyncRepos(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	sources, tasks, runs := NewSyncSourceRepo(d), NewSyncTaskRepo(d), NewSyncRunRepo(d)
	now := time.Now()
	src := &biz.SyncSource{
		ID: mustID(t), Name: "it-ali", Provider: "aliyun", Config: []byte(`{"region_id":"r","access_key_id":"k"}`), Env: "prod",
		IntervalSeconds: 600, GracePeriodSeconds: 0, Enabled: true, NextRunTime: &now, LastStatus: "never", Version: 1, CreateTime: now, UpdateTime: now,
	}
	if err := sources.Create(ctx, src, []byte("ct")); err != nil {
		t.Fatalf("create source: %v", err)
	}
	dup := *src
	dup.ID = mustID(t)
	if err := sources.Create(ctx, &dup, nil); !errors.Is(err, biz.ErrSourceNameExists) {
		t.Fatalf("duplicate name error = %v", err)
	}
	got, err := sources.Get(ctx, src.ID)
	if err != nil || !got.HasCredential || got.ActiveTask || got.NextRunTime == nil || got.Version != 1 {
		t.Fatalf("get source = %+v err=%v", got, err)
	}
	got.Name, got.UpdateTime = "it-ali-2", time.Now()
	if err := sources.Update(ctx, got, 1, nil); err != nil {
		t.Fatalf("update source: %v", err)
	}
	if err := sources.Update(ctx, got, 1, nil); !errors.Is(err, biz.ErrSourceVersionConflict) {
		t.Fatalf("stale version error = %v", err)
	}
	var cred []byte
	if err := d.Pool.QueryRow(ctx, "SELECT credential FROM cmdb_sync_source WHERE id = $1", src.ID).Scan(&cred); err != nil || string(cred) != "ct" {
		t.Fatalf("credential must be kept: %q err=%v", cred, err)
	}
	if err := sources.Update(ctx, got, 2, []byte("ct2")); err != nil {
		t.Fatalf("rotate credential: %v", err)
	}
	if err := d.Pool.QueryRow(ctx, "SELECT credential FROM cmdb_sync_source WHERE id = $1", src.ID).Scan(&cred); err != nil || string(cred) != "ct2" {
		t.Fatalf("credential must be rotated: %q err=%v", cred, err)
	}
	items, total, err := sources.List(ctx, biz.SyncSourceFilter{Keyword: "ALI", Provider: "aliyun"}, 1, 10)
	if err != nil || total != 1 || items[0].Name != "it-ali-2" || items[0].Version != 3 {
		t.Fatalf("list sources = %+v total=%d err=%v", items, total, err)
	}
	task := &biz.SyncTask{ID: mustID(t), SourceID: src.ID, Trigger: "manual", Status: "pending", CreateTime: time.Now()}
	if err := tasks.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := tasks.Create(ctx, &biz.SyncTask{ID: mustID(t), SourceID: src.ID, Trigger: "manual", Status: "pending", CreateTime: time.Now()}); !errors.Is(err, biz.ErrSyncBusy) {
		t.Fatalf("second active task error = %v", err)
	}
	if err := tasks.Create(ctx, &biz.SyncTask{ID: mustID(t), SourceID: mustID(t), Trigger: "manual", Status: "pending", CreateTime: time.Now()}); !errors.Is(err, biz.ErrSourceNotFound) {
		t.Fatalf("unknown source error = %v", err)
	}
	if got, _ := sources.Get(ctx, src.ID); !got.ActiveTask {
		t.Fatal("ActiveTask must be true with a pending task")
	}
	for i, st := range []string{"failed", "succeeded"} {
		if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_sync_run (id, source_id, task_id, status, discovered, created, errors, start_time, finish_time)
			VALUES ($1, $2, $3, $4, $5, $5, $6, now() + make_interval(secs => $7), now())`,
			mustID(t), src.ID, task.ID, st, i+1, `[{"model_code":"cloud_host","external_id":"i-1","name":"x","reason":"conflict"}]`, float64(i)); err != nil {
			t.Fatalf("insert run: %v", err)
		}
	}
	list, total, err := runs.ListBySource(ctx, src.ID, 1, 10)
	if err != nil || total != 2 || list[0].Status != "succeeded" || list[0].Discovered != 2 || len(list[0].Errors) != 1 || list[0].Errors[0].Reason != "conflict" {
		t.Fatalf("list runs = %+v total=%d err=%v", list, total, err)
	}
	if err := sources.SoftDelete(ctx, src.ID, time.Now()); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := sources.Get(ctx, src.ID); !errors.Is(err, biz.ErrSourceNotFound) {
		t.Fatalf("deleted source error = %v", err)
	}
}

// 搜索与拓扑：真实 SQL（LATERAL / 数组参数 / BFS）。
func TestIntegration_CmdbSearchAndTopology(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	cis, rels := NewCIRepo(d), NewRelationRepo(d)
	for _, q := range []string{
		`INSERT INTO cmdb_model (code, name, category) VALUES ('it_host', '主机', 'compute'), ('it_sw', '交换机', 'network')`,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality) VALUES ('it_uplink', '上联', '上联交换机', '下联主机', 'it_host', 'it_sw', 'many_to_many')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	h1, h2, sw := mustID(t), mustID(t), mustID(t)
	for _, row := range [][]any{
		{h1, "it_host", "web-alpha", "prod", []string{"alpha-team"}, `{"ip":"10.0.0.1"}`, ""},
		{h2, "it_host", "db-1", "test", []string{}, `{"ip":"10.0.0.2"}`, "i-alpha"},
		{sw, "it_sw", "sw-1", "prod", []string{}, `{}`, ""},
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name, env, tags, attrs, external_id) VALUES ($1, $2, $3, $4, $5, $6, $7)", row...); err != nil {
			t.Fatal(err)
		}
	}
	for _, src := range []string{h1, h2} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) VALUES ($1, 'it_uplink', $2, $3)", mustID(t), src, sw); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := cis.Search(ctx, biz.SearchFilter{Query: "ALPHA", Limit: 10})
	if err != nil || len(hits) != 2 || hits[0].Field != "name" || hits[1].Field != "externalId" || hits[1].Value != "i-alpha" {
		t.Fatalf("search alpha = %+v err=%v", hits, err)
	}
	if hits, _ = cis.Search(ctx, biz.SearchFilter{Query: "10.0.0.2", Limit: 10}); len(hits) != 1 || hits[0].Field != "ip" {
		t.Fatalf("search attr = %+v", hits)
	}
	if hits, _ = cis.Search(ctx, biz.SearchFilter{Query: "alpha-team", Limit: 10}); len(hits) != 1 || hits[0].Field != "tags" {
		t.Fatalf("search tag = %+v", hits)
	}
	if hits, _ = cis.Search(ctx, biz.SearchFilter{Query: "1", ModelCodes: []string{"it_host"}, Envs: []string{"prod"}, Limit: 10}); len(hits) != 1 || hits[0].CI.ID != h1 {
		t.Fatalf("scoped search = %+v", hits)
	}
	if hits, _ = cis.Search(ctx, biz.SearchFilter{Query: "%", Limit: 10}); len(hits) != 0 {
		t.Fatalf("LIKE metachar must be escaped, got %d", len(hits))
	}
	nodes, edges, truncated, err := rels.Neighbors(ctx, h1, 2, 10)
	if err != nil || truncated || len(nodes) != 3 || len(edges) != 2 || nodes[0].ID != h1 || nodes[1].Depth != 1 || nodes[2].Depth != 2 || nodes[2].ID != h2 {
		t.Fatalf("neighbors = %+v edges=%d trunc=%v err=%v", nodes, len(edges), truncated, err)
	}
	if nodes, edges, truncated, _ := rels.Neighbors(ctx, h1, 3, 2); len(nodes) != 2 || !truncated || len(edges) != 1 {
		t.Fatalf("truncated neighbors = %d edges=%d trunc=%v", len(nodes), len(edges), truncated)
	}
	if _, _, _, err := rels.Neighbors(ctx, mustID(t), 1, 10); !errors.Is(err, biz.ErrCINotFound) {
		t.Fatalf("unknown root error = %v", err)
	}
}
