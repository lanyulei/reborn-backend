//go:build integration

package data

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newIntegrationData 真实连接池；未配置 TEST_DATABASE_SOURCE 时跳过；库名须含 "test"（会 TRUNCATE cmdb 表）。
func newIntegrationData(t *testing.T) *Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := NewData(&conf.Data{Database: &conf.Data_Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE cmdb_sync_conflict, cmdb_sync_staging, cmdb_sync_run, cmdb_sync_task, cmdb_sync_source, cmdb_relation, cmdb_ci_history, cmdb_ci,
		cmdb_ci_unique, cmdb_model_unique_rule, cmdb_model_field, cmdb_relation_type, cmdb_model_view, cmdb_role_scope, cmdb_model CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	// 最小元数据：host（ip 唯一、rack 人工）/ vm（ip 唯一）/ host_runs_vm。
	for _, q := range []string{
		`INSERT INTO cmdb_model (code, name, category, builtin) VALUES ('physical_server', '物理机', 'compute', true), ('virtual_machine', '虚拟机', 'compute', true), ('cloud_host', '云主机', 'cloud', true)`,
		`INSERT INTO cmdb_model_field (id, model_code, code, name, type, "unique", owner, list_visible) VALUES
		   (gen_random_uuid(), 'physical_server', 'ip', 'IP', 'ip', true, 'both', true),
		   (gen_random_uuid(), 'physical_server', 'cpu_cores', 'CPU', 'int', false, 'both', true),
		   (gen_random_uuid(), 'physical_server', 'rack', '机柜', 'string', false, 'manual', false),
		   (gen_random_uuid(), 'virtual_machine', 'ip', 'IP', 'ip', true, 'both', true),
		   (gen_random_uuid(), 'virtual_machine', 'vcpu', 'vCPU', 'int', false, 'both', true),
		   (gen_random_uuid(), 'cloud_host', 'instance_id', '实例', 'string', true, 'both', true)`,
		// 唯一规则（0049）：各类型一条单字段规则；字段的 unique 列只是派生标记。
		`INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes) VALUES
		   (gen_random_uuid(), 'physical_server', 'IP', '{ip}'),
		   (gen_random_uuid(), 'virtual_machine', 'IP', '{ip}'),
		   (gen_random_uuid(), 'cloud_host', '实例', '{instance_id}')`,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality) VALUES
		   ('host_runs_vm', '运行', '运行的虚拟机', '所在物理机', 'physical_server', 'virtual_machine', 'one_to_many')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return d
}

func mustID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertSource(t *testing.T, d *Data, grace int32, enabled bool, next time.Time) string {
	t.Helper()
	id := mustID(t)
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO cmdb_sync_source (id, name, provider, config, credential, env, interval_seconds, grace_period_seconds, enabled, next_run_time, last_status)
		 VALUES ($1, $2, 'vmware', '{"url":"https://vc/sdk","username":"u"}', '\x01', 'prod', 600, $3, $4, $5, 'never')`, id, "src-"+id[len(id)-12:], grace, enabled, next); err != nil {
		t.Fatalf("insert source: %v", err)
	}
	return id
}

type ciRow struct {
	ID, Name, Status string
	// UniqueKeys 该资源在 cmdb_ci_unique 中的键数（每条适用且字段非空的规则一行）。
	UniqueKeys int
	Attrs      map[string]any
	Version    int64
	Stale      bool
	Missing    *time.Time
}

func loadCI(t *testing.T, d *Data, model, source, external string) *ciRow {
	t.Helper()
	var r ciRow
	err := d.Pool.QueryRow(context.Background(),
		"SELECT id, name, status, (SELECT count(*) FROM cmdb_ci_unique u WHERE u.ci_id = cmdb_ci.id), attrs, version, stale, missing_since FROM cmdb_ci WHERE model_code = $1 AND source = $2 AND external_id = $3 AND delete_time IS NULL",
		model, source, external).Scan(&r.ID, &r.Name, &r.Status, &r.UniqueKeys, &r.Attrs, &r.Version, &r.Stale, &r.Missing)
	if err != nil {
		return nil
	}
	return &r
}

func countHistory(t *testing.T, d *Data, ciID, action string) int {
	t.Helper()
	var n int
	if err := d.Pool.QueryRow(context.Background(), "SELECT count(*) FROM cmdb_ci_history WHERE ci_id = $1 AND action = $2", ciID, action).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIntegration_ReconcileLifecycle(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	staging, reconcile, meta := NewStagingRepo(d), NewReconcileRepo(d), NewMetaRepo(d)
	srcID := insertSource(t, d, 0, true, time.Now())
	source := cmdbcontract.SourceOf(srcID)
	params := func(runID, model string, skip bool) biz.ReconcileParams {
		fields, err := meta.Fields(ctx, model)
		if err != nil {
			t.Fatal(err)
		}
		var manual []string
		for _, f := range fields {
			if f.Owner == cmdbcontract.FieldOwnerManual {
				manual = append(manual, f.Code)
			}
		}
		return biz.ReconcileParams{RunID: runID, ModelCode: model, Source: source, Env: "prod", Grace: 0, ManualKeys: manual, SkipDisappear: skip}
	}
	// 既有人工资源：名称与同步数据冲突。
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name, source, attrs) VALUES ($1, 'physical_server', 'manual-dup', 'manual', '{}')", mustID(t)); err != nil {
		t.Fatal(err)
	}

	// 第 1 轮：2 主机 + 2 虚拟机（1 台关联 host-1）；其中一台主机名与人工资源冲突。
	run1 := mustID(t)
	rows := []*biz.Resource{
		{ModelCode: "physical_server", ExternalID: "host-1", Name: "esx-1", Attrs: map[string]any{"ip": "10.0.0.1", "cpu_cores": 32, "rack": "A1"}},
		{ModelCode: "physical_server", ExternalID: "host-2", Name: "manual-dup", Attrs: map[string]any{"ip": "10.0.0.2"}},
		{
			ModelCode: "virtual_machine", ExternalID: "vm-1", Name: "web-1", Attrs: map[string]any{"ip": "10.1.0.1", "vcpu": 2},
			Relations: []cmdbcontract.StagingRelation{{Type: "host_runs_vm", Direction: "incoming", PeerModel: "physical_server", PeerExternalID: "host-1"}},
		},
		{ModelCode: "virtual_machine", ExternalID: "vm-2", Name: "web-2", Attrs: map[string]any{"ip": "10.1.0.2", "vcpu": 4}},
	}
	if err := staging.Append(ctx, run1, rows); err != nil {
		t.Fatal(err)
	}
	if n, _ := staging.Count(ctx, run1); n != 4 {
		t.Fatalf("staging count = %d", n)
	}
	models, _ := staging.Models(ctx, run1)
	if len(models) != 2 || models[0] != "physical_server" {
		t.Fatalf("staging models = %v", models)
	}
	c, errs, err := reconcile.ReconcileModel(ctx, params(run1, "physical_server", false))
	if err != nil {
		t.Fatalf("reconcile hosts: %v", err)
	}
	if c.Created != 1 || c.Failed != 1 || len(errs) != 1 || !strings.Contains(errs[0].Reason, "name already used") || errs[0].ExternalID != "host-2" {
		t.Fatalf("round1 hosts = %+v errs=%+v", c, errs)
	}
	c, _, err = reconcile.ReconcileModel(ctx, params(run1, "virtual_machine", false))
	if err != nil || c.Created != 2 {
		t.Fatalf("round1 vms = %+v err=%v", c, err)
	}
	if err := reconcile.ReconcileRelations(ctx, run1, source); err != nil {
		t.Fatal(err)
	}
	host1 := loadCI(t, d, "physical_server", source, "host-1")
	if host1 == nil || host1.UniqueKeys != 1 || host1.Attrs["rack"] != nil || host1.Attrs["cpu_cores"] != float64(32) {
		t.Fatalf("host1 = %+v", host1)
	}
	if countHistory(t, d, host1.ID, "create") != 1 || countHistory(t, d, host1.ID, "link") != 1 {
		t.Fatal("host1 must have create + link history")
	}
	vm1 := loadCI(t, d, "virtual_machine", source, "vm-1")
	var rel int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_relation WHERE type_code = 'host_runs_vm' AND src_id = $1 AND dst_id = $2 AND source = $3", host1.ID, vm1.ID, source).Scan(&rel)
	if rel != 1 {
		t.Fatal("relation host-1 → vm-1 must exist")
	}
	_ = staging.Clear(ctx, run1)

	// 人工编辑：rack 由人工写入；之后同步不得覆盖。
	if _, err := d.Pool.Exec(ctx, `UPDATE cmdb_ci SET attrs = attrs || '{"rack":"B2"}', version = version + 1 WHERE id = $1`, host1.ID); err != nil {
		t.Fatal(err)
	}

	// 第 2 轮：host-1 改名+CPU 变化（带 rack 试图覆盖）、vm-1 迁移到 host-2（host-2 这次改名不冲突）、vm-2 消失（grace 0 → 立即退役）。
	run2 := mustID(t)
	rows = []*biz.Resource{
		{ModelCode: "physical_server", ExternalID: "host-1", Name: "esx-1-renamed", Attrs: map[string]any{"ip": "10.0.0.1", "cpu_cores": 64, "rack": "SYNC"}},
		{ModelCode: "physical_server", ExternalID: "host-2", Name: "esx-2", Attrs: map[string]any{"ip": "10.0.0.2"}},
		{
			ModelCode: "virtual_machine", ExternalID: "vm-1", Name: "web-1", Attrs: map[string]any{"ip": "10.1.0.1", "vcpu": 2},
			Relations: []cmdbcontract.StagingRelation{{Type: "host_runs_vm", Direction: "incoming", PeerModel: "physical_server", PeerExternalID: "host-2"}},
		},
	}
	if err := staging.Append(ctx, run2, rows); err != nil {
		t.Fatal(err)
	}
	c, errs, err = reconcile.ReconcileModel(ctx, params(run2, "physical_server", false))
	if err != nil || c.Created != 1 || c.Updated != 1 || c.Unchanged != 0 || c.Failed != 0 {
		t.Fatalf("round2 hosts = %+v errs=%+v err=%v", c, errs, err)
	}
	c, _, err = reconcile.ReconcileModel(ctx, params(run2, "virtual_machine", false))
	if err != nil || c.Unchanged != 1 || c.Updated != 0 || c.Disappeared != 1 || c.Retired != 1 {
		t.Fatalf("round2 vms = %+v err=%v", c, err)
	}
	if err := reconcile.ReconcileRelations(ctx, run2, source); err != nil {
		t.Fatal(err)
	}
	host1 = loadCI(t, d, "physical_server", source, "host-1")
	if host1.Name != "esx-1-renamed" || host1.Attrs["cpu_cores"] != float64(64) || host1.Attrs["rack"] != "B2" || host1.Version != 3 {
		t.Fatalf("host1 after round2 = %+v", host1)
	}
	if countHistory(t, d, host1.ID, "update") != 1 {
		t.Fatal("host1 must have one update history")
	}
	var diff string
	_ = d.Pool.QueryRow(ctx, "SELECT diff::text FROM cmdb_ci_history WHERE ci_id = $1 AND action = 'update'", host1.ID).Scan(&diff)
	if !strings.Contains(diff, `"name": "esx-1"`) || !strings.Contains(diff, `"cpu_cores": 64`) || strings.Contains(diff, `"ip"`) {
		t.Fatalf("update diff must only contain changed keys: %s", diff)
	}
	vm2 := loadCI(t, d, "virtual_machine", source, "vm-2")
	if vm2.Status != "retired" || vm2.Missing == nil || countHistory(t, d, vm2.ID, "transition") != 1 {
		t.Fatalf("vm2 must be retired: %+v", vm2)
	}
	host2 := loadCI(t, d, "physical_server", source, "host-2")
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_relation WHERE type_code = 'host_runs_vm' AND dst_id = $1", vm1.ID).Scan(&rel)
	var toHost2 int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_relation WHERE type_code = 'host_runs_vm' AND src_id = $1 AND dst_id = $2", host2.ID, vm1.ID).Scan(&toHost2)
	if rel != 1 || toHost2 != 1 {
		t.Fatalf("vm1 must be moved to host-2: total=%d toHost2=%d", rel, toHost2)
	}
	if countHistory(t, d, vm1.ID, "unlink") != 1 || countHistory(t, d, vm1.ID, "link") != 2 {
		t.Fatal("vm1 must have unlink + 2 link histories")
	}
	_ = staging.Clear(ctx, run2)

	// 第 3 轮：vm-2 重新出现 → 重新激活；空集保护：空暂存 + skip 不得标记消失。
	run3 := mustID(t)
	if err := staging.Append(ctx, run3, []*biz.Resource{{ModelCode: "virtual_machine", ExternalID: "vm-2", Name: "web-2", Attrs: map[string]any{"ip": "10.1.0.2", "vcpu": 4}}}); err != nil {
		t.Fatal(err)
	}
	c, _, err = reconcile.ReconcileModel(ctx, params(run3, "virtual_machine", false))
	if err != nil || c.Updated != 1 || c.Disappeared != 1 {
		t.Fatalf("round3 vms = %+v err=%v", c, err)
	}
	vm2 = loadCI(t, d, "virtual_machine", source, "vm-2")
	if vm2.Status != "active" || vm2.Missing != nil || countHistory(t, d, vm2.ID, "transition") != 2 {
		t.Fatalf("vm2 must be reactivated: %+v", vm2)
	}
	_ = staging.Clear(ctx, run3)
	run4 := mustID(t)
	c, _, err = reconcile.ReconcileModel(ctx, params(run4, "physical_server", true))
	if err != nil || c.Disappeared != 0 || c.Retired != 0 {
		t.Fatalf("skip disappear = %+v err=%v", c, err)
	}
	if h := loadCI(t, d, "physical_server", source, "host-1"); h.Missing != nil {
		t.Fatal("skip must not mark missing")
	}

	// 宽限期：grace 1h 的源，消失只标记不退役。
	src2 := insertSource(t, d, 3600, true, time.Now())
	source2 := cmdbcontract.SourceOf(src2)
	run5 := mustID(t)
	if err := staging.Append(ctx, run5, []*biz.Resource{{ModelCode: "cloud_host", ExternalID: "i-1", Name: "ecs-1", Attrs: map[string]any{"instance_id": "i-1"}}}); err != nil {
		t.Fatal(err)
	}
	p := params(run5, "cloud_host", false)
	p.Source, p.Grace = source2, time.Hour
	if c, _, err = reconcile.ReconcileModel(ctx, p); err != nil || c.Created != 1 {
		t.Fatalf("grace round1 = %+v err=%v", c, err)
	}
	_ = staging.Clear(ctx, run5)
	p.RunID = mustID(t)
	if c, _, err = reconcile.ReconcileModel(ctx, p); err != nil || c.Disappeared != 1 || c.Retired != 0 {
		t.Fatalf("grace round2 = %+v err=%v", c, err)
	}
	if ecs := loadCI(t, d, "cloud_host", source2, "i-1"); ecs.Status != "active" || ecs.Missing == nil {
		t.Fatalf("within grace must stay active: %+v", ecs)
	}
	// 把消失时间拨回 2h 前 → 下一轮退役；级联：云主机上挂了一个 cascade_retire 配件（人工录入）也随之退役，
	// 而 restrict 关系的对端不受影响。
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_ci SET missing_since = now() - interval '2 hours' WHERE source = $1", source2); err != nil {
		t.Fatal(err)
	}
	var ecsID string
	_ = d.Pool.QueryRow(ctx, "SELECT id FROM cmdb_ci WHERE source = $1", source2).Scan(&ecsID)
	partID, kept := mustID(t), mustID(t)
	for _, q := range []string{
		"INSERT INTO cmdb_model (code, name, category) VALUES ('it_part', '配件', 'component')",
		"INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete) VALUES ('it_has_part', '配件', '配件', '安装于', '*', 'it_part', 'one_to_many', 'cascade_retire')",
		"INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete) VALUES ('it_peer', '对等', '对等', '对等', '*', '*', 'many_to_many', 'restrict')",
		"INSERT INTO cmdb_ci (id, model_code, name, source) VALUES ('" + partID + "', 'it_part', 'mem-1', 'manual'), ('" + kept + "', 'it_part', 'mem-2', 'manual')",
		"INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) VALUES ('" + mustID(t) + "', 'it_has_part', '" + ecsID + "', '" + partID + "'), ('" + mustID(t) + "', 'it_peer', '" + ecsID + "', '" + kept + "')",
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	p.RunID = mustID(t)
	if c, _, err = reconcile.ReconcileModel(ctx, p); err != nil || c.Retired != 2 {
		t.Fatalf("grace round3 (with cascade) = %+v err=%v", c, err)
	}
	var partStatus, keptStatus string
	_ = d.Pool.QueryRow(ctx, "SELECT status FROM cmdb_ci WHERE id = $1", partID).Scan(&partStatus)
	_ = d.Pool.QueryRow(ctx, "SELECT status FROM cmdb_ci WHERE id = $1", kept).Scan(&keptStatus)
	if partStatus != "retired" || keptStatus != "active" {
		t.Fatalf("cascade: part=%s peer=%s", partStatus, keptStatus)
	}
	if countHistory(t, d, partID, "transition") != 1 {
		t.Fatal("cascaded part must have a transition history")
	}

	// 通知事件直插：带去重键的事件在 TTL 内只落一行；无去重键每次落。
	notifier := NewNotifier(d)
	if _, err := d.Pool.Exec(ctx, "TRUNCATE notify_event, notify_dedup"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := notifier.Notify(ctx, biz.SyncEvent{Type: "cmdb.sync.failed", Severity: 3, Title: "x", Payload: map[string]any{"a": 1}, DedupKey: "cmdb.sync.failed:s", DedupTTL: time.Hour}); err != nil {
			t.Fatalf("notify: %v", err)
		}
		if err := notifier.Notify(ctx, biz.SyncEvent{Type: "cmdb.sync.disappeared", Severity: 2, Title: "y"}); err != nil {
			t.Fatalf("notify: %v", err)
		}
	}
	var failedN, goneN int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM notify_event WHERE event_type = 'cmdb.sync.failed'").Scan(&failedN)
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM notify_event WHERE event_type = 'cmdb.sync.disappeared' AND source = 'internal' AND biz_id = 'default' AND publish_state = 0").Scan(&goneN)
	if failedN != 1 || goneN != 2 {
		t.Fatalf("events failed=%d disappeared=%d", failedN, goneN)
	}
}

func TestIntegration_TaskLeaseAndScheduler(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	tasks, sched := NewTaskRepo(d), NewSchedulerRepo(d)
	srcID := insertSource(t, d, 0, true, time.Now().Add(-time.Second))
	// 调度：到期源入队一次；再调度不重复（活动任务唯一）；next_run_time 已推进。
	if n, err := sched.EnqueueDue(ctx, time.Now()); err != nil || n != 1 {
		t.Fatalf("enqueue = %d err=%v", n, err)
	}
	if n, err := sched.EnqueueDue(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("second enqueue = %d err=%v", n, err)
	}
	var next time.Time
	_ = d.Pool.QueryRow(ctx, "SELECT next_run_time FROM cmdb_sync_source WHERE id = $1", srcID).Scan(&next)
	if next.Before(time.Now().Add(500 * time.Second)) {
		t.Fatalf("next_run_time not advanced: %v", next)
	}
	// 领取：两个执行器只有一个拿到。
	a, err := tasks.Claim(ctx, "worker-a", time.Minute)
	if err != nil || a == nil || a.Attempts != 1 || a.RunID == "" {
		t.Fatalf("claim a = %+v err=%v", a, err)
	}
	if b, err := tasks.Claim(ctx, "worker-b", time.Minute); err != nil || b != nil {
		t.Fatalf("claim b must get nothing: %+v err=%v", b, err)
	}
	var status, lastStatus string
	_ = d.Pool.QueryRow(ctx, "SELECT status FROM cmdb_sync_run WHERE id = $1", a.RunID).Scan(&status)
	_ = d.Pool.QueryRow(ctx, "SELECT last_status FROM cmdb_sync_source WHERE id = $1", srcID).Scan(&lastStatus)
	if status != "running" || lastStatus != "running" {
		t.Fatalf("run/source must be running: %s %s", status, lastStatus)
	}
	// 续约：属主正确成功；错误属主失主。
	if err := tasks.Renew(ctx, a, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := tasks.Renew(ctx, &biz.Task{ID: a.ID, Token: "intruder"}, time.Minute); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("intruder renew error = %v", err)
	}
	// 租约过期 → 他人重领（attempts=2），旧运行置失败。
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_task SET lease_until = now() - interval '1 second' WHERE id = $1", a.ID); err != nil {
		t.Fatal(err)
	}
	b, err := tasks.Claim(ctx, "worker-b", time.Minute)
	if err != nil || b == nil || b.ID != a.ID || b.Attempts != 2 || b.RunID == a.RunID {
		t.Fatalf("reclaim = %+v err=%v", b, err)
	}
	_ = d.Pool.QueryRow(ctx, "SELECT status FROM cmdb_sync_run WHERE id = $1", a.RunID).Scan(&status)
	if status != "failed" {
		t.Fatalf("previous run must be failed: %s", status)
	}
	// 原属主完成 → 失主；新属主完成成功，统计落库。
	if err := tasks.Finish(ctx, a, "succeeded", biz.Counts{}, "", nil); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("stale owner finish error = %v", err)
	}
	if err := tasks.Finish(ctx, b, "succeeded", biz.Counts{Discovered: 7, Created: 3}, "", []cmdbcontract.RunError{{Reason: "x"}}); err != nil {
		t.Fatal(err)
	}
	var discovered int
	_ = d.Pool.QueryRow(ctx, "SELECT status, discovered FROM cmdb_sync_run WHERE id = $1", b.RunID).Scan(&status, &discovered)
	_ = d.Pool.QueryRow(ctx, "SELECT last_status FROM cmdb_sync_source WHERE id = $1", srcID).Scan(&lastStatus)
	if status != "succeeded" || discovered != 7 || lastStatus != "succeeded" {
		t.Fatalf("finish not persisted: %s %d %s", status, discovered, lastStatus)
	}
	// 尝试耗尽：running 且过期且 attempts≥3 → 置 failed，不再返回。
	dead := mustID(t)
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_sync_task (id, source_id, trigger, status, attempts, claimed_by, lease_until, run_id)
		VALUES ($1, $2, 'manual', 'running', 3, 'gone', now() - interval '1 minute', $3)`, dead, srcID, mustID(t)); err != nil {
		t.Fatal(err)
	}
	if x, err := tasks.Claim(ctx, "worker-c", time.Minute); err != nil || x != nil {
		t.Fatalf("exhausted task must not be claimable: %+v err=%v", x, err)
	}
	_ = d.Pool.QueryRow(ctx, "SELECT status FROM cmdb_sync_task WHERE id = $1", dead).Scan(&status)
	if status != "failed" {
		t.Fatalf("exhausted task status = %s", status)
	}
	// 陈旧标记：last_synced_at 早于 3×interval 的同步资源置 stale；人工资源不受影响。
	src := cmdbcontract.SourceOf(srcID)
	for _, q := range []string{
		"INSERT INTO cmdb_ci (id, model_code, name, source, external_id, last_synced_at) VALUES ('" + mustID(t) + "', 'cloud_host', 'old', '" + src + "', 'i-old', now() - interval '1 day')",
		"INSERT INTO cmdb_ci (id, model_code, name, source, external_id, last_synced_at) VALUES ('" + mustID(t) + "', 'cloud_host', 'fresh', '" + src + "', 'i-new', now())",
		"INSERT INTO cmdb_ci (id, model_code, name, source) VALUES ('" + mustID(t) + "', 'cloud_host', 'manual', 'manual')",
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := sched.MarkStale(ctx, 3); err != nil || n != 1 {
		t.Fatalf("mark stale = %d err=%v", n, err)
	}
	var staleName string
	_ = d.Pool.QueryRow(ctx, "SELECT name FROM cmdb_ci WHERE stale").Scan(&staleName)
	if staleName != "old" {
		t.Fatalf("stale = %q", staleName)
	}
	// 遗留暂存清理：无 running 运行的暂存行被删除。
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_sync_staging (run_id, model_code, external_id, name) VALUES ($1, 'cloud_host', 'x', 'x')", mustID(t)); err != nil {
		t.Fatal(err)
	}
	if n, err := sched.PurgeStaging(ctx, time.Hour); err != nil || n != 1 {
		t.Fatalf("purge = %d err=%v", n, err)
	}
	// 禁用 / 无凭据的源不入队。
	insertSource(t, d, 0, false, time.Now().Add(-time.Hour))
	noCred := insertSource(t, d, 0, true, time.Now().Add(-time.Hour))
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET credential = NULL WHERE id = $1", noCred); err != nil {
		t.Fatal(err)
	}
	if n, err := sched.EnqueueDue(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("disabled/no-credential must not enqueue: %d err=%v", n, err)
	}
	_ = hex.EncodeToString
}

// 分区维护与回调仓储：预建/迁出 DEFAULT/ATTACH、按保留期删除、令牌与限流门闩、入队幂等。
func TestIntegration_MaintenanceAndWebhook(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	maint, hooks := NewMaintenanceRepo(d), NewWebhookRepo(d)
	// 先把一行写进 DEFAULT 分区（远未来月份），再预建该月分区：行须迁入新分区。
	future := time.Date(2031, 3, 15, 0, 0, 0, 0, time.UTC)
	ciID := mustID(t)
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name) VALUES ($1, 'cloud_host', 'h')", ciID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci_history (id, ci_id, action, create_time) VALUES ($1, $2, 'create', $3)", mustID(t), ciID, future); err != nil {
		t.Fatal(err)
	}
	if err := maint.EnsurePartition(ctx, biz.HistoryTable, future); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	if err := maint.EnsurePartition(ctx, biz.HistoryTable, future); err != nil {
		t.Fatalf("ensure partition must be idempotent: %v", err)
	}
	var inPart, inDefault int
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci_history_2031_03").Scan(&inPart)
	_ = d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci_history_default WHERE create_time >= '2031-03-01' AND create_time < '2031-04-01'").Scan(&inDefault)
	if inPart != 1 || inDefault != 0 {
		t.Fatalf("row must move into the new partition: part=%d default=%d", inPart, inDefault)
	}
	// 按保留期删除：上界 ≤ before 的分区被删，之后的保留；DEFAULT 不动。
	// 注意：before 晚于当前预置分区，它们也会被删（随后用 EnsurePartition 恢复，验证"删后可重建"）。
	n, err := maint.DropPartitionsBefore(ctx, biz.HistoryTable, time.Date(2031, 4, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || n < 1 {
		t.Fatalf("drop = %d err=%v", n, err)
	}
	for i := 0; i < 3; i++ {
		if err := maint.EnsurePartition(ctx, biz.HistoryTable, time.Now().UTC().AddDate(0, i, 0)); err != nil {
			t.Fatalf("restore partition: %v", err)
		}
	}
	var exists bool
	_ = d.Pool.QueryRow(ctx, "SELECT to_regclass('cmdb_ci_history_2031_03') IS NOT NULL").Scan(&exists)
	if exists {
		t.Fatal("partition must be dropped")
	}
	_ = d.Pool.QueryRow(ctx, "SELECT to_regclass('cmdb_ci_history_default') IS NOT NULL").Scan(&exists)
	if !exists {
		t.Fatal("default partition must survive")
	}
	if err := maint.EnsurePartition(ctx, "cmdb_ci_history; DROP TABLE cmdb_ci", future); err == nil {
		t.Fatal("table name must be whitelisted")
	}

	// 回调：未启用 → 不存在；设置哈希后可读；限流门闩；入队幂等。
	srcID := insertSource(t, d, 0, true, time.Now())
	target, err := hooks.GetWebhook(ctx, srcID)
	if err != nil || !target.Enabled || len(target.TokenHash) != 0 {
		t.Fatalf("target = %+v err=%v", target, err)
	}
	if _, err := hooks.GetWebhook(ctx, mustID(t)); !errors.Is(err, biz.ErrWebhookSourceNotFound) {
		t.Fatalf("unknown source error = %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET webhook_token_hash = '\\x01' WHERE id = $1", srcID); err != nil {
		t.Fatal(err)
	}
	if target, _ = hooks.GetWebhook(ctx, srcID); len(target.TokenHash) != 1 {
		t.Fatalf("hash not read: %+v", target)
	}
	if ok, err := hooks.TouchWebhook(ctx, srcID, time.Minute); err != nil || !ok {
		t.Fatalf("first touch = %v err=%v", ok, err)
	}
	if ok, _ := hooks.TouchWebhook(ctx, srcID, time.Minute); ok {
		t.Fatal("second touch within interval must be rejected")
	}
	id1, queued, err := hooks.EnqueueTask(ctx, srcID, "webhook")
	if err != nil || !queued || id1 == "" {
		t.Fatalf("enqueue = %s %v %v", id1, queued, err)
	}
	id2, queued, err := hooks.EnqueueTask(ctx, srcID, "webhook")
	if err != nil || queued || id2 != id1 {
		t.Fatalf("second enqueue must return the active task: %s %v %v", id2, queued, err)
	}
}
