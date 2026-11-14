//go:build integration

package data

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

// assertUniqueKeysConsistent 不变量：cmdb_ci_unique 恰好等于"未删除资源 × 适用规则 × 键非空"。
func assertUniqueKeysConsistent(t *testing.T, d *Data) {
	t.Helper()
	const expected = `
		SELECT c.id AS ci_id, r.id AS rule_id, k.key
		FROM cmdb_ci c JOIN cmdb_model_unique_rule r ON r.model_code = c.model_code
		CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, r.field_codes) AS key) k
		WHERE c.delete_time IS NULL AND k.key IS NOT NULL`
	var missing, stale int
	if err := d.Pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM (`+expected+` EXCEPT SELECT ci_id, rule_id, key FROM cmdb_ci_unique) m),
		        (SELECT count(*) FROM (SELECT ci_id, rule_id, key FROM cmdb_ci_unique EXCEPT `+expected+`) s)`).Scan(&missing, &stale); err != nil {
		t.Fatal(err)
	}
	if missing != 0 || stale != 0 {
		t.Fatalf("cmdb_ci_unique inconsistent: %d missing, %d stale", missing, stale)
	}
}

// 对账与唯一规则：同步数据撞规则的行逐条跳过并说明原因，不拖垮整批；多条规则各自独立；
// 键按"实际写入的属性"（人工归属字段不写）判定；写入后键与规则严格对应。
func TestIntegration_ReconcileUniqueRules(t *testing.T) {
	d := newIntegrationData(t) // physical_server：规则 IP（ip）；rack 为人工归属字段
	ctx := context.Background()
	staging, reconcile := NewStagingRepo(d), NewReconcileRepo(d)
	source := cmdbcontract.SourceOf(insertSource(t, d, 3600, true, time.Now()))
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := d.Pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// 再加两条规则：序列号（单字段）、机位（dc + slot 联合）；serial / dc / slot 由同步写入。
	exec(`INSERT INTO cmdb_model_field (id, model_code, code, name, type, owner) VALUES
	        (gen_random_uuid(), 'physical_server', 'serial', '序列号', 'string', 'both'),
	        (gen_random_uuid(), 'physical_server', 'dc', '机房', 'string', 'both'),
	        (gen_random_uuid(), 'physical_server', 'slot', '机位', 'string', 'both')`)
	exec(`INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes) VALUES
	        (gen_random_uuid(), 'physical_server', '序列号', '{serial}'),
	        (gen_random_uuid(), 'physical_server', '机位', '{dc,slot}')`)
	// 既有人工资源占用 ip / 序列号 / 机位各一个值。
	exec(`INSERT INTO cmdb_ci (id, model_code, name, source, attrs) VALUES
	        ($1, 'physical_server', 'manual-ip', 'manual', '{"ip":"10.0.0.9"}'),
	        ($2, 'physical_server', 'manual-sn', 'manual', '{"serial":"SN-M"}'),
	        ($3, 'physical_server', 'manual-slot', 'manual', '{"dc":"bj","slot":"U1"}')`, mustID(t), mustID(t), mustID(t))
	assertUniqueKeysConsistent(t, d)

	run := func(rows ...*biz.Resource) (biz.Counts, map[string]string) {
		t.Helper()
		runID := mustID(t)
		if err := staging.Append(ctx, runID, rows); err != nil {
			t.Fatal(err)
		}
		c, errs, err := reconcile.ReconcileModel(ctx, biz.ReconcileParams{
			RunID: runID, ModelCode: "physical_server", Source: source, Env: "prod", Grace: time.Hour, ManualKeys: []string{"rack"},
		})
		if err != nil {
			t.Fatalf("reconcile must not fail as a whole: %v", err)
		}
		_ = staging.Clear(ctx, runID)
		reasons := map[string]string{}
		for _, e := range errs {
			reasons[e.ExternalID] = e.Reason
		}
		assertUniqueKeysConsistent(t, d)
		return c, reasons
	}
	host := func(eid, name string, attrs map[string]any) *biz.Resource {
		return &biz.Resource{ModelCode: "physical_server", ExternalID: eid, Name: name, Attrs: attrs}
	}

	// 第 1 轮（新建）：
	//   ok-1 / ok-2   互不冲突 → 新建
	//   ip-dup        与人工资源 ip 相同 → 规则 IP
	//   sn-dup        与人工资源序列号相同 → 规则 序列号
	//   slot-dup      与人工资源机位相同 → 规则 机位
	//   slot-ok       同机房不同机位 → 新建（联合规则只在全部字段相同时冲突）
	//   batch-a / -b  本批两条序列号相同 → 按 external_id 取 batch-a
	//   partial       只有 dc 没有 slot → 不受联合规则约束
	c, reasons := run(
		host("ok-1", "ok-1", map[string]any{"ip": "10.0.0.1", "serial": "SN-1", "dc": "bj", "slot": "U2", "rack": "IGNORED"}),
		host("ok-2", "ok-2", map[string]any{"ip": "10.0.0.2"}),
		host("ip-dup", "ip-dup", map[string]any{"ip": "10.0.0.9", "serial": "SN-X"}),
		host("sn-dup", "sn-dup", map[string]any{"ip": "10.0.0.3", "serial": "SN-M"}),
		host("slot-dup", "slot-dup", map[string]any{"ip": "10.0.0.4", "dc": "bj", "slot": "U1"}),
		host("slot-ok", "slot-ok", map[string]any{"ip": "10.0.0.5", "dc": "bj", "slot": "U3"}),
		host("batch-a", "batch-a", map[string]any{"ip": "10.0.0.6", "serial": "SN-B"}),
		host("batch-b", "batch-b", map[string]any{"ip": "10.0.0.7", "serial": "SN-B"}),
		host("partial", "partial", map[string]any{"ip": "10.0.0.8", "dc": "bj"}),
	)
	if c.Created != 5 || c.Failed != 4 || c.Updated != 0 {
		t.Fatalf("round1 = %+v reasons=%v", c, reasons)
	}
	for eid, want := range map[string]string{
		"ip-dup": `unique rule "IP"`, "sn-dup": `unique rule "序列号"`, "slot-dup": `unique rule "机位"`,
		// 胜出的 batch-a 已落库，batch-b 的原因即"序列号被另一资源占用"。
		"batch-b": `unique rule "序列号"`,
	} {
		if !strings.Contains(reasons[eid], want) {
			t.Fatalf("reason[%s] = %q, want %s (all: %v)", eid, reasons[eid], want, reasons)
		}
	}
	// 冲突清单：全部未写入的行都留存（不只样例），带原因。
	conflicts := func() map[string]string {
		t.Helper()
		rows, err := d.Pool.Query(ctx, "SELECT external_id, reason FROM cmdb_sync_conflict WHERE source = $1 AND model_code = 'physical_server'", source)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var eid, reason string
			if err := rows.Scan(&eid, &reason); err != nil {
				t.Fatal(err)
			}
			out[eid] = reason
		}
		return out
	}
	if got := conflicts(); len(got) != 4 || !strings.Contains(got["ip-dup"], `unique rule "IP"`) || got["batch-b"] == "" {
		t.Fatalf("stored conflicts after round1 = %v", got)
	}
	var firstSeen time.Time
	if err := d.Pool.QueryRow(ctx, "SELECT first_seen FROM cmdb_sync_conflict WHERE source = $1 AND external_id = 'slot-dup'", source).Scan(&firstSeen); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reasons["slot-dup"], "dc + slot") {
		t.Fatalf("composite reason must list fields: %q", reasons["slot-dup"])
	}
	for _, eid := range []string{"ok-1", "ok-2", "slot-ok", "batch-a", "partial"} {
		if loadCI(t, d, "physical_server", source, eid) == nil {
			t.Fatalf("%s must be created", eid)
		}
	}
	// ok-1：人工归属字段 rack 不写；三条规则各一个键。ok-2 只有 ip → 一个键。
	if ok1 := loadCI(t, d, "physical_server", source, "ok-1"); ok1.Attrs["rack"] != nil || ok1.UniqueKeys != 3 {
		t.Fatalf("ok-1 = %+v", ok1)
	}
	if ok2 := loadCI(t, d, "physical_server", source, "ok-2"); ok2.UniqueKeys != 1 {
		t.Fatalf("ok-2 keys = %d", ok2.UniqueKeys)
	}

	// 第 2 轮（更新）：
	//   ok-1      序列号改成新值 → 更新，旧值释放
	//   ok-2      ip 改成 ok-1 现有的 ip → 跳过（规则 IP），原值保留
	//   slot-ok   机位改成人工资源占用的 U1 → 跳过（规则 机位）
	//   batch-a   序列号改成 SN-NEW；partial 也改成 SN-NEW → 本批同键，按 id 取其一
	//   sn-dup    这次序列号不冲突 → 新建（上轮失败的行自愈）
	c, reasons = run(
		host("ok-1", "ok-1", map[string]any{"ip": "10.0.0.1", "serial": "SN-1b", "dc": "bj", "slot": "U2"}),
		host("ok-2", "ok-2", map[string]any{"ip": "10.0.0.1"}),
		host("slot-ok", "slot-ok", map[string]any{"ip": "10.0.0.5", "dc": "bj", "slot": "U1"}),
		host("batch-a", "batch-a", map[string]any{"ip": "10.0.0.6", "serial": "SN-NEW"}),
		host("partial", "partial", map[string]any{"ip": "10.0.0.8", "dc": "bj", "serial": "SN-NEW"}),
		host("sn-dup", "sn-dup", map[string]any{"ip": "10.0.0.3", "serial": "SN-1"}),
	)
	if c.Updated != 2 || c.Created != 1 || c.Failed != 3 || c.Unchanged != 0 {
		t.Fatalf("round2 = %+v reasons=%v", c, reasons)
	}
	if !strings.Contains(reasons["ok-2"], `unique rule "IP"`) || !strings.Contains(reasons["slot-ok"], `unique rule "机位"`) {
		t.Fatalf("round2 reasons = %v", reasons)
	}
	// 第 2 轮后清单整体刷新：本轮不再出现 / 已解决的（ip-dup、slot-dup、batch-b、sn-dup）移除，
	// 本轮新的冲突（ok-2、slot-ok、batch-a 或 partial 之一）写入。
	if got := conflicts(); len(got) != 3 || got["ok-2"] == "" || got["slot-ok"] == "" || got["ip-dup"] != "" || got["sn-dup"] != "" {
		t.Fatalf("stored conflicts after round2 = %v", got)
	}
	if ok2 := loadCI(t, d, "physical_server", source, "ok-2"); ok2.Attrs["ip"] != "10.0.0.2" || ok2.Version != 1 {
		t.Fatalf("skipped update must leave the resource untouched: %+v", ok2)
	}
	if ok1 := loadCI(t, d, "physical_server", source, "ok-1"); ok1.Attrs["serial"] != "SN-1b" || ok1.Version != 2 {
		t.Fatalf("ok-1 = %+v", ok1)
	}
	// 本批同键：恰好一条拿到 SN-NEW，另一条保持原值并报告。
	a, p := loadCI(t, d, "physical_server", source, "batch-a"), loadCI(t, d, "physical_server", source, "partial")
	got := 0
	for _, r := range []*ciRow{a, p} {
		if r.Attrs["serial"] == "SN-NEW" {
			got++
		}
	}
	if got != 1 || (reasons["batch-a"] == "") == (reasons["partial"] == "") {
		t.Fatalf("exactly one of batch-a / partial must take SN-NEW: a=%v p=%v reasons=%v", a.Attrs, p.Attrs, reasons)
	}
	// 上一轮被 ok-1 占用的 SN-1 已释放，sn-dup 用它新建成功。
	if sn := loadCI(t, d, "physical_server", source, "sn-dup"); sn == nil || sn.Attrs["serial"] != "SN-1" {
		t.Fatalf("sn-dup must be created with the freed serial: %+v", sn)
	}

	// 第 3 轮：冲突解除后（人工资源让出 U1）被跳过的更新自愈；未变化的行计为 unchanged。
	exec(`UPDATE cmdb_ci SET attrs = '{"dc":"bj","slot":"U9"}', version = version + 1 WHERE model_code = 'physical_server' AND name = 'manual-slot'`)
	c, reasons = run(
		host("slot-ok", "slot-ok", map[string]any{"ip": "10.0.0.5", "dc": "bj", "slot": "U1"}),
		host("ok-1", "ok-1", map[string]any{"ip": "10.0.0.1", "serial": "SN-1b", "dc": "bj", "slot": "U2"}),
	)
	if c.Updated != 1 || c.Unchanged != 1 || c.Failed != 0 || len(reasons) != 0 {
		t.Fatalf("round3 = %+v reasons=%v", c, reasons)
	}
	if got := conflicts(); len(got) != 0 {
		t.Fatalf("resolved conflicts must be cleared: %v", got)
	}
	_ = firstSeen
	if s := loadCI(t, d, "physical_server", source, "slot-ok"); s.Attrs["slot"] != "U1" {
		t.Fatalf("slot-ok must heal: %+v", s)
	}

	// 没有任何规则的类型：对账照常（零规则路径）。
	exec(`DELETE FROM cmdb_model_unique_rule WHERE model_code = 'physical_server'`)
	c, reasons = run(
		host("free-1", "free-1", map[string]any{"ip": "10.0.0.1", "serial": "SN-1b"}),
		host("free-2", "free-2", map[string]any{"ip": "10.0.0.1", "serial": "SN-1b"}),
	)
	if c.Created != 2 || c.Failed != 0 {
		t.Fatalf("no-rule round = %+v reasons=%v", c, reasons)
	}
}

// 同批取舍不得被"本就写不进去的行"左右：行 A 被库中资源永久挡住时，与 A 仅在另一条规则 /
// 名称上相同的行 B 仍应写入（否则只要 A 一直被挡，B 永远进不来）。
func TestIntegration_ReconcileNoStarvation(t *testing.T) {
	d := newIntegrationData(t) // physical_server：规则 IP（ip）
	ctx := context.Background()
	staging, reconcile := NewStagingRepo(d), NewReconcileRepo(d)
	source := cmdbcontract.SourceOf(insertSource(t, d, 3600, true, time.Now()))
	for _, q := range []string{
		`INSERT INTO cmdb_model_field (id, model_code, code, name, type, owner) VALUES (gen_random_uuid(), 'physical_server', 'serial', '序列号', 'string', 'both')`,
		`INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes) VALUES (gen_random_uuid(), 'physical_server', '序列号', '{serial}')`,
		// 人工资源永久占用 ip 10.0.0.9。
		`INSERT INTO cmdb_ci (id, model_code, name, source, attrs) VALUES (gen_random_uuid(), 'physical_server', 'manual', 'manual', '{"ip":"10.0.0.9"}')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	run := func(rows ...*biz.Resource) (biz.Counts, map[string]string) {
		t.Helper()
		runID := mustID(t)
		if err := staging.Append(ctx, runID, rows); err != nil {
			t.Fatal(err)
		}
		c, errs, err := reconcile.ReconcileModel(ctx, biz.ReconcileParams{RunID: runID, ModelCode: "physical_server", Source: source, Env: "prod", Grace: time.Hour})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		_ = staging.Clear(ctx, runID)
		reasons := map[string]string{}
		for _, e := range errs {
			reasons[e.ExternalID] = e.Reason
		}
		assertUniqueKeysConsistent(t, d)
		return c, reasons
	}
	host := func(eid, name string, attrs map[string]any) *biz.Resource {
		return &biz.Resource{ModelCode: "physical_server", ExternalID: eid, Name: name, Attrs: attrs}
	}

	// 新建：a-1 撞人工资源的 ip（永久被挡）；b-2 与 a-1 同序列号、c-3 与 a-1 同名——都应写入。
	c, reasons := run(
		host("a-1", "shared-name", map[string]any{"ip": "10.0.0.9", "serial": "SN-S"}),
		host("b-2", "b-2", map[string]any{"ip": "10.0.0.2", "serial": "SN-S"}),
		host("c-3", "shared-name", map[string]any{"ip": "10.0.0.3"}),
	)
	if c.Created != 2 || c.Failed != 1 || !strings.Contains(reasons["a-1"], `unique rule "IP"`) {
		t.Fatalf("insert round = %+v reasons=%v", c, reasons)
	}
	if loadCI(t, d, "physical_server", source, "b-2") == nil || loadCI(t, d, "physical_server", source, "c-3") == nil {
		t.Fatal("b-2 and c-3 must not be starved by the blocked a-1")
	}

	// 更新：u-1 改 ip 撞人工资源（永久被挡）且想要序列号 SN-U / 名称 n-u；
	// u-2 也想要序列号 SN-U、u-3 也想改名 n-u——都应生效。id 的大小关系不可控，故两种顺序都成立才算过。
	c, _ = run(
		host("u-1", "u-1", map[string]any{"ip": "10.0.1.1"}),
		host("u-2", "u-2", map[string]any{"ip": "10.0.1.2"}),
		host("u-3", "u-3", map[string]any{"ip": "10.0.1.3"}),
	)
	if c.Created != 3 {
		t.Fatalf("seed round = %+v", c)
	}
	c, reasons = run(
		host("u-1", "n-u", map[string]any{"ip": "10.0.0.9", "serial": "SN-U"}),
		host("u-2", "u-2", map[string]any{"ip": "10.0.1.2", "serial": "SN-U"}),
		host("u-3", "n-u", map[string]any{"ip": "10.0.1.3"}),
		host("b-2", "b-2", map[string]any{"ip": "10.0.0.2", "serial": "SN-S"}),
		host("c-3", "shared-name", map[string]any{"ip": "10.0.0.3"}),
	)
	if c.Updated != 2 || c.Failed != 1 || c.Unchanged != 2 || !strings.Contains(reasons["u-1"], `unique rule "IP"`) {
		t.Fatalf("update round = %+v reasons=%v", c, reasons)
	}
	if u2 := loadCI(t, d, "physical_server", source, "u-2"); u2.Attrs["serial"] != "SN-U" {
		t.Fatalf("u-2 must not be starved: %+v", u2)
	}
	if u3 := loadCI(t, d, "physical_server", source, "u-3"); u3.Name != "n-u" {
		t.Fatalf("u-3 must not be starved: %+v", u3)
	}
}

// 大批量：两万行首次同步（含本批内重复）在可接受的时间内完成——同批查重是线性的。
func TestIntegration_ReconcileLargeBatch(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	staging, reconcile := NewStagingRepo(d), NewReconcileRepo(d)
	source := cmdbcontract.SourceOf(insertSource(t, d, 3600, true, time.Now()))
	runID := mustID(t)
	// 关掉暂存表的自动分析：否则它是否恰好在测试期间跑过会左右执行计划，结果不可复现。
	// 统计信息只能来自对账流程自己的 RefreshStats。
	if _, err := d.Pool.Exec(ctx, "ALTER TABLE cmdb_sync_staging SET (autovacuum_enabled = false)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = d.Pool.Exec(context.Background(), "ALTER TABLE cmdb_sync_staging RESET (autovacuum_enabled)")
	})
	const n = 20000
	// 每 100 行里有 1 行与前一行 ip 相同（本批内重复）。
	if _, err := d.Pool.Exec(ctx, `
		INSERT INTO cmdb_sync_staging (run_id, model_code, external_id, name, attrs, relations)
		SELECT $1, 'physical_server', 'e-' || lpad(i::text, 6, '0'), 'host-' || i,
		       jsonb_build_object('ip', '10.' || ((i - (CASE WHEN i % 100 = 0 THEN 1 ELSE 0 END)) / 65536) || '.' ||
		           (((i - (CASE WHEN i % 100 = 0 THEN 1 ELSE 0 END)) / 256) % 256) || '.' || ((i - (CASE WHEN i % 100 = 0 THEN 1 ELSE 0 END)) % 256)), '[]'
		FROM generate_series(1, $2) AS i`, runID, n); err != nil {
		t.Fatal(err)
	}
	// 与 worker 相同的顺序：暂存写完 → 刷新统计 → 对账。
	if err := staging.RefreshStats(ctx); err != nil {
		t.Fatalf("refresh stats: %v", err)
	}
	start := time.Now()
	c, _, err := reconcile.ReconcileModel(ctx, biz.ReconcileParams{RunID: runID, ModelCode: "physical_server", Source: source, Env: "prod", Grace: time.Hour})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if c.Created != n-n/100 || c.Failed != n/100 {
		t.Fatalf("large batch = %+v", c)
	}
	if elapsed > 30*time.Second {
		t.Fatalf("large batch took %s", elapsed)
	}
	t.Logf("reconciled %d rows in %s", n, elapsed)
	assertUniqueKeysConsistent(t, d)

	// 第二轮同样的数据：全部命中、无变化（被跳过的重复行仍报告）。
	start = time.Now()
	c, _, err = reconcile.ReconcileModel(ctx, biz.ReconcileParams{RunID: runID, ModelCode: "physical_server", Source: source, Env: "prod", Grace: time.Hour})
	if err != nil || c.Created != 0 || c.Updated != 0 || c.Unchanged != n-n/100 || c.Failed != n/100 {
		t.Fatalf("second round = %+v err=%v", c, err)
	}
	if again := time.Since(start); again > 30*time.Second {
		t.Fatalf("second round took %s", again)
	} else {
		t.Logf("re-reconciled in %s", again)
	}
}
