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

// ownerParams 按类型字段归属组装对账参数（与 worker 的分流一致：manual → ManualKeys，sync → SyncKeys）。
func ownerParams(t *testing.T, d *Data, runID, model, source string) biz.ReconcileParams {
	t.Helper()
	fields, err := NewMetaRepo(d).Fields(context.Background(), model)
	if err != nil {
		t.Fatal(err)
	}
	p := biz.ReconcileParams{RunID: runID, ModelCode: model, Source: source, Env: "prod"}
	for _, f := range fields {
		switch f.Owner {
		case cmdbcontract.FieldOwnerManual:
			p.ManualKeys = append(p.ManualKeys, f.Code)
		case cmdbcontract.FieldOwnerSync:
			p.SyncKeys = append(p.SyncKeys, f.Code)
		}
	}
	return p
}

// reconcileRound 写一轮暂存并对账单一类型，结束后清暂存。
func reconcileRound(t *testing.T, d *Data, model, source string, rows []*biz.Resource) biz.Counts {
	t.Helper()
	ctx := context.Background()
	staging, runID := NewStagingRepo(d), mustID(t)
	if err := staging.Append(ctx, runID, rows); err != nil {
		t.Fatal(err)
	}
	c, _, err := NewReconcileRepo(d).ReconcileModel(ctx, ownerParams(t, d, runID, model, source))
	if err != nil {
		t.Fatalf("reconcile %s: %v", model, err)
	}
	if err := staging.Clear(ctx, runID); err != nil {
		t.Fatal(err)
	}
	return c
}

// 重新激活只针对「因消失而退役」的资源：人工退役的同步资源再次被采集到时保持退役（属性照常更新）；
// 消失退役的资源重新出现时激活，且被名称冲突挡住的那一轮不会丢掉「消失退役」的凭据。
func TestIntegration_ReconcileReactivatesOnlyDisappearRetired(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	source := cmdbcontract.SourceOf(insertSource(t, d, 0, true, time.Now()))
	const model = "virtual_machine"
	vm := func(eid, name, ip string, vcpu int) *biz.Resource {
		return &biz.Resource{ModelCode: model, ExternalID: eid, Name: name, Attrs: map[string]any{"ip": ip, "vcpu": vcpu}}
	}
	a, b, c := vm("vm-a", "a", "10.0.0.1", 2), vm("vm-b", "b", "10.0.0.2", 2), vm("vm-c", "c", "10.0.0.3", 2)

	if got := reconcileRound(t, d, model, source, []*biz.Resource{a, b, c}); got.Created != 3 {
		t.Fatalf("round1 = %+v", got)
	}
	// 人工退役 vm-a（core 的状态流转只改 status，不碰 missing_since）。
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_ci SET status = 'retired', version = version + 1 WHERE source = $1 AND external_id = 'vm-a'", source); err != nil {
		t.Fatal(err)
	}

	// 第 2 轮：vm-b 消失（grace 0 → 立即退役）；vm-a 仍被采集到，但它是人工退役的 → 保持退役、算无变化。
	got := reconcileRound(t, d, model, source, []*biz.Resource{a, c})
	if got.Updated != 0 || got.Unchanged != 2 || got.Disappeared != 1 || got.Retired != 1 || got.Failed != 0 {
		t.Fatalf("round2 = %+v", got)
	}
	ra := loadCI(t, d, model, source, "vm-a")
	if ra.Status != "retired" || ra.Missing != nil || ra.Stale || countHistory(t, d, ra.ID, "transition") != 0 {
		t.Fatalf("manually retired resource must stay retired: %+v", ra)
	}
	if rb := loadCI(t, d, model, source, "vm-b"); rb.Status != "retired" || rb.Missing == nil {
		t.Fatalf("vm-b must be retired by disappearance: %+v", rb)
	}

	// 第 3 轮：全部出现，vm-a 属性有变化 → 属性更新但仍退役（历史是 update 而非 transition）；vm-b 重新激活。
	a2 := vm("vm-a", "a", "10.0.0.1", 8)
	got = reconcileRound(t, d, model, source, []*biz.Resource{a2, b, c})
	if got.Updated != 2 || got.Unchanged != 1 || got.Failed != 0 {
		t.Fatalf("round3 = %+v", got)
	}
	ra = loadCI(t, d, model, source, "vm-a")
	if ra.Status != "retired" || ra.Attrs["vcpu"] != float64(8) || countHistory(t, d, ra.ID, "transition") != 0 || countHistory(t, d, ra.ID, "update") != 1 {
		t.Fatalf("manually retired resource: attrs follow the source, status stays: %+v", ra)
	}
	rb := loadCI(t, d, model, source, "vm-b")
	if rb.Status != "active" || rb.Missing != nil || countHistory(t, d, rb.ID, "transition") != 2 {
		t.Fatalf("disappear-retired resource must be reactivated: %+v", rb)
	}

	// 第 4 轮：vm-c 消失退役。第 5 轮它带着被人工资源占用的名称回来 → 更新被挡；凭据（missing_since）
	// 必须留着，否则第 6 轮名称恢复后就再也激活不了。
	if got = reconcileRound(t, d, model, source, []*biz.Resource{a2, b}); got.Retired != 1 {
		t.Fatalf("round4 = %+v", got)
	}
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name, source, attrs) VALUES ($1, $2, 'taken', 'manual', '{}')", mustID(t), model); err != nil {
		t.Fatal(err)
	}
	got = reconcileRound(t, d, model, source, []*biz.Resource{a2, b, vm("vm-c", "taken", "10.0.0.3", 2)})
	if got.Failed != 1 || got.Updated != 0 {
		t.Fatalf("round5 = %+v", got)
	}
	if rc := loadCI(t, d, model, source, "vm-c"); rc.Status != "retired" || rc.Missing == nil {
		t.Fatalf("blocked reactivation must keep the disappear marker: %+v", rc)
	}
	if got = reconcileRound(t, d, model, source, []*biz.Resource{a2, b, c}); got.Updated != 1 || got.Failed != 0 {
		t.Fatalf("round6 = %+v", got)
	}
	if rc := loadCI(t, d, model, source, "vm-c"); rc.Status != "active" || rc.Missing != nil {
		t.Fatalf("vm-c must be reactivated once unblocked: %+v", rc)
	}
	// 人工退役的始终没被动过。
	if ra = loadCI(t, d, model, source, "vm-a"); ra.Status != "retired" {
		t.Fatalf("vm-a = %+v", ra)
	}
}

// sync 归属字段以采集结果为准：来源侧消失的键被删除并记入历史；both 归属的键消失时保留；
// manual 归属的键同步既不写也不删；暂存行没有任何可写属性时不删任何键。
func TestIntegration_ReconcileSyncOwnedKeysFollowSource(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	source := cmdbcontract.SourceOf(insertSource(t, d, 0, true, time.Now()))
	const model = "cloud_host"
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_model_field (id, model_code, code, name, type, "unique", owner, list_visible) VALUES
		(gen_random_uuid(), 'cloud_host', 'spec', '规格', 'string', false, 'sync', false),
		(gen_random_uuid(), 'cloud_host', 'state', '状态', 'string', false, 'sync', false),
		(gen_random_uuid(), 'cloud_host', 'region', '地域', 'string', false, 'both', false),
		(gen_random_uuid(), 'cloud_host', 'note', '备注', 'string', false, 'manual', false)`); err != nil {
		t.Fatal(err)
	}
	host := func(attrs map[string]any) []*biz.Resource {
		return []*biz.Resource{{ModelCode: model, ExternalID: "i-1", Name: "ecs-1", Attrs: attrs}}
	}

	if got := reconcileRound(t, d, model, source, host(map[string]any{"instance_id": "i-1", "spec": "4c8g", "state": "running", "region": "cn"})); got.Created != 1 {
		t.Fatalf("round1 = %+v", got)
	}
	ci := loadCI(t, d, model, source, "i-1")
	if _, err := d.Pool.Exec(ctx, `UPDATE cmdb_ci SET attrs = attrs || '{"note":"keep"}', version = version + 1 WHERE id = $1`, ci.ID); err != nil {
		t.Fatal(err)
	}

	// 第 2 轮：state（sync）与 region（both）都不再给出 → 只删 state。
	got := reconcileRound(t, d, model, source, host(map[string]any{"instance_id": "i-1", "spec": "4c8g"}))
	if got.Updated != 1 || got.Unchanged != 0 || got.Failed != 0 {
		t.Fatalf("round2 = %+v", got)
	}
	ci = loadCI(t, d, model, source, "i-1")
	if _, ok := ci.Attrs["state"]; ok {
		t.Fatalf("sync-owned key missing from the source must be removed: %v", ci.Attrs)
	}
	if ci.Attrs["region"] != "cn" || ci.Attrs["note"] != "keep" || ci.Attrs["spec"] != "4c8g" || ci.Attrs["instance_id"] != "i-1" {
		t.Fatalf("both / manual keys and present sync keys must be kept: %v", ci.Attrs)
	}
	if ci.Version != 3 || ci.UniqueKeys != 1 {
		t.Fatalf("ci after round2 = %+v", ci)
	}
	// 历史：只记被删的键，before 为旧值、after 为 null。
	var before, after string
	var regionInDiff bool
	if err := d.Pool.QueryRow(ctx,
		`SELECT diff -> 'before' ->> 'attrs', diff -> 'after' ->> 'attrs', diff::text LIKE '%region%'
		 FROM cmdb_ci_history WHERE ci_id = $1 AND action = 'update'`, ci.ID).Scan(&before, &after, &regionInDiff); err != nil {
		t.Fatalf("removed key must be recorded as one update history: %v", err)
	}
	if strings.ReplaceAll(before, " ", "") != `{"state":"running"}` || strings.ReplaceAll(after, " ", "") != `{"state":null}` || regionInDiff {
		t.Fatalf("history diff: before=%s after=%s regionInDiff=%v", before, after, regionInDiff)
	}

	// 第 3 轮：同样的采集结果 → 无变化（删除是幂等的，不会每轮都记一条历史）。
	got = reconcileRound(t, d, model, source, host(map[string]any{"instance_id": "i-1", "spec": "4c8g"}))
	if got.Updated != 0 || got.Unchanged != 1 || got.Failed != 0 {
		t.Fatalf("round3 = %+v", got)
	}

	// 第 4 轮：这次没取到任何可写属性（只有一个人工归属键，同步不写）→ 一个键都不删、也不覆盖人工值。
	got = reconcileRound(t, d, model, source, host(map[string]any{"note": "SYNC"}))
	if got.Updated != 0 || got.Unchanged != 1 || got.Failed != 0 {
		t.Fatalf("round4 (empty attrs) = %+v", got)
	}
	ci = loadCI(t, d, model, source, "i-1")
	if ci.Attrs["spec"] != "4c8g" || ci.Attrs["note"] != "keep" || ci.Attrs["instance_id"] != "i-1" || ci.Version != 3 {
		t.Fatalf("empty staged attrs must not wipe sync-owned keys: %+v", ci)
	}

	// 第 5 轮：state 又有值了 → 写回。
	got = reconcileRound(t, d, model, source, host(map[string]any{"instance_id": "i-1", "spec": "4c8g", "state": "stopped"}))
	if got.Updated != 1 {
		t.Fatalf("round5 = %+v", got)
	}
	if ci = loadCI(t, d, model, source, "i-1"); ci.Attrs["state"] != "stopped" || ci.Attrs["region"] != "cn" {
		t.Fatalf("ci after round5 = %+v", ci)
	}
}
