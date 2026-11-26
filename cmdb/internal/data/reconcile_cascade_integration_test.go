//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

// stageRound 写一轮暂存（各类型共用同一个运行 ID，与真实同步任务一致），返回运行 ID。
func stageRound(t *testing.T, d *Data, rows ...*biz.Resource) string {
	t.Helper()
	runID := mustID(t)
	if err := NewStagingRepo(d).Append(context.Background(), runID, rows); err != nil {
		t.Fatal(err)
	}
	return runID
}

// reconcileOne 对账一轮里的一个类型。
func reconcileOne(t *testing.T, d *Data, runID, model, source string) biz.Counts {
	t.Helper()
	c, _, err := NewReconcileRepo(d).ReconcileModel(context.Background(), ownerParams(t, d, runID, model, source))
	if err != nil {
		t.Fatalf("reconcile %s: %v", model, err)
	}
	return c
}

// finishRound 解析一轮的关系并清掉暂存。
func finishRound(t *testing.T, d *Data, runID, source string) {
	t.Helper()
	ctx := context.Background()
	if err := NewReconcileRepo(d).ReconcileRelations(ctx, runID, source); err != nil {
		t.Fatalf("reconcile relations: %v", err)
	}
	if err := NewStagingRepo(d).Clear(ctx, runID); err != nil {
		t.Fatal(err)
	}
}

// installedComponentRows 「host-1 上安装了 nic-1」这条关系的行数。
func installedComponentRows(t *testing.T, d *Data) int {
	t.Helper()
	var n int
	if err := d.Pool.QueryRow(context.Background(), `SELECT count(*) FROM cmdb_relation r
		JOIN cmdb_ci s ON s.id = r.src_id JOIN cmdb_ci t ON t.id = r.dst_id
		WHERE r.type_code = $1 AND s.external_id = 'host-1' AND t.external_id = 'nic-1'`, "installed_component").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// cascadeFixture 一台物理机与它上面的一块网卡（配件声明「安装于」这台物理机：installed_component，内置关系类型里
// 唯一 cascade_retire 的一个——主机退役时配件随之退役）。第一轮两者都在，关系已建立。
func cascadeFixture(t *testing.T) (d *Data, source string, host *biz.Resource, nic func() *biz.Resource) {
	t.Helper()
	d = newIntegrationData(t)
	// 夹具只预置了主机 / 虚拟机 / 云主机：补上配件类型与「安装于」（内置关系类型里 cascade_retire 的一个）。
	ctx := context.Background()
	for _, q := range []string{
		`INSERT INTO cmdb_model (code, name, category, builtin) VALUES ('component', '配件', 'component', true)`,
		`INSERT INTO cmdb_model_field (id, model_code, code, name, type, "unique", owner, list_visible)
		 VALUES (gen_random_uuid(), 'component', 'serial_number', '序列号', 'string', true, 'both', true)`,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete)
		 VALUES ('installed_component', '安装配件', '安装的配件', '安装于', '*', 'component', 'one_to_many', 'cascade_retire')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed cascade metadata: %v", err)
		}
	}
	source = cmdbcontract.SourceOf(insertSource(t, d, 0, true, time.Now()))
	host = &biz.Resource{
		ModelCode: "physical_server", ExternalID: "host-1", Name: "h1", Attrs: map[string]any{"ip": "10.0.0.10", "cpu_cores": 8},
	}
	nic = func() *biz.Resource {
		return &biz.Resource{
			ModelCode: "component", ExternalID: "nic-1", Name: "nic-1",
			Attrs: map[string]any{"serial_number": "NIC-001"},
			Relations: []cmdbcontract.StagingRelation{{
				Type: "installed_component", Direction: cmdbcontract.DirectionIncoming, PeerModel: "physical_server", PeerExternalID: "host-1",
			}},
		}
	}
	runID := stageRound(t, d, host, nic())
	if h, c := reconcileOne(t, d, runID, "physical_server", source), reconcileOne(t, d, runID, "component", source); h.Created != 1 || c.Created != 1 {
		t.Fatalf("first round = host %+v, component %+v", h, c)
	}
	finishRound(t, d, runID, source)
	if n := installedComponentRows(t, d); n != 1 {
		t.Fatalf("installed_component relation rows = %d, want 1", n)
	}
	return d, source, host, nic
}

// 随上游级联退役的下游资源带着「因消失而退役」的凭据（missing_since）：上游恢复、它也仍被采集到时，和消失退役的
// 资源一样重新激活——与「只重新激活消失退役的资源」这次修复之前的行为一致。不记这个凭据的话，它会被当成人工
// 退役的，上游恢复后永远停在退役。人工退役的不受影响（见 reconcile_semantics_integration_test.go）。
func TestIntegration_ReconcileCascadeRetiredStaysReactivatable(t *testing.T) {
	d, source, host, nic := cascadeFixture(t)
	load := func() (*ciRow, *ciRow) {
		return loadCI(t, d, "physical_server", source, "host-1"), loadCI(t, d, "component", source, "nic-1")
	}

	// 第 2 轮：物理机消失（宽限期 0 → 立即退役）；网卡随它级联退役，并带上凭据。
	//（这一轮只对账物理机：网卡自己的对账放到了下一轮——两轮之间进程被杀、或类型对账顺序靠后时就是这样。）
	runID := stageRound(t, d, nic())
	if got := reconcileOne(t, d, runID, "physical_server", source); got.Disappeared != 1 || got.Retired != 2 {
		t.Fatalf("second round: host = %+v; want it retired by disappearance and the component cascade-retired", got)
	}
	h, c := load()
	if h.Status != "retired" || h.Missing == nil || c.Status != "retired" || c.Missing == nil {
		t.Fatalf("host = %+v, component = %+v; want both retired with the disappear marker set", h, c)
	}
	finishRound(t, d, runID, source)

	// 第 3 轮：物理机回来，网卡照常被采集到 → 两者都重新激活。
	runID = stageRound(t, d, host, nic())
	if h, c := reconcileOne(t, d, runID, "physical_server", source), reconcileOne(t, d, runID, "component", source); h.Updated != 1 || h.Failed != 0 || c.Updated != 1 || c.Failed != 0 {
		t.Fatalf("third round = host %+v, component %+v; want both reactivated", h, c)
	}
	h, c = load()
	if h.Status != "active" || h.Missing != nil || c.Status != "active" || c.Missing != nil {
		t.Fatalf("host = %+v, component = %+v; want both active again", h, c)
	}
}

// 级联退役的下游在同一轮里仍被采集到时，轮到它自己的对账就被重新激活（修复前的行为，保持不变）。
func TestIntegration_ReconcileCascadeRetiredReportedAgainIsReactivated(t *testing.T) {
	d, source, _, nic := cascadeFixture(t)

	runID := stageRound(t, d, nic())
	if got := reconcileOne(t, d, runID, "physical_server", source); got.Retired != 2 {
		t.Fatalf("second round: host = %+v; want the component cascade-retired", got)
	}
	if c := loadCI(t, d, "component", source, "nic-1"); c.Status != "retired" || c.Missing == nil {
		t.Fatalf("component = %+v; want it cascade-retired with the marker", c)
	}
	if got := reconcileOne(t, d, runID, "component", source); got.Updated != 1 || got.Failed != 0 {
		t.Fatalf("second round: component = %+v; want it reactivated because the source still reports it", got)
	}
	if c := loadCI(t, d, "component", source, "nic-1"); c.Status != "active" || c.Missing != nil {
		t.Fatalf("component = %+v; want it active again", c)
	}
	finishRound(t, d, runID, source)
}
