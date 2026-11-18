//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
)

// TestChange 已批准变更单在数据面的执行（不经 core：直接取计划、封存、写入 k8s_change 的「已批准」行）。
// 需先跑过 TestPlane 或 TestApps（集群行与 Helm 仓库行已存在）；环境变量同 TestApps。
func TestChange(t *testing.T) {
	if os.Getenv("E2E_AGENT") != "" {
		t.Skip("direct-access only")
	}
	ctx := context.Background()
	e := &env{t: t, url: strings.TrimRight(must(t, "E2E_PLANE_URL"), "/"), key: []byte(must(t, "E2E_INTERNAL_KEY"))}
	db, err := pgx.Connect(ctx, must(t, "E2E_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	e.db = db
	e.admin = k8scontract.Identity{UserID: adminID, Username: "admin", IsAdmin: true, ClientIP: "127.0.0.1", RequestID: "chg"}
	box, err := secretbox.New(must(t, "E2E_MASTER_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	const repoID = "018f3e1a-0e2e-7000-8000-0000000000d1"
	ns := fmt.Sprint("chg-", time.Now().Unix()%100000)
	e.eventually(t, "cluster ready for requests", 90*time.Second, func() bool {
		code, _ := e.op(e.admin, k8scontract.OpNamespaces, k8scontract.Empty{}, nil)
		return code == 200
	})
	e.ok(e.admin, k8scontract.OpManifestApply, k8scontract.ManifestRequest{YAML: fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata: {name: %[1]s}
---
apiVersion: batch/v1
kind: CronJob
metadata: {name: tick, namespace: %[1]s}
spec:
  schedule: "0 0 1 1 *"
  suspend: true
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
          - {name: main, image: registry.k8s.io/pause:3.10, imagePullPolicy: IfNotPresent}
`, ns)}, nil)
	t.Cleanup(func() {
		e.op(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: ref(k8scontract.GVRNamespace, "", ns)}, nil)
		_, _ = db.Exec(ctx, `DELETE FROM k8s_change WHERE cluster_id = $1`, clusterID)
	})
	requester, _ := json.Marshal(k8scontract.ChangeRequester{UserID: adminID, Username: "admin", IsAdmin: true})
	seq := 0
	// submit 把计划封存成一张「已批准」的变更单；mutate 可在封存前改动执行计划。
	submit := func(kind int32, exec k8scontract.ChangeExec, started bool) string {
		seq++
		id := fmt.Sprintf("018f3e1a-0e2e-7000-8000-%012d", time.Now().UnixNano()%1_000_000_000_000+int64(seq))
		if exec.ChangeID == "" {
			exec.ChangeID, exec.ClusterID = id, clusterID
		}
		plain, _ := json.Marshal(exec)
		sealed, err := box.Seal(string(plain))
		if err != nil {
			t.Fatal(err)
		}
		status, lease, result := k8scontract.ChangeApproved, any(nil), `{}`
		if started { // 模拟：上一个执行器在开始提交之后失联，租约已过期
			status, lease, result = k8scontract.ChangeExecuting, time.Now().Add(-time.Minute), `{"started":true}`
		}
		e.exec(t, `INSERT INTO k8s_change (id, kind, cluster_id, cluster_name, action, op, exec, status, requester_id, requester_name, requester,
		               approve_time, expire_time, lease_owner, lease_until, result)
		           VALUES ($1, $2, $3, 'e2e', 'update', $4, $5, $6, $7, 'admin', $8, now(), now() + interval '1 hour', $9, $10, $11::jsonb)`,
			id, kind, clusterID, exec.Op, sealed, status, adminID, requester, map[bool]string{true: "dead-instance", false: ""}[started], lease, result)
		return id
	}
	wait := func(id string) (int32, string, string) {
		var status int32
		var message, result string
		e.eventually(t, "change "+id+" finished", 90*time.Second, func() bool {
			_ = db.QueryRow(ctx, `SELECT status, message, result::text FROM k8s_change WHERE id = $1`, id).Scan(&status, &message, &result)
			return k8scontract.ChangeFinal(status)
		})
		return status, message, result
	}
	plan := func(op string, body any) k8scontract.ChangeExec {
		raw, _ := json.Marshal(body)
		var out k8scontract.ChangePlanResponse
		e.ok(e.admin, k8scontract.OpChangePlan, k8scontract.ChangePlanRequest{Op: op, Body: raw}, &out)
		return out.Exec
	}
	jobs := func() []string {
		var out k8scontract.ListResponse
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRJob, ns), &out)
		names := []string{}
		for _, r := range out.Rows {
			names = append(names, r.Name)
		}
		return names
	}
	cj := ref(k8scontract.GVRCronJob, ns, "tick")

	t.Run("trigger-runs-once", func(t *testing.T) {
		exec := plan(k8scontract.OpCronTrigger, k8scontract.ObjectRequest{Ref: cj})
		id := submit(k8scontract.ChangeKindOp, exec, false)
		status, msg, result := wait(id)
		if status != k8scontract.ChangeSucceeded || !strings.Contains(strings.ReplaceAll(result, " ", ""), `"started":true`) {
			t.Fatalf("trigger: status %d message %q result %s", status, msg, result)
		}
		got := jobs()
		if len(got) != 1 || !strings.Contains(got[0], "-chg-") {
			t.Fatalf("job name must be derived from the change id: %v", got)
		}
		// 同一张变更单被再次放回「已批准」（最坏情况：结果丢失）：Job 重名，不会再触发一个。
		e.exec(t, `UPDATE k8s_change SET status = $2, result = '{}', finish_time = NULL WHERE id = $1`, id, k8scontract.ChangeApproved)
		if status, _, _ := wait(id); status != k8scontract.ChangeFailed {
			t.Fatalf("re-executed trigger must fail on the duplicate job name, status %d", status)
		}
		if got := jobs(); len(got) != 1 {
			t.Fatalf("a change executed twice created %d jobs", len(got))
		}
	})

	t.Run("interrupted-after-start-is-not-rerun", func(t *testing.T) {
		exec := plan(k8scontract.OpCronTrigger, k8scontract.ObjectRequest{Ref: cj})
		id := submit(k8scontract.ChangeKindOp, exec, true)
		status, msg, _ := wait(id)
		if status != k8scontract.ChangeFailed || !strings.Contains(msg, "may be partly applied") {
			t.Fatalf("status %d message %q", status, msg)
		}
		if got := jobs(); len(got) != 1 {
			t.Fatalf("an interrupted change must not be re-run: jobs %v", got)
		}
	})

	t.Run("plan-bound-to-change", func(t *testing.T) {
		exec := plan(k8scontract.OpCronSuspend, k8scontract.SuspendRequest{Ref: cj, Suspend: true})
		exec.ChangeID, exec.ClusterID = "018f3e1a-0e2e-7000-8000-ffffffffffff", clusterID // 别的变更单的计划
		status, msg, _ := wait(submit(k8scontract.ChangeKindOp, exec, false))
		if status != k8scontract.ChangeFailed || !strings.Contains(msg, "does not belong") {
			t.Fatalf("status %d message %q", status, msg)
		}
	})

	t.Run("namespace-request-idempotent", func(t *testing.T) {
		name := ns + "-req"
		req := &k8scontract.NamespaceRequest{Name: name, Quota: map[string]string{"pods": "5"}}
		id := submit(k8scontract.ChangeKindNamespace, k8scontract.ChangeExec{Namespace: req}, false)
		if status, msg, _ := wait(id); status != k8scontract.ChangeSucceeded {
			t.Fatalf("namespace request: %d %s", status, msg)
		}
		t.Cleanup(func() {
			e.op(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: ref(k8scontract.GVRNamespace, "", name)}, nil)
		})
		// 重试同一张变更单（上次建到一半的情形）：认出是自己建的命名空间，成功；配额仍只有一份。
		e.exec(t, `UPDATE k8s_change SET status = $2, result = '{}', finish_time = NULL WHERE id = $1`, id, k8scontract.ChangeApproved)
		if status, msg, _ := wait(id); status != k8scontract.ChangeSucceeded {
			t.Fatalf("retry of a namespace request must be idempotent: %d %s", status, msg)
		}
		var q k8scontract.ListResponse
		e.ok(e.admin, k8scontract.OpList, k8scontract.ListRequest{Version: "v1", Resource: "resourcequotas", Namespace: name}, &q)
		if len(q.Rows) != 1 {
			t.Fatalf("quota objects: %d", len(q.Rows))
		}
		// 另一张变更单申请同名命名空间：被占用。
		other := submit(k8scontract.ChangeKindNamespace, k8scontract.ChangeExec{Namespace: req}, false)
		if status, msg, _ := wait(other); status != k8scontract.ChangeFailed || !strings.Contains(msg, "already exists") {
			t.Fatalf("namespace taken by another change: %d %s", status, msg)
		}
		// 保留名与标签在数据面再校验一次。
		for _, bad := range []*k8scontract.NamespaceRequest{{Name: "kube-evil"}, {Name: ns + "-lbl", Labels: map[string]string{"istio-injection": "disabled"}}} {
			if status, msg, _ := wait(submit(k8scontract.ChangeKindNamespace, k8scontract.ChangeExec{Namespace: bad}, false)); status != k8scontract.ChangeFailed {
				t.Fatalf("%+v must be refused by the plane: %d %s", bad, status, msg)
			}
		}
	})

	t.Run("helm-pinned", func(t *testing.T) {
		var n int
		_ = db.QueryRow(ctx, `SELECT count(*) FROM k8s_helm_repo WHERE id = $1 AND delete_time IS NULL`, repoID).Scan(&n)
		if n == 0 {
			t.Skip("helm repository row missing: run TestApps first")
		}
		install := k8scontract.HelmApplyRequest{Namespace: ns, Name: "pinned", RepoID: repoID, Chart: "demo", Version: "0.1.0", Values: "greeting: hi\nauth:\n  adminPassword: pin-pw\n"}
		raw, _ := json.Marshal(install)
		var out k8scontract.ChangePlanResponse
		e.ok(e.admin, k8scontract.OpChangePlan, k8scontract.ChangePlanRequest{Op: k8scontract.OpHelmInstall, Body: raw}, &out)
		if out.Exec.Helm == nil || out.Exec.Helm.ChartDigest == "" || out.Exec.Helm.ManifestHash == "" {
			t.Fatalf("helm plan must pin the chart digest and the rendered manifest: %+v", out.Exec.Helm)
		}
		// 审批人看得到 values 与清单，且口令与 Secret 内容是掩码。
		shown := out.Display.Items[0].AfterYAML
		if !strings.Contains(shown, "kind: Deployment") || !strings.Contains(shown, "greeting: hi") || strings.Contains(shown, "pin-pw") || strings.Contains(shown, "cGluLXB3") {
			t.Fatalf("change display for helm: %s", shown)
		}
		// 审批之后 Chart 被换了（摘要不同）：拒绝执行。
		tampered := out.Exec
		pin := *tampered.Helm
		pin.ChartDigest = strings.Repeat("0", 64)
		tampered.Helm = &pin
		if status, msg, _ := wait(submit(k8scontract.ChangeKindOp, tampered, false)); status != k8scontract.ChangeFailed || !strings.Contains(msg, "digest changed") {
			t.Fatalf("tampered chart: %d %s", status, msg)
		}
		// 渲染结果与审批时不同：拒绝执行。
		drift := out.Exec
		pin2 := *drift.Helm
		pin2.ManifestHash = strings.Repeat("0", 64)
		drift.Helm = &pin2
		if status, msg, _ := wait(submit(k8scontract.ChangeKindOp, drift, false)); status != k8scontract.ChangeFailed || !strings.Contains(msg, "renders differently") {
			t.Fatalf("drifted manifest: %d %s", status, msg)
		}
		var list k8scontract.HelmListResponse
		e.ok(e.admin, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: ns}, &list)
		if len(list.Items) != 0 {
			t.Fatalf("refused changes must not install anything: %+v", list.Items)
		}
		// 未被改动的计划：执行成功，并且等到了真正的结果（不是「后台继续」）。
		status, msg, result := wait(submit(k8scontract.ChangeKindOp, out.Exec, false))
		if status != k8scontract.ChangeSucceeded || !strings.Contains(strings.ReplaceAll(result, " ", ""), `"started":true`) {
			t.Fatalf("helm install through approval: %d %s %s", status, msg, result)
		}
		e.ok(e.admin, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: ns}, &list)
		if len(list.Items) != 1 || list.Items[0].Status != "deployed" {
			t.Fatalf("release after approved install: %+v", list.Items)
		}
		e.ok(e.admin, k8scontract.OpHelmUninstall, k8scontract.HelmReleaseRequest{Namespace: ns, Name: "pinned"}, nil)
	})
}
