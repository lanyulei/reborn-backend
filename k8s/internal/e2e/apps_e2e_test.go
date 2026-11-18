//go:build e2e

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
	"sigs.k8s.io/yaml"
)

// TestApps Helm 应用与命名空间配额的端到端验证（直接走数据面的内部接口）。除 TestE2E 的环境变量外还需要
// E2E_HELM_REPO：一个 HTTP Helm 仓库的地址，其中有 Chart demo 的 0.1.0 与 0.2.0 两个版本
// （values：greeting / replicas / auth.adminPassword；渲染出一个 ConfigMap 与一个 Deployment，
// 设置了 auth.adminPassword 时另有一个 Secret <release>-auth，键 adminPassword）。数据面须以
// K8S_HELM_ALLOW_LOOPBACK=1 启动（仓库在本机回环地址上）。
func TestApps(t *testing.T) {
	ctx := context.Background()
	e := &env{t: t, url: strings.TrimRight(must(t, "E2E_PLANE_URL"), "/"), key: []byte(must(t, "E2E_INTERNAL_KEY"))}
	db, err := pgx.Connect(ctx, must(t, "E2E_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	e.db = db
	e.admin = k8scontract.Identity{UserID: adminID, Username: "admin", IsAdmin: true, ClientIP: "127.0.0.1", RequestID: "apps"}
	e.user = k8scontract.Identity{UserID: userID, Username: "dev", RoleIDs: []string{roleID}, ClientIP: "127.0.0.1"}
	const repoID = "018f3e1a-0e2e-7000-8000-0000000000d1"
	suffix := fmt.Sprint(time.Now().Unix() % 100000)
	nsA, nsB := "apps-a-"+suffix, "apps-b-"+suffix

	var kc struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
				CA     string `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
		Users []struct {
			User struct {
				Cert string `json:"client-certificate-data"`
				Key  string `json:"client-key-data"`
			} `json:"user"`
		} `json:"users"`
	}
	raw, err := os.ReadFile(must(t, "E2E_KUBECONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &kc); err != nil || len(kc.Clusters) == 0 || len(kc.Users) == 0 {
		t.Fatalf("parse kubeconfig: %v", err)
	}
	b64 := func(s string) string { out, _ := base64.StdEncoding.DecodeString(s); return string(out) }
	box, err := secretbox.New(must(t, "E2E_MASTER_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := json.Marshal(k8scontract.Credential{ClientCert: b64(kc.Users[0].User.Cert), ClientKey: b64(kc.Users[0].User.Key)})
	sealed, err := box.Seal(string(plain))
	if err != nil {
		t.Fatal(err)
	}
	e.exec(t, `INSERT INTO k8s_cluster (id, name, access_mode, agent_location, api_server, ca_cert, credential, env)
	           VALUES ($1, 'e2e', 2, 0, $2, $3, $4, 'test') ON CONFLICT (id) DO NOTHING`,
		clusterID, kc.Clusters[0].Cluster.Server, b64(kc.Clusters[0].Cluster.CA), sealed)
	e.exec(t, `DELETE FROM k8s_role_scope WHERE cluster_id = $1`, clusterID)
	e.exec(t, `INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ($1, $2, $3, 1)`, roleID, clusterID, nsA)
	e.exec(t, `INSERT INTO k8s_helm_repo (id, name, type, url) VALUES ($1, 'e2e', 1, $2)
	           ON CONFLICT (id) DO UPDATE SET url = EXCLUDED.url, version = k8s_helm_repo.version + 1, delete_time = NULL, enabled = true`,
		repoID, strings.TrimRight(must(t, "E2E_HELM_REPO"), "/"))
	e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1`)
	e.eventually(t, "cluster ready for requests", 90*time.Second, func() bool {
		code, _ := e.op(e.admin, k8scontract.OpNamespaces, k8scontract.Empty{}, nil)
		return code == 200
	})
	nsRef := func(name string) k8scontract.Ref { return ref(k8scontract.GVRNamespace, "", name) }
	for _, ns := range []string{nsA, nsB} {
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: k8scontract.Ref{Version: "v1", Resource: "namespaces"},
			YAML: "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: " + ns + "\n"}, nil)
	}
	t.Cleanup(func() {
		for _, ns := range []string{nsA, nsB} {
			e.op(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: nsRef(ns)}, nil)
		}
	})

	t.Run("repo", func(t *testing.T) {
		var test k8scontract.HelmRepoTestResponse
		e.ok(e.admin, k8scontract.OpHelmRepoTest, k8scontract.HelmRepoTestRequest{Type: 1, URL: os.Getenv("E2E_HELM_REPO")}, &test)
		if !test.OK || test.Charts < 1 {
			t.Fatalf("repo test: %+v", test)
		}
		e.ok(e.admin, k8scontract.OpHelmRepoTest, k8scontract.HelmRepoTestRequest{Type: 1, URL: "http://169.254.169.254/x"}, &test)
		if test.OK || !strings.Contains(test.Error, "not allowed") {
			t.Fatalf("link-local repo must be refused: %+v", test)
		}
		var charts k8scontract.HelmChartsResponse
		e.ok(e.user, k8scontract.OpHelmCharts, k8scontract.HelmChartsRequest{RepoID: repoID, Keyword: "demo"}, &charts)
		if len(charts.Items) != 1 || charts.Items[0].Versions[0].Version != "0.2.0" {
			t.Fatalf("charts: %+v", charts)
		}
		var detail k8scontract.HelmChartResponse
		e.ok(e.admin, k8scontract.OpHelmChart, k8scontract.HelmChartRequest{RepoID: repoID, Chart: "demo", Version: "0.1.0"}, &detail)
		if !strings.Contains(detail.Values, "greeting") {
			t.Fatalf("chart detail: %+v", detail)
		}
		e.deny(e.admin, k8scontract.OpHelmChart, k8scontract.HelmChartRequest{RepoID: repoID, Chart: "../x", Version: "0.1.0"}, http.StatusBadRequest)
		e.deny(e.admin, k8scontract.OpHelmChart, k8scontract.HelmChartRequest{RepoID: repoID, Chart: "demo", Version: "9.9.9"}, http.StatusNotFound)
	})

	apply := func(ns, name, version, values string) k8scontract.HelmApplyRequest {
		return k8scontract.HelmApplyRequest{Namespace: ns, Name: name, RepoID: repoID, Chart: "demo", Version: version, Values: values}
	}
	rel := k8scontract.HelmReleaseRequest{Namespace: nsA, Name: "web"}

	t.Run("lifecycle", func(t *testing.T) {
		var pv k8scontract.HelmPreviewResponse
		e.ok(e.admin, k8scontract.OpHelmPreview, apply(nsA, "web", "0.1.0", "greeting: hi\n"), &pv)
		if !strings.Contains(pv.Manifest, "kind: Deployment") {
			t.Fatalf("preview manifest: %s", pv.Manifest)
		}
		var list k8scontract.HelmListResponse
		e.ok(e.admin, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: nsA}, &list)
		if len(list.Items) != 0 {
			t.Fatalf("preview must not create a release: %+v", list)
		}
		var res k8scontract.HelmApplyResponse
		e.ok(e.admin, k8scontract.OpHelmInstall, apply(nsA, "web", "0.1.0", "greeting: hi\nauth:\n  adminPassword: s3cr3t-value\n"), &res)
		if res.Pending || res.Revision != 1 || res.Status != "deployed" {
			t.Fatalf("install: %+v", res)
		}
		e.deny(e.admin, k8scontract.OpHelmInstall, apply(nsA, "web", "0.1.0", ""), http.StatusConflict)
		up := apply(nsA, "web", "0.2.0", "greeting: hello2\nreplicas: 2\n")
		up.Upgrade = true
		e.ok(e.admin, k8scontract.OpHelmPreview, up, &pv)
		if !pv.Upgrade || !strings.Contains(pv.CurrentValues, "greeting: hi") || !strings.Contains(pv.Manifest, "replicas: 2") || !strings.Contains(pv.CurrentManifest, "replicas: 1") {
			t.Fatalf("upgrade preview: %+v", pv)
		}
		e.ok(e.admin, k8scontract.OpHelmUpgrade, up, &res)
		if res.Revision != 2 {
			t.Fatalf("upgrade: %+v", res)
		}
		e.ok(e.admin, k8scontract.OpHelmRollback, k8scontract.HelmRollbackRequest{Namespace: nsA, Name: "web", Revision: 1}, &res)
		if res.Revision != 3 {
			t.Fatalf("rollback: %+v", res)
		}
		var detail k8scontract.HelmReleaseDetail
		e.ok(e.admin, k8scontract.OpHelmGet, rel, &detail)
		if detail.Release.ChartVersion != "0.1.0" || len(detail.History) != 3 || len(detail.Resources) != 3 || detail.Resources[0].Ref.Resource == "" {
			t.Fatalf("detail: %+v", detail)
		}
		// 给人看的 values 与清单：口令与 Secret 内容都是掩码。
		secretB64 := base64.StdEncoding.EncodeToString([]byte("s3cr3t-value"))
		if strings.Contains(detail.Values, "s3cr3t-value") || !strings.Contains(detail.Values, k8scontract.MaskedValue) {
			t.Fatalf("stored values must be masked: %s", detail.Values)
		}
		if strings.Contains(detail.Manifest, secretB64) || strings.Contains(detail.Manifest, "s3cr3t-value") || !strings.Contains(detail.Manifest, "kind: Secret") {
			t.Fatalf("secret data in the manifest must be masked: %s", detail.Manifest)
		}
	})

	t.Run("masked-values-keep-current", func(t *testing.T) {
		m := "******"
		// 安装时没有「现值」：掩码被拒绝，绝不当作真实的值。
		e.deny(e.admin, k8scontract.OpHelmInstall, apply(nsA, "masked", "0.1.0", "auth:\n  adminPassword: '"+m+"'\n"), http.StatusBadRequest)
		var res k8scontract.HelmApplyResponse
		e.ok(e.admin, k8scontract.OpHelmInstall, apply(nsA, "masked", "0.1.0", "greeting: one\nauth:\n  adminPassword: keep-this-pw\n"), &res)
		// 升级：把界面上看到的（掩码过的）values 原样提交回来，只改别的字段。
		up := apply(nsA, "masked", "0.1.0", "greeting: two\nauth:\n  adminPassword: '"+m+"'\n")
		up.Upgrade = true
		var pv k8scontract.HelmPreviewResponse
		e.ok(e.admin, k8scontract.OpHelmPreview, up, &pv)
		if strings.Contains(pv.Values, "keep-this-pw") || strings.Contains(pv.Manifest, base64.StdEncoding.EncodeToString([]byte("keep-this-pw"))) {
			t.Fatalf("preview must not reveal the stored value: %+v", pv)
		}
		e.ok(e.admin, k8scontract.OpHelmUpgrade, up, &res)
		var rv k8scontract.SecretRevealResponse
		e.ok(e.admin, k8scontract.OpSecretReveal, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRSecret, nsA, "masked-auth")}, &rv)
		if rv.Data["adminPassword"] != "keep-this-pw" {
			t.Fatalf("the masked value must keep the current one, got %q", rv.Data["adminPassword"])
		}
		// 掩码填在现值里没有的路径上：拒绝并指出路径。
		bad := apply(nsA, "masked", "0.1.0", "extra:\n  token: '"+m+"'\n")
		bad.Upgrade = true
		if code, msg := e.op(e.admin, k8scontract.OpHelmUpgrade, bad, nil); code != http.StatusBadRequest || !strings.Contains(msg, "extra.token") {
			t.Fatalf("mask on an unknown path: %d %s", code, msg)
		}
		e.ok(e.admin, k8scontract.OpHelmUninstall, k8scontract.HelmReleaseRequest{Namespace: nsA, Name: "masked"}, nil)
	})

	t.Run("scope", func(t *testing.T) {
		// view：读不了 Secret，看不了 Release，也装不了。
		e.eventually(t, "view scope applied", 30*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), nil)
			return code == 200
		})
		if code, msg := e.op(e.user, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: nsA}, nil); code != http.StatusForbidden || !strings.Contains(msg, "Secrets") {
			t.Fatalf("view list: %d %s", code, msg)
		}
		e.deny(e.user, k8scontract.OpHelmInstall, apply(nsA, "dev1", "0.1.0", ""), http.StatusForbidden)
		e.exec(t, `UPDATE k8s_role_scope SET level = 2 WHERE role_id = $1 AND cluster_id = $2`, roleID, clusterID)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "edit scope applied", 40*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: nsA}, nil)
			return code == 200
		})
		var list k8scontract.HelmListResponse
		e.ok(e.user, k8scontract.OpHelmList, k8scontract.HelmListRequest{}, &list)
		if len(list.PartialNamespaces) != 1 || list.PartialNamespaces[0] != nsA {
			t.Fatalf("all-namespaces list must be partial: %+v", list)
		}
		e.ok(e.user, k8scontract.OpHelmInstall, apply(nsA, "dev1", "0.1.0", ""), nil)
		e.deny(e.user, k8scontract.OpHelmInstall, apply(nsB, "dev2", "0.1.0", ""), http.StatusForbidden)
		e.deny(e.user, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: nsB}, http.StatusForbidden)
		e.ok(e.admin, k8scontract.OpHelmList, k8scontract.HelmListRequest{Namespace: nsB}, &list)
		if len(list.Items) != 0 {
			t.Fatalf("refused install must leave nothing behind: %+v", list)
		}
		e.ok(e.user, k8scontract.OpHelmUninstall, k8scontract.HelmReleaseRequest{Namespace: nsA, Name: "dev1"}, nil)
	})

	t.Run("quota", func(t *testing.T) {
		e.ok(e.admin, k8scontract.OpManifestApply, k8scontract.ManifestRequest{
			YAML: "apiVersion: v1\nkind: ResourceQuota\nmetadata: {name: default, namespace: " + nsA + "}\nspec:\n  hard: {pods: '10'}\n"}, nil)
		var out k8scontract.NamespaceQuotaResponse
		e.eventually(t, "quota status", 20*time.Second, func() bool {
			e.ok(e.user, k8scontract.OpNamespaceQuota, k8scontract.NamespaceQuotaRequest{Namespace: nsA}, &out)
			return len(out.Items) == 1 && len(out.Items[0].Items) == 1 && out.Items[0].Items[0].Hard == "10"
		})
		e.deny(e.user, k8scontract.OpNamespaceQuota, k8scontract.NamespaceQuotaRequest{Namespace: nsB}, http.StatusForbidden)
	})

	t.Run("uninstall-and-audit", func(t *testing.T) {
		e.ok(e.admin, k8scontract.OpHelmUninstall, rel, nil)
		e.deny(e.admin, k8scontract.OpHelmGet, rel, http.StatusNotFound)
		rows, err := db.Query(ctx, `SELECT action, name, result, COALESCE(patch::text, '') FROM k8s_audit
		                            WHERE cluster_id = $1 AND resource = $2 AND namespace = ANY($3) ORDER BY create_time`,
			clusterID, k8scontract.HelmReleaseResource, []string{nsA, nsB})
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		seen := map[string]int{}
		for rows.Next() {
			var action, name, patch string
			var result int32
			if err := rows.Scan(&action, &name, &result, &patch); err != nil {
				t.Fatal(err)
			}
			seen[fmt.Sprintf("%s/%s/%d", action, name, result)]++
			if strings.Contains(patch, "s3cr3t-value") {
				t.Fatalf("secret-looking value leaked into audit: %s", patch)
			}
		}
		for _, want := range []string{
			"helm_install/web/1", "helm_install/web/2", "helm_upgrade/web/1", "helm_rollback/web/1", "helm_uninstall/web/1",
			"helm_install/dev1/2", "helm_install/dev1/1", "helm_install/dev2/2", "helm_uninstall/dev1/1",
		} {
			if seen[want] != 1 {
				t.Errorf("audit %s: got %d rows, want exactly 1 (all: %v)", want, seen[want], seen)
			}
		}
	})
}
