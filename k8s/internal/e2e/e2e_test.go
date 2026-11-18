//go:build e2e

// Package e2e 端到端验证：对着一个真实集群与一个已启动的数据面，经内部接口走完读、写、权限、日志、
// 终端的主要路径。运行方式见 README「端到端验证」。
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"sigs.k8s.io/yaml"
)

const (
	clusterID = "018f3e1a-0e2e-7000-8000-000000000001"
	adminID   = "018f3e1a-0e2e-7000-8000-0000000000a1"
	userID    = "018f3e1a-0e2e-7000-8000-0000000000b1"
	roleID    = "018f3e1a-0e2e-7000-8000-0000000000c1"
	image     = "docker.io/library/alpine:3.20"
)

type env struct {
	t     *testing.T
	url   string
	key   []byte
	db    *pgx.Conn
	admin k8scontract.Identity
	user  k8scontract.Identity
}

func must(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s is required", name)
	}
	return v
}

func (e *env) request(ctx context.Context, id k8scontract.Identity, path string, body any) (*http.Response, error) {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.url+path, bytes.NewReader(raw))
	id.IssuedAt = time.Now().Unix()
	if err := k8scontract.SignIdentity(req.Header, e.key, id); err != nil {
		return nil, err
	}
	req.Header.Set(k8scontract.ClusterHeader, clusterID)
	return http.DefaultClient.Do(req)
}

// op 调用一个普通操作；返回 HTTP 状态码、错误原因（失败时）并把成功响应解码进 out。
func (e *env) op(id k8scontract.Identity, op string, body, out any) (int, string) {
	e.t.Helper()
	resp, err := e.request(context.Background(), id, k8scontract.InternalOpPath+op, body)
	if err != nil {
		e.t.Fatalf("%s: %v", op, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var eb k8scontract.ErrorBody
		_ = json.Unmarshal(raw, &eb)
		return resp.StatusCode, eb.Reason + ": " + eb.Message
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			e.t.Fatalf("%s: decode %s: %v", op, raw, err)
		}
	}
	return resp.StatusCode, ""
}

func (e *env) ok(id k8scontract.Identity, op string, body, out any) {
	e.t.Helper()
	if code, msg := e.op(id, op, body, out); code != http.StatusOK {
		e.t.Fatalf("%s: want 200, got %d %s", op, code, msg)
	}
}

func (e *env) deny(id k8scontract.Identity, op string, body any, want int) {
	e.t.Helper()
	if code, msg := e.op(id, op, body, nil); code != want {
		e.t.Fatalf("%s: want %d, got %d %s", op, want, code, msg)
	}
}

type sseEvent struct{ event, id, data string }

// stream 打开一条流，把事件送进通道；cancel 结束它。
func (e *env) stream(id k8scontract.Identity, op string, body any) (<-chan sseEvent, context.CancelFunc, int) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.request(ctx, id, k8scontract.InternalStreamPath+op, body)
	if err != nil {
		cancel()
		e.t.Fatalf("stream %s: %v", op, err)
	}
	ch := make(chan sseEvent, 4096)
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		close(ch)
		return ch, cancel, resp.StatusCode
	}
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		r := bufio.NewReaderSize(resp.Body, 1<<20)
		var ev sseEvent
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "":
				if ev.event != "" {
					ch <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, "event: "):
				ev.event = line[7:]
			case strings.HasPrefix(line, "id: "):
				ev.id = line[4:]
			case strings.HasPrefix(line, "data: "):
				ev.data = line[6:]
			}
		}
	}()
	return ch, cancel, http.StatusOK
}

func (e *env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.db.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("sql: %v", err)
	}
}

func (e *env) eventually(t *testing.T, what string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func ref(g k8scontract.GVR, ns, name string) k8scontract.Ref {
	return k8scontract.Ref{Group: g.Group, Version: g.Version, Resource: g.Resource, Namespace: ns, Name: name}
}

func listReq(g k8scontract.GVR, ns string) k8scontract.ListRequest {
	return k8scontract.ListRequest{Group: g.Group, Version: g.Version, Resource: g.Resource, Namespace: ns, PageSize: 100}
}

const manifest = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: %[1]s
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web, e2e: plane}}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
      - name: main
        image: %[2]s
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh","-c","i=0; while true; do i=$((i+1)); echo line-$i; echo line-$i-b; sleep 0.2; done"]
---
apiVersion: v1
kind: Secret
metadata: {name: creds, namespace: %[1]s}
stringData: {password: s3cr3t-value}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: conf, namespace: %[1]s}
data: {k: v}
---
apiVersion: batch/v1
kind: CronJob
metadata: {name: tick, namespace: %[1]s}
spec:
  schedule: "0 0 1 1 *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
          - {name: main, image: %[2]s, imagePullPolicy: IfNotPresent, command: ["/bin/sh","-c","echo done"]}
`

func TestPlane(t *testing.T) {
	ctx := context.Background()
	e := &env{t: t, url: strings.TrimRight(must(t, "E2E_PLANE_URL"), "/"), key: []byte(must(t, "E2E_INTERNAL_KEY"))}
	db, err := pgx.Connect(ctx, must(t, "E2E_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	e.db = db
	e.admin = k8scontract.Identity{UserID: adminID, Username: "admin", IsAdmin: true, ClientIP: "127.0.0.1", RequestID: "e2e"}
	e.user = k8scontract.Identity{UserID: userID, Username: "dev", RoleIDs: []string{roleID}, ClientIP: "127.0.0.1"}
	suffix := fmt.Sprint(time.Now().Unix() % 100000)
	nsA, nsB := "e2e-a-"+suffix, "e2e-b-"+suffix

	// ---- 登记集群 ----
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
	cred := k8scontract.Credential{ClientCert: b64(kc.Users[0].User.Cert), ClientKey: b64(kc.Users[0].User.Key)}
	server, ca := kc.Clusters[0].Cluster.Server, b64(kc.Clusters[0].Cluster.CA)

	t.Run("cluster-test", func(t *testing.T) {
		var out k8scontract.ClusterTestResponse
		e.ok(e.admin, k8scontract.OpClusterTest, k8scontract.ClusterTestRequest{APIServer: server, CACert: ca, Credential: cred}, &out)
		if !out.OK || out.Version == "" || len(out.Checks) < 10 {
			t.Fatalf("cluster test: %+v", out)
		}
		e.ok(e.admin, k8scontract.OpClusterTest, k8scontract.ClusterTestRequest{APIServer: server, CACert: ca, Credential: k8scontract.Credential{Token: "bogus"}}, &out)
		if out.OK || out.Error == "" {
			t.Fatalf("bogus credential must fail: %+v", out)
		}
	})

	box, err := secretbox.New(must(t, "E2E_MASTER_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := json.Marshal(cred)
	sealed, err := box.Seal(string(plain))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("E2E_AGENT") != "" {
		// Agent 接入：集群行不带地址与凭据，全部请求经隧道到达（需另行启动 Agent 并用注册令牌接入）。
		e.exec(t, `INSERT INTO k8s_cluster (id, name, access_mode, agent_location, env)
		           VALUES ($1, 'e2e', 1, 2, 'test') ON CONFLICT (id) DO UPDATE SET version = k8s_cluster.version + 1, update_time = now()`,
			clusterID)
	} else {
		e.exec(t, `INSERT INTO k8s_cluster (id, name, access_mode, agent_location, api_server, ca_cert, credential, env)
		           VALUES ($1, 'e2e', 2, 0, $2, $3, $4, 'test') ON CONFLICT (id) DO UPDATE SET credential = EXCLUDED.credential, version = k8s_cluster.version + 1, update_time = now()`,
			clusterID, server, ca, sealed)
	}
	e.exec(t, `DELETE FROM k8s_role_scope WHERE cluster_id = $1`, clusterID)
	e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1`)
	e.eventually(t, "cluster online", 60*time.Second, func() bool {
		var status int32
		var lastErr string
		_ = db.QueryRow(ctx, `SELECT status, last_error FROM k8s_cluster_runtime WHERE cluster_id = $1`, clusterID).Scan(&status, &lastErr)
		if lastErr != "" {
			t.Logf("cluster last_error: %s", lastErr)
		}
		return status == k8scontract.ClusterOnline
	})

	time.Sleep(8 * time.Second) // 重复运行时集群行被更新：等数据面按新配置重建连接
	e.eventually(t, "cluster ready for requests", 60*time.Second, func() bool {
		code, _ := e.op(e.admin, k8scontract.OpNamespaces, k8scontract.Empty{}, nil)
		return code == 200
	})

	t.Run("identity", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, e.url+k8scontract.InternalOpPath+k8scontract.OpNamespaces, strings.NewReader("{}"))
		req.Header.Set(k8scontract.ClusterHeader, clusterID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unsigned request: %d", resp.StatusCode)
		}
		_ = k8scontract.SignIdentity(req.Header, []byte(strings.Repeat("x", 32)), e.admin)
		req.Body = io.NopCloser(strings.NewReader("{}"))
		resp, _ = http.DefaultClient.Do(req)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong key: %d", resp.StatusCode)
		}
	})

	// ---- 管理员：建命名空间与工作负载 ----
	for _, ns := range []string{nsA, nsB} {
		var out k8scontract.ApplyResponse
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{
			Ref: ref(k8scontract.GVRNamespace, "", ""), YAML: "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: " + ns + "\n",
		}, &out)
	}
	defer func() {
		for _, ns := range []string{nsA, nsB} {
			e.op(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: ref(k8scontract.GVRNamespace, "", ns)}, nil)
		}
	}()
	t.Run("manifest", func(t *testing.T) {
		for _, ns := range []string{nsA, nsB} {
			var out k8scontract.ManifestResponse
			body := k8scontract.ManifestRequest{YAML: fmt.Sprintf(manifest, ns, image)}
			e.ok(e.admin, k8scontract.OpManifestPreview, body, &out)
			if !out.OK || len(out.Items) != 4 || !out.Items[0].Create || out.Items[0].AfterYAML == "" {
				t.Fatalf("manifest preview: %+v", out)
			}
			if strings.Contains(out.Items[1].AfterYAML, "s3cr3t") {
				t.Fatal("secret plaintext leaked into manifest preview")
			}
			e.ok(e.admin, k8scontract.OpManifestApply, body, &out)
			if !out.OK {
				t.Fatalf("manifest apply: %+v", out)
			}
		}
		var out k8scontract.ManifestResponse
		e.ok(e.admin, k8scontract.OpManifestPreview, k8scontract.ManifestRequest{YAML: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: Bad_Name, namespace: " + nsA + "}\n"}, &out)
		if out.OK || out.Items[0].Error == "" {
			t.Fatalf("invalid object must fail preview: %+v", out)
		}
	})

	var web k8scontract.Row
	t.Run("list-cached", func(t *testing.T) {
		e.eventually(t, "deployment ready", 90*time.Second, func() bool {
			var out k8scontract.ListResponse
			e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRDeployment, nsA), &out)
			if len(out.Rows) == 1 && out.Rows[0].State == k8scontract.StateOK {
				web = out.Rows[0]
				if !out.Capabilities.Cached || out.Total != 1 || len(out.Columns) == 0 {
					t.Fatalf("capabilities: %+v", out)
				}
				return true
			}
			return false
		})
		var out k8scontract.ListResponse
		req := listReq(k8scontract.GVRPod, "")
		req.Filters = map[string]string{k8scontract.FilterOwner: "nope"}
		e.ok(e.admin, k8scontract.OpList, req, &out)
		if out.Total != 0 {
			t.Fatalf("owner filter: %d", out.Total)
		}
		req = listReq(k8scontract.GVRPod, "")
		// 带上本用例专属的标签：共享集群里别人也可能有名字以 web- 开头的 Pod。
		req.Keyword, req.SortBy, req.SortDesc, req.PageSize, req.LabelSelector = "web-", "namespace", true, 1, "e2e=plane"
		e.ok(e.admin, k8scontract.OpList, req, &out)
		if out.Total != 2 || len(out.Rows) != 1 || out.Rows[0].Namespace != nsB {
			t.Fatalf("keyword/sort/page: total=%d rows=%+v", out.Total, out.Rows)
		}
		req = listReq(k8scontract.GVRPod, nsA)
		req.LabelSelector = "app=web"
		e.ok(e.admin, k8scontract.OpList, req, &out)
		if out.Total != 1 || out.Rows[0].Cells["status"].(map[string]any)["text"] != "Running" {
			t.Fatalf("label selector: %+v", out.Rows)
		}
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRNode, ""), &out)
		if out.Total != 1 || out.Rows[0].State != k8scontract.StateOK {
			t.Fatalf("nodes: %+v", out.Rows)
		}
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRSecret, nsA), &out)
		if raw, _ := json.Marshal(out); strings.Contains(string(raw), "s3cr3t") || out.Total != 1 {
			t.Fatalf("secret list: %s", raw)
		}
	})

	t.Run("list-uncached-and-crd", func(t *testing.T) {
		var out k8scontract.ListResponse
		sa := k8scontract.GVR{Version: "v1", Resource: "serviceaccounts"}
		e.ok(e.admin, k8scontract.OpList, listReq(sa, nsA), &out)
		if out.Capabilities.Cached || out.Total != -1 || len(out.Rows) == 0 || out.Rows[0].Name == "" {
			t.Fatalf("uncached list: %+v", out)
		}
		crd := `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: widgets.e2e.reborn.io}
spec:
  group: e2e.reborn.io
  scope: Namespaced
  names: {plural: widgets, singular: widget, kind: Widget}
  versions:
  - name: v1
    served: true
    storage: true
    additionalPrinterColumns: [{name: Size, type: integer, jsonPath: .spec.size}]
    schema: {openAPIV3Schema: {type: object, properties: {spec: {type: object, properties: {size: {type: integer}}}}}}
`
		crdGVR := k8scontract.GVR{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: ref(crdGVR, "", ""), YAML: crd}, nil)
		defer e.op(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: ref(crdGVR, "", "widgets.e2e.reborn.io")}, nil)
		widget := k8scontract.GVR{Group: "e2e.reborn.io", Version: "v1", Resource: "widgets"}
		e.eventually(t, "crd served", 30*time.Second, func() bool {
			code, _ := e.op(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{
				Ref: ref(widget, nsA, ""), YAML: "apiVersion: e2e.reborn.io/v1\nkind: Widget\nmetadata: {name: w1}\nspec: {size: 7}\n",
			}, nil)
			return code == 200 || code == 409
		})
		e.ok(e.admin, k8scontract.OpList, listReq(widget, nsA), &out)
		if len(out.Rows) != 1 || fmt.Sprint(out.Rows[0].Cells["Size"]) != "7" {
			t.Fatalf("crd list: %+v", out)
		}
		var res k8scontract.APIResourcesResponse
		e.ok(e.admin, k8scontract.OpAPIResources, k8scontract.APIResourcesRequest{Refresh: true}, &res)
		found := false
		for _, r := range res.Items {
			if r.Resource == "widgets" && r.CRD && !r.Cached {
				found = true
			}
		}
		if !found {
			t.Fatal("crd missing from api-resources")
		}
	})

	t.Run("get-preview-apply-delete", func(t *testing.T) {
		var obj k8scontract.ObjectResponse
		cm := ref(k8scontract.GVRConfigMap, nsA, "conf")
		e.ok(e.admin, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: cm}, &obj)
		if strings.Contains(obj.YAML, "managedFields") || obj.ResourceVersion == "" {
			t.Fatalf("get: %s", obj.YAML)
		}
		edited := strings.Replace(obj.YAML, "k: v", "k: v2", 1)
		var pv k8scontract.PreviewResponse
		e.ok(e.admin, k8scontract.OpPreview, k8scontract.ApplyRequest{Ref: cm, YAML: edited}, &pv)
		if pv.Create || !pv.Changed || !strings.Contains(pv.AfterYAML, "k: v2") || !strings.Contains(pv.BeforeYAML, "k: v") {
			t.Fatalf("preview: %+v", pv)
		}
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: cm, YAML: edited, ResourceVersion: pv.ResourceVersion}, nil)
		// 同一个旧版本再提交一次：乐观锁冲突。
		e.deny(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: cm, YAML: edited, ResourceVersion: pv.ResourceVersion}, http.StatusConflict)
		e.deny(e.admin, k8scontract.OpPreview, k8scontract.ApplyRequest{Ref: cm, YAML: strings.Replace(edited, "name: conf", "name: other", 1)}, http.StatusBadRequest)

		// Secret：读到的是掩码；原样提交不得把真实值覆盖成掩码。
		sec := ref(k8scontract.GVRSecret, nsA, "creds")
		e.ok(e.admin, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: sec}, &obj)
		if !strings.Contains(obj.YAML, k8scontract.MaskedValue) || strings.Contains(obj.YAML, "czNjcjN0") {
			t.Fatalf("secret must be masked: %s", obj.YAML)
		}
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: sec, YAML: obj.YAML}, nil)
		var rv k8scontract.SecretRevealResponse
		e.ok(e.admin, k8scontract.OpSecretReveal, k8scontract.ObjectRequest{Ref: sec}, &rv)
		if rv.Data["password"] != "s3cr3t-value" {
			t.Fatalf("secret value changed after masked round-trip: %+v", rv)
		}
		var evs k8scontract.EventsResponse
		e.ok(e.admin, k8scontract.OpEvents, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRDeployment, nsA, "web")}, &evs)
		if len(evs.Items) == 0 {
			t.Fatal("expected deployment events")
		}
		e.deny(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: cm, UID: "00000000-0000-0000-0000-000000000000"}, http.StatusConflict)
		e.ok(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: cm, UID: obj.UID[:0]}, nil)
		e.deny(e.admin, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: cm}, http.StatusNotFound)
	})

	t.Run("scope", func(t *testing.T) {
		// 没有任何授权：什么都看不到。
		e.deny(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), http.StatusForbidden)
		e.exec(t, `INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ($1, $2, $3, 1)`, roleID, clusterID, nsA)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "view scope synced", 40*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), nil)
			return code == 200
		})
		// 只读：另一个命名空间不可读；全部命名空间只返回有权限的那个。
		e.deny(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsB), http.StatusForbidden)
		e.deny(e.user, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRDeployment, nsB, "web")}, http.StatusForbidden)
		var out k8scontract.ListResponse
		e.ok(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, ""), &out)
		if len(out.PartialNamespaces) != 1 || out.PartialNamespaces[0] != nsA || out.Total != 1 || out.Rows[0].Namespace != nsA {
			t.Fatalf("all-namespaces for scoped user: %+v", out)
		}
		var nss k8scontract.NamespacesResponse
		e.ok(e.user, k8scontract.OpNamespaces, k8scontract.Empty{}, &nss)
		if nss.All || len(nss.Items) == 0 {
			t.Fatalf("namespaces: %+v", nss)
		}
		// 集群级只读（reborn:cluster-view）：能看节点。
		e.ok(e.user, k8scontract.OpList, listReq(k8scontract.GVRNode, ""), &out)
		// 只读：不能写、不能看 Secret（view 不含 secrets）、不能看明文、不能进终端。
		dep := ref(k8scontract.GVRDeployment, nsA, "web")
		e.deny(e.user, k8scontract.OpScale, k8scontract.ScaleRequest{Ref: dep, Replicas: 2}, http.StatusForbidden)
		e.deny(e.user, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: dep}, http.StatusForbidden)
		e.deny(e.user, k8scontract.OpList, listReq(k8scontract.GVRSecret, nsA), http.StatusForbidden)
		e.deny(e.user, k8scontract.OpSecretReveal, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRSecret, nsA, "creds")}, http.StatusForbidden)
		e.deny(e.user, k8scontract.OpNodeCordon, k8scontract.CordonRequest{Name: "reborn-k8s-control-plane", Unschedulable: true}, http.StatusForbidden)
		_, _, code := e.stream(e.user, k8scontract.StreamWatch, k8scontract.WatchRequest{Version: "v1", Resource: "pods", Namespace: nsB})
		if code != http.StatusForbidden {
			t.Fatalf("watch on foreign namespace: %d", code)
		}
		// 伪造身份头里的管理员标记不改变结果以外的东西：非管理员带上别人的角色 ID 才有别人的权限，
		// 而身份由 core 签名——这里验证的是「同一用户不带角色时无权限」。
		noRole := e.user
		noRole.RoleIDs = nil
		e.deny(noRole, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), http.StatusForbidden)

		// 提升到开发档位：可以伸缩。
		e.exec(t, `UPDATE k8s_role_scope SET level = 2 WHERE role_id = $1 AND cluster_id = $2`, roleID, clusterID)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "edit scope synced", 40*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpScale, k8scontract.ScaleRequest{Ref: dep, Replicas: 2}, nil)
			return code == 200
		})
		e.deny(e.user, k8scontract.OpScale, k8scontract.ScaleRequest{Ref: ref(k8scontract.GVRDeployment, nsB, "web"), Replicas: 2}, http.StatusForbidden)
		var synced int64
		_ = db.QueryRow(ctx, `SELECT scope_version FROM k8s_cluster_runtime WHERE cluster_id = $1`, clusterID).Scan(&synced)
		if synced == 0 {
			t.Fatal("scope_version not written back")
		}

		// Secret / ConfigMap 的列表来自只取元数据的缓存：能列出，且没有「类型」列（它不在元数据里）。
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRSecret, nsA), &out)
		if out.Total < 1 || !out.Capabilities.Cached {
			t.Fatalf("secret list from the metadata cache: %+v", out)
		}
		for _, c := range out.Columns {
			if c.Key == "type" {
				t.Fatal("the secret type column must be gone (metadata-only cache)")
			}
		}
		// 已保存的凭据只发往已保存的地址。
		var test k8scontract.ClusterTestResponse
		e.ok(e.admin, k8scontract.OpClusterTest, k8scontract.ClusterTestRequest{ClusterID: clusterID, APIServer: "https://127.0.0.1:1"}, &test)
		if test.OK || !strings.Contains(test.Error, "re-enter the credential") {
			t.Fatalf("saved credential must not be sent to another address: %+v", test)
		}
	})

	t.Run("namespace-admin-revocation", func(t *testing.T) {
		if os.Getenv("E2E_AGENT") != "" {
			t.Skip("uses kubectl against the cluster directly")
		}
		kubectl := func(args ...string) (string, error) {
			cmd := exec.Command("kubectl", append([]string{"--kubeconfig", must(t, "E2E_KUBECONFIG")}, args...)...)
			raw, err := cmd.CombinedOutput()
			return string(raw), err
		}
		binding := "reborn:" + roleID + ":admin"
		// 命名空间管理员档位：绑定的是平台维护的 reborn:ns-admin，不是内置的 admin。
		e.exec(t, `UPDATE k8s_role_scope SET level = 3 WHERE role_id = $1 AND cluster_id = $2`, roleID, clusterID)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "namespace admin binding", 60*time.Second, func() bool {
			out, err := kubectl("-n", nsA, "get", "rolebinding", binding, "-o", "jsonpath={.roleRef.name}")
			return err == nil && strings.TrimSpace(out) == "reborn:ns-admin"
		})
		e.eventually(t, "namespace admin can still write workloads", 30*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpScale, k8scontract.ScaleRequest{Ref: ref(k8scontract.GVRDeployment, nsA, "web"), Replicas: 2}, nil)
			return code == 200
		})
		// 不能自建绑定（否则可以把权限绑给自己的平台用户名，收回授权后依然有效）。
		rb := fmt.Sprintf(`apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: keep-my-access
  namespace: %s
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: edit}
subjects:
- {apiGroup: rbac.authorization.k8s.io, kind: User, name: "reborn:%s"}
`, nsA, userID)
		rbRef := k8scontract.Ref{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings", Namespace: nsA}
		e.deny(e.user, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: rbRef, YAML: rb}, http.StatusForbidden)
		// 也改不了平台的绑定（摘标签）。由集群管理员摘掉标签，模拟「标签被人动过」：收回授权后绑定仍须被删除。
		if out, err := kubectl("-n", nsA, "label", "rolebinding", binding, k8scontract.ManagedByLabel+"-"); err != nil {
			t.Fatalf("remove label: %v %s", err, out)
		}
		e.exec(t, `DELETE FROM k8s_role_scope WHERE role_id = $1 AND cluster_id = $2`, roleID, clusterID)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "relabelled binding removed after revocation", 60*time.Second, func() bool {
			out, err := kubectl("-n", nsA, "get", "rolebinding", binding)
			return err != nil && strings.Contains(out, "NotFound")
		})
		e.eventually(t, "access revoked", 40*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), nil)
			return code == http.StatusForbidden
		})
		// 恢复为开发档位，后续用例照常进行。
		e.exec(t, `INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ($1, $2, $3, 2)`, roleID, clusterID, nsA)
		e.exec(t, `UPDATE k8s_cfg_version SET version = version + 1 WHERE name = 'k8s_role_scope'`)
		e.eventually(t, "edit scope restored", 60*time.Second, func() bool {
			code, _ := e.op(e.user, k8scontract.OpList, listReq(k8scontract.GVRPod, nsA), nil)
			return code == 200
		})
	})

	t.Run("quick-ops", func(t *testing.T) {
		dep := ref(k8scontract.GVRDeployment, nsA, "web")
		e.ok(e.admin, k8scontract.OpScale, k8scontract.ScaleRequest{Ref: dep, Replicas: 1}, nil)
		e.ok(e.admin, k8scontract.OpSetImage, k8scontract.SetImageRequest{Ref: dep, Container: "main", Image: "registry.k8s.io/pause:3.10"}, nil)
		e.deny(e.admin, k8scontract.OpSetImage, k8scontract.SetImageRequest{Ref: dep, Container: "nope", Image: "x"}, http.StatusBadRequest)
		var revs k8scontract.RevisionsResponse
		e.eventually(t, "two revisions", 20*time.Second, func() bool {
			e.ok(e.admin, k8scontract.OpRevisions, k8scontract.ObjectRequest{Ref: dep}, &revs)
			return len(revs.Items) == 2 && revs.Items[0].Current
		})
		if !revs.Items[0].Current || revs.Items[0].Images[0] != "registry.k8s.io/pause:3.10" || revs.Items[1].Revision != 1 {
			t.Fatalf("revisions: %+v", revs.Items)
		}
		e.ok(e.admin, k8scontract.OpRollback, k8scontract.RollbackRequest{Ref: dep, Revision: 1}, nil)
		var obj k8scontract.ObjectResponse
		e.ok(e.admin, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: dep}, &obj)
		if !strings.Contains(obj.YAML, image) {
			t.Fatalf("rollback did not restore the image: %s", obj.YAML)
		}
		e.ok(e.admin, k8scontract.OpPause, k8scontract.PauseRequest{Ref: dep, Paused: true}, nil)
		e.ok(e.admin, k8scontract.OpPause, k8scontract.PauseRequest{Ref: dep, Paused: false}, nil)
		e.ok(e.admin, k8scontract.OpRestart, k8scontract.ObjectRequest{Ref: dep}, nil)
		e.deny(e.admin, k8scontract.OpRestart, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRService, nsA, "x")}, http.StatusBadRequest)
		cj := ref(k8scontract.GVRCronJob, nsA, "tick")
		var job k8scontract.ApplyResponse
		e.ok(e.admin, k8scontract.OpCronTrigger, k8scontract.ObjectRequest{Ref: cj}, &job)
		if job.Kind != "Job" || !strings.HasPrefix(job.Ref.Name, "tick-manual-") {
			t.Fatalf("trigger: %+v", job)
		}
		e.ok(e.admin, k8scontract.OpCronSuspend, k8scontract.SuspendRequest{Ref: cj, Suspend: true}, nil)
		node := "reborn-k8s-control-plane"
		e.ok(e.admin, k8scontract.OpNodeCordon, k8scontract.CordonRequest{Name: node, Unschedulable: true}, nil)
		e.ok(e.admin, k8scontract.OpNodeCordon, k8scontract.CordonRequest{Name: node, Unschedulable: false}, nil)
		var dp k8scontract.DrainPreviewResponse
		e.ok(e.admin, k8scontract.OpDrainPreview, k8scontract.DrainRequest{Name: node}, &dp)
		actions := map[string]int{}
		for _, p := range dp.Pods {
			actions[p.Action]++
		}
		if actions["skip"] == 0 || actions["evict"] == 0 || len(dp.Pods) < 5 {
			t.Fatalf("drain preview: %+v", actions)
		}
		// 排空：有「无控制器 + emptyDir」的 Pod 时，按缺省选项被阻止——在停止调度之前就拒绝，节点不受影响。
		// （真正的排空会驱逐这个单节点集群上的全部 Pod，这里不执行。）
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{Ref: ref(k8scontract.GVRPod, nsA, ""), YAML: `apiVersion: v1
kind: Pod
metadata: {name: naked}
spec:
  terminationGracePeriodSeconds: 1
  containers: [{name: main, image: ` + image + `, imagePullPolicy: IfNotPresent, command: ["sleep", "3600"], volumeMounts: [{name: tmp, mountPath: /tmp}]}]
  volumes: [{name: tmp, emptyDir: {}}]
`}, nil)
		e.eventually(t, "naked pod scheduled", 30*time.Second, func() bool {
			e.ok(e.admin, k8scontract.OpDrainPreview, k8scontract.DrainRequest{Name: node}, &dp)
			return dp.Blocked
		})
		if _, _, code := e.stream(e.admin, k8scontract.StreamDrain, k8scontract.DrainRequest{Name: node}); code != http.StatusConflict {
			t.Fatalf("blocked drain: %d", code)
		}
		e.ok(e.admin, k8scontract.OpDrainPreview, k8scontract.DrainRequest{Name: node, Force: true, DeleteEmptyDir: true}, &dp)
		if dp.Blocked {
			t.Fatal("force + delete emptyDir must unblock the drain")
		}
		var nodes k8scontract.ListResponse
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRNode, ""), &nodes)
		if nodes.Rows[0].State != k8scontract.StateOK {
			t.Fatalf("a refused drain must not cordon the node: %+v", nodes.Rows[0].Cells["status"])
		}
		e.ok(e.admin, k8scontract.OpDelete, k8scontract.DeleteRequest{Ref: ref(k8scontract.GVRPod, nsA, "naked")}, nil)
		// 驱逐 nsB 的 Pod（会被 Deployment 重建）。
		var pods k8scontract.ListResponse
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRPod, nsB), &pods)
		e.ok(e.admin, k8scontract.OpPodEvict, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRPod, nsB, pods.Rows[0].Name)}, nil)
	})

	// 等回滚 + 重启的滚动发布彻底结束：工作负载正常，且命名空间里只剩一个 Pod（旧 Pod 已删除）。
	var pod string
	e.eventually(t, "rollout finished", 120*time.Second, func() bool {
		var deps, pods k8scontract.ListResponse
		e.ok(e.admin, k8scontract.OpList, listReq(k8scontract.GVRDeployment, nsA), &deps)
		req := listReq(k8scontract.GVRPod, nsA)
		req.LabelSelector = "app=web"
		e.ok(e.admin, k8scontract.OpList, req, &pods)
		if len(deps.Rows) != 1 || deps.Rows[0].State != k8scontract.StateOK || pods.Total != 1 || pods.Rows[0].State != k8scontract.StateOK {
			return false
		}
		pod = pods.Rows[0].Name
		return true
	})
	_ = web

	t.Run("watch", func(t *testing.T) {
		ch, cancel, code := e.stream(e.admin, k8scontract.StreamWatch, k8scontract.WatchRequest{Version: "v1", Resource: "configmaps", Namespace: nsA})
		defer cancel()
		if code != 200 {
			t.Fatalf("watch: %d", code)
		}
		time.Sleep(500 * time.Millisecond)
		e.ok(e.admin, k8scontract.OpApply, k8scontract.ApplyRequest{
			Ref: ref(k8scontract.GVRConfigMap, nsA, ""), YAML: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: w}\ndata: {a: b}\n",
		}, nil)
		deadline := time.After(10 * time.Second)
		for got := false; !got; {
			select {
			case ev := <-ch:
				got = ev.event == k8scontract.EventChange
			case <-deadline:
				t.Fatal("no change event")
			}
		}
	})

	t.Run("log-resume", func(t *testing.T) {
		req := k8scontract.PodLogRequest{Namespace: nsA, Pod: pod, Follow: true, TailLines: 5}
		ch, cancel, code := e.stream(e.user, k8scontract.StreamPodLog, req)
		if code != 200 {
			t.Fatalf("log: %d", code)
		}
		var lines []string
		cursor := ""
		for len(lines) < 12 {
			select {
			case ev := <-ch:
				if ev.event != k8scontract.EventLog {
					continue
				}
				var l k8scontract.LogLine
				_ = json.Unmarshal([]byte(ev.data), &l)
				lines, cursor = append(lines, l.Text), ev.id
			case <-time.After(20 * time.Second):
				t.Fatalf("log stalled after %d lines", len(lines))
			}
		}
		cancel()
		time.Sleep(1500 * time.Millisecond) // 断线期间容器继续输出
		req.Cursor = cursor
		ch, cancel, code = e.stream(e.user, k8scontract.StreamPodLog, req)
		defer cancel()
		if code != 200 {
			t.Fatalf("resume log: %d", code)
		}
		for len(lines) < 30 {
			select {
			case ev, open := <-ch:
				if !open {
					t.Fatalf("resumed log stream closed after %d lines", len(lines))
				}
				if ev.event == k8scontract.EventEnd {
					t.Fatalf("resumed log stream ended: %s", ev.data)
				}
				if ev.event == k8scontract.EventLog {
					var l k8scontract.LogLine
					_ = json.Unmarshal([]byte(ev.data), &l)
					lines = append(lines, l.Text)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("resumed log stalled after %d lines", len(lines))
			}
		}
		// 容器按 line-N、line-N-b 成对输出：续接前后拼起来必须连续，不丢不重。
		var n int
		fmt.Sscanf(lines[0], "line-%d", &n)
		start := 0
		if strings.HasSuffix(lines[0], "-b") {
			start, n = 1, n+1
		}
		for i := start; i+1 < len(lines); i += 2 {
			if lines[i] != fmt.Sprintf("line-%d", n) || lines[i+1] != fmt.Sprintf("line-%d-b", n) {
				t.Fatalf("log not contiguous at %d: %q %q (want line-%d)\n%v", i, lines[i], lines[i+1], n, lines)
			}
			n++
		}
		// 关键字过滤 + 非跟随：读完即 end。
		ch2, cancel2, _ := e.stream(e.user, k8scontract.StreamPodLog, k8scontract.PodLogRequest{Namespace: nsA, Pod: pod, TailLines: 20, Keyword: "-b"})
		defer cancel2()
		got, ended := 0, false
		for ev := range ch2 {
			switch ev.event {
			case k8scontract.EventLog:
				if !strings.Contains(ev.data, "-b") {
					t.Fatalf("keyword filter leaked: %s", ev.data)
				}
				got++
			case k8scontract.EventEnd:
				ended = strings.Contains(ev.data, k8scontract.EndCompleted)
			}
		}
		if got == 0 || !ended {
			t.Fatalf("non-follow log: got=%d ended=%v", got, ended)
		}
		// 工作负载级日志。
		ch3, cancel3, code := e.stream(e.admin, k8scontract.StreamWorkloadLog, k8scontract.WorkloadLogRequest{Ref: ref(k8scontract.GVRDeployment, nsA, "web"), TailLines: 2})
		defer cancel3()
		if code != 200 {
			t.Fatalf("workload log: %d", code)
		}
		sawPods, sawLine := false, false
		timeout := time.After(15 * time.Second)
		for !(sawPods && sawLine) {
			select {
			case ev := <-ch3:
				sawPods = sawPods || ev.event == k8scontract.EventPods
				sawLine = sawLine || (ev.event == k8scontract.EventLog && strings.Contains(ev.data, pod))
			case <-timeout:
				t.Fatalf("workload log: pods=%v line=%v", sawPods, sawLine)
			}
		}
		e.deny(e.user, k8scontract.OpGet, k8scontract.ObjectRequest{Ref: ref(k8scontract.GVRPod, nsB, "x")}, http.StatusForbidden)
	})

	t.Run("exec-detach-resume", func(t *testing.T) {
		dial := func(id k8scontract.Identity, p k8scontract.ExecParams) (*websocket.Conn, error) {
			raw, _ := json.Marshal(p)
			h := http.Header{}
			id.IssuedAt = time.Now().Unix()
			_ = k8scontract.SignIdentity(h, e.key, id)
			h.Set(k8scontract.ClusterHeader, clusterID)
			u := "ws" + strings.TrimPrefix(e.url, "http") + k8scontract.InternalExecPath + "?params=" + base64.RawURLEncoding.EncodeToString(raw)
			ws, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: h})
			return ws, err
		}
		// 读帧直到输出里出现 want；返回会话状态帧。
		read := func(ws *websocket.Conn, want string, d time.Duration) (string, []k8scontract.ExecStatus) {
			rctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			var out strings.Builder
			var sts []k8scontract.ExecStatus
			for {
				_, data, err := ws.Read(rctx)
				if err != nil {
					return out.String(), sts
				}
				switch data[0] {
				case k8scontract.FrameOutput:
					out.Write(data[1:])
				case k8scontract.FrameStatus:
					var st k8scontract.ExecStatus
					_ = json.Unmarshal(data[1:], &st)
					sts = append(sts, st)
				}
				if want != "" && strings.Contains(out.String(), want) {
					return out.String(), sts
				}
			}
		}
		send := func(ws *websocket.Conn, s string) {
			_ = ws.Write(ctx, websocket.MessageBinary, append([]byte{k8scontract.FrameInput}, s...))
		}
		// 无权限的命名空间：进不去。
		ws, err := dial(e.user, k8scontract.ExecParams{Namespace: nsB, Pod: "whatever"})
		if err == nil {
			_, sts := read(ws, "", 5*time.Second)
			if len(sts) == 0 || sts[0].State != k8scontract.ExecClosed || sts[0].Reason != k8scontract.ReasonForbidden {
				t.Fatalf("exec into foreign namespace must be refused: %+v", sts)
			}
		}
		ws, err = dial(e.user, k8scontract.ExecParams{Namespace: nsA, Pod: pod, Cols: 100, Rows: 30})
		if err != nil {
			t.Fatal(err)
		}
		send(ws, "echo marker-$((40+2)); stty size\n")
		out, sts := read(ws, "100", 15*time.Second)
		if len(sts) == 0 || sts[0].State != k8scontract.ExecReady || sts[0].SessionID == "" || !strings.Contains(out, "marker-42") || !strings.Contains(out, "30 100") {
			t.Fatalf("exec: sts=%+v out=%q", sts, out)
		}
		sid := sts[0].SessionID
		// 异常断开（不是正常关闭）：会话保留。断线期间让容器里的命令继续产生输出。
		send(ws, "(sleep 1; echo while-detached) &\n")
		time.Sleep(200 * time.Millisecond)
		_ = ws.CloseNow()
		time.Sleep(2500 * time.Millisecond)
		// 别的用户接不回来。
		other := e.admin
		if ws2, err := dial(other, k8scontract.ExecParams{SessionID: sid}); err == nil {
			_, sts := read(ws2, "", 5*time.Second)
			if len(sts) == 0 || sts[0].State != k8scontract.ExecClosed {
				t.Fatalf("resume by another user must be refused: %+v", sts)
			}
		}
		ws, err = dial(e.user, k8scontract.ExecParams{SessionID: sid})
		if err != nil {
			t.Fatal(err)
		}
		out, sts = read(ws, "while-detached", 10*time.Second)
		if len(sts) == 0 || sts[0].State != k8scontract.ExecResumed || !strings.Contains(out, "while-detached") {
			t.Fatalf("resume: sts=%+v out=%q", sts, out)
		}
		send(ws, "exit\n")
		_, sts = read(ws, "", 10*time.Second)
		if len(sts) == 0 || sts[len(sts)-1].State != k8scontract.ExecClosed || sts[len(sts)-1].Reason != k8scontract.ExecEndContainerExit {
			t.Fatalf("exit: %+v", sts)
		}
		e.eventually(t, "exec session finished", 10*time.Second, func() bool {
			var reason string
			var in, outBytes int64
			_ = db.QueryRow(ctx, `SELECT end_reason, bytes_in, bytes_out FROM k8s_exec_session WHERE id = $1 AND end_time IS NOT NULL`, sid).Scan(&reason, &in, &outBytes)
			return reason == k8scontract.ExecEndContainerExit && in > 0 && outBytes > 0
		})
	})

	t.Run("audit", func(t *testing.T) {
		count := func(where string, args ...any) int {
			var n int
			if err := db.QueryRow(ctx, "SELECT count(*) FROM k8s_audit WHERE cluster_id = '"+clusterID+"' AND "+where, args...).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		for _, action := range []string{"create", "update", "delete", "scale", "set_image", "rollback", "restart", "pause", "resume", "trigger", "suspend", "cordon", "uncordon", "evict", "secret_reveal", "exec"} {
			if count("action = $1 AND result = 1", action) == 0 {
				t.Errorf("no successful audit row for %s", action)
			}
		}
		if count("result = 2 AND user_id = $1 AND action = 'scale'", userID) == 0 {
			t.Error("denied scale must be audited as failed")
		}
		if count("result = 2 AND user_id = $1 AND action = 'secret_reveal'", userID) == 0 {
			t.Error("denied secret reveal must be audited as failed")
		}
		if count("username = 'admin' AND client_ip = '127.0.0.1' AND request_id = 'e2e'") == 0 {
			t.Error("identity fields missing from audit")
		}
		var leaked int
		_ = db.QueryRow(ctx, `SELECT count(*) FROM k8s_audit WHERE before_obj::text LIKE '%czNjcjN0%' OR after_obj::text LIKE '%czNjcjN0%'
		                      OR patch::text LIKE '%czNjcjN0%' OR after_obj::text LIKE '%s3cr3t%' OR patch::text LIKE '%s3cr3t%' OR before_obj::text LIKE '%managedFields%'`).Scan(&leaked)
		if leaked != 0 {
			t.Errorf("%d audit rows leak secret data or managedFields", leaked)
		}
		if count("action = 'update' AND kind = 'ConfigMap' AND before_obj IS NOT NULL AND after_obj IS NOT NULL AND patch IS NOT NULL") == 0 {
			t.Error("update audit must carry before/after/patch")
		}
	})
}
