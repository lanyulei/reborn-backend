package kube

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 多文档清单里用 generateName 创建的对象：名称由集群生成，审计要记实际创建的那个（原先审计的 name 为空，
// 按名称检索 / 展示时对不上对象）。
func TestManifestAuditRecordsGeneratedName(t *testing.T) {
	t.Parallel()
	const generated = "cm-x7q2k"
	seen := &seenRequests{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/namespaces/a/configmaps" {
			clusterNotFound(w) // GET …/configmaps/fixed：对象不存在 → 走创建
			return
		}
		var obj map[string]any
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
			t.Errorf("decode create body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		meta, _ := obj["metadata"].(map[string]any)
		if name, _ := meta["name"].(string); name == "" { // generateName：名称由集群生成
			meta["name"] = generated
			if r.URL.Query().Get("dryRun") == "All" {
				meta["name"] = generated + "-dry"
			}
		}
		meta["uid"], meta["resourceVersion"] = "u-"+str(meta, "name"), "1"
		_ = json.NewEncoder(w).Encode(obj)
	})
	u := fakeClusterUser(t, handler)
	req := k8scontract.ManifestRequest{Namespace: "a", YAML: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  generateName: cm-\ndata: {k: v}\n" +
		"---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fixed\ndata: {k: v}\n"}
	const creates = "POST /api/v1/namespaces/a/configmaps"

	// 预检：每个对象一次 dry-run，不产生审计。
	if _, audits, err := u.manifest(req, false); err != nil || audits != nil {
		t.Fatalf("preview: %v %v", audits, err)
	}
	if preview := seen.on(creates); len(preview) != 2 || preview[0].query.Get("dryRun") != "All" || preview[1].query.Get("dryRun") != "All" {
		t.Fatalf("preview creates = %+v", preview)
	}
	seen.reset()

	out, audits, err := u.manifest(req, true)
	if err != nil || !out.OK || len(out.Items) != 2 {
		t.Fatalf("commit: ok=%v items=%+v err=%v", out.OK, out.Items, err)
	}
	// 提交：先各自 dry-run 一遍，再真实创建。
	all := seen.on(creates)
	if len(all) != 4 {
		t.Fatalf("commit creates = %d, want 4 (a dry-run and a real create per object)", len(all))
	}
	for i, c := range all {
		if want := i < 2; (c.query.Get("dryRun") == "All") != want {
			t.Errorf("create request %d: dry-run = %v, want %v", i, !want, want)
		}
	}
	if len(audits) != 2 {
		t.Fatalf("audits = %d, want one per object", len(audits))
	}
	for i, want := range []string{generated, "fixed"} {
		a := audits[i]
		if a.Err != nil || a.Action != k8scontract.ActionCreate || a.Kind != "ConfigMap" || a.Ref.Name != want ||
			a.Ref.Namespace != "a" || a.Ref.Resource != "configmaps" || str(nestedMap(a.After, "metadata"), "name") != want {
			t.Fatalf("audit %d = %+v, want the name of the created object %q", i, a, want)
		}
	}
}
