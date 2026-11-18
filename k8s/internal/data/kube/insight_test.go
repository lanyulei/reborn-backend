package kube

import (
	"slices"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/labels"
)

func TestPodSpecRefs(t *testing.T) {
	t.Parallel()
	spec := map[string]any{
		"containers": []any{map[string]any{
			"env": []any{
				map[string]any{"name": "A", "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "app-conf", "key": "a"}}},
				map[string]any{"name": "B", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "app-secret", "key": "b"}}},
			},
			"envFrom": []any{map[string]any{"secretRef": map[string]any{"name": "app-secret"}}},
		}},
		"initContainers": []any{map[string]any{"envFrom": []any{map[string]any{"configMapRef": map[string]any{"name": "init-conf"}}}}},
		"volumes": []any{
			map[string]any{"name": "v1", "configMap": map[string]any{"name": "app-conf"}},
			map[string]any{"name": "v2", "secret": map[string]any{"secretName": "tls"}},
			map[string]any{"name": "v3", "projected": map[string]any{"sources": []any{map[string]any{"secret": map[string]any{"name": "proj"}}}}},
		},
		"imagePullSecrets": []any{map[string]any{"name": "regcred"}},
	}
	cases := []struct {
		secret bool
		name   string
		want   []string
	}{
		{false, "app-conf", []string{"env", "volume"}},
		{false, "init-conf", []string{"envFrom"}},
		{true, "app-secret", []string{"env", "envFrom"}},
		{true, "tls", []string{"volume"}},
		{true, "proj", []string{"projected"}},
		{true, "regcred", []string{"imagePullSecret"}},
		{true, "app-conf", nil}, // 同名的 ConfigMap 不算 Secret 的引用
		{false, "app-secret", nil},
		{false, "missing", nil},
	}
	for _, c := range cases {
		if got := podSpecRefs(spec, c.secret, c.name); !slices.Equal(got, c.want) {
			t.Errorf("podSpecRefs(secret=%v, %s) = %v, want %v", c.secret, c.name, got, c.want)
		}
	}
}

func TestPodSelectorMatchExpressions(t *testing.T) {
	t.Parallel()
	obj := map[string]any{"spec": map[string]any{"selector": map[string]any{
		"matchLabels":      map[string]any{"app": "web"},
		"matchExpressions": []any{map[string]any{"key": "tier", "operator": "In", "values": []any{"a", "b"}}},
	}}}
	sel, err := podSelector(k8scontract.GVRDeployment, obj)
	if err != nil || sel == nil {
		t.Fatalf("selector = %v, %v", sel, err)
	}
	if !sel.Matches(labels.Set{"app": "web", "tier": "b"}) || sel.Matches(labels.Set{"app": "web", "tier": "c"}) || sel.Matches(labels.Set{"app": "web"}) {
		t.Errorf("matchExpressions are not honoured: %s", sel)
	}
	if sel, _ := podSelector(k8scontract.GVRService, map[string]any{"spec": map[string]any{}}); sel != nil {
		t.Error("a service without selector selects nothing")
	}
	if sel, _ := podSelector(k8scontract.GVRDeployment, map[string]any{"spec": map[string]any{"selector": map[string]any{}}}); sel != nil {
		t.Error("an empty selector must not be treated as match-all")
	}
	svc, _ := podSelector(k8scontract.GVRService, map[string]any{"spec": map[string]any{"selector": map[string]any{"app": "web"}}})
	if svc == nil || svc.String() != "app=web" {
		t.Errorf("service selector = %v", svc)
	}
}

func TestServicePort(t *testing.T) {
	t.Parallel()
	svc := map[string]any{"spec": map[string]any{"ports": []any{
		map[string]any{"name": "http", "port": int64(80)}, map[string]any{"name": "metrics", "port": int64(9090)},
	}}}
	cases := []struct {
		backend map[string]any
		port    string
		found   bool
	}{
		{map[string]any{"port": map[string]any{"number": int64(80)}}, "80", true},
		{map[string]any{"port": map[string]any{"name": "metrics"}}, "metrics", true},
		{map[string]any{"port": map[string]any{"number": int64(8080)}}, "8080", false},
		{map[string]any{"port": map[string]any{"name": "grpc"}}, "grpc", false},
	}
	for _, c := range cases {
		if port, found := servicePort(c.backend, svc); port != c.port || found != c.found {
			t.Errorf("servicePort(%v) = %s, %v", c.backend, port, found)
		}
	}
	if _, found := servicePort(cases[2].backend, nil); !found {
		t.Error("a missing service is reported as not_found, not as a port mismatch")
	}
}

func TestWorkloadOfAndPodReady(t *testing.T) {
	t.Parallel()
	e := func(owner, ready, st string) *biz.Entry {
		return &biz.Entry{Row: k8scontract.Row{Name: "pod-x", State: st, Cells: map[string]any{"owner": owner, "ready": ready}}}
	}
	cases := map[string][2]string{
		"ReplicaSet/web-5d9c7b": {"Deployment", "web"},
		"StatefulSet/db":        {"StatefulSet", "db"},
		"DaemonSet/agent":       {"DaemonSet", "agent"},
		"Job/backup-123":        {"Job", "backup-123"},
		"":                      {"Pod", "pod-x"},
	}
	for owner, want := range cases {
		if kind, name := workloadOf(e(owner, "1/1", sOK)); kind != want[0] || name != want[1] {
			t.Errorf("workloadOf(%q) = %s/%s", owner, kind, name)
		}
	}
	if !podReady(e("", "2/2", sOK)) || podReady(e("", "1/2", sOK)) || podReady(e("", "1/1", sAbnormal)) || podReady(e("", "0/0", sOK)) {
		t.Error("podReady is wrong")
	}
}

func TestQuantities(t *testing.T) {
	t.Parallel()
	if milliCPU("250m") != 250 || milliCPU("2") != 2000 || milliCPU("") != 0 || milliCPU("bad") != 0 {
		t.Error("milliCPU is wrong")
	}
	if quantityBytes("1Gi") != 1<<30 || quantityBytes("128Mi") != 128<<20 || quantityBytes("") != 0 {
		t.Error("quantityBytes is wrong")
	}
}
