package kube

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func resizePod() map[string]any {
	return map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "web-0", "namespace": "a", "uid": "u-1"},
		"spec": map[string]any{
			"initContainers": []any{
				map[string]any{"name": "init", "image": "busybox"},
				map[string]any{"name": "mesh", "image": "proxy", "restartPolicy": "Always", "resources": map[string]any{
					"limits": map[string]any{"memory": "64Mi"},
				}},
			},
			"containers": []any{
				map[string]any{"name": "app", "image": "nginx", "resources": map[string]any{
					"requests": map[string]any{"cpu": "100m", "memory": "128Mi"},
					"limits":   map[string]any{"cpu": "500m", "memory": "256Mi"},
				}},
				map[string]any{"name": "bare", "image": "nginx"},
			},
		},
	}
}

func qty(s string) *string { return &s }

func resizeReq(containers ...k8scontract.ContainerResize) k8scontract.ResizeRequest {
	return k8scontract.ResizeRequest{Ref: podRef("a", "web-0"), Containers: containers}
}

func TestPlanResizeValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		req  k8scontract.ResizeRequest
		want string
	}{
		{"no containers", resizeReq(), "containers are required"},
		{"empty name", resizeReq(k8scontract.ContainerResize{}), "container name is required"},
		{
			"unknown container",
			resizeReq(k8scontract.ContainerResize{Name: "nope", Limits: &k8scontract.ResizeQuantity{CPU: qty("1")}}),
			`container "nope" not found in web-0`,
		},
		{
			"plain init container",
			resizeReq(k8scontract.ContainerResize{Name: "init", Limits: &k8scontract.ResizeQuantity{CPU: qty("1")}}),
			"is an init container",
		},
		{
			"duplicate container",
			resizeReq(
				k8scontract.ContainerResize{Name: "app", Limits: &k8scontract.ResizeQuantity{CPU: qty("1")}},
				k8scontract.ContainerResize{Name: "app", Limits: &k8scontract.ResizeQuantity{CPU: qty("2")}},
			),
			"listed more than once",
		},
		{
			"empty quantity",
			resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("")}}),
			"requests.cpu must not be empty",
		},
		{
			"malformed quantity",
			resizeReq(k8scontract.ContainerResize{Name: "app", Limits: &k8scontract.ResizeQuantity{Memory: qty("lots")}}),
			`limits.memory "lots" is not a valid quantity`,
		},
		{
			"negative quantity",
			resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("-1")}}),
			"is not a valid quantity",
		},
		{
			// 只抬高请求：与现有的上限（500m）合并后超限。
			"request above the current limit",
			resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("1")}}),
			"cpu request 1 exceeds its limit 500m",
		},
		{
			// 只压低上限：低于现有的请求（128Mi）。
			"limit below the current request",
			resizeReq(k8scontract.ContainerResize{Name: "app", Limits: &k8scontract.ResizeQuantity{Memory: qty("64Mi")}}),
			"memory request 128Mi exceeds its limit 64Mi",
		},
		{
			"request above the limit in the same request",
			resizeReq(k8scontract.ContainerResize{
				Name: "bare", Requests: &k8scontract.ResizeQuantity{CPU: qty("2")}, Limits: &k8scontract.ResizeQuantity{CPU: qty("1")},
			}),
			"cpu request 2 exceeds its limit 1",
		},
		{
			// 写法不同、数量相同：不算改动。
			"nothing changes",
			resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("0.1")}, Limits: &k8scontract.ResizeQuantity{}}),
			"does not change any container resources",
		},
	}
	for _, tc := range cases {
		_, err := planResize(resizePod(), tc.req)
		be := biz.AsError(err)
		if err == nil || be.Code != http.StatusBadRequest || be.Reason != k8scontract.ReasonInvalidArgument || !strings.Contains(be.Message, tc.want) {
			t.Errorf("%s: got %v, want 400 %s containing %q", tc.name, err, k8scontract.ReasonInvalidArgument, tc.want)
		}
	}
}

func TestPlanResizePatchAndSummary(t *testing.T) {
	t.Parallel()
	req := resizeReq(
		k8scontract.ContainerResize{
			Name:     "app",
			Requests: &k8scontract.ResizeQuantity{CPU: qty("200m"), Memory: qty("128Mi")}, // 内存请求与现状相同：不进补丁
			Limits:   &k8scontract.ResizeQuantity{Memory: qty("512Mi")},
		},
		k8scontract.ContainerResize{Name: "mesh", Requests: &k8scontract.ResizeQuantity{Memory: qty("32Mi")}},
	)
	changes, err := planResize(resizePod(), req)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, c := range changes {
		paths = append(paths, biz.FieldPath(c.path()))
	}
	wantPaths := []string{
		"spec.containers[name=app].resources.requests.cpu",
		"spec.containers[name=app].resources.limits.memory",
		"spec.initContainers[name=mesh].resources.requests.memory",
	}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("touched fields = %v, want %v", paths, wantPaths)
	}
	// 涉及的字段能在 Pod 里按路径取到（审批期间的冲突比对依赖它）；原先没有的字段记为不存在。
	if v, ok := biz.LookupField(resizePod(), changes[0].path()); !ok || v != "100m" {
		t.Fatalf("lookup %v = %v %v", changes[0].path(), v, ok)
	}
	if _, ok := biz.LookupField(resizePod(), changes[2].path()); ok {
		t.Fatal("an unset resource must be absent")
	}
	raw, err := json.Marshal(resizePatch(changes))
	if err != nil {
		t.Fatal(err)
	}
	const wantPatch = `{"spec":{"containers":[{"name":"app","resources":{"limits":{"memory":"512Mi"},"requests":{"cpu":"200m"}}}],` +
		`"initContainers":[{"name":"mesh","resources":{"requests":{"memory":"32Mi"}}}]}}`
	if string(raw) != wantPatch {
		t.Fatalf("patch = %s\nwant    %s", raw, wantPatch)
	}
	const wantSummary = "调整资源 app: CPU 请求 100m→200m / 内存上限 256Mi→512Mi；mesh: 内存请求 未设置→32Mi"
	if got := resizeSummary(changes); got != wantSummary {
		t.Fatalf("summary = %q, want %q", got, wantSummary)
	}
	params, _ := json.Marshal(resizeParams(changes[:1]))
	if string(params) != `[{"container":"app","field":"requests.cpu","from":"100m","to":"200m"}]` {
		t.Fatalf("audit params = %s", params)
	}
}

func TestResizeError(t *testing.T) {
	t.Parallel()
	pods := schema.GroupResource{Resource: "pods"}
	// 旧版本集群：路径不存在，响应不带对象名。
	bare := &apierrors.StatusError{}
	bare.ErrStatus.Code, bare.ErrStatus.Message = http.StatusNotFound, "the server could not find the requested resource"
	// 中间层返回的不是集群的 Status：客户端自己拼出带对象名的 404。
	generic := apierrors.NewGenericServerResponse(http.StatusNotFound, "patch", pods, "web-0", "404 page not found", 0, true)
	for name, err := range map[string]error{"bare status": bare, "non-status body": generic} {
		be := biz.AsError(mapError(resizeError(err)))
		if be.Code != http.StatusUnprocessableEntity || be.Reason != k8scontract.ReasonRejected || !strings.Contains(be.Message, "Kubernetes 1.33+") {
			t.Errorf("%s: got %+v", name, be)
		}
	}
	// Pod 恰好被删：仍是普通的 NotFound。
	if be := biz.AsError(mapError(resizeError(apierrors.NewNotFound(pods, "web-0")))); be.Reason != k8scontract.ReasonNotFound {
		t.Errorf("deleted pod: got %+v", be)
	}
	// 集群的拒绝原样透出，沿用既有的归类。
	forbidden := apierrors.NewForbidden(pods, "web-0", io.ErrUnexpectedEOF)
	if be := biz.AsError(mapError(resizeError(forbidden))); be.Reason != k8scontract.ReasonForbidden {
		t.Errorf("forbidden: got %+v", be)
	}
	invalid := apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "web-0", nil)
	if be := biz.AsError(mapError(resizeError(invalid))); be.Reason != k8scontract.ReasonRejected {
		t.Errorf("invalid: got %+v", be)
	}
}

// resizeCluster 一个只认识 Pod 的假集群：记录到达 resize 子资源的请求；status 非 200 时以该状态拒绝。
type resizeCluster struct {
	mu      sync.Mutex
	status  int
	reply   string
	patches []*http.Request
	bodies  []string
	other   []string // 除发现与读取 Pod 之外的写请求
}

func (f *resizeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	const podPath = "/api/v1/namespaces/a/pods/web-0"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api":
		_, _ = io.WriteString(w, `{"kind":"APIVersions","versions":["v1"]}`)
	case r.Method == http.MethodGet && r.URL.Path == "/apis":
		_, _ = io.WriteString(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1":
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","groupVersion":"v1","resources":[`+
			`{"name":"pods","namespaced":true,"kind":"Pod","verbs":["get","list","patch"]},`+
			`{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["get","patch","update"]}]}`)
	case r.Method == http.MethodGet && r.URL.Path == podPath:
		_ = json.NewEncoder(w).Encode(resizePod())
	case r.Method == http.MethodPatch && r.URL.Path == podPath+"/resize":
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.patches, f.bodies = append(f.patches, r.Clone(context.Background())), append(f.bodies, string(body))
		status, reply := f.status, f.reply
		f.mu.Unlock()
		if status != 0 && status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, reply)
			return
		}
		pod := resizePod()
		asMap(nestedList(pod, "spec", "containers")[0])["resources"] = map[string]any{
			"requests": map[string]any{"cpu": "200m", "memory": "128Mi"}, "limits": map[string]any{"cpu": "500m", "memory": "256Mi"},
		}
		_ = json.NewEncoder(w).Encode(pod)
	default:
		if r.Method != http.MethodGet {
			f.mu.Lock()
			f.other = append(f.other, r.Method+" "+r.URL.Path)
			f.mu.Unlock()
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
	}
}

func resizeUser(t *testing.T, f *resizeCluster) *userClients {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	m := NewManager(nil, nil, nil, dialerFunc(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}), Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(m.Close)
	if err := m.Connect(context.Background(), &biz.Cluster{ID: "resize", Name: "r", AccessMode: k8scontract.AccessAgent}); err != nil {
		t.Fatal(err)
	}
	c, err := m.conn("resize")
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.asUser(context.Background(), biz.Identity{UserID: uid1, RoleIDs: []string{role1}})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// 经真实的客户端栈走一遍：请求打到 Pod 的 resize 子资源，带用户身份，补丁只含要改的那一项。
func TestResizeHitsSubresourceAsUser(t *testing.T) {
	t.Parallel()
	f := &resizeCluster{}
	u := resizeUser(t, f)
	body, _ := json.Marshal(resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("200m")}}))
	resp, audits, err := u.dispatch(k8scontract.OpPodResize, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := resp.(k8scontract.Empty); !ok {
		t.Fatalf("response = %#v", resp)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.patches) != 1 || len(f.other) != 0 {
		t.Fatalf("patches = %d, other writes = %v", len(f.patches), f.other)
	}
	r := f.patches[0]
	if r.Header.Get("Impersonate-User") != k8scontract.ImpersonateUser(uid1) {
		t.Fatalf("Impersonate-User = %q", r.Header.Get("Impersonate-User"))
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/strategic-merge-patch+json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if f.bodies[0] != `{"spec":{"containers":[{"name":"app","resources":{"requests":{"cpu":"200m"}}}]}}` {
		t.Fatalf("patch body = %s", f.bodies[0])
	}
	if len(audits) != 1 {
		t.Fatalf("audits = %d", len(audits))
	}
	a := audits[0]
	if a.Action != k8scontract.ActionResize || a.Kind != "Pod" || a.Ref != podRef("a", "web-0") || a.Err != nil {
		t.Fatalf("audit = %+v", a)
	}
	if str(asMap(nestedList(a.Before, "spec", "containers")[0]), "resources", "requests", "cpu") != "100m" ||
		str(asMap(nestedList(a.After, "spec", "containers")[0]), "resources", "requests", "cpu") != "200m" {
		t.Fatalf("audit before/after do not carry the resources: %v → %v", a.Before, a.After)
	}
}

func TestResizeFailures(t *testing.T) {
	t.Parallel()
	status := func(code int, reason, msg, details string) string {
		return `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"` + reason + `","code":` +
			strconv.Itoa(code) + `,"message":"` + msg + `"` + details + `}`
	}
	cases := []struct {
		name       string
		status     int
		reply      string
		req        k8scontract.ResizeRequest
		code       int
		reason     string
		contains   string
		wantPatch  bool
		wantBefore bool
	}{
		{
			name: "unknown container never reaches the cluster", req: resizeReq(k8scontract.ContainerResize{Name: "nope", Limits: &k8scontract.ResizeQuantity{CPU: qty("1")}}),
			code: 400, reason: k8scontract.ReasonInvalidArgument, contains: `container "nope" not found`,
		},
		{
			name: "request above limit never reaches the cluster", req: resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("2")}}),
			code: 400, reason: k8scontract.ReasonInvalidArgument, contains: "exceeds its limit",
		},
		{
			name: "missing namespace", req: k8scontract.ResizeRequest{Ref: podRef("", "web-0"), Containers: []k8scontract.ContainerResize{{Name: "app"}}},
			code: 400, reason: k8scontract.ReasonInvalidArgument, contains: "namespace and name are required",
		},
		{
			name: "subresource not served", status: 404, reply: status(404, "NotFound", "the server could not find the requested resource", `,"details":{}`),
			req:  resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("200m")}}),
			code: 422, reason: k8scontract.ReasonRejected, contains: "Kubernetes 1.33+", wantPatch: true, wantBefore: true,
		},
		{
			name: "rejected by validation", status: 422, reply: status(422, "Invalid", "Pod QOS Class may not change as a result of resizing", ""),
			req:  resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("200m")}}),
			code: 422, reason: k8scontract.ReasonRejected, contains: "QOS Class", wantPatch: true, wantBefore: true,
		},
		{
			name: "forbidden by rbac", status: 403, reply: status(403, "Forbidden", "pods web-0 is forbidden: cannot patch resource pods/resize", ""),
			req:  resizeReq(k8scontract.ContainerResize{Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("200m")}}),
			code: 403, reason: k8scontract.ReasonForbidden, contains: "pods/resize", wantPatch: true, wantBefore: true,
		},
	}
	for _, tc := range cases {
		f := &resizeCluster{status: tc.status, reply: tc.reply}
		u := resizeUser(t, f)
		body, _ := json.Marshal(tc.req)
		_, audits, err := u.dispatch(k8scontract.OpPodResize, body)
		be := biz.AsError(mapError(err))
		if err == nil || be.Code != tc.code || be.Reason != tc.reason || !strings.Contains(be.Message, tc.contains) {
			t.Errorf("%s: got %v, want %d %s containing %q", tc.name, err, tc.code, tc.reason, tc.contains)
			continue
		}
		// 失败也要审计，且审计里的错误与返回的一致。
		if len(audits) != 1 || audits[0].Action != k8scontract.ActionResize || biz.AsError(audits[0].Err).Reason != tc.reason {
			t.Errorf("%s: audits = %+v", tc.name, audits)
			continue
		}
		if (audits[0].Before != nil) != tc.wantBefore {
			t.Errorf("%s: audit before = %v", tc.name, audits[0].Before != nil)
		}
		f.mu.Lock()
		if (len(f.patches) == 1) != tc.wantPatch || len(f.other) != 0 {
			t.Errorf("%s: patches = %d, other writes = %v", tc.name, len(f.patches), f.other)
		}
		f.mu.Unlock()
	}
}

// 变更计划：涉及的字段逐项列出（审批期间被改动即冲突），说明给出前后值；计划阶段不向集群写任何东西。
func TestResizeChangePlan(t *testing.T) {
	t.Parallel()
	f := &resizeCluster{}
	u := resizeUser(t, f)
	body, _ := json.Marshal(resizeReq(k8scontract.ContainerResize{
		Name: "app", Requests: &k8scontract.ResizeQuantity{CPU: qty("200m")}, Limits: &k8scontract.ResizeQuantity{Memory: qty("512Mi")},
	}))
	plan, err := u.changePlan(k8scontract.ChangePlanRequest{Op: k8scontract.OpPodResize, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Display.Action != k8scontract.ActionResize || plan.Display.Target != podRef("a", "web-0") || plan.Display.Kind != "Pod" {
		t.Fatalf("display = %+v", plan.Display)
	}
	if want := "Pod web-0：调整资源 app: CPU 请求 100m→200m / 内存上限 256Mi→512Mi"; plan.Display.Summary != want {
		t.Fatalf("summary = %q, want %q", plan.Display.Summary, want)
	}
	if len(plan.Display.Items) != 1 || len(plan.Exec.Items) != 1 {
		t.Fatalf("items = %d / %d", len(plan.Display.Items), len(plan.Exec.Items))
	}
	wantFields := []k8scontract.ChangeField{
		{Path: "spec.containers[name=app].resources.requests.cpu", Before: `"100m"`, After: `"200m"`},
		{Path: "spec.containers[name=app].resources.limits.memory", Before: `"256Mi"`, After: `"512Mi"`},
	}
	if !reflect.DeepEqual(plan.Display.Items[0].Fields, wantFields) {
		t.Fatalf("fields = %+v", plan.Display.Items[0].Fields)
	}
	e := plan.Exec.Items[0]
	if e.Mode != k8scontract.ChangeModeCheck || e.UID != "u-1" || len(e.Checks) != 2 || e.Checks[0].Value != "100m" || e.Checks[1].Value != "256Mi" {
		t.Fatalf("exec item = %+v", e)
	}
	if plan.Exec.Op != k8scontract.OpPodResize || string(plan.Exec.Body) != string(body) {
		t.Fatalf("exec = %s %s", plan.Exec.Op, plan.Exec.Body)
	}
	// 现状未变：比对通过；审批期间有人改了其中一项：冲突。
	if err := biz.VerifyChecks(resizePod(), e.Checks); err != nil {
		t.Fatalf("unchanged pod must pass the checks: %v", err)
	}
	moved := resizePod()
	nestedMap(asMap(nestedList(moved, "spec", "containers")[0]), "resources", "requests")["cpu"] = "150m"
	if err := biz.VerifyChecks(moved, e.Checks); err == nil {
		t.Fatal("a concurrent change to a touched field must conflict")
	}
	// 校验失败的请求在提交时就被拒绝，不生成变更单。
	bad, _ := json.Marshal(resizeReq(k8scontract.ContainerResize{Name: "nope", Limits: &k8scontract.ResizeQuantity{CPU: qty("1")}}))
	if _, err := u.changePlan(k8scontract.ChangePlanRequest{Op: k8scontract.OpPodResize, Body: bad}); biz.AsError(err).Reason != k8scontract.ReasonInvalidArgument {
		t.Fatalf("invalid resize must not be plannable: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.patches) != 0 || len(f.other) != 0 {
		t.Fatalf("planning must not write to the cluster: %d %v", len(f.patches), f.other)
	}
}
