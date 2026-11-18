package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/k8splane"
	k8sservice "github.com/lanyulei/reborn/backend/core/internal/service/k8s"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
)

// k8sTestKey 测试用的内部密钥（core 与假数据面共用）。
const k8sTestKey = "k8s-e2e-internal-key-0123456789abcdef"

// k8sServices 容器平台域的 e2e 装配产物。
type k8sServices struct {
	cluster  *k8sservice.ClusterService
	scope    *k8sservice.ScopeService
	audit    *k8sservice.AuditService
	resource *k8sservice.ResourceService
	workload *k8sservice.WorkloadService
	stream   *k8sservice.StreamService
	insight  *k8sservice.InsightService
	insights *testfake.K8sInsight
	conf     *conf.K8S
	store    *testfake.K8sStore
}

// newK8sServices 组装容器平台域：内存仓储 + 真实的数据面客户端（k8splane）+ 真实的角色解析。
func newK8sServices(store *testfake.Store, authz *biz.AuthzUsecase) k8sServices {
	ks := testfake.NewK8sStore()
	users := monitorUsers{store: store}
	c := &conf.K8S{
		InternalKey: k8sTestKey,
		Agent:       &conf.K8S_Agent{ServerUrl: "https://tunnel.example.com", Image: "registry.example.com/reborn/k8s-agent:v1"},
	}
	plane := k8splane.New(ks.Resolver(), c, testLogger())
	planes := bizk8s.NewPlaneUsecase(plane, authz, authz, users, ks, ks.Tickets())
	clusterUC := bizk8s.NewClusterUsecase(ks, ks, ks, &testfake.K8sSealer{}, authz, users, planes, c)
	insights := testfake.NewK8sInsight(ks)
	access := bizk8s.NewAccessResolver(authz, insights, planes)
	return k8sServices{
		insight:  k8sservice.NewInsightService(bizk8s.NewInsightUsecase(insights, insights, ks, authz, insights, insights, planes, clusterUC), planes),
		insights: insights,
		cluster:  k8sservice.NewClusterService(clusterUC),
		scope:    k8sservice.NewScopeService(bizk8s.NewScopeUsecase(ks.Scopes(), users)),
		audit:    k8sservice.NewAuditService(bizk8s.NewAuditUsecase(ks.AuditReader(), access)),
		resource: k8sservice.NewResourceService(planes),
		workload: k8sservice.NewWorkloadService(planes),
		stream:   k8sservice.NewStreamService(planes),
		conf:     c,
		store:    ks,
	}
}

// fakePlane 内存里的假数据面：校验身份头签名，按测试设定应答。
type fakePlane struct {
	t  *testing.T
	ts *httptest.Server

	mu         sync.Mutex
	identities []kc.Identity
	clusters   []string
	bodies     map[string]string // 路径 → 最近一次请求体
	// notOwner 前若干次请求应答 421 K8S_NOT_OWNER。
	notOwner atomic.Int32
	// fail 非空时一元调用应答该错误体。
	fail *kc.ErrorBody
	// release 流式接口在发出第一个事件后等待它再发第二个。
	release chan struct{}
}

func newFakePlane(t *testing.T) *fakePlane {
	t.Helper()
	p := &fakePlane{t: t, bodies: map[string]string{}, release: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc(kc.InternalOpPath, p.auth(p.op))
	mux.HandleFunc(kc.InternalStreamPath, p.auth(p.stream))
	mux.HandleFunc(kc.InternalExecPath, p.auth(p.exec))
	p.ts = httptest.NewServer(mux)
	t.Cleanup(p.ts.Close)
	return p
}

func writeErr(w http.ResponseWriter, body kc.ErrorBody) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(body.Code)
	_ = json.NewEncoder(w).Encode(body)
}

func (p *fakePlane) auth(next func(http.ResponseWriter, *http.Request, kc.Identity)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := kc.VerifyIdentity(r.Header, []byte(k8sTestKey), time.Now())
		if err != nil {
			writeErr(w, kc.ErrorBody{Code: 401, Reason: kc.ReasonIdentity, Message: err.Error()})
			return
		}
		if p.notOwner.Load() > 0 {
			p.notOwner.Add(-1)
			writeErr(w, kc.ErrorBody{Code: 421, Reason: kc.ReasonNotOwner, Message: "not owner"})
			return
		}
		p.mu.Lock()
		p.identities = append(p.identities, id)
		p.clusters = append(p.clusters, r.Header.Get(kc.ClusterHeader))
		p.mu.Unlock()
		next(w, r, id)
	}
}

func (p *fakePlane) op(w http.ResponseWriter, r *http.Request, _ kc.Identity) {
	raw, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.bodies[r.URL.Path] = string(raw)
	fail := p.fail
	p.mu.Unlock()
	if fail != nil {
		writeErr(w, *fail)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch strings.TrimPrefix(r.URL.Path, kc.InternalOpPath) {
	case kc.OpClusterTest:
		_ = json.NewEncoder(w).Encode(kc.ClusterTestResponse{
			OK: true, Version: "v1.35.1", DurationMS: 12,
			Checks: []kc.SelfCheck{{Name: "impersonate", OK: true}},
		})
	default:
		_, _ = w.Write([]byte(`{"total":1,"rows":[{"name":"web","uid":"u-1"}]}`))
	}
}

func (p *fakePlane) stream(w http.ResponseWriter, r *http.Request, _ kc.Identity) {
	raw, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.bodies[r.URL.Path] = string(raw)
	fail := p.fail
	p.mu.Unlock()
	if fail != nil {
		writeErr(w, *fail)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f := w.(http.Flusher)
	_, _ = w.Write([]byte("event: log\nid: 1\ndata: {\"text\":\"first\"}\n\n"))
	f.Flush()
	select {
	case <-p.release:
	case <-r.Context().Done():
		return
	}
	_, _ = w.Write([]byte("event: end\ndata: {\"reason\":\"completed\"}\n\n"))
	f.Flush()
}

// exec 回显：收到输入帧原样作为输出帧发回；先发一个 ready 状态帧。
func (p *fakePlane) exec(w http.ResponseWriter, r *http.Request, id kc.Identity) {
	raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("params"))
	if err != nil {
		writeErr(w, kc.ErrorBody{Code: 400, Reason: kc.ReasonInvalidArgument, Message: "bad params"})
		return
	}
	p.mu.Lock()
	p.bodies[r.URL.Path] = string(raw)
	p.mu.Unlock()
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()
	ctx := r.Context()
	status, _ := json.Marshal(kc.ExecStatus{State: kc.ExecReady, SessionID: "s-" + id.UserID})
	if err := conn.Write(ctx, websocket.MessageBinary, append([]byte{kc.FrameStatus}, status...)); err != nil {
		return
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if len(data) > 0 && data[0] == kc.FrameInput {
			if err := conn.Write(ctx, websocket.MessageBinary, append([]byte{kc.FrameOutput}, data[1:]...)); err != nil {
				return
			}
		}
	}
}

func (p *fakePlane) lastIdentity() kc.Identity {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.identities) == 0 {
		p.t.Fatal("fake plane received no request")
	}
	return p.identities[len(p.identities)-1]
}

func (p *fakePlane) body(path string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bodies[path]
}

func (p *fakePlane) setFail(b *kc.ErrorBody) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = b
}

// k8sOps 容器平台域全部可授权接口。
func k8sOps() []string {
	var ops []string
	for _, m := range service.AuthorizableMethods() {
		if strings.HasPrefix(m.Name, "/k8s.v1.") {
			ops = append(ops, m.Name)
		}
	}
	return ops
}

// k8sFixture 一个已登记的 Agent 集群 + 假数据面。
type k8sFixture struct {
	ts        *httptest.Server
	svc       testServices
	plane     *fakePlane
	admin     string
	clusterID string
}

const k8sAgentCluster = `{"spec":{"name":"prod-1","displayName":"生产一","env":"prod","accessMode":1,"agentLocation":1,"enabled":true}}`

func newK8sFixture(t *testing.T) k8sFixture {
	t.Helper()
	ts, svc := newTestHTTPServerWithServices(t)
	f := k8sFixture{ts: ts, svc: svc, plane: newFakePlane(t), admin: testBearer(t, svc.signer, e2eActorID)}
	f.clusterID = monitorID(t, ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin, k8sAgentCluster, "cluster")
	svc.k8s.store.SetOwner(f.clusterID, f.plane.ts.URL)
	svc.k8s.store.SetAny(f.plane.ts.URL)
	return f
}

// grant 给角色授予该集群全部命名空间的开发档位：没有任何数据范围的用户不会被转发到数据面。
func (f k8sFixture) grant(t *testing.T, roleID string) {
	t.Helper()
	expectStatus(t, "grant scope", f.ts, http.MethodPut, "/api/v1/k8s/role-scope", f.admin,
		`{"roleId":"`+roleID+`","items":[{"clusterId":"`+f.clusterID+`","namespace":"*","level":2}]}`, http.StatusOK)
}

func (f k8sFixture) path(suffix string) string {
	return "/api/v1/k8s/cluster/" + f.clusterID + suffix
}

// 认证与接口授权：未登录 401；没有容器平台接口权限 403；只授予读接口的角色不能调用写接口。
func TestK8s_AuthAndAuthz(t *testing.T) {
	t.Parallel()
	f := newK8sFixture(t)
	list := f.path("/resource?version=v1&resource=pods")
	expectStatus(t, "anonymous", f.ts, http.MethodGet, list, "", "", http.StatusUnauthorized)
	plain := testBearer(t, f.svc.signer, createUser(t, f.ts, f.admin, "k8s_plain").Id)
	expectStatus(t, "no permission", f.ts, http.MethodGet, list, plain, "", http.StatusForbidden)
	expectStatus(t, "no permission: clusters", f.ts, http.MethodGet, "/api/v1/k8s/cluster?page=1&pageSize=10", plain, "", http.StatusForbidden)
	expectStatus(t, "no permission: stream", f.ts, http.MethodPost, f.path("/pod/log"), plain, `{"namespace":"default","pod":"web"}`, http.StatusForbidden)

	reader, _, readerRole := monitorUser(t, f.ts, f.svc, f.admin, "k8s_reader", []string{"/k8s.v1.K8sResourceService/ListResources"})
	// 有接口权限但在该集群上没有任何数据范围：不转发（按集群不存在处理）。
	expectStatus(t, "reader without scope", f.ts, http.MethodGet, list, reader, "", http.StatusNotFound, "K8S_CLUSTER_NOT_FOUND")
	f.grant(t, readerRole)
	expectStatus(t, "reader lists", f.ts, http.MethodGet, list, reader, "", http.StatusOK, `"web"`)
	expectStatus(t, "reader cannot delete", f.ts, http.MethodPost, f.path("/object:delete"), reader,
		`{"ref":{"version":"v1","resource":"pods","namespace":"default","name":"web"}}`, http.StatusForbidden)
	expectStatus(t, "reader cannot open terminal", f.ts, http.MethodPost, f.path("/exec-ticket"), reader,
		`{"namespace":"default","pod":"web"}`, http.StatusForbidden)
}

// 转发：响应体原样透传；身份头带真实的用户、启用角色与是否超管；请求被转换为契约结构；
// 数据面的错误体以同样的状态码与 reason 返回；归属转移时刷新后重试一次。
func TestK8s_ForwardedOps(t *testing.T) {
	t.Parallel()
	f := newK8sFixture(t)
	body := expectStatus(t, "list", f.ts, http.MethodGet,
		f.path("/resource?group=apps&version=v1&resource=deployments&namespace=default&keyword=web&state=abnormal&node=n1&sortBy=name&sortDesc=true&page=2&pageSize=50"),
		f.admin, "", http.StatusOK)
	if body != `{"total":1,"rows":[{"name":"web","uid":"u-1"}]}` {
		t.Fatalf("list body must pass through verbatim, got %s", body)
	}
	var req kc.ListRequest
	if err := json.Unmarshal([]byte(f.plane.body(kc.InternalOpPath+kc.OpList)), &req); err != nil {
		t.Fatalf("decode forwarded list request: %v", err)
	}
	if req.Group != "apps" || req.Resource != "deployments" || req.Namespace != "default" || req.Keyword != "web" ||
		req.Filters[kc.FilterState] != "abnormal" || req.Filters[kc.FilterNode] != "n1" || req.SortBy != "name" || !req.SortDesc ||
		req.Page != 2 || req.PageSize != 50 {
		t.Fatalf("forwarded list request = %+v", req)
	}
	id := f.plane.lastIdentity()
	if id.UserID != e2eActorID || !id.IsAdmin || id.Username != "e2e-actor" || id.RequestID == "" {
		t.Fatalf("admin identity = %+v", id)
	}

	// 非超管：身份里是其启用角色，不带超管标记。
	bearer, userID, roleID := monitorUser(t, f.ts, f.svc, f.admin, "k8s_dev", k8sOps())
	f.grant(t, roleID)
	expectStatus(t, "apply", f.ts, http.MethodPost, f.path("/object:apply"), bearer,
		`{"ref":{"group":"apps","version":"v1","resource":"deployments","namespace":"default","name":"web"},"yaml":"kind: Deployment\n","resourceVersion":"42"}`,
		http.StatusOK)
	id = f.plane.lastIdentity()
	if id.UserID != userID || id.IsAdmin || len(id.RoleIDs) != 1 || id.RoleIDs[0] != roleID {
		t.Fatalf("user identity = %+v, want role %s", id, roleID)
	}
	var apply kc.ApplyRequest
	_ = json.Unmarshal([]byte(f.plane.body(kc.InternalOpPath+kc.OpApply)), &apply)
	if apply.Ref.Name != "web" || apply.ResourceVersion != "42" || apply.YAML != "kind: Deployment\n" {
		t.Fatalf("forwarded apply request = %+v", apply)
	}

	// 数据面的错误体原样变成业务错误。
	f.plane.setFail(&kc.ErrorBody{Code: 403, Reason: kc.ReasonForbidden, Message: `pods is forbidden: User "reborn:x" cannot list`})
	expectStatus(t, "plane forbidden", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), bearer, "",
		http.StatusForbidden, kc.ReasonForbidden, "cannot list")
	f.plane.setFail(&kc.ErrorBody{Code: 409, Reason: kc.ReasonConflict, Message: "modified"})
	expectStatus(t, "plane conflict", f.ts, http.MethodPost, f.path("/workload:scale"), bearer,
		`{"ref":{"group":"apps","version":"v1","resource":"deployments","namespace":"default","name":"web"},"replicas":3}`,
		http.StatusConflict, kc.ReasonConflict)
	f.plane.setFail(&kc.ErrorBody{Code: 418, Reason: "SOMETHING_NEW", Message: "x"})
	expectStatus(t, "unknown reason", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), bearer, "",
		http.StatusBadGateway, kc.ReasonUpstream)
	f.plane.setFail(nil)

	// 归属转移：第一次 421，刷新归属后重试成功。
	f.plane.notOwner.Store(1)
	expectStatus(t, "retry after not-owner", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), bearer, "", http.StatusOK)
	if inv := f.svc.k8s.store.Invalidated(); len(inv) != 1 || inv[0] != f.clusterID {
		t.Fatalf("owner cache invalidations = %v", inv)
	}
	// 连续两次 421：不再重试，503。
	f.plane.notOwner.Store(2)
	expectStatus(t, "not-owner twice", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), bearer, "",
		http.StatusServiceUnavailable, kc.ReasonPlaneDown)

	// 没有存活的负责实例 / 集群不存在。
	f.svc.k8s.store.SetOwner(f.clusterID, "")
	expectStatus(t, "no owner", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), bearer, "",
		http.StatusServiceUnavailable, kc.ReasonPlaneDown)
	expectStatus(t, "unknown cluster", f.ts, http.MethodGet,
		"/api/v1/k8s/cluster/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9bffff/resource?version=v1&resource=pods", bearer, "",
		http.StatusNotFound, kc.ReasonClusterNotFound)
	// 参数校验在 core 就拦下，不到数据面。
	expectStatus(t, "validation", f.ts, http.MethodGet, f.path("/resource?resource=pods"), bearer, "", http.StatusBadRequest)
}

// SSE：数据面每发出一个事件，浏览器立刻收到（不等流结束）；建立前的失败是普通 JSON 错误。
func TestK8s_StreamProxy(t *testing.T) {
	t.Parallel()
	f := newK8sFixture(t)
	req, _ := http.NewRequest(http.MethodPost, f.ts.URL+f.path("/pod/log"),
		strings.NewReader(`{"namespace":"default","pod":"web-1","container":"app","tailLines":100,"follow":true,"cursor":"c-9"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.admin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open log stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") ||
		resp.Header.Get("X-Accel-Buffering") != "no" || !strings.Contains(resp.Header.Get("Cache-Control"), "no-transform") {
		t.Fatalf("stream response: %d %v", resp.StatusCode, resp.Header)
	}
	reader := bufio.NewReader(resp.Body)
	readEvent := func() string {
		var sb strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("read stream: %v (got %q)", err, sb.String())
			}
			if line == "\n" {
				return sb.String()
			}
			sb.WriteString(line)
		}
	}
	got := make(chan string, 1)
	go func() { got <- readEvent() }()
	select {
	case ev := <-got:
		if !strings.Contains(ev, "event: log") || !strings.Contains(ev, `"first"`) || !strings.Contains(ev, "id: 1") {
			t.Fatalf("first event = %q", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first event was not flushed before the stream ended")
	}
	close(f.plane.release)
	if ev := readEvent(); !strings.Contains(ev, "event: end") {
		t.Fatalf("second event = %q", ev)
	}
	var fwd kc.PodLogRequest
	_ = json.Unmarshal([]byte(f.plane.body(kc.InternalStreamPath+kc.StreamPodLog)), &fwd)
	if fwd.Pod != "web-1" || fwd.Container != "app" || fwd.TailLines != 100 || !fwd.Follow || fwd.Cursor != "c-9" {
		t.Fatalf("forwarded log request = %+v", fwd)
	}

	f.plane.setFail(&kc.ErrorBody{Code: 429, Reason: kc.ReasonTooManyStreams, Message: "too many"})
	expectStatus(t, "stream refused", f.ts, http.MethodPost, f.path("/watch"), f.admin,
		`{"version":"v1","resource":"pods"}`, http.StatusTooManyRequests, kc.ReasonTooManyStreams)
}

// dialExec 以票据连接终端；返回连接与握手响应。
func dialExec(t *testing.T, ts *httptest.Server, ticket string, header http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/k8s/exec?ticket="+ticket, &websocket.DialOptions{HTTPHeader: header})
}

func openTicket(t *testing.T, f k8sFixture, bearer string) string {
	t.Helper()
	body := expectStatus(t, "open exec ticket", f.ts, http.MethodPost, f.path("/exec-ticket"), bearer,
		`{"namespace":"default","pod":"web-1","container":"app","cols":120,"rows":40}`, http.StatusOK)
	var out struct{ Ticket string }
	if err := json.Unmarshal([]byte(body), &out); err != nil || len(out.Ticket) < 32 {
		t.Fatalf("ticket response = %s", body)
	}
	return out.Ticket
}

// readFrame 读一帧并返回类型与载荷。
func readFrame(t *testing.T, c *websocket.Conn) (byte, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty frame")
	}
	return data[0], data[1:]
}

// expectClosed 连接成功但被以状态帧拒绝。
func expectClosed(t *testing.T, what string, ts *httptest.Server, ticket, reason string) {
	t.Helper()
	conn, _, err := dialExec(t, ts, ticket, nil)
	if err != nil {
		t.Fatalf("%s: dial: %v", what, err)
	}
	defer func() { _ = conn.CloseNow() }()
	typ, payload := readFrame(t, conn)
	var st kc.ExecStatus
	_ = json.Unmarshal(payload, &st)
	if typ != kc.FrameStatus || st.State != kc.ExecClosed || st.Reason != reason {
		t.Fatalf("%s: frame %d %s, want closed/%s", what, typ, payload, reason)
	}
}

// 终端：票据一次性、会过期、绑定签发用户；帧双向转发；跨站 Origin 被拒；禁用终端的集群不签发票据。
func TestK8s_Exec(t *testing.T) {
	t.Parallel()
	f := newK8sFixture(t)
	bearer, userID, execRole := monitorUser(t, f.ts, f.svc, f.admin, "k8s_exec", k8sOps())
	f.grant(t, execRole)

	ticket := openTicket(t, f, bearer)
	conn, _, err := dialExec(t, f.ts, ticket, nil)
	if err != nil {
		t.Fatalf("dial exec: %v", err)
	}
	typ, payload := readFrame(t, conn)
	var st kc.ExecStatus
	_ = json.Unmarshal(payload, &st)
	if typ != kc.FrameStatus || st.State != kc.ExecReady || st.SessionID != "s-"+userID {
		t.Fatalf("ready frame = %d %s", typ, payload)
	}
	ctx := context.Background()
	if err := conn.Write(ctx, websocket.MessageBinary, append([]byte{kc.FrameInput}, "ls -l\n"...)); err != nil {
		t.Fatalf("write input: %v", err)
	}
	if typ, payload = readFrame(t, conn); typ != kc.FrameOutput || string(payload) != "ls -l\n" {
		t.Fatalf("echo frame = %d %q", typ, payload)
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	// 数据面收到的身份是票据的签发用户（实时解析角色），参数来自票据。
	if id := f.plane.lastIdentity(); id.UserID != userID || id.IsAdmin || len(id.RoleIDs) != 1 {
		t.Fatalf("exec identity = %+v", id)
	}
	var params kc.ExecParams
	_ = json.Unmarshal([]byte(f.plane.body(kc.InternalExecPath)), &params)
	if params.Pod != "web-1" || params.Container != "app" || params.Cols != 120 || params.Rows != 40 || params.Kind != kc.ExecKindShell {
		t.Fatalf("exec params = %+v", params)
	}

	// 一次性：同一张票据再用被拒。
	expectClosed(t, "reused ticket", f.ts, ticket, "K8S_TICKET_INVALID")
	expectClosed(t, "forged ticket", f.ts, strings.Repeat("A", 43), "K8S_TICKET_INVALID")
	// 没带票据：握手前就是 401。
	if code, body := doRequest(t, http.MethodGet, f.ts.URL+"/api/v1/k8s/exec", ""); code != http.StatusUnauthorized || !strings.Contains(body, "K8S_TICKET_INVALID") {
		t.Fatalf("missing ticket: %d %s", code, body)
	}

	// 过期：时钟拨到 31 秒后。
	ticket = openTicket(t, f, bearer)
	f.svc.k8s.store.SetNow(func() time.Time { return time.Now().Add(kc.ExecTicketTTL + time.Second) })
	expectClosed(t, "expired ticket", f.ts, ticket, "K8S_TICKET_INVALID")
	f.svc.k8s.store.SetNow(time.Now)

	// 账号在签发后被停用：核销时拒绝。
	ticket = openTicket(t, f, bearer)
	expectStatus(t, "disable user", f.ts, http.MethodPost, "/api/v1/system/user/"+userID+"/status", f.admin, `{"status":2}`, http.StatusOK)
	expectClosed(t, "disabled user", f.ts, ticket, "K8S_TICKET_INVALID")

	// 跨站 Origin：握手被拒（403），票据不被消耗。
	ticket = openTicket(t, f, f.admin)
	if _, resp, err := dialExec(t, f.ts, ticket, http.Header{"Origin": {"https://evil.example.com"}}); err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin dial: err = %v resp = %v", err, resp)
	}
	conn, _, err = dialExec(t, f.ts, ticket, nil)
	if err != nil {
		t.Fatalf("dial after rejected origin: %v", err)
	}
	if typ, _ = readFrame(t, conn); typ != kc.FrameStatus {
		t.Fatalf("frame after rejected origin = %d", typ)
	}
	_ = conn.CloseNow()

	// 集群禁止终端：不签发票据；已签发的票据在核销时也被拒。
	ticket = openTicket(t, f, f.admin)
	expectStatus(t, "disable exec", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+f.clusterID, f.admin,
		`{"spec":{"name":"prod-1","env":"prod","accessMode":1,"agentLocation":1,"enabled":true,"execDisabled":true},"version":1}`, http.StatusOK)
	expectStatus(t, "exec disabled", f.ts, http.MethodPost, f.path("/exec-ticket"), f.admin,
		`{"namespace":"default","pod":"web-1"}`, http.StatusForbidden, "K8S_EXEC_DISABLED")
	expectClosed(t, "exec disabled at redeem", f.ts, ticket, "K8S_EXEC_DISABLED")
}

const k8sTestCA = `-----BEGIN CERTIFICATE-----
MIIBfTCCASOgAwIBAgIUR0v3x0n0Q0n0Q0n0Q0n0Q0n0Q0owCgYIKoZIzj0EAwIw
-----END CERTIFICATE-----`

// 集群登记：凭据只写不读、名称与接入方式不可改、乐观锁、测试连接、Agent 安装与吊销、审计、数据范围与集群选项。
func TestK8s_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	f := newK8sFixture(t)
	ks := f.svc.k8s.store

	// 直连：必须 https、必须带凭据；响应不含凭据。
	direct := func(server, cred string) string {
		return `{"spec":{"name":"dev-1","env":"dev","accessMode":2,"apiServer":"` + server + `","enabled":true}` + cred + `}`
	}
	expectStatus(t, "http api server", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		direct("http://10.0.0.1:6443", `,"credential":{"token":"tok-123"}`), http.StatusBadRequest, "K8S_CLUSTER_INVALID", "https")
	expectStatus(t, "embedded credentials", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		direct("https://u:p@10.0.0.1:6443", `,"credential":{"token":"tok-123"}`), http.StatusBadRequest, "K8S_CLUSTER_INVALID")
	expectStatus(t, "missing credential", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		direct("https://10.0.0.1:6443", ""), http.StatusBadRequest, "K8S_CLUSTER_INVALID")
	expectStatus(t, "bad CA", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		`{"spec":{"name":"dev-1","accessMode":2,"apiServer":"https://10.0.0.1:6443","caCert":`+jsonString(k8sTestCA)+`,"enabled":true},"credential":{"token":"tok-123"}}`,
		http.StatusBadRequest, "K8S_CLUSTER_INVALID", "ca certificate")
	expectStatus(t, "agent with credential", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		`{"spec":{"name":"x-1","accessMode":1,"agentLocation":2,"enabled":true},"credential":{"token":"tok-123"}}`, http.StatusBadRequest, "K8S_CLUSTER_INVALID")
	body := expectStatus(t, "create direct", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		direct("https://10.0.0.1:6443", `,"credential":{"token":"super-secret-token"}`), http.StatusOK, `"hasCredential":true`)
	if strings.Contains(body, "super-secret-token") {
		t.Fatalf("create response leaks the credential: %s", body)
	}
	var created struct{ Cluster struct{ Id string } }
	_ = json.Unmarshal([]byte(body), &created)
	directID := created.Cluster.Id
	if sealed := string(ks.Credential(directID)); sealed != `sealed:{"token":"super-secret-token"}` {
		t.Fatalf("stored credential = %q", sealed)
	}
	expectStatus(t, "duplicate name", f.ts, http.MethodPost, "/api/v1/k8s/cluster", f.admin,
		direct("https://10.0.0.2:6443", `,"credential":{"token":"t2"}`), http.StatusConflict, "K8S_CLUSTER_NAME_EXISTS")

	// 编辑：不传凭据沿用；名称 / 接入方式不可改；版本冲突。
	upd := func(name string, mode int, version int) string {
		loc := 0
		api := `,"apiServer":"https://10.0.0.1:6443"`
		if mode == 1 {
			loc, api = 1, ""
		}
		return `{"spec":{"name":"` + name + `","env":"dev","accessMode":` + jsonInt(mode) + `,"agentLocation":` + jsonInt(loc) + api + `,"enabled":true},"version":` + jsonInt(version) + `}`
	}
	expectStatus(t, "rename", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+directID, f.admin, upd("dev-2", 2, 1), http.StatusBadRequest, "cannot be changed")
	expectStatus(t, "change mode", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+directID, f.admin, upd("dev-1", 1, 1), http.StatusBadRequest, "cannot be changed")
	expectStatus(t, "stale version", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+directID, f.admin, upd("dev-1", 2, 7), http.StatusConflict, "K8S_VERSION_CONFLICT")
	expectStatus(t, "update", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+directID, f.admin, upd("dev-1", 2, 1), http.StatusOK,
		`"apiServer":"https://10.0.0.1:6443"`, `"hasCredential":true`, `"version":2`)
	// 已保存的凭据只属于已保存的地址：改地址而不重填凭据会把凭据发给新地址，必须拒绝（编辑与测试连接都是）。
	moved := `{"spec":{"name":"dev-1","env":"dev","accessMode":2,"agentLocation":0,"apiServer":"https://evil.example:6443","enabled":true},"version":2}`
	expectStatus(t, "move address keeping credential", f.ts, http.MethodPut, "/api/v1/k8s/cluster/"+directID, f.admin, moved,
		http.StatusBadRequest, "K8S_CLUSTER_INVALID", "re-enter the credential")
	expectStatus(t, "test saved credential against another address", f.ts, http.MethodPost, "/api/v1/k8s/cluster:test", f.admin,
		`{"id":"`+directID+`","apiServer":"https://evil.example:6443"}`, http.StatusBadRequest, "K8S_CLUSTER_INVALID", "re-enter the credential")
	if sealed := string(ks.Credential(directID)); !strings.Contains(sealed, "super-secret-token") {
		t.Fatalf("credential must be kept when not provided, got %q", sealed)
	}

	// 测试连接：转给任一数据面实例；未带凭据时沿用已保存的（只传集群 ID，凭据由数据面读取）。
	expectStatus(t, "test", f.ts, http.MethodPost, "/api/v1/k8s/cluster:test", f.admin,
		`{"id":"`+directID+`","apiServer":"https://10.0.0.1:6443"}`, http.StatusOK, `"ok":true`, `"version":"v1.35.1"`, `"impersonate"`)
	var test kc.ClusterTestRequest
	_ = json.Unmarshal([]byte(f.plane.body(kc.InternalOpPath+kc.OpClusterTest)), &test)
	if test.ClusterID != directID || test.Credential.Token != "" || test.APIServer != "https://10.0.0.1:6443" {
		t.Fatalf("forwarded test request = %+v", test)
	}
	expectStatus(t, "test without credential", f.ts, http.MethodPost, "/api/v1/k8s/cluster:test", f.admin,
		`{"apiServer":"https://10.0.0.9:6443"}`, http.StatusBadRequest, "K8S_CLUSTER_INVALID")

	// Agent：安装清单带一次性的注册令牌；直连集群不适用；吊销清空令牌。
	expectStatus(t, "agent install", f.ts, http.MethodPost, "/api/v1/k8s/cluster/"+f.clusterID+":agent-install", f.admin, `{}`,
		http.StatusOK, `"location":1`, "kind: Deployment", "cluster-admin", "https://tunnel.example.com", "REBORN_REGISTER_TOKEN")
	if ks.TokenCount(f.clusterID, kc.TokenRegister) != 1 {
		t.Fatal("register token was not stored")
	}
	expectStatus(t, "agent install on direct", f.ts, http.MethodPost, "/api/v1/k8s/cluster/"+directID+":agent-install", f.admin, `{}`,
		http.StatusBadRequest, "K8S_CLUSTER_INVALID")
	ks.AddLongTermToken(f.clusterID)
	expectStatus(t, "agent tokens", f.ts, http.MethodGet, "/api/v1/k8s/cluster/"+f.clusterID, f.admin, "", http.StatusOK, `"agentTokens":1`)
	expectStatus(t, "agent revoke", f.ts, http.MethodPost, "/api/v1/k8s/cluster/"+f.clusterID+":agent-revoke", f.admin, `{}`, http.StatusOK)
	if ks.TokenCount(f.clusterID, kc.TokenLongTerm)+ks.TokenCount(f.clusterID, kc.TokenRegister) != 0 || ks.CfgVersion(kc.CfgAgentToken) != 2 {
		t.Fatalf("tokens after revoke: cfg version %d", ks.CfgVersion(kc.CfgAgentToken))
	}

	// 数据范围 + 集群选项：非超管只看到有授权的集群及最高档位。
	bearer, _, roleID := monitorUser(t, f.ts, f.svc, f.admin, "k8s_scoped", k8sOps())
	expectStatus(t, "no scope", f.ts, http.MethodGet, "/api/v1/k8s/cluster-option", bearer, "", http.StatusOK, `"items":[]`)
	scope := func(items string) string { return `{"roleId":"` + roleID + `","items":[` + items + `]}` }
	item := func(cluster, ns string, level int) string {
		return `{"clusterId":"` + cluster + `","namespace":"` + ns + `","level":` + jsonInt(level) + `}`
	}
	expectStatus(t, "admin on namespace", f.ts, http.MethodPut, "/api/v1/k8s/role-scope", f.admin,
		scope(item(f.clusterID, "default", 4)), http.StatusBadRequest, "K8S_SCOPE_INVALID")
	expectStatus(t, "duplicate scope", f.ts, http.MethodPut, "/api/v1/k8s/role-scope", f.admin,
		scope(item(f.clusterID, "default", 1)+","+item(f.clusterID, "default", 2)), http.StatusBadRequest, "K8S_SCOPE_INVALID", "duplicate")
	expectStatus(t, "unknown cluster", f.ts, http.MethodPut, "/api/v1/k8s/role-scope", f.admin,
		scope(item("018f3e1a-7b1c-7cc1-9d2a-3f5e7a9bffff", "*", 1)), http.StatusBadRequest, "K8S_SCOPE_INVALID")
	expectStatus(t, "replace scope", f.ts, http.MethodPut, "/api/v1/k8s/role-scope", f.admin,
		scope(item(f.clusterID, "default", 1)+","+item(f.clusterID, "team-a", 2)), http.StatusOK, `"clusterName":"prod-1"`, `"team-a"`)
	expectStatus(t, "get scope", f.ts, http.MethodGet, "/api/v1/k8s/role-scope?roleId="+roleID, f.admin, "", http.StatusOK, `"namespace":"default"`)
	body = expectStatus(t, "scoped options", f.ts, http.MethodGet, "/api/v1/k8s/cluster-option", bearer, "", http.StatusOK, `"name":"prod-1"`, `"level":2`)
	if strings.Contains(body, "dev-1") {
		t.Fatalf("options must not include clusters without scope: %s", body)
	}
	expectStatus(t, "admin options", f.ts, http.MethodGet, "/api/v1/k8s/cluster-option", f.admin, "", http.StatusOK, `"dev-1"`, `"prod-1"`, `"level":4`)

	// 审计：core 侧的配置变更都有记录，且不含凭据与令牌。
	body = expectStatus(t, "audits", f.ts, http.MethodGet, "/api/v1/k8s/audit?page=1&pageSize=50", f.admin, "", http.StatusOK,
		`"action":"create"`, `"action":"update"`, `"action":"agent_install"`, `"action":"agent_revoke"`, `"action":"scope_replace"`, `"username":"e2e-actor"`)
	for _, a := range ks.Audits() {
		if strings.Contains(string(a.Patch), "super-secret-token") || strings.Contains(string(a.Patch), "sealed:") {
			t.Fatalf("audit leaks a credential: %s", a.Patch)
		}
	}
	var audits struct{ Items []struct{ Id, Action string } }
	_ = json.Unmarshal([]byte(body), &audits)
	var scopeAudit string
	for _, a := range audits.Items {
		if a.Action == kc.ActionScopeReplace {
			scopeAudit = a.Id
		}
	}
	expectStatus(t, "audit detail", f.ts, http.MethodGet, "/api/v1/k8s/audit/"+scopeAudit, f.admin, "", http.StatusOK, "team-a", roleID)
	expectStatus(t, "audit window", f.ts, http.MethodGet,
		"/api/v1/k8s/audit?page=1&pageSize=10&createTimeStart=2026-01-01T00:00:00Z&createTimeEnd=2026-06-01T00:00:00Z", f.admin, "",
		http.StatusBadRequest, "K8S_TIME_RANGE_INVALID")
	expectStatus(t, "exec sessions", f.ts, http.MethodGet, "/api/v1/k8s/exec-session?page=1&pageSize=10", f.admin, "", http.StatusOK)

	// 移除集群：数据范围与令牌一并清除；之后的资源请求 404。
	expectStatus(t, "delete", f.ts, http.MethodDelete, "/api/v1/k8s/cluster/"+f.clusterID, f.admin, "", http.StatusOK)
	expectStatus(t, "scope after delete", f.ts, http.MethodGet, "/api/v1/k8s/role-scope?roleId="+roleID, f.admin, "", http.StatusOK, `"items":[]`)
	expectStatus(t, "resource after delete", f.ts, http.MethodGet, f.path("/resource?version=v1&resource=pods"), f.admin, "",
		http.StatusNotFound, kc.ReasonClusterNotFound)
}

func jsonInt(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
