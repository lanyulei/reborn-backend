package kube

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
)

// fakeCluster 一个够用的假集群：资源目录只有核心组（pods / events / configmaps），其余请求交给 handler。
type fakeCluster struct{ handler http.Handler }

func (f fakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api":
		_, _ = io.WriteString(w, `{"kind":"APIVersions","versions":["v1"]}`)
	case r.Method == http.MethodGet && r.URL.Path == "/apis":
		_, _ = io.WriteString(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1":
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","groupVersion":"v1","resources":[`+
			`{"name":"pods","namespaced":true,"kind":"Pod","verbs":["get","list"]},`+
			`{"name":"events","namespaced":true,"kind":"Event","verbs":["get","list"]},`+
			`{"name":"configmaps","namespaced":true,"kind":"ConfigMap","verbs":["get","create"]},`+
			`{"name":"nodes","namespaced":false,"kind":"Node","verbs":["get","list"]}]}`)
	default:
		f.handler.ServeHTTP(w, r)
	}
}

// clusterNotFound 以集群的 NotFound 状态对象应答。
func clusterNotFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
}

// fakeClusterUser 经真实的客户端栈（隧道拨号器 → 假集群）取得以用户身份访问集群的客户端。
func fakeClusterUser(t *testing.T, handler http.Handler) *userClients {
	t.Helper()
	srv := httptest.NewServer(fakeCluster{handler: handler})
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	m := NewManager(nil, nil, nil, dialerFunc(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}), Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(m.Close)
	if err := m.Connect(context.Background(), &biz.Cluster{ID: "fake", Name: "fake", AccessMode: k8scontract.AccessAgent}); err != nil {
		t.Fatal(err)
	}
	c, err := m.conn("fake")
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.asUser(context.Background(), biz.Identity{UserID: uid1, RoleIDs: []string{role1}})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// seenRequest 到达假集群业务处理函数的一个请求（查询参数已解析）。
type seenRequest struct {
	query url.Values
	path  string
}

// seenRequests 记录到达假集群业务处理函数的请求。
type seenRequests struct {
	mu   sync.Mutex
	seen []seenRequest
}

func (s *seenRequests) add(r *http.Request) {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, seenRequest{query: q, path: r.Method + " " + r.URL.Path})
}

func (s *seenRequests) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = nil
}

// on 「方法 + 路径」为 target 的请求，按到达顺序。
func (s *seenRequests) on(target string) []seenRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []seenRequest
	for _, one := range s.seen {
		if one.path == target {
			out = append(out, one)
		}
	}
	return out
}

// decodeClusterBody 解出发给假集群的请求体：类型化客户端按 protobuf 发送，动态客户端按 JSON，两种都认。
func decodeClusterBody(r *http.Request, into runtime.Object) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	_, _, err = scheme.Codecs.UniversalDeserializer().Decode(raw, nil, into)
	return err
}
