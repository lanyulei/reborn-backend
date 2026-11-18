package biz

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"gopkg.in/yaml.v3"
)

const (
	govUser    = "018f3e1a-0000-7000-8000-000000000001"
	govRole    = "018f3e1a-0000-7000-8000-0000000000a1"
	govCluster = "018f3e1a-0000-7000-8000-0000000000c1"
	govOther   = "018f3e1a-0000-7000-8000-0000000000c2"
)

// govFakes 一组内存实现：令牌、账号状态、角色、集群、路由。
type govFakes struct {
	tokens   map[string]*KubeTokenAuth
	lookups  int
	touches  int
	active   bool
	admin    bool
	roles    []string
	grants   []*RoleScope
	clusters map[string]*Cluster
	routed   []kc.Identity
	created  []*KubeToken
	revoked  []string
}

func (f *govFakes) Lookup(_ context.Context, id string) (*KubeTokenAuth, error) {
	f.lookups++
	return f.tokens[id], nil
}
func (f *govFakes) Touch(context.Context, string, string) error  { f.touches++; return nil }
func (f *govFakes) Active(context.Context, string) (bool, error) { return f.active, nil }
func (f *govFakes) EnabledRoleIDs(context.Context, string) (bool, []string, error) {
	return f.admin, f.roles, nil
}

func (f *govFakes) UserNames(_ context.Context, ids []string) (map[string]string, error) {
	return map[string]string{ids[0]: "alice"}, nil
}
func (f *govFakes) Grants(context.Context, []string) ([]*RoleScope, error) { return f.grants, nil }
func (f *govFakes) List(context.Context, ClusterFilter, int64, int64) ([]*Cluster, int64, error) {
	return nil, 0, nil
}

func (f *govFakes) Get(_ context.Context, id string) (*Cluster, error) {
	if c := f.clusters[id]; c != nil {
		return c, nil
	}
	return nil, ErrClusterNotFound
}

func (f *govFakes) Options(context.Context, bool, []string) ([]*ClusterOption, error) {
	return nil, nil
}

func (f *govFakes) Route(_ context.Context, id kc.Identity, _ string) (string, http.Header, error) {
	f.routed = append(f.routed, id)
	return "http://plane:8060", http.Header{}, nil
}
func (f *govFakes) Invalidate(string) {}

type govTokenRepo struct{ f *govFakes }

func (r govTokenRepo) Create(_ context.Context, t *KubeToken, hash []byte, _ int, _ *AuditEntry) error {
	r.f.created = append(r.f.created, t)
	r.f.tokens[t.ID] = &KubeTokenAuth{ID: t.ID, UserID: t.UserID, ClusterID: t.ClusterID, Hash: hash, ExpireTime: t.ExpireTime, ClusterEnabled: true}
	return nil
}

func (r govTokenRepo) List(context.Context, KubeTokenFilter, int64, int64) ([]*KubeToken, int64, error) {
	return nil, 0, nil
}

func (r govTokenRepo) Revoke(_ context.Context, id, owner, _ string, _ *AuditEntry) (int, error) {
	n := 0
	for tid, t := range r.f.tokens {
		if (id == "" || id == tid) && (owner == "" || owner == t.UserID) && !t.Revoked {
			t.Revoked = true
			r.f.revoked = append(r.f.revoked, tid)
			n++
		}
	}
	return n, nil
}

func newGov(t *testing.T) (*GovernanceUsecase, *govFakes, *time.Time) {
	t.Helper()
	f := &govFakes{
		tokens: map[string]*KubeTokenAuth{}, active: true, roles: []string{govRole},
		grants:   []*RoleScope{{ClusterID: govCluster, Namespace: "team-a", Level: kc.LevelEdit}},
		clusters: map[string]*Cluster{govCluster: {ID: govCluster, Name: "prod-1", Enabled: true}, govOther: {ID: govOther, Name: "prod-2", Enabled: true}},
	}
	planes := NewPlaneUsecase(nil, f, f, f, f, nil)
	uc := NewGovernanceUsecase(govTokenRepo{f}, f, nil, nil, f, f, f, f, f, planes, f, GovOptions{ServerURL: "https://ops.example.com/"})
	now := time.Unix(1_800_000_000, 0)
	uc.now = func() time.Time { return now }
	planes.now = uc.now
	return uc, f, &now
}

func reasonOf(err error) string {
	for _, r := range v1.ErrorReason_name {
		if isReason(err, v1.ErrorReason(v1.ErrorReason_value[r])) {
			return r
		}
	}
	return fmt.Sprint(err)
}

func TestKubeTokenLifecycle(t *testing.T) {
	t.Parallel()
	uc, f, now := newGov(t)
	ctx := context.Background()
	caller := Caller{UserID: govUser, ClientIP: "10.0.0.9"}

	// 签发：有效期范围、数据范围。
	for _, ttl := range []time.Duration{time.Minute, 8 * 24 * time.Hour} {
		if _, err := uc.IssueKubeconfig(ctx, caller, govCluster, ttl, ""); !isReason(err, v1.ErrorReason_K8S_KUBE_TOKEN_INVALID) {
			t.Fatalf("ttl %s: %v", ttl, err)
		}
	}
	if _, err := uc.IssueKubeconfig(ctx, caller, govOther, time.Hour, ""); !isReason(err, v1.ErrorReason_K8S_CLUSTER_NOT_FOUND) {
		t.Fatalf("cluster outside the caller's scope must look missing: %v", err)
	}
	out, err := uc.IssueKubeconfig(ctx, caller, govCluster, 2*time.Hour, " ci ")
	if err != nil {
		t.Fatal(err)
	}
	if out.Server != "https://ops.example.com/api/v1/k8s/proxy/"+govCluster || out.Token.Note != "ci" || out.Token.Username != "alice" {
		t.Fatalf("issued = %+v", out)
	}
	var cfg struct {
		Clusters []struct {
			Cluster struct{ Server string }
		}
		Users []struct {
			User struct{ Token string }
		}
		CurrentContext string `yaml:"current-context"`
	}
	if err := yaml.Unmarshal([]byte(out.Kubeconfig), &cfg); err != nil || cfg.Clusters[0].Cluster.Server != out.Server ||
		cfg.Users[0].User.Token != out.Secret || cfg.CurrentContext != "reborn-prod-1" {
		t.Fatalf("kubeconfig does not round-trip: %v\n%s", err, out.Kubeconfig)
	}
	if strings.Contains(fmt.Sprintf("%+v", f.created[0]), out.Secret) {
		t.Fatal("token secret must not be stored")
	}

	auth := func(bearer, cluster, method, path string) error {
		_, err := uc.AuthorizeKube(ctx, bearer, cluster, method, path, "10.0.0.9")
		return err
	}
	const pods = "/api/v1/namespaces/team-a/pods"
	// 正常请求：身份按用户此刻的角色解析。
	if err := auth(out.Secret, govCluster, "GET", pods); err != nil {
		t.Fatal(err)
	}
	if id := f.routed[0]; id.UserID != govUser || len(id.RoleIDs) != 1 || id.RoleIDs[0] != govRole || id.IsAdmin || id.ClientIP != "10.0.0.9" {
		t.Fatalf("identity = %+v", id)
	}
	// 5 秒内的重复请求走缓存。
	lookups := f.lookups
	_ = auth(out.Secret, govCluster, "GET", pods)
	if f.lookups != lookups {
		t.Fatal("second request within the cache window must not hit the database")
	}
	// 各种不合法的凭据：一律 401，不区分原因。
	id, secret, _ := kc.ParseToken(out.Secret)
	for name, bearer := range map[string]string{
		"garbage": "abc", "wrong secret": id + "." + strings.Repeat("x", len(secret)),
		"unknown id": "018f3e1a-0000-7000-8000-00000000ffff." + secret, "empty": "",
	} {
		if err := auth(bearer, govCluster, "GET", pods); !isReason(err, v1.ErrorReason_K8S_TICKET_INVALID) {
			t.Errorf("%s: %s", name, reasonOf(err))
		}
	}
	// 令牌只对签发的集群有效。
	if err := auth(out.Secret, govOther, "GET", pods); !isReason(err, v1.ErrorReason_K8S_TICKET_INVALID) {
		t.Fatalf("token used on another cluster: %s", reasonOf(err))
	}

	// 账号停用：缓存过期后拒绝。
	f.active = false
	*now = now.Add(kubeSessionTTL + time.Second)
	if err := auth(out.Secret, govCluster, "GET", pods); !isReason(err, v1.ErrorReason_K8S_TICKET_INVALID) {
		t.Fatalf("disabled user: %s", reasonOf(err))
	}
	f.active = true
	// 角色变化：下一个缓存窗口里身份随之变化。
	f.roles = nil
	*now = now.Add(kubeSessionTTL + time.Second)
	if err := auth(out.Secret, govCluster, "GET", pods); err != nil {
		t.Fatal(err)
	}
	if got := f.routed[len(f.routed)-1]; len(got.RoleIDs) != 0 {
		t.Fatalf("identity must follow the user's current roles, got %+v", got)
	}

	// 需审批的集群：非超管的变更被拒绝，读与 exec 不受影响；超管放行。
	f.tokens[id].ApprovalRequired = true
	*now = now.Add(kubeSessionTTL + time.Second)
	if err := auth(out.Secret, govCluster, "POST", pods); !isReason(err, v1.ErrorReason_K8S_APPROVAL_REQUIRED) {
		t.Fatalf("write on an approval cluster: %s", reasonOf(err))
	}
	if err := auth(out.Secret, govCluster, "GET", pods); err != nil {
		t.Fatalf("read on an approval cluster: %v", err)
	}
	if err := auth(out.Secret, govCluster, "POST", pods+"/p/exec"); err != nil {
		t.Fatalf("exec on an approval cluster: %v", err)
	}
	f.admin = true
	*now = now.Add(kubeSessionTTL + time.Second)
	if err := auth(out.Secret, govCluster, "DELETE", pods+"/p"); err != nil {
		t.Fatalf("admin write on an approval cluster: %v", err)
	}
	f.admin = false

	// 吊销：本实例立即生效（不等缓存过期）。
	if err := uc.RevokeKubeToken(ctx, caller, id); err != nil {
		t.Fatal(err)
	}
	if err := auth(out.Secret, govCluster, "GET", pods); !isReason(err, v1.ErrorReason_K8S_TICKET_INVALID) {
		t.Fatalf("revoked token: %s", reasonOf(err))
	}
	if err := uc.RevokeKubeToken(ctx, caller, id); !isReason(err, v1.ErrorReason_K8S_KUBE_TOKEN_NOT_FOUND) {
		t.Fatalf("revoking twice: %s", reasonOf(err))
	}

	// 失去全部角色（即失去数据范围）的用户不能再签发。
	if _, err := uc.IssueKubeconfig(ctx, caller, govCluster, time.Hour, ""); !isReason(err, v1.ErrorReason_K8S_CLUSTER_NOT_FOUND) {
		t.Fatalf("user without scope: %s", reasonOf(err))
	}
	f.roles = []string{govRole}

	// 过期。
	out2, err := uc.IssueKubeconfig(ctx, caller, govCluster, time.Hour, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth(out2.Secret, govCluster, "GET", pods); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Hour + time.Second)
	if err := auth(out2.Secret, govCluster, "GET", pods); !isReason(err, v1.ErrorReason_K8S_TICKET_INVALID) {
		t.Fatalf("expired token: %s", reasonOf(err))
	}

	// 吊销他人的全部令牌仅限超管。
	if _, err := uc.RevokeUserKubeTokens(ctx, caller, govUser); !isReason(err, v1.ErrorReason_K8S_FORBIDDEN) {
		t.Fatalf("non-admin revoke-user: %s", reasonOf(err))
	}
}

func TestKubeTokenRateLimit(t *testing.T) {
	t.Parallel()
	uc, _, now := newGov(t)
	ctx := context.Background()
	out, err := uc.IssueKubeconfig(ctx, Caller{UserID: govUser}, govCluster, time.Hour, "")
	if err != nil {
		t.Fatal(err)
	}
	limited := 0
	for i := 0; i < int(kubeBurst)+20; i++ {
		route, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/version", "")
		if isReason(err, v1.ErrorReason_K8S_TOO_MANY_STREAMS) {
			limited++
		}
		if route != nil {
			route.Release()
		}
	}
	if limited != 20 {
		t.Fatalf("limited = %d, want 20", limited)
	}
	*now = now.Add(time.Second)
	if _, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/version", ""); err != nil {
		t.Fatalf("bucket must refill: %v", err)
	}
}

func gz(t *testing.T, s string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, _ = zw.Write([]byte(s))
	_ = zw.Close()
	return b.Bytes()
}

func TestDecodeRecording(t *testing.T) {
	t.Parallel()
	b64 := base64.StdEncoding.EncodeToString
	chunks := [][]byte{
		gz(t, `[0,"r","120x40"]`+"\n"+`[15,"o","`+b64([]byte("$ ls\r\n"))+`"]`+"\n"),
		gz(t, `[900,"o","`+b64([]byte{0xff, 0x00, 'x'})+`"]`+"\n"+"not json\n"+`[1,"i","aGk="]`+"\n"+`[2,"r","bad"]`+"\n"+`[3,"o","***"]`+"\n"),
	}
	events, err := DecodeRecording(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Cols != 120 || events[0].Rows != 40 || string(events[1].Data) != "$ ls\r\n" ||
		events[2].OffsetMS != 900 || len(events[2].Data) != 3 {
		t.Fatalf("events = %+v", events)
	}
	if _, err := DecodeRecording([][]byte{[]byte("not gzip")}); err == nil {
		t.Fatal("corrupted chunk must fail")
	}
}

// TestAlertRuleTemplate 内置规则模板：是合法的 Prometheus 规则 YAML，规则名唯一，每条都有表达式、
// 级别与摘要；按对象聚合的规则保留 cluster 标签（告警事件据此跳转到资源页）。
func TestAlertRuleTemplate(t *testing.T) {
	t.Parallel()
	content, n := AlertRuleTemplate()
	var file struct {
		Groups []struct {
			Name  string
			Rules []struct {
				Alert       string
				Expr        string
				For         string
				Labels      map[string]string
				Annotations map[string]string
			}
		}
	}
	if err := yaml.Unmarshal([]byte(content), &file); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	total := 0
	for _, g := range file.Groups {
		for _, r := range g.Rules {
			total++
			if r.Alert == "" || r.Expr == "" || r.Annotations["summary"] == "" || seen[r.Alert] {
				t.Errorf("group %s: bad or duplicated rule %+v", g.Name, r)
			}
			seen[r.Alert] = true
			if s := r.Labels["severity"]; s != "info" && s != "warning" && s != "critical" {
				t.Errorf("%s: severity %q", r.Alert, s)
			}
			if strings.Contains(r.Expr, " by (") && !strings.Contains(r.Expr, "by (cluster") {
				t.Errorf("%s: aggregation drops the cluster label: %s", r.Alert, r.Expr)
			}
		}
	}
	if total != n || total < 20 || len(file.Groups) != 5 {
		t.Fatalf("rules = %d (reported %d), groups = %d", total, n, len(file.Groups))
	}
}

// 并发连接上限：同一令牌在途的请求（watch、exec 等长连接）超过上限即拒绝，归还后恢复；Release 幂等。
func TestKubeTokenConnectionCap(t *testing.T) {
	t.Parallel()
	uc, _, now := newGov(t)
	ctx := context.Background()
	out, err := uc.IssueKubeconfig(ctx, Caller{UserID: govUser}, govCluster, time.Hour, "")
	if err != nil {
		t.Fatal(err)
	}
	var held []*KubeRoute
	for i := 0; i < maxKubeConnsPerToken; i++ {
		if i%100 == 99 {
			*now = now.Add(10 * time.Second) // 不让请求速率限制先触发
		}
		route, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/api/v1/pods", "")
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		held = append(held, route)
	}
	*now = now.Add(10 * time.Second)
	if _, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/api/v1/pods", ""); !isReason(err, v1.ErrorReason_K8S_TOO_MANY_STREAMS) {
		t.Fatalf("over the cap: err = %v", err)
	}
	held[0].Release()
	held[0].Release() // 重复归还不多退名额
	if _, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/api/v1/pods", ""); err != nil {
		t.Fatalf("after one release: %v", err)
	}
	if _, err := uc.AuthorizeKube(ctx, out.Secret, govCluster, "GET", "/api/v1/pods", ""); !isReason(err, v1.ErrorReason_K8S_TOO_MANY_STREAMS) {
		t.Fatalf("double release must free only one slot: err = %v", err)
	}
}
