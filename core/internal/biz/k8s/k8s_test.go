package biz

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"gopkg.in/yaml.v3"
)

// testPair 现生成的自签证书与私钥（PEM）；不对应任何真实集群。
func testPair(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "k8s-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func isReason(err error, reason v1.ErrorReason) bool {
	e := kerrors.FromError(err)
	return err != nil && e.Reason == reason.String()
}

func TestClusterInputNormalize(t *testing.T) {
	t.Parallel()
	cert, _ := testPair(t)
	direct := func(mut func(*ClusterInput)) ClusterInput {
		in := ClusterInput{Name: "dev-1", AccessMode: kc.AccessDirect, APIServer: "https://10.0.0.1:6443"}
		if mut != nil {
			mut(&in)
		}
		return in
	}
	ok := []ClusterInput{
		direct(nil),
		direct(func(in *ClusterInput) { in.CACert = cert + cert; in.ProxyURL = "http://proxy:3128"; in.Env = "prod" }),
		{Name: "a", AccessMode: kc.AccessAgent, AgentLocation: kc.AgentLocationInCluster},
		{Name: "relay-9", AccessMode: kc.AccessAgent, AgentLocation: kc.AgentLocationRelay},
	}
	for _, in := range ok {
		if err := in.normalize(); err != nil {
			t.Errorf("normalize(%+v) = %v, want ok", in.Name, err)
		}
	}
	bad := map[string]ClusterInput{
		"uppercase name":       direct(func(in *ClusterInput) { in.Name = "Dev" }),
		"name with quote":      direct(func(in *ClusterInput) { in.Name = `a";rm -rf /` }),
		"name with newline":    direct(func(in *ClusterInput) { in.Name = "a\nb" }),
		"name too long":        direct(func(in *ClusterInput) { in.Name = strings.Repeat("a", 64) }),
		"leading hyphen":       direct(func(in *ClusterInput) { in.Name = "-a" }),
		"bad env":              direct(func(in *ClusterInput) { in.Env = "Prod!" }),
		"http api server":      direct(func(in *ClusterInput) { in.APIServer = "http://10.0.0.1:6443" }),
		"blank api server":     direct(func(in *ClusterInput) { in.APIServer = " " }),
		"userinfo":             direct(func(in *ClusterInput) { in.APIServer = "https://u:p@10.0.0.1" }),
		"query":                direct(func(in *ClusterInput) { in.APIServer = "https://10.0.0.1/?a=1" }),
		"fragment":             direct(func(in *ClusterInput) { in.APIServer = "https://10.0.0.1/#x" }),
		"no host":              direct(func(in *ClusterInput) { in.APIServer = "https://" }),
		"socks proxy":          direct(func(in *ClusterInput) { in.ProxyURL = "socks5://p:1080" }),
		"garbage CA":           direct(func(in *ClusterInput) { in.CACert = "not a cert" }),
		"CA with trailing":     direct(func(in *ClusterInput) { in.CACert = cert + "garbage" }),
		"direct with location": direct(func(in *ClusterInput) { in.AgentLocation = kc.AgentLocationRelay }),
		"agent without loc":    {Name: "a", AccessMode: kc.AccessAgent},
		"agent with server":    {Name: "a", AccessMode: kc.AccessAgent, AgentLocation: 1, APIServer: "https://x"},
		"unknown mode":         {Name: "a", AccessMode: 3},
	}
	for name, in := range bad {
		if err := in.normalize(); !isReason(err, v1.ErrorReason_K8S_CLUSTER_INVALID) {
			t.Errorf("%s: err = %v, want K8S_CLUSTER_INVALID", name, err)
		}
	}
}

func TestNormalizeCredential(t *testing.T) {
	t.Parallel()
	cert, key := testPair(t)
	_, otherKey := testPair(t)
	if c, err := normalizeCredential(&CredentialInput{Token: " tok \n"}); err != nil || c.Token != "tok" {
		t.Fatalf("token: %+v %v", c, err)
	}
	if c, err := normalizeCredential(&CredentialInput{ClientCert: cert, ClientKey: key}); err != nil || c.Token != "" || c.ClientKey == "" {
		t.Fatalf("pair: %v", err)
	}
	for name, in := range map[string]CredentialInput{
		"empty":          {},
		"both":           {Token: "t", ClientCert: cert, ClientKey: key},
		"cert only":      {ClientCert: cert},
		"mismatched key": {ClientCert: cert, ClientKey: otherKey},
		"token spaces":   {Token: "a b"},
	} {
		_, err := normalizeCredential(&in)
		if err == nil {
			t.Errorf("%s: want error", name)
		} else if strings.Contains(err.Error(), "PRIVATE KEY") {
			t.Errorf("%s: error leaks key material", name)
		}
	}
}

func kubeconfig(user string) string {
	return `apiVersion: v1
kind: Config
current-context: ctx-a
clusters:
- name: c1
  cluster:
    server: https://10.0.0.1:6443
    proxy-url: http://proxy:3128
contexts:
- name: ctx-a
  context: {cluster: c1, user: u1}
- name: ctx-b
  context: {cluster: c1, user: u2}
users:
- name: u1
  user:
` + user + `
- name: u2
  user:
    token: tok-b
`
}

func TestParseKubeconfig(t *testing.T) {
	t.Parallel()
	cert, key := testPair(t)
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	k, err := ParseKubeconfig(kubeconfig("    token: tok-a"), "")
	if err != nil || k.Context != "ctx-a" || k.APIServer != "https://10.0.0.1:6443" || k.ProxyURL != "http://proxy:3128" ||
		k.Credential.Token != "tok-a" || len(k.Contexts) != 2 {
		t.Fatalf("token kubeconfig = %+v, %v", k, err)
	}
	if k, err = ParseKubeconfig(kubeconfig("    token: tok-a"), "ctx-b"); err != nil || k.Credential.Token != "tok-b" {
		t.Fatalf("explicit context = %+v, %v", k, err)
	}
	k, err = ParseKubeconfig(kubeconfig("    client-certificate-data: "+b64(cert)+"\n    client-key-data: "+b64(key)), "")
	if err != nil || k.Credential.ClientCert != cert || k.Credential.ClientKey != key || k.Credential.Token != "" {
		t.Fatalf("cert kubeconfig: %v", err)
	}
	withCA := strings.Replace(kubeconfig("    token: tok-a"), "    proxy-url:", "    certificate-authority-data: "+b64(cert)+"\n    proxy-url:", 1)
	if k, err = ParseKubeconfig(withCA, ""); err != nil || k.CACert != cert {
		t.Fatalf("CA kubeconfig: %v", err)
	}

	rejected := map[string]string{
		"exec":          kubeconfig("    exec:\n      command: aws\n      args: [eks, get-token]"),
		"auth provider": kubeconfig("    auth-provider:\n      name: gcp"),
		"token file":    kubeconfig("    tokenFile: /var/run/token"),
		"cert files":    kubeconfig("    client-certificate: /a.crt\n    client-key: /a.key"),
		"basic auth":    kubeconfig("    username: admin\n    password: p"),
		"no credential": kubeconfig("    {}"),
		"CA file":       strings.Replace(kubeconfig("    token: t"), "    proxy-url:", "    certificate-authority: /ca.crt\n    proxy-url:", 1),
		"insecure":      strings.Replace(kubeconfig("    token: t"), "    proxy-url:", "    insecure-skip-tls-verify: true\n    proxy-url:", 1),
		"http server":   strings.Replace(kubeconfig("    token: t"), "https://10.0.0.1:6443", "http://10.0.0.1:8080", 1),
		"bad base64":    kubeconfig("    client-certificate-data: '***'\n    client-key-data: '***'"),
		"not yaml":      "{{{",
		"empty":         "apiVersion: v1\n",
	}
	for name, raw := range rejected {
		if _, err := ParseKubeconfig(raw, ""); !isReason(err, v1.ErrorReason_K8S_CLUSTER_INVALID) {
			t.Errorf("%s: err = %v, want K8S_CLUSTER_INVALID", name, err)
		}
	}
	if _, err := ParseKubeconfig(kubeconfig("    exec:\n      command: aws"), ""); err == nil || !strings.Contains(err.Error(), "agent access") {
		t.Errorf("exec kubeconfig must suggest agent access, got %v", err)
	}
	if _, err := ParseKubeconfig(kubeconfig("    token: t"), "nope"); !isReason(err, v1.ErrorReason_K8S_CLUSTER_INVALID) {
		t.Errorf("unknown context: %v", err)
	}
}

func TestValidateScopes(t *testing.T) {
	t.Parallel()
	c := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b0001"
	if err := validateScopes([]*RoleScope{
		{ClusterID: c, Namespace: "*", Level: kc.LevelClusterAdmin}, {ClusterID: c, Namespace: "team-a", Level: kc.LevelEdit},
	}); err != nil {
		t.Fatalf("valid scopes: %v", err)
	}
	if err := validateScopes(nil); err != nil {
		t.Fatalf("empty scopes: %v", err)
	}
	many := make([]*RoleScope, 0, maxRoleScopes+1)
	for i := range maxRoleScopes + 1 {
		many = append(many, &RoleScope{ClusterID: c, Namespace: "ns-" + strconv.Itoa(i), Level: 1})
	}
	for name, items := range map[string][]*RoleScope{
		"admin on namespace": {{ClusterID: c, Namespace: "default", Level: kc.LevelClusterAdmin}},
		"level 0":            {{ClusterID: c, Namespace: "default", Level: 0}},
		"level 5":            {{ClusterID: c, Namespace: "*", Level: 5}},
		"bad namespace":      {{ClusterID: c, Namespace: "Kube_System", Level: 1}},
		"empty namespace":    {{ClusterID: c, Namespace: "", Level: 1}},
		"no cluster":         {{Namespace: "*", Level: 1}},
		"duplicate":          {{ClusterID: c, Namespace: "a", Level: 1}, {ClusterID: c, Namespace: "a", Level: 2}},
		"too many":           many,
	} {
		if err := validateScopes(items); !isReason(err, v1.ErrorReason_K8S_SCOPE_INVALID) {
			t.Errorf("%s: err = %v, want K8S_SCOPE_INVALID", name, err)
		}
	}
}

func TestWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	s, e, err := window(nil, nil, now)
	if err != nil || !e.Equal(now) || e.Sub(s) != defaultAuditWindow {
		t.Fatalf("default window = %v..%v, %v", s, e, err)
	}
	if s, e, err = window(nil, at(-24*time.Hour), now); err != nil || e.Sub(s) != defaultAuditWindow {
		t.Fatalf("end only: %v..%v, %v", s, e, err)
	}
	if _, _, err = window(at(-maxAuditWindow), nil, now); err != nil {
		t.Fatalf("exactly 90 days must be allowed: %v", err)
	}
	if _, _, err = window(at(-maxAuditWindow-time.Second), nil, now); !isReason(err, v1.ErrorReason_K8S_TIME_RANGE_INVALID) {
		t.Fatalf("over 90 days: %v", err)
	}
	if _, _, err = window(at(time.Hour), at(-time.Hour), now); !isReason(err, v1.ErrorReason_K8S_TIME_RANGE_INVALID) {
		t.Fatalf("reversed: %v", err)
	}
}

func TestObjectYAML(t *testing.T) {
	t.Parallel()
	got := ObjectYAML(json.RawMessage(`{"kind":"Deployment","metadata":{"name":"web","labels":{"app":"web"}},"spec":{"replicas":3,"args":["a","b"]}}`))
	want := "kind: Deployment\nmetadata:\n  name: web\n  labels:\n    app: web\nspec:\n  replicas: 3\n  args:\n    - a\n    - b\n"
	if got != want {
		t.Fatalf("ObjectYAML =\n%s\nwant\n%s", got, want)
	}
	for _, raw := range []string{"", "null", " "} {
		if out := ObjectYAML(json.RawMessage(raw)); out != "" {
			t.Errorf("ObjectYAML(%q) = %q", raw, out)
		}
	}
}

func TestAgentRendering(t *testing.T) {
	t.Parallel()
	// 注册令牌与地址里放进 YAML / shell 的元字符：渲染结果仍须是合法的 YAML，且命令里被单引号包住。
	hostile := `tok"en: x'$(id)` + "\n- injected: true"
	a := agentConfig{ServerURL: "https://tunnel.example.com/a?b='c'", Image: "registry.example.com/reborn/agent:v1"}
	if err := a.check(); err != nil {
		t.Fatalf("check: %v", err)
	}
	manifest, err := a.manifest("prod-1", hostile)
	if err != nil {
		t.Fatal(err)
	}
	dec := yaml.NewDecoder(strings.NewReader(manifest))
	kinds := map[string]map[string]any{}
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("manifest is not valid YAML: %v\n%s", err, manifest)
		}
		kinds[doc["kind"].(string)] = doc
	}
	for _, kind := range []string{"Namespace", "ServiceAccount", "ClusterRoleBinding", "Secret", "Deployment"} {
		if kinds[kind] == nil {
			t.Fatalf("manifest lacks %s", kind)
		}
	}
	if len(kinds) != 5 {
		t.Fatalf("manifest has unexpected documents: %d", len(kinds))
	}
	if tok := kinds["Secret"]["stringData"].(map[string]any)["token"]; tok != hostile {
		t.Fatalf("token did not round-trip: %q", tok)
	}
	if ref := kinds["ClusterRoleBinding"]["roleRef"].(map[string]any)["name"]; ref != "cluster-admin" {
		t.Fatalf("roleRef = %v", ref)
	}
	spec := kinds["Deployment"]["spec"].(map[string]any)
	if spec["replicas"] != 1 {
		t.Fatalf("replicas = %v", spec["replicas"])
	}
	pod := spec["template"].(map[string]any)["spec"].(map[string]any)
	if pod["securityContext"].(map[string]any)["runAsNonRoot"] != true {
		t.Fatal("pod must run as non-root")
	}
	ctr := pod["containers"].([]any)[0].(map[string]any)
	sc := ctr["securityContext"].(map[string]any)
	if sc["readOnlyRootFilesystem"] != true || sc["allowPrivilegeEscalation"] != false || ctr["resources"].(map[string]any)["limits"] == nil {
		t.Fatalf("container hardening missing: %v", ctr)
	}
	envs := map[string]any{}
	for _, e := range ctr["env"].([]any) {
		m := e.(map[string]any)
		envs[m["name"].(string)] = m["value"]
	}
	if envs[AgentEnvServerURL] != a.ServerURL {
		t.Fatalf("server url env = %v", envs[AgentEnvServerURL])
	}
	if _, plain := envs[AgentEnvRegisterToken].(string); plain {
		t.Fatal("register token must come from the Secret, not a literal env value")
	}

	cmd, err := a.relayCommand("prod-1", hostile)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"-e " + AgentEnvRegisterToken + "=" + shellQuote(hostile),
		"-e " + AgentEnvServerURL + "=" + `'https://tunnel.example.com/a?b='\''c'\'''`,
		"--read-only", "--cap-drop ALL", "/etc/reborn/prod-1.kubeconfig:/etc/reborn/kubeconfig:ro", "'registry.example.com/reborn/agent:v1'",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("relay command lacks %q:\n%s", want, cmd)
		}
	}
	// 集群名进入文件路径与容器名：名称规则本身排除了一切 shell / YAML 元字符。
	for _, name := range []string{`a b`, `a'b`, "a$b", "a/b", "a\nb", `a"b`, "../x", "a;b", "A"} {
		if clusterNamePattern.MatchString(name) {
			t.Errorf("cluster name %q must be rejected", name)
		}
	}
	// 未配置或配置非法：不生成安装物。
	for _, bad := range []agentConfig{{}, {ServerURL: "https://x"}, {Image: "img"}, {ServerURL: "ftp://x", Image: "img"}, {ServerURL: "https://x", Image: "img; rm -rf /"}} {
		if err := bad.check(); !isReason(err, v1.ErrorReason_K8S_AGENT_UNCONFIGURED) {
			t.Errorf("check(%+v) = %v", bad, err)
		}
	}
}

// 守护：kit/k8scontract 的每个 Reason* 常量都在 proto 的 ErrorReason 枚举里，且 PlaneError 原样保留它。
func TestContractReasonsExistInProto(t *testing.T) {
	t.Parallel()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../../../../kit/k8scontract/api.go", nil, 0)
	if err != nil {
		t.Fatalf("parse k8scontract: %v", err)
	}
	n := 0
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if !strings.HasPrefix(name.Name, "Reason") || i >= len(spec.Values) {
				continue
			}
			lit, ok := spec.Values[i].(*ast.BasicLit)
			if !ok {
				continue
			}
			reason, _ := strconv.Unquote(lit.Value)
			n++
			if _, ok := v1.ErrorReason_value[reason]; !ok {
				t.Errorf("k8scontract.%s = %q is missing from api/k8s/v1 ErrorReason", name.Name, reason)
			}
			if got := kerrors.FromError(PlaneError(409, reason, "m")); got.Reason != reason || got.Code != 409 || got.Message != "m" {
				t.Errorf("PlaneError does not preserve %q: %+v", reason, got)
			}
		}
		return true
	})
	if n < 15 {
		t.Fatalf("found only %d Reason constants in k8scontract; the guard is not reading the right file", n)
	}
}

func TestPlaneError(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		code       int
		reason     string
		wantCode   int32
		wantReason string
	}{
		{403, kc.ReasonForbidden, 403, kc.ReasonForbidden},
		{421, kc.ReasonNotOwner, 421, kc.ReasonNotOwner},
		{400, "NOT_IN_ENUM", 502, kc.ReasonUpstream},
		{200, kc.ReasonForbidden, 502, kc.ReasonUpstream},
		{700, kc.ReasonForbidden, 502, kc.ReasonUpstream},
	} {
		got := kerrors.FromError(PlaneError(c.code, c.reason, "x"))
		if got.Code != c.wantCode || got.Reason != c.wantReason {
			t.Errorf("PlaneError(%d, %s) = %d %s", c.code, c.reason, got.Code, got.Reason)
		}
	}
	if !IsNotOwner(PlaneError(421, kc.ReasonNotOwner, "")) || IsNotOwner(PlaneError(403, kc.ReasonForbidden, "")) || IsNotOwner(nil) {
		t.Fatal("IsNotOwner mismatch")
	}
}

// ---- 派发 ----

type fakeOutbox struct {
	rows   []*OutboxItem
	status map[string]int32
	reason map[string]string
}

func (f *fakeOutbox) ClaimTx(_ context.Context, _ txrun.Tx, exclude []string) (*OutboxItem, error) {
	for _, r := range f.rows {
		if f.status[r.ID] == OutboxPending && !strings.Contains(strings.Join(exclude, ","), r.ID) {
			cp := *r
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *fakeOutbox) FinishTx(_ context.Context, _ txrun.Tx, id string, status int32, reason string) error {
	f.status[id], f.reason[id] = status, reason
	return nil
}

func (f *fakeOutbox) Fail(_ context.Context, id, reason string, maxAttempts int32) (bool, error) {
	for _, r := range f.rows {
		if r.ID == id {
			r.Attempts++
			f.reason[id] = reason
			if r.Attempts >= maxAttempts {
				f.status[id] = OutboxDropped
				return true, nil
			}
		}
	}
	return false, nil
}

type fakeTxm struct{}

func (fakeTxm) WithinTx(_ context.Context, fn func(tx txrun.Tx) error) error { return fn(nil) }

type fakeEvents struct {
	events []biznotify.Event
	fail   map[string]error // event type → error
}

func (f *fakeEvents) NotifyEvent(_ context.Context, _ txrun.Tx, e biznotify.Event) (*biznotify.Event, error) {
	if err := f.fail[e.EventType]; err != nil {
		return nil, err
	}
	f.events = append(f.events, e)
	return &e, nil
}

func TestDispatch(t *testing.T) {
	t.Parallel()
	recipients := make([]string, 45)
	for i := range recipients {
		recipients[i] = "u" + strconv.Itoa(i)
	}
	payload := json.RawMessage(`{"cluster_name":"prod-1","error":"dial timeout"}`)
	box := &fakeOutbox{status: map[string]int32{}, reason: map[string]string{}, rows: []*OutboxItem{
		{ID: "a", EventType: kc.NotifyClusterUnreachable, Severity: 3, DedupKey: "k8s:c1:1:t", GroupKey: "k8s:c1", Payload: payload},
		{ID: "b", EventType: "k8s.event.oomkilled", Severity: 2, DedupKey: strings.Repeat("d", 200), Recipients: recipients, Payload: json.RawMessage(`{"title":"OOM"}`)},
		{ID: "c", EventType: "k8s.broken", Severity: 2, DedupKey: "c"},
		{ID: "d", EventType: "k8s.invalid", Severity: 2, DedupKey: "d", Attempts: OutboxMaxAttempts - 1},
		{ID: "e", EventType: "k8s.invalid", Severity: 2, DedupKey: "e"},
	}}
	for _, r := range box.rows {
		box.status[r.ID] = OutboxPending
	}
	events := &fakeEvents{fail: map[string]error{
		"k8s.broken":  biznotify.ErrNotificationInvalid,
		"k8s.invalid": context.DeadlineExceeded,
	}}
	uc := NewDispatchUsecase(fakeTxm{}, box, events, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done, err := uc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// a 派发；b 分 3 片派发；c 内容不合法直接丢弃（算已处理）；d 到达上限被丢弃；e 失败留待重试。
	if done != 3 {
		t.Fatalf("done = %d, want 3", done)
	}
	want := map[string]int32{"a": OutboxDone, "b": OutboxDone, "c": OutboxDropped, "d": OutboxDropped, "e": OutboxPending}
	for id, st := range want {
		if box.status[id] != st {
			t.Errorf("row %s status = %d, want %d (%s)", id, box.status[id], st, box.reason[id])
		}
	}
	if len(events.events) != 4 {
		t.Fatalf("events = %d, want 4", len(events.events))
	}
	first := events.events[0]
	if first.Title != "集群不可达：prod-1" || first.Severity != 3 || first.GroupKey != "k8s:c1" || len(first.Recipients) != 0 || first.BizID != "" {
		t.Fatalf("first event = %+v", first)
	}
	seen := map[string]bool{}
	total := 0
	for _, e := range events.events[1:] {
		if e.Title != "OOM" || len(e.DedupKey) > maxNotifyKeyLen || seen[e.DedupKey] || len(e.Recipients) > maxNotifyRecipients {
			t.Fatalf("chunk event = %+v", e)
		}
		seen[e.DedupKey] = true
		total += len(e.Recipients)
	}
	if total != 45 {
		t.Fatalf("recipients delivered = %d", total)
	}
	if box.rows[4].Attempts != 1 {
		t.Fatalf("failed row attempts = %d", box.rows[4].Attempts)
	}
}
