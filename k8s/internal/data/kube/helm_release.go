package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/apimachinery/pkg/api/meta"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/yaml"
)

// identityRT 把固定的用户身份放进每个请求的上下文，再交给用户身份的传输层（userRoundTripper）。
// Helm SDK 的多数集群调用不带调用方的 context，身份无法经 ctx 传入；加 Impersonate-* 头的仍然只有
// userRoundTripper 一处——没有身份它会拒绝发出，这里只负责把身份带到它面前。
type identityRT struct {
	id   biz.Identity
	next http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t identityRT) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(req.WithContext(context.WithValue(req.Context(), identityKey{}, t.id)))
}

// helmGetter Helm 用的 RESTClientGetter：全部客户端都建在「以该用户身份」的传输层上，
// Helm 的每个动作（读写 Release 的 Secret、创建资源、执行钩子）都由集群按调用者的数据范围判定。
type helmGetter struct {
	cfg *rest.Config
	ns  string

	once  sync.Once
	disco discovery.CachedDiscoveryInterface
	err   error
}

func (u *userClients) helmGetter(namespace string) *helmGetter {
	return &helmGetter{ns: namespace, cfg: &rest.Config{
		Host: u.c.host, Transport: identityRT{id: u.id, next: u.c.userRT},
		QPS: u.c.m.opts.QPS, Burst: u.c.m.opts.Burst, UserAgent: userAgent,
	}}
}

// ToRESTConfig implements genericclioptions.RESTClientGetter.
func (g *helmGetter) ToRESTConfig() (*rest.Config, error) { return rest.CopyConfig(g.cfg), nil }

// ToDiscoveryClient implements genericclioptions.RESTClientGetter（内存缓存，随请求结束释放）。
func (g *helmGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	g.once.Do(func() {
		dc, err := discovery.NewDiscoveryClientForConfig(g.cfg)
		if err != nil {
			g.err = err
			return
		}
		g.disco = memory.NewMemCacheClient(dc)
	})
	return g.disco, g.err
}

// ToRESTMapper implements genericclioptions.RESTClientGetter.
func (g *helmGetter) ToRESTMapper() (meta.RESTMapper, error) {
	dc, err := g.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}
	return restmapper.NewShortcutExpander(restmapper.NewDeferredDiscoveryRESTMapper(dc), dc, nil), nil
}

// ToRawKubeConfigLoader implements genericclioptions.RESTClientGetter：只提供命名空间。
func (g *helmGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig { return nsConfig{g} }

type nsConfig struct{ g *helmGetter }

func (c nsConfig) RawConfig() (clientcmdapi.Config, error) { return clientcmdapi.Config{}, nil }
func (c nsConfig) ClientConfig() (*rest.Config, error)     { return c.g.ToRESTConfig() }
func (c nsConfig) Namespace() (string, bool, error)        { return c.g.ns, c.g.ns != "", nil }
func (c nsConfig) ConfigAccess() clientcmd.ConfigAccess    { return clientcmd.NewDefaultPathOptions() }

// helmConfig 每次请求新建一个 Helm 动作配置（不按集群缓存：它持有发现缓存与 REST 映射，内存不小）。
func (u *userClients) helmConfig(namespace string) (*action.Configuration, error) {
	cfg := new(action.Configuration)
	if err := cfg.Init(u.helmGetter(namespace), namespace, "secret", func(string, ...any) {}); err != nil {
		return nil, helmError(err)
	}
	return cfg, nil
}

// helmError 把 Helm 的错误归一为业务错误：集群返回的状态错误按通用规则映射，其余（渲染失败、
// 名称占用、校验不过）一律 422 并保留原文。
func helmError(err error) error {
	if err == nil {
		return nil
	}
	var be *biz.Error
	if errors.As(err, &be) {
		return be
	}
	if errors.Is(err, driver.ErrReleaseNotFound) || errors.Is(err, driver.ErrNoDeployedReleases) {
		return biz.ErrNotFound("helm release not found")
	}
	msg := truncateText(err.Error(), 2000)
	mapped := mapError(err)
	if me, ok := mapped.(*biz.Error); ok && me.Reason != k8scontract.ReasonUpstream { //nolint:errorlint // mapError 恒返回 *biz.Error
		// 保留 Helm 包装的上下文（哪一步、哪个资源），状态码与原因取集群的。
		return biz.NewError(me.Code, me.Reason, "%s", msg)
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "forbidden"):
		return biz.ErrForbidden("%s", msg)
	case strings.Contains(lower, "still in use") || strings.Contains(lower, "operation in progress") || strings.Contains(lower, "already exists"):
		return biz.NewError(http.StatusConflict, k8scontract.ReasonConflict, "%s", msg)
	}
	return biz.NewError(http.StatusUnprocessableEntity, k8scontract.ReasonRejected, "%s", msg)
}

// secretsForbidden Release 存放在 Secret 里：读不了 Secret 就看不了 Release，给出明确的说明。
func secretsForbidden(namespace string) error {
	where := "this namespace"
	if namespace == "" {
		where = "any namespace"
	}
	return biz.ErrForbidden("Helm releases are stored in Secrets; your access level cannot read Secrets in %s (the read-only level does not include Secrets)", where)
}

func toRelease(r *release.Release) k8scontract.HelmRelease {
	out := k8scontract.HelmRelease{Namespace: r.Namespace, Name: r.Name, Revision: r.Version}
	if r.Info != nil {
		out.Status, out.Description, out.UpdateTime = r.Info.Status.String(), r.Info.Description, r.Info.LastDeployed.Time
	}
	if r.Chart != nil && r.Chart.Metadata != nil {
		out.Chart, out.ChartVersion, out.AppVersion = r.Chart.Metadata.Name, r.Chart.Metadata.Version, r.Chart.Metadata.AppVersion
	}
	return out
}

func checkReleaseRef(namespace, name string) error {
	if !nsPattern.MatchString(namespace) {
		return biz.ErrInvalid("invalid namespace")
	}
	if !k8scontract.ValidHelmReleaseName(name) {
		return biz.ErrInvalid("invalid release name: lowercase letters, digits, '-' and '.', at most 53 characters")
	}
	return nil
}

// helmList Release 列表。先由集群判定该用户能读哪些命名空间的 Secret，再以用户身份读取。
func (u *userClients) helmList(req k8scontract.HelmListRequest) (k8scontract.HelmListResponse, error) {
	out := k8scontract.HelmListResponse{Items: []k8scontract.HelmRelease{}}
	if req.Namespace != "" && !nsPattern.MatchString(req.Namespace) {
		return out, biz.ErrInvalid("invalid namespace")
	}
	namespaces, all, err := u.c.readableNamespaces(u.ctx, u.id, k8scontract.GVRSecret, true, req.Namespace)
	if err != nil {
		var be *biz.Error
		if errors.As(err, &be) && be.Reason == k8scontract.ReasonForbidden {
			return out, secretsForbidden(req.Namespace)
		}
		return out, err
	}
	targets := namespaces
	if all {
		targets = []string{req.Namespace}
	} else {
		out.PartialNamespaces = namespaces
	}
	kw := strings.ToLower(strings.TrimSpace(req.Keyword))
	for _, ns := range targets {
		cfg, err := u.helmConfig(ns)
		if err != nil {
			return out, err
		}
		l := action.NewList(cfg)
		l.All, l.AllNamespaces = true, ns == ""
		l.SetStateMask()
		rels, err := l.Run()
		if err != nil {
			return out, helmError(err)
		}
		for _, r := range rels {
			if kw == "" || strings.Contains(strings.ToLower(r.Name), kw) {
				out.Items = append(out.Items, toRelease(r))
			}
		}
	}
	sort.Slice(out.Items, func(i, j int) bool {
		if out.Items[i].Namespace != out.Items[j].Namespace {
			return out.Items[i].Namespace < out.Items[j].Namespace
		}
		return out.Items[i].Name < out.Items[j].Name
	})
	return out, nil
}

func valuesYAML(v map[string]any) string {
	if len(v) == 0 {
		return ""
	}
	raw, err := yaml.Marshal(v)
	if err != nil {
		return ""
	}
	return string(raw)
}

// manifestResources 列出清单里的资源；类型解析不了（CRD 尚未安装等）的只给 Kind 与名称。
func (u *userClients) manifestResources(manifest, namespace string) []k8scontract.HelmResource {
	out := []k8scontract.HelmResource{}
	dec := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	for len(out) < 500 {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			break // 清单不合法：返回已解析的部分
		}
		kind, _ := obj["kind"].(string)
		apiVersion, _ := obj["apiVersion"].(string)
		md, _ := obj["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		if kind == "" || name == "" {
			continue
		}
		res := k8scontract.HelmResource{Kind: kind, Ref: k8scontract.Ref{Name: name}}
		if r, err := u.c.resolveKind(u.ctx, apiVersion, kind); err == nil {
			res.Ref.Group, res.Ref.Version, res.Ref.Resource = r.Group, r.Version, r.Resource
			if r.Namespaced {
				if res.Ref.Namespace, _ = md["namespace"].(string); res.Ref.Namespace == "" {
					res.Ref.Namespace = namespace
				}
			}
		}
		out = append(out, res)
	}
	return out
}

// helmGet Release 详情。
func (u *userClients) helmGet(req k8scontract.HelmReleaseRequest) (k8scontract.HelmReleaseDetail, error) {
	var out k8scontract.HelmReleaseDetail
	if err := checkReleaseRef(req.Namespace, req.Name); err != nil {
		return out, err
	}
	cfg, err := u.helmConfig(req.Namespace)
	if err != nil {
		return out, err
	}
	rel, err := action.NewGet(cfg).Run(req.Name)
	if err != nil {
		return out, helmError(err)
	}
	// 给人看的 values 与清单一律掩码：Release 里的口令与渲染出的 Secret 内容不经这里明文外泄
	// （看 Secret 明文要走单独授权并记审计的「查看明文」）。升级时掩码表示「保持现值」，见 RestoreMasked。
	out.Release, out.Values, out.Manifest = toRelease(rel), valuesYAML(biz.MaskValues(rel.Config)), maskManifestSecrets(rel.Manifest)
	if rel.Info != nil {
		out.Notes = rel.Info.Notes
	}
	out.Resources = u.manifestResources(rel.Manifest, rel.Namespace)
	h := action.NewHistory(cfg)
	h.Max = 30
	hist, err := h.Run(req.Name)
	if err != nil {
		return out, helmError(err)
	}
	out.History = make([]k8scontract.HelmRelease, 0, len(hist))
	for _, r := range hist {
		out.History = append(out.History, toRelease(r))
	}
	sort.Slice(out.History, func(i, j int) bool { return out.History[i].Revision > out.History[j].Revision })
	return out, nil
}

// helmInput 一次安装 / 升级的输入。
type helmInput struct {
	chart  *chart.Chart
	repo   *HelmRepo
	digest string // Chart 包的 SHA-256（十六进制）
	// vals 实际使用的 values（掩码已换回现值）；typed 用户提交的 values 的掩码副本（回显 / 审计 / 展示用）。
	vals  map[string]any
	typed map[string]any
	// current 升级时的当前 Release；安装为 nil。
	current *release.Release
}

// helmInputs 校验安装 / 升级请求并准备 Chart 与 values。values 里等于掩码的标量表示「保持现值」：
// 升级时换回当前 Release 同一路径上的值，没有对应的现值（含安装）则拒绝——掩码绝不作为真实的值使用。
func (u *userClients) helmInputs(req k8scontract.HelmApplyRequest, upgrade bool) (*helmInput, error) {
	if err := checkReleaseRef(req.Namespace, req.Name); err != nil {
		return nil, err
	}
	if len(req.Values) > k8scontract.MaxHelmValuesBytes {
		return nil, biz.ErrInvalid("values exceed %d KiB", k8scontract.MaxHelmValuesBytes>>10)
	}
	vals := map[string]any{}
	if strings.TrimSpace(req.Values) != "" {
		if err := yaml.Unmarshal([]byte(req.Values), &vals); err != nil {
			return nil, biz.ErrInvalid("values is not a valid YAML mapping: %s", truncateText(err.Error(), 300))
		}
	}
	in := &helmInput{typed: biz.MaskValues(vals)}
	var curVals map[string]any
	if upgrade {
		cfg, err := u.helmConfig(req.Namespace)
		if err != nil {
			return nil, err
		}
		cur, err := action.NewGet(cfg).Run(req.Name)
		if err != nil {
			return nil, helmError(err)
		}
		in.current, curVals = cur, cur.Config
		if curVals == nil {
			curVals = map[string]any{}
		}
	}
	if err := biz.RestoreMasked(vals, curVals); err != nil {
		return nil, err
	}
	in.vals = vals
	var err error
	if in.chart, in.repo, in.digest, err = u.c.m.loadChart(u.ctx, req.RepoID, req.Chart, req.Version); err != nil {
		return nil, err
	}
	ch := in.chart
	if ch.Metadata.Type != "" && ch.Metadata.Type != "application" {
		return nil, biz.ErrInvalid("chart %s is a %s chart and cannot be installed", ch.Metadata.Name, ch.Metadata.Type)
	}
	if err := action.CheckDependencies(ch, ch.Metadata.Dependencies); err != nil {
		return nil, biz.ErrInvalid("chart dependencies are not bundled in the package: %s", truncateText(err.Error(), 300))
	}
	return in, nil
}

// helmRender 服务端 dry-run 渲染：不写 Release，不创建资源。返回渲染出的（未掩码的）Release。
func (u *userClients) helmRender(req k8scontract.HelmApplyRequest, in *helmInput) (*release.Release, error) {
	cfg, err := u.helmConfig(req.Namespace)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(u.ctx, time.Minute)
	defer cancel()
	var rel *release.Release
	if in.current != nil {
		up := action.NewUpgrade(cfg)
		up.Namespace, up.DryRun, up.DryRunOption, up.Timeout = req.Namespace, true, "server", time.Minute
		rel, err = up.RunWithContext(ctx, req.Name, in.chart, in.vals)
	} else {
		ins := action.NewInstall(cfg)
		ins.ReleaseName, ins.Namespace, ins.DryRun, ins.DryRunOption, ins.Timeout = req.Name, req.Namespace, true, "server", time.Minute
		rel, err = ins.RunWithContext(ctx, in.chart, in.vals)
	}
	if err != nil {
		return nil, helmError(err)
	}
	return rel, nil
}

// helmPreview 预检（服务端 dry-run）：不写 Release，不创建资源。清单里的 Secret 内容与已保存的 values 掩码返回；
// 用户本次提交的 values 按提交的样子回显（其中的掩码保持为掩码）。
func (u *userClients) helmPreview(req k8scontract.HelmApplyRequest) (k8scontract.HelmPreviewResponse, error) {
	out := k8scontract.HelmPreviewResponse{Upgrade: req.Upgrade}
	in, err := u.helmInputs(req, req.Upgrade)
	if err != nil {
		return out, err
	}
	rel, err := u.helmRender(req, in)
	if err != nil {
		return out, err
	}
	if cur := in.current; cur != nil {
		cr := toRelease(cur)
		out.CurrentManifest, out.CurrentValues, out.CurrentChart = maskManifestSecrets(cur.Manifest), valuesYAML(biz.MaskValues(cur.Config)), cr.Chart+"-"+cr.ChartVersion
	}
	out.Manifest, out.Values = maskManifestSecrets(rel.Manifest), valuesYAML(in.typed)
	if rel.Info != nil {
		out.Notes = rel.Info.Notes
	}
	return out, nil
}

type helmResult struct {
	resp k8scontract.HelmApplyResponse
	err  error
}

func releaseResult(rel *release.Release, err error) helmResult {
	r := helmResult{err: helmError(err)}
	if rel != nil {
		r.resp.Revision = rel.Version
		if rel.Info != nil {
			r.resp.Status, r.resp.Notes = rel.Info.Status.String(), rel.Info.Notes
		}
	}
	return r
}

// helmOps 在途的 Helm 写操作。它们脱离发起的请求运行，所以要有地方记着：退出时等它们收尾，
// 集群不再归本实例负责时取消它们，并限制每个集群同时进行的个数。
type helmOps struct {
	ctx    context.Context // 进程级：Close 等待超时后取消
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	seq     int
	running map[string]map[int]context.CancelFunc // 集群 ID → 在途操作
}

// maxHelmOpsPerCluster 每个集群同时进行的 Helm 写操作上限。
const maxHelmOpsPerCluster = 4

// helmShutdownWait 进程退出时等待在途 Helm 操作收尾的时长；超过则取消（操作记为失败并落审计）。
const helmShutdownWait = 20 * time.Second

func newHelmOps() *helmOps {
	ctx, cancel := context.WithCancel(context.Background())
	return &helmOps{ctx: ctx, cancel: cancel, running: map[string]map[int]context.CancelFunc{}}
}

func (h *helmOps) begin(clusterID string, timeout time.Duration) (context.Context, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil {
		return nil, nil, biz.ErrClusterOffline("service is shutting down")
	}
	if len(h.running[clusterID]) >= maxHelmOpsPerCluster {
		return nil, nil, biz.NewError(http.StatusTooManyRequests, k8scontract.ReasonTooManyStreams, "too many helm operations are in progress on this cluster, retry later")
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	h.seq++
	n := h.seq
	if h.running[clusterID] == nil {
		h.running[clusterID] = map[int]context.CancelFunc{}
	}
	h.running[clusterID][n] = cancel
	h.wg.Add(1)
	return ctx, func() {
		cancel()
		h.mu.Lock()
		delete(h.running[clusterID], n)
		if len(h.running[clusterID]) == 0 {
			delete(h.running, clusterID)
		}
		h.mu.Unlock()
		h.wg.Done()
	}, nil
}

// cancelCluster 取消某个集群上在途的操作（集群不再归本实例负责 / 被移除）。仅仅是连接配置重载不取消：
// 操作持有的是旧连接的客户端，可以跑完。
func (h *helmOps) cancelCluster(clusterID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, cancel := range h.running[clusterID] {
		cancel()
	}
}

// shutdown 等待在途操作收尾，超时则全部取消。
func (h *helmOps) shutdown(wait time.Duration) {
	done := make(chan struct{})
	go func() { // 退出路径：全部操作结束（取消后 Helm 的调用按各自的超时返回）
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(wait):
	}
	h.cancel()
}

// helmAsync 执行一个 Helm 写操作：在脱离请求的 ctx 里运行（时限 HelmOpTimeout），请求最多等 HelmSyncWait
// （执行变更单时为 u.helmWait：等到真正的结果）。按时完成：审计随响应返回，由 Ops 落库；超时或请求中断：
// 返回 Pending，操作继续，结束时经 sink 补记审计——无论哪条路径，审计恰好一条。
func (u *userClients) helmAsync(audit *biz.AuditEvent, run func(ctx context.Context, ou *userClients) helmResult) (any, []*biz.AuditEvent, error) {
	wait := k8scontract.HelmSyncWait
	if u.helmWait > 0 {
		wait = u.helmWait
	}
	return u.helmAsyncWait(audit, wait, run)
}

func (u *userClients) helmAsyncWait(audit *biz.AuditEvent, wait time.Duration, run func(ctx context.Context, ou *userClients) helmResult) (any, []*biz.AuditEvent, error) {
	octx, done, err := u.c.m.helm.begin(u.c.cluster.ID, k8scontract.HelmOpTimeout)
	if err != nil {
		return nil, nil, err
	}
	// 执行变更单：全部核对（Chart、渲染结果、版本号）都已通过，从这里起才真正改动集群。
	// 先拿到并发名额再记「已开始」：名额暂满或进程正在关停时集群没有被改动，这时不能留下 started
	// （否则变更单会被记为失败，而不是放回队列稍后重试，见 biz.ChangeRunner.execute）。
	if err := u.markStarted(); err != nil {
		done()
		return nil, nil, err
	}
	ou := &userClients{ctx: context.WithValue(octx, identityKey{}, u.id), id: u.id, dyn: u.dyn, kube: u.kube, c: u.c}
	var (
		mu   sync.Mutex
		res  *helmResult
		late bool
	)
	finished := make(chan struct{})
	sink, cluster, id := u.c.m.apps.sink, u.c.cluster, u.id
	go func() { // 退出路径：run 返回（受 octx 与 Helm 自身的 Timeout 约束）；helmOps.shutdown 等它
		defer done()
		var r helmResult
		func() {
			defer func() {
				if p := recover(); p != nil {
					r = helmResult{err: biz.NewError(http.StatusInternalServerError, k8scontract.ReasonUnspecified, "helm operation panicked: %v", p)}
				}
			}()
			r = run(octx, ou)
		}()
		if r.err == nil && octx.Err() != nil && r.resp.Revision == 0 {
			r.err = biz.ErrClusterOffline("helm operation was cancelled before it finished; check the release state")
		}
		audit.Err = r.err
		if p, ok := audit.Patch.(map[string]any); ok && p != nil && r.err == nil {
			p["revision"] = r.resp.Revision
		}
		mu.Lock()
		res = &r
		isLate := late
		mu.Unlock()
		close(finished)
		if isLate && sink != nil {
			sink(context.WithoutCancel(octx), cluster, id, []*biz.AuditEvent{audit})
		}
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
	case <-u.ctx.Done():
	}
	mu.Lock()
	defer mu.Unlock()
	if res != nil {
		return res.resp, []*biz.AuditEvent{audit}, res.err
	}
	late = true
	return k8scontract.HelmApplyResponse{Pending: true}, nil, nil
}

func helmAudit(action, namespace, name string, patch map[string]any) *biz.AuditEvent {
	return &biz.AuditEvent{
		Ref: k8scontract.Ref{
			Group: k8scontract.HelmReleaseGroup, Version: k8scontract.HelmReleaseVersion,
			Resource: k8scontract.HelmReleaseResource, Namespace: namespace, Name: name,
		},
		Kind: k8scontract.HelmReleaseKind, Action: action, Patch: patch,
	}
}

// helmApply 安装或升级。values 进审计前脱敏。
func (u *userClients) helmApply(req k8scontract.HelmApplyRequest, upgrade bool) (any, []*biz.AuditEvent, error) {
	in, err := u.helmInputs(req, upgrade)
	if err != nil {
		return nil, nil, err
	}
	// 变更单执行：Chart 与渲染结果须与审批时一致（见 planHelm）。
	if pin := u.helmPin; pin != nil {
		if err := u.checkHelmPin(req, in, pin); err != nil {
			return nil, nil, err
		}
	}
	ch, vals := in.chart, in.vals
	patch := map[string]any{"repo": in.repo.Name, "chart": ch.Metadata.Name, "version": ch.Metadata.Version, "values": biz.MaskValues(vals)}
	act := k8scontract.ActionHelmInstall
	if upgrade {
		act = k8scontract.ActionHelmUpgrade
		cr := toRelease(in.current)
		patch["previous"] = map[string]any{"chart": cr.Chart, "version": cr.ChartVersion, "revision": cr.Revision, "values": biz.MaskValues(in.current.Config)}
	}
	fitHelmAuditPatch(patch)
	return u.helmAsync(helmAudit(act, req.Namespace, req.Name, patch), func(ctx context.Context, ou *userClients) helmResult {
		cfg, cerr := ou.helmConfig(req.Namespace)
		if cerr != nil {
			return helmResult{err: cerr}
		}
		if upgrade {
			up := action.NewUpgrade(cfg)
			up.Namespace, up.Timeout, up.MaxHistory = req.Namespace, k8scontract.HelmOpTimeout, 10
			return releaseResult(up.RunWithContext(ctx, req.Name, ch, vals))
		}
		in := action.NewInstall(cfg)
		in.ReleaseName, in.Namespace, in.Timeout = req.Name, req.Namespace, k8scontract.HelmOpTimeout
		return releaseResult(in.RunWithContext(ctx, ch, vals))
	})
}

const (
	// helmAuditValuesBytes 审计 patch 超限时其中每份 values 保留的文本字节数。
	helmAuditValuesBytes = 64 << 10
	// helmAuditPatchSlack 截断后给之后写入的键（revision）留出的余量。
	helmAuditPatchSlack = 256
)

// fitHelmAuditPatch 安装 / 升级的审计 patch 超过审计单个字段的上限时，把其中的 values（本次提交的，以及升级时
// 上一个版本的；合计可达 1MiB）换成截断后的文本并标注 truncated。不处理的话整份 patch 会被丢弃
// （见 biz.Auditor.Record）：仓库、Chart、版本、上一个版本这些关键事实会一起丢失。
func fitHelmAuditPatch(patch map[string]any) {
	size := func() int {
		raw, err := json.Marshal(patch)
		if err != nil {
			return k8scontract.MaxAuditObjectBytes + 1
		}
		return len(raw)
	}
	if size() <= k8scontract.MaxAuditObjectBytes {
		return // 未超限：保持原样
	}
	patch["truncated"] = true
	prev, _ := patch["previous"].(map[string]any)
	for _, part := range []map[string]any{patch, prev} {
		if m, ok := part["values"].(map[string]any); ok {
			part["values"] = truncateText(valuesYAML(m), helmAuditValuesBytes)
		}
	}
	if size() <= k8scontract.MaxAuditObjectBytes-helmAuditPatchSlack {
		return
	}
	// 文本经 JSON 转义后仍超限（极端内容）：values 不再保留，只留下「被截断」与其余事实。
	patch["values"] = nil
	if prev != nil {
		prev["values"] = nil
	}
}

// manifestHash 渲染清单的摘要（变更单据此确认执行时渲染出的内容与审批时一致）。
func manifestHash(manifest string) string {
	sum := sha256.Sum256([]byte(manifest))
	return hex.EncodeToString(sum[:])
}

// checkHelmPin 执行已批准的 Helm 变更前：Chart 包的摘要、渲染出的清单须与审批时一致。仓库里同一版本号的
// Chart 被替换、仓库地址被改，都会在这里拦下——否则审批人看到的与实际执行的可以不是同一份内容。
func (u *userClients) checkHelmPin(req k8scontract.HelmApplyRequest, in *helmInput, pin *k8scontract.HelmCheck) error {
	if pin.ChartDigest != "" && !strings.EqualFold(pin.ChartDigest, in.digest) {
		return conflictError("chart %s %s in the repository is no longer the package that was approved (digest changed); resubmit the change", req.Chart, req.Version)
	}
	if pin.ManifestHash == "" {
		return nil
	}
	rel, err := u.helmRender(req, in)
	if err != nil {
		return err
	}
	if manifestHash(rel.Manifest) != pin.ManifestHash {
		return conflictError("the chart now renders differently from what was approved (cluster state or chart content changed); resubmit the change")
	}
	return nil
}

// helmRollback 回滚到指定版本。
func (u *userClients) helmRollback(req k8scontract.HelmRollbackRequest) (any, []*biz.AuditEvent, error) {
	if err := checkReleaseRef(req.Namespace, req.Name); err != nil {
		return nil, nil, err
	}
	if req.Revision < 1 {
		return nil, nil, biz.ErrInvalid("revision must be positive")
	}
	audit := helmAudit(k8scontract.ActionHelmRollback, req.Namespace, req.Name, map[string]any{"toRevision": req.Revision})
	return u.helmAsync(audit, func(_ context.Context, ou *userClients) helmResult {
		cfg, cerr := ou.helmConfig(req.Namespace)
		if cerr != nil {
			return helmResult{err: cerr}
		}
		rb := action.NewRollback(cfg)
		rb.Version, rb.Timeout, rb.MaxHistory = req.Revision, k8scontract.HelmOpTimeout, 10
		if err := rb.Run(req.Name); err != nil {
			return helmResult{err: helmError(err)}
		}
		return releaseResult(action.NewGet(cfg).Run(req.Name))
	})
}

// helmUninstall 卸载（不保留历史）。
func (u *userClients) helmUninstall(req k8scontract.HelmReleaseRequest) (any, []*biz.AuditEvent, error) {
	if err := checkReleaseRef(req.Namespace, req.Name); err != nil {
		return nil, nil, err
	}
	audit := helmAudit(k8scontract.ActionHelmUninstall, req.Namespace, req.Name, map[string]any{})
	return u.helmAsync(audit, func(_ context.Context, ou *userClients) helmResult {
		cfg, cerr := ou.helmConfig(req.Namespace)
		if cerr != nil {
			return helmResult{err: cerr}
		}
		un := action.NewUninstall(cfg)
		un.Timeout = k8scontract.HelmOpTimeout
		resp, err := un.Run(req.Name)
		if err != nil {
			return helmResult{err: helmError(err)}
		}
		r := helmResult{}
		if resp != nil && resp.Release != nil {
			r = releaseResult(resp.Release, nil)
			if p, ok := audit.Patch.(map[string]any); ok {
				cr := toRelease(resp.Release)
				p["chart"], p["version"] = cr.Chart, cr.ChartVersion
			}
		}
		return r
	})
}
