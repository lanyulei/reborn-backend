package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/Masterminds/semver/v3"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/registry"
	"sigs.k8s.io/yaml"
)

// HelmRepo Helm 仓库（k8s_helm_repo 的只读视图）。Secret 是密文，只在本包解密。
type HelmRepo struct {
	ID       string
	Name     string
	Type     int32
	URL      string
	Username string
	Secret   []byte
	CACert   string
	Insecure bool
	Enabled  bool
	Version  int32
}

// HelmRepoReader 读取 Helm 仓库；不存在返回 404 业务错误。
type HelmRepoReader interface {
	HelmRepo(ctx context.Context, id string) (*HelmRepo, error)
}

// repoAccess 访问一个仓库所需的全部参数（凭据为明文，只驻留调用栈）。
type repoAccess struct {
	typ      int32
	url      string
	username string
	password string
	caCert   string
	insecure bool
}

const (
	helmIndexTTL     = 5 * time.Minute
	helmIndexEntries = 8 // 索引缓存的仓库数上限
	helmFetchTimeout = 2 * time.Minute
	maxHelmReadme    = 256 << 10
)

// errHelmRepo 仓库访问失败（地址不通、认证失败、内容不合法）：502，原文给用户。
func errHelmRepo(format string, args ...any) *biz.Error {
	return biz.NewError(http.StatusBadGateway, k8scontract.ReasonUpstream, "helm repository: "+format, args...)
}

// helmLoopbackEnv 置为 1 时允许 Helm 仓库指向回环地址（仅本地开发与端到端测试）。
const helmLoopbackEnv = "K8S_HELM_ALLOW_LOOPBACK"

var helmAllowLoopback = os.Getenv(helmLoopbackEnv) == "1"

// guardDial 拨号前拒绝回环地址、链路本地地址（含云元数据 169.254.169.254）与未指定地址。不拦私网：
// 仓库由持有仓库管理权限的人登记，天然指向内网（与监控数据源同一立场）。回环地址指向数据面自己所在的
// 主机（指标、pprof、旁路进程），仓库地址、索引里的下载地址与重定向都不该到达那里。对解析后的真实地址
// 判定，免疫 DNS 重绑定；重定向目标同样经过这里。
func guardDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	return guardIP(net.ParseIP(host), helmAllowLoopback)
}

func guardIP(ip net.IP, allowLoopback bool) error {
	if ip == nil || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() ||
		(ip.IsLoopback() && !allowLoopback) {
		return fmt.Errorf("address %s is not allowed for a helm repository", ip)
	}
	return nil
}

// checkRepoRedirect 重定向策略：至多 5 次；不允许从 https 降级到 http；主机一变就去掉认证头
// （net/http 对子域名与同主机降级仍会带上，凭据因此可能以明文发出）。
func checkRepoRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return errors.New("too many redirects")
	}
	first := via[0].URL
	if first.Scheme == "https" && req.URL.Scheme != "https" {
		return errors.New("redirect from https to http is not allowed")
	}
	if !strings.EqualFold(req.URL.Host, first.Host) {
		req.Header.Del("Authorization")
	}
	return nil
}

// repoHTTPClient 访问仓库的 HTTP 客户端：不走环境变量代理，限制重定向次数，TLS 按仓库配置。
func repoHTTPClient(a repoAccess) (*http.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: a.insecure} //nolint:gosec // 由仓库管理员显式开启
	if strings.TrimSpace(a.caCert) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(a.caCert)) {
			return nil, biz.ErrInvalid("helm repository CA certificate is not valid PEM")
		}
		tlsCfg.RootCAs = pool
	}
	d := &net.Dialer{Timeout: 10 * time.Second, Control: guardDial}
	return &http.Client{
		Timeout: helmFetchTimeout,
		Transport: &http.Transport{
			DialContext: d.DialContext, TLSClientConfig: tlsCfg, TLSHandshakeTimeout: 10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second, MaxIdleConns: 4, IdleConnTimeout: 30 * time.Second, ForceAttemptHTTP2: true,
		},
		CheckRedirect: checkRepoRedirect,
	}, nil
}

// fetch 下载一个地址，至多 limit 字节。凭据只发给与仓库同主机的地址（跨主机重定向时 net/http 会去掉认证头）。
func fetch(ctx context.Context, c *http.Client, a repoAccess, raw string, limit int64) ([]byte, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errHelmRepo("unsupported download address")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errHelmRepo("%s", err.Error())
	}
	req.Header.Set("User-Agent", userAgent)
	if base, perr := url.Parse(a.url); perr == nil && a.username != "" && strings.EqualFold(base.Host, u.Host) && (u.Scheme == "https" || base.Scheme == "http") {
		req.SetBasicAuth(a.username, a.password)
	}
	resp, err := c.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // 去掉 URL，避免把带签名的下载地址写进错误信息
		}
		return nil, errHelmRepo("request failed: %s", err.Error())
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, errHelmRepo("%s responded %s", u.Host, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, errHelmRepo("read failed: %s", err.Error())
	}
	if int64(len(data)) > limit {
		return nil, errHelmRepo("content exceeds the %d MiB limit", limit>>20)
	}
	return data, nil
}

// indexEntry index.yaml 里 Chart 的一个版本（只取用到的字段）。
type indexEntry struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	AppVersion  string   `json:"appVersion"`
	Description string   `json:"description"`
	Icon        string   `json:"icon"`
	Home        string   `json:"home"`
	Keywords    []string `json:"keywords"`
	URLs        []string `json:"urls"`
	Created     string   `json:"created"`
	Deprecated  bool     `json:"deprecated"`
	Digest      string   `json:"digest"`
}

type helmIndex struct {
	fetched time.Time
	entries map[string][]indexEntry // 每个 Chart 的版本，新的在前
}

// parseIndex 解析 index.yaml：丢弃名称 / 版本不合法的条目，版本按语义化版本降序。
func parseIndex(data []byte) (*helmIndex, error) {
	var f struct {
		Entries map[string][]indexEntry `json:"entries"`
	}
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, errHelmRepo("index.yaml is not valid: %s", truncateText(err.Error(), 200))
	}
	if f.Entries == nil {
		return nil, errHelmRepo("index.yaml has no entries (is this a Helm repository?)")
	}
	out := &helmIndex{fetched: time.Now(), entries: make(map[string][]indexEntry, len(f.Entries))}
	for name, versions := range f.Entries {
		if !k8scontract.ValidHelmChartName(name) {
			continue
		}
		kept := versions[:0]
		for _, v := range versions {
			if k8scontract.ValidHelmVersion(v.Version) {
				v.Name = name
				kept = append(kept, v)
			}
		}
		sortVersions(kept, func(e indexEntry) string { return e.Version })
		if len(kept) > 0 {
			out.entries[name] = kept
		}
	}
	return out, nil
}

// sortVersions 按语义化版本降序；不是合法语义化版本的排在后面（按字符串降序）。
func sortVersions[T any](items []T, ver func(T) string) {
	type keyed struct {
		v *semver.Version
		s string
	}
	keys := make(map[string]keyed, len(items))
	for _, it := range items {
		s := ver(it)
		v, _ := semver.NewVersion(s)
		keys[s] = keyed{v: v, s: s}
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := keys[ver(items[i])], keys[ver(items[j])]
		switch {
		case a.v != nil && b.v != nil:
			return a.v.GreaterThan(b.v)
		case a.v != nil:
			return true
		case b.v != nil:
			return false
		}
		return a.s > b.s
	})
}

func truncateText(s string, n int) string {
	if t := biz.Truncate(s, n); len(t) < len(s) {
		return t + "…"
	}
	return s
}

// appsState Manager 上「应用」相关的状态：仓库读取、事后审计、索引缓存。
type appsState struct {
	repos HelmRepoReader
	sink  biz.AuditSink

	mu    sync.Mutex
	index map[string]*helmIndex // 键 = 仓库 ID / 版本
	// inflight 正在下载的索引：同一个仓库的并发请求共用一次下载（一份索引可达几十 MiB，各下各的会叠加）。
	inflight map[string]*indexCall
	// fetchSem 同时进行的仓库下载（索引、Chart 包）数上限：把下载占用的内存压在「上限 × 单个大小上限」之内。
	fetchSem chan struct{}
}

type indexCall struct {
	done chan struct{}
	ix   *helmIndex
	err  error
}

// helmFetchParallel 同时进行的仓库下载数上限。
const helmFetchParallel = 4

// acquireFetch 占一个下载名额；ctx 结束返回错误。
func (s *appsState) acquireFetch(ctx context.Context) (func(), error) {
	select {
	case s.fetchSem <- struct{}{}:
		return func() { <-s.fetchSem }, nil
	case <-ctx.Done():
		return nil, errHelmRepo("too many downloads in progress, retry later")
	}
}

// EnableApps 注入 Helm 仓库读取（装配期调用一次，先于任何请求）。
func (m *Manager) EnableApps(repos HelmRepoReader) {
	m.apps = &appsState{repos: repos, index: map[string]*helmIndex{}, inflight: map[string]*indexCall{}, fetchSem: make(chan struct{}, helmFetchParallel)}
}

// SetAuditSink implements biz.AuditSinkSetter：异步完成的 Helm 写操作在结束时经它补记审计（装配期调用一次）。
func (m *Manager) SetAuditSink(sink biz.AuditSink) {
	if m.apps != nil {
		m.apps.sink = sink
	}
}

// repo 读取仓库并解出访问参数。requireEnabled：安装 / 升级 / 浏览目录要求仓库已启用。
func (m *Manager) repo(ctx context.Context, id string, requireEnabled bool) (*HelmRepo, repoAccess, error) {
	if m.apps == nil {
		return nil, repoAccess{}, biz.ErrUnsupported("helm is not enabled on this data plane")
	}
	if !identityIDPattern.MatchString(id) {
		return nil, repoAccess{}, biz.ErrInvalid("invalid helm repository id")
	}
	r, err := m.apps.repos.HelmRepo(ctx, id)
	if err != nil {
		return nil, repoAccess{}, err
	}
	if requireEnabled && !r.Enabled {
		return nil, repoAccess{}, biz.ErrInvalid("helm repository %s is disabled", r.Name)
	}
	a := repoAccess{typ: r.Type, username: r.Username, caCert: r.CACert, insecure: r.Insecure}
	if len(r.Secret) > 0 {
		plain, oerr := m.opener.Open(r.Secret)
		if oerr != nil {
			return nil, repoAccess{}, biz.NewError(http.StatusServiceUnavailable, k8scontract.ReasonUpstream, "cannot decrypt the helm repository credential (master key missing or changed)")
		}
		a.password = plain
	}
	if a.url, err = k8scontract.NormalizeHelmRepoURL(r.Type, r.URL, a.username != "" && a.password != ""); err != nil {
		return nil, repoAccess{}, biz.ErrInvalid("helm repository %s: %s", r.Name, err.Error())
	}
	return r, a, nil
}

func (s *appsState) cached(key string) *helmIndex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ix := s.index[key]; ix != nil && time.Since(ix.fetched) < helmIndexTTL {
		return ix
	}
	return nil
}

func (s *appsState) store(repoID, key string, ix *helmIndex) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.index { // 同一仓库的旧版本与过期项先清掉
		if strings.HasPrefix(k, repoID+"/") || time.Since(v.fetched) >= helmIndexTTL {
			delete(s.index, k)
		}
	}
	for len(s.index) >= helmIndexEntries { // 仍超限：淘汰最旧的
		oldest := ""
		for k, v := range s.index {
			if oldest == "" || v.fetched.Before(s.index[oldest].fetched) {
				oldest = k
			}
		}
		delete(s.index, oldest)
	}
	s.index[key] = ix
}

// httpIndex 取 HTTP 仓库的索引（5 分钟缓存；仓库配置变更即换键）。
func (m *Manager) httpIndex(ctx context.Context, r *HelmRepo, a repoAccess, refresh bool) (*helmIndex, error) {
	key := fmt.Sprintf("%s/%d", r.ID, r.Version)
	if !refresh {
		if ix := m.apps.cached(key); ix != nil {
			return ix, nil
		}
	}
	s := m.apps
	s.mu.Lock()
	call := s.inflight[key]
	if call == nil {
		call = &indexCall{done: make(chan struct{})}
		s.inflight[key] = call
		s.mu.Unlock()
		// 下载脱离发起它的那个请求：别的请求也在等这一次的结果，发起者断开不应让大家一起失败。
		go func() { // 退出路径：fetchIndex 受 helmFetchTimeout 约束
			defer close(call.done)
			defer func() {
				if p := recover(); p != nil {
					call.err = errHelmRepo("index download failed unexpectedly")
				}
				s.mu.Lock()
				delete(s.inflight, key)
				s.mu.Unlock()
			}()
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), helmFetchTimeout)
			defer cancel()
			release, err := s.acquireFetch(fctx)
			if err != nil {
				call.err = err
				return
			}
			defer release()
			if call.ix, call.err = fetchIndex(fctx, a); call.err == nil {
				s.store(r.ID, key, call.ix)
			}
		}()
	} else {
		s.mu.Unlock()
	}
	select {
	case <-call.done:
		return call.ix, call.err
	case <-ctx.Done():
		return nil, errHelmRepo("request cancelled while the index was being downloaded")
	}
}

func fetchIndex(ctx context.Context, a repoAccess) (*helmIndex, error) {
	c, err := repoHTTPClient(a)
	if err != nil {
		return nil, err
	}
	defer c.CloseIdleConnections()
	data, err := fetch(ctx, c, a, a.url+"/index.yaml", k8scontract.MaxHelmIndexBytes)
	if err != nil {
		return nil, err
	}
	return parseIndex(data)
}

// ociClient OCI 仓库客户端：凭据只来自仓库配置，不读本机的 docker / helm 凭据文件，不写缓存。
func ociClient(a repoAccess) (*registry.Client, func(), error) {
	hc, err := repoHTTPClient(a)
	if err != nil {
		return nil, nil, err
	}
	// 仓库客户端会把整层读进内存再交给调用方：在传输层给每个响应体设上限，超大的 Chart 层读到上限即失败，
	// 而不是先占满内存再被事后的大小检查拒绝。
	hc.Transport = bodyLimitTransport{next: hc.Transport, limit: k8scontract.MaxHelmChartBytes + 1<<20}
	opts := []registry.ClientOption{
		registry.ClientOptHTTPClient(hc), registry.ClientOptEnableCache(false), registry.ClientOptWriter(io.Discard),
		registry.ClientOptCredentialsFile(filepath.Join("/nonexistent", "reborn-helm-registry.json")),
	}
	if a.username != "" {
		opts = append(opts, registry.ClientOptBasicAuth(a.username, a.password))
	}
	rc, err := registry.NewClient(opts...)
	if err != nil {
		return nil, nil, errHelmRepo("%s", err.Error())
	}
	return rc, hc.CloseIdleConnections, nil
}

func ociRef(a repoAccess, chartName string) string {
	return strings.TrimPrefix(a.url, "oci://") + "/" + chartName
}

// ociTags OCI 仓库里某个 Chart 的标签（只留合法版本，新的在前）。
func ociTags(a repoAccess, chartName string) ([]string, error) {
	rc, done, err := ociClient(a)
	if err != nil {
		return nil, err
	}
	defer done()
	tags, err := rc.Tags(ociRef(a, chartName))
	if err != nil {
		return nil, errHelmRepo("list tags of %s: %s", chartName, truncateText(err.Error(), 300))
	}
	tags = slices.DeleteFunc(tags, func(t string) bool { return !k8scontract.ValidHelmVersion(t) })
	sortVersions(tags, func(s string) string { return s })
	return tags, nil
}

// charts Chart 目录。
func (m *Manager) charts(ctx context.Context, req k8scontract.HelmChartsRequest) (k8scontract.HelmChartsResponse, error) {
	out := k8scontract.HelmChartsResponse{Items: []k8scontract.HelmChartSummary{}, FetchTime: time.Now()}
	r, a, err := m.repo(ctx, req.RepoID, true)
	if err != nil {
		return out, err
	}
	if req.Chart != "" && !k8scontract.ValidHelmChartName(req.Chart) {
		return out, biz.ErrInvalid("invalid chart name")
	}
	if a.typ == k8scontract.HelmRepoOCI {
		if req.Chart == "" {
			return out, biz.ErrInvalid("an OCI repository has no catalogue: enter the chart name to list its versions")
		}
		tags, err := ociTags(a, req.Chart)
		if err != nil {
			return out, err
		}
		s := k8scontract.HelmChartSummary{Name: req.Chart, Versions: []k8scontract.HelmChartVersion{}}
		for _, t := range tags[:min(len(tags), k8scontract.MaxHelmVersions)] {
			s.Versions = append(s.Versions, k8scontract.HelmChartVersion{Version: t})
		}
		out.Items = append(out.Items, s)
		return out, nil
	}
	ix, err := m.httpIndex(ctx, r, a, req.Refresh)
	if err != nil {
		return out, err
	}
	out.FetchTime = ix.fetched
	kw := strings.ToLower(strings.TrimSpace(req.Keyword))
	names := make([]string, 0, len(ix.entries))
	for name, versions := range ix.entries {
		if req.Chart != "" && name != req.Chart {
			continue
		}
		if kw != "" && !chartMatches(name, versions[0], kw) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > k8scontract.MaxHelmCharts {
		names, out.Truncated = names[:k8scontract.MaxHelmCharts], true
	}
	for _, name := range names {
		versions := ix.entries[name]
		latest := versions[0]
		s := k8scontract.HelmChartSummary{
			Name: name, Description: latest.Description, Icon: latest.Icon, Home: latest.Home, Keywords: latest.Keywords,
			Versions: make([]k8scontract.HelmChartVersion, 0, min(len(versions), k8scontract.MaxHelmVersions)),
		}
		for _, v := range versions[:min(len(versions), k8scontract.MaxHelmVersions)] {
			cv := k8scontract.HelmChartVersion{Version: v.Version, AppVersion: v.AppVersion, Deprecated: v.Deprecated}
			if t, perr := time.Parse(time.RFC3339Nano, v.Created); perr == nil {
				cv.Created = &t
			}
			s.Versions = append(s.Versions, cv)
		}
		out.Items = append(out.Items, s)
	}
	return out, nil
}

func chartMatches(name string, e indexEntry, kw string) bool {
	if strings.Contains(strings.ToLower(name), kw) || strings.Contains(strings.ToLower(e.Description), kw) {
		return true
	}
	return slices.ContainsFunc(e.Keywords, func(k string) bool { return strings.Contains(strings.ToLower(k), kw) })
}

// chartURL 把索引里的下载地址解析为绝对地址（相对地址相对于仓库地址）。
func chartURL(repoURL, ref string) (string, error) {
	base, err := url.Parse(repoURL + "/")
	if err != nil {
		return "", err
	}
	u, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	abs := base.ResolveReference(u)
	if abs.Scheme != "http" && abs.Scheme != "https" {
		return "", errors.New("unsupported scheme")
	}
	return abs.String(), nil
}

// loadChart 下载并解析一个 Chart（大小受限；HTTP 仓库校验索引里的摘要）。
func (m *Manager) loadChart(ctx context.Context, repoID, chartName, version string) (*chart.Chart, *HelmRepo, string, error) {
	ch, r, data, err := m.loadChartData(ctx, repoID, chartName, version)
	if err != nil {
		return nil, nil, "", err
	}
	sum := sha256.Sum256(data)
	return ch, r, hex.EncodeToString(sum[:]), nil
}

func (m *Manager) loadChartData(ctx context.Context, repoID, chartName, version string) (*chart.Chart, *HelmRepo, []byte, error) {
	if !k8scontract.ValidHelmChartName(chartName) || !k8scontract.ValidHelmVersion(version) {
		return nil, nil, nil, biz.ErrInvalid("invalid chart name or version")
	}
	r, a, err := m.repo(ctx, repoID, true)
	if err != nil {
		return nil, nil, nil, err
	}
	var data []byte
	release, err := m.apps.acquireFetch(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	defer release()
	if a.typ == k8scontract.HelmRepoOCI {
		rc, done, cerr := ociClient(a)
		if cerr != nil {
			return nil, nil, nil, cerr
		}
		defer done()
		res, perr := rc.Pull(ociRef(a, chartName)+":"+version, registry.PullOptWithChart(true))
		if perr != nil || res.Chart == nil {
			msg := "chart layer missing"
			if perr != nil {
				msg = truncateText(perr.Error(), 300)
			}
			return nil, nil, nil, errHelmRepo("pull %s:%s: %s", chartName, version, msg)
		}
		data = res.Chart.Data
	} else {
		ix, ierr := m.httpIndex(ctx, r, a, false)
		if ierr != nil {
			return nil, nil, nil, ierr
		}
		i := slices.IndexFunc(ix.entries[chartName], func(e indexEntry) bool { return e.Version == version })
		if i < 0 || len(ix.entries[chartName][i].URLs) == 0 {
			return nil, nil, nil, biz.ErrNotFound("chart %s version %s is not in repository %s", chartName, version, r.Name)
		}
		e := ix.entries[chartName][i]
		abs, uerr := chartURL(a.url, e.URLs[0])
		if uerr != nil {
			return nil, nil, nil, errHelmRepo("chart %s has an unsupported download address", chartName)
		}
		c, cerr := repoHTTPClient(a)
		if cerr != nil {
			return nil, nil, nil, cerr
		}
		defer c.CloseIdleConnections()
		if data, err = fetch(ctx, c, a, abs, k8scontract.MaxHelmChartBytes); err != nil {
			return nil, nil, nil, err
		}
		if d := strings.TrimPrefix(e.Digest, "sha256:"); d != "" {
			if sum := sha256.Sum256(data); !strings.EqualFold(hex.EncodeToString(sum[:]), d) {
				return nil, nil, nil, errHelmRepo("chart %s:%s does not match the digest in the index", chartName, version)
			}
		}
	}
	if len(data) > k8scontract.MaxHelmChartBytes {
		return nil, nil, nil, errHelmRepo("chart exceeds the %d MiB limit", k8scontract.MaxHelmChartBytes>>20)
	}
	ch, err := loader.LoadArchive(bytes.NewReader(data))
	if err != nil {
		return nil, nil, nil, errHelmRepo("chart %s:%s cannot be loaded: %s", chartName, version, truncateText(err.Error(), 300))
	}
	if ch.Metadata == nil {
		return nil, nil, nil, errHelmRepo("chart %s:%s has no Chart.yaml", chartName, version)
	}
	return ch, r, data, nil
}

// chartDetail Chart 详情：缺省 values、schema、README。
func (m *Manager) chartDetail(ctx context.Context, req k8scontract.HelmChartRequest) (k8scontract.HelmChartResponse, error) {
	ch, _, _, err := m.loadChart(ctx, req.RepoID, req.Chart, req.Version)
	if err != nil {
		return k8scontract.HelmChartResponse{}, err
	}
	out := k8scontract.HelmChartResponse{
		Name: ch.Metadata.Name, Version: ch.Metadata.Version, AppVersion: ch.Metadata.AppVersion,
		Description: ch.Metadata.Description, Type: ch.Metadata.Type, Schema: string(ch.Schema),
	}
	for _, f := range ch.Raw {
		if f.Name == "values.yaml" {
			out.Values = string(f.Data)
		}
	}
	for _, f := range ch.Files {
		if strings.EqualFold(f.Name, "README.md") {
			out.Readme = truncateText(string(f.Data), maxHelmReadme)
		}
	}
	return out, nil
}

// testRepo 测试仓库配置：HTTP 仓库抓一次索引；OCI 仓库给了 Chart 名则列一次标签，否则只校验地址。
func (m *Manager) testRepo(ctx context.Context, req k8scontract.HelmRepoTestRequest) k8scontract.HelmRepoTestResponse {
	start := time.Now()
	out := k8scontract.HelmRepoTestResponse{}
	fail := func(err error) k8scontract.HelmRepoTestResponse {
		out.Error, out.DurationMS = biz.AuditErrorText(err), time.Since(start).Milliseconds()
		return out
	}
	a := repoAccess{typ: req.Type, username: req.Username, password: req.Secret, caCert: req.CACert, insecure: req.InsecureSkipVerify}
	if req.Secret == "" && req.RepoID != "" { // 沿用已保存的凭据：仅当地址主机未变（防止把凭据引到别的主机）
		saved, sa, err := m.repo(ctx, req.RepoID, false)
		if err != nil {
			return fail(err)
		}
		su, _ := url.Parse(saved.URL)
		nu, perr := url.Parse(strings.TrimSpace(req.URL))
		if perr != nil || su == nil || !strings.EqualFold(su.Host, nu.Host) {
			return fail(biz.ErrInvalid("the address host changed: re-enter the credential to test"))
		}
		a.password = sa.password
	}
	var err error
	if a.url, err = k8scontract.NormalizeHelmRepoURL(req.Type, req.URL, a.username != "" && a.password != ""); err != nil {
		return fail(biz.ErrInvalid("%s", err.Error()))
	}
	if a.typ == k8scontract.HelmRepoOCI {
		if req.Chart != "" {
			if !k8scontract.ValidHelmChartName(req.Chart) {
				return fail(biz.ErrInvalid("invalid chart name"))
			}
			tags, terr := ociTags(a, req.Chart)
			if terr != nil {
				return fail(terr)
			}
			out.Charts = len(tags)
		}
	} else {
		ix, ierr := fetchIndex(ctx, a)
		if ierr != nil {
			return fail(ierr)
		}
		out.Charts = len(ix.entries)
	}
	out.OK, out.DurationMS = true, time.Since(start).Milliseconds()
	return out
}

// bodyLimitTransport 给每个响应体加字节上限（超过即读取失败）。
type bodyLimitTransport struct {
	next  http.RoundTripper
	limit int64
}

// RoundTrip implements http.RoundTripper.
func (t bodyLimitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(r)
	if err != nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &cappedBody{rc: resp.Body, left: t.limit}
	return resp, nil
}
