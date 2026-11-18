package biz

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// KubeProxier 把一个集群 API 请求以用户身份原样转给集群（data/kube 实现）：支持协议升级与长响应，
// 身份头只由传输层按 id 设置，入站的 Impersonate-* / Authorization 一律丢弃。
type KubeProxier interface {
	KubeHandler(c *Cluster, id Identity) (http.Handler, error)
}

// KubeAction 集群代理对一个请求的处置。
type KubeAction struct {
	Target k8scontract.KubeRequest
	// Audit 非空 = 需要审计，取值为审计动作。
	Audit string
}

// KubePolicy 判定经集群代理（限时 kubeconfig）的请求是否放行，以及是否需要审计。纯函数。
//
//   - 路径必须在允许的根之下且已规范化；
//   - 一律拒绝 portforward 与 proxy 子资源（它们把集群内网暴露给客户端，平台无法审计其内容）；
//   - 集群禁止终端时拒绝 exec / attach；
//   - 写方法（POST / PUT / PATCH / DELETE）与 exec / attach 记审计；权限询问类资源与 dry-run 不记。
func KubePolicy(method, path, rawQuery string, execDisabled bool) (KubeAction, error) {
	var act KubeAction
	if !k8scontract.KubePathAllowed(path) {
		return act, NewError(http.StatusForbidden, k8scontract.ReasonForbidden, "path %q is not accessible through the cluster proxy", path)
	}
	t := k8scontract.ParseKubePath(path)
	act.Target = t
	sub := t.Subresource
	first, _, _ := strings.Cut(sub, "/")
	switch first {
	case "portforward", "proxy":
		return act, NewError(http.StatusForbidden, k8scontract.ReasonForbidden, "%s is not allowed through the cluster proxy", first)
	case "exec", "attach":
		if execDisabled {
			return act, errExecDisabled
		}
		act.Audit = k8scontract.ActionExec
		return act, nil
	}
	if t.Resource == "proxy" { // 形如 /api/v1/proxy/... 的旧式代理路径
		return act, NewError(http.StatusForbidden, k8scontract.ReasonForbidden, "proxy is not allowed through the cluster proxy")
	}
	if !t.Resourceful {
		return act, nil
	}
	switch method {
	case http.MethodPost:
		act.Audit = k8scontract.ActionCreate
	case http.MethodPut, http.MethodPatch:
		act.Audit = k8scontract.ActionUpdate
	case http.MethodDelete:
		act.Audit = k8scontract.ActionDelete
	default:
		return act, nil
	}
	if t.Group == "authorization.k8s.io" || t.Group == "authentication.k8s.io" || isDryRun(rawQuery) {
		act.Audit = ""
	}
	return act, nil
}

// KubeWrite 报告该处置是否属于「变更」（创建 / 更新 / 删除）：需审批的集群上由 core 拒绝。
func (a KubeAction) KubeWrite() bool {
	return a.Audit == k8scontract.ActionCreate || a.Audit == k8scontract.ActionUpdate || a.Audit == k8scontract.ActionDelete
}

// KubeGateway 集群代理的入口：归属与在线判定 → 策略 → 以用户身份转发 → 审计。
type KubeGateway struct {
	registry *Registry
	prober   *Prober
	scopes   *ScopeSyncer
	proxier  KubeProxier
	auditor  *Auditor

	mu     sync.Mutex
	active map[string]int // 每用户在途请求数（含 watch / exec 等长连接）
}

// maxKubeProxyPerUser 每个用户经集群代理的并发请求上限。代理请求不经过客户端限速器，长连接（watch、exec）
// 也没有时长上限：不设并发上限，一个令牌就能占满数据面到集群的连接与文件描述符。
const maxKubeProxyPerUser = 64

// NewKubeGateway new a KubeGateway.
func NewKubeGateway(registry *Registry, prober *Prober, scopes *ScopeSyncer, proxier KubeProxier, auditor *Auditor) *KubeGateway {
	return &KubeGateway{registry: registry, prober: prober, scopes: scopes, proxier: proxier, auditor: auditor, active: map[string]int{}}
}

// gate 取本实例负责、在线且已完成授权同步的集群。
func gate(registry *Registry, prober *Prober, scopes *ScopeSyncer, clusterID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, errInvalid("missing cluster id")
	}
	c, err := registry.Owned(clusterID)
	if err != nil {
		return nil, err
	}
	if !prober.Online(c.ID) {
		return nil, ErrClusterOffline("cluster %s is unreachable or has not passed the access self-check", c.Name)
	}
	if !scopes.Ready(c) {
		return nil, ErrClusterOffline("cluster %s: authorisation sync pending (initialising, or the last sync failed — see the cluster's scope error), retry in a few seconds", c.Name)
	}
	return c, nil
}

// statusWriter 记录响应状态码；Unwrap 让 http.ResponseController 仍能 Flush / Hijack（协议升级）。
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(p)
}

// Unwrap implements the http.ResponseController contract.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Serve 处理一个集群 API 请求。r.URL.Path 须已是集群 API 的路径；tokenID 为发起请求的限时 kubeconfig
// 令牌（写入审计）。策略拒绝与集群不可用以 error 返回（调用方写出错误体）；其余由代理直接写出响应。
func (g *KubeGateway) Serve(w http.ResponseWriter, r *http.Request, clusterID string, id Identity, tokenID string) error {
	c, err := gate(g.registry, g.prober, g.scopes, clusterID)
	if err != nil {
		return err
	}
	act, err := KubePolicy(r.Method, r.URL.Path, r.URL.RawQuery, c.ExecDisabled)
	if err != nil {
		return err
	}
	// 临时容器只能经平台的「调试容器」功能添加（镜像受允许清单约束）：经代理直接写这个子资源可以指定
	// 任意镜像与安全上下文，绕过清单。超管不受此限。
	if first, _, _ := strings.Cut(act.Target.Subresource, "/"); first == "ephemeralcontainers" && act.Audit != "" && !id.IsAdmin {
		return NewError(http.StatusForbidden, k8scontract.ReasonForbidden,
			"ephemeral containers cannot be changed through the cluster proxy; use the debug container feature of the platform")
	}
	if !g.acquire(id.UserID) {
		return errTooManyStreams
	}
	defer g.release(id.UserID)
	h, err := g.proxier.KubeHandler(c, id)
	if err != nil {
		return err
	}
	sw := &statusWriter{ResponseWriter: w}
	h.ServeHTTP(sw, r)
	if act.Audit != "" {
		ev := &AuditEvent{
			Ref: k8scontract.Ref{
				Group: act.Target.Group, Version: act.Target.Version, Resource: act.Target.Resource,
				Namespace: act.Target.Namespace, Name: act.Target.Name,
			},
			Action: act.Audit,
			Patch: map[string]any{
				"via": k8scontract.AuditViaKubeconfig, "method": r.Method, "subresource": act.Target.Subresource, "tokenId": tokenID,
			},
		}
		if sw.code >= 400 {
			ev.Err = NewError(sw.code, k8scontract.ReasonUpstream, "request rejected with HTTP %d", sw.code)
		}
		g.auditor.Record(context.WithoutCancel(r.Context()), c, id, []*AuditEvent{ev})
	}
	return nil
}

// isDryRun 报告请求是否为纯试运行：存在 dryRun 参数且每个取值都是 All。按参数名精确解析——
// 子串匹配会把 ?nodryRun=1 这样的真实写入也当成试运行而漏掉审计。
func isDryRun(rawQuery string) bool {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	vals := q["dryRun"]
	if len(vals) == 0 {
		return false
	}
	for _, v := range vals {
		if v != "All" {
			return false
		}
	}
	return true
}

func (g *KubeGateway) acquire(userID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active[userID] >= maxKubeProxyPerUser {
		return false
	}
	g.active[userID]++
	return true
}

func (g *KubeGateway) release(userID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active[userID]--; g.active[userID] <= 0 {
		delete(g.active, userID)
	}
}
