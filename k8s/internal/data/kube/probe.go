package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Probe implements biz.ClusterProbe（平台身份）。节点数优先取自缓存，未缓存时向集群列一次
// （resourceVersion=0：由 API Server 的缓存应答，不打到 etcd）。
func (m *Manager) Probe(ctx context.Context, clusterID string) (biz.ProbeInfo, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return biz.ProbeInfo{}, err
	}
	return c.probe(ctx)
}

func (c *conn) probe(ctx context.Context) (biz.ProbeInfo, error) {
	var info biz.ProbeInfo
	raw, err := c.platKube.Discovery().RESTClient().Get().AbsPath("/version").DoRaw(ctx)
	if err != nil {
		return info, probeError(err)
	}
	var v struct {
		GitVersion string `json:"gitVersion"`
	}
	if err := json.Unmarshal(raw, &v); err != nil || v.GitVersion == "" {
		return info, fmt.Errorf("cluster returned an unexpected /version response")
	}
	info.Version = v.GitVersion
	if store := c.cache.peek(k8scontract.GVRNode); store != nil {
		for _, e := range store.snapshot(nil) {
			info.NodeTotal++
			if e.Row.State == sOK || e.Row.State == sPending { // Ready（含已停止调度）
				info.NodeReady++
			}
		}
		return info, nil
	}
	// 节点缓存未建立：/version 已证明集群可达，节点数只做低频刷新（分页读取）。每 30 秒全量列出
	// 上千个节点的完整对象，对大集群是不必要的读压力。
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()
	if !c.nodesAt.IsZero() && time.Since(c.nodesAt) < nodeCountInterval {
		info.NodeTotal, info.NodeReady = c.nodesTotal, c.nodesReady
		return info, nil
	}
	total, ready := 0, 0
	opts := metav1.ListOptions{Limit: 500}
	for {
		nodes, err := c.platKube.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			return info, probeError(err)
		}
		for i := range nodes.Items {
			total++
			for _, cond := range nodes.Items[i].Status.Conditions {
				if cond.Type == "Ready" && cond.Status == "True" {
					ready++
				}
			}
		}
		if opts.Continue = nodes.Continue; opts.Continue == "" {
			break
		}
	}
	c.nodesTotal, c.nodesReady, c.nodesAt = total, ready, time.Now()
	info.NodeTotal, info.NodeReady = total, ready
	return info, nil
}

// nodeCountInterval 节点缓存未建立时，探活刷新节点数的间隔。
const nodeCountInterval = 5 * time.Minute

// probeError 探活失败的原因：给人看的原文（不含凭据：client-go 的错误不带请求头）。
func probeError(err error) error {
	return fmt.Errorf("%s", biz.AuditErrorText(mapError(err)))
}

// serverMinor 解析版本里的次版本号（v1.35.1 → 35；带后缀如 v1.32.4-eks 也可）。
func serverMinor(gitVersion string) (major, minor int, ok bool) {
	parts := strings.SplitN(strings.TrimPrefix(gitVersion, "v"), ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err1 := strconv.Atoi(parts[0])
	digits := parts[1]
	for i, r := range digits {
		if r < '0' || r > '9' {
			digits = digits[:i]
			break
		}
	}
	minor, err2 := strconv.Atoi(digits)
	return major, minor, err1 == nil && err2 == nil
}

// SelfCheck implements biz.ClusterProbe.
func (m *Manager) SelfCheck(ctx context.Context, clusterID string) ([]k8scontract.SelfCheck, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return nil, err
	}
	info, err := c.probe(ctx)
	if err != nil {
		return nil, err
	}
	return c.selfCheck(ctx, info.Version), nil
}

type accessCheck struct {
	name, verb, group, resource string
}

// selfChecks 权限方案（Impersonation + 授权同步 + 权限询问）要求接入凭据具备的能力。
var selfChecks = []accessCheck{
	{"impersonate users", "impersonate", "", "users"},
	{"impersonate groups", "impersonate", "", "groups"},
	{"create clusterroles", "create", "rbac.authorization.k8s.io", "clusterroles"},
	{"delete clusterroles", "delete", "rbac.authorization.k8s.io", "clusterroles"},
	{"create clusterrolebindings", "create", "rbac.authorization.k8s.io", "clusterrolebindings"},
	{"delete clusterrolebindings", "delete", "rbac.authorization.k8s.io", "clusterrolebindings"},
	{"create rolebindings", "create", "rbac.authorization.k8s.io", "rolebindings"},
	{"delete rolebindings", "delete", "rbac.authorization.k8s.io", "rolebindings"},
	// 绑定 cluster-admin 需要自身已具备其全部权限或有 bind 权限：以「对全部资源的全部动作」近似。
	{"full access (bind cluster-admin)", "*", "*", "*"},
	{"create subjectaccessreviews", "create", "authorization.k8s.io", "subjectaccessreviews"},
}

// selfCheckParallel 自检请求的并发度：十几项串行时，链路延迟偏高（跨地域直连、经代理、API Server 本身响应慢）
// 的集群一次自检就把 10 秒的探活预算耗尽，可达的集群被判为不可达且每轮重复。
const selfCheckParallel = 5

// selfCheck 接入自检：只做询问（SelfSubjectAccessReview）与读取，不在集群里创建任何对象。
// 各项互不依赖，有界并发发出；任一项出错（含超时取消）照常记为未通过——并发只缩短用时，不改变判定。
func (c *conn) selfCheck(ctx context.Context, version string) []k8scontract.SelfCheck {
	out := make([]k8scontract.SelfCheck, 0, len(selfChecks)+4)
	vc := k8scontract.SelfCheck{Name: fmt.Sprintf("kubernetes version >= 1.%d", k8scontract.MinServerMinor), OK: true}
	if major, minor, ok := serverMinor(version); !ok {
		vc.OK, vc.Message = false, "cannot parse server version "+version
	} else if major < 1 || (major == 1 && minor < k8scontract.MinServerMinor) {
		vc.OK, vc.Message = false, "server version is "+version
	}
	out = append(out, vc)
	checks := make([]func() k8scontract.SelfCheck, 0, len(selfChecks)+3)
	for _, ck := range selfChecks {
		checks = append(checks, func() k8scontract.SelfCheck {
			item := k8scontract.SelfCheck{Name: ck.name}
			review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authv1.ResourceAttributes{Verb: ck.verb, Group: ck.group, Resource: ck.resource},
			}}
			res, err := c.platKube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
			switch {
			case err != nil:
				item.Message = biz.AuditErrorText(mapError(err))
			case !res.Status.Allowed:
				item.Message = "the access credential is not allowed to " + ck.name
			default:
				item.OK = true
			}
			return item
		})
	}
	for _, role := range []string{"view", "edit", "admin"} {
		checks = append(checks, func() k8scontract.SelfCheck {
			item := k8scontract.SelfCheck{Name: "built-in clusterrole " + role, OK: true}
			if _, err := c.platKube.RbacV1().ClusterRoles().Get(ctx, role, metav1.GetOptions{}); err != nil {
				item.OK, item.Message = false, biz.AuditErrorText(mapError(err))
			}
			return item
		})
	}
	results := make([]k8scontract.SelfCheck, len(checks))
	sem := make(chan struct{}, selfCheckParallel)
	var wg sync.WaitGroup
	for i, run := range checks {
		wg.Add(1)
		sem <- struct{}{}
		go func() { // 退出路径：请求受 ctx 约束，返回即退出；wg.Wait 在本函数返回前收齐
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = run()
		}()
	}
	wg.Wait()
	return append(out, results...)
}

// testTimeout 测试连接的总时长上限。
const testTimeout = 15 * time.Second

// Test implements biz.ClusterTester：按未保存的直连配置实际连接一次并执行接入自检。
// 用完即弃：不登记连接、不建缓存。
func (m *Manager) Test(ctx context.Context, req k8scontract.ClusterTestRequest) k8scontract.ClusterTestResponse {
	start := time.Now()
	out := k8scontract.ClusterTestResponse{}
	fail := func(msg string) k8scontract.ClusterTestResponse {
		out.Error, out.DurationMS = msg, time.Since(start).Milliseconds()
		return out
	}
	cred := req.Credential
	if cred == (k8scontract.Credential{}) && req.ClusterID != "" {
		sealed, saved, err := m.creds.Credential(ctx, req.ClusterID)
		if err != nil {
			m.log.Warn("read saved credential failed", "error", err)
			return fail("cannot read the saved credential")
		}
		if len(sealed) == 0 {
			return fail("no saved credential for this cluster")
		}
		if !sameEndpoint(saved, req) {
			return fail("the API server address, proxy or CA differs from the saved cluster: re-enter the credential to test a changed address")
		}
		plain, err := m.opener.Open(sealed)
		if err != nil {
			return fail("cannot decrypt the saved credential: " + err.Error())
		}
		if json.Unmarshal([]byte(plain), &cred) != nil {
			return fail("the saved credential is malformed")
		}
	}
	c, err := m.build(&biz.Cluster{Name: "test"}, &directSpec{apiServer: req.APIServer, proxyURL: req.ProxyURL, caCert: req.CACert, cred: cred})
	if err != nil {
		return fail(err.Error())
	}
	defer c.close()
	tctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()
	info, err := c.probe(tctx)
	if err != nil {
		return fail(err.Error())
	}
	out.Version = info.Version
	out.Checks = c.selfCheck(tctx, info.Version)
	out.OK = biz.FailedChecks(out.Checks) == ""
	if !out.OK {
		out.Error = "access self-check failed"
	}
	out.DurationMS = time.Since(start).Milliseconds()
	return out
}

// sameEndpoint 报告测试请求的连接地址是否与已保存的完全一致（已保存的凭据只发往已保存的地址）。
func sameEndpoint(saved SavedEndpoint, req k8scontract.ClusterTestRequest) bool {
	return strings.TrimRight(saved.APIServer, "/") == strings.TrimRight(req.APIServer, "/") &&
		saved.ProxyURL == req.ProxyURL && strings.TrimSpace(saved.CACert) == strings.TrimSpace(req.CACert)
}
