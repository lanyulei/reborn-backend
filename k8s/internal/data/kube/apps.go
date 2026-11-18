package kube

import (
	"context"
	"regexp"
	"sort"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ biz.GlobalExecutor = (*Manager)(nil)

// ExecuteGlobal implements biz.GlobalExecutor：与集群无关的 Helm 仓库操作。身份只用于校验
// （请求必须来自一个合法的平台用户）；仓库访问用仓库自己的凭据。
func (m *Manager) ExecuteGlobal(ctx context.Context, id biz.Identity, op string, body []byte) (any, error) {
	if err := validIdentity(id); err != nil {
		return nil, err
	}
	if m.apps == nil {
		return nil, biz.ErrUnsupported("helm is not enabled on this data plane")
	}
	switch op {
	case k8scontract.OpHelmRepoTest:
		req, err := decode[k8scontract.HelmRepoTestRequest](body)
		if err != nil {
			return nil, err
		}
		return m.testRepo(ctx, req), nil
	case k8scontract.OpHelmCharts:
		req, err := decode[k8scontract.HelmChartsRequest](body)
		if err != nil {
			return nil, err
		}
		return m.charts(ctx, req)
	case k8scontract.OpHelmChart:
		req, err := decode[k8scontract.HelmChartRequest](body)
		if err != nil {
			return nil, err
		}
		return m.chartDetail(ctx, req)
	}
	return nil, biz.ErrUnsupported("unknown operation %s", op)
}

// isAppsOp 报告该操作是否由 dispatchApps 处理。
func isAppsOp(op string) bool {
	return op == k8scontract.OpNamespaceQuota || (k8scontract.IsHelmOp(op) && !k8scontract.HelmClusterless(op))
}

// dispatchApps Helm Release 与命名空间配额（全部以用户身份访问集群）。
func (u *userClients) dispatchApps(op string, body []byte) (any, []*biz.AuditEvent, error) {
	if u.c.m.apps == nil {
		return nil, nil, biz.ErrUnsupported("helm is not enabled on this data plane")
	}
	switch op {
	case k8scontract.OpHelmList:
		req, err := decode[k8scontract.HelmListRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.helmList(req)
		return r, nil, err
	case k8scontract.OpHelmGet:
		req, err := decode[k8scontract.HelmReleaseRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.helmGet(req)
		return r, nil, err
	case k8scontract.OpHelmPreview:
		req, err := decode[k8scontract.HelmApplyRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.helmPreview(req)
		return r, nil, err
	case k8scontract.OpHelmInstall, k8scontract.OpHelmUpgrade:
		req, err := decode[k8scontract.HelmApplyRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.helmApply(req, op == k8scontract.OpHelmUpgrade)
	case k8scontract.OpHelmRollback:
		req, err := decode[k8scontract.HelmRollbackRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.helmRollback(req)
	case k8scontract.OpHelmUninstall:
		req, err := decode[k8scontract.HelmReleaseRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.helmUninstall(req)
	case k8scontract.OpNamespaceQuota:
		req, err := decode[k8scontract.NamespaceQuotaRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.namespaceQuota(req)
		return r, nil, err
	}
	return nil, nil, biz.ErrUnsupported("unknown operation %s", op)
}

// namespaceQuota 命名空间的配额用量：以用户身份读取 ResourceQuota。请求全部命名空间而只有部分
// 权限时，只返回有权限的命名空间。
func (u *userClients) namespaceQuota(req k8scontract.NamespaceQuotaRequest) (k8scontract.NamespaceQuotaResponse, error) {
	out := k8scontract.NamespaceQuotaResponse{Items: []k8scontract.NamespaceQuota{}}
	if req.Namespace != "" && !nsPattern.MatchString(req.Namespace) {
		return out, biz.ErrInvalid("invalid namespace")
	}
	namespaces, all, err := u.c.readableNamespaces(u.ctx, u.id, k8scontract.GVRQuota, true, req.Namespace)
	if err != nil {
		return out, err
	}
	if all {
		namespaces = []string{req.Namespace}
	}
	var quotas []corev1.ResourceQuota
	for _, ns := range namespaces {
		list, lerr := u.kube.CoreV1().ResourceQuotas(ns).List(u.ctx, metav1.ListOptions{})
		if lerr != nil {
			return out, mapError(lerr)
		}
		quotas = append(quotas, list.Items...)
	}
	out.Items = summarizeQuotas(quotas)
	return out, nil
}

// summarizeQuotas 按命名空间汇总：同一资源出现在多个 ResourceQuota 里时取使用率最高的那个（最先被触达的上限）。
func summarizeQuotas(quotas []corev1.ResourceQuota) []k8scontract.NamespaceQuota {
	type agg struct {
		names []string
		items map[string]k8scontract.QuotaUsage
	}
	byNS := map[string]*agg{}
	for i := range quotas {
		q := &quotas[i]
		a := byNS[q.Namespace]
		if a == nil {
			a = &agg{items: map[string]k8scontract.QuotaUsage{}}
			byNS[q.Namespace] = a
		}
		a.names = append(a.names, q.Name)
		for name, hard := range q.Status.Hard {
			used := q.Status.Used[name]
			u := k8scontract.QuotaUsage{Resource: string(name), Used: used.String(), Hard: hard.String(), Ratio: quotaRatio(used, hard)}
			if cur, ok := a.items[u.Resource]; !ok || u.Ratio > cur.Ratio {
				a.items[u.Resource] = u
			}
		}
	}
	out := make([]k8scontract.NamespaceQuota, 0, len(byNS))
	for ns, a := range byNS {
		nq := k8scontract.NamespaceQuota{Namespace: ns, Quotas: a.names, Items: make([]k8scontract.QuotaUsage, 0, len(a.items))}
		sort.Strings(nq.Quotas)
		for _, it := range a.items {
			nq.Items = append(nq.Items, it)
		}
		sort.Slice(nq.Items, func(i, j int) bool { return nq.Items[i].Resource < nq.Items[j].Resource })
		out = append(out, nq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	return out
}

func quotaRatio(used, hard resource.Quantity) float64 {
	h := hard.AsApproximateFloat64()
	if h <= 0 {
		return 0
	}
	r := used.AsApproximateFloat64() / h
	if r < 0 {
		return 0
	}
	return r
}

// nsPattern 命名空间名（DNS 标签）。
var nsPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
