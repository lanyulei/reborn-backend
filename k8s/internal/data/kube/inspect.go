package kube

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ biz.Inspector = (*Manager)(nil)

const (
	inspectPageSize = 500
	// inspectMaxPages 单类资源至多读取的页数（500 × 300 = 15 万个对象），超出标记为不完整。
	inspectMaxPages = 300
	// inspectSecretPageSize TLS Secret 带证书与私钥，单页取小一些。
	inspectSecretPageSize = 100
)

// Inspect implements biz.Inspector。
//
// 开销：Pod、Deployment、StatefulSet、PVC、节点各一次分页遍历（每页 500），TLS Secret 一次（每页 100，
// 用字段选择器只取 type=kubernetes.io/tls，其余 Secret 不读）。全部以平台身份直接向集群读取、逐页处理后
// 丢弃：内存占用与一页的大小成正比，不建立也不依赖任何缓存。1200 节点 / 5 万 Pod 的集群约 110 次列表
// 请求，受该集群的客户端限速约束；由调用方串行执行、每个集群每天一次。
// 私钥从不解析、不保留：只解析 tls.crt 的第一张证书取到期时间。
func (m *Manager) Inspect(ctx context.Context, clusterID string) (biz.InspectionResult, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return biz.InspectionResult{}, err
	}
	set := biz.NewFindingSet()
	var sum k8scontract.InspectionSummary
	now := time.Now()
	core := c.platKube.CoreV1()
	apps := c.platKube.AppsV1()

	serverMinor := -1
	if v, verr := c.platDisco.ServerVersion(); verr == nil {
		serverMinor = minorOf(v.GitVersion)
	}

	err = paged(ctx, inspectPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		l, err := core.Nodes().List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			sum.Nodes++
			inspectNode(set, &l.Items[i], serverMinor)
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	err = paged(ctx, inspectPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		l, err := core.Pods(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			sum.Pods++
			inspectPod(set, &l.Items[i])
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	err = paged(ctx, inspectPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		l, err := apps.Deployments(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			sum.Workloads++
			d := &l.Items[i]
			inspectReplicas(set, "Deployment", d.Namespace, d.Name, d.Spec.Replicas)
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	err = paged(ctx, inspectPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		l, err := apps.StatefulSets(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			sum.Workloads++
			s := &l.Items[i]
			inspectReplicas(set, "StatefulSet", s.Namespace, s.Name, s.Spec.Replicas)
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	err = paged(ctx, inspectPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		l, err := core.PersistentVolumeClaims(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			inspectPVC(set, &l.Items[i], now)
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	err = paged(ctx, inspectSecretPageSize, &sum, func(o metav1.ListOptions) (string, error) {
		o.FieldSelector = "type=" + string(corev1.SecretTypeTLS)
		l, err := core.Secrets(metav1.NamespaceAll).List(ctx, o)
		if err != nil {
			return "", err
		}
		for i := range l.Items {
			sum.Certs++
			s := &l.Items[i]
			inspectTLS(set, s.Namespace, s.Name, s.Data[corev1.TLSCertKey], now)
			s.Data = nil // 尽早丢弃私钥
		}
		return l.Continue, nil
	})
	if err != nil {
		return biz.InspectionResult{}, mapError(err)
	}
	return set.Result(sum), nil
}

// paged 分页遍历；页数超过上限时把摘要标记为不完整并停止。
func paged(ctx context.Context, size int64, sum *k8scontract.InspectionSummary, page func(metav1.ListOptions) (string, error)) error {
	opts := metav1.ListOptions{Limit: size}
	for i := 0; i < inspectMaxPages; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cont, err := page(opts)
		if err != nil {
			return err
		}
		if cont == "" {
			return nil
		}
		opts.Continue = cont
	}
	sum.Partial = true
	return nil
}

var minorPattern = regexp.MustCompile(`^v?\d+\.(\d+)`)

// minorOf 取版本号里的次版本（v1.35.1 → 35）；无法解析返回 -1。
func minorOf(version string) int {
	m := minorPattern.FindStringSubmatch(version)
	if m == nil {
		return -1
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return -1
	}
	return n
}

// hashSuffix ReplicaSet 名称末尾的 Pod 模板哈希。
var hashSuffix = regexp.MustCompile(`-[a-z0-9]{6,10}$`)

// podOwner Pod 归并到的对象：ReplicaSet 归到其 Deployment（去掉模板哈希），其余取直接的控制器；
// 没有控制器的 Pod 就是它自己。同一工作负载的多个副本因此只报告一次。
func podOwner(p *corev1.Pod) (kind, name string) {
	for _, o := range p.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			continue
		}
		if o.Kind == "ReplicaSet" {
			if base := hashSuffix.ReplaceAllString(o.Name, ""); base != o.Name {
				return "Deployment", base
			}
		}
		return o.Kind, o.Name
	}
	return "Pod", p.Name
}

// imageUntagged 报告镜像是否使用 latest 或未指定标签（按摘要引用的不算）。
func imageUntagged(image string) bool {
	if strings.Contains(image, "@") {
		return false
	}
	last := image[strings.LastIndex(image, "/")+1:]
	_, tag, ok := strings.Cut(last, ":")
	return !ok || tag == "" || tag == "latest"
}

// inspectPod 检查一个 Pod：资源请求 / 限制、镜像标签、探针、重启次数。已结束的 Pod 不检查。
func inspectPod(set *biz.FindingSet, p *corev1.Pod) {
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return
	}
	kind, name := podOwner(p)
	batch := kind == "Job"
	for i := range p.Spec.Containers {
		c := &p.Spec.Containers[i]
		add := func(check string, sev int32, format string, args ...any) {
			set.Add(biz.Finding{
				Check: check, Severity: sev, Namespace: p.Namespace, Kind: kind, Name: name,
				Message: fmt.Sprintf("container %s: ", c.Name) + fmt.Sprintf(format, args...),
			})
		}
		req, lim := c.Resources.Requests, c.Resources.Limits
		_, cpuReq := req[corev1.ResourceCPU]
		_, memReq := req[corev1.ResourceMemory]
		if !cpuReq || !memReq {
			add(k8scontract.CheckNoRequests, k8scontract.FindingWarning, "CPU or memory request is not set")
		}
		if _, memLim := lim[corev1.ResourceMemory]; !memLim {
			add(k8scontract.CheckNoLimits, k8scontract.FindingInfo, "memory limit is not set")
		}
		if imageUntagged(c.Image) {
			add(k8scontract.CheckImageLatest, k8scontract.FindingWarning, "image %s uses latest or has no tag", c.Image)
		}
		if !batch && c.ReadinessProbe == nil && c.LivenessProbe == nil {
			add(k8scontract.CheckNoProbe, k8scontract.FindingInfo, "neither readiness nor liveness probe is set")
		}
	}
	for i := range p.Status.ContainerStatuses {
		st := &p.Status.ContainerStatuses[i]
		if st.RestartCount >= k8scontract.InspectRestartThreshold {
			set.Add(biz.Finding{
				Check: k8scontract.CheckPodRestarts, Severity: k8scontract.FindingWarning, Namespace: p.Namespace, Kind: "Pod", Name: p.Name,
				Message: fmt.Sprintf("container %s restarted %d times", st.Name, st.RestartCount),
			})
		}
	}
}

// inspectReplicas 检查副本数：只有一个副本的 Deployment / StatefulSet 在节点故障或发布时会中断服务。
func inspectReplicas(set *biz.FindingSet, kind, namespace, name string, replicas *int32) {
	if replicas != nil && *replicas == 1 {
		set.Add(biz.Finding{
			Check: k8scontract.CheckSingleReplica, Severity: k8scontract.FindingInfo, Namespace: namespace, Kind: kind, Name: name,
			Message: "only one replica",
		})
	}
}

// inspectPVC 检查长时间 Pending 的 PVC。
func inspectPVC(set *biz.FindingSet, pvc *corev1.PersistentVolumeClaim, now time.Time) {
	if pvc.Status.Phase != corev1.ClaimPending {
		return
	}
	if age := now.Sub(pvc.CreationTimestamp.Time); age >= k8scontract.InspectPVCPendingAge {
		set.Add(biz.Finding{
			Check: k8scontract.CheckPVCPending, Severity: k8scontract.FindingWarning, Namespace: pvc.Namespace, Kind: "PersistentVolumeClaim",
			Name: pvc.Name, Message: fmt.Sprintf("pending for %s", age.Round(time.Minute)),
		})
	}
}

// inspectNode 检查节点：NotReady、压力状况、kubelet 与控制面的版本偏差（落后 2 个及以上次版本，或比控制面新）。
func inspectNode(set *biz.FindingSet, n *corev1.Node, serverMinor int) {
	ready := false
	var problems []string
	for _, cond := range n.Status.Conditions {
		switch cond.Type {
		case corev1.NodeReady:
			ready = cond.Status == corev1.ConditionTrue
		case corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure:
			if cond.Status == corev1.ConditionTrue {
				problems = append(problems, string(cond.Type))
			}
		}
	}
	sev := k8scontract.FindingWarning
	if !ready {
		sev = k8scontract.FindingCritical
		problems = append([]string{"node is not ready"}, problems...)
	}
	if len(problems) > 0 {
		set.Add(biz.Finding{
			Check: k8scontract.CheckNodeUnhealthy, Severity: sev, Kind: "Node", Name: n.Name, Message: strings.Join(problems, ", "),
		})
	}
	if minor := minorOf(n.Status.NodeInfo.KubeletVersion); serverMinor >= 0 && minor >= 0 && (serverMinor-minor >= 2 || minor > serverMinor) {
		set.Add(biz.Finding{
			Check: k8scontract.CheckVersionSkew, Severity: k8scontract.FindingWarning, Kind: "Node", Name: n.Name,
			Message: fmt.Sprintf("kubelet %s vs control plane 1.%d", n.Status.NodeInfo.KubeletVersion, serverMinor),
		})
	}
}

// inspectTLS 检查 TLS Secret 的证书到期时间：已过期为严重，InspectCertDays 天内到期为警告。
// 只解析证书（tls.crt 的第一个 PEM 块）；无法解析的不报告。
func inspectTLS(set *biz.FindingSet, namespace, name string, crt []byte, now time.Time) {
	block, _ := pem.Decode(crt)
	if block == nil || block.Type != "CERTIFICATE" {
		return
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return
	}
	left := cert.NotAfter.Sub(now)
	switch {
	case left <= 0:
		set.Add(biz.Finding{
			Check: k8scontract.CheckCertExpiring, Severity: k8scontract.FindingCritical, Namespace: namespace, Kind: "Secret", Name: name,
			Message: fmt.Sprintf("certificate %s expired on %s", cert.Subject.CommonName, cert.NotAfter.Format(time.DateOnly)),
		})
	case left < k8scontract.InspectCertDays*24*time.Hour:
		set.Add(biz.Finding{
			Check: k8scontract.CheckCertExpiring, Severity: k8scontract.FindingWarning, Namespace: namespace, Kind: "Secret", Name: name,
			Message: fmt.Sprintf("certificate %s expires in %d days (%s)", cert.Subject.CommonName, int(left.Hours()/24), cert.NotAfter.Format(time.DateOnly)),
		})
	}
}
