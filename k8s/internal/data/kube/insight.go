package kube

import (
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// dispatchInsight 可观测部分的只读操作（契约见 kit/k8scontract/insight.go）。handled = false 表示不是这里的操作。
func (u *userClients) dispatchInsight(op string, body []byte) (resp any, handled bool, err error) {
	switch op {
	case k8scontract.OpSelector, k8scontract.OpRelations, k8scontract.OpDiagnosePod, k8scontract.OpDiagnoseWorkload:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, true, err
		}
		if err := checkRef(req.Ref, true); err != nil {
			return nil, true, err
		}
		switch op {
		case k8scontract.OpSelector:
			resp, err = u.selector(req.Ref)
		case k8scontract.OpRelations:
			resp, err = u.relations(req.Ref)
		case k8scontract.OpDiagnosePod:
			resp, err = u.diagnosePod(req.Ref)
		default:
			resp, err = u.diagnoseWorkload(req.Ref)
		}
		return resp, true, err
	case k8scontract.OpLocateNode:
		req, err := decode[k8scontract.LocateNodeRequest](body)
		if err != nil {
			return nil, true, err
		}
		resp, err = u.locateNode(req)
		return resp, true, err
	}
	return nil, false, nil
}

// podSelector 取出对象选中 Pod 的选择器：工作负载 / Job 的 spec.selector（LabelSelector，含 matchExpressions），
// Service 的 spec.selector（标签映射）。没有选择器返回 nil。
func podSelector(gvr k8scontract.GVR, obj map[string]any) (labels.Selector, error) {
	if gvr == k8scontract.GVRService {
		set := stringMap(obj, "spec", "selector")
		if len(set) == 0 {
			return nil, nil
		}
		return labels.SelectorFromSet(set), nil
	}
	raw := nestedMap(obj, "spec", "selector")
	if raw == nil {
		return nil, nil
	}
	var ls metav1.LabelSelector
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &ls); err != nil {
		return nil, biz.ErrInvalid("invalid pod selector")
	}
	if len(ls.MatchLabels) == 0 && len(ls.MatchExpressions) == 0 {
		return nil, nil
	}
	sel, err := metav1.LabelSelectorAsSelector(&ls)
	if err != nil {
		return nil, biz.ErrInvalid("invalid pod selector")
	}
	return sel, nil
}

// getObject 以用户身份读取对象。
func (u *userClients) getObject(ref k8scontract.Ref) (*unstructured.Unstructured, k8scontract.APIResource, error) {
	rc, res, err := u.resource(ref)
	if err != nil {
		return nil, res, err
	}
	obj, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	return obj, res, err
}

func (u *userClients) selector(ref k8scontract.Ref) (k8scontract.SelectorResponse, error) {
	out := k8scontract.SelectorResponse{Namespace: ref.Namespace, Empty: true}
	obj, _, err := u.getObject(ref)
	if err != nil {
		return out, err
	}
	sel, err := podSelector(ref.GVR(), obj.Object)
	if err != nil || sel == nil {
		return out, err
	}
	out.LabelSelector, out.Empty = sel.String(), false
	return out, nil
}

// ---- 关联链路 ----

func podReady(e *biz.Entry) bool {
	if e.Row.State != sOK {
		return false
	}
	ready, _ := e.Row.Cells["ready"].(string)
	a, b, ok := strings.Cut(ready, "/")
	return ok && a == b && a != "0"
}

// selectedPods 命名空间里被选择器选中的 Pod（来自 Pod 缓存；先由集群确认用户可列出该命名空间的 Pod）。
// allowed = false 表示用户无权查看 Pod。
func (u *userClients) selectedPods(namespace string, sel labels.Selector) (pods []*biz.Entry, allowed bool, err error) {
	ok, err := u.c.allowed(u.ctx, u.id, namespace, k8scontract.GVRPod, "list")
	if err != nil || !ok {
		return nil, false, err
	}
	e, err := u.c.cache.get(u.ctx, k8scontract.GVRPod)
	if err != nil {
		return nil, true, err
	}
	for _, en := range e.store.snapshot([]string{namespace}) {
		if sel.Matches(labels.Set(en.Labels)) {
			pods = append(pods, en)
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Row.Name < pods[j].Row.Name })
	return pods, true, nil
}

// serviceChain Service → Pod 一段的健康状况。svc 为 nil 表示 Service 不存在（或用户无权查看）。
func (u *userClients) serviceChain(namespace, name string, svc map[string]any) (k8scontract.RelationService, error) {
	out := k8scontract.RelationService{Name: name, Pods: []k8scontract.RelationPod{}}
	if svc == nil {
		out.State, out.Problem = sAbnormal, k8scontract.ProblemNotFound
		return out, nil
	}
	out.Type = str(svc, "spec", "type")
	sel, err := podSelector(k8scontract.GVRService, svc)
	if err != nil {
		return out, err
	}
	if sel == nil {
		// 没有选择器的 Service（ExternalName、手工维护 Endpoints）：后端不由 Pod 决定，无法判断。
		out.State, out.Problem = sUnknown, k8scontract.ProblemNoSelector
		return out, nil
	}
	out.Selector = sel.String()
	pods, allowed, err := u.selectedPods(namespace, sel)
	if err != nil {
		return out, err
	}
	if !allowed {
		out.State = sUnknown
		return out, nil
	}
	out.TotalPods = len(pods)
	for _, p := range pods {
		ready := podReady(p)
		if ready {
			out.ReadyPods++
		}
		if len(out.Pods) < k8scontract.MaxRelationPods {
			rp := k8scontract.RelationPod{Name: p.Row.Name, Ready: ready, Node: p.Node}
			if c, ok := p.Row.Cells["status"].(state); ok {
				rp.Status = c.Text
			}
			rp.IP, _ = p.Row.Cells["ip"].(string)
			out.Pods = append(out.Pods, rp)
		}
	}
	out.PodsOmitted = out.TotalPods - len(out.Pods)
	switch {
	case out.TotalPods == 0:
		out.State, out.Problem = sAbnormal, k8scontract.ProblemNoPods
	case out.ReadyPods == 0:
		out.State, out.Problem = sAbnormal, k8scontract.ProblemNotReady
	case out.ReadyPods < out.TotalPods:
		out.State = sPending
	default:
		out.State = sOK
	}
	return out, nil
}

// getService 读取 Service；不存在或无权查看返回 nil。
func (u *userClients) getService(namespace, name string) (map[string]any, error) {
	obj, err := u.dyn.Resource(toSchema(k8scontract.GVRService)).Namespace(namespace).Get(u.ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return obj.Object, nil
}

// servicePort Ingress 后端里的端口（名称或数字）及 Service 是否有这个端口。
func servicePort(backend, svc map[string]any) (port string, found bool) {
	name := str(backend, "port", "name")
	number, hasNumber := num(backend, "port", "number")
	if name != "" {
		port = name
	} else if hasNumber {
		port = strconv.FormatInt(number, 10)
	}
	if svc == nil || port == "" {
		return port, true
	}
	for _, p := range nestedList(svc, "spec", "ports") {
		m := asMap(p)
		if name != "" && str(m, "name") == name {
			return port, true
		}
		if n, ok := num(m, "port"); name == "" && ok && n == number {
			return port, true
		}
	}
	return port, false
}

func (u *userClients) ingressRules(ing map[string]any, namespace string) ([]k8scontract.RelationRule, error) {
	type target struct {
		host, path string
		backend    map[string]any
	}
	var targets []target
	if b := nestedMap(ing, "spec", "defaultBackend", "service"); b != nil {
		targets = append(targets, target{"*", "(default)", b})
	}
	for _, r := range nestedList(ing, "spec", "rules") {
		rule := asMap(r)
		host := str(rule, "host")
		if host == "" {
			host = "*"
		}
		for _, p := range nestedList(rule, "http", "paths") {
			pm := asMap(p)
			if b := nestedMap(pm, "backend", "service"); b != nil {
				path := str(pm, "path")
				if path == "" {
					path = "/"
				}
				targets = append(targets, target{host, path, b})
			}
		}
	}
	type cached struct {
		svc   map[string]any
		chain k8scontract.RelationService
	}
	services := map[string]*cached{}
	out := make([]k8scontract.RelationRule, 0, len(targets))
	for _, t := range targets {
		name := str(t.backend, "name")
		c := services[name]
		if c == nil {
			svc, err := u.getService(namespace, name)
			if err != nil {
				return nil, err
			}
			chain, err := u.serviceChain(namespace, name, svc)
			if err != nil {
				return nil, err
			}
			c = &cached{svc: svc, chain: chain}
			services[name] = c
		}
		chain := c.chain
		port, found := servicePort(t.backend, c.svc)
		chain.Port = port
		if !found && chain.Problem == "" {
			chain.State, chain.Problem = sAbnormal, k8scontract.ProblemPortMismatch
		}
		out = append(out, k8scontract.RelationRule{Host: t.host, Path: t.path, Service: chain})
	}
	return out, nil
}

// ---- 引用关系 ----

// podSpecRefs 一个 Pod 模板对某个 ConfigMap / Secret 的引用方式；没有引用返回 nil。
func podSpecRefs(spec map[string]any, secret bool, name string) []string {
	keyRef, envFromRef, volKey, volName, projKey := "configMapKeyRef", "configMapRef", "configMap", "name", "configMap"
	if secret {
		keyRef, envFromRef, volKey, volName, projKey = "secretKeyRef", "secretRef", "secret", "secretName", "secret"
	}
	via := map[string]struct{}{}
	for _, field := range []string{"containers", "initContainers", "ephemeralContainers"} {
		for _, c := range nestedList(spec, field) {
			m := asMap(c)
			for _, e := range nestedList(m, "env") {
				if str(asMap(e), "valueFrom", keyRef, "name") == name {
					via["env"] = struct{}{}
				}
			}
			for _, e := range nestedList(m, "envFrom") {
				if str(asMap(e), envFromRef, "name") == name {
					via["envFrom"] = struct{}{}
				}
			}
		}
	}
	for _, v := range nestedList(spec, "volumes") {
		m := asMap(v)
		if str(m, volKey, volName) == name {
			via["volume"] = struct{}{}
		}
		for _, src := range nestedList(m, "projected", "sources") {
			if str(asMap(src), projKey, "name") == name {
				via["projected"] = struct{}{}
			}
		}
	}
	if secret {
		for _, s := range nestedList(spec, "imagePullSecrets") {
			if str(asMap(s), "name") == name {
				via["imagePullSecret"] = struct{}{}
			}
		}
	}
	if len(via) == 0 {
		return nil
	}
	out := make([]string, 0, len(via))
	for k := range via {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// referenceLimit 引用关系扫描时每类工作负载至多读取的对象数。
const referenceLimit = 500

// references 命名空间里引用了某个 ConfigMap / Secret 的工作负载（以用户身份列出各类工作负载并扫描 Pod 模板；
// 用户无权查看的类型跳过）。
func (u *userClients) references(ref k8scontract.Ref) ([]k8scontract.Reference, bool, error) {
	secret := ref.GVR() == k8scontract.GVRSecret
	kinds := []struct {
		gvr         k8scontract.GVR
		kind        string
		path        []string
		restartable bool
	}{
		{k8scontract.GVRDeployment, "Deployment", []string{"spec", "template", "spec"}, true},
		{k8scontract.GVRStatefulSet, "StatefulSet", []string{"spec", "template", "spec"}, true},
		{k8scontract.GVRDaemonSet, "DaemonSet", []string{"spec", "template", "spec"}, true},
		{k8scontract.GVRCronJob, "CronJob", []string{"spec", "jobTemplate", "spec", "template", "spec"}, false},
	}
	out := []k8scontract.Reference{}
	truncated := false
	for _, k := range kinds {
		list, err := u.dyn.Resource(toSchema(k.gvr)).Namespace(ref.Namespace).List(u.ctx, metav1.ListOptions{Limit: referenceLimit})
		if apierrors.IsForbidden(err) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		if list.GetContinue() != "" {
			truncated = true
		}
		for i := range list.Items {
			item := &list.Items[i]
			if via := podSpecRefs(nestedMap(item.Object, k.path...), secret, ref.Name); via != nil {
				out = append(out, k8scontract.Reference{
					Ref:  k8scontract.Ref{Group: k.gvr.Group, Version: k.gvr.Version, Resource: k.gvr.Resource, Namespace: ref.Namespace, Name: item.GetName()},
					Kind: k.kind, Via: via, Restartable: k.restartable,
				})
			}
		}
	}
	return out, truncated, nil
}

func (u *userClients) relations(ref k8scontract.Ref) (k8scontract.RelationsResponse, error) {
	var out k8scontract.RelationsResponse
	switch ref.GVR() {
	case k8scontract.GVRIngress:
		obj, _, err := u.getObject(ref)
		if err != nil {
			return out, err
		}
		out.Rules, err = u.ingressRules(obj.Object, ref.Namespace)
		return out, err
	case k8scontract.GVRService:
		obj, _, err := u.getObject(ref)
		if err != nil {
			return out, err
		}
		chain, err := u.serviceChain(ref.Namespace, ref.Name, obj.Object)
		out.Service = &chain
		return out, err
	case k8scontract.GVRConfigMap, k8scontract.GVRSecret:
		// 先确认对象存在且用户可读（Secret 的引用关系不向无权查看它的人透露）。
		if _, _, err := u.getObject(ref); err != nil {
			return out, err
		}
		refs, truncated, err := u.references(ref)
		out.References, out.Truncated = refs, truncated
		return out, err
	}
	return out, biz.ErrUnsupported("relations are available for ingresses, services, configmaps and secrets only")
}

// ---- 诊断摘要 ----

func diagConditions(obj map[string]any) []k8scontract.DiagCondition {
	out := []k8scontract.DiagCondition{}
	for _, c := range nestedList(obj, "status", "conditions") {
		m := asMap(c)
		out = append(out, k8scontract.DiagCondition{
			Type: str(m, "type"), Status: str(m, "status"), Reason: str(m, "reason"), Message: clipText(str(m, "message"), 300),
		})
	}
	return out
}

func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n] + "…"
}

func (u *userClients) diagEvents(ref k8scontract.Ref) []k8scontract.Event {
	evs, err := u.events(ref)
	if err != nil {
		return []k8scontract.Event{}
	}
	items := evs.Items
	if len(items) > k8scontract.MaxDiagEvents {
		items = items[:k8scontract.MaxDiagEvents]
	}
	for i := range items {
		items[i].Message = clipText(items[i].Message, 400)
	}
	return items
}

func resourceText(m map[string]any) string {
	parts := []string{}
	for _, k := range []string{"cpu", "memory"} {
		if v := str(m, k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, ",")
}

// logTail 读取容器日志的末尾若干行（以用户身份；失败返回原因）。
func (u *userClients) logTail(namespace, pod, container string, previous bool) ([]string, string) {
	tail, limit := int64(k8scontract.MaxDiagLogLines), int64(k8scontract.MaxDiagLogBytes)
	raw, err := u.kube.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container: container, Previous: previous, TailLines: &tail, LimitBytes: &limit,
	}).DoRaw(u.ctx)
	if err != nil {
		return nil, clipText(mapError(err).Error(), 200)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, ""
	}
	for i := range lines {
		lines[i] = clipText(lines[i], 500)
	}
	return lines, ""
}

func (u *userClients) diagnosePod(ref k8scontract.Ref) (k8scontract.PodDiagnosis, error) {
	ref.Group, ref.Version, ref.Resource = k8scontract.GVRPod.Group, k8scontract.GVRPod.Version, k8scontract.GVRPod.Resource
	out := k8scontract.PodDiagnosis{Namespace: ref.Namespace, Name: ref.Name, Containers: []k8scontract.DiagContainer{}}
	pod, _, err := u.getObject(ref)
	if err != nil {
		return out, err
	}
	obj := pod.Object
	out.Phase = str(obj, "status", "phase")
	out.State, out.Status, out.Reason, _, _, _ = PodStatus(obj)
	out.Reason = clipText(out.Reason, 400)
	out.Node, out.Owner = str(obj, "spec", "nodeName"), ownerText(obj)
	if t, ok := timeAt(obj, "status", "startTime"); ok {
		out.StartTime = &t
	}
	out.Conditions = diagConditions(obj)
	logs := 0
	for _, group := range []struct {
		spec, status string
		init         bool
	}{{"initContainers", "initContainerStatuses", true}, {"containers", "containerStatuses", false}} {
		statuses := map[string]map[string]any{}
		for _, s := range nestedList(obj, "status", group.status) {
			m := asMap(s)
			statuses[str(m, "name")] = m
		}
		for _, c := range nestedList(obj, "spec", group.spec) {
			spec := asMap(c)
			d := k8scontract.DiagContainer{
				Name: str(spec, "name"), Image: str(spec, "image"), Init: group.init, State: "waiting",
				Requests: resourceText(nestedMap(spec, "resources", "requests")), Limits: resourceText(nestedMap(spec, "resources", "limits")),
			}
			st := statuses[d.Name]
			d.Ready = boolean(st, "ready")
			d.RestartCount = numOr(st, 0, "restartCount")
			for _, name := range []string{"running", "waiting", "terminated"} {
				if m := nestedMap(st, "state", name); m != nil {
					d.State, d.Reason, d.Message = name, str(m, "reason"), clipText(str(m, "message"), 400)
					if code, ok := num(m, "exitCode"); ok && name == "terminated" {
						d.Message = strings.TrimSpace(fmt.Sprintf("exit code %d %s", code, d.Message))
					}
				}
			}
			if last := nestedMap(st, "lastState", "terminated"); last != nil {
				d.LastReason = str(last, "reason")
				if code, ok := num(last, "exitCode"); ok {
					d.LastExitCode = &code
				}
				if t, ok := timeAt(last, "finishedAt"); ok {
					d.LastFinished = &t
				}
			}
			// 只读有问题的容器的日志，且至多 4 个容器：把输出控制在助手的工具输出上限以内。
			// 名额只算真正读了日志的容器：waiting 且没重启过的容器没有日志可读，不能把名额占掉。
			completedInit := group.init && d.State == "terminated" && d.Reason == "Completed"
			if st != nil && !completedInit && (d.State != "waiting" || d.RestartCount > 0) && (!d.Ready || d.RestartCount > 0) && logs < 4 {
				logs++
				d.LogTail, d.LogError = u.logTail(ref.Namespace, ref.Name, d.Name, false)
				if d.RestartCount > 0 {
					d.PreviousLogTail, _ = u.logTail(ref.Namespace, ref.Name, d.Name, true)
				}
			}
			out.Containers = append(out.Containers, d)
		}
	}
	out.Events = u.diagEvents(ref)
	return out, nil
}

var diagWorkloads = map[k8scontract.GVR]string{
	k8scontract.GVRDeployment: "Deployment", k8scontract.GVRStatefulSet: "StatefulSet", k8scontract.GVRDaemonSet: "DaemonSet",
}

func (u *userClients) diagnoseWorkload(ref k8scontract.Ref) (k8scontract.WorkloadDiagnosis, error) {
	out := k8scontract.WorkloadDiagnosis{Namespace: ref.Namespace, Name: ref.Name, AbnormalPods: []k8scontract.DiagPod{}}
	kind, ok := diagWorkloads[ref.GVR()]
	if !ok {
		return out, biz.ErrUnsupported("workload diagnosis is available for deployments, statefulsets and daemonsets only")
	}
	out.Kind = kind
	w, _, err := u.getObject(ref)
	if err != nil {
		return out, err
	}
	obj := w.Object
	if kind == "DaemonSet" {
		out.Desired = numOr(obj, 0, "status", "desiredNumberScheduled")
		out.Ready = numOr(obj, 0, "status", "numberReady")
		out.Updated = numOr(obj, 0, "status", "updatedNumberScheduled")
		out.Available = numOr(obj, 0, "status", "numberAvailable")
	} else {
		out.Desired = numOr(obj, 1, "spec", "replicas")
		out.Ready = numOr(obj, 0, "status", "readyReplicas")
		out.Updated = numOr(obj, 0, "status", "updatedReplicas")
		out.Available = numOr(obj, 0, "status", "availableReplicas")
	}
	out.Paused = boolean(obj, "spec", "paused")
	out.Images = templateImages(obj)
	out.Conditions = diagConditions(obj)
	if sel, err := podSelector(ref.GVR(), obj); err == nil && sel != nil {
		list, err := u.dyn.Resource(toSchema(k8scontract.GVRPod)).Namespace(ref.Namespace).List(u.ctx, metav1.ListOptions{LabelSelector: sel.String(), Limit: 500})
		if err == nil {
			out.TotalPods = len(list.Items)
			for i := range list.Items {
				p := list.Items[i].Object
				st, text, reason, ready, total, restarts := PodStatus(p)
				if (st == sOK && ready == total) || st == sDone {
					continue
				}
				if len(out.AbnormalPods) < k8scontract.MaxDiagPods {
					out.AbnormalPods = append(out.AbnormalPods, k8scontract.DiagPod{
						Name: list.Items[i].GetName(), Status: text, Reason: clipText(reason, 300),
						Ready: fmt.Sprintf("%d/%d", ready, total), Restarts: restarts, Node: str(p, "spec", "nodeName"),
					})
				}
			}
		}
	}
	out.Events = u.diagEvents(ref)
	return out, nil
}

// ---- 主机定位 ----

// nodeByIP 按内网地址定位节点：与节点的每一个 InternalIP 比较（不只第一个）；IPv4-mapped 的写法
// （::ffff:10.0.0.1）两侧都归一成 IPv4 再比。没有返回 nil。
func nodeByIP(entries []*biz.Entry, addr netip.Addr) *biz.Entry {
	addr = addr.Unmap()
	for _, e := range entries {
		for _, ip := range e.InternalIPs {
			if a, err := netip.ParseAddr(ip); err == nil && a.Unmap() == addr {
				return e
			}
		}
	}
	return nil
}

// locateMaxPods 定位时节点上至多统计的 Pod 数。
const locateMaxPods = 2000

// workloadOf 由 Pod 的所有者推出所属的工作负载：ReplicaSet 归到同名前缀的 Deployment，
// Job 保持为 Job，没有所有者的 Pod 自成一项。
func workloadOf(e *biz.Entry) (kind, name string) {
	owner, _ := e.Row.Cells["owner"].(string)
	kind, name, ok := strings.Cut(owner, "/")
	if !ok || name == "" {
		return "Pod", e.Row.Name
	}
	if kind == "ReplicaSet" {
		if i := strings.LastIndexByte(name, '-'); i > 0 {
			return "Deployment", name[:i]
		}
	}
	return kind, name
}

func (u *userClients) locateNode(req k8scontract.LocateNodeRequest) (k8scontract.LocateNodeResponse, error) {
	var out k8scontract.LocateNodeResponse
	addr, err := netip.ParseAddr(strings.TrimSpace(req.IP))
	if err != nil {
		return out, biz.ErrInvalid("ip is not a valid address")
	}
	if ok, err := u.c.allowed(u.ctx, u.id, "", k8scontract.GVRNode, "list"); err != nil || !ok {
		return out, err // 无权查看节点：按未找到处理，不透露该主机是否属于此集群
	}
	nodes, err := u.c.cache.get(u.ctx, k8scontract.GVRNode)
	if err != nil {
		return out, err
	}
	node := nodeByIP(nodes.store.snapshot(nil), addr)
	if node == nil {
		return out, nil
	}
	out.Found, out.Node, out.State = true, node.Row.Name, node.Row.State
	if c, ok := node.Row.Cells["status"].(state); ok {
		out.Status = c.Text
	}
	var pods []*biz.Entry
	if store := u.c.cache.peek(k8scontract.GVRPod); store != nil {
		for _, e := range store.snapshot(nil) {
			if e.Node == node.Row.Name {
				pods = append(pods, e)
			}
		}
	} else {
		// 没有 Pod 缓存：只列这个节点上的 Pod（按 spec.nodeName 筛选，一个节点至多几百个），不为此建立缓存。
		convert := converterFor(k8scontract.GVRPod)
		list, err := u.c.platDyn.Resource(toSchema(k8scontract.GVRPod)).List(u.ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + node.Row.Name, Limit: locateMaxPods,
		})
		if err != nil {
			return out, mapError(err)
		}
		out.Truncated = list.GetContinue() != ""
		for i := range list.Items {
			pods = append(pods, convert(&list.Items[i]))
		}
	}
	// 只统计用户可见命名空间里的 Pod（逐个命名空间由集群确认，结果有缓存）。
	visible := map[string]bool{}
	type key struct{ ns, kind, name string }
	counts := map[key]int{}
	for _, p := range pods {
		ok, seen := visible[p.Row.Namespace]
		if !seen {
			if ok, err = u.c.allowed(u.ctx, u.id, p.Row.Namespace, k8scontract.GVRPod, "list"); err != nil {
				return out, err
			}
			visible[p.Row.Namespace] = ok
		}
		if !ok {
			continue
		}
		out.Pods++
		kind, name := workloadOf(p)
		counts[key{p.Row.Namespace, kind, name}]++
	}
	out.Workloads = make([]k8scontract.LocatedWorkload, 0, len(counts))
	for k, n := range counts {
		out.Workloads = append(out.Workloads, k8scontract.LocatedWorkload{Namespace: k.ns, Kind: k.kind, Name: k.name, Pods: n})
	}
	sort.Slice(out.Workloads, func(i, j int) bool {
		a, b := out.Workloads[i], out.Workloads[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return out, nil
}
