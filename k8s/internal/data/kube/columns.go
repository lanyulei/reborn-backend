package kube

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type (
	col   = k8scontract.Column
	state = k8scontract.StateCell
)

const (
	sOK       = k8scontract.StateOK
	sPending  = k8scontract.StatePending
	sAbnormal = k8scontract.StateAbnormal
	sUnknown  = k8scontract.StateUnknown
	sDone     = k8scontract.StateDone
)

// resourceView 一种缓存资源的列定义与行提取。fill 写入 Row.State 与 Row.Cells，并可补充 Entry.Node。
type resourceView struct {
	columns []col
	fill    func(obj map[string]any, e *biz.Entry)
}

func textCol(key, title string, priority ...int) col {
	c := col{Key: key, Title: title, Type: k8scontract.ColText, Sortable: true}
	if len(priority) > 0 {
		c.Priority = priority[0]
	}
	return c
}

func numCol(key, title string, priority ...int) col {
	c := textCol(key, title, priority...)
	c.Type = k8scontract.ColNumber
	return c
}

func typedCol(key, title, typ string, priority ...int) col {
	c := textCol(key, title, priority...)
	c.Type = typ
	c.Sortable = typ == k8scontract.ColState || typ == k8scontract.ColTime
	return c
}

var stateCol = typedCol("status", "状态", k8scontract.ColState)

var views = map[k8scontract.GVR]resourceView{
	k8scontract.GVRPod: {
		columns: []col{
			stateCol, textCol("ready", "就绪"), numCol("restarts", "重启次数"), textCol("node", "节点"), textCol("ip", "IP"),
			typedCol("images", "镜像", k8scontract.ColImages, 1), textCol("qos", "QoS", 1), textCol("owner", "所属", 1),
		},
		fill: fillPod,
	},
	k8scontract.GVRDeployment: {
		columns: []col{
			stateCol, textCol("ready", "就绪"), numCol("upToDate", "已更新"), numCol("available", "可用"),
			typedCol("images", "镜像", k8scontract.ColImages), textCol("strategy", "策略", 1),
		},
		fill: fillDeployment,
	},
	k8scontract.GVRStatefulSet: {
		columns: []col{
			stateCol, textCol("ready", "就绪"), numCol("upToDate", "已更新"),
			typedCol("images", "镜像", k8scontract.ColImages), textCol("service", "服务", 1),
		},
		fill: fillStatefulSet,
	},
	k8scontract.GVRDaemonSet: {
		columns: []col{
			stateCol, textCol("ready", "就绪"), numCol("desired", "期望"), numCol("upToDate", "已更新"), numCol("available", "可用"),
			typedCol("images", "镜像", k8scontract.ColImages),
		},
		fill: fillDaemonSet,
	},
	k8scontract.GVRReplicaSet: {
		columns: []col{
			stateCol, textCol("ready", "就绪"), numCol("revision", "版本"), textCol("owner", "所属"),
			typedCol("images", "镜像", k8scontract.ColImages),
		},
		fill: fillReplicaSet,
	},
	k8scontract.GVRJob: {
		columns: []col{
			stateCol, textCol("completions", "完成数"), textCol("duration", "耗时"), textCol("owner", "所属"),
			typedCol("images", "镜像", k8scontract.ColImages, 1),
		},
		fill: fillJob,
	},
	k8scontract.GVRCronJob: {
		columns: []col{
			stateCol, textCol("schedule", "调度"), textCol("timeZone", "时区", 1), numCol("active", "运行中"),
			typedCol("lastSchedule", "上次调度", k8scontract.ColTime), typedCol("lastSuccess", "上次成功", k8scontract.ColTime),
			typedCol("images", "镜像", k8scontract.ColImages, 1),
		},
		fill: fillCronJob,
	},
	k8scontract.GVRNode: {
		columns: []col{
			stateCol, typedCol("roles", "角色", k8scontract.ColList), textCol("version", "版本"), textCol("internalIP", "内网 IP"),
			textCol("cpu", "CPU 可分配"), textCol("memory", "内存可分配"), numCol("pods", "Pod 容量"),
			typedCol("taints", "污点", k8scontract.ColList), textCol("os", "系统", 1), textCol("kernel", "内核", 1),
			textCol("runtime", "运行时", 1), textCol("arch", "架构", 1),
		},
		fill: fillNode,
	},
	k8scontract.GVRNamespace: {
		columns: []col{stateCol, typedCol("labels", "标签", k8scontract.ColLabels)},
		fill:    fillNamespace,
	},
	k8scontract.GVRService: {
		columns: []col{
			textCol("type", "类型"), textCol("clusterIP", "集群 IP"), typedCol("externalIP", "外部地址", k8scontract.ColList),
			typedCol("ports", "端口", k8scontract.ColList), typedCol("selector", "选择器", k8scontract.ColLabels),
		},
		fill: fillService,
	},
	k8scontract.GVRIngress: {
		columns: []col{
			textCol("class", "类"), typedCol("hosts", "域名", k8scontract.ColList), typedCol("address", "地址", k8scontract.ColList),
			typedCol("tls", "TLS", k8scontract.ColList), typedCol("backends", "后端", k8scontract.ColList, 1),
		},
		fill: fillIngress,
	},
	k8scontract.GVRConfigMap: {
		columns: []col{typedCol("labels", "标签", k8scontract.ColLabels)},
		fill:    func(obj map[string]any, e *biz.Entry) { e.Row.Cells["labels"] = e.Labels },
	},
	k8scontract.GVRSecret: {
		// 没有「类型」列：缓存只取元数据（见 cache.go start），type 不在元数据里。
		columns: []col{typedCol("labels", "标签", k8scontract.ColLabels)},
		fill:    func(obj map[string]any, e *biz.Entry) { e.Row.Cells["labels"] = e.Labels },
	},
	k8scontract.GVRPVC: {
		columns: []col{
			stateCol, textCol("volume", "卷"), textCol("capacity", "容量"), typedCol("accessModes", "访问模式", k8scontract.ColList),
			textCol("storageClass", "存储类"),
		},
		fill: fillPVC,
	},
	k8scontract.GVRPV: {
		columns: []col{
			stateCol, textCol("capacity", "容量"), typedCol("accessModes", "访问模式", k8scontract.ColList),
			textCol("reclaimPolicy", "回收策略"), textCol("claim", "声明"), textCol("storageClass", "存储类"),
		},
		fill: fillPV,
	},
	k8scontract.GVRStorageClass: {
		columns: []col{
			textCol("provisioner", "供应者"), textCol("reclaimPolicy", "回收策略"), textCol("bindingMode", "绑定模式"),
			textCol("expansion", "允许扩容"), textCol("default", "默认"),
		},
		fill: fillStorageClass,
	},
}

// converterFor 返回把集群对象转成缓存条目的函数。缓存里不保留对象原文（managedFields、
// last-applied 注解、ConfigMap / Secret 的内容都不进内存）。
func converterFor(gvr k8scontract.GVR) func(u *unstructured.Unstructured) *biz.Entry {
	view := views[gvr]
	return func(u *unstructured.Unstructured) *biz.Entry {
		e := &biz.Entry{
			Row: k8scontract.Row{
				Namespace: u.GetNamespace(), Name: u.GetName(), UID: string(u.GetUID()),
				CreateTime: u.GetCreationTimestamp().Time, Cells: map[string]any{},
			},
			Labels: u.GetLabels(),
		}
		for _, o := range u.GetOwnerReferences() {
			e.Owners = append(e.Owners, string(o.UID))
		}
		if view.fill != nil {
			view.fill(u.Object, e)
		}
		return e
	}
}

func setState(e *biz.Entry, st, text, reason string) {
	e.Row.State = st
	e.Row.Cells["status"] = state{State: st, Text: text, Reason: reason}
}

func ownerText(obj map[string]any) string {
	for _, o := range nestedList(obj, "metadata", "ownerReferences") {
		m := asMap(o)
		return str(m, "kind") + "/" + str(m, "name")
	}
	return ""
}

func containerImages(obj map[string]any, path ...string) []string {
	var out []string
	for _, c := range nestedList(obj, append(path, "containers")...) {
		if img := str(asMap(c), "image"); img != "" {
			out = append(out, img)
		}
	}
	return out
}

func templateImages(obj map[string]any) []string {
	return containerImages(obj, "spec", "template", "spec")
}

// ---- Pod ----

// PodStatus 推导 Pod 的展示状态：与 kubectl get pods 的 STATUS 列同一套规则，细化到容器的实际原因
// （CrashLoopBackOff、ImagePullBackOff、OOMKilled、Init:… 等）。
func PodStatus(obj map[string]any) (st, text, reason string, ready, total int, restarts int64) {
	phase := str(obj, "status", "phase")
	text = phase
	if r := str(obj, "status", "reason"); r != "" {
		text = r
	}
	initContainers := nestedList(obj, "spec", "initContainers")
	restartable := map[string]bool{}
	for _, c := range initContainers {
		m := asMap(c)
		if str(m, "restartPolicy") == "Always" { // 边车容器
			restartable[str(m, "name")] = true
		}
	}
	message := ""
	initializing := false
	for i, cs := range nestedList(obj, "status", "initContainerStatuses") {
		m := asMap(cs)
		restarts += numOr(m, 0, "restartCount")
		term, wait := nestedMap(m, "state", "terminated"), nestedMap(m, "state", "waiting")
		switch {
		case term != nil && numOr(term, 0, "exitCode") == 0:
			continue
		case restartable[str(m, "name")] && boolean(m, "started"):
			continue
		case term != nil:
			if r := str(term, "reason"); r != "" {
				text = "Init:" + r
			} else if sig := numOr(term, 0, "signal"); sig != 0 {
				text = "Init:Signal:" + strconv.FormatInt(sig, 10)
			} else {
				text = "Init:ExitCode:" + strconv.FormatInt(numOr(term, 0, "exitCode"), 10)
			}
			message = str(term, "message")
			initializing = true
		case wait != nil && str(wait, "reason") != "" && str(wait, "reason") != "PodInitializing":
			text = "Init:" + str(wait, "reason")
			message = str(wait, "message")
			initializing = true
		default:
			text = fmt.Sprintf("Init:%d/%d", i, len(initContainers))
			initializing = true
		}
		if initializing {
			break
		}
	}
	statuses := nestedList(obj, "status", "containerStatuses")
	total = len(nestedList(obj, "spec", "containers"))
	if !initializing {
		hasRunning := false
		for i := len(statuses) - 1; i >= 0; i-- {
			m := asMap(statuses[i])
			term, wait := nestedMap(m, "state", "terminated"), nestedMap(m, "state", "waiting")
			switch {
			case wait != nil && str(wait, "reason") != "":
				text, message = str(wait, "reason"), str(wait, "message")
			case term != nil && str(term, "reason") != "":
				text, message = str(term, "reason"), str(term, "message")
			case term != nil:
				if sig := numOr(term, 0, "signal"); sig != 0 {
					text = "Signal:" + strconv.FormatInt(sig, 10)
				} else {
					text = "ExitCode:" + strconv.FormatInt(numOr(term, 0, "exitCode"), 10)
				}
			case boolean(m, "ready") && nestedMap(m, "state", "running") != nil:
				hasRunning = true
			}
		}
		if text == "Completed" && hasRunning {
			if c := condition(obj, "Ready"); c != nil && str(c, "status") == "True" {
				text = "Running"
			} else {
				text = "NotReady"
			}
		}
	}
	for _, cs := range statuses {
		m := asMap(cs)
		restarts += numOr(m, 0, "restartCount")
		if boolean(m, "ready") {
			ready++
		}
	}
	if _, deleting := nestedString(obj, "metadata", "deletionTimestamp"); deleting {
		if str(obj, "status", "reason") == "NodeLost" {
			text = "Unknown"
		} else {
			text = "Terminating"
		}
	}
	if message == "" {
		message = str(obj, "status", "message")
	}
	if message == "" && phase == "Pending" {
		if c := condition(obj, "PodScheduled"); c != nil && str(c, "status") == "False" {
			message = str(c, "message")
			if text == "Pending" && str(c, "reason") != "" {
				text = str(c, "reason")
			}
		}
	}
	reason = message
	switch {
	case text == "Running" && ready == total:
		st = sOK
	case text == "Running", text == "NotReady":
		st, text = sAbnormal, "NotReady"
		if reason == "" {
			reason = fmt.Sprintf("%d/%d containers ready", ready, total)
		}
	case text == "Completed" || text == "Succeeded":
		st = sDone
	case text == "Terminating", text == "ContainerCreating", text == "PodInitializing", text == "Pending",
		strings.HasPrefix(text, "Init:") && !initFailed(text):
		st = sPending
	case text == "Unknown":
		st = sUnknown
	default:
		st = sAbnormal
	}
	return st, text, reason, ready, total, restarts
}

// initFailed 报告 Init:… 状态是否为失败（Init:2/3 这样的进度不算）。
func initFailed(text string) bool {
	rest := strings.TrimPrefix(text, "Init:")
	return rest != "" && (rest[0] < '0' || rest[0] > '9')
}

func fillPod(obj map[string]any, e *biz.Entry) {
	st, text, reason, ready, total, restarts := PodStatus(obj)
	setState(e, st, text, reason)
	e.Node = str(obj, "spec", "nodeName")
	c := e.Row.Cells
	c["ready"] = fmt.Sprintf("%d/%d", ready, total)
	c["restarts"] = restarts
	c["node"] = e.Node
	c["ip"] = str(obj, "status", "podIP")
	c["images"] = containerImages(obj, "spec")
	c["qos"] = str(obj, "status", "qosClass")
	c["owner"] = ownerText(obj)
	if phase := str(obj, "status", "phase"); phase != "Succeeded" && phase != "Failed" {
		for _, ct := range nestedList(obj, "spec", "containers") {
			m := asMap(ct)
			e.CPU += milliCPU(str(m, "resources", "requests", "cpu"))
			e.Memory += quantityBytes(str(m, "resources", "requests", "memory"))
		}
	}
}

// ---- 工作负载 ----

func workloadState(e *biz.Entry, obj map[string]any, desired, ready, updated int64, extra func() (string, string, string)) {
	gen, _ := num(obj, "metadata", "generation")
	observed, _ := num(obj, "status", "observedGeneration")
	switch {
	case extra != nil:
		if st, text, reason := extra(); st != "" {
			setState(e, st, text, reason)
			return
		}
		fallthrough
	default:
		switch {
		case desired == 0 && ready == 0:
			setState(e, sDone, "已停止", "")
		case observed < gen || updated < desired:
			setState(e, sPending, "更新中", "")
		case ready >= desired:
			setState(e, sOK, "运行中", "")
		case ready == 0:
			setState(e, sAbnormal, "不可用", fmt.Sprintf("%d/%d ready", ready, desired))
		default:
			setState(e, sAbnormal, "部分就绪", fmt.Sprintf("%d/%d ready", ready, desired))
		}
	}
}

func fillDeployment(obj map[string]any, e *biz.Entry) {
	desired := numOr(obj, 1, "spec", "replicas")
	ready := numOr(obj, 0, "status", "readyReplicas")
	updated := numOr(obj, 0, "status", "updatedReplicas")
	workloadState(e, obj, desired, ready, updated, func() (string, string, string) {
		if boolean(obj, "spec", "paused") {
			return sPending, "已暂停", ""
		}
		if c := condition(obj, "Progressing"); c != nil && str(c, "status") == "False" {
			return sAbnormal, "发布失败", str(c, "message")
		}
		if c := condition(obj, "ReplicaFailure"); c != nil && str(c, "status") == "True" {
			return sAbnormal, "创建失败", str(c, "message")
		}
		return "", "", ""
	})
	c := e.Row.Cells
	c["ready"] = fmt.Sprintf("%d/%d", ready, desired)
	c["upToDate"] = updated
	c["available"] = numOr(obj, 0, "status", "availableReplicas")
	c["images"] = templateImages(obj)
	c["strategy"] = str(obj, "spec", "strategy", "type")
}

func fillStatefulSet(obj map[string]any, e *biz.Entry) {
	desired := numOr(obj, 1, "spec", "replicas")
	ready := numOr(obj, 0, "status", "readyReplicas")
	updated := numOr(obj, 0, "status", "updatedReplicas")
	workloadState(e, obj, desired, ready, updated, nil)
	c := e.Row.Cells
	c["ready"] = fmt.Sprintf("%d/%d", ready, desired)
	c["upToDate"] = updated
	c["images"] = templateImages(obj)
	c["service"] = str(obj, "spec", "serviceName")
}

func fillDaemonSet(obj map[string]any, e *biz.Entry) {
	desired := numOr(obj, 0, "status", "desiredNumberScheduled")
	ready := numOr(obj, 0, "status", "numberReady")
	updated := numOr(obj, 0, "status", "updatedNumberScheduled")
	workloadState(e, obj, desired, ready, updated, nil)
	c := e.Row.Cells
	c["ready"] = fmt.Sprintf("%d/%d", ready, desired)
	c["desired"] = desired
	c["upToDate"] = updated
	c["available"] = numOr(obj, 0, "status", "numberAvailable")
	c["images"] = templateImages(obj)
}

func fillReplicaSet(obj map[string]any, e *biz.Entry) {
	desired := numOr(obj, 1, "spec", "replicas")
	ready := numOr(obj, 0, "status", "readyReplicas")
	workloadState(e, obj, desired, ready, desired, nil)
	c := e.Row.Cells
	c["ready"] = fmt.Sprintf("%d/%d", ready, desired)
	rev, _ := strconv.ParseInt(str(obj, "metadata", "annotations", revisionAnnotation), 10, 64)
	c["revision"] = rev
	c["owner"] = ownerText(obj)
	c["images"] = templateImages(obj)
}

func fillJob(obj map[string]any, e *biz.Entry) {
	succeeded := numOr(obj, 0, "status", "succeeded")
	failed := numOr(obj, 0, "status", "failed")
	active := numOr(obj, 0, "status", "active")
	c := e.Row.Cells
	if n, ok := num(obj, "spec", "completions"); ok {
		c["completions"] = fmt.Sprintf("%d/%d", succeeded, n)
	} else {
		c["completions"] = fmt.Sprintf("%d/1", succeeded)
	}
	switch {
	case condTrue(obj, "Complete"):
		setState(e, sDone, "已完成", "")
	case condTrue(obj, "Failed"):
		setState(e, sAbnormal, "失败", str(condition(obj, "Failed"), "message"))
	case boolean(obj, "spec", "suspend"):
		setState(e, sPending, "已暂停", "")
	case active > 0:
		setState(e, sPending, "运行中", "")
	case failed > 0:
		setState(e, sAbnormal, "重试中", fmt.Sprintf("%d failed", failed))
	default:
		setState(e, sPending, "等待中", "")
	}
	if start, ok := timeAt(obj, "status", "startTime"); ok {
		if end, ok := timeAt(obj, "status", "completionTime"); ok {
			c["duration"] = end.Sub(start).Round(1e9).String()
		}
	}
	c["owner"] = ownerText(obj)
	c["images"] = templateImages(obj)
}

func condTrue(obj map[string]any, typ string) bool {
	c := condition(obj, typ)
	return c != nil && str(c, "status") == "True"
}

func fillCronJob(obj map[string]any, e *biz.Entry) {
	if boolean(obj, "spec", "suspend") {
		setState(e, sPending, "已暂停", "")
	} else {
		setState(e, sOK, "已启用", "")
	}
	c := e.Row.Cells
	c["schedule"] = str(obj, "spec", "schedule")
	c["timeZone"] = str(obj, "spec", "timeZone")
	c["active"] = int64(len(nestedList(obj, "status", "active")))
	if t, ok := timeAt(obj, "status", "lastScheduleTime"); ok {
		c["lastSchedule"] = t
	}
	if t, ok := timeAt(obj, "status", "lastSuccessfulTime"); ok {
		c["lastSuccess"] = t
	}
	c["images"] = containerImages(obj, "spec", "jobTemplate", "spec", "template", "spec")
}

// ---- 节点、命名空间 ----

func fillNode(obj map[string]any, e *biz.Entry) {
	ready := condition(obj, "Ready")
	unschedulable := boolean(obj, "spec", "unschedulable")
	switch {
	case ready == nil || str(ready, "status") == "Unknown":
		setState(e, sUnknown, "Unknown", str(ready, "message"))
	case str(ready, "status") != "True":
		setState(e, sAbnormal, "NotReady", str(ready, "message"))
	case unschedulable:
		setState(e, sPending, "Ready,SchedulingDisabled", "")
	default:
		setState(e, sOK, "Ready", "")
	}
	var roles []string
	for k := range e.Labels {
		if r, ok := strings.CutPrefix(k, "node-role.kubernetes.io/"); ok && r != "" {
			roles = append(roles, r)
		}
	}
	sort.Strings(roles)
	c := e.Row.Cells
	c["roles"] = roles
	c["version"] = str(obj, "status", "nodeInfo", "kubeletVersion")
	for _, a := range nestedList(obj, "status", "addresses") {
		if m := asMap(a); str(m, "type") == "InternalIP" {
			c["internalIP"] = str(m, "address")
			break
		}
	}
	// 主机定位要能匹配任意一个内网地址（双栈节点；CMDB 侧登记的不一定是列表里展示的那个）：全部记下。
	for _, a := range nestedList(obj, "status", "addresses") {
		if m := asMap(a); str(m, "type") == "InternalIP" {
			if ip := str(m, "address"); ip != "" {
				e.InternalIPs = append(e.InternalIPs, ip)
			}
		}
	}
	c["cpu"] = str(obj, "status", "allocatable", "cpu")
	e.CPU = milliCPU(str(obj, "status", "allocatable", "cpu"))
	e.Memory = quantityBytes(str(obj, "status", "allocatable", "memory"))
	c["memory"] = humanQuantity(str(obj, "status", "allocatable", "memory"))
	pods, _ := strconv.ParseInt(str(obj, "status", "allocatable", "pods"), 10, 64)
	c["pods"] = pods
	var taints []string
	for _, t := range nestedList(obj, "spec", "taints") {
		m := asMap(t)
		s := str(m, "key")
		if v := str(m, "value"); v != "" {
			s += "=" + v
		}
		taints = append(taints, s+":"+str(m, "effect"))
	}
	c["taints"] = taints
	c["os"] = str(obj, "status", "nodeInfo", "osImage")
	c["kernel"] = str(obj, "status", "nodeInfo", "kernelVersion")
	c["runtime"] = str(obj, "status", "nodeInfo", "containerRuntimeVersion")
	c["arch"] = str(obj, "status", "nodeInfo", "architecture")
}

// humanQuantity 把内存 / 存储量格式化为二进制单位（Gi / Mi）。无法解析时原样返回。
func humanQuantity(s string) string {
	if s == "" {
		return ""
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return s
	}
	v := float64(q.Value())
	switch {
	case v >= 1<<40:
		return strconv.FormatFloat(v/(1<<40), 'f', 1, 64) + "Ti"
	case v >= 1<<30:
		return strconv.FormatFloat(v/(1<<30), 'f', 1, 64) + "Gi"
	case v >= 1<<20:
		return strconv.FormatFloat(v/(1<<20), 'f', 0, 64) + "Mi"
	}
	return s
}

func fillNamespace(obj map[string]any, e *biz.Entry) {
	if phase := str(obj, "status", "phase"); phase == "Terminating" {
		setState(e, sPending, "Terminating", "")
	} else {
		setState(e, sOK, "Active", "")
	}
	e.Row.Cells["labels"] = e.Labels
}

// ---- 网络 ----

func fillService(obj map[string]any, e *biz.Entry) {
	c := e.Row.Cells
	typ := str(obj, "spec", "type")
	c["type"] = typ
	c["clusterIP"] = str(obj, "spec", "clusterIP")
	var ext []string
	for _, ing := range nestedList(obj, "status", "loadBalancer", "ingress") {
		m := asMap(ing)
		if ip := str(m, "ip"); ip != "" {
			ext = append(ext, ip)
		} else if h := str(m, "hostname"); h != "" {
			ext = append(ext, h)
		}
	}
	for _, ip := range nestedList(obj, "spec", "externalIPs") {
		if s, ok := ip.(string); ok {
			ext = append(ext, s)
		}
	}
	if typ == "ExternalName" {
		ext = append(ext, str(obj, "spec", "externalName"))
	}
	c["externalIP"] = ext
	var ports []string
	for _, p := range nestedList(obj, "spec", "ports") {
		m := asMap(p)
		s := strconv.FormatInt(numOr(m, 0, "port"), 10)
		if np := numOr(m, 0, "nodePort"); np != 0 {
			s += ":" + strconv.FormatInt(np, 10)
		}
		proto := str(m, "protocol")
		if proto == "" {
			proto = "TCP"
		}
		ports = append(ports, s+"/"+proto)
	}
	c["ports"] = ports
	c["selector"] = stringMap(obj, "spec", "selector")
}

func fillIngress(obj map[string]any, e *biz.Entry) {
	c := e.Row.Cells
	class := str(obj, "spec", "ingressClassName")
	if class == "" {
		class = str(obj, "metadata", "annotations", "kubernetes.io/ingress.class")
	}
	c["class"] = class
	var hosts, backends []string
	seen := map[string]struct{}{}
	for _, r := range nestedList(obj, "spec", "rules") {
		m := asMap(r)
		h := str(m, "host")
		if h == "" {
			h = "*"
		}
		hosts = append(hosts, h)
		for _, p := range nestedList(m, "http", "paths") {
			if svc := str(asMap(p), "backend", "service", "name"); svc != "" {
				if _, ok := seen[svc]; !ok {
					seen[svc] = struct{}{}
					backends = append(backends, svc)
				}
			}
		}
	}
	c["hosts"] = hosts
	c["backends"] = backends
	var addrs []string
	for _, ing := range nestedList(obj, "status", "loadBalancer", "ingress") {
		m := asMap(ing)
		if ip := str(m, "ip"); ip != "" {
			addrs = append(addrs, ip)
		} else if h := str(m, "hostname"); h != "" {
			addrs = append(addrs, h)
		}
	}
	c["address"] = addrs
	var tls []string
	for _, t := range nestedList(obj, "spec", "tls") {
		if s := str(asMap(t), "secretName"); s != "" {
			tls = append(tls, s)
		}
	}
	c["tls"] = tls
}

// ---- 存储 ----

func accessModes(obj map[string]any, path ...string) []string {
	var out []string
	for _, m := range nestedList(obj, path...) {
		if s, ok := m.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func volumeState(e *biz.Entry, phase string) {
	switch phase {
	case "Bound", "Available":
		setState(e, sOK, phase, "")
	case "Pending", "Released":
		setState(e, sPending, phase, "")
	case "Lost", "Failed":
		setState(e, sAbnormal, phase, "")
	default:
		setState(e, sUnknown, phase, "")
	}
}

func fillPVC(obj map[string]any, e *biz.Entry) {
	volumeState(e, str(obj, "status", "phase"))
	c := e.Row.Cells
	c["volume"] = str(obj, "spec", "volumeName")
	capacity := str(obj, "status", "capacity", "storage")
	if capacity == "" {
		capacity = str(obj, "spec", "resources", "requests", "storage")
	}
	c["capacity"] = capacity
	c["accessModes"] = accessModes(obj, "spec", "accessModes")
	c["storageClass"] = str(obj, "spec", "storageClassName")
}

func fillPV(obj map[string]any, e *biz.Entry) {
	volumeState(e, str(obj, "status", "phase"))
	c := e.Row.Cells
	c["capacity"] = str(obj, "spec", "capacity", "storage")
	c["accessModes"] = accessModes(obj, "spec", "accessModes")
	c["reclaimPolicy"] = str(obj, "spec", "persistentVolumeReclaimPolicy")
	if name := str(obj, "spec", "claimRef", "name"); name != "" {
		c["claim"] = str(obj, "spec", "claimRef", "namespace") + "/" + name
	}
	c["storageClass"] = str(obj, "spec", "storageClassName")
}

func fillStorageClass(obj map[string]any, e *biz.Entry) {
	c := e.Row.Cells
	c["provisioner"] = str(obj, "provisioner")
	c["reclaimPolicy"] = str(obj, "reclaimPolicy")
	c["bindingMode"] = str(obj, "volumeBindingMode")
	yesNo := func(b bool) string {
		if b {
			return "是"
		}
		return "否"
	}
	c["expansion"] = yesNo(boolean(obj, "allowVolumeExpansion"))
	c["default"] = yesNo(str(obj, "metadata", "annotations", "storageclass.kubernetes.io/is-default-class") == "true")
}

// revisionAnnotation Deployment 的版本号注解（ReplicaSet 与 Deployment 上都有）。
const revisionAnnotation = "deployment.kubernetes.io/revision"
