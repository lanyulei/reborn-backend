package biz

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// 指标面板：查询语句全部是服务端的固定模板，调用方只能选面板与目标，不能提交查询语句。
// 填进模板的值只有集群名、命名空间、节点 / 工作负载 / Pod 名：先按 DNS 名称的字符集校验
// （不含引号、反斜杠、花括号），再经 promString / promRegex 转义——两道防线，任一道都足以防注入。

// MetricsTarget 指标面板的目标。
type MetricsTarget struct {
	// Kind cluster / node / workload / pod。
	Kind         string
	Namespace    string
	Name         string
	WorkloadKind string
}

// 目标类型。
const (
	TargetCluster  = "cluster"
	TargetNode     = "node"
	TargetWorkload = "workload"
	TargetPod      = "pod"
)

// MetricQuery 面板里的一条序列。
type MetricQuery struct {
	Name string
	Expr string
}

// MetricPanel 一个面板：标题、单位与若干条序列的查询。
type MetricPanel struct {
	ID    string
	Title string
	// Unit cores / bytes / percent / bps / count。
	Unit    string
	Queries []MetricQuery
}

var (
	metricNSPattern   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	metricNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$`)
)

// promString 转义为 PromQL 双引号字符串的内容。
func promString(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// promRegex 把字面量转义为正则片段，再转义为字符串内容。
func promRegex(s string) string { return promString(regexp.QuoteMeta(s)) }

// eq 标签等值匹配。
func eq(label, value string) string { return label + `="` + promString(value) + `"` }

// validateTarget 校验目标：字段与目标类型匹配，名称只含 DNS 名称允许的字符。
func validateTarget(t MetricsTarget) error {
	switch t.Kind {
	case TargetCluster:
		if t.Namespace != "" || t.Name != "" || t.WorkloadKind != "" {
			return detail(ErrInvalidArgument, "cluster target takes no namespace or name")
		}
		return nil
	case TargetNode:
		if t.Namespace != "" || t.WorkloadKind != "" || !metricNamePattern.MatchString(t.Name) {
			return detail(ErrInvalidArgument, "node target requires a valid node name")
		}
		return nil
	case TargetWorkload, TargetPod:
		if !metricNSPattern.MatchString(t.Namespace) || !metricNamePattern.MatchString(t.Name) {
			return detail(ErrInvalidArgument, "%s target requires a valid namespace and name", t.Kind)
		}
		if t.Kind == TargetPod {
			if t.WorkloadKind != "" {
				return detail(ErrInvalidArgument, "pod target takes no workload kind")
			}
			return nil
		}
		switch t.WorkloadKind {
		case "Deployment", "StatefulSet", "DaemonSet":
			return nil
		}
		return detail(ErrInvalidArgument, "workload kind must be Deployment, StatefulSet or DaemonSet")
	}
	return detail(ErrInvalidArgument, "unknown metrics target kind")
}

// podMatcher 目标对应的 Pod 标签匹配：Pod 精确匹配；工作负载按其 Pod 的命名规则做正则匹配
// （Deployment：<名称>-<模板哈希>-<随机串>；StatefulSet：<名称>-<序号>；DaemonSet：<名称>-<随机串>）。
func podMatcher(t MetricsTarget) string {
	if t.Kind == TargetPod {
		return eq("pod", t.Name)
	}
	suffix := "-[a-z0-9]+"
	switch t.WorkloadKind {
	case "Deployment":
		suffix = "-[a-z0-9]+-[a-z0-9]+"
	case "StatefulSet":
		suffix = "-[0-9]+"
	}
	return `pod=~"` + promRegex(t.Name) + suffix + `"`
}

// BuildPanels 生成目标的全部面板。cluster 是集群名（指标标签 cluster 的取值），每条查询都带它。
func BuildPanels(cluster string, t MetricsTarget) ([]MetricPanel, error) {
	if !metricNSPattern.MatchString(cluster) {
		return nil, detail(ErrInvalidArgument, "invalid cluster name")
	}
	if err := validateTarget(t); err != nil {
		return nil, err
	}
	c := eq("cluster", cluster)
	const realContainers = `container!="",container!="POD"`
	q := func(name, format string, args ...any) MetricQuery {
		return MetricQuery{Name: name, Expr: fmt.Sprintf(format, args...)}
	}
	switch t.Kind {
	case TargetCluster:
		return []MetricPanel{
			{ID: "cpu", Title: "CPU", Unit: "cores", Queries: []MetricQuery{
				q("usage", `sum(rate(container_cpu_usage_seconds_total{%s,%s}[5m]))`, c, realContainers),
				q("requests", `sum(kube_pod_container_resource_requests{%s,resource="cpu"})`, c),
				q("allocatable", `sum(kube_node_status_allocatable{%s,resource="cpu"})`, c),
			}},
			{ID: "memory", Title: "Memory", Unit: "bytes", Queries: []MetricQuery{
				q("usage", `sum(container_memory_working_set_bytes{%s,%s})`, c, realContainers),
				q("requests", `sum(kube_pod_container_resource_requests{%s,resource="memory"})`, c),
				q("allocatable", `sum(kube_node_status_allocatable{%s,resource="memory"})`, c),
			}},
			{ID: "pods", Title: "Pods", Unit: "count", Queries: []MetricQuery{
				q("running", `sum(kube_pod_status_phase{%s,phase="Running"})`, c),
				q("pending", `sum(kube_pod_status_phase{%s,phase="Pending"})`, c),
				q("failed", `sum(kube_pod_status_phase{%s,phase="Failed"})`, c),
			}},
			{ID: "nodes", Title: "Nodes", Unit: "count", Queries: []MetricQuery{
				q("ready", `sum(kube_node_status_condition{%s,condition="Ready",status="true"})`, c),
				q("total", `count(kube_node_info{%s})`, c),
			}},
		}, nil
	case TargetNode:
		// node-exporter 的 instance 标签按 kube-prometheus 的惯例是节点名（可带端口）。
		inst := `instance=~"` + promRegex(t.Name) + `(:[0-9]+)?"`
		const realFS = `fstype!~"tmpfs|overlay|squashfs|ramfs|devtmpfs"`
		const realNIC = `device!~"lo|veth.*|docker.*|cni.*|flannel.*|cali.*|tunl.*|kube-ipvs.*"`
		return []MetricPanel{
			{ID: "cpu", Title: "CPU", Unit: "percent", Queries: []MetricQuery{
				q("usage", `100 * (1 - avg(rate(node_cpu_seconds_total{%s,%s,mode="idle"}[5m])))`, c, inst),
			}},
			{ID: "memory", Title: "Memory", Unit: "percent", Queries: []MetricQuery{
				q("usage", `100 * (1 - sum(node_memory_MemAvailable_bytes{%s,%s}) / sum(node_memory_MemTotal_bytes{%s,%s}))`, c, inst, c, inst),
			}},
			{ID: "disk", Title: "Disk", Unit: "percent", Queries: []MetricQuery{
				q("usage", `100 * (1 - sum(node_filesystem_avail_bytes{%s,%s,%s}) / sum(node_filesystem_size_bytes{%s,%s,%s}))`, c, inst, realFS, c, inst, realFS),
			}},
			{ID: "network", Title: "Network", Unit: "bps", Queries: []MetricQuery{
				q("receive", `8 * sum(rate(node_network_receive_bytes_total{%s,%s,%s}[5m]))`, c, inst, realNIC),
				q("transmit", `8 * sum(rate(node_network_transmit_bytes_total{%s,%s,%s}[5m]))`, c, inst, realNIC),
			}},
			{ID: "pods", Title: "Pods", Unit: "count", Queries: []MetricQuery{
				q("pods", `count(kube_pod_info{%s,%s})`, c, eq("node", t.Name)),
			}},
		}, nil
	}
	sel := c + "," + eq("namespace", t.Namespace) + "," + podMatcher(t)
	return []MetricPanel{
		{ID: "cpu", Title: "CPU", Unit: "cores", Queries: []MetricQuery{
			q("usage", `sum(rate(container_cpu_usage_seconds_total{%s,%s}[5m]))`, sel, realContainers),
			q("requests", `sum(kube_pod_container_resource_requests{%s,resource="cpu"})`, sel),
			q("limits", `sum(kube_pod_container_resource_limits{%s,resource="cpu"})`, sel),
		}},
		{ID: "memory", Title: "Memory", Unit: "bytes", Queries: []MetricQuery{
			q("usage", `sum(container_memory_working_set_bytes{%s,%s})`, sel, realContainers),
			q("requests", `sum(kube_pod_container_resource_requests{%s,resource="memory"})`, sel),
			q("limits", `sum(kube_pod_container_resource_limits{%s,resource="memory"})`, sel),
		}},
		{ID: "network", Title: "Network", Unit: "bps", Queries: []MetricQuery{
			q("receive", `8 * sum(rate(container_network_receive_bytes_total{%s}[5m]))`, sel),
			q("transmit", `8 * sum(rate(container_network_transmit_bytes_total{%s}[5m]))`, sel),
		}},
		{ID: "restarts", Title: "Restarts", Unit: "count", Queries: []MetricQuery{
			q("restarts", `sum(kube_pod_container_status_restarts_total{%s})`, sel),
		}},
	}, nil
}

// 指标查询的时间范围。
const (
	defaultMetricsRange = time.Hour
	maxMetricsRange     = 7 * 24 * time.Hour
	metricsPoints       = 120
	minMetricsStep      = 15 * time.Second
)

// metricsWindow 规整时间范围并算出步长（约 120 个点，不小于 15 秒，取整到秒）。
func metricsWindow(start, end *time.Time, now time.Time) (time.Time, time.Time, time.Duration, error) {
	e := now
	if end != nil {
		e = *end
	}
	s := e.Add(-defaultMetricsRange)
	if start != nil {
		s = *start
	}
	switch {
	case !s.Before(e):
		return s, e, 0, detail(ErrTimeRangeInvalid, "start time must be earlier than end time")
	case e.Sub(s) > maxMetricsRange:
		return s, e, 0, detail(ErrTimeRangeInvalid, "time range must not exceed 7 days")
	}
	step := max((e.Sub(s) / metricsPoints).Truncate(time.Second), minMetricsStep)
	return s, e, step, nil
}
