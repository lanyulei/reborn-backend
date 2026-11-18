package k8scontract

import (
	"strings"
	"time"
)

// 本文件是「可观测」部分的契约：总览摘要、Warning 事件、关联链路与引用关系、诊断摘要、主机定位。

// 操作名（InternalOpPath 之后的路径段）。
const (
	OpSelector         = "selector"          // ObjectRequest → SelectorResponse
	OpRelations        = "relations"         // ObjectRequest → RelationsResponse
	OpDiagnosePod      = "diagnose-pod"      // ObjectRequest → PodDiagnosis
	OpDiagnoseWorkload = "diagnose-workload" // ObjectRequest → WorkloadDiagnosis
	OpLocateNode       = "locate-node"       // LocateNodeRequest → LocateNodeResponse
)

// SelectorResponse 工作负载 / Service 选中 Pod 的标签选择器（K8s 语法，含 matchExpressions），
// 前端把它作为 Pod 列表的 labelSelector。Empty = 该对象没有选择器（不选中任何 Pod）。
type SelectorResponse struct {
	Namespace     string `json:"namespace"`
	LabelSelector string `json:"labelSelector"`
	Empty         bool   `json:"empty"`
}

// ---- 关联链路与引用关系 ----

// 链路节点的健康状态复用 State*（ok / pending / abnormal / unknown）。

// RelationPod 链路末端的一个 Pod。
type RelationPod struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	// Status Pod 的状态文本（Running / CrashLoopBackOff …）。
	Status string `json:"status"`
	Node   string `json:"node,omitempty"`
	IP     string `json:"ip,omitempty"`
}

// RelationService 链路中的一个 Service 及其选中的 Pod。
type RelationService struct {
	Name string `json:"name"`
	// Port Ingress 指向的端口（名称或数字）；Service 自身的链路为空。
	Port  string `json:"port,omitempty"`
	Type  string `json:"type,omitempty"`
	State string `json:"state"`
	// Problem 断点说明：not_found（Service 不存在）/ no_selector（没有选择器，无法判断后端）/
	// no_pods（选择器没有选中任何 Pod）/ not_ready（选中的 Pod 没有一个就绪）/ port_mismatch（Service 没有该端口）。
	Problem     string        `json:"problem,omitempty"`
	Selector    string        `json:"selector,omitempty"`
	ReadyPods   int           `json:"readyPods"`
	TotalPods   int           `json:"totalPods"`
	Pods        []RelationPod `json:"pods"`
	PodsOmitted int           `json:"podsOmitted,omitempty"`
}

// 链路断点（RelationService.Problem）。
const (
	ProblemNotFound     = "not_found"
	ProblemNoSelector   = "no_selector"
	ProblemNoPods       = "no_pods"
	ProblemNotReady     = "not_ready"
	ProblemPortMismatch = "port_mismatch"
)

// RelationRule Ingress 的一条转发规则（域名 + 路径 → Service）。
type RelationRule struct {
	Host    string          `json:"host"`
	Path    string          `json:"path"`
	Service RelationService `json:"service"`
}

// Reference 引用了某个 ConfigMap / Secret 的工作负载。
type Reference struct {
	Ref  Ref    `json:"ref"`
	Kind string `json:"kind"`
	// Via 引用方式：env / envFrom / volume / imagePullSecret / projected。
	Via []string `json:"via"`
	// Restartable 可以滚动重启（Deployment / StatefulSet / DaemonSet）。
	Restartable bool `json:"restartable"`
}

// RelationsResponse 对象的关联关系。按对象类型只填其中一部分：
// Ingress → Rules；Service → Service；ConfigMap / Secret → References。
type RelationsResponse struct {
	Rules      []RelationRule   `json:"rules,omitempty"`
	Service    *RelationService `json:"service,omitempty"`
	References []Reference      `json:"references,omitempty"`
	// Truncated 引用关系扫描的工作负载数达到上限，结果可能不全。
	Truncated bool `json:"truncated,omitempty"`
}

// MaxRelationPods 链路里每个 Service 至多列出的 Pod 数。
const MaxRelationPods = 50

// ---- 诊断摘要（面向智能助手：输出控制在 32KB 以内）----

// DiagContainer 容器状态。
type DiagContainer struct {
	Name         string `json:"name"`
	Image        string `json:"image"`
	Init         bool   `json:"init,omitempty"`
	Ready        bool   `json:"ready"`
	RestartCount int64  `json:"restartCount"`
	// State running / waiting / terminated；Reason / Message 为等待或终止的原因。
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
	// Last* 上一次终止的情况（发生过重启时）。
	LastReason   string     `json:"lastReason,omitempty"`
	LastExitCode *int64     `json:"lastExitCode,omitempty"`
	LastFinished *time.Time `json:"lastFinished,omitempty"`
	// Requests / Limits 资源配置（形如 cpu=100m,memory=128Mi）。
	Requests string `json:"requests,omitempty"`
	Limits   string `json:"limits,omitempty"`
	// LogTail 最近若干行日志（只对未就绪或重启过的容器读取）；LogError 读取失败的原因。
	LogTail  []string `json:"logTail,omitempty"`
	LogError string   `json:"logError,omitempty"`
	// PreviousLogTail 上一次（崩溃前）的日志末尾。
	PreviousLogTail []string `json:"previousLogTail,omitempty"`
}

// DiagCondition 一个 Condition。
type DiagCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// PodDiagnosis Pod 诊断摘要。
type PodDiagnosis struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Phase     string `json:"phase"`
	// State / Status / Reason 与列表一致的归一状态。
	State      string          `json:"state"`
	Status     string          `json:"status"`
	Reason     string          `json:"reason,omitempty"`
	Node       string          `json:"node,omitempty"`
	Owner      string          `json:"owner,omitempty"`
	StartTime  *time.Time      `json:"startTime,omitempty"`
	Conditions []DiagCondition `json:"conditions"`
	Containers []DiagContainer `json:"containers"`
	Events     []Event         `json:"events"`
}

// DiagPod 工作负载下的一个异常 Pod。
type DiagPod struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Reason   string `json:"reason,omitempty"`
	Ready    string `json:"ready"`
	Restarts int64  `json:"restarts"`
	Node     string `json:"node,omitempty"`
}

// WorkloadDiagnosis 工作负载诊断摘要。
type WorkloadDiagnosis struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	// Desired / Ready / Updated / Available 副本数。
	Desired    int64           `json:"desired"`
	Ready      int64           `json:"ready"`
	Updated    int64           `json:"updated"`
	Available  int64           `json:"available"`
	Paused     bool            `json:"paused,omitempty"`
	Images     []string        `json:"images"`
	Conditions []DiagCondition `json:"conditions"`
	// TotalPods 选中的 Pod 总数；AbnormalPods 其中未就绪或异常的（至多 MaxDiagPods 个）。
	TotalPods    int       `json:"totalPods"`
	AbnormalPods []DiagPod `json:"abnormalPods"`
	Events       []Event   `json:"events"`
}

// 诊断摘要的上限。
const (
	MaxDiagLogLines = 30
	MaxDiagLogBytes = 4 << 10
	MaxDiagEvents   = 15
	MaxDiagPods     = 10
)

// ---- 主机定位（CMDB 主机 → 集群节点）----

// LocateNodeRequest 按内网 IP 定位节点。
type LocateNodeRequest struct {
	IP string `json:"ip"`
}

// LocatedWorkload 节点上的一个工作负载（按 Pod 的所有者归并）。
type LocatedWorkload struct {
	Namespace string `json:"namespace"`
	// Kind / Name 所有者：Deployment / StatefulSet / DaemonSet / Job / Pod（无控制器的 Pod）。
	Kind string `json:"kind"`
	Name string `json:"name"`
	Pods int    `json:"pods"`
}

// LocateNodeResponse 定位结果。Found 为 false 时其余字段为空。
type LocateNodeResponse struct {
	Found     bool              `json:"found"`
	Node      string            `json:"node,omitempty"`
	State     string            `json:"state,omitempty"`
	Status    string            `json:"status,omitempty"`
	Workloads []LocatedWorkload `json:"workloads,omitempty"`
	// Pods 节点上（调用者可见）的 Pod 总数。
	Pods int `json:"pods"`
	// Truncated Pod 数超过上限，工作负载统计可能不全。
	Truncated bool `json:"truncated,omitempty"`
}

// ---- 总览摘要（k8s_cluster_runtime.summary）----

// SummaryItem 摘要里点名的一个对象。
type SummaryItem struct {
	Namespace string `json:"namespace,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Name      string `json:"name"`
	// Text 状态文本或补充说明。
	Text string `json:"text,omitempty"`
	// Value 数值（重启次数等）。
	Value int64 `json:"value,omitempty"`
}

// ClusterSummary 集群总览摘要：数据面周期计算后落库，总览页只读库，不实时查集群。
// CPU 单位为毫核，内存单位为字节。
type ClusterSummary struct {
	NodeTotal    int `json:"nodeTotal"`
	NodeReady    int `json:"nodeReady"`
	NodeCordoned int `json:"nodeCordoned"`

	CPUAllocatable    int64 `json:"cpuAllocatable"`
	CPURequested      int64 `json:"cpuRequested"`
	MemoryAllocatable int64 `json:"memoryAllocatable"`
	MemoryRequested   int64 `json:"memoryRequested"`
	PodCapacity       int64 `json:"podCapacity"`

	// Pods 各归一状态（State*）的 Pod 数。
	Pods     map[string]int `json:"pods"`
	PodTotal int            `json:"podTotal"`

	WorkloadTotal    int `json:"workloadTotal"`
	WorkloadAbnormal int `json:"workloadAbnormal"`
	// AbnormalWorkloads 未就绪的工作负载（至多 SummaryTopN 个）。
	AbnormalWorkloads []SummaryItem `json:"abnormalWorkloads"`
	// RestartPods 重启次数最多的 Pod（重启数 ≥ SummaryRestartThreshold，至多 SummaryTopN 个）。
	RestartPods []SummaryItem `json:"restartPods"`
	// AbnormalNodes 未就绪的节点（至多 SummaryTopN 个）。
	AbnormalNodes []SummaryItem `json:"abnormalNodes"`

	// Partial 本次统计没有覆盖全部对象（对象数超过上限或中途出错），数字偏小。
	Partial bool `json:"partial,omitempty"`
	// DurationMS 本次统计耗时。
	DurationMS int64 `json:"durationMs"`
}

// 摘要参数。
const (
	SummaryTopN             = 10
	SummaryRestartThreshold = 5
)

// ---- Warning 事件 ----

// DefaultEventReasons 缺省上报到通知的 Warning 事件原因（k8s_cluster.event_reasons 为空时使用）。
func DefaultEventReasons() []string {
	return []string{
		"NodeNotReady", "OOMKilling", "OOMKilled", "FailedScheduling", "Failed", "ErrImagePull", "ImagePullBackOff",
		"FailedMount", "FailedAttachVolume", "BackOff",
	}
}

// 事件通知的参数。
const (
	// EventNotifyWindow 同一对象同一原因的事件，在这个窗口内至多通知一次。
	EventNotifyWindow = 30 * time.Minute
	// EventNotifyFreshness 只有最近发生的事件才通知（数据面重启后重放的旧事件不通知）。
	EventNotifyFreshness = 10 * time.Minute
	// EventRetention Warning 事件的保留期（按最近发生时间）。
	EventRetention = 7 * 24 * time.Hour
	// NotifyEventPrefix 事件通知的事件类型前缀：k8s.event.<原因小写>。
	NotifyEventPrefix = "k8s.event."
)

// EventNotifyType 某个事件原因对应的通知事件类型；原因里不合法的字符（通知事件类型只允许小写字母、
// 数字、点、下划线、连字符）替换为连字符。
func EventNotifyType(reason string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(reason) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	s := b.String()
	if s == "" {
		s = "unknown"
	}
	if len(s) > 100 {
		s = s[:100]
	}
	return NotifyEventPrefix + s
}

// EventNotifyPayload 事件通知的内容体（内置模板读取这些字段）。
type EventNotifyPayload struct {
	Title       string `json:"title"`
	ClusterID   string `json:"cluster_id"`
	ClusterName string `json:"cluster_name"`
	Namespace   string `json:"namespace"`
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Reason      string `json:"reason"`
	Message     string `json:"message"`
	Count       int64  `json:"count"`
	// Time 已格式化的时间文本。
	Time string `json:"time"`
}
