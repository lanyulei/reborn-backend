package k8scontract

import (
	"path"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 治理（P3）：限时 kubeconfig 的集群代理、文件传输、调试容器、终端录制、巡检。
// 表结构见 core/migrations/0077_k8s_governance.up.sql。
// ---------------------------------------------------------------------------

// core → 数据面的原始通道（除身份头与 ClusterHeader 外，请求与响应原样透传，不缓冲）。
const (
	// InternalKubePath 集群 API 的原始透传：其后直接接集群 API 的路径（/api、/apis、/version、/openapi），
	// 方法、查询串、请求体不变；数据面以用户身份转发，支持协议升级（exec / attach）与长响应（watch、日志）。
	InternalKubePath = "/internal/v1/kube"
	// InternalFileDownloadPath / InternalFileUploadPath 容器文件传输，参数见 FileQuery* 查询参数。
	// 下载：GET，响应体是文件原文（单个文件）或 tar（目录），文件名在 Content-Disposition。
	// 上传：POST，请求体是文件原文，Content-Length 必填。
	InternalFileDownloadPath = "/internal/v1/file/download"
	InternalFileUploadPath   = "/internal/v1/file/upload"

	// KubeTokenHeader 发起请求的限时 kubeconfig 令牌 ID（写入审计）。
	KubeTokenHeader = "X-Reborn-Kube-Token" //nolint:gosec // 请求头的名称，不是凭据
)

// 文件传输的查询参数。
const (
	FileQueryNamespace = "namespace"
	FileQueryPod       = "pod"
	FileQueryContainer = "container"
	// FileQueryPath 下载：容器内的文件或目录的绝对路径；上传：目标目录的绝对路径。
	FileQueryPath = "path"
	// FileQueryName 上传：文件名（不含路径）。
	FileQueryName = "name"
)

// DefaultMaxFileBytes 文件传输的单次大小上限缺省值。
const DefaultMaxFileBytes = 100 << 20

// kubeAPIRoots 集群代理允许的路径根。
var kubeAPIRoots = []string{"/api", "/apis", "/version", "/openapi", "/healthz", "/livez", "/readyz"}

// KubePathAllowed 报告集群 API 路径是否允许经代理访问：必须是规范化的绝对路径、位于允许的根之下。
func KubePathAllowed(p string) bool {
	if p == "" || p[0] != '/' || strings.ContainsAny(p, "\\\x00") || path.Clean(p) != strings.TrimRight(p, "/") && p != "/" {
		return false
	}
	for _, root := range kubeAPIRoots {
		if p == root || strings.HasPrefix(p, root+"/") {
			return true
		}
	}
	return false
}

// KubeRequest 从集群 API 路径解析出的请求目标（审计与策略用）。
type KubeRequest struct {
	Group       string
	Version     string
	Resource    string
	Namespace   string
	Name        string
	Subresource string
	// Resourceful 路径指向一种资源（否则是 /version、发现文档等）。
	Resourceful bool
}

// ParseKubePath 解析集群 API 路径：/api/v1/... 或 /apis/<group>/<version>/...，
// 其后为 [namespaces/<ns>/]<resource>[/<name>[/<subresource>...]]。
func ParseKubePath(p string) KubeRequest {
	var out KubeRequest
	seg := strings.Split(strings.Trim(p, "/"), "/")
	switch {
	case len(seg) >= 2 && seg[0] == "api":
		out.Version, seg = seg[1], seg[2:]
	case len(seg) >= 3 && seg[0] == "apis":
		out.Group, out.Version, seg = seg[1], seg[2], seg[3:]
	default:
		return out
	}
	// /namespaces/<ns> 本身是对 namespaces 资源的访问；其后还有段才是命名空间限定。
	// 例外：核心组的 /namespaces/<name>/status 与 /finalize 是 Namespace 自身的子资源（Resource=namespaces、
	// Name=<name>、Subresource=status / finalize）。只在核心组这样判：其他组里可能有复数名恰好叫
	// status / finalize 的命名空间级资源，那种仍按命名空间限定解析。
	namespaceSubresource := out.Group == "" && len(seg) == 3 && (seg[2] == "status" || seg[2] == "finalize")
	if len(seg) >= 3 && seg[0] == "namespaces" && !namespaceSubresource {
		out.Namespace, seg = seg[1], seg[2:]
	}
	if len(seg) == 0 || seg[0] == "" {
		return out
	}
	out.Resourceful, out.Resource = true, seg[0]
	if len(seg) >= 2 {
		out.Name = seg[1]
	}
	if len(seg) >= 3 {
		out.Subresource = strings.Join(seg[2:], "/")
	}
	return out
}

// 新增的审计动作（k8s_audit.action）。
const (
	ActionDebug            = "debug"            // 启动调试容器
	ActionFileUpload       = "file_upload"      // 向容器上传文件
	ActionFileDownload     = "file_download"    // 从容器下载文件
	ActionExecReplay       = "exec_replay"      // 回放终端录制
	ActionKubeconfigIssue  = "kubeconfig_issue" // 签发限时 kubeconfig
	ActionKubeconfigRevoke = "kubeconfig_revoke"
	ActionInspect          = "inspect" // 手动触发巡检
)

// 新增的审计资源类型。
const (
	AuditResourceKubeToken  = "kube-token"
	AuditResourceExecRecord = "exec-session"
	AuditResourceInspection = "inspection"
)

// AuditViaKubeconfig 经限时 kubeconfig（kubectl）发起的写操作在审计 patch 里的 via 取值。
const AuditViaKubeconfig = "kubeconfig"

// ---- 限时 kubeconfig ----

// 限时 kubeconfig 的有效期范围。
const (
	MinKubeTokenTTL     = time.Hour
	DefaultMaxKubeToken = 7 * 24 * time.Hour
	// MaxKubeTokensPerUser 每个用户未失效的令牌数上限。
	MaxKubeTokensPerUser = 20
)

// ---- 终端录制 ----

// 终端录制限制。
const (
	// DefaultMaxRecordBytes 单个会话录制的输出字节上限，超过后停止录制并标记不完整。
	DefaultMaxRecordBytes = 8 << 20
	// RecordFlushInterval / RecordFlushBytes 录制缓冲的落库条件（先到为准）。
	RecordFlushInterval = 3 * time.Second
	RecordFlushBytes    = 256 << 10
)

// 录制事件类型（块内每行 [毫秒偏移, 类型, 数据]）。
const (
	RecordOutput = "o" // 数据为 base64 的终端输出
	RecordResize = "r" // 数据为 "<列>x<行>"
)

// ---- 巡检 ----

// 巡检触发方式（k8s_inspection_run.trigger）。
const (
	InspectScheduled int32 = 1
	InspectManual    int32 = 2
)

// 巡检状态（k8s_inspection_run.status）。
const (
	InspectPending int32 = 1
	InspectRunning int32 = 2
	InspectDone    int32 = 3
	InspectFailed  int32 = 4
)

// 发现项级别（k8s_inspection_finding.severity）。
const (
	FindingInfo     int32 = 1
	FindingWarning  int32 = 2
	FindingCritical int32 = 3
)

// 检查项（k8s_inspection_finding.check_id）。
const (
	CheckNoRequests    = "no_requests"    // 容器未设置资源请求
	CheckNoLimits      = "no_limits"      // 容器未设置资源限制
	CheckImageLatest   = "image_latest"   // 镜像使用 latest 或未指定标签
	CheckSingleReplica = "single_replica" // Deployment / StatefulSet 只有一个副本
	CheckNoProbe       = "no_probe"       // 容器缺少就绪或存活探针
	CheckCertExpiring  = "cert_expiring"  // TLS 证书即将到期或已过期
	CheckPodRestarts   = "pod_restarts"   // Pod 频繁重启
	CheckPVCPending    = "pvc_pending"    // PVC 长时间 Pending
	CheckNodeUnhealthy = "node_unhealthy" // 节点 NotReady 或有压力状况
	CheckVersionSkew   = "version_skew"   // 节点版本与控制面偏差过大
)

// 巡检阈值与限制。
const (
	// MaxFindingsPerCheck 每个检查项落库的发现项上限（总数仍计入摘要）。
	MaxFindingsPerCheck = 500
	// InspectCertDays 证书剩余有效期低于此天数即报告。
	InspectCertDays = 30
	// InspectRestartThreshold 容器重启次数达到此值即报告。
	InspectRestartThreshold = 10
	// InspectPVCPendingAge PVC 处于 Pending 超过此时长即报告。
	InspectPVCPendingAge = time.Hour
	// InspectInterval 定时巡检的间隔；InspectKeepRuns 每个集群保留的运行数。
	InspectInterval = 24 * time.Hour
	InspectKeepRuns = 30
)

// InspectionCheck 摘要里一个检查项的统计。
type InspectionCheck struct {
	Check    string `json:"check"`
	Severity int32  `json:"severity"`
	// Count 发现项总数（可能大于落库的条数）。
	Count int `json:"count"`
}

// InspectionSummary 巡检摘要（k8s_inspection_run.summary）。
type InspectionSummary struct {
	Checks []InspectionCheck `json:"checks"`
	// Objects 各类被检查对象的数量。
	Pods      int `json:"pods"`
	Workloads int `json:"workloads"`
	Nodes     int `json:"nodes"`
	Certs     int `json:"certs"`
	// Critical / Warning / Info 各级别的发现项总数。
	Critical int `json:"critical"`
	Warning  int `json:"warning"`
	Info     int `json:"info"`
	// Partial 对象数超过遍历上限，结果不完整。
	Partial bool `json:"partial,omitempty"`
}

// InspectionScore 由各级别的发现数算出 0-100 的得分：严重每项扣 5、警告扣 1、提示扣 0.2，各自封顶
// 50 / 30 / 10。只是排序与趋势用的粗略刻度，不代表合规结论。
func InspectionScore(critical, warning, info int) int {
	score := 100 - min(critical*5, 50) - min(warning, 30) - min(info/5, 10)
	return max(score, 0)
}

// NotifyInspectionReport 巡检报告的通知事件类型。
const NotifyInspectionReport = "k8s.inspection.report"

// InspectionNotifyPayload 巡检报告通知的内容体（内置模板读取这些字段）。
type InspectionNotifyPayload struct {
	ClusterID   string `json:"cluster_id"`
	ClusterName string `json:"cluster_name"`
	RunID       string `json:"run_id"`
	Score       int    `json:"score"`
	Critical    int    `json:"critical"`
	Warning     int    `json:"warning"`
	Info        int    `json:"info"`
	Time        string `json:"time"`
	// URL 站内相对路径（/k8s/inspection?cluster=…&run=…）。
	URL string `json:"url"`
}

// 文件传输的错误原因（与 core/api/k8s/v1 的 ErrorReason 同名）。
const (
	ReasonFileInvalid  = "K8S_FILE_INVALID"   // 400
	ReasonFileTooLarge = "K8S_FILE_TOO_LARGE" // 413
)

// KubeIsChange 报告一个集群 API 请求是否属于「变更」：对资源的创建、更新、删除。读请求、exec / attach
// （终端不走审批）、权限询问类资源不算。需审批的集群上，经限时 kubeconfig 发起的变更由 core 拒绝。
func KubeIsChange(method, p string) bool {
	switch method {
	case "POST", "PUT", "PATCH", "DELETE":
	default:
		return false
	}
	t := ParseKubePath(p)
	if !t.Resourceful {
		return false
	}
	first, _, _ := strings.Cut(t.Subresource, "/")
	if first == "exec" || first == "attach" {
		return false
	}
	return t.Group != "authorization.k8s.io" && t.Group != "authentication.k8s.io"
}

// DebugRole 平台创建的 ClusterRole：允许给 Pod 添加临时容器（调试容器）。内置的 edit / admin 不含
// pods/ephemeralcontainers，没有它只有集群管理员能用调试容器。随「开发」及以上档位的授权一并绑定。
const DebugRole = "reborn:debug"
