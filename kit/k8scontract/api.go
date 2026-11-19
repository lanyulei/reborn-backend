package k8scontract

import "time"

// ---------------------------------------------------------------------------
// core → 数据面的内部接口（仅内网；每个请求带身份头，见 identity.go）
//
//   POST InternalOpPath + <op>      请求体与响应体均为 JSON（下方的 *Request / *Response）
//   POST InternalStreamPath + <op>  请求体为 JSON，响应为 SSE（事件见各流的说明）
//   GET  InternalExecPath           WebSocket；查询参数 params = base64url(JSON ExecParams)
//
// 除 OpClusterTest 外都要带 ClusterHeader。失败时响应体为 ErrorBody，HTTP 状态码 = ErrorBody.Code。
// JSON 字段一律小驼峰：core 把响应体原样透传给浏览器。
// ---------------------------------------------------------------------------

// 内部接口路径。
const (
	InternalOpPath     = "/internal/v1/op/"
	InternalStreamPath = "/internal/v1/stream/"
	InternalExecPath   = "/internal/v1/exec"
)

// 操作名（InternalOpPath 之后的路径段）。
const (
	OpClusterTest     = "cluster-test"       // ClusterTestRequest → ClusterTestResponse
	OpAPIResources    = "api-resources"      // APIResourcesRequest → APIResourcesResponse
	OpNamespaces      = "namespaces"         // Empty → NamespacesResponse
	OpList            = "list"               // ListRequest → ListResponse
	OpGet             = "get"                // ObjectRequest → ObjectResponse
	OpEvents          = "events"             // ObjectRequest → EventsResponse
	OpPreview         = "preview"            // ApplyRequest → PreviewResponse
	OpApply           = "apply"              // ApplyRequest → ApplyResponse
	OpDelete          = "delete"             // DeleteRequest → Empty
	OpManifestPreview = "manifest-preview"   // ManifestRequest → ManifestResponse
	OpManifestApply   = "manifest-apply"     // ManifestRequest → ManifestResponse
	OpScale           = "scale"              // ScaleRequest → Empty
	OpRestart         = "restart"            // ObjectRequest → Empty
	OpSetImage        = "set-image"          // SetImageRequest → Empty
	OpRevisions       = "revisions"          // ObjectRequest → RevisionsResponse
	OpRollback        = "rollback"           // RollbackRequest → Empty
	OpPause           = "pause"              // PauseRequest → Empty
	OpCronTrigger     = "cronjob-trigger"    // ObjectRequest → ApplyResponse（新建的 Job）
	OpCronSuspend     = "cronjob-suspend"    // SuspendRequest → Empty
	OpPodEvict        = "pod-evict"          // ObjectRequest → Empty
	OpPodResize       = "pod-resize"         // ResizeRequest → Empty
	OpNodeCordon      = "node-cordon"        // CordonRequest → Empty
	OpDrainPreview    = "node-drain-preview" // DrainRequest → DrainPreviewResponse
	OpSecretReveal    = "secret-reveal"      // ObjectRequest → SecretRevealResponse
)

// 流名（InternalStreamPath 之后的路径段）。
const (
	// StreamWatch WatchRequest。事件 change：data = WatchEvent（不含对象内容，合并为至多每秒一条）。
	StreamWatch = "watch"
	// StreamPodLog PodLogRequest。事件 log：id = 行游标，data = LogLine；事件 end：data = StreamEnd。
	StreamPodLog = "pod-log"
	// StreamWorkloadLog WorkloadLogRequest。事件同 StreamPodLog；另有事件 pods：data = LogPods。
	StreamWorkloadLog = "workload-log"
	// StreamDrain DrainRequest。事件 progress：data = DrainProgress；事件 end：data = StreamEnd。
	StreamDrain = "node-drain"
)

// SSE 事件名。
const (
	EventChange   = "change"
	EventLog      = "log"
	EventPods     = "pods"
	EventProgress = "progress"
	EventEnd      = "end"
	EventPing     = "ping"
)

// ErrorBody 内部接口的错误响应；core 据此构造带 reason 的业务错误。
type ErrorBody struct {
	Code    int    `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// 错误原因（与 core/api/k8s/v1 的 ErrorReason 同名，由 core 的守护测试比对）。
const (
	ReasonUnspecified     = "K8S_UNSPECIFIED"
	ReasonInvalidArgument = "K8S_INVALID_ARGUMENT"    // 400
	ReasonForbidden       = "K8S_FORBIDDEN"           // 403：集群拒绝（数据范围不足）
	ReasonNotFound        = "K8S_NOT_FOUND"           // 404
	ReasonConflict        = "K8S_CONFLICT"            // 409：对象已被他人修改 / 已存在
	ReasonRejected        = "K8S_REJECTED"            // 422：集群校验或准入拒绝
	ReasonClusterNotFound = "K8S_CLUSTER_NOT_FOUND"   // 404
	ReasonClusterOffline  = "K8S_CLUSTER_UNREACHABLE" // 503：集群不可达或未通过接入自检
	ReasonNotOwner        = "K8S_NOT_OWNER"           // 421：该实例不负责此集群，core 重新选实例后重试
	ReasonCacheSyncing    = "K8S_CACHE_SYNCING"       // 503：缓存建立中，稍后重试
	ReasonUnsupported     = "K8S_UNSUPPORTED"         // 400：该资源不支持此操作
	ReasonTooManyStreams  = "K8S_TOO_MANY_STREAMS"    // 429：并发日志流 / 终端会话超限
	ReasonExecDisabled    = "K8S_EXEC_DISABLED"       // 403：该集群禁止终端
	ReasonIdentity        = "K8S_IDENTITY_INVALID"    // 401：身份头缺失、签名错误或过期
	ReasonUpstream        = "K8S_UPSTREAM_ERROR"      // 502：集群返回了未归类的错误
	ReasonPlaneDown       = "K8S_PLANE_UNAVAILABLE"   // 503：没有可用的数据面实例（仅 core 产生）
)

// Empty 空请求 / 空响应。
type Empty struct{}

// Ref 定位一个资源对象。集群级资源 Namespace 为空；核心组 Group 为空。
type Ref struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
}

// ObjectRequest 针对单个对象的请求。
type ObjectRequest struct {
	Ref Ref `json:"ref"`
}

// ---- 集群 ----

// ClusterTestRequest 测试连接（未保存的直连配置）。ClusterID 非空且 Credential 为空时沿用已保存的凭据。
type ClusterTestRequest struct {
	ClusterID  string     `json:"clusterId,omitempty"`
	APIServer  string     `json:"apiServer"`
	ProxyURL   string     `json:"proxyUrl,omitempty"`
	CACert     string     `json:"caCert,omitempty"`
	Credential Credential `json:"credential"`
}

// ClusterTestResponse 测试结果。OK 为 false 时 Error 是集群或网络返回的原文；Checks 为接入自检明细。
type ClusterTestResponse struct {
	OK         bool        `json:"ok"`
	Version    string      `json:"version,omitempty"`
	DurationMS int64       `json:"durationMs"`
	Error      string      `json:"error,omitempty"`
	Checks     []SelfCheck `json:"checks,omitempty"`
}

// SelfCheck 接入自检的一项。
type SelfCheck struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// ---- 资源目录 ----

// APIResourcesRequest 读取资源目录；Refresh 为 true 时跳过 5 分钟缓存。
type APIResourcesRequest struct {
	Refresh bool `json:"refresh,omitempty"`
}

// APIResource 一种资源类型。
type APIResource struct {
	Group      string   `json:"group"`
	Version    string   `json:"version"`
	Resource   string   `json:"resource"`
	Kind       string   `json:"kind"`
	Namespaced bool     `json:"namespaced"`
	Verbs      []string `json:"verbs"`
	ShortNames []string `json:"shortNames,omitempty"`
	// Cached 该资源走服务端缓存（支持搜索、排序、总数、变更订阅）。
	Cached bool `json:"cached"`
	// CRD 来自自定义资源定义。
	CRD bool `json:"crd"`
}

// APIResourcesResponse 资源目录。
type APIResourcesResponse struct {
	Items []APIResource `json:"items"`
}

// NamespacesResponse 当前用户可见的命名空间。
type NamespacesResponse struct {
	Items []string `json:"items"`
	// All 用户可以选择「全部命名空间」。
	All bool `json:"all"`
}

// ---- 列表 ----

// ListRequest 资源列表。Namespace 为空 = 全部命名空间（集群级资源恒为空）。
// Keyword / Filters / SortBy / Page 只对走缓存的资源有效；其余资源用 Limit(PageSize) / Continue 翻页。
type ListRequest struct {
	Group         string `json:"group"`
	Version       string `json:"version"`
	Resource      string `json:"resource"`
	Namespace     string `json:"namespace,omitempty"`
	Keyword       string `json:"keyword,omitempty"`
	LabelSelector string `json:"labelSelector,omitempty"`
	// Filters 字段筛选：键见 Filter*；值含义随键而定。
	Filters  map[string]string `json:"filters,omitempty"`
	SortBy   string            `json:"sortBy,omitempty"`
	SortDesc bool              `json:"sortDesc,omitempty"`
	Page     int               `json:"page,omitempty"`
	PageSize int               `json:"pageSize,omitempty"`
	Continue string            `json:"continue,omitempty"`
}

// 列表的字段筛选键（ListRequest.Filters）。
const (
	FilterState = "state" // 状态：State* 之一
	FilterNode  = "node"  // Pod：所在节点
	FilterOwner = "owner" // 所有者对象的 UID（如某个工作负载下的 Pod、某个 CronJob 的 Job）
)

// 列类型（Column.Type），前端据此渲染单元格。
const (
	ColText   = "text"   // 字符串
	ColNumber = "number" // 数字
	ColState  = "state"  // StateCell
	ColTime   = "time"   // RFC 3339 时间；前端显示为相对时间
	ColImages = "images" // []string
	ColLabels = "labels" // map[string]string
	ColList   = "list"   // []string
)

// 统一状态（Row.State 与 StateCell.State）。
const (
	StateOK       = "ok"       // 正常
	StatePending  = "pending"  // 进行中
	StateAbnormal = "abnormal" // 异常
	StateUnknown  = "unknown"  // 未知
	StateDone     = "done"     // 已结束（Job 完成、Pod Succeeded）
)

// Column 列定义。
type Column struct {
	Key      string `json:"key"`
	Title    string `json:"title"`
	Type     string `json:"type"`
	Sortable bool   `json:"sortable,omitempty"`
	// Priority 0 = 默认展示；>0 = 次要列，默认隐藏（同 kubectl -o wide）。
	Priority int `json:"priority,omitempty"`
}

// StateCell ColState 列的单元格。
type StateCell struct {
	State string `json:"state"`
	Text  string `json:"text"`
	// Reason 异常时的原因说明（可选）。
	Reason string `json:"reason,omitempty"`
}

// Row 列表的一行。
type Row struct {
	Namespace  string    `json:"namespace,omitempty"`
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	CreateTime time.Time `json:"createTime"`
	// State 行的整体状态（没有状态概念的资源为空）。
	State string `json:"state,omitempty"`
	// Cells 列键 → 值。
	Cells map[string]any `json:"cells"`
}

// Capabilities 该列表支持的能力。
type Capabilities struct {
	Cached  bool `json:"cached"`
	Keyword bool `json:"keyword"`
	Sort    bool `json:"sort"`
	Total   bool `json:"total"`
	Watch   bool `json:"watch"`
	// Filters 支持的字段筛选键。
	Filters []string `json:"filters,omitempty"`
}

// ListResponse 列表响应。未缓存的资源 Total 为 -1，用 Continue 翻页。
type ListResponse struct {
	Columns      []Column     `json:"columns"`
	Rows         []Row        `json:"rows"`
	Total        int64        `json:"total"`
	Continue     string       `json:"continue,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
	// PartialNamespaces 请求全部命名空间但用户只有部分权限时，实际返回了数据的命名空间。
	PartialNamespaces []string `json:"partialNamespaces,omitempty"`
}

// ---- 对象 ----

// ObjectResponse 单个对象（以用户身份直接读自集群）。Secret 的 data 已掩码。
type ObjectResponse struct {
	Kind            string `json:"kind"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
	// Object 去掉 managedFields 的对象。
	Object map[string]any `json:"object"`
	// YAML 同一对象的 YAML 文本（编辑用）。
	YAML string `json:"yaml"`
}

// Event 一条 K8s 事件。
type Event struct {
	Type      string    `json:"type"`
	Reason    string    `json:"reason"`
	Message   string    `json:"message"`
	Count     int32     `json:"count"`
	FirstTime time.Time `json:"firstTime"`
	LastTime  time.Time `json:"lastTime"`
	Source    string    `json:"source,omitempty"`
	// Object 事件关联的对象，形如 Pod/name。
	Object string `json:"object,omitempty"`
}

// EventsResponse 对象的关联事件，最近的在前，至多 200 条。
type EventsResponse struct {
	Items []Event `json:"items"`
}

// ---- 变更 ----

// ApplyRequest 创建或更新单个对象。Ref.Name 为空 = 创建（名称取自 YAML）。
// ResourceVersion 为预览时读到的版本，更新时用于乐观锁；为空则不校验。
type ApplyRequest struct {
	Ref             Ref    `json:"ref"`
	YAML            string `json:"yaml"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

// PreviewResponse 预检结果（dryRun=All）：当前与变更后的 YAML，供前端做 Diff。
type PreviewResponse struct {
	Create          bool   `json:"create"`
	BeforeYAML      string `json:"beforeYaml"`
	AfterYAML       string `json:"afterYaml"`
	ResourceVersion string `json:"resourceVersion"`
	// Changed 变更后与当前是否有差异。
	Changed bool `json:"changed"`
}

// ApplyResponse 提交结果。
type ApplyResponse struct {
	Ref             Ref    `json:"ref"`
	Kind            string `json:"kind"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

// DeleteRequest 删除对象。UID 非空时作为前置条件，防止误删同名的重建对象。
type DeleteRequest struct {
	Ref Ref    `json:"ref"`
	UID string `json:"uid,omitempty"`
	// Propagation 级联策略：Foreground / Background / Orphan；空 = Background。
	Propagation string `json:"propagation,omitempty"`
}

// ManifestRequest 多文档 YAML。Namespace 为未写命名空间的对象提供缺省值。
type ManifestRequest struct {
	YAML      string `json:"yaml"`
	Namespace string `json:"namespace,omitempty"`
}

// MaxManifestDocs 多文档 YAML 的对象数上限。
const MaxManifestDocs = 50

// ManifestItem 多文档 YAML 中的一个对象的预检 / 提交结果。
type ManifestItem struct {
	Ref    Ref    `json:"ref"`
	Kind   string `json:"kind"`
	Create bool   `json:"create"`
	// Error 非空 = 该对象失败。
	Error      string `json:"error,omitempty"`
	BeforeYAML string `json:"beforeYaml,omitempty"`
	AfterYAML  string `json:"afterYaml,omitempty"`
	// Skipped 提交时因前面的对象失败而未执行。
	Skipped bool `json:"skipped,omitempty"`
}

// ManifestResponse 多文档 YAML 的结果。OK = 全部成功。
type ManifestResponse struct {
	OK    bool           `json:"ok"`
	Items []ManifestItem `json:"items"`
}

// ---- 快捷操作 ----

// ScaleRequest 伸缩（Deployment / StatefulSet / ReplicaSet）。
type ScaleRequest struct {
	Ref      Ref   `json:"ref"`
	Replicas int32 `json:"replicas"`
}

// SetImageRequest 更新某个容器的镜像。
type SetImageRequest struct {
	Ref       Ref    `json:"ref"`
	Container string `json:"container"`
	Image     string `json:"image"`
}

// ResizeRequest 原地调整运行中 Pod 的容器资源（Pod 的 resize 子资源，Kubernetes 1.33+；不重建 Pod）。
type ResizeRequest struct {
	Ref        Ref               `json:"ref"`
	Containers []ContainerResize `json:"containers"`
}

// ContainerResize 一个容器的目标资源；Requests / Limits 为空 = 不改。
type ContainerResize struct {
	Name     string          `json:"name"`
	Requests *ResizeQuantity `json:"requests,omitempty"`
	Limits   *ResizeQuantity `json:"limits,omitempty"`
}

// ResizeQuantity CPU 与内存的数量（如 500m、256Mi）。nil = 保持不变；不接受空串（不能借此删除已有的设置）。
type ResizeQuantity struct {
	CPU    *string `json:"cpu,omitempty"`
	Memory *string `json:"memory,omitempty"`
}

// Revision 工作负载的一个历史版本。
type Revision struct {
	Revision   int64     `json:"revision"`
	CreateTime time.Time `json:"createTime"`
	Images     []string  `json:"images"`
	Current    bool      `json:"current"`
	// ChangeCause kubernetes.io/change-cause 注解。
	ChangeCause string `json:"changeCause,omitempty"`
	// TemplateYAML 该版本的 Pod 模板。
	TemplateYAML string `json:"templateYaml"`
}

// RevisionsResponse 历史版本，新的在前。
type RevisionsResponse struct {
	Items []Revision `json:"items"`
}

// RollbackRequest 回滚到指定版本。
type RollbackRequest struct {
	Ref      Ref   `json:"ref"`
	Revision int64 `json:"revision"`
}

// PauseRequest 暂停 / 恢复发布（Deployment）。
type PauseRequest struct {
	Ref    Ref  `json:"ref"`
	Paused bool `json:"paused"`
}

// SuspendRequest 暂停 / 恢复 CronJob。
type SuspendRequest struct {
	Ref     Ref  `json:"ref"`
	Suspend bool `json:"suspend"`
}

// CordonRequest 停止 / 恢复调度。
type CordonRequest struct {
	Name          string `json:"name"`
	Unschedulable bool   `json:"unschedulable"`
}

// DrainRequest 排空节点。
type DrainRequest struct {
	Name string `json:"name"`
	// DeleteEmptyDir 允许驱逐使用 emptyDir 的 Pod（其数据会丢失）。
	DeleteEmptyDir bool `json:"deleteEmptyDir,omitempty"`
	// Force 允许驱逐没有控制器管理的 Pod（不会被重建）。
	Force bool `json:"force,omitempty"`
	// GracePeriodSeconds 优雅终止时间；<0 = 用 Pod 自己的设置。
	GracePeriodSeconds int64 `json:"gracePeriodSeconds,omitempty"`
}

// DrainPod 排空时涉及的一个 Pod。
type DrainPod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// Action evict = 将被驱逐；skip = 跳过（DaemonSet、静态 Pod、已结束）；block = 阻止排空。
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

// DrainPreviewResponse 排空预览。
type DrainPreviewResponse struct {
	Pods []DrainPod `json:"pods"`
	// Blocked 存在 Action = block 的 Pod，按当前选项无法排空。
	Blocked bool `json:"blocked"`
}

// DrainProgress 排空进度（StreamDrain 的 progress 事件）。
type DrainProgress struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// Status evicted / pending（受 PodDisruptionBudget 限制，重试中）/ failed。
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Done    int    `json:"done"`
	Total   int    `json:"total"`
}

// SecretRevealResponse Secret 的明文（已 base64 解码；非 UTF-8 的值以 base64 返回并在 Binary 中列出键）。
type SecretRevealResponse struct {
	Data   map[string]string `json:"data"`
	Binary []string          `json:"binary,omitempty"`
}

// ---- 流 ----

// StreamEnd 流正常结束时的最后一个事件。
type StreamEnd struct {
	// Reason max_duration（到达单连接时长上限，客户端应续接）/ completed / error。
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

// 流结束原因。
const (
	EndMaxDuration = "max_duration"
	EndCompleted   = "completed"
	EndError       = "error"
)

// WatchRequest 订阅某类资源的变更（仅走缓存的资源）。
type WatchRequest struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
}

// WatchEvent 变更通知：不含对象内容，前端收到后重新请求当前页。
type WatchEvent struct {
	Count int `json:"count"`
}

// PodLogRequest Pod 实时日志。
type PodLogRequest struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container,omitempty"`
	Previous  bool   `json:"previous,omitempty"`
	// TailLines 起始回看行数，缺省 DefaultTailLines，上限 MaxTailLines。
	TailLines    int64 `json:"tailLines,omitempty"`
	SinceSeconds int64 `json:"sinceSeconds,omitempty"`
	// Follow 是否持续跟随；false = 读完现有内容即结束（下载用）。
	Follow bool `json:"follow"`
	// Keyword 服务端关键字过滤（包含匹配）。
	Keyword string `json:"keyword,omitempty"`
	// Cursor 续接游标：上一次收到的最后一个 log 事件的 id。
	Cursor string `json:"cursor,omitempty"`
}

// WorkloadLogRequest 工作负载级日志：同时跟随其下的多个 Pod（至多 MaxWorkloadLogPods 个）。
type WorkloadLogRequest struct {
	Ref          Ref    `json:"ref"`
	Container    string `json:"container,omitempty"`
	TailLines    int64  `json:"tailLines,omitempty"`
	SinceSeconds int64  `json:"sinceSeconds,omitempty"`
	Keyword      string `json:"keyword,omitempty"`
	// Cursors Pod 名 → 续接游标。
	Cursors map[string]string `json:"cursors,omitempty"`
}

// 日志限制。
const (
	DefaultTailLines   = 500
	MaxTailLines       = 5000
	MaxLogLineBytes    = 16 << 10
	MaxWorkloadLogPods = 20
	MaxLogStreams      = 10 // 每用户并发日志流
	MaxExecSessions    = 5  // 每用户并发终端会话
)

// LogLine 一行日志。
type LogLine struct {
	// Pod / Container 工作负载级日志时标明来源。
	Pod       string    `json:"pod,omitempty"`
	Container string    `json:"container,omitempty"`
	Time      time.Time `json:"time"`
	Text      string    `json:"text"`
	// Truncated 该行超过 MaxLogLineBytes 被截断。
	Truncated bool `json:"truncated,omitempty"`
}

// LogPods 工作负载级日志实际跟随的 Pod（pods 事件）。
type LogPods struct {
	Pods []string `json:"pods"`
	// Omitted 因超过上限未跟随的 Pod 数。
	Omitted int `json:"omitted"`
}

// ---- 终端 ----

// 终端会话类型（k8s_exec_session.kind）。
const (
	ExecKindShell int32 = 1
	ExecKindDebug int32 = 2
)

// ExecParams 终端连接参数（k8s_exec_ticket.params；core 核销票据后传给数据面）。
type ExecParams struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container,omitempty"`
	// SessionID 非空 = 接回已有会话（仅会话的发起用户可接回）。
	SessionID string `json:"sessionId,omitempty"`
	Kind      int32  `json:"kind,omitempty"`
	// DebugImage 调试容器（Kind = ExecKindDebug）使用的镜像；须在数据面配置的允许清单内，空 = 清单的第一个。
	DebugImage string `json:"debugImage,omitempty"`
	Cols       uint16 `json:"cols,omitempty"`
	Rows       uint16 `json:"rows,omitempty"`
}

// 终端帧：WebSocket 二进制帧，首字节为类型，其余为载荷。浏览器 ↔ core ↔ 数据面全程同一格式。
const (
	FrameInput  byte = 0 // 客户端 → 服务端：键盘输入
	FrameOutput byte = 1 // 服务端 → 客户端：终端输出
	FrameResize byte = 2 // 客户端 → 服务端：JSON ExecResize
	FrameStatus byte = 3 // 服务端 → 客户端：JSON ExecStatus
	FramePing   byte = 4 // 双向心跳，无载荷
)

// ExecResize 窗口大小。
type ExecResize struct {
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

// ExecStatus 会话状态通知。
type ExecStatus struct {
	// State ready（会话就绪，带 SessionID）/ resumed（已接回，随后回放缓冲）/ closed。
	State     string `json:"state"`
	SessionID string `json:"sessionId,omitempty"`
	// Reason 结束原因（ExecEnd*），仅 State = closed。
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// 终端会话状态与结束原因（k8s_exec_session.end_reason）。
const (
	ExecReady   = "ready"
	ExecResumed = "resumed"
	ExecClosed  = "closed"

	ExecEndClientClose   = "client_close"
	ExecEndIdleTimeout   = "idle_timeout"
	ExecEndMaxDuration   = "max_duration"
	ExecEndDetachTimeout = "detach_timeout"
	ExecEndContainerExit = "container_exit"
	ExecEndError         = "error"
	ExecEndShutdown      = "shutdown"
)

// 终端限制。
const (
	ExecDetachGrace  = 60 * time.Second // 断线后会话保留时长
	ExecIdleTimeout  = 15 * time.Minute
	ExecMaxDuration  = 8 * time.Hour
	ExecReplayBytes  = 256 << 10 // 断线期间输出的环形缓冲
	ExecTicketTTL    = 30 * time.Second
	ExecMaxFrameSize = 64 << 10
)

// ---- 通知 ----

// 通知事件类型（k8s_notify_outbox.event_type）。
const (
	NotifyClusterUnreachable = "k8s.cluster.unreachable"
	NotifyClusterRecovered   = "k8s.cluster.recovered"
)

// ClusterNotifyPayload 集群连通性通知的内容体（内置模板读取这些字段）。
type ClusterNotifyPayload struct {
	ClusterID   string `json:"cluster_id"`
	ClusterName string `json:"cluster_name"`
	DisplayName string `json:"display_name"`
	Env         string `json:"env"`
	Error       string `json:"error,omitempty"`
	// Time 已格式化的时间文本。
	Time string `json:"time"`
}
