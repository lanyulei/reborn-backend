package k8scontract

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// 变更审批（k8s_change）
//
// 集群配置了审批用的工单模板后，写操作不立即执行：core 先向数据面要一份「变更计划」
// （OpChangePlan，以发起人身份读取现状并预检），把计划存为变更单并建一张工单；工单审批通过后，
// 负责该集群的数据面领取变更单，以发起人的身份执行。
//
// 冲突按「本次变更涉及的字段」判定：计划里记下这些字段在提交时的值（FieldCheck），执行时逐个与
// 现状比对，全部一致才执行；其余字段（含状态、自动扩缩容调整的副本数）在审批期间被改动不算冲突。
// 列表类字段作为一个整体比对（合并补丁对列表是整体替换）：只改一个容器的镜像而别人改了同一工作负载
// 的另一个容器，会被判为冲突——偏保守，但不会覆盖他人的改动。
// ---------------------------------------------------------------------------

// OpChangePlan 生成变更计划：ChangePlanRequest → ChangePlanResponse（以用户身份；只读 + dry-run）。
const OpChangePlan = "change-plan"

// 变更类型（k8s_change.kind）。
const (
	ChangeKindOp         int32 = 1 // 一次写操作（对象变更 / 快捷操作 / Helm）
	ChangeKindNamespace  int32 = 2 // 申请命名空间与配额（以平台身份执行：审批即授权）
	ChangeKindPermission int32 = 3 // 申请集群权限（由 core 执行：写入数据范围）
)

// 变更单状态（k8s_change.status）。
const (
	ChangePending   int32 = 1 // 待审批
	ChangeApproved  int32 = 2 // 已批准，待执行
	ChangeExecuting int32 = 3 // 执行中（数据面已领取）
	ChangeSucceeded int32 = 4 // 执行成功
	ChangeFailed    int32 = 5 // 执行失败
	ChangeRejected  int32 = 6 // 审批未通过（驳回 / 流程被终止）
	ChangeCancelled int32 = 7 // 发起人已撤回
	ChangeExpired   int32 = 8 // 批准后超过有效期未执行
)

// ChangeFinal 报告状态是否为终态。
func ChangeFinal(status int32) bool { return status >= ChangeSucceeded }

// 变更单的限制。
const (
	// DefaultChangeTTL 批准后的有效期：超过即作废，不再执行。
	DefaultChangeTTL = 24 * time.Hour
	// ChangeLease 数据面领取变更单后的租约：执行器崩溃后过期可被重新领取。
	ChangeLease = 10 * time.Minute
	// ChangeMaxAttempts 提交时遇到并发修改（409）后从比对重新开始的次数上限。
	ChangeMaxAttempts = 3
	// MaxChangeMessage 执行结果说明的长度上限。
	MaxChangeMessage = 2000
)

// 通知事件类型：变更执行结果（发给发起人）。
const (
	NotifyChangeSucceeded = "k8s.change.succeeded"
	NotifyChangeFailed    = "k8s.change.failed"
)

// EmergencyHeader 浏览器 → core：持有免审批权限的用户在需审批的集群上直接执行（紧急变更）。
// 只是「请求免审批」的意愿；是否允许由 core 按接口授权判定。
const EmergencyHeader = "X-K8s-Emergency"

// 错误原因（与 core/api/k8s/v1 的 ErrorReason 同名）。
const (
	// ReasonChangeSubmitted 不是失败：该集群的变更需审批，已生成变更单与工单（HTTP 428，
	// metadata：change_id / ticket_id / ticket_number）。仅 core 产生。
	ReasonChangeSubmitted = "K8S_CHANGE_SUBMITTED"
)

// writeOps 改动集群的操作（一元）；writeStreams 改动集群的流式操作。需审批的集群上它们都生成变更单。
var (
	writeOps = map[string]struct{}{
		OpApply: {}, OpDelete: {}, OpManifestApply: {}, OpScale: {}, OpRestart: {}, OpSetImage: {}, OpRollback: {},
		OpPause: {}, OpCronTrigger: {}, OpCronSuspend: {}, OpPodEvict: {}, OpPodResize: {}, OpNodeCordon: {},
		OpHelmInstall: {}, OpHelmUpgrade: {}, OpHelmRollback: {}, OpHelmUninstall: {},
	}
	writeStreams = map[string]struct{}{StreamDrain: {}}
)

// IsWriteOp 报告一元操作是否改动集群。
func IsWriteOp(op string) bool {
	_, ok := writeOps[op]
	return ok
}

// IsWriteStream 报告流式操作是否改动集群。
func IsWriteStream(stream string) bool {
	_, ok := writeStreams[stream]
	return ok
}

// ChangePlanRequest 为一次写操作生成变更计划。Op 为一元操作名或流名（node-drain）。
type ChangePlanRequest struct {
	Op   string          `json:"op"`
	Body json.RawMessage `json:"body"`
}

// ChangePlanResponse 变更计划：Display 给人看（已掩码，明文落库），Exec 给执行器用
// （可能含 Secret 内容与 Helm values：core 加密后落库，只有数据面解密）。
type ChangePlanResponse struct {
	Display ChangeDisplay `json:"display"`
	Exec    ChangeExec    `json:"exec"`
}

// ChangeDisplay 变更内容的展示形态（k8s_change.display）。
type ChangeDisplay struct {
	// Action 动作（审计动作常量，如 update / scale / helm_upgrade）。
	Action string `json:"action"`
	// Target 主要目标对象；多对象变更时为第一个。
	Target Ref    `json:"target"`
	Kind   string `json:"kind"`
	// Summary 一句话说明（如「副本数 2 → 5」）。
	Summary string              `json:"summary"`
	Items   []ChangeItemDisplay `json:"items"`
}

// ChangeItemDisplay 变更涉及的一个对象。
type ChangeItemDisplay struct {
	Ref  Ref    `json:"ref"`
	Kind string `json:"kind"`
	// Action create / update / delete，或快捷操作的动作名。
	Action string `json:"action"`
	// BeforeYAML / AfterYAML 提交时的对象与预检后的对象（Secret 已掩码）；快捷操作为空。
	BeforeYAML string `json:"beforeYaml,omitempty"`
	AfterYAML  string `json:"afterYaml,omitempty"`
	// Fields 本次变更涉及的字段（执行时逐个与现状比对）。
	Fields []ChangeField `json:"fields"`
}

// ChangeField 一个涉及的字段：Before / After 为 JSON 文本（Secret 的值已掩码；过长截断）。
type ChangeField struct {
	Path   string `json:"path"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// 变更计划中一个对象的执行方式（ChangeExecItem.Mode）。
const (
	ChangeModeCreate = "create" // 创建 Object；执行时对象必须仍不存在
	ChangeModePatch  = "patch"  // 把 Patch 合并到现状后更新；执行前 Checks 须全部一致
	ChangeModeCheck  = "check"  // 只比对（UID / Checks），实际动作由重放 Op 完成
)

// ChangeExec 变更计划的执行形态（k8s_change.exec，加密）。
type ChangeExec struct {
	// Op / Body 原始请求。对象变更（apply / manifest-apply）按 Items 执行；其余在比对通过后重放 Op。
	Op    string           `json:"op"`
	Body  json.RawMessage  `json:"body"`
	Items []ChangeExecItem `json:"items"`
	// Helm Helm 变更的前置条件。
	Helm *HelmCheck `json:"helm,omitempty"`
	// Namespace 申请命名空间（ChangeKindNamespace）的内容。
	Namespace *NamespaceRequest `json:"namespace,omitempty"`
	// ChangeID / ClusterID 计划所属的变更单与集群：数据面解密后核对，非空而不符即拒绝执行
	// （防止一份加密的计划被挪到另一张变更单或另一个集群上）。core 在封存前填入；为空则不核对（旧数据）。
	ChangeID  string `json:"changeId,omitempty"`
	ClusterID string `json:"clusterId,omitempty"`
}

// ChangeExecItem 一个对象的执行步骤。
type ChangeExecItem struct {
	Ref  Ref    `json:"ref"`
	Kind string `json:"kind"`
	Mode string `json:"mode"`
	// Object 待创建的对象（ChangeModeCreate）。
	Object map[string]any `json:"object,omitempty"`
	// Patch JSON Merge Patch：提交时的对象 → 期望的对象（ChangeModePatch）。
	Patch map[string]any `json:"patch,omitempty"`
	// Checks 涉及的字段在提交时的值。
	Checks []FieldCheck `json:"checks,omitempty"`
	// UID 提交时对象的 UID：非空则执行时必须一致（对象未被删除后重建）。
	UID string `json:"uid,omitempty"`
	// SkipDryRun 所在命名空间由同一份清单创建，无法预检。
	SkipDryRun bool `json:"skipDryRun,omitempty"`
}

// FieldCheck 一个字段在提交时的值。Path 的每一段是对象的键；形如 "name=web" 的段在列表里按 name 选元素。
type FieldCheck struct {
	Path []string `json:"path"`
	// Value 提交时的值；Absent 为 true 表示当时不存在。
	Value  any  `json:"value,omitempty"`
	Absent bool `json:"absent,omitempty"`
}

// HelmCheck Helm 变更的前置条件：Release 的当前版本号须仍为 Revision（0 = Release 必须仍不存在）。
type HelmCheck struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Revision  int    `json:"revision"`
	// ChartDigest 生成计划时 Chart 包的 SHA-256（十六进制）；执行时下载到的包须与之一致（安装 / 升级）。
	ChartDigest string `json:"chartDigest,omitempty"`
	// ManifestHash 生成计划时渲染清单的 SHA-256；执行时重新渲染的结果须与之一致。为空 = 该 Chart 每次
	// 渲染结果不同（模板里有随机数等），无法固定。
	ManifestHash string `json:"manifestHash,omitempty"`
}

// NamespaceRequest 申请命名空间与配额。执行器只会创建 Namespace、ResourceQuota、LimitRange 这三种对象，
// 且都由这里的结构化字段生成——申请人无法借此创建其他资源。
type NamespaceRequest struct {
	Name string `json:"name"`
	// Labels 命名空间的标签，可选。
	Labels map[string]string `json:"labels,omitempty"`
	// Quota ResourceQuota 的 hard：仅允许 NamespaceQuotaKeys 中的键，值为 K8s 数量；空 = 不建配额。
	Quota map[string]string `json:"quota,omitempty"`
	// LimitDefault / LimitDefaultRequest LimitRange 的容器缺省 limits / requests：仅 cpu、memory；都空 = 不建。
	LimitDefault        map[string]string `json:"limitDefault,omitempty"`
	LimitDefaultRequest map[string]string `json:"limitDefaultRequest,omitempty"`
}

// NamespaceQuotaKeys 申请命名空间时允许设置的配额项。
var NamespaceQuotaKeys = map[string]struct{}{
	"requests.cpu": {}, "requests.memory": {}, "limits.cpu": {}, "limits.memory": {}, "pods": {},
	"requests.storage": {}, "persistentvolumeclaims": {}, "services": {}, "configmaps": {}, "secrets": {},
}

// NamespaceLimitKeys LimitRange 缺省值允许的资源。
var NamespaceLimitKeys = map[string]struct{}{"cpu": {}, "memory": {}}

// PermissionRequest 申请集群权限（ChangeKindPermission；k8s_change.display 之外另存于 params）。
type PermissionRequest struct {
	// Namespace 命名空间；* = 全部命名空间。
	Namespace string `json:"namespace"`
	// Level 档位 1–3（集群管理员不可自助申请）。
	Level int32 `json:"level"`
	// Reason 申请理由。
	Reason string `json:"reason,omitempty"`
}

// ChangeResult 执行结果（k8s_change.result）。
type ChangeResult struct {
	// Applied 已执行的对象（多对象变更中途失败时据此判断做到了哪一步）。
	Applied []Ref `json:"applied,omitempty"`
	// Attempts 因并发修改重试的次数。
	Attempts int `json:"attempts,omitempty"`
	// Started 已经开始向集群提交（预检与比对都已通过）。执行在此之后中断（进程退出、租约过期）的变更单
	// 不再自动重跑：它可能已部分或全部生效，重跑会让不可重入的操作（触发任务、驱逐、Helm）执行两次。
	Started bool `json:"started,omitempty"`
}

// ChangeRequester 发起人在批准时刻的身份（k8s_change.requester；数据面据此以发起人身份执行）。
type ChangeRequester struct {
	UserID   string   `json:"userId"`
	Username string   `json:"username"`
	RoleIDs  []string `json:"roleIds"`
	IsAdmin  bool     `json:"isAdmin"`
}
