package biz

import (
	"encoding/json"
	"time"
)

// Cluster 集群登记（k8s_cluster）与其运行状态。恒不含凭据。
type Cluster struct {
	ID            string
	Name          string
	DisplayName   string
	Env           string
	AccessMode    int32
	AgentLocation int32
	APIServer     string
	ProxyURL      string
	CACert        string
	ExecDisabled  bool
	Description   string
	Enabled       bool
	// DatasourceID 关联的监控数据源（空 = 未配置）；EventReasons 上报到通知的事件原因（空 = 缺省清单）。
	DatasourceID string
	EventReasons []string
	// ApprovalTemplateID 变更审批所用的工单模板（空 = 变更不需审批）。
	ApprovalTemplateID string
	HasCredential      bool
	Runtime            ClusterRuntime
	// AgentTokens 已签发的长期 Agent 令牌数。
	AgentTokens int32
	Version     int32
	CreateTime  time.Time
	UpdateTime  time.Time
}

// ClusterRuntime 集群运行状态（k8s_cluster_runtime，数据面维护）。
type ClusterRuntime struct {
	Status        int32
	StatusSince   *time.Time
	K8sVersion    string
	NodeTotal     int32
	NodeReady     int32
	AgentVersion  string
	AgentLastSeen *time.Time
	LastError     string
	ProbeTime     *time.Time
	ScopeError    string
	// ScopeSynced 当前的数据范围配置已同步到集群。
	ScopeSynced bool
}

// ClusterInput 集群的可编辑内容。
type ClusterInput struct {
	Name          string
	DisplayName   string
	Env           string
	AccessMode    int32
	AgentLocation int32
	APIServer     string
	ProxyURL      string
	CACert        string
	ExecDisabled  bool
	Description   string
	Enabled       bool
	DatasourceID  string
	EventReasons  []string
	// ApprovalTemplateID 变更审批所用的工单模板（空 = 变更不需审批）。
	ApprovalTemplateID string
}

// CredentialInput 直连凭据（明文，只在请求处理期间存在）。
type CredentialInput struct {
	Token      string
	ClientCert string
	ClientKey  string
}

// empty 报告是否未提供任何凭据。
func (c *CredentialInput) empty() bool {
	return c == nil || (c.Token == "" && c.ClientCert == "" && c.ClientKey == "")
}

// ClusterFilter 集群列表过滤条件；指针为 nil 表示不过滤。
type ClusterFilter struct {
	Keyword string
	Env     *string
	Status  *int32
}

// ClusterOption 集群切换器的一项。
type ClusterOption struct {
	ID           string
	Name         string
	DisplayName  string
	Env          string
	Status       int32
	LastError    string
	ExecDisabled bool
	K8sVersion   string
	// Level 当前用户在该集群上的最高档位。
	Level int32
	// ApprovalRequired 该集群的变更需审批。
	ApprovalRequired bool
}

// RoleScope 角色数据范围的一条（k8s_role_scope）。
type RoleScope struct {
	ClusterID   string
	ClusterName string
	Namespace   string
	Level       int32
}

// AuditEntry core 自己写入的一条审计（集群与授权的配置变更）。Detail 不得含凭据与令牌。
type AuditEntry struct {
	UserID    string
	Username  string
	ClientIP  string
	RequestID string
	Resource  string
	Action    string
	// Detail 动作参数（落 patch 列）。
	Detail json.RawMessage
}

// Audit 一条审计记录（k8s_audit）。对象内容只在详情里读出。
type Audit struct {
	ID          string
	CreateTime  time.Time
	UserID      string
	Username    string
	ClientIP    string
	ClusterID   string
	ClusterName string
	Namespace   string
	APIGroup    string
	APIVersion  string
	Resource    string
	Kind        string
	Name        string
	Action      string
	Result      int32
	Error       string
	Emergency   bool
	ChangeID    string
	Truncated   bool
	BeforeObj   json.RawMessage
	AfterObj    json.RawMessage
	Patch       json.RawMessage
}

// AuditFilter 审计列表过滤条件；Start / End 已由用例规整为有效区间。
type AuditFilter struct {
	Start, End time.Time
	ClusterID  string
	Namespace  string
	Resource   string
	Name       string
	Action     string
	UserID     string
	Result     *int32
	Keyword    string
	// Access 调用者的数据范围（用例填入）。
	Access Access
}

// ExecSession 终端会话记录（k8s_exec_session）。
type ExecSession struct {
	ID          string
	UserID      string
	Username    string
	ClientIP    string
	ClusterID   string
	ClusterName string
	Namespace   string
	Pod         string
	Container   string
	Kind        int32
	StartTime   time.Time
	EndTime     *time.Time
	EndReason   string
	BytesIn     int64
	BytesOut    int64
	Recorded    bool
}

// ExecSessionFilter 终端会话过滤条件。
type ExecSessionFilter struct {
	Start, End time.Time
	ClusterID  string
	UserID     string
	Keyword    string
	// Access 调用者的数据范围（用例填入）。
	Access Access
}

// Caller 发起请求的登录用户与来源（service 层从传输上下文提取）。
type Caller struct {
	UserID   string
	ClientIP string
	// Emergency 调用者请求免审批直接执行（紧急变更）；是否允许由变更审批按接口授权判定。
	Emergency bool
}

// AgentInstall 生成的 Agent 安装物。
type AgentInstall struct {
	Location   int32
	Manifest   string
	Command    string
	ExpireTime time.Time
}

// OutboxItem 通知发件箱的一行（k8s_notify_outbox）。
type OutboxItem struct {
	ID         string
	ClusterID  string
	EventType  string
	Severity   int32
	DedupKey   string
	GroupKey   string
	Recipients []string
	Payload    json.RawMessage
	Attempts   int32
}
