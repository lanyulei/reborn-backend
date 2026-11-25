// Package k8scontract 是 core 管理面（k8s 域）、数据面（backend/k8s）与 Agent 之间的数据契约：
// 三者互不 import 业务代码，共享的只有 k8s_* 表「落库长什么样」（jsonb 列结构、状态与枚举字面量），
// core → 数据面的内部接口（路径、请求 / 响应结构、身份头的签名与校验），数据面 ↔ Agent 的隧道协议，
// 以及各方必须算出同一结果的纯函数（令牌、档位到集群角色的映射、用户与分组的命名）。
// 任何一方改契约必须改这里，由编译器把漂移点全部暴露。本包不 import 任何驱动、传输层与 client-go，
// biz 层可用。
//
// 表结构见 core/migrations/0072_k8s_init.up.sql；设计见 docs/k8s-tech-design.md。
package k8scontract

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
)

// 接入方式（k8s_cluster.access_mode）。
const (
	AccessAgent  int32 = 1 // Agent 反向隧道
	AccessDirect int32 = 2 // 直连
)

// Agent 部署位置（k8s_cluster.agent_location）。
const (
	AgentLocationNone      int32 = 0 // 不适用（直连）
	AgentLocationInCluster int32 = 1 // 集群内
	AgentLocationRelay     int32 = 2 // 转发节点
)

// 集群状态（k8s_cluster_runtime.status）。
const (
	ClusterUnknown     int32 = 0
	ClusterOnline      int32 = 1
	ClusterUnreachable int32 = 2
)

// 配置版本名（k8s_cfg_version.name）。
const (
	CfgCluster    = "k8s_cluster"
	CfgRoleScope  = "k8s_role_scope"
	CfgAgentToken = "k8s_agent_token" //nolint:gosec // 配置版本表里的键名，不是凭据
)

// EnvProd 生产环境的取值：危险操作需输入资源名确认。
const EnvProd = "prod"

// MinServerMinor 平台支持的最低 K8s 次版本（1.32）：终端只实现 WebSocket 协议（v5.channel.k8s.io）。
const MinServerMinor = 32

// Credential 直连集群凭据的明文结构（k8s_cluster.credential 解密后）。Token 与客户端证书二选一。
type Credential struct {
	Token      string `json:"token,omitempty"`
	ClientCert string `json:"clientCert,omitempty"`
	ClientKey  string `json:"clientKey,omitempty"`
}

// ---------------------------------------------------------------------------
// 数据范围：档位、集群内的身份与对象命名
// ---------------------------------------------------------------------------

// 档位（k8s_role_scope.level）。
const (
	LevelView         int32 = 1 // 只读
	LevelEdit         int32 = 2 // 开发
	LevelAdmin        int32 = 3 // 命名空间管理员
	LevelClusterAdmin int32 = 4 // 集群管理员
)

// AllNamespaces k8s_role_scope.namespace 中表示全部命名空间的取值。
const AllNamespaces = "*"

// 集群内由平台管理的对象。
const (
	// ManagedByLabel / ManagedByValue 平台创建的 RBAC 对象都带此标签；授权对账只增删改带此标签的对象。
	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagedByValue = "reborn"
	// ClusterViewRole 平台创建的 ClusterRole：集群级资源（节点、命名空间、PV、存储类、CRD 等）只读。
	// 内置的 view 不含这些资源，没有它低档位用户看不到节点列表与 CRD 目录。
	ClusterViewRole = "reborn:cluster-view"
	// AdminGroup 超管所在分组，绑定 cluster-admin。
	AdminGroup = "reborn:admin"
	// userPrefix / roleGroupPrefix 集群内的用户名与分组名前缀。
	userPrefix      = "reborn:"
	roleGroupPrefix = "reborn:role:"
	// BindingPrefix 平台创建的 RoleBinding / ClusterRoleBinding 的名称前缀。
	BindingPrefix = "reborn:"
)

// LevelClusterRole 档位对应的 K8s 内置 ClusterRole；非法档位返回空串。
func LevelClusterRole(level int32) string {
	switch level {
	case LevelView:
		return "view"
	case LevelEdit:
		return "edit"
	case LevelAdmin:
		return "admin"
	case LevelClusterAdmin:
		return "cluster-admin"
	}
	return ""
}

// ImpersonateUser 平台用户在集群内的用户名（Impersonate-User）。
func ImpersonateUser(userID string) string { return userPrefix + userID }

// RoleGroup 平台角色在集群内的分组名（Impersonate-Group；RoleBinding 的主体）。
func RoleGroup(roleID string) string { return roleGroupPrefix + roleID }

// ImpersonateGroups 用户在集群内的全部分组：每个启用角色一个，超管另加 AdminGroup，
// 以及 system:authenticated（impersonate 时集群不会自动补上）。
func ImpersonateGroups(roleIDs []string, isAdmin bool) []string {
	out := make([]string, 0, len(roleIDs)+2)
	for _, id := range roleIDs {
		out = append(out, RoleGroup(id))
	}
	if isAdmin {
		out = append(out, AdminGroup)
	}
	return append(out, "system:authenticated")
}

// ---------------------------------------------------------------------------
// 审计
// ---------------------------------------------------------------------------

// 审计结果（k8s_audit.result）。
const (
	AuditOK     int32 = 1
	AuditFailed int32 = 2
)

// 审计动作（k8s_audit.action）。
const (
	ActionCreate       = "create"
	ActionUpdate       = "update"
	ActionDelete       = "delete"
	ActionScale        = "scale"
	ActionRestart      = "restart"
	ActionSetImage     = "set_image"
	ActionRollback     = "rollback"
	ActionPause        = "pause"
	ActionResume       = "resume"
	ActionTrigger      = "trigger"
	ActionSuspend      = "suspend"
	ActionUnsuspend    = "unsuspend"
	ActionEvict        = "evict"
	ActionResize       = "resize"
	ActionCordon       = "cordon"
	ActionUncordon     = "uncordon"
	ActionDrain        = "drain"
	ActionSecretReveal = "secret_reveal"
	ActionExec         = "exec"
	ActionAgentInstall = "agent_install"
	ActionAgentRevoke  = "agent_revoke"
	ActionScopeReplace = "scope_replace"
)

// 平台配置变更在审计里的资源类型（k8s_audit.resource）。
const (
	AuditResourceCluster    = "cluster"
	AuditResourceRoleScope  = "role-scope"
	AuditResourceAgentToken = "agent-token"
)

// MaxAuditObjectBytes 审计里单个对象的大小上限，超过则置空并标记 truncated。
const MaxAuditObjectBytes = 256 << 10

// MaskedValue 掩码替换值（Secret 的 data、审计对象）。
const MaskedValue = "******"

// ---------------------------------------------------------------------------
// Agent 接入令牌：<令牌 ID>.<随机密钥>；库里只存密钥的 SHA-256（k8s_agent_token）
// ---------------------------------------------------------------------------

// 令牌类型（k8s_agent_token.kind）。
const (
	TokenRegister int32 = 1 // 注册令牌：一次性，24 小时有效
	TokenLongTerm int32 = 2 // 长期令牌
)

// MaxLongTermTokens 每个集群的长期令牌上限（转发节点主备各一）。
const MaxLongTermTokens = 2

// NewToken 为给定的令牌 ID 生成令牌，返回明文与应落库的摘要。
func NewToken(tokenID string) (token string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	secret := base64.RawURLEncoding.EncodeToString(buf)
	return tokenID + "." + secret, HashSecret(secret), nil
}

// ParseToken 拆出令牌里的令牌 ID 与密钥；格式不对返回 ok = false。
func ParseToken(token string) (tokenID, secret string, ok bool) {
	tokenID, secret, ok = strings.Cut(token, ".")
	if !ok || !canonicalUUID(tokenID) || len(secret) < 32 || len(secret) > 128 {
		return "", "", false
	}
	return tokenID, secret, true
}

// canonicalUUID 报告 s 是否为规范形式的 UUID（8-4-4-4-12，小写十六进制）。令牌 ID 会被用作数据库的
// uuid 参数与进程内限速 / 缓存的键：不是 UUID 会让查询报错（未认证的入口因此返回 500），大小写变体
// 则在数据库里等价、在缓存键上不等价（可借此绕开按令牌的限速）。
func canonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
	}
	return true
}

// HashSecret 密钥的摘要（k8s_agent_token.token_hash、k8s_exec_ticket.token_hash）。
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// SecretMatches 常数时间比较密钥与落库摘要。
func SecretMatches(secret string, hash []byte) bool {
	return subtle.ConstantTimeCompare(HashSecret(secret), hash) == 1
}

// NewTicket 生成一次性票据（终端），返回明文与应落库的摘要。
func NewTicket() (ticket string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	ticket = base64.RawURLEncoding.EncodeToString(buf)
	return ticket, HashSecret(ticket), nil
}

// ---------------------------------------------------------------------------
// 数据面 ↔ Agent 隧道协议
//
// 注册：POST AgentRegisterPath，JSON AgentRegisterRequest → AgentRegisterResponse。
// 连接：GET AgentConnectPath（WebSocket），Authorization: Bearer <长期令牌>，
//       AgentVersionHeader 带 Agent 版本。连接建立后在其上跑 yamux：数据面是 yamux 客户端
//       （只有它能开流），Agent 是 yamux 服务端。每条流是一条到该集群 API Server 的明文 HTTP/1.1
//       连接——Agent 把流里的请求反向代理到启动时配置的唯一地址，去掉入站的 Authorization，
//       换上自己的凭据，保留 Impersonate-* 头。
// ---------------------------------------------------------------------------

// Agent 协议路径与头。
const (
	AgentRegisterPath  = "/agent/v1/register"
	AgentConnectPath   = "/agent/v1/connect"
	AgentVersionHeader = "X-Reborn-Agent-Version"
	// AgentSubprotocol 隧道 WebSocket 的子协议名。
	AgentSubprotocol = "reborn.k8s.tunnel.v1"
)

// AgentRegisterRequest 注册请求：用一次性的注册令牌换取长期令牌。
type AgentRegisterRequest struct {
	Token   string `json:"token"`
	Version string `json:"version"`
}

// AgentRegisterResponse 注册响应。
type AgentRegisterResponse struct {
	// Token 长期令牌明文，只返回这一次。
	Token string `json:"token"`
	// ClusterName 集群名称（Agent 仅用于日志）。
	ClusterName string `json:"clusterName"`
}
