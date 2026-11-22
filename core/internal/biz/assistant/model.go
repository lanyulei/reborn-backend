// Package biz 智能助手域业务核心：会话/消息/工具调用领域模型、仓储与外部
// 能力接口（依赖倒置：模型客户端与回环执行器在 data 层实现）、Agent 用例。
// 本包零驱动、零传输层、零反射：工具目录由 service 层从 proto 描述符生成后
// 以纯数据（ToolSpec）注入。
package biz

import (
	"context"
	"encoding/json"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 工具调用状态常量，与库表 assistant_tool_call.status 契约对齐。
const (
	// ToolStatusPending 写操作等待用户确认。
	ToolStatusPending = "pending"
	// ToolStatusApproved 用户已确认，执行中。
	ToolStatusApproved = "approved"
	// ToolStatusRejected 用户拒绝执行。
	ToolStatusRejected = "rejected"
	// ToolStatusDenied 回环接口返回 403（无权限 / 越权）。
	ToolStatusDenied = "denied"
	// ToolStatusSucceeded 接口调用成功（2xx）。
	ToolStatusSucceeded = "succeeded"
	// ToolStatusFailed 接口调用失败（非 2xx 非 403 / 网络错误 / 参数非法）。
	ToolStatusFailed = "failed"
	// ToolStatusCancelled 轮次被取消或用户发送新消息放弃了确认。
	ToolStatusCancelled = "cancelled"
	// ToolStatusCaptured 无人值守干跑：写操作未执行，只记录了拟调用参数（与「未调用」「失败」区分）。
	ToolStatusCaptured = "captured"
)

// 会话类型：用户对话对本人可见；Agent 运行的会话归机器账号，不出现在任何人的会话列表里，
// 只经运维 Agent 域的运行详情回放。
const (
	ConversationKindUser  int32 = 1
	ConversationKindAgent int32 = 2
)

// 消息角色常量，与模型对话协议一致。
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
	RoleTool      = "tool"
)

// 轮次结束状态（done 事件）。
const (
	DoneCompleted            = "completed"
	DoneAwaitingConfirmation = "awaiting_confirmation"
	DoneCancelled            = "cancelled"
)

// 哨兵错误：reason 来自 api/assistant/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrDisabled               = errors.ServiceUnavailable(v1.ErrorReason_ASSISTANT_DISABLED.String(), "assistant is not configured")
	ErrConversationNotFound   = errors.NotFound(v1.ErrorReason_ASSISTANT_CONVERSATION_NOT_FOUND.String(), "conversation not found")
	ErrRunBusy                = errors.Conflict(v1.ErrorReason_ASSISTANT_RUN_BUSY.String(), "conversation has a run in progress")
	ErrConfirmationNotPending = errors.Conflict(v1.ErrorReason_ASSISTANT_CONFIRMATION_NOT_PENDING.String(), "no pending confirmation")
	ErrLLMUnavailable         = errors.ServiceUnavailable(v1.ErrorReason_ASSISTANT_LLM_UNAVAILABLE.String(), "model service unavailable")
	// ErrRunLimited 并发名额已满（单用户或单实例）：与"本会话已有轮次在进行"同一 reason 与状态码
	//（对外契约不变），仅文案区分，便于用户与排查者分辨。
	ErrRunLimited = errors.Conflict(v1.ErrorReason_ASSISTANT_RUN_BUSY.String(), "too many assistant runs in progress, please retry later")
	// ErrLLMRejected 模型接口拒绝了请求本身（4xx：上下文过长、历史结构不被接受等），重试同一请求无意义。
	ErrLLMRejected = errors.ServiceUnavailable(v1.ErrorReason_ASSISTANT_LLM_UNAVAILABLE.String(), "模型拒绝了本次请求（可能是对话过长），请新建会话后重试")
	// ErrSessionExpired 轮次进行中访问令牌失效：后续工具调用都会 401，直接结束本轮。
	ErrSessionExpired = errors.Unauthorized("TOKEN_EXPIRED", "登录状态已失效，请重新登录后继续")
	// ErrContextUnsupported 请求携带页内上下文但未装配任何上下文提供者。
	ErrContextUnsupported = errors.BadRequest(v1.ErrorReason_ASSISTANT_UNSPECIFIED.String(), "page context is not supported")
)

// clientIPKey 发起本轮对话的客户端 IP 在 ctx 里的键。
type clientIPKey struct{}

// WithClientIP 把发起对话的客户端 IP 放进 ctx：回环执行器据此透传 X-Forwarded-For，
// 使经助手执行的操作在既有审计里记录真实来源而不是 127.0.0.1。
func WithClientIP(ctx context.Context, ip string) context.Context {
	if ip == "" {
		return ctx
	}
	return context.WithValue(ctx, clientIPKey{}, ip)
}

// ClientIPFromContext 取 WithClientIP 放入的客户端 IP；没有则为空串。
func ClientIPFromContext(ctx context.Context) string {
	ip, _ := ctx.Value(clientIPKey{}).(string)
	return ip
}

// ToolSpec 是一个可供模型调用的工具（= 本服务一个 RPC）的纯数据描述，由
// service 层从 proto 描述符生成（启动期一次），biz 运行期只读、不反射。
type ToolSpec struct {
	// Name 工具名 <Service>_<Method>，满足模型侧 ^[a-zA-Z0-9_-]{1,64}$。
	Name string
	// Module 所属功能模块（proto 包名首段：cmdb / notify / oncall / system），按模块加载工具用。
	Module string
	// Operation proto full method（/pkg.Service/Method），审计与日志用。
	Operation string
	// Code 授权 code（md5(Operation)），与 system_permission 同口径，权限过滤用。
	Code string
	// Title 接口中文名（system.v1.title 注解）。
	Title string
	// Description 给模型的说明（title + HTTP 方法与路径）。
	Description string
	// Write 是否写操作（非 GET 且未标记为只读预览）；写操作执行前须用户确认。
	Write bool
	// Resident 常驻工具：工具按模块懒加载时也始终下发（跨模块的通用能力，如知识库检索）。
	Resident bool
	// HTTPMethod 回环调用的 HTTP 动词。
	HTTPMethod string
	// PathTemplate 回环路径模板（含 {field} 占位与自定义动词后缀）。
	PathTemplate string
	// PathFields 路径占位对应的入参 JSON 字段名（protojson 名），按出现顺序。
	PathFields []string
	// BodyAll 为 true 时整个入参作为 JSON body；否则非路径字段编码为 query。
	BodyAll bool
	// InputSchema 入参 JSON Schema（object），供模型生成参数。
	InputSchema json.RawMessage
}

// ToolCatalog 工具目录（全集，未按用户过滤）；由 service 层构造并经 wire 注入。
type ToolCatalog []ToolSpec

// Find 按工具名查找；未命中返回 nil。
func (c ToolCatalog) Find(name string) *ToolSpec {
	for i := range c {
		if c[i].Name == name {
			return &c[i]
		}
	}
	return nil
}

// Conversation 会话领域模型。
type Conversation struct {
	ID     string
	UserID string
	Title  string
	// Kind 会话类型（ConversationKindUser / ConversationKindAgent）；零值按用户对话。
	Kind       int32
	CreateTime time.Time
	UpdateTime time.Time
}

// ToolCallRef 模型在一条 assistant 消息里发出的工具调用引用（协议层对象；
// ID 为模型侧 ID，与审计记录 ToolCall.CallID 对应）。
type ToolCallRef struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	// Invalid 模型给出的参数不是合法 JSON 对象（多为输出被截断）：Arguments 已规整为 "{}"
	// 以保证历史可回放，该调用不执行、直接以失败回灌模型。仅本轮内存有效，不落库。
	Invalid bool `json:"-"`
}

// Message 对话消息领域模型：模型协议结构原样承载（user / assistant / tool）。
// 库表 content jsonb 存 {content, reasoning, tool_calls, tool_call_id}。
type Message struct {
	ID             string
	ConversationID string
	Seq            int32
	Role           string
	Content        string
	Reasoning      string
	ToolCalls      []ToolCallRef
	ToolCallID     string
	CreateTime     time.Time
}

// ToolCall 工具调用审计记录（一次接口调用的全生命周期）。Input/Output 已脱敏。
type ToolCall struct {
	ID             string
	ConversationID string
	MessageID      string
	UserID         string
	// CallID 模型侧调用 ID（tool_calls[].id），tool 消息经此回灌模型。
	CallID        string
	ToolName      string
	Title         string
	Operation     string
	OperationCode string
	Write         bool
	Input         json.RawMessage
	Status        string
	Output        json.RawMessage
	Error         string
	DurationMS    int64
	CreateTime    time.Time
	ResolveTime   *time.Time
}

// Event 是 Agent 运行期推送给传输层的事件；Type 决定 Payload 形态。
type Event struct {
	Type    string
	Payload any
}

// 事件类型常量（与前端 SSE event 名一致）。
const (
	EventConversation        = "conversation"
	EventThinkingDelta       = "thinking_delta"
	EventTextDelta           = "text_delta"
	EventToolCall            = "tool_call"
	EventToolResult          = "tool_result"
	EventToolConfirmRequired = "tool_confirm_required"
	EventDone                = "done"
	EventError               = "error"
)

// ConversationPayload conversation 事件载荷。
type ConversationPayload struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// DeltaPayload text_delta / thinking_delta 事件载荷。
type DeltaPayload struct {
	Text string `json:"text"`
}

// ToolCallPayload tool_call / tool_result / tool_confirm_required 事件载荷。
type ToolCallPayload struct {
	ID         string          `json:"id"`
	Tool       string          `json:"tool"`
	Title      string          `json:"title"`
	Write      bool            `json:"write"`
	Status     string          `json:"status"`
	Input      json.RawMessage `json:"input,omitempty"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	DurationMS int64           `json:"durationMs,omitempty"`
}

// DonePayload done 事件载荷。
type DonePayload struct {
	Status string `json:"status"`
}

// ErrorPayload error 事件载荷。
type ErrorPayload struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// ConversationRepo 会话仓储接口（data 层实现）。
type ConversationRepo interface {
	Create(ctx context.Context, c *Conversation) error
	// Get 取本人会话；不存在/他人/已删除返回 ErrConversationNotFound。
	Get(ctx context.Context, id, userID string) (*Conversation, error)
	// GetAny 不校验归属地取会话（运维 Agent 域读运行的会话，可见性由它自己判定）；不存在/已删除返回 ErrConversationNotFound。
	GetAny(ctx context.Context, id string) (*Conversation, error)
	// List 本人的用户对话（kind=user），按最近活动倒序分页；Agent 运行的会话不在其中。
	List(ctx context.Context, userID string, page, pageSize int64) ([]*Conversation, int64, error)
	// Touch 刷新最近活动时间。
	Touch(ctx context.Context, id string, at time.Time) error
	// SoftDelete 软删本人会话；不存在返回 ErrConversationNotFound。
	SoftDelete(ctx context.Context, id, userID string, at time.Time) error
	// AcquireLease 条件更新抢占会话租约：空闲或租约已过期才成功（多实例互斥）。到期时间
	// 由数据库时钟计算（now() + ttl），与过期判定同源，不受实例间时钟偏差影响。
	AcquireLease(ctx context.Context, id, userID, token string, ttl time.Duration) (bool, error)
	// ReleaseLease 释放本轮租约（属主校验：仅 token 匹配才清空）。
	ReleaseLease(ctx context.Context, id, token string) error
	// Purge 物理删除会话及其全部消息与工具调用记录（运维 Agent 运行的保留期清理）；不存在的 ID 忽略。
	Purge(ctx context.Context, ids []string) error
}

// MessageRepo 消息仓储接口（data 层实现）。
type MessageRepo interface {
	// Append 追加消息；Seq 由仓储按会话内 max(seq)+1 分配并回填。
	Append(ctx context.Context, m *Message) error
	// List 返回会话完整历史（seq 正序）；面向用户的历史查询使用。
	List(ctx context.Context, conversationID string) ([]*Message, error)
	// ListRecent 返回会话最近 limit 条消息（seq 正序）；构建模型上下文使用，
	// 避免每轮加载全量历史。截断点可能落在 assistant 调用块中间（首部出现
	// 无主 tool 消息），由 contextWindow 在发送侧丢弃。
	ListRecent(ctx context.Context, conversationID string, limit int) ([]*Message, error)
	// UpdateToolCalls 覆写一条 assistant 消息的 tool_calls（调用结束后把参数里的
	// 敏感值打码落库）；消息不存在视为成功。
	UpdateToolCalls(ctx context.Context, m *Message) error
}

// ToolCallRepo 工具调用审计仓储接口（data 层实现）。
type ToolCallRepo interface {
	Create(ctx context.Context, tc *ToolCall) error
	// ListActive 返回一轮运行需要的审计记录：待确认的，以及属于指定 assistant 消息的
	//（messageID 为空则只有待确认的）；按发起时间正序。不加载整个会话的历史记录。
	ListActive(ctx context.Context, conversationID, messageID string) ([]*ToolCall, error)
	// ListSince 返回会话内 since 及之后发起的审计记录（零值 = 全部），按发起时间正序。
	ListSince(ctx context.Context, conversationID string, since time.Time) ([]*ToolCall, error)
	// Transition 条件状态迁移（status=from 才更新为 to），返回是否命中；
	// 用于 pending → approved/rejected/cancelled 的幂等处理。
	Transition(ctx context.Context, id, conversationID, from, to string, at time.Time) (bool, error)
	// Finish 写入终态（status/output/error/duration/resolve_time）。
	Finish(ctx context.Context, tc *ToolCall) error
}

// ChatRequest 一次模型调用的输入。
type ChatRequest struct {
	System   string
	Messages []*Message
	Tools    []ToolSpec
}

// Delta 模型流式增量。
type Delta struct {
	Text      string
	Reasoning string
}

// Completion 模型一次完整输出。
type Completion struct {
	Content      string
	Reasoning    string
	ToolCalls    []ToolCallRef
	FinishReason string
	// PromptTokens / CompletionTokens 模型接口回报的用量；接口不回报时为 0。
	PromptTokens     int64
	CompletionTokens int64
}

// ChatModel 模型客户端接口（data 层以 OpenAI 兼容协议实现）。onDelta 返回
// 错误时中止流式读取。
type ChatModel interface {
	Stream(ctx context.Context, req ChatRequest, onDelta func(Delta) error) (*Completion, error)
}

// InvokeResult 一次回环接口调用的结果；err 仅表示传输层失败。
type InvokeResult struct {
	// StatusCode HTTP 状态码。
	StatusCode int
	// Body 响应体（2xx 为接口响应 JSON；非 2xx 为 kratos 错误 JSON）。
	Body json.RawMessage
	// Reason / Message 非 2xx 时解析自 kratos 错误体。
	Reason  string
	Message string
}

// ToolInvoker 回环执行器接口（data 层以 net/http 实现）：以 bearer 身份调用
// 本服务接口，权限由既有中间件判定。
type ToolInvoker interface {
	Invoke(ctx context.Context, spec ToolSpec, input json.RawMessage, bearer string) (*InvokeResult, error)
}

// PermissionSource 跨域窄接口：用户有效授权操作 code 并集（system 域
// AuthzUsecase 实现；超管返回空集，由 ActorDescriber.IsAdmin 直通）。
type PermissionSource interface {
	EffectiveOperations(ctx context.Context, userID string) ([]string, error)
}

// ActorDescriber 跨域窄接口：当前操作者的展示信息（system 域 UserUsecase 实现）。
type ActorDescriber interface {
	DescribeActor(ctx context.Context, userID string) (username, nickname string, isAdmin bool, err error)
}

// ChatContext 页内助手上下文：用户正在查看的对象。
type ChatContext struct {
	Type string
	ID   string
}

// ContextProvider 跨域窄接口：按对象类型描述页内上下文（校验读权限后返回拼进系统提示的文本
// 与所属模块名）；类型不支持返回错误。由持有该对象的业务域实现。
type ContextProvider interface {
	DescribeContext(ctx context.Context, userID, typ, id string) (module, text string, err error)
}
