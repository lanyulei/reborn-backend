package biz

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

// 跨域窄接口：全部由组合根（internal/biz/agent_adapters.go）用其他域已有的公开用例适配；本包不 import 其他业务域
//（assistant 的无人值守入口类型除外——它就是为本域定义的契约）。

// Headless 无人值守运行一个阶段（assistant 域 AgentUsecase.RunHeadless）。
type Headless interface {
	RunHeadless(ctx context.Context, spec bizassistant.HeadlessSpec) (*bizassistant.HeadlessResult, error)
}

// Conversations 助手会话（kind=agent）的创建与转录读取（assistant 域仓储）。
type Conversations interface {
	// Create 为机器账号新建一个 kind=agent 的会话，返回会话 ID。
	Create(ctx context.Context, userID, title string) (string, error)
	// Transcript 会话的全部消息与工具调用记录（不校验归属：可见性由本域判定）。
	Transcript(ctx context.Context, conversationID string) ([]*bizassistant.Message, []*bizassistant.ToolCall, error)
	// Purge 物理删除会话及其消息与工具调用记录（保留期清理）。
	Purge(ctx context.Context, conversationIDs []string) error
}

// Tools 助手工具目录（assistant 域 ToolCatalog）：Profile 白名单校验、工具目录展示、运行器自己的回环调用。
type Tools interface {
	// Find 按工具名查找；未命中返回 nil。
	Find(name string) *bizassistant.ToolSpec
	// All 全部工具。
	All() []bizassistant.ToolSpec
}

// Invoker 回环执行器（assistant 域 ToolInvoker）：运行器用它取 / 释放故障的 Agent 租约。
type Invoker interface {
	Invoke(ctx context.Context, spec bizassistant.ToolSpec, input json.RawMessage, bearer string) (*bizassistant.InvokeResult, error)
}

// Accounts 机器账号（system 域）：令牌签发、类型与状态、显示名。
type Accounts interface {
	// IssueToken 为机器账号签发短期访问令牌；不是机器账号 / 已停用返回错误。
	IssueToken(ctx context.Context, userID string, ttl time.Duration) (token string, expires time.Time, err error)
	// IsMachine 账号是否为可用的机器账号；不存在返回 (false, nil)。
	IsMachine(ctx context.Context, userID string) (bool, error)
	// Names 批量取用户显示名（不存在的不在结果中）。
	Names(ctx context.Context, ids []string) (map[string]string, error)
	// HasMenu 用户是否持有某菜单权限（超管恒 true）：完整转录只对持有「Agent 任务」菜单者开放。
	HasMenu(ctx context.Context, userID, menu string) (bool, error)
	// IsAdmin 超管判定。
	IsAdmin(ctx context.Context, userID string) (bool, error)
}

// IncidentSnapshot 故障快照（领取前的状态检查与提示词用）。
type IncidentSnapshot struct {
	ID          string
	Number      string
	Title       string
	Severity    int32
	Status      int32
	Active      bool
	BusinessID  string
	CommanderID string
	RespondTime *time.Time
	OccurTime   time.Time
	ResolveTime *time.Time
	AgentOwner  string
	// LeaseValid 当前有人持有有效 Agent 租约。
	LeaseValid bool
	IsDrill    bool
}

// Incidents 故障（incident 域）的只读窄接口：状态快照与成员判定；写入一律经回环调用。
type Incidents interface {
	// Snapshot 不存在返回 incident 域的 NotFound 错误。
	Snapshot(ctx context.Context, id string) (*IncidentSnapshot, error)
	// MemberAccess 调用者在该故障里的身份。
	MemberAccess(ctx context.Context, userID, incidentID string) (admin, commander, responder bool, err error)
	// CanRead 调用者是否持有「查询故障详情」接口权限（故障记录对持有者全局可见）。
	CanRead(ctx context.Context, userID string) (bool, error)
}

// Notifier 平台自监控通知（熔断 / 预算耗尽 / 运行器暂停）：经 notify 域发给管理员角色。
type Notifier interface {
	// Notify 写一条自监控事件；失败只记日志，不影响任务。
	Notify(ctx context.Context, kind, title, text string, dedup string) error
}

// Metrics 可观测指标（server 层以 OTel → Prometheus 实现；nil 安全，由运行器经 noopMetrics 兜底）。
type Metrics interface {
	// TaskFinished 一条任务到达终态。
	TaskFinished(ctx context.Context, status string)
	// StageFinished 一个阶段结束（含失败）。
	StageFinished(ctx context.Context, stage, status string, d time.Duration)
	// ModelFailure 模型侧失败（rejected / unavailable）。
	ModelFailure(ctx context.Context, kind string)
	// Tokens 一次运行的 token 用量增量。
	Tokens(ctx context.Context, prompt, completion int64)
}

// noopMetrics 未装配指标时的空实现。
type noopMetrics struct{}

func (noopMetrics) TaskFinished(context.Context, string)                         {}
func (noopMetrics) StageFinished(context.Context, string, string, time.Duration) {}
func (noopMetrics) ModelFailure(context.Context, string)                         {}
func (noopMetrics) Tokens(context.Context, int64, int64)                         {}

// Deps 本域全部用例共用的依赖（wire.Struct 装配）。
type Deps struct {
	Profiles      ProfileRepo
	Tasks         TaskRepo
	Runs          RunRepo
	Budgets       BudgetRepo
	Headless      Headless
	Conversations Conversations
	Tools         Tools
	Invoker       Invoker
	Accounts      Accounts
	Incidents     Incidents
	Notifier      Notifier
	Metrics       Metrics
	Config        Config
	Logger        *slog.Logger
}
