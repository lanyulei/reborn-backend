// Package biz 运维 Agent 域（agent）的业务核心：Profile（策略）、任务队列、分阶段运行器、预算与自愈判定、统计。
// 模型客户端、工具目录、回环执行、审计脱敏与无人值守运行循环全部复用 assistant 域（经窄接口），动作执行全部复用
// incident 域的建议 / 预案 / 熔断链路；本域只做任务编排、运行预算与安全策略、结果回写。零驱动、零传输层 import。
package biz

import (
	"time"
)

// Profile / 任务类型。
const (
	// KindDiagnose 故障诊断：四阶段，产出发现与建议。
	KindDiagnose = "diagnose"
	// KindPostmortem 经验沉淀：复盘完成后读复盘与上下文，在指定知识空间起草「故障经验」文档（只建草稿）。
	KindPostmortem = "postmortem"
)

// StageOrderFor 某类型 Profile 的阶段顺序：诊断四阶段；经验沉淀只有收集与结论。
func StageOrderFor(kind string) []string {
	if kind == KindPostmortem {
		return []string{StageCollect, StageConclude}
	}
	return StageOrder
}

// 任务来源。
const (
	SourceIncident   int32 = 1
	SourceManual     int32 = 2
	SourcePostmortem int32 = 3
	SourceSchedule   int32 = 4
	SourceDryRun     int32 = 5
)

// 任务状态。
const (
	TaskQueued    = "queued"
	TaskRunning   = "running"
	TaskSucceeded = "succeeded"
	TaskNoFinding = "no_finding"
	TaskFailed    = "failed"
	TaskCancelled = "cancelled"
	TaskSkipped   = "skipped"
	TaskExpired   = "expired"
)

// 运行状态。
const (
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunNoFinding = "no_finding"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
)

// 阶段键（固定顺序：收集 → 假设 → 验证 → 结论）。
const (
	StageCollect     = "collect"
	StageHypothesize = "hypothesize"
	StageVerify      = "verify"
	StageConclude    = "conclude"
)

// StageOrder 阶段顺序。
var StageOrder = []string{StageCollect, StageHypothesize, StageVerify, StageConclude}

// 阶段状态。
const (
	StagePending = "pending"
	StageRunning = "running"
	StageDone    = "done"
	StageSkipped = "skipped"
	StageFailed  = "failed"
)

// 人工评价。
const (
	FeedbackNone      int32 = 0
	FeedbackHelpful   int32 = 1
	FeedbackUnhelpful int32 = 2
)

// 入队原因。
const (
	ReasonOpened    = "opened"
	ReasonEscalated = "escalated"
	ReasonManual    = "manual"
	ReasonDryRun    = "dry_run"
)

// StageSpec 一个阶段的配置。
type StageSpec struct {
	Key      string   `json:"key"`
	MaxSteps int32    `json:"max_steps"`
	Tools    []string `json:"tools,omitempty"`
}

// BudgetSpec 单任务预算。
type BudgetSpec struct {
	MaxSteps         int32 `json:"max_steps"`
	TimeoutS         int32 `json:"timeout_s"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// AutoSpec Agent 侧的自动通过条件。
type AutoSpec struct {
	Enabled               bool     `json:"enabled"`
	MinConfidence         int32    `json:"min_confidence"`
	RequireMetricEvidence bool     `json:"require_metric_evidence"`
	MaxTargets            int32    `json:"max_targets"`
	Envs                  []string `json:"envs,omitempty"`
}

// ProfileSpec Profile 内容（agent_profile.spec）。
type ProfileSpec struct {
	Name             string      `json:"name"`
	Description      string      `json:"description,omitempty"`
	Kind             string      `json:"kind"`
	AgentUserID      string      `json:"agent_user_id"`
	Priority         int32       `json:"priority"`
	Enabled          bool        `json:"enabled"`
	Severities       []int32     `json:"severities,omitempty"`
	BusinessIDs      []string    `json:"business_ids,omitempty"`
	HourlyLimit      int32       `json:"hourly_limit"`
	Stages           []StageSpec `json:"stages"`
	Budget           BudgetSpec  `json:"budget"`
	Auto             AutoSpec    `json:"auto"`
	KnowledgeSpaceID string      `json:"knowledge_space_id,omitempty"`
}

// Stage 取某阶段配置（没有返回 nil）。
func (s *ProfileSpec) Stage(key string) *StageSpec {
	for i := range s.Stages {
		if s.Stages[i].Key == key {
			return &s.Stages[i]
		}
	}
	return nil
}

// Profile 一条 Profile。
type Profile struct {
	ID            string
	Spec          ProfileSpec
	BreakerOpen   bool
	BreakerReason string
	Version       int32
	CreateTime    time.Time
	UpdateTime    time.Time
	CreateUser    string
	UpdateUser    string
}

// ProfileView Profile 及其展示信息。
type ProfileView struct {
	*Profile
	AgentUserName  string
	AgentUserGone  bool
	UpdateUserName string
}

// ProfileHistory 一次变更。
type ProfileHistory struct {
	ID         string
	ProfileID  string
	Version    int32
	ActorID    string
	Before     *ProfileSpec
	After      ProfileSpec
	Note       string
	CreateTime time.Time
}

// ProfileHistoryView 变更及其展示信息。
type ProfileHistoryView struct {
	*ProfileHistory
	ActorName string
}

// Task 一条任务。
type Task struct {
	ID             string
	ProfileID      string
	Kind           string
	Source         int32
	DryRun         bool
	IncidentID     string
	IncidentNumber string
	IncidentTitle  string
	Severity       int32
	BusinessID     string
	RuleID         string
	Reason         string
	PeriodKey      string
	Status         string
	Attempts       int32
	NextTime       time.Time
	LockToken      string
	LockedUntil    *time.Time
	RequesterID    string
	Input          string
	Result         string
	RunID          string
	QueuedTime     time.Time
	StartTime      *time.Time
	EndTime        *time.Time
	UpdateTime     time.Time
}

// Active 报告任务是否仍在排队 / 进行中。
func (t *Task) Active() bool { return t.Status == TaskQueued || t.Status == TaskRunning }

// TaskView 任务及其展示信息。
type TaskView struct {
	*Task
	ProfileName   string
	RequesterName string
}

// TaskFilter 任务列表过滤条件；零值不过滤。
type TaskFilter struct {
	ProfileID  string
	IncidentID string
	Status     string
	Source     int32
}

// StageTrace 一个阶段的轨迹（agent_run.stages 数组元素）。
type StageTrace struct {
	Key              string `json:"key"`
	Status           string `json:"status"`
	Steps            int32  `json:"steps,omitempty"`
	PromptTokens     int64  `json:"prompt_tokens,omitempty"`
	CompletionTokens int64  `json:"completion_tokens,omitempty"`
	DurationMS       int64  `json:"duration_ms,omitempty"`
	Summary          string `json:"summary,omitempty"`
	EndReason        string `json:"end_reason,omitempty"`
	LastMessageID    string `json:"last_message_id,omitempty"`
}

// Finding 运行产出的发现（与 incident.Finding 同形，本域自己持有一份避免横向依赖）。
type Finding struct {
	Title      string   `json:"title"`
	Conclusion string   `json:"conclusion,omitempty"`
	Confidence int32    `json:"confidence,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
	NextStep   string   `json:"next_step,omitempty"`
}

// Ref 引用：ID + 名称。
type Ref struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// DeniedCall 被拒绝（403）的工具调用。
type DeniedCall struct {
	Tool    string `json:"tool"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// CapturedCall 干跑捕获的写调用。
type CapturedCall struct {
	Tool  string `json:"tool"`
	Input string `json:"input,omitempty"`
}

// Run 一次运行。
type Run struct {
	ID               string
	TaskID           string
	ProfileID        string
	IncidentID       string
	AgentUserID      string
	ConversationID   string
	Status           string
	DryRun           bool
	Stages           []StageTrace
	PromptTokens     int64
	CompletionTokens int64
	TokensEstimated  bool
	Finding          *Finding
	FindingEventID   string
	Proposals        []Ref
	Denied           []DeniedCall
	Captured         []CapturedCall
	EndReason        string
	Feedback         int32
	FeedbackUser     string
	FeedbackTime     *time.Time
	FeedbackNote     string
	StartTime        time.Time
	EndTime          *time.Time
	CreateTime       time.Time
}

// RunView 运行及其展示信息。
type RunView struct {
	*Run
	ProfileName      string
	IncidentNumber   string
	AgentUserName    string
	FeedbackUserName string
	CanRate          bool
}

// Budget 一个周期的用量。
type Budget struct {
	Tasks            int32
	PromptTokens     int64
	CompletionTokens int64
}

// StatsRange 统计范围。
type StatsRange struct {
	Start     time.Time
	End       time.Time
	ProfileID string
}

// ProfileStat 一个 Profile 的统计。
type ProfileStat struct {
	ProfileID        string
	ProfileName      string
	Tasks            int32
	Succeeded        int32
	NoFinding        int32
	Failed           int32
	Helpful          int32
	Unhelpful        int32
	PromptTokens     int64
	CompletionTokens int64
	AvgDurationS     float64
}

// Overview 统计概览。
type Overview struct {
	TasksByStatus     map[string]int32
	Runs              int32
	Succeeded         int32
	NoFinding         int32
	Failed            int32
	AvgDurationS      float64
	P95DurationS      float64
	PromptTokens      int64
	CompletionTokens  int64
	Proposals         int32
	AutoEligible      int32
	Helpful           int32
	Unhelpful         int32
	ByProfile         []ProfileStat
	TodayTasks        int32
	TodayPromptTokens int64
	DailyPromptBudget int64
}
