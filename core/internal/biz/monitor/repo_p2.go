package biz

import (
	"context"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 第二、三期里被多个用例共用的领域模型与仓储接口（各功能自己独有的接口定义在各自的文件里）。
// 写入分工见迁移 0076：core 对 monitor_alert 只能写 ack_user / ack_time / ticket_id /
// workflow_instance_id / ai_summary 与两个通知计数列；对 monitor_alert_event / monitor_alert_cmd 只能
// SELECT、INSERT。写 monitor_escalation / monitor_inhibit / monitor_ci_index / monitor_calendar 时
// 同事务 bump 对应的 monitor_cfg_version 行。

// CIEntry monitor_ci_index 的一行：CMDB 资源在监控侧的物化副本（monitor-ci-sync 循环写）。
type CIEntry struct {
	ID        string
	ModelCode string
	Name      string
	Env       string
	Status    string
	// Owner 负责人：用户 ID 或自由文本（是可用用户的 ID 时才能作为通知对象）。
	Owner string
	Tags  []string
	// Attrs 可匹配属性 {属性名: [取值…]}，只含部署配置里登记的匹配属性。
	Attrs       map[string][]string
	UpstreamIDs []string
	// Valid 资源被删除或退役后为 false（保留行，供已有告警展示名称）。
	Valid            bool
	SourceUpdateTime *time.Time
}

// CIIndexReader CMDB 资源索引读仓储。
type CIIndexReader interface {
	// Briefs 批量按 ID 取（含已失效的行）；不存在的不在结果中。
	Briefs(ctx context.Context, ids []string) (map[string]*CIEntry, error)
	// ListValid 全部有效的行，按 ci_id 排序，至多 limit 条（模拟触发据此构造与引擎相同的资源索引）。
	ListValid(ctx context.Context, limit int) ([]*CIEntry, error)
	// Summary 有效资源数与最近一次同步时间（从未同步为 nil）。
	Summary(ctx context.Context) (valid int32, lastSync *time.Time, err error)
}

// CfgVersionReader 配置版本号（monitor_cfg_version）：进程内只读缓存据此判断是否过期。
type CfgVersionReader interface {
	// CfgVersion name 取 monitorcontract.Cfg* 常量；行不存在返回 0。
	CfgVersion(ctx context.Context, name string) (int64, error)
}

// CalendarReader 工作日历快照读仓储（monitor_calendar，恒一行）。
type CalendarReader interface {
	// Snapshot 尚未物化时返回零值（Compile 会报错，判定时按「没有日历」处理）。
	Snapshot(ctx context.Context) (mc.CalendarSnapshot, error)
}

// EscalationSpec 升级策略的可编辑内容。GroupID 为空 = 全局策略。
type EscalationSpec struct {
	GroupID     string
	Name        string
	Description string
	Mode        int32
	MinSeverity int32
	Levels      []mc.EscalationLevel
	Enabled     bool
}

// Escalation 升级策略。
type Escalation struct {
	ID         string
	Spec       EscalationSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	UpdateUser string
}

// UsableIn 报告策略能否被某个分组内的规则引用：全局策略，或归属于该分组 / 其上级分组的策略。
// ancestors 是规则所在分组自根到自身的 ID 链（Scope.Ancestors）。
func (e *Escalation) UsableIn(ancestors []string) bool {
	return e.Spec.GroupID == "" || slices.Contains(ancestors, e.Spec.GroupID)
}

// EscalationFilter 升级策略列表过滤条件。Groups 为可见分组；IncludeGlobal 表示同时返回全局策略。
// Global 非空时只看全局策略（true）或只看分组策略（false）。
type EscalationFilter struct {
	Groups        GroupFilter
	IncludeGlobal bool
	Global        *bool
	Keyword       string
	Enabled       *bool
}

// EscalationReader 升级策略读仓储。
type EscalationReader interface {
	// Get 不存在（含已删除）返回 ErrEscalationNotFound。
	Get(ctx context.Context, id string) (*Escalation, error)
	// ListByIDs 未删除的策略；不存在的不在结果中。
	ListByIDs(ctx context.Context, ids []string) ([]*Escalation, error)
	// List 全局策略在前，其余按名称排序。
	List(ctx context.Context, filter EscalationFilter, page, pageSize int64) ([]*Escalation, int64, error)
}

// InhibitSpec 抑制规则的可编辑内容。GroupID 为空 = 作用于全部分组。
type InhibitSpec struct {
	GroupID     string
	Name        string
	Description string
	Source      mc.InhibitSelector
	Target      mc.InhibitSelector
	EqualLabels []string
	Enabled     bool
}

// Inhibit 抑制规则。
type Inhibit struct {
	ID         string
	Spec       InhibitSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	CreateUser string
	UpdateUser string
}

// InhibitFilter 抑制规则列表过滤条件（口径同 MuteFilter）。
type InhibitFilter struct {
	Groups        GroupFilter
	IncludeGlobal bool
	Keyword       string
	Enabled       *bool
}

// InhibitReader 抑制规则读仓储。
type InhibitReader interface {
	// Get 不存在（含已删除）返回 ErrInhibitNotFound。
	Get(ctx context.Context, id string) (*Inhibit, error)
	List(ctx context.Context, filter InhibitFilter, page, pageSize int64) ([]*Inhibit, int64, error)
	// ListEnabled 全部启用且未删除的抑制规则，按 ID 排序，至多 limit 条（模拟触发用，与引擎加载的一致）。
	ListEnabled(ctx context.Context, limit int) ([]*Inhibit, error)
}

// InhibitNames 抑制规则名称（含已删除的：历史事件仍要展示命中的抑制规则）。
type InhibitNames interface {
	InhibitNames(ctx context.Context, ids []string) (map[string]string, error)
}

// AlertEvent 事件时间线的一条（monitor_alert_event，append-only）。Operator 为空 = 系统产生。
type AlertEvent struct {
	ID         string
	AlertID    string
	Type       string
	Operator   string
	Detail     mc.AlertEventDetail
	CreateTime time.Time
}

// AlertEventRepo 事件时间线仓储。core 只写 monitorcontract 约定由 core 写的类型。
type AlertEventRepo interface {
	// Append 追加一条（独立语句）；CreateTime 由数据库 now() 写入并回填。
	Append(ctx context.Context, ev *AlertEvent) error
	// AppendTx 在调用方事务内追加一条。
	AppendTx(ctx context.Context, tx txrun.Tx, ev *AlertEvent) error
	// ListByAlert 事件的时间线，按时间升序，至多 limit 条；超过 limit 条时保留最新的 limit 条。
	ListByAlert(ctx context.Context, alertID string, limit int) ([]*AlertEvent, error)
}

// TicketLink 事件关联工单的留痕（取自时间线里最近一条 ticket 条目）。Creator 是提单人：手动转工单为
// 操作人，自动建单为代理提单人——告警恢复时以他的名义在工单上追加记录。
type TicketLink struct {
	TicketID string
	Number   string
	Creator  string
	Auto     bool
}

// AlertTicketLinks 事件 → 关联工单的留痕（批量，列表页一次取齐）。
type AlertTicketLinks interface {
	// TicketLinks 没有 ticket 条目的事件不在结果中。
	TicketLinks(ctx context.Context, alertIDs []string) (map[string]TicketLink, error)
}

// AlertLinkWriter 回写事件上由 core 负责的关联列（列级授权）。都在调用方事务内执行，便于与时间线
// 条目、事件动作的状态一起提交。
type AlertLinkWriter interface {
	// LinkTicketTx 把工单关联到事件；事件已关联工单（不论是不是同一张）时不写，返回 false 与已关联的
	// 工单 ID。事件不存在返回 ErrAlertNotFound。
	LinkTicketTx(ctx context.Context, tx txrun.Tx, alertID, ticketID string) (linked bool, existing string, err error)
	// SetWorkflowInstanceTx 记下触发的流程实例（已有值时不覆盖）。
	SetWorkflowInstanceTx(ctx context.Context, tx txrun.Tx, alertID, instanceID string) error
	// SetAISummaryTx 写入 AI 摘要（调用方已截断到 monitorcontract.MaxAISummaryBytes 以内）。
	SetAISummaryTx(ctx context.Context, tx txrun.Tx, alertID, summary string) error
}

// AlertCIStats 按资源统计活跃告警。
type AlertCIStats interface {
	// FiringByCI 这些资源当前的活跃告警数（只统计 groups 范围内的事件）；没有活跃告警的不在结果中。
	FiringByCI(ctx context.Context, ciIDs []string, groups GroupFilter) (map[string]int32, error)
}

// AlertViewDecorator 补齐事件视图里第二、三期的展示名：认领人、关闭人、抑制规则与源告警、关联资源、
// 工单编号、是否已下达关闭（AlertView 里注明「由 AlertDecorator 填充」的字段）。收件人视图
// （AlertView.Recipient）不带指向其他对象的名称。列表页一次批量取齐，不逐行查询。
type AlertViewDecorator interface {
	Decorate(ctx context.Context, views []*AlertView) error
}
