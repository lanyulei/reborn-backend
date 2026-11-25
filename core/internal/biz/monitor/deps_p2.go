package biz

import (
	"context"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 第二、三期对其他域的依赖：同样只读 / 单向，一律是在此定义的窄接口，由组合根（internal/biz 的
// monitor_p2.go）以对方域**已有的公开用例方法**适配。其他域不依赖 monitor，monitor 不读其他域的表。

// TicketTemplate 可选的工单模板（最小披露）。
type TicketTemplate struct {
	ID           string
	Name         string
	CategoryName string
}

// TicketCatalog 工单模板目录（ticket 域 CatalogUsecase 适配）。
type TicketCatalog interface {
	// Templates 启用的工单模板，按服务目录的顺序，至多 500 个。
	Templates(ctx context.Context) ([]TicketTemplate, error)
	// Template 模板对 userID 是否可用：存在且启用返回其摘要，否则返回 (nil, nil)。
	Template(ctx context.Context, templateID, userID string) (*TicketTemplate, error)
}

// TicketCreate 提单入参。RequestID 为幂等键（同一提单人以相同的键重复提交返回首次创建的工单）。
type TicketCreate struct {
	TemplateID  string
	Title       string
	Priority    int32
	Data        map[string]any
	Creator     string
	CreatorDept string
	RequestID   string
	// ResourceIDs 关联的 CMDB 资源：提单人无权查看的资源会被工单域拒绝，调用方先行判断。
	ResourceIDs []string
}

// TicketInfo 工单的摘要。
type TicketInfo struct {
	ID      string
	Number  string
	Title   string
	Status  int32
	Creator string
}

// TicketDesk 工单的创建、查看与评论（ticket 域 Usecase 适配）。
type TicketDesk interface {
	// Create 以 Creator 的名义提单。工单域的业务错误（模板不可用、表单校验失败等）原样返回。
	Create(ctx context.Context, p TicketCreate) (*TicketInfo, error)
	// View 以 userID 的身份读取工单；工单不存在或他无权查看返回 (nil, nil)。
	View(ctx context.Context, ticketID, userID string) (*TicketInfo, error)
	// Comment 以 userID 的名义在工单上追加一条评论（userID 须能查看该工单）。
	Comment(ctx context.Context, ticketID, userID, content string) error
}

// WorkflowDef 可发起的流程定义（最小披露）。
type WorkflowDef struct {
	Key  string
	Name string
}

// WorkflowStart 发起流程的入参。业务对象恒为 (monitorcontract.WorkflowBusinessType, AlertID)，
// 并以 AlertID 作幂等键：同一事件重复发起返回首次的实例。
type WorkflowStart struct {
	DefinitionKey string
	AlertID       string
	Title         string
	Initiator     string
	Variables     map[string]any
}

// Workflows 流程定义与发起（workflow 域 DefinitionUsecase / RuntimeUsecase 适配）。
type Workflows interface {
	// Definitions 启用且已发布的流程定义，按名称排序，至多 500 个。
	Definitions(ctx context.Context) ([]WorkflowDef, error)
	// Definition 流程标识当前能否发起：能则返回其摘要，否则返回 (nil, nil)。
	Definition(ctx context.Context, key string) (*WorkflowDef, error)
	// Start 发起流程，返回流程实例 ID。流程域的业务错误原样返回。
	Start(ctx context.Context, p WorkflowStart) (instanceID string, err error)
}

// CICard 事件关联资源的卡片（来自 CMDB，已按调用方的权限读取）。
type CICard struct {
	ID        string
	Name      string
	ModelCode string
	ModelName string
	Env       string
	Status    string
	// Owner 负责人原文（用户 ID 或自由文本）；OwnerName 负责人是系统用户时的显示名。
	Owner     string
	OwnerName string
	Tags      []string
}

// CIImpact 影响面里的一个下游资源。CanView 为 false 时只有 ID、Name、Depth 有值。
type CIImpact struct {
	ID        string
	Name      string
	ModelCode string
	ModelName string
	Depth     int32
	CanView   bool
}

// CMDBViewer 以调用方自己的 CMDB 接口权限与数据范围读取资源（cmdb 域 CIUsecase / RelationUsecase /
// ScopeUsecase 适配）。监控域不替调用方放宽 CMDB 的可见范围。
type CMDBViewer interface {
	// Card 读取资源卡片。exists 表示资源在 CMDB 里仍然存在（未删除）；card 为 nil 表示资源不存在，或
	// userID 无权在资源管理里查看它（没有查看资源的接口权限，或资源不在他的数据范围内）。
	Card(ctx context.Context, userID, ciID string) (card *CICard, exists bool, err error)
	// Impact 资源的下游资源：按部署配置的拓扑关系向下至多 depth 层、至多 limit 个，按层级、名称排序；
	// truncated 表示超出 limit 已截断。资源不存在返回空。每个节点带 userID 能否查看。
	Impact(ctx context.Context, userID, ciID string, depth, limit int) (nodes []CIImpact, truncated bool, err error)
}

// CIModel 可选的 CMDB 模型。
type CIModel struct {
	Code string
	Name string
}

// CMDBCatalog 规则表单里 CMDB 配置项的候选（cmdb 域适配；最小披露）。
type CMDBCatalog interface {
	// Models 启用的模型，按名称排序，至多 500 个。
	Models(ctx context.Context) ([]CIModel, error)
}

// OncallGroup 值班分组（最小披露）。
type OncallGroup struct {
	ID   string
	Name string
}

// OncallGroups 值班分组与其管理员（oncall 域 GroupUsecase 适配）。
type OncallGroups interface {
	// Groups 全部值班分组，按名称排序，至多 500 个。
	Groups(ctx context.Context) ([]OncallGroup, error)
	// AdminIDs 分组的管理员（只含可用用户）；分组不存在返回空。
	AdminIDs(ctx context.Context, groupID string) ([]string, error)
}

// DutyRoster 值班表此刻的主 / 备值班人（oncall 域 ScheduleUsecase 适配）。升级策略的「仅备班」用它；
// 规则与订阅的值班表仍用 Duty（主班在前、备班在后）。
type DutyRoster interface {
	// OnDutyByRole 值班表不存在、已停用或无人值班时对应的返回值为空。
	OnDutyByRole(ctx context.Context, scheduleID string) (primary, backup []string, err error)
}

// ShiftSlot 值班表的一个班次。
type ShiftSlot struct {
	UserID   string
	UserName string
	// Role 1=主班 2=备班。
	Role  int32
	Start time.Time
	End   time.Time
}

// Shifts 值班表的班次（oncall 域 ScheduleUsecase 适配）。
type Shifts interface {
	// Shifts 值班表里与 [from, to) 有交集的班次，按开始时间升序，至多 limit 个；exists 为 false 表示
	// 值班表不存在。班次多于 limit 个时返回 ErrTimeRangeInvalid（提示缩小时间范围），而不是只给前 limit 个。
	Shifts(ctx context.Context, scheduleID string, from, to time.Time, limit int) (slots []ShiftSlot, exists bool, err error)
}

// Summarizer 文本摘要（assistant 域的模型客户端适配）。
type Summarizer interface {
	// Available 智能助手是否已配置模型密钥；未配置时不调用 Summarize。
	Available() bool
	// Summarize 以 system 为系统提示、prompt 为用户输入生成一段文本。调用方负责超时（ctx）与输出截断。
	Summarize(ctx context.Context, system, prompt string) (string, error)
}

// EngineAudience 平台自监控通知的收件人（system 域 DirectoryUsecase 按配置的角色编码适配）。
type EngineAudience interface {
	// UserIDs 可用用户；未配置收件角色返回空（事件不带收件人，交由通知路由）。
	UserIDs(ctx context.Context) ([]string, error)
}

// CalendarSource 流程域的工作日历（workflow 域 CalendarUsecase 适配）。
type CalendarSource interface {
	// WorkCalendar 当前的工作日历配置；流程域尚未配置日历时 ok 为 false。
	WorkCalendar(ctx context.Context) (snap mc.CalendarSnapshot, ok bool, err error)
}

// OncallAccess 值班域的接口权限判定（组合根以 system 域 AuthzUsecase 适配，口径同 Permissions）。
type OncallAccess interface {
	// CanViewOncallCalendar 是否持有「查询值班班次」接口权限（值班日历页）。
	CanViewOncallCalendar(ctx context.Context, userID string) (bool, error)
}
