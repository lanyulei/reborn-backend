// Package biz 工单域业务核心：服务目录（分类 / 模板）、工单与评论。
// 工单的审批流转完全交给流程引擎：提单时在同一事务里发起流程，流程结束时由引擎
// 在它的事务里回调本域同步工单状态——工单状态与流程状态不会不一致。
package biz

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/ticket/v1"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// BusinessType 工单在流程引擎与文件域里的业务类型标识。
const BusinessType = "ticket"

// 模板状态。
const (
	TemplateDisabled int32 = 0
	TemplateEnabled  int32 = 1
)

// 工单状态：与流程实例状态一一对应（由流程同步）。
const (
	StatusProcessing = bizworkflow.InstanceRunning
	StatusCompleted  = bizworkflow.InstanceCompleted
	StatusRejected   = bizworkflow.InstanceRejected
	StatusCancelled  = bizworkflow.InstanceCancelled
	StatusTerminated = bizworkflow.InstanceTerminated
)

const (
	// maxFormFields 模板表单字段数上限（与 proto 校验一致）。
	maxFormFields = 50
	// maxCategories / maxTemplates 服务目录规模上限（目录接口不分页）。
	maxCategories = 100
	maxTemplates  = 500
	// maxComments 评论列表返回上限，也是单张工单的评论数上限。
	maxComments = 500
	// maxCommentsPerMinute 单个用户一分钟内（跨全部工单）最多发表的评论数。
	maxCommentsPerMinute = 30
	// maxDataBytes 表单数据 JSON 的大小上限。
	maxDataBytes = 32 << 10
	// priorityVar 优先级在流程变量里的键（条件表达式以 form.priority 引用）。
	priorityVar = "priority"
)

// 哨兵错误：reason 来自 api/ticket/v1 错误码枚举。
var (
	ErrCategoryNotFound    = errors.NotFound(v1.ErrorReason_TICKET_CATEGORY_NOT_FOUND.String(), "ticket category not found")
	ErrCategoryNameExists  = errors.Conflict(v1.ErrorReason_TICKET_CATEGORY_NAME_EXISTS.String(), "ticket category name already exists")
	ErrCategoryInUse       = errors.Conflict(v1.ErrorReason_TICKET_CATEGORY_IN_USE.String(), "ticket category still has templates")
	ErrTemplateNotFound    = errors.NotFound(v1.ErrorReason_TICKET_TEMPLATE_NOT_FOUND.String(), "ticket template not found")
	ErrTemplateNameExists  = errors.Conflict(v1.ErrorReason_TICKET_TEMPLATE_NAME_EXISTS.String(), "ticket template name already exists")
	ErrTemplateInvalid     = errors.BadRequest(v1.ErrorReason_TICKET_TEMPLATE_INVALID.String(), "invalid ticket template")
	ErrTemplateUnavailable = errors.Conflict(v1.ErrorReason_TICKET_TEMPLATE_UNAVAILABLE.String(), "ticket template is disabled")
	ErrTicketNotFound      = errors.NotFound(v1.ErrorReason_TICKET_NOT_FOUND.String(), "ticket not found")
	ErrTicketForbidden     = errors.Forbidden(v1.ErrorReason_TICKET_FORBIDDEN.String(), "no permission to view this ticket")
	ErrFormInvalid         = errors.BadRequest(v1.ErrorReason_TICKET_FORM_INVALID.String(), "invalid ticket form data")
	// ErrTimezoneUnknown 统计请求里的时区名称无法识别（复用表单不合法的原因码）。
	ErrTimezoneUnknown = errors.BadRequest(v1.ErrorReason_TICKET_FORM_INVALID.String(), "unknown time zone")
	// ErrCommentParentInvalid 回复的评论不存在、不属于本工单，或它本身就是一条回复（复用表单不合法的原因码）。
	ErrCommentParentInvalid = errors.BadRequest(v1.ErrorReason_TICKET_FORM_INVALID.String(), "the comment being replied to is not a top-level comment of this ticket")
	ErrCommentRateLimited   = errors.New(429, v1.ErrorReason_TICKET_COMMENT_RATE_LIMITED.String(), "too many comments, please try again later")
	ErrRatingUnavailable    = errors.Conflict(v1.ErrorReason_TICKET_RATING_UNAVAILABLE.String(), "this ticket cannot be rated now")
	ErrFormLocked           = errors.Conflict(v1.ErrorReason_TICKET_FORM_LOCKED.String(), "the form cannot be edited at this step")
	ErrLimitExceeded        = errors.Conflict(v1.ErrorReason_TICKET_LIMIT_EXCEEDED.String(), "ticket limit exceeded")
)

// ErrDuplicateRequest 同一幂等键的并发提交抢先落库（仓储 → 用例的内部信号，不出域）：
// 此时首张工单已提交，用例回读并返回它。
var ErrDuplicateRequest = stderrors.New("ticket: duplicate request")

// clean 去掉文本里的 NUL 字符并裁掉首尾空白（PostgreSQL 的 text / jsonb 不接受 \x00）。
func clean(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "\x00", ""))
}

// detail 复用哨兵错误的 code / reason，换上具体的说明。
func detail(base *errors.Error, message string) error {
	return errors.New(int(base.Code), base.Reason, message)
}

// FormField 表单字段定义（jsonb 持久化，json 标签即契约）。
type FormField struct {
	Code        string          `json:"code"`
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Required    bool            `json:"required,omitempty"`
	EnumValues  []string        `json:"enumValues,omitempty"`
	Rule        formschema.Rule `json:"rule"`
	Description string          `json:"description,omitempty"`
}

// 提单时关联 CMDB 资源的要求（ticket_template.ci_link）。
const (
	CILinkNone     int32 = 0
	CILinkOptional int32 = 1
	CILinkRequired int32 = 2
)

const (
	// maxResources 单张工单关联的资源数上限（与 proto 校验一致）。
	maxResources = 20
	// maxSLAMinutes SLA 时限上限（折合分钟，一年）。
	maxSLAMinutes = 366 * 24 * 60
)

// Duration 一段时限（与流程引擎的时长同构，json 标签即契约）。
type Duration struct {
	Value    int32  `json:"value"`
	Unit     string `json:"unit"`
	WorkTime bool   `json:"workTime,omitempty"`
}

// SLARule 某个优先级的 SLA 规则。
type SLARule struct {
	Priority int32     `json:"priority"`
	Response *Duration `json:"response,omitempty"`
	Resolve  *Duration `json:"resolve,omitempty"`
}

// Resource 工单关联的一个 CMDB 资源（名称为关联时的快照）。
type Resource struct {
	ID        string
	Name      string
	ModelName string
}

// Category 工单分类。Templates 仅目录读路径填充。
type Category struct {
	ID          string
	Name        string
	Description string
	Sort        int32
	Templates   []*Template
}

// Template 工单模板。Form 仅详情读路径填充。
type Template struct {
	ID           string
	CategoryID   string
	CategoryName string
	Name         string
	Description  string
	WorkflowKey  string
	NumberPrefix string
	// Icon 图标标识（前端图标集的键）；空 = 缺省图标。
	Icon string
	// FormColumns 表单布局：1=单列（一行一个字段） 2=双列（一行两个字段）。
	FormColumns int32
	Status      int32
	Sort        int32
	Form        []FormField
	UpdateTime  time.Time
	// SLA 规则、超时升级通知对象与关联资源的要求；仅详情读路径填充前两项。
	SLA         []SLARule
	SLAEscalate []string
	CILink      int32
}

// Ticket 工单。Form / Data 仅详情读路径填充。
type Ticket struct {
	ID           string
	Number       string
	TemplateID   string
	TemplateName string
	CategoryName string
	Title        string
	Priority     int32
	Form         []FormField
	// FormColumns 提单时模板的表单布局快照：1=单列 2=双列。
	FormColumns int32
	Data        map[string]any
	Status      int32
	Creator     string
	CreatorDept string
	InstanceID  string
	CreateTime  time.Time
	EndTime     time.Time
	// ResponseDue / ResolveDue SLA 时限（未配置为零值）；ResponseTime 首次响应时间。
	ResponseDue  time.Time
	ResolveDue   time.Time
	ResponseTime time.Time
	// SLAEscalate 提单时的升级通知对象快照（仅落库用）。
	SLAEscalate []string
	// Rating 满意度评分 1-5；0 = 未评价。
	Rating        int32
	RatingComment string
	RatingTime    time.Time
}

// TicketFilter 工单列表过滤条件；空值不过滤。
type TicketFilter struct {
	Keyword    string
	Status     *int32
	TemplateID string
	Priority   *int32
	Creator    string
	// ResourceID 只看关联了该 CMDB 资源的工单；Overdue 只看 SLA 已超时的。
	ResourceID string
	Overdue    bool
}

// Comment 工单评论。
type Comment struct {
	ID         string
	TicketID   string
	UserID     string
	Content    string
	CreateTime time.Time
	// ParentID 所回复的评论；空 = 顶层评论。回复只有一层。
	ParentID string
}

// CatalogReader 服务目录只读仓储接口。读取可能发生在提单事务内，实现须复用事务连接。
type CatalogReader interface {
	// ListCategories 全部未删除分类，按排序值升序。
	ListCategories(ctx context.Context) ([]*Category, error)
	// ListTemplates 全部未删除模板（不含表单），按排序值升序；enabledOnly 时只含启用的。
	ListTemplates(ctx context.Context, enabledOnly bool) ([]*Template, error)
	// GetTemplate 按 ID 查询未删除模板（含表单与分类名）；不存在返回 ErrTemplateNotFound。
	GetTemplate(ctx context.Context, id string) (*Template, error)
}

// WorkflowReferences 模板对流程定义的引用查询（由模板仓储实现，登记给流程引擎：流程被停用 /
// 删除之前询问是否还有启用中的模板绑定着它）。独立成只读接口：它不依赖任何流程用例，
// 装配时不与流程定义用例成环。
type WorkflowReferences interface {
	// DefinitionReferences 仍绑定流程标识 key 的启用中的模板的简短描述，至多几条；没有返回空。
	DefinitionReferences(ctx context.Context, key string) ([]string, error)
}

// CategoryWriter 分类写仓储接口。
type CategoryWriter interface {
	// CreateCategory 创建分类；重名返回 ErrCategoryNameExists，数量超限返回 ErrLimitExceeded。
	CreateCategory(ctx context.Context, c *Category, limit int) error
	// UpdateCategory 整组更新；不存在返回 ErrCategoryNotFound。
	UpdateCategory(ctx context.Context, c *Category) error
	// DeleteCategory 软删；分类下仍有模板返回 ErrCategoryInUse。
	DeleteCategory(ctx context.Context, id string) error
}

// TemplateWriter 模板写仓储接口。
type TemplateWriter interface {
	// CreateTemplate 创建模板；分类不存在返回 ErrCategoryNotFound，重名返回 ErrTemplateNameExists，
	// 数量超限返回 ErrLimitExceeded。
	CreateTemplate(ctx context.Context, t *Template, limit int) error
	// UpdateTemplate 整组更新；不存在返回 ErrTemplateNotFound。
	UpdateTemplate(ctx context.Context, t *Template) error
	// DeleteTemplate 软删；不存在返回 ErrTemplateNotFound。
	DeleteTemplate(ctx context.Context, id string) error
}

// TicketStore 工单写仓储接口（均经调用方事务句柄）。
type TicketStore interface {
	// NextNumberTx 取某前缀当天的下一个序号（行锁串行；事务回滚不留空号）。「当天」由数据库
	// 按会话时区判定并一并返回（yyyymmdd）：各实例的进程时区不一致也不会把同一天拆成两天。
	NextNumberTx(ctx context.Context, tx txrun.Tx, prefix string) (day string, seq int, err error)
	// FindByRequestTx 按提单人 + 幂等键查找已创建的工单；没有返回 (nil, nil)。
	FindByRequestTx(ctx context.Context, tx txrun.Tx, creator, requestID string) (*Ticket, error)
	// CreateTx 落库一张工单；幂等键被并发抢先占用返回 ErrDuplicateRequest。
	CreateTx(ctx context.Context, tx txrun.Tx, t *Ticket, requestID string) error
	// SyncTx 回写流程侧的信息：状态恒写；流程实例 ID、提单部门非空才写，结束时间非零才写。
	// 工单已挂在另一个流程实例上时不写（防止过期实例的回调覆盖状态）。
	SyncTx(ctx context.Context, tx txrun.Tx, id string, sync FlowSync) error
}

// TicketOps 工单在处理过程中的事务内操作。
type TicketOps interface {
	// LockFormTx 锁住工单行并读出表单定义与数据；不存在返回 ErrTicketNotFound。
	LockFormTx(ctx context.Context, tx txrun.Tx, id string) (form []FormField, data map[string]any, err error)
	// SetDataTx 整体改写表单数据。
	SetDataTx(ctx context.Context, tx txrun.Tx, id string, data map[string]any) error
	// MarkResponseTx 记下首次响应时间（已有则不改）。
	MarkResponseTx(ctx context.Context, tx txrun.Tx, id string, at time.Time) error
	// LinkResourcesTx 写入工单关联的资源。
	LinkResourcesTx(ctx context.Context, tx txrun.Tx, id string, resources []Resource) error
}

// SLABreach 一条待通知的 SLA 超时。
type SLABreach struct {
	TicketID   string
	Number     string
	Title      string
	InstanceID string
	Creator    string
	// Kind response = 响应超时，resolve = 解决超时。
	Kind     string
	Escalate []string
}

// Stats 工单统计。
type Stats struct {
	Total      int64
	ByStatus   map[int32]int64
	ByPriority map[int32]int64
	// ByTemplate 工单量最多的服务，降序。
	ByTemplate []NamedCount
	// Trend 每日提交 / 结束的工单数，日期升序（只含有工单的日期）。
	Trend             []TrendPoint
	AvgResolveSeconds int64
	ResponseTotal     int64
	ResponseMet       int64
	ResolveTotal      int64
	ResolveMet        int64
	Rated             int64
	AvgRating         float64
	// Timezone 趋势分日所用的时区（IANA 名称）；空 = 数据库会话时区。
	Timezone string
}

// NamedCount 名称 + 数量。
type NamedCount struct {
	Name  string
	Count int64
}

// TrendPoint 某一天的工单量（Day 为 yyyy-mm-dd）。
type TrendPoint struct {
	Day     string
	Created int64
	Closed  int64
}

// TicketInsight 工单的评价、资源、统计与 SLA 巡检。
type TicketInsight interface {
	// Rate 提单人评价已结束（完成 / 拒绝）且未评价的工单；条件不满足返回 false。
	Rate(ctx context.Context, id, creator string, rating int32, comment string) (bool, error)
	// ListResources 工单关联的资源。
	ListResources(ctx context.Context, id string) ([]Resource, error)
	// Stats 统计 since 之后提交的工单；趋势按 zone（IANA 时区名）的自然日分组，空 = 数据库会话时区。
	Stats(ctx context.Context, since time.Time, zone string) (*Stats, error)
	// ClaimBreachesTx 领取至多 limit 条尚未通知的 SLA 超时并打上「已通知」标记（SKIP LOCKED，
	// 多实例不重复通知；同事务回滚则标记一并回滚）。skip 中的工单不领取。
	ClaimBreachesTx(ctx context.Context, tx txrun.Tx, limit int, skip []string) ([]*SLABreach, error)
}

// FlowSync 由流程同步到工单的信息。
type FlowSync struct {
	InstanceID  string
	CreatorDept string
	Status      int32
	EndTime     time.Time
}

// TicketReader 工单只读仓储接口。
type TicketReader interface {
	// Get 按 ID 查询工单（含表单快照与数据）；不存在返回 ErrTicketNotFound。
	Get(ctx context.Context, id string) (*Ticket, error)
	// List 分页查询（不含表单与数据），提单时间倒序。
	List(ctx context.Context, filter TicketFilter, page, pageSize int64) ([]*Ticket, int64, error)
}

// CommentRepo 评论仓储接口。
type CommentRepo interface {
	// Add 追加一条评论；该工单评论数已达 limit 时返回 ErrLimitExceeded，评论人最近一分钟内
	// （跨全部工单）已发表 perMinute 条时返回 ErrCommentRateLimited。两项上限在并发下都不会被冲破。
	Add(ctx context.Context, c *Comment, limit, perMinute int) error
	// List 某工单的评论，时间升序，至多 limit 条。
	List(ctx context.Context, ticketID string, limit int) ([]*Comment, error)
}

// Workflow 流程引擎端口（由 workflow 域实现）。
type Workflow interface {
	// StartTx 在调用方事务内发起流程。
	StartTx(ctx context.Context, tx txrun.Tx, p bizworkflow.StartParams) (*bizworkflow.Instance, error)
}

// WorkflowForms 流程引擎的表单协作端口（由 workflow 域实现）。
type WorkflowForms interface {
	// Amend 由当前处理人修改流程变量：fn 在锁住流程实例的事务内被调用，拿到他待处理任务
	// 所在节点的字段权限，返回要并入流程变量的键值。他没有待处理的任务返回
	// bizworkflow.ErrTaskNotAssignee。
	Amend(ctx context.Context, instanceID, userID string,
		fn func(ctx context.Context, tx txrun.Tx, perms map[string]string) (map[string]any, error)) error
	// FormAccess 用户对该实例表单字段的权限视图。
	FormAccess(ctx context.Context, instanceID, userID string) (*bizworkflow.FormAccess, error)
	// PendingUsers 实例上当前待处理的用户。
	PendingUsers(ctx context.Context, instanceID string) ([]string, error)
	// Admins 实例所用流程版本配置的流程管理员（只含可用的用户）。
	Admins(ctx context.Context, instanceID string) ([]string, error)
}

// Deadlines 时限推算端口（由 workflow 域的工作日历实现）。
type Deadlines interface {
	Due(ctx context.Context, from time.Time, d Duration) (time.Time, error)
}

// Resources CMDB 资源端口（由 cmdb 域实现）：解析用户有权查看的资源。
type Resources interface {
	// Resolve 返回 ids 中该用户有权查看的资源（不存在或无权的不在结果中）。
	Resolve(ctx context.Context, userID string, ids []string) ([]Resource, error)
}

// WorkflowCatalog 流程定义端口：模板保存时校验所绑定的流程存在。
type WorkflowCatalog interface {
	// KeyStartable 报告给定标识的流程定义是否存在（未删除），以及当前能否发起（启用且已发布）。
	KeyStartable(ctx context.Context, key string) (exists, startable bool, err error)
}

// Attachments 附件端口（由 file 域实现）。
type Attachments interface {
	// Bind 在调用方事务内把提单人上传的临时文件绑定到工单。
	Bind(ctx context.Context, tx txrun.Tx, uploader, bizType, bizID string, fileIDs []string) error
	// ListByBiz 某业务对象的附件。
	ListByBiz(ctx context.Context, bizType, bizID string) ([]*bizfile.File, error)
}

// Directory 组织目录端口（由 system 域实现）：取显示名。
type Directory interface {
	// UserNames 用户 ID → 显示名；不存在的不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
	// DeptNames 部门 ID → 名称；不存在的不在结果中。
	DeptNames(ctx context.Context, ids []string) (map[string]string, error)
}

// CatalogManager 判定用户是否可查看完整的服务目录（含已停用的模板；由组合根以授权用例适配）。
type CatalogManager interface {
	CanManageCatalog(ctx context.Context, userID string) (bool, error)
}

// Supervisor 判定用户是否可查看全部工单（持有管理视图权限；由组合根以授权用例适配）。
type Supervisor interface {
	CanViewAll(ctx context.Context, userID string) (bool, error)
}
