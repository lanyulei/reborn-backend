package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

const (
	// maxVariablesBytes 流程变量 JSON 的大小上限。
	maxVariablesBytes = 64 << 10
	// candidateLimit 可选用户单次返回上限。
	candidateLimit = 50
	// notifyChunk 单个通知事件的显式收件上限（与 notify 入参约束一致），超出则拆分。
	notifyChunk = 20
	// severityInfo 流程通知的事件级别。
	severityInfo int32 = 1
)

// 流程通知事件类型（通知域按它匹配路由与模板）。
const (
	EventTaskAssigned     = "workflow.task.assigned"
	EventTaskCC           = "workflow.task.cc"
	EventInstanceFinished = "workflow.instance.finished"
	EventTaskRemind       = "workflow.task.remind"
	EventServiceFailed    = "workflow.service.failed"
)

// handoverBatch 一次交接请求最多处理的实例数（其余由调用方再次请求）。
const handoverBatch = 100

// handoverReserve 交接请求在 HTTP 超时前留出的余量：剩余时间不足时不再开始处理新的实例
// （一个实例一个事务，开了就要么做完要么回滚），把应答的时间留出来。
const handoverReserve = 500 * time.Millisecond

// maxNoticeTitle 通知标题的字节上限（通知域限 255 字节，留出余量）。
const maxNoticeTitle = 240

// ActionAmend 处理人修改了流程变量（表单字段），detail.fields 为改动的键。
const ActionAmend = "amend"

// 哨兵错误。
var (
	ErrBusinessDuplicate = errors.Conflict(v1.ErrorReason_WORKFLOW_BUSINESS_DUPLICATE.String(), "a workflow has already been started for this business object")
	ErrVariablesInvalid  = errors.BadRequest(v1.ErrorReason_WORKFLOW_VARIABLES_INVALID.String(), "workflow variables must be a JSON object of at most 64KB")
	// ErrSubProcessUnavailable 子流程发起不了：命令整体回滚，流程停在子流程节点之前。
	ErrSubProcessUnavailable = errors.Conflict(v1.ErrorReason_WORKFLOW_SUBPROCESS_UNAVAILABLE.String(), "the sub-process cannot be started: it is unpublished, disabled or nested too deeply")
)

// StartParams 发起流程的入参。
type StartParams struct {
	// DefinitionKey 流程标识。
	DefinitionKey string
	// BusinessType / BusinessID 所挂的业务对象；均为空 = 独立发起。同一业务对象只能发起一次。
	BusinessType string
	BusinessID   string
	// FormKey 所用表单的标识（如工单的模板 ID）：节点上按表单区分的字段权限据此取用；可空。
	FormKey   string
	Title     string
	Initiator string
	// InitiatorDept 发起部门；空 = 取发起人的主部门。非空时须为发起人所属部门。
	InitiatorDept string
	Variables     map[string]any
	// RequestID 幂等键（可空）：同一发起人以相同的键重复发起时返回首次发起的实例。
	RequestID string

	// 子流程发起时由引擎填写：父流程里等它的节点与执行位置。
	parentNodeID  string
	parentTokenID string
	// clientID 经开放接口发起时的接入凭据 ID。
	clientID string
}

// InstanceFilter 实例列表过滤条件；空值不过滤。
type InstanceFilter struct {
	Keyword      string
	Status       *int32
	DefinitionID string
	Initiator    string
	// BusinessType 业务类型；InstanceFilterNoBusiness = 只看不挂业务对象的。
	BusinessType string
	// BusinessIDs 限定业务对象；StartAfter / StartBefore 发起时间窗（故障管理的变更关联用）。
	BusinessIDs []string
	StartAfter  *time.Time
	StartBefore *time.Time
}

// InstanceFilterNoBusiness 实例列表按业务类型过滤时，表示「不挂业务对象的独立流程」的取值。
const InstanceFilterNoBusiness = "-"

// InstanceItem 实例列表项。
type InstanceItem struct {
	Instance
	// CurrentNodes 当前待处理的节点名称。
	CurrentNodes []string
}

// TaskItem 待办 / 已办 / 抄送列表项。
type TaskItem struct {
	TaskID         string
	InstanceID     string
	Title          string
	DefinitionName string
	NodeName       string
	Initiator      string
	InstanceStatus int32
	Status         int32
	CreateTime     time.Time
	ActionTime     time.Time
	// BusinessType / BusinessID 任务最终所属的业务对象：子流程里的任务给出根流程所挂的那个。
	BusinessType string
	BusinessID   string
}

// 任务列表视图。
const (
	TaskViewTodo = "todo"
	TaskViewDone = "done"
	TaskViewCC   = "cc"
)

// InstanceStore 实例执行状态的存取（均经调用方事务句柄）。
type InstanceStore interface {
	// CreateTx 落库一个新实例及其初始执行状态；同一业务对象已有进行中的流程、或幂等键
	// 被并发抢先占用，返回 ErrBusinessDuplicate。requestID 可空。
	CreateTx(ctx context.Context, tx txrun.Tx, s *State, requestID string) error
	// FindByRequestTx 按发起人 + 幂等键查找已发起的实例；没有返回 (nil, nil)。
	FindByRequestTx(ctx context.Context, tx txrun.Tx, initiator, requestID string) (*Instance, error)
	// LoadForUpdateTx 锁住实例行（FOR UPDATE）并读出完整执行状态；不存在返回
	// ErrInstanceNotFound。同一实例的写命令因此串行。
	LoadForUpdateTx(ctx context.Context, tx txrun.Tx, instanceID string) (*State, error)
	// SaveTx 回写执行状态：实例状态、执行位置、任务、处理人（新增的插入、已有的更新），
	// 追加本次产生的历史与作业；VarsChanged 时回写流程变量。实例已结束时，把它名下仍待
	// 执行的作业作废，并为仍在进行的子流程各排一条终止作业。
	SaveTx(ctx context.Context, tx txrun.Tx, s *State) error
}

// ChildRef 一个子流程实例的概要。
type ChildRef struct {
	ID             string
	NodeID         string
	DefinitionName string
	Status         int32
}

// InstanceLookup 实例的轻量查询（不读整份执行状态）。
type InstanceLookup interface {
	// GetInstance 读出实例行（含流程变量）；不存在返回 ErrInstanceNotFound。
	GetInstance(ctx context.Context, instanceID string) (*Instance, error)
	// Children 由该实例的子流程节点发起的子流程，发起时间升序。
	Children(ctx context.Context, instanceID string) ([]*ChildRef, error)
	// InstancesWithSeats 某用户仍有未了结席位的进行中实例 ID（升序），只取 after 之后的（空 =
	// 从头），至多 limit 个。
	InstancesWithSeats(ctx context.Context, userID, after string, limit int) ([]string, error)
	// Snapshot 读出实例当前的执行状态（不含历史与作业）；不存在返回 ErrInstanceNotFound。
	Snapshot(ctx context.Context, instanceID string) (*State, error)
	// PendingUsers 实例及其名下各级子流程里，进行中的实例上当前待处理的用户（去重）。
	PendingUsers(ctx context.Context, instanceID string) ([]string, error)
	// SeatsInTree 用户在 instanceID 所在的整棵流程树（根流程及其各级子流程）里持有席位（处理 /
	// 抄送，未被取消；被代理人也算）的实例。instanceID 自身排在最前。
	SeatsInTree(ctx context.Context, instanceID, userID string) ([]SeatRef, error)
}

// SeatRef 用户在流程树里某个实例上的席位概况。
type SeatRef struct {
	InstanceID string
	// Pending 他在该实例上当前有待处理的任务。
	Pending bool
}

// InstanceReader 实例只读查询。
type InstanceReader interface {
	// Load 读出实例的完整状态（含全部历史，以及尚未了结的定时 / 自动任务作业）；不存在返回
	// ErrInstanceNotFound。
	Load(ctx context.Context, instanceID string) (*State, error)
	// InstanceIDOfTask 任务所属的实例 ID；任务不存在返回 ErrTaskNotFound。
	InstanceIDOfTask(ctx context.Context, taskID string) (string, error)
	// ListInstances 分页查询实例，发起时间倒序。
	ListInstances(ctx context.Context, filter InstanceFilter, page, pageSize int64) ([]*InstanceItem, int64, error)
	// ListTasks 分页查询某用户的任务视图（todo / done / cc），时间倒序；businessType 非空时
	// 只返回挂在该类业务对象上的流程任务。
	ListTasks(ctx context.Context, userID, view, keyword, businessType string, page, pageSize int64) ([]*TaskItem, int64, error)
}

// ParticipantChecker 判定用户是否参与了某个实例（发起人 / 处理人 / 抄送人）：单条存在性查询，
// 供上层业务域做访问控制（不必读出整个实例）。instanceID 为顶层流程时，它名下各级子流程
// 里的处理人 / 抄送人同样算参与者——他们处理的是同一个业务对象。
type ParticipantChecker interface {
	IsParticipant(ctx context.Context, instanceID, userID string) (bool, error)
}

// UserInfo 用户展示信息的只读视图。
type UserInfo struct {
	ID       string
	Username string
	Nickname string
}

// Users 用户目录：由 system 域实现（依赖倒置）。
type Users interface {
	// UserNames 批量取用户显示名（昵称优先）；不存在的用户不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
	// Candidates 可用用户按关键字匹配，至多 limit 个。
	Candidates(ctx context.Context, keyword string, limit int64) ([]UserInfo, error)
	// PrimaryDept 用户的主部门 ID 与其全部所属部门 ID；无部门返回空。
	PrimaryDept(ctx context.Context, userID string) (primary string, all []string, err error)
}

// Listener 流程事件监听器：上层业务域实现，在引擎的同一事务内被调用（业务状态与
// 流程状态原子一致）。返回错误则整个命令回滚。
type Listener interface {
	// OnInstanceFinished 实例结束（status 为终态）。
	OnInstanceFinished(ctx context.Context, tx txrun.Tx, in *Instance) error
}

// Listeners 业务类型 → 监听器（装配期构造，只读）。登记的监听器还可以选择实现 Guard /
// ProgressListener，引擎在对应时机回调（同一事务）。
//
// OnInstanceFinished 只对挂着业务对象的顶层流程触发。Guard / ProgressListener 对子流程里的
// 任务同样触发（子流程的处理人面对的是同一个业务对象）：此时传入的是子流程实例，业务对象
// 一律取 Instance.RootBusinessID（顶层流程上它与 BusinessID 相同）。
type Listeners map[string]Listener

// Guard 可选能力：人工任务被「同意」之前询问业务域是否放行（如工单要求本环节必填的字段已填）。
// 返回错误则同意动作不生效。任务超时由系统自动同意之前也会询问（此时 userID 为空）：
// 不放行则不自动同意，改为提醒处理人。
type Guard interface {
	BeforeApprove(ctx context.Context, tx txrun.Tx, in *Instance, n *Node, userID string) error
}

// ProgressListener 可选能力：流程上有了处理人的实质动作（同意 / 拒绝 / 退回 / 转办 / 加签 /
// 委派 / 回复；发起人自己的动作不算）时通知业务域，at 为动作时间。
type ProgressListener interface {
	OnInstanceProgress(ctx context.Context, tx txrun.Tx, in *Instance, at time.Time) error
}

// VariableOwner 可选能力：业务域声明哪些流程变量归业务对象所有（如工单的表单字段与优先级）。
// 这些变量只能经业务域自己的入口修改（Amend：业务数据与流程变量同事务更新）；外部写入——
// 管理员投递信号时带的变量、自动任务（Webhook）返回的变量——不得改写它们，否则流程按新值
// 走分支、解析处理人，业务对象上显示的却还是旧值。
type VariableOwner interface {
	// OwnedVariables 返回 in 所属业务对象名下的变量名（in 可能是子流程，业务对象取 RootBusinessID）。
	OwnedVariables(ctx context.Context, tx txrun.Tx, in *Instance) ([]string, error)
	// CurrentVariables 返回这些变量此刻在业务对象上的取值（已清空的给 nil；不随业务对象变化的
	// 变量可以不给）。一棵流程树上的父子实例各有一份变量，业务数据却只有一份：引擎在每条
	// 命令执行前按它校准本实例的变量，于是在任何一个实例上做的修改，树上其它实例在自己的
	// 下一条命令里都会看到——不需要跨实例加锁，也不存在「传播延迟」。
	CurrentVariables(ctx context.Context, tx txrun.Tx, in *Instance) (map[string]any, error)
}

// Viewer 业务侧的实例查看权限：实例详情默认只对参与者开放，业务域可以按自己的规则
// 再放行一部分人（如工单的管理视图）。只在用户不是参与者时才会被询问。子流程实例按
// 根流程所挂的业务对象询问。
type Viewer interface {
	// CanViewInstance 报告用户能否查看业务对象 businessID 对应的流程实例。
	CanViewInstance(ctx context.Context, userID, businessID string) (bool, error)
}

// Viewers 业务类型 → 查看权限判定（装配期构造，只读）；未登记的业务类型只有参与者可看。
type Viewers map[string]Viewer

// RuntimeUsecase 流程运行时用例：每条写命令 = 一个事务（锁实例行 → 执行 → 回写 →
// 通知事件同事务直插 → 监听器），失败整体回滚。
type RuntimeUsecase struct {
	defs      *DefinitionUsecase
	store     InstanceStore
	reader    InstanceReader
	dir       Directory
	users     Users
	txs       txrun.Manager
	notify    biznotify.NotificationWriter
	listeners Listeners
	viewers   Viewers
	lookup    InstanceLookup
	proxies   Proxies
	calendar  *CalendarUsecase
	// duty 值班查询（值班人规则）；nil = 该规则解析为空。
	duty Duty
	// admins 系统管理员查询（「系统管理员」审批人规则）；nil = 该规则解析为空。
	admins Admins
}

// NewRuntimeUsecase new a RuntimeUsecase.
func NewRuntimeUsecase(defs *DefinitionUsecase, store InstanceStore, reader InstanceReader, lookup InstanceLookup, dir Directory,
	users Users, proxies Proxies, calendar *CalendarUsecase, txs txrun.Manager, notify biznotify.NotificationWriter,
	listeners Listeners, viewers Viewers, duty Duty, admins Admins,
) *RuntimeUsecase {
	return &RuntimeUsecase{
		defs: defs, store: store, reader: reader, lookup: lookup, dir: dir, users: users, proxies: proxies,
		calendar: calendar, txs: txs, notify: notify, listeners: listeners, viewers: viewers, duty: duty, admins: admins,
	}
}

// exec 构造一条命令的执行上下文：带上代理设置与当前的工作日历。
func (uc *RuntimeUsecase) exec(ctx context.Context, g *Graph, s *State) (*Execution, error) {
	cal, err := uc.calendar.Current(ctx)
	if err != nil {
		return nil, err
	}
	x := NewExecution(g, s, uc.dir, time.Now()).WithProxies(uc.proxies).WithCalendar(cal).WithDuty(uc.duty).WithAdmins(uc.admins)
	// 超时自动同意与人工同意走同一道业务校验（登记的监听器实现了 Guard 时）。
	if guard, ok := uc.business(&s.Instance).(Guard); ok {
		x.WithApproveGuard(func(ctx context.Context, n *Node) error {
			return guard.BeforeApprove(ctx, txrun.FromContext(ctx), &s.Instance, n, "")
		})
	}
	return x, nil
}

// syncOwned 命令执行前，把业务对象名下的变量校准为业务对象上的当前值（见 VariableOwner）。
// 只对进行中的实例做：已结束的实例不再流转，变量保持结束时的样子。
func (uc *RuntimeUsecase) syncOwned(ctx context.Context, x *Execution, s *State) error {
	in := &s.Instance
	owner, ok := uc.business(in).(VariableOwner)
	if !ok || in.Status != InstanceRunning {
		return nil
	}
	current, err := owner.CurrentVariables(ctx, txrun.FromContext(ctx), in)
	if err != nil {
		return err
	}
	if current, err = normalizeVariables(current); err != nil {
		return err
	}
	diff := map[string]any{}
	for name, v := range current {
		old, has := in.Variables[name]
		if (v == nil && old == nil) || (has && reflect.DeepEqual(old, v)) {
			continue
		}
		diff[name] = v
	}
	return x.mergeVars(diff)
}

// business 实例所属业务域登记的监听器：按根流程所挂的业务对象找（子流程自己不挂业务对象）；
// 没有业务对象或未登记返回 nil。
func (uc *RuntimeUsecase) business(in *Instance) Listener {
	if in.RootBusinessType == "" {
		return nil
	}
	return uc.listeners[in.RootBusinessType]
}

// checkVariables 校验流程变量可序列化、不超过大小上限，且不含 NUL 字符（PostgreSQL 的
// jsonb / text 不接受 \u0000，放进去会在落库时报错）。
func checkVariables(vars map[string]any) error {
	_, err := normalizeVariables(vars)
	return err
}

// normalizeVariables 校验流程变量（同 checkVariables）并规整为 JSON 取值：数字一律是
// float64、数组是 []any——与之后每条命令从库里读回的类型完全一致。不规整的话，发起那一条
// 命令里变量还带着 Go 的整数类型，条件表达式（CEL 不做隐式数值转换）在发起时与之后会得出
// 不同的结果。nil 原样返回。
func normalizeVariables(vars map[string]any) (map[string]any, error) {
	if vars == nil {
		return nil, nil
	}
	raw, err := json.Marshal(vars)
	if err != nil || len(raw) > maxVariablesBytes || bytes.Contains(raw, []byte(`\u0000`)) {
		return nil, ErrVariablesInvalid
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, ErrVariablesInvalid
	}
	return out, nil
}

// clean 去掉文本里的 NUL 字符（同上，text 列不接受）。
func clean(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}

// Start 发起一个独立流程（自开事务）。
func (uc *RuntimeUsecase) Start(ctx context.Context, p StartParams) (*Instance, error) {
	var in *Instance
	run := func() error {
		return uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			in, err = uc.StartTx(ctx, tx, p)
			return err
		})
	}
	err := run()
	if errors.Is(err, ErrBusinessDuplicate) && p.RequestID != "" {
		// 同一幂等键的并发请求抢先落库：对方已提交，再来一次即可按幂等键读到首次发起的实例
		// （若撞的是业务对象的唯一约束，重试仍会得到同一个错误）。
		err = run()
	}
	return in, err
}

// StartTx 在调用方事务内发起流程（跨域窄接口：业务域在自己的写事务里发起，业务
// 提交 = 流程已发起）。流程可能在发起时即走到结束（全部节点被跳过），监听器照常触发。
func (uc *RuntimeUsecase) StartTx(ctx context.Context, tx txrun.Tx, p StartParams) (*Instance, error) {
	return uc.start(ctx, tx, p, nil)
}

// start 发起流程；parent 非空时发起的是子流程（继承父流程的发起部门，不再校验归属）。
func (uc *RuntimeUsecase) start(ctx context.Context, tx txrun.Tx, p StartParams, parent *Instance) (*Instance, error) {
	// 事务内的只读查询（定义、组织目录）复用事务连接，不额外占用连接池。
	ctx = txrun.NewContext(ctx, tx)
	vars, err := normalizeVariables(p.Variables)
	if err != nil {
		return nil, err
	}
	if p.RequestID != "" {
		existing, err := uc.store.FindByRequestTx(ctx, tx, p.Initiator, p.RequestID)
		if err != nil || existing != nil {
			return existing, err
		}
	}
	if (p.BusinessType == "") != (p.BusinessID == "") {
		return nil, ErrVariablesInvalid
	}
	def, err := uc.defs.published(ctx, p.DefinitionKey)
	if err != nil {
		return nil, err
	}
	g, err := uc.defs.graph(ctx, def.PublishedVersionID)
	if err != nil {
		return nil, err
	}
	dept := strings.ToLower(p.InitiatorDept)
	if parent == nil {
		primary, all, err := uc.users.PrimaryDept(ctx, p.Initiator)
		if err != nil {
			return nil, err
		}
		switch {
		case dept == "":
			dept = primary
		case !slices.Contains(all, dept):
			return nil, errors.BadRequest(ErrVariablesInvalid.Reason, "initiator department must be one of the initiator's departments")
		}
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	// 标题去掉 NUL 后可能为空（通知域不接受空标题，之后每条通知都会失败）：退回流程名称。
	title := strings.TrimSpace(clean(p.Title))
	if title == "" {
		title = def.Name
	}
	s := &State{Instance: Instance{
		ID: id, DefinitionID: def.ID, DefinitionKey: def.Key, DefinitionName: def.Name,
		VersionID: def.PublishedVersionID, Version: def.PublishedVersion,
		BusinessType: p.BusinessType, BusinessID: p.BusinessID, Title: title,
		Initiator: p.Initiator, InitiatorDept: dept, Status: InstanceRunning,
		Variables: vars, StartTime: now, ClientID: p.clientID,
		RootID: id, RootBusinessType: p.BusinessType, RootBusinessID: p.BusinessID,
		FormKey: p.FormKey,
	}}
	if parent != nil {
		in := &s.Instance
		in.ParentInstanceID, in.Depth = parent.ID, parent.Depth+1
		in.ParentNodeID, in.ParentTokenID = p.parentNodeID, p.parentTokenID
		// 子流程继承根流程的业务对象引用与接入凭据（自己不挂业务对象）。
		in.RootID, in.RootBusinessType, in.RootBusinessID = parent.RootID, parent.RootBusinessType, parent.RootBusinessID
		in.ClientID = parent.ClientID
		// 子流程的处理人面对的是根流程的那张表单。
		in.FormKey = parent.FormKey
	}
	x, err := uc.exec(ctx, g, s)
	if err != nil {
		return nil, err
	}
	if err := x.Start(ctx); err != nil {
		return nil, err
	}
	if err := uc.store.CreateTx(ctx, tx, s, p.RequestID); err != nil {
		return nil, err
	}
	if err := uc.after(ctx, tx, s); err != nil {
		return nil, err
	}
	return &s.Instance, nil
}

// spawn 发起本次命令到达的子流程节点各自的子流程（同一事务）。子流程继承父流程的标题、
// 发起人、发起部门与流程变量；自己不挂业务对象（实例结束的监听只对顶层流程触发），
// 但带着根流程的业务对象引用。
func (uc *RuntimeUsecase) spawn(ctx context.Context, tx txrun.Tx, s *State) error {
	spawns := s.Spawns
	s.Spawns = nil
	in := &s.Instance
	for _, sp := range spawns {
		if in.Depth+1 > maxSubProcessDepth {
			return ErrSubProcessUnavailable
		}
		_, err := uc.start(ctx, tx, StartParams{
			DefinitionKey: sp.DefinitionKey, Title: in.Title, Initiator: in.Initiator, InitiatorDept: in.InitiatorDept,
			Variables: nonNilVars(in.Variables), parentNodeID: sp.NodeID, parentTokenID: sp.TokenID,
		}, in)
		if errors.Is(err, ErrDefinitionNotFound) || errors.Is(err, ErrDefinitionUnavailable) || errors.Is(err, ErrVersionInvalid) {
			return ErrSubProcessUnavailable
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// command 对一个实例执行一条写命令的公共骨架。needGraph=false 用于撤回 / 终止：它们
// 不推进流程，不依赖流程图——即便某个历史版本的流程图已无法通过当前的校验规则，
// 实例仍然可以被结束。
func (uc *RuntimeUsecase) command(ctx context.Context, instanceID string, needGraph bool, fn func(ctx context.Context, x *Execution) error) (int32, error) {
	var status int32
	err := uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		ctx := txrun.NewContext(ctx, tx)
		s, err := uc.store.LoadForUpdateTx(ctx, tx, instanceID)
		if err != nil {
			return err
		}
		var g *Graph
		if needGraph {
			if g, err = uc.defs.graph(ctx, s.Instance.VersionID); err != nil {
				return err
			}
		}
		x, err := uc.exec(ctx, g, s)
		if err != nil {
			return err
		}
		if err := uc.syncOwned(ctx, x, s); err != nil {
			return err
		}
		if err := fn(ctx, x); err != nil {
			return err
		}
		if err := uc.store.SaveTx(ctx, tx, s); err != nil {
			return err
		}
		status = s.Instance.Status
		return uc.after(ctx, tx, s)
	})
	return status, err
}

// taskCommand 按任务 ID 定位实例后执行命令。
func (uc *RuntimeUsecase) taskCommand(ctx context.Context, taskID string, fn func(ctx context.Context, x *Execution) error) (int32, error) {
	instanceID, err := uc.reader.InstanceIDOfTask(ctx, taskID)
	if err != nil {
		return 0, err
	}
	return uc.command(ctx, instanceID, true, fn)
}

// after 命令成功后的同事务收尾：发起子流程、直插通知事件、触发业务监听器。
func (uc *RuntimeUsecase) after(ctx context.Context, tx txrun.Tx, s *State) error {
	if err := uc.spawn(ctx, tx, s); err != nil {
		return err
	}
	in := &s.Instance
	for _, n := range s.Notices {
		eventType, title := EventTaskAssigned, in.Title
		switch n.Kind {
		case NoticeTaskCC:
			eventType = EventTaskCC
		case NoticeInstanceFinished:
			eventType = EventInstanceFinished
		case NoticeTaskRemind:
			eventType = EventTaskRemind
		case NoticeServiceFailed:
			eventType = EventServiceFailed
		}
		payload, err := json.Marshal(map[string]any{
			"instanceId": in.ID, "title": in.Title, "definitionName": in.DefinitionName,
			"nodeName": n.NodeName, "taskId": n.TaskID, "status": in.Status,
			"initiator": in.Initiator, "businessType": in.RootBusinessType, "businessId": in.RootBusinessID,
		})
		if err != nil {
			return err
		}
		// 不带 GroupKey：通知域对带分组键的事件可以按路由配置做聚合，聚合后的摘要只发给路由
		// 自己的收件人——事件显式指定的处理人会被丢掉。这些通知都是发给特定的人的。
		for chunk := range slices.Chunk(n.Recipients, notifyChunk) {
			if _, err := uc.notify.NotifyEvent(ctx, tx, biznotify.Event{
				BizID: biznotify.DefaultBizID, EventType: eventType, Severity: severityInfo,
				Title: truncate(title, maxNoticeTitle), Payload: payload, Recipients: chunk,
			}); err != nil {
				return err
			}
		}
	}
	l := uc.business(in)
	if l == nil {
		return nil
	}
	if p, ok := l.(ProgressListener); ok {
		for _, h := range s.Histories {
			if h.Operator != "" && h.Operator != in.Initiator && progressActions[h.Action] {
				if err := p.OnInstanceProgress(ctx, tx, in, h.CreateTime); err != nil {
					return err
				}
				break
			}
		}
	}
	// 实例结束的监听只对挂着业务对象的顶层流程触发（子流程结束不等于业务对象结束）。
	if in.Status != InstanceRunning && in.BusinessType != "" {
		return l.OnInstanceFinished(ctx, tx, in)
	}
	return nil
}

// progressActions 算作「处理人已响应」的动作。
var progressActions = map[string]bool{
	ActionApprove: true, ActionReject: true, ActionReturn: true, ActionTransfer: true,
	ActionAddSign: true, ActionDelegate: true, ActionReply: true,
}

// guard 同意之前询问业务域（登记的监听器实现了 Guard 时）。只对确有待处理席位的人询问：
// 不是处理人的请求由引擎自己拒绝，不该先拿到业务侧的提示。
func (uc *RuntimeUsecase) guard(ctx context.Context, x *Execution, taskID, userID string) error {
	in := x.Instance()
	g, ok := uc.business(in).(Guard)
	if !ok {
		return nil
	}
	n := x.TaskNode(taskID)
	if _, seated := x.SeatPermissions(userID); n == nil || !seated {
		return nil // 任务不存在、不是处理人等情况交给引擎自己报错
	}
	return g.BeforeApprove(ctx, txrun.FromContext(ctx), in, n, userID)
}

// ownedIn 报告外部写入的变量 vars 里，哪些归实例所属的业务对象所有（见 VariableOwner）；
// 业务域未声明时为空。结果按变量名升序。
func (uc *RuntimeUsecase) ownedIn(ctx context.Context, in *Instance, vars map[string]any) ([]string, error) {
	owner, ok := uc.business(in).(VariableOwner)
	if !ok || len(vars) == 0 {
		return nil, nil
	}
	owned, err := owner.OwnedVariables(ctx, txrun.FromContext(ctx), in)
	if err != nil {
		return nil, err
	}
	var hit []string
	for _, name := range owned {
		if _, sent := vars[name]; sent {
			hit = append(hit, name)
		}
	}
	slices.Sort(hit)
	return slices.Compact(hit), nil
}

// Amend 由当前处理人修改流程变量：userID 须有待处理的任务——在 instanceID 这个实例上，或在
// 它所在流程树的某个子流程上（此时锁的是他任务所在的那个实例）。fn 在锁住该实例行的事务内
// 被调用，拿到他在整棵流程树上待处理任务所在节点的字段权限并集（字段码 → hidden /
// readonly / editable / required），返回要并入流程变量的键值（业务域在 fn 里同事务更新
// 自己的数据）。父子实例各有一份变量：这里只写被锁的那个实例，树上其它实例在各自的下一条
// 命令执行前按业务对象校准（见 VariableOwner.CurrentVariables）。
func (uc *RuntimeUsecase) Amend(ctx context.Context, instanceID, userID string,
	fn func(ctx context.Context, tx txrun.Tx, perms map[string]string) (map[string]any, error),
) error {
	refs, err := uc.lookup.SeatsInTree(ctx, instanceID, userID)
	if err != nil {
		return err
	}
	if i := slices.IndexFunc(refs, func(r SeatRef) bool { return r.Pending }); i >= 0 {
		instanceID = refs[i].InstanceID
	}
	// 他在树上其它实例的待处理任务所在节点：事务外读（不同时锁父子实例），与被锁实例上的
	// 节点合并出权限——与 FormAccess 给前端的口径一致。
	var elsewhere []*Node
	for _, ref := range refs {
		if !ref.Pending || ref.InstanceID == instanceID {
			continue
		}
		nodes, _, _, err := uc.seatNodesOf(ctx, ref.InstanceID, userID, nil)
		if err != nil {
			return err
		}
		elsewhere = append(elsewhere, nodes...)
	}
	_, err = uc.command(ctx, instanceID, true, func(ctx context.Context, x *Execution) error {
		own := x.PendingNodes(userID)
		if len(own) == 0 {
			return ErrTaskNotAssignee
		}
		// 一棵流程树上的实例用的是同一张表单。
		perms := PermissionsOf(append(own, elsewhere...), x.Instance().FormKey)
		vars, err := fn(ctx, txrun.FromContext(ctx), perms)
		if err != nil {
			return err
		}
		if err := x.mergeVars(vars); err != nil {
			return err
		}
		// 留痕：谁改了哪些字段（这些值会影响后续的分支与处理人）。
		fields := slices.Sorted(maps.Keys(vars))
		return x.history(nil, "", ActionAmend, userID, "", map[string]any{"fields": fields})
	})
	return err
}

// FormAccess 某用户对实例表单字段的权限视图。
type FormAccess struct {
	// Pending 他当前有待处理的任务；Permissions 为这些任务所在节点的字段权限
	//（字段码 → hidden / readonly / editable / required）。
	Pending     bool
	Permissions map[string]string
	// Seated 他在流程上有过席位（处理 / 抄送）；Hidden 为应当对他隐藏的字段。
	Seated bool
	Hidden []string
	// HideAll 他有席位，但所在流程版本的流程图已无法解析（校验规则收紧后的存量版本）：
	// 判断不了哪些字段该对他隐藏，调用方应当按「全部隐藏」处理。
	HideAll bool
}

// FormAccess 用户对该实例表单字段的权限视图（业务域据此决定哪些字段可改、哪些不给看）。
// 席位按整棵流程树看：他的任务在子流程里时，权限取自子流程的节点。
func (uc *RuntimeUsecase) FormAccess(ctx context.Context, instanceID, userID string) (*FormAccess, error) {
	return uc.formAccess(ctx, instanceID, userID, nil)
}

// formAccess 见 FormAccess；loaded 非空时是调用方已经读出的 instanceID 的执行状态（免再读一遍）。
// Permissions 与 Amend 同口径：他在整棵树上待处理任务所在节点的权限并集。
func (uc *RuntimeUsecase) formAccess(ctx context.Context, instanceID, userID string, loaded *State) (*FormAccess, error) {
	refs, err := uc.lookup.SeatsInTree(ctx, instanceID, userID)
	if err != nil {
		return nil, err
	}
	fa := &FormAccess{Seated: len(refs) > 0}
	var (
		nodes, pending []*Node
		// 一棵流程树上的实例用的是同一张表单：取任意一个读到的即可。
		form string
	)
	for _, ref := range refs {
		p, seated, key, err := uc.seatNodesOf(ctx, ref.InstanceID, userID, loaded)
		if errors.Is(err, ErrVersionInvalid) {
			fa.HideAll = true
			continue
		}
		if err != nil {
			return nil, err
		}
		form = key
		pending = append(pending, p...)
		nodes = append(nodes, seated...)
	}
	if len(pending) > 0 {
		fa.Pending, fa.Permissions = true, PermissionsOf(pending, form)
	}
	fa.Hidden = HiddenOf(nodes, form)
	return fa, nil
}

// seatNodesOf 用户在某个实例上待处理任务所在的节点、有过席位的节点，以及该实例所用表单的
// 标识（只读快照）。loaded 非空且正是该实例时免再读一遍。流程图已无法解析时返回 ErrVersionInvalid。
func (uc *RuntimeUsecase) seatNodesOf(ctx context.Context, instanceID, userID string, loaded *State) (pending, seated []*Node, form string, err error) {
	s := loaded
	if s == nil || s.Instance.ID != instanceID {
		if s, err = uc.lookup.Snapshot(ctx, instanceID); err != nil {
			return nil, nil, "", err
		}
	}
	g, err := uc.defs.graph(ctx, s.Instance.VersionID)
	if err != nil {
		return nil, nil, "", err
	}
	x := NewExecution(g, s, uc.dir, time.Now())
	return x.PendingNodes(userID), x.SeatedNodes(userID), s.Instance.FormKey, nil
}

// PendingUsers 实例上当前待处理的用户（业务域发催办 / 超时通知用）。
func (uc *RuntimeUsecase) PendingUsers(ctx context.Context, instanceID string) ([]string, error) {
	return uc.lookup.PendingUsers(ctx, instanceID)
}

// Admins 实例所用流程版本配置的流程管理员（只含可用的用户）。业务域在流程上无人待处理时
// 以他们为兜底通知对象。流程图已无法解析时返回空。
func (uc *RuntimeUsecase) Admins(ctx context.Context, instanceID string) ([]string, error) {
	s, err := uc.lookup.Snapshot(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	g, err := uc.defs.graph(ctx, s.Instance.VersionID)
	if errors.Is(err, ErrVersionInvalid) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return uc.dir.ActiveUserIDs(ctx, g.Settings.Admins)
}

// truncate 截到至多 n 字节且不切断字符（通知标题按字节限长：按字符数截会让多字节文字超限）。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// Approve 同意任务；返回动作后的实例状态。
func (uc *RuntimeUsecase) Approve(ctx context.Context, taskID, userID, comment string) (int32, error) {
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		if err := uc.guard(ctx, x, taskID, userID); err != nil {
			return err
		}
		return x.Approve(ctx, taskID, userID, clean(comment))
	})
}

// Reject 拒绝任务。
func (uc *RuntimeUsecase) Reject(ctx context.Context, taskID, userID, comment string) (int32, error) {
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		return x.Reject(ctx, taskID, userID, clean(comment))
	})
}

// Return 退回到此前走过的节点。
func (uc *RuntimeUsecase) Return(ctx context.Context, taskID, userID, toNodeID, comment string) (int32, error) {
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		return x.Return(ctx, taskID, userID, toNodeID, clean(comment))
	})
}

// Transfer 转办。
func (uc *RuntimeUsecase) Transfer(ctx context.Context, taskID, userID, toUserID, comment string) (int32, error) {
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		return x.Transfer(ctx, taskID, userID, strings.ToLower(toUserID), clean(comment))
	})
}

// Reassign 管理员改派（接口权限由授权中间件判定）。
func (uc *RuntimeUsecase) Reassign(ctx context.Context, taskID, operator string, userIDs []string, reason string) (int32, error) {
	ids := make([]string, len(userIDs))
	for i, id := range userIDs {
		ids[i] = strings.ToLower(id)
	}
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		return x.Reassign(ctx, taskID, operator, ids, clean(reason))
	})
}

// AddSign 加签（position = before / after）。
func (uc *RuntimeUsecase) AddSign(ctx context.Context, taskID, userID string, userIDs []string, position, comment string) (int32, error) {
	ids := lowerAll(userIDs)
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		if position == OriginAfter { // 后加签即自己现在同意
			if err := uc.guard(ctx, x, taskID, userID); err != nil {
				return err
			}
		}
		return x.AddSign(ctx, taskID, userID, ids, position, clean(comment))
	})
}

// Delegate 委派。
func (uc *RuntimeUsecase) Delegate(ctx context.Context, taskID, userID, toUserID, comment string) (int32, error) {
	return uc.taskCommand(ctx, taskID, func(ctx context.Context, x *Execution) error {
		return x.Delegate(ctx, taskID, userID, strings.ToLower(toUserID), clean(comment))
	})
}

// Signal 向实例投递一个外部信号，vars 并入流程变量。
func (uc *RuntimeUsecase) Signal(ctx context.Context, instanceID, name string, vars map[string]any) (int32, error) {
	if err := checkVariables(vars); err != nil {
		return 0, err
	}
	return uc.command(ctx, instanceID, true, func(ctx context.Context, x *Execution) error {
		// 业务对象名下的变量（如工单的表单字段）不能经信号改写：明确拒绝，而不是悄悄丢弃。
		owned, err := uc.ownedIn(ctx, x.Instance(), vars)
		if err != nil {
			return err
		}
		if len(owned) > 0 {
			return errors.BadRequest(ErrVariablesInvalid.Reason,
				fmt.Sprintf("variable %q belongs to the business form and cannot be set by a signal", owned[0]))
		}
		return x.Signal(ctx, name, vars)
	})
}

// Handover 管理员交接：把 from 名下全部未了结的待办席位换给 to（离职 / 调岗）。一次请求
// 至多处理 handoverBatch 个实例（按实例 ID 升序，从 cursor 之后开始；空 = 从头）；next 非空
// 表示还有剩余，调用方应带着它再次请求。各实例各自一个事务：某个实例失败不影响其余实例。
// 返回已交接的席位数与失败的实例数——失败的实例留在原处理人名下，游标照常越过它们
// （否则调用方会对着同一批失败的实例空转）；err 只表示请求本身不成立。
//
// 请求有时间上限（HTTP 超时），而每个实例各是一个事务：时间用尽后没轮到的实例不算失败，
// 也不被游标越过——next 停在最后一个处理过的实例上（一个都没处理则沿用入参游标），调用方
// 带着它再次请求时从断点继续。
func (uc *RuntimeUsecase) Handover(ctx context.Context, from, to, operator, reason, cursor string) (moved, failed int, next string, err error) {
	from, to, reason, cursor = strings.ToLower(from), strings.ToLower(to), clean(reason), strings.ToLower(cursor)
	if from == to || (cursor != "" && !isUUID(cursor)) {
		return 0, 0, "", ErrTaskTargetInvalid
	}
	active, err := uc.dir.ActiveUserIDs(ctx, []string{to})
	if err != nil {
		return 0, 0, "", err
	}
	if len(active) != 1 {
		return 0, 0, "", ErrTaskTargetInvalid
	}
	ids, err := uc.lookup.InstancesWithSeats(ctx, from, cursor, handoverBatch+1)
	if err != nil {
		return 0, 0, "", err
	}
	more := len(ids) > handoverBatch
	if more {
		ids = ids[:handoverBatch]
	}
	processed := 0
	for i, id := range ids {
		if !handoverTimeLeft(ctx) {
			break
		}
		n := 0
		if _, cerr := uc.command(ctx, id, true, func(ctx context.Context, x *Execution) (herr error) {
			n, herr = x.Handover(ctx, from, to, operator, reason)
			return herr
		}); cerr != nil {
			if ctx.Err() != nil {
				// 被超时 / 取消打断的事务已回滚：这个实例没有处理结果，不算失败。
				break
			}
			failed++
		} else {
			moved += n
		}
		processed = i + 1
	}
	switch {
	case processed == len(ids) && !more:
		// 全部处理完，没有更多。
	case processed > 0:
		next = ids[processed-1]
	case cursor != "":
		next = cursor
	default:
		// 一个实例都没来得及处理，又没有游标可返回：如实报告本次请求没有完成。调用方从头再试
		// 即可——交接过的实例不再有 from 的席位，不会被重复交接。
		cause := ctx.Err()
		if cause == nil {
			cause = context.DeadlineExceeded
		}
		return 0, 0, "", fmt.Errorf("workflow handover made no progress before the request deadline: %w", cause)
	}
	return moved, failed, next, nil
}

// handoverTimeLeft 报告是否还来得及开始处理下一个实例：请求已取消 / 超时，或已知截止时间
// 且剩余不足 handoverReserve 时为假。
func handoverTimeLeft(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) > handoverReserve
}

func lowerAll(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strings.ToLower(id)
	}
	return out
}

// Cancel 发起人撤回。
func (uc *RuntimeUsecase) Cancel(ctx context.Context, instanceID, userID, comment string) error {
	_, err := uc.command(ctx, instanceID, false, func(_ context.Context, x *Execution) error {
		return x.Cancel(userID, clean(comment))
	})
	return err
}

// Terminate 管理员终止（接口权限由授权中间件判定）。
func (uc *RuntimeUsecase) Terminate(ctx context.Context, instanceID, operator, reason string) error {
	_, err := uc.command(ctx, instanceID, false, func(_ context.Context, x *Execution) error {
		return x.Terminate(operator, clean(reason))
	})
	return err
}

// InstanceDetail 实例详情。
type InstanceDetail struct {
	State *State
	Graph json.RawMessage
	// MyTaskID 查看者当前可处理的任务；没有为空。
	MyTaskID string
	// Names 涉及用户的显示名。
	Names map[string]string
	// Children 由子流程节点发起的子流程。
	Children []*ChildRef
	// Signals 实例正停在其上、等待外部信号的定时节点。
	Signals []SignalWait
}

// SignalWait 一个正在等待外部信号的定时节点。
type SignalWait struct {
	NodeID string
	Signal string
}

// involved 报告用户是否参与了实例：发起人、当前或曾经处理过的处理人、抄送人。被取消
// 席位的人（改派走、任务被他人处理而未轮到）没有参与，不因此获得查看权。
func involved(s *State, userID string) bool {
	if s.Instance.Initiator == userID {
		return true
	}
	for _, t := range s.Tasks {
		if slices.ContainsFunc(t.Assignees, func(a *TaskAssignee) bool {
			// 被代理人也算参与者：代理人替他处理的待办，他本人可以查看。
			return (a.UserID == userID || a.OnBehalf == userID) && a.Status != AssigneeCancelled
		}) {
			return true
		}
	}
	return false
}

// seatedOnAncestors 报告查看者是否在子流程的某个祖先实例（父实例及其上级，直到根流程）上
// 处理过或抄送过任务（席位未被取消，被代理人也算）。发起人不必另查：子流程的发起人继承自
// 父流程，involved 已判过。只对子流程实例成立；祖先链按 parent_instance_id 逐级上溯，步数
// 以嵌套深度为界。
func (uc *RuntimeUsecase) seatedOnAncestors(ctx context.Context, in *Instance, userID string) (bool, error) {
	if in.ParentInstanceID == "" {
		return false, nil
	}
	refs, err := uc.lookup.SeatsInTree(ctx, in.ID, userID)
	if err != nil || len(refs) == 0 {
		return false, err
	}
	seated := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		seated[ref.InstanceID] = struct{}{}
	}
	for id, left := in.ParentInstanceID, in.Depth; id != "" && left > 0; left-- {
		if _, ok := seated[id]; ok {
			return true, nil
		}
		if id == in.RootID {
			return false, nil
		}
		parent, err := uc.lookup.GetInstance(ctx, id)
		if errors.Is(err, ErrInstanceNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		id = parent.ParentInstanceID
	}
	return false, nil
}

// Get 查询实例详情。viewAll=false 时只有参与者（发起人 / 处理人 / 抄送人）以及业务域
// 放行的人可看；不挂业务对象时，在祖先实例上有席位的查看者可以打开子流程的详情（只读）。
func (uc *RuntimeUsecase) Get(ctx context.Context, instanceID, userID string, viewAll bool) (*InstanceDetail, error) {
	s, err := uc.reader.Load(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if !viewAll && !involved(s, userID) {
		// 子流程按根流程所挂的业务对象询问业务域（工单的管理视图、同一张工单上的其他参与者）。
		viewer := uc.viewers[s.Instance.RootBusinessType]
		if viewer == nil || s.Instance.RootBusinessType == "" {
			// 没有业务域的查看规则可问（不挂业务对象的独立流程）：只按本实例的参与关系判，父流程里
			// 处理过任务的人在子流程上没有席位，会被拒在子流程详情之外。子流程是父流程的一部分，
			// 他在祖先实例上的席位同样放行；只放行查看，写操作仍只认本实例上的待处理席位。
			ok, err := uc.seatedOnAncestors(ctx, &s.Instance, userID)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, ErrInstanceForbidden
			}
		} else if ok, err := viewer.CanViewInstance(ctx, userID, s.Instance.RootBusinessID); err != nil {
			return nil, err
		} else if !ok {
			return nil, ErrInstanceForbidden
		}
	}
	graph, err := uc.defs.GraphJSON(ctx, s.Instance.VersionID)
	if err != nil {
		return nil, err
	}
	// 查看者不是发起人、也没有管理视图：他若在流程树上有席位（处理人 / 抄送人），节点上对他
	// 隐藏的表单字段在流程变量里同样不给看。流程图已无法解析时判断不了，整份变量都不给。
	if !viewAll && s.Instance.Initiator != userID && len(s.Instance.Variables) > 0 {
		fa, err := uc.formAccess(ctx, instanceID, userID, s)
		if err != nil {
			return nil, err
		}
		switch {
		case fa.HideAll:
			s.Instance.Variables = map[string]any{}
		case len(fa.Hidden) > 0:
			vars := maps.Clone(s.Instance.Variables)
			for _, field := range fa.Hidden {
				delete(vars, field)
			}
			s.Instance.Variables = vars
		}
	}
	d := &InstanceDetail{State: s, Graph: graph}
	if d.Children, err = uc.lookup.Children(ctx, instanceID); err != nil {
		return nil, err
	}
	// 等信号的定时节点没有作业，从执行位置推出来；存量版本的流程图解析不了时略过（只影响展示）。
	if g, gerr := uc.defs.graph(ctx, s.Instance.VersionID); gerr == nil && s.Instance.Status == InstanceRunning {
		for _, t := range s.Tokens {
			if n := g.Node(t.NodeID); t.Status == TokenActive && n != nil && n.Type == NodeTimer && n.Timer.Mode == TimerSignal {
				d.Signals = append(d.Signals, SignalWait{NodeID: n.ID, Signal: n.Timer.Signal})
			}
		}
	}
	ids := []string{s.Instance.Initiator}
	for _, t := range s.Tasks {
		for _, a := range t.Assignees {
			ids = append(ids, a.UserID, a.OnBehalf)
			if t.Status == TaskPending && a.UserID == userID && a.Status == AssigneePending {
				d.MyTaskID = t.ID
			}
		}
	}
	for _, h := range s.Histories {
		if h.Operator != "" {
			ids = append(ids, h.Operator)
		}
	}
	if d.Names, err = uc.users.UserNames(ctx, uniqueStrings(ids)); err != nil {
		return nil, err
	}
	return d, nil
}

// ReturnTargets 当前任务可退回到的节点（此前走过、同一并行区域内的人工任务），最近到达的在前。
func (uc *RuntimeUsecase) ReturnTargets(ctx context.Context, taskID, userID string) ([]*Node, error) {
	instanceID, err := uc.reader.InstanceIDOfTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	s, err := uc.reader.Load(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(s.Tasks, func(t *Task) bool { return t.ID == taskID })
	if i < 0 {
		return nil, ErrTaskNotFound
	}
	cur := s.Tasks[i]
	if !slices.ContainsFunc(cur.Assignees, func(a *TaskAssignee) bool {
		return a.UserID == userID && a.Status == AssigneePending
	}) {
		return nil, ErrTaskNotAssignee
	}
	g, err := uc.defs.graph(ctx, s.Instance.VersionID)
	if err != nil {
		return nil, err
	}
	return ReturnTargets(g, s, cur), nil
}

// ListInstances 分页查询实例。
func (uc *RuntimeUsecase) ListInstances(ctx context.Context, filter InstanceFilter, page, pageSize int64) ([]*InstanceItem, map[string]string, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = clean(filter.Keyword)
	items, total, err := uc.reader.ListInstances(ctx, filter, page, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Initiator)
	}
	names, err := uc.users.UserNames(ctx, uniqueStrings(ids))
	return items, names, total, err
}

// ListTasks 分页查询某用户的待办 / 已办 / 抄送。
func (uc *RuntimeUsecase) ListTasks(ctx context.Context, userID, view, keyword, businessType string, page, pageSize int64) ([]*TaskItem, map[string]string, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.reader.ListTasks(ctx, userID, view, clean(keyword), businessType, page, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.Initiator)
	}
	names, err := uc.users.UserNames(ctx, uniqueStrings(ids))
	return items, names, total, err
}

// Candidates 可选用户（转办 / 改派 / 设计器选人）。
func (uc *RuntimeUsecase) Candidates(ctx context.Context, keyword string) ([]UserInfo, error) {
	return uc.users.Candidates(ctx, keyword, candidateLimit)
}

// uniqueStrings 去重并保持首次出现顺序。
func uniqueStrings(items []string) []string {
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, s := range items {
		if _, ok := seen[s]; ok || s == "" {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
