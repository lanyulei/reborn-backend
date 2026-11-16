package biz

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/go-kratos/kratos/v3/errors"
)

// 实例状态（workflow_instance.status）。
const (
	InstanceRunning    int32 = 1
	InstanceCompleted  int32 = 2
	InstanceRejected   int32 = 3
	InstanceCancelled  int32 = 4
	InstanceTerminated int32 = 5
)

// 执行位置状态（workflow_token.status）。
const (
	// TokenActive 位于某节点上（人工任务等待处理）。
	TokenActive int32 = 1
	// TokenWaiting 已到达并行汇聚，等待其余分支。
	TokenWaiting int32 = 2
	// TokenDone 已消耗。
	TokenDone int32 = 3
)

// 任务类型（workflow_task.kind）。
const (
	TaskKindApprove = "approve"
	TaskKindCC      = "cc"
)

// 任务状态（workflow_task.status）。
const (
	TaskPending   int32 = 1
	TaskApproved  int32 = 2
	TaskRejected  int32 = 3
	TaskCancelled int32 = 4
	TaskReturned  int32 = 5
	// TaskNotified 抄送任务（创建即终态，不阻塞流程）。
	TaskNotified int32 = 6
)

// 处理人状态（workflow_task_assignee.status）。
const (
	// AssigneePending 待处理（出现在待办里）。
	AssigneePending int32 = 1
	// AssigneeQueued 依次审批中尚未轮到。
	AssigneeQueued      int32 = 2
	AssigneeApproved    int32 = 3
	AssigneeRejected    int32 = 4
	AssigneeCancelled   int32 = 5
	AssigneeTransferred int32 = 6
	AssigneeReturned    int32 = 7
	// AssigneeNotified 抄送接收人。
	AssigneeNotified int32 = 8
	// AssigneeSuspended 已前加签 / 委派给别人，等对方处理完再回到自己手上。
	AssigneeSuspended int32 = 9
)

// 席位来源（workflow_task_assignee.origin）：决定该席位处理完之后如何接续。
const (
	// OriginBefore 前加签：处理完后，加签人（owner）的席位恢复待处理。
	OriginBefore = "before"
	// OriginAfter 后加签：加签人已同意，本席位处理完后加签人的同意才生效。
	OriginAfter = "after"
	// OriginDelegate 委派：处理完（同意 / 拒绝都只是给出意见）后回到委派人手上。
	OriginDelegate = "delegate"
)

// 作业类型（workflow_job.kind）。
const (
	JobTimer   = "timer"
	JobTimeout = "timeout"
	JobRemind  = "remind"
	JobService = "service"
	// JobResume 子流程结束，唤醒父流程（挂在父实例上）。
	JobResume = "resume"
	// JobStop 父流程已结束，终止仍在进行的子流程（挂在子实例上）。
	JobStop = "stop"
)

// 作业状态（workflow_job.status）。
const (
	JobPending   int32 = 1
	JobDone      int32 = 2
	JobCancelled int32 = 3
	JobDead      int32 = 4
)

// 历史动作（workflow_history.action）。
const (
	ActionStart     = "start"
	ActionApprove   = "approve"
	ActionReject    = "reject"
	ActionReturn    = "return"
	ActionTransfer  = "transfer"
	ActionReassign  = "reassign"
	ActionSkip      = "skip"
	ActionCC        = "cc"
	ActionCancel    = "cancel"
	ActionTerminate = "terminate"
	ActionComplete  = "complete"
	// ActionAddSign 加签（detail.position = before / after）。
	ActionAddSign = "addSign"
	// ActionDelegate 委派；ActionReply 被委派人给出意见（detail.agree）。
	ActionDelegate = "delegate"
	ActionReply    = "reply"
	// ActionHandover 管理员把某人的待办交接给另一人。
	ActionHandover = "handover"
	// ActionTimeout 任务超时后系统执行的动作（detail.action）。
	ActionTimeout = "timeout"
	// ActionTimer 定时节点到期 / 收到信号后继续。
	ActionTimer = "timer"
	// ActionService 自动任务执行成功；ActionServiceFailed 重试耗尽进入死信。
	ActionService       = "service"
	ActionServiceFailed = "serviceFailed"
	// ActionSubProcess 发起子流程 / 子流程结束（detail.child、detail.status）。
	ActionSubProcess = "subProcess"
	// ActionConditionError 网关选路时有分支条件求值出错（detail.edges：[{edge, error}]）；
	// 出错的条件按不命中处理，流程照常继续。
	ActionConditionError = "conditionError"
)

// 通知种类（引擎只产出语义，投递由用例层经通知域完成）。
const (
	NoticeTaskAssigned     = "task.assigned"
	NoticeTaskCC           = "task.cc"
	NoticeInstanceFinished = "instance.finished"
	// NoticeTaskRemind 任务即将超时 / 已超时的提醒。
	NoticeTaskRemind = "task.remind"
	// NoticeServiceFailed 自动任务进入死信，通知流程管理员处理。
	NoticeServiceFailed = "service.failed"
)

// 执行上限：单次命令的推进步数、单实例的任务总数（防回路失控）。
const (
	maxAdvanceSteps     = 200
	maxTasksPerInstance = 500
	maxAssigneesPerTask = 100
	// maxAutoSkips 单实例自动跳过节点（处理人为空、定时已过）的累计上限。回路里的人工任务
	// 一旦解析不出处理人就不再阻塞：回路经定时节点每个周期空转一圈，历史与作业无限增长。
	// 到上限即拒绝继续推进（作业转死信并通知流程管理员），由人工处理。
	maxAutoSkips = 500
)

// 哨兵错误：reason 来自 api/workflow/v1 错误码枚举。
var (
	ErrInstanceNotFound   = errors.NotFound(v1.ErrorReason_WORKFLOW_INSTANCE_NOT_FOUND.String(), "workflow instance not found")
	ErrInstanceNotRunning = errors.Conflict(v1.ErrorReason_WORKFLOW_INSTANCE_NOT_RUNNING.String(), "workflow instance is not running")
	ErrInstanceForbidden  = errors.Forbidden(v1.ErrorReason_WORKFLOW_INSTANCE_FORBIDDEN.String(), "no permission on this workflow instance")
	ErrInstanceLimit      = errors.Conflict(v1.ErrorReason_WORKFLOW_INSTANCE_LIMIT.String(), "workflow instance exceeded its step or task limit")
	ErrTaskNotFound       = errors.NotFound(v1.ErrorReason_WORKFLOW_TASK_NOT_FOUND.String(), "workflow task not found")
	ErrTaskNotPending     = errors.Conflict(v1.ErrorReason_WORKFLOW_TASK_NOT_PENDING.String(), "workflow task is not pending for you")
	ErrTaskNotAssignee    = errors.Forbidden(v1.ErrorReason_WORKFLOW_TASK_NOT_ASSIGNEE.String(), "you are not an assignee of this task")
	ErrTaskTargetInvalid  = errors.BadRequest(v1.ErrorReason_WORKFLOW_TASK_TARGET_INVALID.String(), "invalid transfer user or return node")
	ErrSignalNotAwaited   = errors.Conflict(v1.ErrorReason_WORKFLOW_SIGNAL_NOT_AWAITED.String(), "no node of this instance is waiting for the signal")
)

// Instance 流程实例。
type Instance struct {
	ID             string
	DefinitionID   string
	DefinitionKey  string
	DefinitionName string
	VersionID      string
	Version        int32
	BusinessType   string
	BusinessID     string
	Title          string
	Initiator      string
	InitiatorDept  string
	Status         int32
	// Variables 流程变量（表单数据）；条件表达式里以 form 引用。
	Variables map[string]any
	StartTime time.Time
	EndTime   time.Time
	// ParentInstanceID / ParentNodeID / ParentTokenID 由子流程节点发起时指向父流程里等它的位置。
	ParentInstanceID string
	ParentNodeID     string
	ParentTokenID    string
	// Depth 子流程嵌套深度，0 = 顶层。
	Depth int32
	// ClientID 经开放接口发起时的接入凭据 ID（子流程继承父流程的）。
	ClientID string
	// RootID 根流程实例 ID：顶层流程为自身，子流程为调用链最顶层的那个实例。
	RootID string
	// RootBusinessType / RootBusinessID 根流程所挂的业务对象（子流程继承，只读）：子流程自己
	// 不挂业务对象（BusinessType / BusinessID 为空），但它的处理人面对的仍是根流程的那个
	// 业务对象。顶层流程上与 BusinessType / BusinessID 相同。
	RootBusinessType string
	RootBusinessID   string
	// FormKey 实例所用表单的标识（发起时由业务域给出，如工单的模板 ID；子流程继承根流程的）。
	// 节点上按表单区分的字段权限据此取用；空 = 只用通用的字段权限。
	FormKey string
	// AutoSkips 因处理人为空 / 定时已过而自动跳过节点的累计次数（见 maxAutoSkips）。
	AutoSkips int32
}

// Token 执行位置：并行分支各持一个。
type Token struct {
	ID     string
	NodeID string
	Status int32
	// Scope 包容网关的激活栈（见 pushScope）：汇聚据栈顶知道这一次要等几条分支。
	Scope string
}

// TaskAssignee 任务的一个处理人（或抄送接收人）。
type TaskAssignee struct {
	ID         string
	UserID     string
	Seq        int32
	Status     int32
	Comment    string
	CreateTime time.Time
	ActionTime time.Time
	// Origin / OwnerID 加签 / 委派关系：本席位由 OwnerID 那个席位加签 / 委派而来。
	Origin  string
	OwnerID string
	// OnBehalf 被代理人：本席位是代理人替他处理。
	OnBehalf string
}

// Task 人工任务或抄送任务。
type Task struct {
	ID         string
	TokenID    string
	NodeID     string
	NodeName   string
	Kind       string
	Mode       string
	Status     int32
	CreateTime time.Time
	EndTime    time.Time
	// DueTime 处理时限；零值 = 未配置超时。
	DueTime   time.Time
	Assignees []*TaskAssignee
}

// Job 一条异步作业。引擎只产出「要排什么作业」，领取与执行由用例层的执行器完成。
type Job struct {
	ID string
	// InstanceID 作业所属的实例；空 = 当前实例（唤醒父流程的作业挂在父实例上）。
	InstanceID string
	Kind       string
	NodeID     string
	TokenID    string
	TaskID     string
	Payload    map[string]any
	Status     int32
	// DueTime 到期时间；零值 = 立即执行（落库时取数据库的当前时间——到期判定用的是数据库
	// 时钟，应用与数据库的时钟有偏差时「立即」的作业不应被推迟）。
	DueTime   time.Time
	Attempts  int32
	LastError string
	// LockUntil 领取时拿到的租约到期时间（续租时据此确认租约仍是自己的）。
	LockUntil time.Time
}

// Spawn 引擎要求发起的一个子流程（由用例层在同一事务内发起）。
type Spawn struct {
	NodeID        string
	TokenID       string
	DefinitionKey string
}

// History 一条流转历史（只追加）。
type History struct {
	ID         string
	NodeID     string
	NodeName   string
	TaskID     string
	Action     string
	Operator   string
	Comment    string
	Detail     map[string]any
	CreateTime time.Time
}

// Notice 引擎产出的一条待发通知。
type Notice struct {
	Kind       string
	TaskID     string
	NodeName   string
	Recipients []string
}

// State 一个实例的完整执行状态：在实例行锁内整体读出、由引擎修改、再回写变化的部分。
// Histories / Notices 只含本次命令新产生的条目。
type State struct {
	Instance  Instance
	Tokens    []*Token
	Tasks     []*Task
	Histories []*History
	Notices   []*Notice
	// Jobs 写路径上是本次命令新排的作业；读路径（详情）上是实例尚未了结的作业（待执行 / 死信）。
	Jobs []*Job
	// Spawns 本次命令要发起的子流程。
	Spawns []*Spawn
	// VarsChanged 本次命令改写了流程变量（自动任务的返回值、信号携带的数据），须回写。
	VarsChanged bool

	// 命令开始时的基线（NewExecution 记录）：回写时据此只写发生变化的执行位置与任务，
	// 而不是每条命令都把整个实例重写一遍。新建的实例没有基线，全部视为变化。
	baseTokens map[string]Token
	baseTasks  map[string]string
}

// taskDigest 任务及其处理人中「会被引擎改写」的字段的摘要。
func taskDigest(t *Task) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(t.Status)))
	b.WriteString(t.EndTime.String())
	b.WriteString(t.DueTime.String())
	for _, a := range t.Assignees {
		b.WriteByte('|')
		b.WriteString(a.ID)
		b.WriteString(a.OwnerID)
		b.WriteString(strconv.Itoa(int(a.Status)))
		b.WriteString(a.Comment)
		b.WriteString(a.ActionTime.String())
	}
	return b.String()
}

// snapshot 记录基线。
func (s *State) snapshot() {
	s.baseTokens = make(map[string]Token, len(s.Tokens))
	for _, t := range s.Tokens {
		s.baseTokens[t.ID] = *t
	}
	s.baseTasks = make(map[string]string, len(s.Tasks))
	for _, t := range s.Tasks {
		s.baseTasks[t.ID] = taskDigest(t)
	}
}

// ChangedTokens 相对基线新增或变化的执行位置。
func (s *State) ChangedTokens() []*Token {
	out := make([]*Token, 0, len(s.Tokens))
	for _, t := range s.Tokens {
		if base, ok := s.baseTokens[t.ID]; !ok || base != *t {
			out = append(out, t)
		}
	}
	return out
}

// ChangedTasks 相对基线新增或变化的任务（处理人随任务整体回写）。
func (s *State) ChangedTasks() []*Task {
	out := make([]*Task, 0, len(s.Tasks))
	for _, t := range s.Tasks {
		if base, ok := s.baseTasks[t.ID]; !ok || base != taskDigest(t) {
			out = append(out, t)
		}
	}
	return out
}

// Directory 组织目录：由 system 域实现（依赖倒置），供解析处理人。
type Directory interface {
	// ActiveUserIDs 过滤出存在且为可用状态的用户，保持入参顺序并去重。
	ActiveUserIDs(ctx context.Context, ids []string) ([]string, error)
	// UserIDsByRoles 持有任一给定角色（启用）的全部可用用户。
	UserIDsByRoles(ctx context.Context, roleCodes []string) ([]string, error)
	// DeptLeaderIDs 部门负责人：从 deptID 向上第 level 级部门（1 = 自身）取可用的负责人
	// （排除 exclude）；该级为空则继续向上，直到根部门。部门不存在返回空。
	DeptLeaderIDs(ctx context.Context, deptID string, level int, exclude string) ([]string, error)
}

// Execution 对一个实例执行一条命令的上下文。非并发安全：调用方须持有实例行锁。
type Execution struct {
	g       *Graph
	s       *State
	dir     Directory
	proxies Proxies
	cal     *Calendar
	now     time.Time
	steps   int
	// trace 非空时在每到达一个节点时回调（模拟运行据此记录路径）。
	trace func(n *Node)
	// approveGuard 非空时，系统在任务超时要「自动同意」之前先问一遍业务域（与人工同意同一道
	// 校验）；返回业务错误即不自动同意，改为提醒处理人。
	approveGuard func(ctx context.Context, n *Node) error
	// memo 非空时按节点记住解析出的处理人（模拟运行用：变量与组织在一次模拟里不变，回路
	// 反复经过同一节点不必重复查目录）。
	memo map[string][]string
}

// WithApproveGuard 设置超时自动同意之前的业务校验。
func (x *Execution) WithApproveGuard(guard func(ctx context.Context, n *Node) error) *Execution {
	x.approveGuard = guard
	return x
}

// Proxies 代理设置：给定用户在 at 时刻生效的代理人（用户 → 代理人）；没有设置的不在结果中。
type Proxies interface {
	ActiveProxies(ctx context.Context, userIDs []string, at time.Time) (map[string]string, error)
}

// WithProxies 启用代理设置：新产生的待办席位落到代理人头上。
func (x *Execution) WithProxies(p Proxies) *Execution {
	x.proxies = p
	return x
}

// WithCalendar 设置工作日历：按工作时间计的时限据此推算（未设置时按自然时间）。
func (x *Execution) WithCalendar(c *Calendar) *Execution {
	x.cal = c
	return x
}

// NewExecution 构造执行上下文并记录状态基线；g 须已通过 Validate（只做撤回 / 终止时
// 可为 nil），s 须在实例行锁内读出。
func NewExecution(g *Graph, s *State, dir Directory, now time.Time) *Execution {
	s.snapshot()
	return &Execution{g: g, s: s, dir: dir, now: now.UTC()}
}

func (x *Execution) history(n *Node, taskID, action, operator, comment string, detail map[string]any) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	h := &History{ID: id, TaskID: taskID, Action: action, Operator: operator, Comment: comment, Detail: detail, CreateTime: x.now}
	if n != nil {
		h.NodeID, h.NodeName = n.ID, n.Name
	}
	x.s.Histories = append(x.s.Histories, h)
	return nil
}

func (x *Execution) notify(kind string, t *Task, recipients []string) {
	if len(recipients) == 0 {
		return
	}
	n := &Notice{Kind: kind, Recipients: recipients}
	if t != nil {
		n.TaskID, n.NodeName = t.ID, t.NodeName
	}
	x.s.Notices = append(x.s.Notices, n)
}

func (x *Execution) newToken(nodeID, scope string) (*Token, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	t := &Token{ID: id, NodeID: nodeID, Status: TokenActive, Scope: scope}
	x.s.Tokens = append(x.s.Tokens, t)
	return t, nil
}

// Start 发起流程：在开始节点放置执行位置并推进到第一个等待点。
func (x *Execution) Start(ctx context.Context) error {
	var start *Node
	for i := range x.g.Nodes {
		if x.g.Nodes[i].Type == NodeStart {
			start = &x.g.Nodes[i]
			break
		}
	}
	if err := x.history(start, "", ActionStart, x.s.Instance.Initiator, "", nil); err != nil {
		return err
	}
	token, err := x.newToken(start.ID, "")
	if err != nil {
		return err
	}
	return x.advance(ctx, token)
}

// advance 从 token 当前节点起推进，直到到达等待点（人工任务 / 汇聚未齐 / 定时、自动任务、
// 子流程节点）或流程结束。
func (x *Execution) advance(ctx context.Context, token *Token) error {
	for {
		if x.steps++; x.steps > maxAdvanceSteps {
			return ErrInstanceLimit
		}
		n := x.g.Node(token.NodeID)
		out := x.g.out[n.ID]
		if x.trace != nil {
			x.trace(n)
		}
		switch n.Type {
		case NodeStart:
			token.NodeID = out[0].Target
		case NodeUserTask:
			created, err := x.openTask(ctx, token, n)
			if err != nil {
				return err
			}
			if created {
				return nil
			}
			token.NodeID = out[0].Target // 处理人为空：跳过
		case NodeCC:
			if err := x.sendCC(ctx, token, n); err != nil {
				return err
			}
			token.NodeID = out[0].Target
		case NodeExclusive:
			e, err := x.choose(n, out)
			if err != nil {
				return err
			}
			token.NodeID = e.Target
		case NodeParallel:
			if x.g.in[n.ID] > 1 { // 汇聚：等齐全部分支
				token.Status = TokenWaiting
				var waiting []*Token
				for _, t := range x.s.Tokens {
					if t.NodeID == n.ID && t.Status == TokenWaiting {
						waiting = append(waiting, t)
					}
				}
				if len(waiting) < x.g.in[n.ID] {
					return nil
				}
				for _, t := range waiting {
					t.Status = TokenDone
				}
				merged, err := x.newToken(n.ID, token.Scope)
				if err != nil {
					return err
				}
				token = merged
			}
			if len(out) == 1 {
				token.NodeID = out[0].Target
				continue
			}
			return x.fork(ctx, token, out, token.Scope) // 分叉：每条出边一个新的执行位置
		case NodeInclusive:
			if x.g.in[n.ID] > 1 { // 汇聚：等齐这一次被激活的分支
				merged, err := x.joinInclusive(token, n)
				if err != nil || merged == nil {
					return err
				}
				token = merged
			}
			if len(out) == 1 {
				token.NodeID = out[0].Target
				continue
			}
			chosen, err := x.chooseAll(n, out)
			if err != nil {
				return err
			}
			return x.fork(ctx, token, chosen, pushScope(token.Scope, token.ID, len(chosen)))
		case NodeTimer:
			wait, err := x.openTimer(token, n)
			if err != nil {
				return err
			}
			if wait {
				return nil
			}
			token.NodeID = out[0].Target
		case NodeService:
			return x.addJob(&Job{Kind: JobService, NodeID: n.ID, TokenID: token.ID, Payload: map[string]any{"webhookId": n.Service.WebhookID}})
		case NodeSubProcess:
			x.s.Spawns = append(x.s.Spawns, &Spawn{NodeID: n.ID, TokenID: token.ID, DefinitionKey: n.SubProcess.DefinitionKey})
			return x.history(n, "", ActionSubProcess, "", "", map[string]any{"definitionKey": n.SubProcess.DefinitionKey})
		case NodeEnd:
			token.Status = TokenDone
			if x.hasLiveToken() {
				// 发布校验保证结束节点不在并行分支内，到达结束时不应还有别的执行位置；
				// 出现即说明流程图不成块结构，拒绝继续而不是留下一个半结束的实例。
				// 确定性的定义问题（重试也不会成功）：按冲突类错误返回，作业执行器据此直接转死信。
				return errors.Conflict(ErrInstanceLimit.Reason, "workflow reached its end with live branches; the definition is not block-structured")
			}
			return x.finish(InstanceCompleted, n, ActionComplete, "", "")
		}
	}
}

// autoSkip 记一次自动跳过；累计超过上限返回 ErrInstanceLimit。
func (x *Execution) autoSkip() error {
	if x.s.Instance.AutoSkips++; x.s.Instance.AutoSkips > maxAutoSkips {
		return ErrInstanceLimit
	}
	return nil
}

func (x *Execution) hasLiveToken() bool {
	return slices.ContainsFunc(x.s.Tokens, func(t *Token) bool { return t.Status != TokenDone })
}

// choose 排他网关选路：按优先级取第一条条件为真的出边，否则走默认分支。条件求值
// 出错（变量缺失、类型不符、成本超限）视为不命中——流程不会因表单数据异常而卡死，
// 发布校验保证多分支网关恒有默认分支。出错的条件记入历史（ActionConditionError），
// 让「为什么走了默认分支」有迹可查。
func (x *Execution) choose(n *Node, out []*Edge) (*Edge, error) {
	if len(out) == 1 {
		return out[0], nil
	}
	vars := x.vars()
	var (
		def    *Edge
		failed []any
	)
	for _, e := range out {
		if e.IsDefault {
			def = e
			continue
		}
		if prog := x.g.programs[e.ID]; prog != nil {
			ok, err := prog.eval(vars)
			if err == nil && ok {
				return e, x.conditionErrors(n, failed)
			}
			if err != nil {
				failed = append(failed, map[string]any{"edge": e.ID, "error": truncate(err.Error(), maxConditionError)})
			}
		}
	}
	return def, x.conditionErrors(n, failed)
}

// maxConditionError 记入历史的单条条件求值错误的字节上限。
const maxConditionError = 200

// conditionErrors 把一次网关选路里求值出错的条件记入历史；没有出错的不记。
func (x *Execution) conditionErrors(n *Node, failed []any) error {
	if len(failed) == 0 {
		return nil
	}
	return x.history(n, "", ActionConditionError, "", "", map[string]any{"edges": failed})
}

// resolve 解析节点的处理人：各规则结果取并集、过滤为可用用户；为空时按 onEmpty 兜底。
func (x *Execution) resolve(ctx context.Context, n *Node) ([]string, error) {
	if x.memo == nil {
		return x.lookup(ctx, n)
	}
	if users, ok := x.memo[n.ID]; ok {
		return slices.Clone(users), nil
	}
	users, err := x.lookup(ctx, n)
	if err == nil {
		x.memo[n.ID] = slices.Clone(users)
	}
	return users, err
}

// lookup 见 resolve（不经 memo）。
func (x *Execution) lookup(ctx context.Context, n *Node) ([]string, error) {
	in := &x.s.Instance
	var ids []string
	for _, r := range n.Assignee.Rules {
		var (
			got []string
			err error
		)
		switch r.Kind {
		case RuleUser:
			got = r.UserIDs
		case RuleRole:
			got, err = x.dir.UserIDsByRoles(ctx, r.RoleCodes)
		case RuleInitiator:
			got = []string{in.Initiator}
		case RuleDeptLeader:
			if in.InitiatorDept != "" {
				got, err = x.dir.DeptLeaderIDs(ctx, in.InitiatorDept, r.Level, in.Initiator)
			}
		case RuleDeptLeaderOf:
			got, err = x.dir.DeptLeaderIDs(ctx, r.DeptID, 1, "")
		case RuleFormUser:
			// 表单里选的人不含发起人自己：否则发起人把自己填进去就能自己审批自己。先规整为
			// 小写 UUID 再比较——目录按不区分大小写匹配用户，大写的 ID 不应绕过这道排除。
			got = slices.DeleteFunc(formIDs(in.Variables[r.Field]), func(id string) bool { return id == strings.ToLower(in.Initiator) })
		case RuleFormDeptLeader:
			for _, deptID := range formIDs(in.Variables[r.Field]) {
				leaders, lerr := x.dir.DeptLeaderIDs(ctx, deptID, 1, "")
				if lerr != nil {
					return nil, lerr
				}
				got = append(got, leaders...)
			}
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, got...)
	}
	users, err := x.dir.ActiveUserIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(users) == 0 && n.Assignee.OnEmpty == OnEmptyAdmin {
		if users, err = x.dir.ActiveUserIDs(ctx, x.g.Settings.Admins); err != nil {
			return nil, err
		}
	}
	if len(users) > maxAssigneesPerTask {
		users = users[:maxAssigneesPerTask]
	}
	return users, nil
}

// formStrings 把表单值规整为字符串列表：单个字符串或字符串数组；其余类型忽略。
func formStrings(v any) []string {
	switch val := v.(type) {
	case string:
		if val != "" {
			return []string{val}
		}
	case []any:
		out := make([]string, 0, min(len(val), maxFormTargets))
		for _, item := range val {
			if s, ok := item.(string); ok && s != "" && len(out) < maxFormTargets {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val[:min(len(val), maxFormTargets)]
	}
	return nil
}

// formIDs 把表单值规整为 ID 列表：去掉首尾空白、统一小写并去重。ID 是否真是一个存在的
// 用户 / 部门由组织目录判定（目录对不是 UUID 的值一律当作不存在）；这里统一大小写，是为了
// 让引擎自己的比较（如「排除发起人」）与目录不区分大小写的匹配口径一致。
func formIDs(v any) []string {
	raw := formStrings(v)
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if id := strings.ToLower(strings.TrimSpace(s)); id != "" && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// isUUID 报告 s 是否为带连字符的小写 UUID 文本（8-4-4-4-12）。
func isUUID(s string) bool {
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

func (x *Execution) newTask(token *Token, n *Node, kind string, status int32) (*Task, error) {
	if len(x.s.Tasks) >= maxTasksPerInstance {
		return nil, ErrInstanceLimit
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	t := &Task{ID: id, TokenID: token.ID, NodeID: n.ID, NodeName: n.Name, Kind: kind, Status: status, CreateTime: x.now}
	x.s.Tasks = append(x.s.Tasks, t)
	return t, nil
}

func (x *Execution) addAssignee(t *Task, userID string, seq, status int32) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	t.Assignees = append(t.Assignees, &TaskAssignee{ID: id, UserID: userID, Seq: seq, Status: status, CreateTime: x.now})
	return nil
}

// openTask 在人工任务节点创建任务；处理人为空返回 created=false（调用方跳过该节点）。
func (x *Execution) openTask(ctx context.Context, token *Token, n *Node) (bool, error) {
	users, err := x.resolve(ctx, n)
	if err != nil {
		return false, err
	}
	if len(users) == 0 {
		if err := x.autoSkip(); err != nil {
			return false, err
		}
		return false, x.history(n, "", ActionSkip, "", "", map[string]any{"reason": "no assignee"})
	}
	t, err := x.newTask(token, n, TaskKindApprove, TaskPending)
	if err != nil {
		return false, err
	}
	t.Mode = cmpOr(n.Assignee.Mode, ModeAny)
	if err := x.assign(ctx, t, users); err != nil {
		return false, err
	}
	return true, x.scheduleTimeout(token, n, t)
}

// assign 按任务的多人模式写入处理人（设了代理的换成代理人）并通知当前可处理的人。
func (x *Execution) assign(ctx context.Context, t *Task, users []string) error {
	seats, err := x.proxied(ctx, t, users)
	if err != nil {
		return err
	}
	var seq int32
	for _, a := range t.Assignees {
		seq = max(seq, a.Seq)
	}
	notify := make([]string, 0, len(seats))
	for i, st := range seats {
		status := AssigneePending
		if t.Mode == ModeSequential && i > 0 {
			status = AssigneeQueued
		}
		seq++
		if err := x.addAssignee(t, st.user, seq, status); err != nil {
			return err
		}
		t.Assignees[len(t.Assignees)-1].OnBehalf = st.onBehalf
		if status == AssigneePending {
			notify = append(notify, st.user)
		}
	}
	x.notify(NoticeTaskAssigned, t, notify)
	return nil
}

func cmpOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// sendCC 抄送节点：创建终态的抄送任务并通知，不阻塞流程。
func (x *Execution) sendCC(ctx context.Context, token *Token, n *Node) error {
	users, err := x.resolve(ctx, n)
	if err != nil || len(users) == 0 {
		return err
	}
	t, err := x.newTask(token, n, TaskKindCC, TaskNotified)
	if err != nil {
		return err
	}
	t.EndTime = x.now
	for i, userID := range users {
		if err := x.addAssignee(t, userID, int32(i+1), AssigneeNotified); err != nil {
			return err
		}
	}
	x.notify(NoticeTaskCC, t, users)
	return x.history(n, t.ID, ActionCC, "", "", map[string]any{"users": users})
}

// finish 结束实例：消耗全部执行位置、取消未完成的任务、记历史并通知发起人。
func (x *Execution) finish(status int32, n *Node, action, operator, comment string) error {
	for _, t := range x.s.Tokens {
		t.Status = TokenDone
	}
	for _, t := range x.s.Tasks {
		if t.Status == TaskPending {
			x.closeTask(t, TaskCancelled)
		}
	}
	in := &x.s.Instance
	in.Status, in.EndTime = status, x.now
	x.s.Spawns = nil // 同一条命令里先到达子流程节点、随后流程即结束：不再发起
	if in.ParentInstanceID != "" {
		// 子流程结束：排一条唤醒父流程的作业（挂在父实例上，由执行器只锁父实例去处理，
		// 父子两个实例从不在同一事务里同时加锁）。
		if err := x.addJob(&Job{
			InstanceID: in.ParentInstanceID, Kind: JobResume, NodeID: in.ParentNodeID, TokenID: in.ParentTokenID,
			Payload: map[string]any{"child": in.ID, "status": status},
		}); err != nil {
			return err
		}
	} else {
		x.notify(NoticeInstanceFinished, nil, []string{in.Initiator})
	}
	return x.history(n, "", action, operator, comment, nil)
}

// live 报告席位是否仍未了结（待处理 / 排队 / 等待加签或委派的人）。
func live(a *TaskAssignee) bool {
	return a.Status == AssigneePending || a.Status == AssigneeQueued || a.Status == AssigneeSuspended
}

// closeTask 把任务置为终态，并取消仍未了结的处理人。
func (x *Execution) closeTask(t *Task, status int32) {
	t.Status, t.EndTime = status, x.now
	x.cancelSeats(t)
}

// cancelSeats 取消任务上全部未了结的席位。
func (x *Execution) cancelSeats(t *Task) {
	for _, a := range t.Assignees {
		if live(a) {
			a.Status, a.ActionTime = AssigneeCancelled, x.now
		}
	}
}

// pendingTask 定位待处理任务及 userID 在其上的待处理席位，并做全部前置校验。
func (x *Execution) pendingTask(taskID, userID string) (*Task, *TaskAssignee, *Token, error) {
	if x.s.Instance.Status != InstanceRunning {
		return nil, nil, nil, ErrInstanceNotRunning
	}
	i := slices.IndexFunc(x.s.Tasks, func(t *Task) bool { return t.ID == taskID })
	if i < 0 {
		return nil, nil, nil, ErrTaskNotFound
	}
	t := x.s.Tasks[i]
	if t.Kind != TaskKindApprove || t.Status != TaskPending {
		return nil, nil, nil, ErrTaskNotPending
	}
	var seat *TaskAssignee
	involved := false
	for _, a := range t.Assignees {
		if a.UserID != userID {
			continue
		}
		involved = true
		if a.Status == AssigneePending {
			seat = a
		}
	}
	if !involved {
		return nil, nil, nil, ErrTaskNotAssignee
	}
	if seat == nil {
		return nil, nil, nil, ErrTaskNotPending
	}
	j := slices.IndexFunc(x.s.Tokens, func(tk *Token) bool { return tk.ID == t.TokenID })
	if j < 0 || x.s.Tokens[j].Status != TokenActive {
		return nil, nil, nil, ErrTaskNotPending
	}
	return t, seat, x.s.Tokens[j], nil
}

// Approve 同意：按多人模式判定任务是否通过，通过后继续推进。
func (x *Execution) Approve(ctx context.Context, taskID, userID, comment string) error {
	t, seat, token, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	n := x.g.Node(t.NodeID)
	seat.Status, seat.Comment, seat.ActionTime = AssigneeApproved, comment, x.now
	action, detail := ActionApprove, map[string]any(nil)
	if seat.Origin == OriginDelegate {
		action, detail = ActionReply, map[string]any{"agree": true}
	}
	if err := x.history(n, t.ID, action, userID, comment, detail); err != nil {
		return err
	}
	return x.seatDone(ctx, t, seat, token)
}

// unresolved 报告席位是否尚未了结：自身未了结，或由它加签 / 委派出去的席位（递归）里还有
// 未了结的。后加签的人还没处理完时，加签人虽已「同意」，这份同意尚未生效——仍算未了结。
func unresolved(t *Task, seat *TaskAssignee) bool {
	if live(seat) {
		return true
	}
	return slices.ContainsFunc(t.Assignees, func(a *TaskAssignee) bool { return a.OwnerID == seat.ID && unresolved(t, a) })
}

// seatDone 一个席位了结（同意）之后的接续，沿加签 / 委派关系自下而上：
//  1. 它自己加签出去的人还没处理完 → 等；
//  2. 它是加签 / 委派出来的席位 → 同一批（同一个加签人名下的全部席位，含各自的下级）都
//     了结后交还给加签人：加签人在等（前加签 / 委派）则恢复待处理；加签人已同意
//     （后加签）则这份同意此刻生效，继续向上接续；
//  3. 普通席位 → 按任务的多人模式判定任务是否通过。
func (x *Execution) seatDone(ctx context.Context, t *Task, seat *TaskAssignee, token *Token) error {
	if unresolved(t, seat) {
		return nil
	}
	if seat.OwnerID != "" {
		if i := slices.IndexFunc(t.Assignees, func(a *TaskAssignee) bool { return a.ID == seat.OwnerID }); i >= 0 {
			owner := t.Assignees[i]
			if slices.ContainsFunc(t.Assignees, func(a *TaskAssignee) bool { return a.OwnerID == owner.ID && unresolved(t, a) }) {
				return nil // 同一批加签的人还没处理完
			}
			if owner.Status == AssigneeSuspended {
				owner.Status = AssigneePending
				x.notify(NoticeTaskAssigned, t, []string{owner.UserID})
				return nil
			}
			return x.seatDone(ctx, t, owner, token)
		}
	}
	if seat.Status != AssigneeApproved {
		return nil // 只有「同意」才推动任务通过；被取消等其它了结方式不算
	}
	n := x.g.Node(t.NodeID)
	switch t.Mode {
	case ModeAll:
		if slices.ContainsFunc(t.Assignees, func(a *TaskAssignee) bool { return unresolved(t, a) }) {
			return nil
		}
	case ModeSequential:
		if next := nextQueued(t); next != nil {
			next.Status = AssigneePending
			x.notify(NoticeTaskAssigned, t, []string{next.UserID})
			return nil
		}
	}
	x.closeTask(t, TaskApproved)
	token.NodeID = x.g.out[n.ID][0].Target
	return x.advance(ctx, token)
}

// nextQueued 依次审批中下一个排队的处理人（按 seq）。
func nextQueued(t *Task) *TaskAssignee {
	var next *TaskAssignee
	for _, a := range t.Assignees {
		if a.Status == AssigneeQueued && (next == nil || a.Seq < next.Seq) {
			next = a
		}
	}
	return next
}

// Reject 拒绝：任务即刻以拒绝结束；节点配置了拒绝去向则退回该节点，否则流程以「已拒绝」结束。
func (x *Execution) Reject(ctx context.Context, taskID, userID, comment string) error {
	t, seat, token, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	n := x.g.Node(t.NodeID)
	seat.Status, seat.Comment, seat.ActionTime = AssigneeRejected, comment, x.now
	if seat.Origin == OriginDelegate {
		// 被委派人的「拒绝」只是给委派人的意见，不直接决定任务。
		if err := x.history(n, t.ID, ActionReply, userID, comment, map[string]any{"agree": false}); err != nil {
			return err
		}
		return x.seatDone(ctx, t, seat, token)
	}
	return x.rejectTask(ctx, t, token, ActionReject, userID, comment, nil)
}

// rejectTask 任务以拒绝结束：节点配置了拒绝去向则退回该节点，否则流程以「已拒绝」结束。
func (x *Execution) rejectTask(ctx context.Context, t *Task, token *Token, action, operator, comment string, detail map[string]any) error {
	n := x.g.Node(t.NodeID)
	x.closeTask(t, TaskRejected)
	if n.OnReject == nil || n.OnReject.To == "" {
		if err := x.history(n, t.ID, action, operator, comment, detail); err != nil {
			return err
		}
		return x.finish(InstanceRejected, n, ActionComplete, "", "")
	}
	if detail == nil {
		detail = map[string]any{}
	}
	detail["to"] = n.OnReject.To
	if err := x.history(n, t.ID, action, operator, comment, detail); err != nil {
		return err
	}
	token.NodeID = n.OnReject.To
	return x.advance(ctx, token)
}

// Return 退回到此前走过的某个人工任务节点（须在同一并行区域内）。
func (x *Execution) Return(ctx context.Context, taskID, userID, toNodeID, comment string) error {
	t, seat, token, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	// 加签 / 委派来的人只对自己那一份意见负责，不能把整个任务退回。
	if seat.OwnerID != "" || !slices.ContainsFunc(ReturnTargets(x.g, x.s, t), func(n *Node) bool { return n.ID == toNodeID }) {
		return ErrTaskTargetInvalid
	}
	seat.Status, seat.Comment, seat.ActionTime = AssigneeReturned, comment, x.now
	x.closeTask(t, TaskReturned)
	if err := x.history(x.g.Node(t.NodeID), t.ID, ActionReturn, userID, comment, map[string]any{"to": toNodeID}); err != nil {
		return err
	}
	token.NodeID = toNodeID
	return x.advance(ctx, token)
}

// Transfer 转办：把自己的处理席位交给另一名可用用户（对方接替原顺序）。
func (x *Execution) Transfer(ctx context.Context, taskID, userID, toUserID, comment string) error {
	t, seat, _, err := x.pendingTask(taskID, userID)
	if err != nil {
		return err
	}
	targets, err := x.targets(ctx, t, userID, []string{toUserID})
	if err != nil {
		return err
	}
	seat.Comment = comment
	if err := x.replaceSeat(t, seat, targets[0]); err != nil {
		return err
	}
	x.notify(NoticeTaskAssigned, t, []string{targets[0].user})
	return x.history(x.g.Node(t.NodeID), t.ID, ActionTransfer, userID, comment, map[string]any{"to": targets[0].user})
}

// Reassign 管理员改派：取消任务当前全部待处理 / 排队的处理人，换成给定的可用用户。
func (x *Execution) Reassign(ctx context.Context, taskID, operator string, userIDs []string, reason string) error {
	if x.s.Instance.Status != InstanceRunning {
		return ErrInstanceNotRunning
	}
	i := slices.IndexFunc(x.s.Tasks, func(t *Task) bool { return t.ID == taskID })
	if i < 0 {
		return ErrTaskNotFound
	}
	t := x.s.Tasks[i]
	if t.Kind != TaskKindApprove || t.Status != TaskPending {
		return ErrTaskNotPending
	}
	users, err := x.dir.ActiveUserIDs(ctx, userIDs)
	if err != nil {
		return err
	}
	if len(users) == 0 || len(users) > maxAssigneesPerTask {
		return ErrTaskTargetInvalid
	}
	x.cancelSeats(t)
	if err := x.assign(ctx, t, users); err != nil {
		return err
	}
	// 改派后时限重新起算：否则临近超时才改派，新的处理人还没来得及看就被自动处理了。
	n := x.g.Node(t.NodeID)
	if j := slices.IndexFunc(x.s.Tokens, func(tk *Token) bool { return tk.ID == t.TokenID }); j >= 0 {
		if err := x.scheduleTimeout(x.s.Tokens[j], n, t); err != nil {
			return err
		}
	}
	return x.history(n, t.ID, ActionReassign, operator, reason, map[string]any{"users": users})
}

// Cancel 发起人撤回：流程以「已取消」结束。
func (x *Execution) Cancel(userID, comment string) error {
	if x.s.Instance.Status != InstanceRunning {
		return ErrInstanceNotRunning
	}
	// 子流程不能单独撤回：它是父流程的一个环节，要撤回就撤回父流程。
	if x.s.Instance.Initiator != userID || x.s.Instance.ParentInstanceID != "" {
		return ErrInstanceForbidden
	}
	return x.finish(InstanceCancelled, nil, ActionCancel, userID, comment)
}

// Terminate 管理员终止：流程以「已终止」结束（原因必填，由用例层校验）。
func (x *Execution) Terminate(operator, reason string) error {
	if x.s.Instance.Status != InstanceRunning {
		return ErrInstanceNotRunning
	}
	return x.finish(InstanceTerminated, nil, ActionTerminate, operator, reason)
}

// ReturnTargets 任务 cur 可退回到的节点：实例「当前路径」上位于它之前、且与它处于同一
// 并行区域的人工任务，最近到达的在前。
//
// 当前路径按任务到达顺序推演：再次到达某节点（退回 / 拒绝去向造成）时，路径截断回该
// 节点——之后走过的节点不再算作「之前」。因此退回只能往回走，不能借「曾经到过」
// 跳到后面的节点而绕过中间环节。
func ReturnTargets(g *Graph, s *State, cur *Task) []*Node {
	var path []string
	for _, t := range s.Tasks {
		if t.Kind != TaskKindApprove || !g.SameRegion(cur.NodeID, t.NodeID) {
			continue
		}
		if i := slices.Index(path, t.NodeID); i >= 0 {
			path = path[:i]
		}
		path = append(path, t.NodeID)
		if t.ID == cur.ID {
			break
		}
	}
	out := make([]*Node, 0, len(path))
	for i := len(path) - 2; i >= 0; i-- { // 末项是 cur 自身
		if n := g.Node(path[i]); n != nil && n.Type == NodeUserTask {
			out = append(out, n)
		}
	}
	return out
}
