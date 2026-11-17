// Package biz 流程引擎业务核心：流程定义（有向图 DSL）与校验、条件表达式、执行状态机。
// 引擎与业务无关：只认 business_type + business_id + 变量，不认识工单等上层域。
package biz

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// 节点类型。
const (
	NodeStart     = "start"
	NodeEnd       = "end"
	NodeUserTask  = "userTask"
	NodeCC        = "cc"
	NodeExclusive = "exclusiveGateway"
	NodeParallel  = "parallelGateway"
	// NodeInclusive 包容网关：分叉时走全部条件成立的分支（都不成立走默认分支），汇聚时等齐
	// 这一次被激活的那些分支。与并行网关一样须成对出现（join）。
	NodeInclusive = "inclusiveGateway"
	// NodeTimer 定时 / 等待：延时一段时间、等到表单里的某个时刻，或等待外部信号。
	NodeTimer = "timer"
	// NodeService 自动任务：异步调用一个已登记的 Webhook 端点，成功后继续（失败重试，耗尽进死信）。
	NodeService = "serviceTask"
	// NodeSubProcess 子流程：发起另一个流程并等它结束。
	NodeSubProcess = "subProcess"
)

// 定时节点的等待方式（timer.mode）。
const (
	// TimerDelay 延时 duration。
	TimerDelay = "delay"
	// TimerUntil 等到表单字段 field 所示的时刻（RFC3339）；已过或取不到则立即继续。
	TimerUntil = "until"
	// TimerSignal 等待名为 signal 的外部信号。
	TimerSignal = "signal"
)

// 时长单位。
const (
	UnitMinute = "minute"
	UnitHour   = "hour"
	UnitDay    = "day"
)

// 任务超时后的动作（userTask.timeout.action）。
const (
	// TimeoutRemind 只提醒当前处理人。
	TimeoutRemind = "remind"
	// TimeoutApprove 自动同意。
	TimeoutApprove = "approve"
	// TimeoutReject 自动拒绝（按节点的拒绝去向处理）。
	TimeoutReject = "reject"
	// TimeoutAdmin 转交流程管理员（没有可用的管理员时退化为提醒）。
	TimeoutAdmin = "admin"
)

// 子流程未正常通过时父流程的处理（由子流程的结束状态决定，不可配置）：
// 子流程被拒绝 → 父流程以「已拒绝」结束；被撤回 / 终止 → 父流程以「已终止」结束。

// 多人处理模式（userTask.assignee.mode）。
const (
	// ModeAny 或签：任一人同意即通过。
	ModeAny = "any"
	// ModeAll 会签：全部同意才通过，任一人拒绝即拒绝。
	ModeAll = "all"
	// ModeSequential 依次审批：按顺序逐个处理。
	ModeSequential = "sequential"
)

// 处理人为空时的策略（userTask.assignee.onEmpty）。
const (
	// OnEmptySkip 自动跳过该节点。
	OnEmptySkip = "skip"
	// OnEmptyAdmin 转交流程管理员（settings.admins）；管理员也为空时跳过。
	OnEmptyAdmin = "admin"
)

// 处理人规则类型（assignee.rules[].kind）。
const (
	// RuleUser 指定用户（userIds）。
	RuleUser = "user"
	// RuleRole 指定角色（roleCodes）的全部可用用户。
	RuleRole = "role"
	// RuleInitiator 发起人本人。
	RuleInitiator = "initiator"
	// RuleDeptLeader 发起部门向上第 level 级部门的负责人（1 = 发起部门自身）。
	RuleDeptLeader = "deptLeader"
	// RuleDeptLeaderOf 指定部门（deptId）的负责人。
	RuleDeptLeaderOf = "deptLeaderOf"
	// RuleFormUser 表单中人员字段（field）的值；其中的发起人本人会被排除（不能自己审批自己）。
	RuleFormUser = "formUser"
	// RuleFormDeptLeader 表单中部门字段（field）所指部门的负责人。
	RuleFormDeptLeader = "formDeptLeader"
	// RuleOncall 值班表（scheduleId）在流程到达该节点那一刻的主、备值班人；其中的发起人本人
	// 会被排除（不能自己审批自己）。groupId 是值班表所属的值班组，仅供设计器回显。
	RuleOncall = "oncall"
)

// 定义规模上限：发布校验与执行成本都必须有界。
const (
	maxNodes         = 200
	maxEdges         = 400
	maxRulesPerNode  = 10
	maxRuleTargets   = 50
	maxDeptLevel     = 10
	maxConditionSize = 1024
	maxNodeName      = 128
	maxAdmins        = 20
	// maxFormTargets 表单字段型规则（formUser / formDeptLeader）单次解析采纳的取值上限。
	maxFormTargets = 20
	// maxDurationMinutes 时长配置的上限（折合分钟，一年）。
	maxDurationMinutes = 366 * 24 * 60
	// maxFormPermissions 单个节点的字段权限条数上限（通用的与各表单专属的合计）。
	maxFormPermissions = 300
	// maxSubProcessDepth 子流程嵌套深度上限（防止流程互相调用无限递归）。
	maxSubProcessDepth = 5
)

// uuidPattern 小写 UUID（Webhook 端点 ID）。
var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// definitionKeyPattern 流程标识格式（与 proto 校验一致）。
var definitionKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,63}$`)

// signalPattern 信号名格式。
var signalPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,63}$`)

// idPattern 节点 / 连线 ID 的格式：落库列为 varchar(64)，且并行区域签名以 ',' '#' 分隔，
// ID 不得含这些字符。
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// AssigneeRule 一条处理人规则；按 Kind 取用对应字段。
type AssigneeRule struct {
	Kind      string   `json:"kind"`
	UserIDs   []string `json:"userIds,omitempty"`
	RoleCodes []string `json:"roleCodes,omitempty"`
	DeptID    string   `json:"deptId,omitempty"`
	Level     int      `json:"level,omitempty"`
	Field     string   `json:"field,omitempty"`
	// GroupID / ScheduleID 值班人规则：值班组与其下的值班表。
	GroupID    string `json:"groupId,omitempty"`
	ScheduleID string `json:"scheduleId,omitempty"`
}

// Assignee 节点的处理人配置：多条规则的结果取并集（去重）。
type Assignee struct {
	Rules   []AssigneeRule `json:"rules"`
	Mode    string         `json:"mode,omitempty"`
	OnEmpty string         `json:"onEmpty,omitempty"`
}

// OnReject 人工任务被拒绝后的去向：To 为空 = 流程以「已拒绝」结束；否则退回到 To 节点。
type OnReject struct {
	To string `json:"to,omitempty"`
}

// Duration 一段时长。WorkTime 为 true 时按工作日历计：分钟 / 小时只消耗工作时段，
// 天 = 工作日。
type Duration struct {
	Value    int    `json:"value"`
	Unit     string `json:"unit"`
	WorkTime bool   `json:"workTime,omitempty"`
}

// minutes 折合成分钟（天按 24 小时，仅用于上限校验）。
func (d Duration) minutes() int {
	switch d.Unit {
	case UnitHour:
		return d.Value * 60
	case UnitDay:
		return d.Value * 24 * 60
	}
	return d.Value
}

func (d Duration) valid() bool {
	switch d.Unit {
	case UnitMinute, UnitHour, UnitDay:
	default:
		return false
	}
	// 先限数值再折算：避免极大的数值在折算时溢出后反而通过校验。
	return d.Value >= 1 && d.Value <= maxDurationMinutes && d.minutes() <= maxDurationMinutes
}

// Timeout 人工任务的处理时限：到期执行 Action；Remind 非空时在到期前这么久先提醒一次。
type Timeout struct {
	Duration
	Action string    `json:"action"`
	Remind *Duration `json:"remind,omitempty"`
}

// Timer 定时节点配置；按 Mode 取用对应字段。
type Timer struct {
	Mode     string    `json:"mode"`
	Duration *Duration `json:"duration,omitempty"`
	Field    string    `json:"field,omitempty"`
	Signal   string    `json:"signal,omitempty"`
}

// Service 自动任务配置：调用哪个 Webhook 端点（workflow_webhook.id）。地址与密钥不进流程图。
type Service struct {
	WebhookID string `json:"webhookId"`
}

// SubProcess 子流程配置：发起哪个流程（按标识取其当前发布版本）。子流程继承父流程的
// 发起人、发起部门与流程变量。
type SubProcess struct {
	DefinitionKey string `json:"definitionKey"`
}

// Node 流程节点。
type Node struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	// Assignee 处理人（userTask）或抄送人（cc）。
	Assignee *Assignee `json:"assignee,omitempty"`
	// OnReject 拒绝后的去向（userTask）。
	OnReject *OnReject `json:"onReject,omitempty"`
	// Join 并行 / 包容分叉网关配对的汇聚网关 ID（出边 > 1 时必填）。
	Join string `json:"join,omitempty"`
	// Timeout 处理时限（userTask，可选）。
	Timeout *Timeout `json:"timeout,omitempty"`
	// Timer 定时配置（timer）。
	Timer *Timer `json:"timer,omitempty"`
	// Service 自动任务配置（serviceTask）。
	Service *Service `json:"service,omitempty"`
	// SubProcess 子流程配置（subProcess）。
	SubProcess *SubProcess `json:"subProcess,omitempty"`
	// FormPermissions 节点级字段权限（字段码 → hidden / readonly / editable / required），
	// 对所有使用该流程的表单生效。
	FormPermissions map[string]string `json:"formPermissions,omitempty"`
	// FormPermissionsByForm 某张表单专属的字段权限：表单标识（发起流程时由业务域给出，见
	// Instance.FormKey；工单为模板 ID）→ 字段码 → 权限。同一字段在专属里配置了就以专属为准，
	// 否则回落到 FormPermissions。
	FormPermissionsByForm map[string]map[string]string `json:"formPermissionsByForm,omitempty"`
}

// PermissionsFor 节点对表单 form 生效的字段权限：通用配置叠加该表单的专属配置（专属优先）。
// form 为空或没有专属配置时就是通用配置本身；返回值只读。
func (n *Node) PermissionsFor(form string) map[string]string {
	own := n.FormPermissionsByForm[form]
	if form == "" || len(own) == 0 {
		return n.FormPermissions
	}
	out := make(map[string]string, len(n.FormPermissions)+len(own))
	maps.Copy(out, n.FormPermissions)
	maps.Copy(out, own)
	return out
}

// Edge 连线。Condition 为 CEL 表达式（仅排他 / 包容网关的出边可带）；Priority 小者先判定；
// IsDefault 为排他网关的默认分支；Jump 只供设计器还原结构，引擎不读。
type Edge struct {
	ID        string `json:"id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Condition string `json:"condition,omitempty"`
	Priority  int    `json:"priority,omitempty"`
	IsDefault bool   `json:"isDefault,omitempty"`
	Jump      bool   `json:"jump,omitempty"`
}

// Settings 流程级设置。
type Settings struct {
	// Admins 流程管理员用户 ID：处理人为空时的兜底处理人（onEmpty = admin），也是任务超时
	// 转交、自动任务进死信时的通知对象。
	Admins []string `json:"admins,omitempty"`
}

// Graph 流程定义（有向图）。零值不可用，经 ParseGraph 构造。
type Graph struct {
	Nodes    []Node   `json:"nodes"`
	Edges    []Edge   `json:"edges"`
	Settings Settings `json:"settings"`

	nodes    map[string]*Node
	out      map[string][]*Edge // 出边，按 (priority, id) 排序
	in       map[string]int     // 入度
	regions  map[string]string  // 节点 → 所在并行区域签名（包围它的「分叉网关#分支序号」集合）
	programs map[string]*condition
}

// Issue 一条校验问题：NodeID / EdgeID 指向出问题的元素（可为空），Code 为稳定的
// 机器可读码（前端据此本地化），Message 为英文说明。
type Issue struct {
	NodeID  string
	EdgeID  string
	Code    string
	Message string
}

// ParseGraph 解析流程定义 JSON 并建立索引（不做语义校验，见 Validate）。
func ParseGraph(raw []byte) (*Graph, error) {
	var g Graph
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, fmt.Errorf("parse workflow graph: %w", err)
	}
	g.index()
	return &g, nil
}

// index 建立节点 / 出边 / 入度索引。重复 ID 时后者不覆盖前者（由 Validate 报告）。
func (g *Graph) index() {
	g.nodes = make(map[string]*Node, len(g.Nodes))
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if _, dup := g.nodes[n.ID]; !dup {
			g.nodes[n.ID] = n
		}
	}
	g.out = make(map[string][]*Edge, len(g.Nodes))
	g.in = make(map[string]int, len(g.Nodes))
	for i := range g.Edges {
		e := &g.Edges[i]
		g.out[e.Source] = append(g.out[e.Source], e)
		g.in[e.Target]++
	}
	for _, edges := range g.out {
		slices.SortStableFunc(edges, func(a, b *Edge) int {
			return cmp.Or(cmp.Compare(a.Priority, b.Priority), strings.Compare(a.ID, b.ID))
		})
	}
}

// Node 按 ID 取节点；不存在返回 nil。返回值只读。
func (g *Graph) Node(id string) *Node { return g.nodes[id] }

// paired 报告节点类型是否为须成对出现的网关（并行 / 包容）。
func paired(nodeType string) bool {
	return nodeType == NodeParallel || nodeType == NodeInclusive
}

// isFork 报告节点是否为分叉（并行 / 包容网关且出边 > 1）。
func (g *Graph) isFork(n *Node) bool {
	return paired(n.Type) && len(g.out[n.ID]) > 1
}

// isWait 报告节点是否为「到达后停下、由作业或信号唤醒」的等待点。
func isWait(nodeType string) bool {
	return nodeType == NodeTimer || nodeType == NodeService || nodeType == NodeSubProcess
}

// SameRegion 报告两个节点是否处于同一并行区域（位于完全相同的一组并行分支之内；
// 同一分叉下的不同分支属于不同区域）。
// 只有同一区域内的节点之间才允许跳转 / 退回：跨区域移动执行位置会破坏并行配对。
func (g *Graph) SameRegion(a, b string) bool {
	return g.regions[a] == g.regions[b]
}

// Validate 校验流程定义；返回全部问题（空 = 合法）。校验通过后图可安全执行：
// 表达式已编译、并行区域已计算。
func (g *Graph) Validate() []Issue {
	v := &validator{g: g}
	v.structure()
	if len(v.issues) > 0 {
		return v.issues // 结构不成立时后续分析没有意义
	}
	v.nodeRules()
	v.reachability()
	v.parallelRegions()
	v.jumps()
	v.autoLoops()
	return v.issues
}

type validator struct {
	g      *Graph
	issues []Issue
}

func (v *validator) node(id, code, format string, args ...any) {
	v.issues = append(v.issues, Issue{NodeID: id, Code: code, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) edge(id, code, format string, args ...any) {
	v.issues = append(v.issues, Issue{EdgeID: id, Code: code, Message: fmt.Sprintf(format, args...)})
}

// structure 基础结构：规模、ID 唯一、连线端点存在、开始 / 结束节点数量。
func (v *validator) structure() {
	g := v.g
	if len(g.Nodes) == 0 || len(g.Nodes) > maxNodes || len(g.Edges) > maxEdges {
		v.node("", "GRAPH_SIZE", "graph must have 1-%d nodes and at most %d edges", maxNodes, maxEdges)
		return
	}
	seen := make(map[string]struct{}, len(g.Nodes))
	starts, ends := 0, 0
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if !idPattern.MatchString(n.ID) {
			v.node(n.ID, "NODE_ID_INVALID", "node id must be 1-64 letters, digits, underscores or hyphens")
			continue
		}
		if len(n.Name) > maxNodeName {
			v.node(n.ID, "NODE_NAME_TOO_LONG", "node name exceeds %d bytes", maxNodeName)
		}
		if _, dup := seen[n.ID]; dup {
			v.node(n.ID, "NODE_ID_DUPLICATE", "duplicate node id %q", n.ID)
		}
		seen[n.ID] = struct{}{}
		switch n.Type {
		case NodeStart:
			starts++
		case NodeEnd:
			ends++
		case NodeUserTask, NodeCC, NodeExclusive, NodeParallel, NodeInclusive, NodeTimer, NodeService, NodeSubProcess:
		default:
			v.node(n.ID, "NODE_TYPE_UNKNOWN", "unknown node type %q", n.Type)
		}
	}
	if starts != 1 {
		v.node("", "START_COUNT", "graph must have exactly one start node, got %d", starts)
	}
	if ends == 0 {
		v.node("", "END_MISSING", "graph must have at least one end node")
	}
	if len(g.Settings.Admins) > maxAdmins {
		v.node("", "ADMINS_INVALID", "workflow admins must be at most %d user ids", maxAdmins)
	}
	edgeIDs := make(map[string]struct{}, len(g.Edges))
	pairs := make(map[[2]string]struct{}, len(g.Edges))
	for i := range g.Edges {
		e := &g.Edges[i]
		if !idPattern.MatchString(e.ID) {
			v.edge(e.ID, "EDGE_ID_INVALID", "edge id must be 1-64 letters, digits, underscores or hyphens")
			continue
		}
		if _, dup := edgeIDs[e.ID]; dup {
			v.edge(e.ID, "EDGE_ID_DUPLICATE", "duplicate edge id %q", e.ID)
		}
		edgeIDs[e.ID] = struct{}{}
		if g.nodes[e.Source] == nil || g.nodes[e.Target] == nil {
			v.edge(e.ID, "EDGE_ENDPOINT_MISSING", "edge %q references a missing node", e.ID)
			continue
		}
		if e.Source == e.Target {
			v.edge(e.ID, "EDGE_SELF_LOOP", "edge %q connects a node to itself", e.ID)
		}
		pair := [2]string{e.Source, e.Target}
		if _, dup := pairs[pair]; dup {
			v.edge(e.ID, "EDGE_DUPLICATE", "duplicate edge from %q to %q", e.Source, e.Target)
		}
		pairs[pair] = struct{}{}
	}
}

// nodeRules 各类型节点的度数、处理人、条件规则；并编译条件表达式。
func (v *validator) nodeRules() {
	g := v.g
	g.programs = make(map[string]*condition, len(g.Edges))
	for i := range g.Nodes {
		n := &g.Nodes[i]
		out, in := g.out[n.ID], g.in[n.ID]
		if n.Type != NodeExclusive && n.Type != NodeInclusive {
			for _, e := range out {
				if e.Condition != "" || e.IsDefault {
					v.edge(e.ID, "CONDITION_NOT_ALLOWED", "only edges leaving an exclusive or inclusive gateway may carry a condition")
				}
			}
		}
		if !validFormPermissions(n) {
			v.node(n.ID, "FORM_PERMISSION_INVALID", "form permissions must map field codes to hidden, readonly, editable or required")
		}
		if n.Timeout != nil && n.Type != NodeUserTask {
			v.node(n.ID, "TIMEOUT_NOT_ALLOWED", "only user tasks may configure a timeout")
		}
		switch n.Type {
		case NodeStart:
			if in != 0 || len(out) != 1 {
				v.node(n.ID, "START_DEGREE", "start node must have no incoming and exactly one outgoing edge")
			}
		case NodeEnd:
			if len(out) != 0 || in == 0 {
				v.node(n.ID, "END_DEGREE", "end node must have incoming edges and no outgoing edge")
			}
		case NodeUserTask, NodeCC:
			if in == 0 || len(out) != 1 {
				v.node(n.ID, "TASK_DEGREE", "task node must have incoming edges and exactly one outgoing edge")
			}
			v.assignee(n)
			v.timeout(n)
			if n.OnReject != nil && n.OnReject.To != "" {
				if n.Type != NodeUserTask {
					v.node(n.ID, "ON_REJECT_NOT_ALLOWED", "only user tasks may configure a reject target")
				} else if t := g.nodes[n.OnReject.To]; t == nil || t.Type != NodeUserTask || t.ID == n.ID {
					v.node(n.ID, "ON_REJECT_TARGET", "reject target must be another user task")
				}
			}
		case NodeExclusive, NodeInclusive:
			v.exclusive(n, out, in)
		case NodeTimer, NodeService, NodeSubProcess:
			if in == 0 || len(out) != 1 {
				v.node(n.ID, "TASK_DEGREE", "task node must have incoming edges and exactly one outgoing edge")
			}
			v.waitNode(n)
		case NodeParallel:
			if in == 0 || len(out) == 0 {
				v.node(n.ID, "GATEWAY_DEGREE", "gateway must have incoming and outgoing edges")
			}
		}
	}
}

func (v *validator) exclusive(n *Node, out []*Edge, in int) {
	if in == 0 || len(out) == 0 {
		v.node(n.ID, "GATEWAY_DEGREE", "gateway must have incoming and outgoing edges")
		return
	}
	if len(out) == 1 { // 纯汇合：唯一出边无条件
		if out[0].Condition != "" {
			v.edge(out[0].ID, "CONDITION_NOT_ALLOWED", "the single outgoing edge of a merging gateway must not carry a condition")
		}
		return
	}
	defaults := 0
	for _, e := range out {
		if e.IsDefault {
			defaults++
			if e.Condition != "" {
				v.edge(e.ID, "DEFAULT_WITH_CONDITION", "the default branch must not carry a condition")
			}
			continue
		}
		switch {
		case strings.TrimSpace(e.Condition) == "":
			v.edge(e.ID, "CONDITION_MISSING", "branch condition is required")
		case len(e.Condition) > maxConditionSize:
			v.edge(e.ID, "CONDITION_TOO_LONG", "branch condition exceeds %d characters", maxConditionSize)
		default:
			prog, err := compileCondition(e.Condition)
			if err != nil {
				v.edge(e.ID, "CONDITION_INVALID", "invalid branch condition: %v", err)
				continue
			}
			v.g.programs[e.ID] = prog
		}
	}
	if defaults != 1 {
		v.node(n.ID, "DEFAULT_BRANCH", "an exclusive gateway with several branches must have exactly one default branch, got %d", defaults)
	}
}

func (v *validator) assignee(n *Node) {
	a := n.Assignee
	if a == nil || len(a.Rules) == 0 {
		v.node(n.ID, "ASSIGNEE_MISSING", "assignee rules are required")
		return
	}
	if len(a.Rules) > maxRulesPerNode {
		v.node(n.ID, "ASSIGNEE_TOO_MANY", "at most %d assignee rules per node", maxRulesPerNode)
	}
	if n.Type == NodeUserTask {
		switch a.Mode {
		case "", ModeAny, ModeAll, ModeSequential:
		default:
			v.node(n.ID, "ASSIGNEE_MODE", "unknown assignee mode %q", a.Mode)
		}
	}
	switch a.OnEmpty {
	case "", OnEmptySkip, OnEmptyAdmin:
	default:
		v.node(n.ID, "ASSIGNEE_ON_EMPTY", "unknown onEmpty policy %q", a.OnEmpty)
	}
	for _, r := range a.Rules {
		ok := true
		switch r.Kind {
		case RuleUser:
			ok = len(r.UserIDs) > 0 && len(r.UserIDs) <= maxRuleTargets
		case RuleRole:
			ok = len(r.RoleCodes) > 0 && len(r.RoleCodes) <= maxRuleTargets
		case RuleInitiator:
		case RuleDeptLeader:
			ok = r.Level >= 1 && r.Level <= maxDeptLevel
		case RuleDeptLeaderOf:
			ok = r.DeptID != "" && len(r.DeptID) <= 64
		case RuleFormUser, RuleFormDeptLeader:
			ok = r.Field != "" && len(r.Field) <= 64
		case RuleOncall:
			ok = r.ScheduleID != "" && len(r.ScheduleID) <= 64 && len(r.GroupID) <= 64
		default:
			v.node(n.ID, "ASSIGNEE_RULE_UNKNOWN", "unknown assignee rule kind %q", r.Kind)
			continue
		}
		if !ok {
			v.node(n.ID, "ASSIGNEE_RULE_INVALID", "assignee rule %q is incomplete", r.Kind)
		}
	}
}

// timeout 人工任务的处理时限配置。
func (v *validator) timeout(n *Node) {
	t := n.Timeout
	if t == nil || n.Type != NodeUserTask {
		return
	}
	switch t.Action {
	case TimeoutRemind, TimeoutApprove, TimeoutReject, TimeoutAdmin:
	default:
		v.node(n.ID, "TIMEOUT_INVALID", "unknown timeout action %q", t.Action)
	}
	if !t.valid() {
		v.node(n.ID, "TIMEOUT_INVALID", "timeout duration is invalid")
	}
	if r := t.Remind; r != nil && (!r.valid() || r.WorkTime != t.WorkTime || r.Unit != t.Unit || r.Value >= t.Value) {
		v.node(n.ID, "TIMEOUT_INVALID", "the reminder must use the same unit and calendar as the timeout and be shorter")
	}
}

// waitNode 定时 / 自动任务 / 子流程节点各自的配置。
func (v *validator) waitNode(n *Node) {
	switch n.Type {
	case NodeTimer:
		t := n.Timer
		ok := t != nil
		if ok {
			switch t.Mode {
			case TimerDelay:
				ok = t.Duration != nil && t.Duration.valid()
			case TimerUntil:
				ok = t.Field != "" && len(t.Field) <= 64
			case TimerSignal:
				ok = signalPattern.MatchString(t.Signal)
			default:
				ok = false
			}
		}
		if !ok {
			v.node(n.ID, "TIMER_INVALID", "timer configuration is incomplete or invalid")
		}
	case NodeService:
		if n.Service == nil || !uuidPattern.MatchString(n.Service.WebhookID) {
			v.node(n.ID, "SERVICE_INVALID", "an automatic task must reference a webhook endpoint")
		}
	case NodeSubProcess:
		if n.SubProcess == nil || !definitionKeyPattern.MatchString(n.SubProcess.DefinitionKey) {
			v.node(n.ID, "SUBPROCESS_INVALID", "a sub-process node must reference a workflow key")
		}
	}
}

// reachability 每个节点都从开始节点可达，且都能到达某个结束节点（无孤岛、无死路）。
func (v *validator) reachability() {
	g := v.g
	var start string
	rev := make(map[string][]string, len(g.Nodes))
	var ends []string
	for i := range g.Nodes {
		switch n := &g.Nodes[i]; n.Type {
		case NodeStart:
			start = n.ID
		case NodeEnd:
			ends = append(ends, n.ID)
		}
	}
	for i := range g.Edges {
		rev[g.Edges[i].Target] = append(rev[g.Edges[i].Target], g.Edges[i].Source)
	}
	forward := walk([]string{start}, func(id string) []string {
		next := make([]string, 0, len(g.out[id]))
		for _, e := range g.out[id] {
			next = append(next, e.Target)
		}
		return next
	})
	backward := walk(ends, func(id string) []string { return rev[id] })
	for i := range g.Nodes {
		id := g.Nodes[i].ID
		if _, ok := forward[id]; !ok {
			v.node(id, "UNREACHABLE", "node is not reachable from the start node")
		} else if _, ok := backward[id]; !ok {
			v.node(id, "DEAD_END", "node cannot reach any end node")
		}
	}
}

// walk 从 seeds 出发做广度遍历，返回访问到的节点集合。
func walk(seeds []string, next func(id string) []string) map[string]struct{} {
	seen := make(map[string]struct{}, len(seeds))
	queue := slices.Clone(seeds)
	for _, s := range seeds {
		seen[s] = struct{}{}
	}
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for _, n := range next(id) {
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				queue = append(queue, n)
			}
		}
	}
	return seen
}

// parallelRegions 校验并行网关构成严格的块结构，并计算每个节点所在的并行区域。
//
// 引擎在并行汇聚处等齐「入边条数」个执行位置才继续，因此到达汇聚的执行位置必须
// 恰好来自与它配对的那一个分叉的各条分支，一个不多、一个不少。为此要求：
//  1. 入边多于一条的并行网关必是汇聚，且恰被一个分叉引用；汇聚的入边数 = 分叉的分支数；
//  2. 分支 = 从分叉的一条出边出发、到达配对汇聚之前经过的全部节点；同一分叉的各分支
//     互不相交，分支内不得出现开始 / 结束节点，也不得绕回分叉；
//  3. 进入分支的连线只能来自分叉自身或同一分支内部（不能从外面连进并行分支）；
//     进入汇聚的连线只能来自分叉自身（空分支）或它的分支内部；
//  4. 嵌套分叉的汇聚必须位于外层分支之内（不能直接借用外层的汇聚）。
//
// 满足以上各条时，并行区域构成一棵嵌套的树，任何一次分叉产生的执行位置都会且只会
// 在它配对的汇聚处合并。
func (v *validator) parallelRegions() {
	g := v.g
	joinOf := make(map[string]string, 8)                 // 汇聚 → 分叉
	enclosing := make(map[string][]string, len(g.Nodes)) // 节点 → 包围它的「分叉#分支」
	type region struct {
		fork, join string
		inside     map[string]struct{} // 全部分支的节点并集
	}
	var regions []region
	for i := range g.Nodes {
		f := &g.Nodes[i]
		if !paired(f.Type) {
			continue
		}
		if !g.isFork(f) {
			if f.Join != "" {
				v.node(f.ID, "JOIN_NOT_ALLOWED", "only a forking parallel gateway may reference a join")
			}
			continue
		}
		j := g.nodes[f.Join]
		if j == nil || j.Type != f.Type || j.ID == f.ID {
			v.node(f.ID, "JOIN_MISSING", "a forking gateway must reference its joining gateway of the same type")
			continue
		}
		if other, taken := joinOf[j.ID]; taken {
			v.node(f.ID, "JOIN_SHARED", "join %q is already paired with fork %q", j.ID, other)
			continue
		}
		joinOf[j.ID] = f.ID
		if g.in[j.ID] != len(g.out[f.ID]) {
			v.node(f.ID, "JOIN_MISMATCH", "fork has %d branches but its join has %d incoming edges", len(g.out[f.ID]), g.in[j.ID])
		}
		r := region{fork: f.ID, join: j.ID, inside: map[string]struct{}{}}
		escaped, overlapped := false, false
		for branch, first := range g.out[f.ID] {
			if first.Target == j.ID {
				continue // 空分支：直接连到汇聚
			}
			nodes := walk([]string{first.Target}, func(id string) []string {
				next := make([]string, 0, len(g.out[id]))
				for _, e := range g.out[id] {
					if e.Target != j.ID {
						next = append(next, e.Target)
					}
				}
				return next
			})
			tag := f.ID + "#" + strconv.Itoa(branch)
			for id := range nodes {
				if t := g.nodes[id].Type; t == NodeStart || t == NodeEnd || id == f.ID {
					escaped = true
				}
				if _, dup := r.inside[id]; dup {
					overlapped = true
				}
				r.inside[id] = struct{}{}
				enclosing[id] = append(enclosing[id], tag)
			}
		}
		if escaped {
			v.node(f.ID, "PARALLEL_ESCAPE", "a parallel branch must reach its join; it cannot end the flow or leave the branch")
		}
		if overlapped {
			v.node(f.ID, "PARALLEL_OVERLAP", "branches of a parallel gateway must not share nodes")
		}
		regions = append(regions, r)
	}

	// 入边多于一条的并行网关必须是某个分叉的汇聚（否则它会永远等不齐）。
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if paired(n.Type) && g.in[n.ID] > 1 {
			if _, ok := joinOf[n.ID]; !ok {
				v.node(n.ID, "JOIN_UNPAIRED", "a parallel gateway with several incoming edges must be the join of a fork")
			}
		}
	}

	g.regions = make(map[string]string, len(enclosing))
	for id, tags := range enclosing {
		slices.Sort(tags)
		g.regions[id] = strings.Join(tags, ",")
	}

	// 连线不得穿越并行区域的边界。
	for _, r := range regions {
		for i := range g.Edges {
			e := &g.Edges[i]
			_, srcIn := r.inside[e.Source]
			_, dstIn := r.inside[e.Target]
			switch {
			case dstIn && !srcIn && e.Source != r.fork:
				// 从外面连进并行分支。
				v.edge(e.ID, "PARALLEL_ENTRY", "an edge may not enter a parallel branch from outside its fork")
			case dstIn && srcIn && g.regions[e.Source] != g.regions[e.Target] && !nestedStep(g, e):
				// 同一分叉内跨分支。
				v.edge(e.ID, "PARALLEL_CROSS", "an edge may not connect two different parallel branches")
			case e.Target == r.join && !srcIn && e.Source != r.fork:
				v.edge(e.ID, "PARALLEL_ENTRY", "an edge may not enter a join from outside its fork")
			}
		}
		// 嵌套：内层分叉位于本区域内时，它的汇聚也必须在本区域内。
		for inner, fork := range joinOf {
			if _, forkIn := r.inside[fork]; !forkIn {
				continue
			}
			if _, joinIn := r.inside[inner]; !joinIn {
				v.node(fork, "JOIN_OUTSIDE", "the join of a nested fork must stay inside the enclosing parallel branch")
			}
		}
	}
}

// nestedStep 报告连线是否是「进入内层分叉的分支」或「从内层分支到内层汇聚」这类合法的
// 嵌套层级变化：一端是并行网关，且两端的区域签名互为前缀扩展。
func nestedStep(g *Graph, e *Edge) bool {
	src, dst := g.nodes[e.Source], g.nodes[e.Target]
	a, b := g.regions[e.Source], g.regions[e.Target]
	if paired(src.Type) && isExtension(a, b) {
		return true // 分叉 → 分支首节点
	}
	return paired(dst.Type) && isExtension(b, a) // 分支末节点 → 汇聚
}

// isExtension 报告区域签名 inner 是否是在 outer 的基础上多套了若干层分支。
func isExtension(outer, inner string) bool {
	if outer == "" {
		return inner != ""
	}
	have := strings.Split(inner, ",")
	for _, tag := range strings.Split(outer, ",") {
		if !slices.Contains(have, tag) {
			return false
		}
	}
	return len(have) > strings.Count(outer, ",")+1
}

// jumps 拒绝去向必须留在同一并行区域内（跨区域移动执行位置会破坏并行配对）。
func (v *validator) jumps() {
	g := v.g
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.OnReject != nil && n.OnReject.To != "" && g.nodes[n.OnReject.To] != nil && !g.SameRegion(n.ID, n.OnReject.To) {
			v.node(n.ID, "ON_REJECT_REGION", "reject target must be in the same parallel branch")
		}
	}
}

// autoLoops 不经过任何人工任务的回路会让引擎空转：在去掉人工任务后的子图上找环。
func (v *validator) autoLoops() {
	g := v.g
	const (
		white = iota
		grey
		black
	)
	color := make(map[string]int, len(g.Nodes))
	var visit func(id string) bool
	visit = func(id string) bool {
		color[id] = grey
		for _, e := range g.out[id] {
			t := g.nodes[e.Target]
			if t.Type == NodeUserTask {
				continue
			}
			switch color[t.ID] {
			case grey:
				return true
			case white:
				if visit(t.ID) {
					return true
				}
			}
		}
		color[id] = black
		return false
	}
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.Type != NodeUserTask && color[n.ID] == white && visit(n.ID) {
			v.node(n.ID, "AUTO_LOOP", "the flow contains a loop that passes no user task")
			return
		}
	}
}

// validFormPermissions 节点的字段权限是否合法：字段码与表单标识非空且 ≤64 位、权限取值在
// 约定范围内、通用与各表单专属的合计条数不超过上限。
func validFormPermissions(n *Node) bool {
	valid := func(perms map[string]string) bool {
		for field, perm := range perms {
			if _, ok := permRank[perm]; !ok || field == "" || len(field) > 64 {
				return false
			}
		}
		return true
	}
	total := len(n.FormPermissions)
	if !valid(n.FormPermissions) {
		return false
	}
	for form, perms := range n.FormPermissionsByForm {
		if form == "" || len(form) > 64 || len(perms) == 0 || !valid(perms) {
			return false
		}
		total += len(perms)
	}
	return total <= maxFormPermissions
}
