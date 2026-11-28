// Package biz 承载业务核心：领域模型、仓储接口（依赖倒置）与用例。
// 本包禁止 import 传输层（transport/http、transport/grpc）与任何存储驱动。
//
// 分域结构：core 为单服务多域（模块化单体），各业务域在 internal/biz/<domain>/
// 子包实现（子包包名保持 biz，引用前缀不变）；本包只保留跨域聚合的
// ProviderSet，不放任何业务代码。新增域（如 workflow）时：建子包、
// 子包内定义自己的 ProviderSet，并在下方聚合处追加一行。
package biz

import (
	"context"
	"slices"
	"time"

	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	monitorv1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"

	ticketv1 "github.com/lanyulei/reborn/backend/core/api/ticket/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/google/wire"

	bizagent "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizincident "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	bizknowledge "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// ProviderSet 聚合各业务域的 biz providers；跨域窄接口在聚合处绑定
// （组合根性质：notify 不横向 import system，绑定只出现在此）。
var ProviderSet = wire.NewSet(
	bizsystem.ProviderSet,
	biznotify.ProviderSet,
	bizassistant.ProviderSet,
	bizcmdb.ProviderSet,
	bizoncall.ProviderSet,
	bizfile.ProviderSet,
	NewFileAccessCheckers,
	bizworkflow.ProviderSet,
	NewWorkflowListeners,
	NewWorkflowViewers,
	NewWorkflowDefinitionReferrers,
	// 流程引擎解析处理人 / 用户信息：由 system 域的组织目录实现（单向依赖 workflow → system）。
	wire.Bind(new(bizworkflow.Directory), new(*bizsystem.DirectoryUsecase)),
	// 「系统管理员」审批人规则：流程引擎定义的 Admins 由 system 域实现（单向依赖 workflow → system）。
	wire.Bind(new(bizworkflow.Admins), new(*bizsystem.AdminDirectory)),
	// 值班人规则：流程引擎定义的 Duty 由值班域实现（单向依赖 workflow → oncall 的只读查询）。
	wire.Bind(new(bizworkflow.Duty), new(*bizoncall.DutyUsecase)),
	NewWorkflowUsers,
	// 工单域：流程 / 附件 / 组织目录 / 管理视图判定由其他域实现（单向依赖 ticket → workflow / file / system）。
	bizticket.ProviderSet,
	wire.Bind(new(bizticket.Workflow), new(*bizworkflow.RuntimeUsecase)),
	wire.Bind(new(bizticket.WorkflowCatalog), new(*bizworkflow.DefinitionUsecase)),
	wire.Bind(new(bizticket.Attachments), new(*bizfile.Usecase)),
	// 系统 logo / 用户头像：内容经 file 域落在对象存储驱动里（单向依赖 system → file）。
	wire.Bind(new(bizsystem.SingletonFiles), new(*bizfile.Usecase)),
	wire.Bind(new(bizticket.Directory), new(*bizsystem.DirectoryUsecase)),
	NewTicketSupervisor,
	NewTicketCatalogManager,
	NewTicketDeadlines,
	NewTicketResources,
	wire.Bind(new(bizticket.WorkflowForms), new(*bizworkflow.RuntimeUsecase)),
	// 智能助手工具按用户授权过滤：有效操作集由 system 域 AuthzUsecase 解析；
	// 操作者展示信息由 system 域 UserUsecase 提供（单向依赖 assistant → system）。
	wire.Bind(new(bizassistant.PermissionSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizassistant.ActorDescriber), new(*bizsystem.UserUsecase)),
	// CMDB 数据范围：启用角色由 system 域 AuthzUsecase 解析，角色目录由 RoleUsecase 提供。
	wire.Bind(new(bizcmdb.RoleSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizcmdb.RoleDirectory), new(*bizsystem.RoleUsecase)),
	// 订阅代管校验：notify 定义的 AdminResolver 由 system 用户用例实现。
	wire.Bind(new(biznotify.SubscriptionAdminResolver), new(*bizsystem.UserUsecase)),
	// notify 控制面按 biz 隔离：角色 biz_scope 并集由 system 域 AuthzUsecase 解析。
	wire.Bind(new(biznotify.BizScopeResolver), new(*bizsystem.AuthzUsecase)),
	// 业务事件同事务直插（设计 §11）：system 消费 notify 的
	// NotificationWriter——实现固定为 NotificationUsecase（校验/补全收敛
	// 在 notify，system 仅提供业务语义字段；单向依赖 system → notify）。
	wire.Bind(new(biznotify.NotificationWriter), new(*biznotify.NotificationUsecase)),
	// 监控告警域：角色 / 组织目录由 system 域提供，当前值班人与值班表名称由 oncall 域提供，通知事件经
	// notify 的 NotificationWriter 写入（单向依赖 monitor → system / oncall / notify）。
	bizmonitor.ProviderSet,
	wire.Bind(new(bizmonitor.RoleSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizmonitor.Directory), new(*bizsystem.DirectoryUsecase)),
	wire.Bind(new(bizmonitor.DeptLeaders), new(*bizsystem.DirectoryUsecase)),
	wire.Bind(new(bizmonitor.Duty), new(*bizoncall.DutyUsecase)),
	NewMonitorRoles,
	NewMonitorSchedules,
	NewMonitorTargets,
	NewMonitorPermissions,
	// 监控告警域第二、三期：工单 / 流程 / CMDB / 值班 / 助手 / 通知路由的窄接口适配器（monitor_p2.go、monitor_route.go）。
	MonitorP2ProviderSet,
	NewMonitorRouteProbe,
	// 作业中心域：用户显示名由 system 域提供，目标主机经适配器按 CMDB 数据范围解析
	// （单向依赖 job → system / cmdb；适配器见 job_hosts.go）。
	bizjob.ProviderSet,
	wire.Bind(new(bizjob.Directory), new(*bizsystem.DirectoryUsecase)),
	NewJobHostSource,
	// 作业中心二期 / 三期：审批与信号经流程引擎，结束通知经通知域，待分发的文件经文件域，
	// 告警自愈读监控域的告警事件（适配器见 job_integrations.go；单向依赖 job → 各域）。
	NewJobApprovals, NewJobApprovalEvidence, NewJobSignals, NewJobRunPermission, NewJobNotifier, NewJobSiteURL, NewJobFiles, NewJobAlertFeed, NewJobAlertRules,
	// 容器平台域：启用角色与账号状态、用户显示名由 system 域提供，通知事件经 notify 的
	// NotificationWriter 写入（单向依赖 k8s → system / notify）。
	bizk8s.ProviderSet,
	wire.Bind(new(bizk8s.RoleSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizk8s.Accounts), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizk8s.Directory), new(*bizsystem.DirectoryUsecase)),
	// 可观测：指标面板经 monitor 域的数据源查询，节点与主机的关联经 cmdb 域按其数据范围查找
	// （单向依赖 k8s → monitor / cmdb；适配器见 k8s_insight.go）。
	NewK8sMetricsSource,
	NewK8sHostFinder,
	// 变更审批：审批就是一张工单（ticket / workflow 域），免审批权限与自助授权角色由 system 域提供
	// （单向依赖 k8s → ticket / workflow / system；适配器见 k8s_change.go）。
	NewK8sChangeTickets, NewK8sTicketViewers, NewK8sTicketNotes, NewK8sBypassChecker, NewK8sSelfRoles, NewK8sChangeSiteURL,
	// 知识库域：启用角色 / 组织目录 / 角色目录由 system 域提供，附件经 file 域，AI 摘要复用监控域的摘要适配器，
	// 通知事件经 notify 的 NotificationWriter 写入；助手的页内上下文由知识库的文档用例提供
	// （单向依赖 knowledge → system / file / notify；assistant → knowledge 只经窄接口）。
	bizknowledge.ProviderSet,
	wire.Bind(new(bizknowledge.RoleSource), new(*bizsystem.AuthzUsecase)),
	wire.Bind(new(bizknowledge.Directory), new(*bizsystem.DirectoryUsecase)),
	wire.Bind(new(bizknowledge.Files), new(*bizfile.Usecase)),
	NewKnowledgeRoles,
	NewKnowledgeSummarizer,
	// 故障管理域：组织目录 / 管理员 / 接口权限由 system 域提供，值班与值班表由 oncall 域提供，告警与匹配器复用
	// 监控域，资源与拓扑经 cmdb 域，变更来源来自 job / workflow / k8s，工单 / 流程 / 作业以代理执行人身份经各域
	// 用例（其权限与风险门禁照常生效），AI 摘要复用监控域的摘要适配器（适配器见 incident_adapters.go；
	// 单向依赖 incident → 各域）。
	bizincident.ProviderSet,
	IncidentProviderSet,
	wire.Bind(new(bizincident.DeptLeaders), new(*bizsystem.DirectoryUsecase)),
	// 助手页内上下文：知识库文档与故障各自实现，按对象类型分发（assistant → knowledge / incident 只经窄接口）。
	NewAssistantContextProviders,
	// 运维 Agent 域：无人值守运行 / 会话 / 工具目录 / 回环执行器复用 assistant，机器账号与权限由 system 提供，故障快照与成员
	// 判定由 incident 提供，自监控事件经 notify 写入；incident → agent 的入队窄接口只依赖任务仓储（适配器见 agent_adapters.go）。
	bizagent.ProviderSet,
	AgentProviderSet,
)

// assistantContexts 按对象类型把助手的页内上下文请求分发到持有该对象的业务域。
type assistantContexts struct {
	knowledge *bizknowledge.DocumentUsecase
	incidents *bizincident.IncidentUsecase
}

// NewAssistantContextProviders 装配助手页内上下文提供者（对象类型 → 业务域）。
func NewAssistantContextProviders(knowledge *bizknowledge.DocumentUsecase, incidents *bizincident.IncidentUsecase) bizassistant.ContextProvider {
	return assistantContexts{knowledge: knowledge, incidents: incidents}
}

// DescribeContext implements bizassistant.ContextProvider.
func (p assistantContexts) DescribeContext(ctx context.Context, userID, typ, id string) (module, text string, err error) {
	if typ == bizincident.ContextType {
		return p.incidents.DescribeContext(ctx, userID, typ, id)
	}
	return p.knowledge.DescribeContext(ctx, userID, typ, id)
}

// NewFileAccessCheckers 装配附件访问检查器（业务类型 → 检查器）：持有附件的业务域
// 在此登记自己的检查器；未登记的业务类型其附件一律不可读（deny-by-default）。
func NewFileAccessCheckers(tickets *bizticket.FileAccess, knowledge *bizknowledge.FileAccess, incidents *bizincident.FileAccess) bizfile.AccessCheckers {
	return bizfile.AccessCheckers{bizticket.BusinessType: tickets, bizknowledge.BusinessType: knowledge, bizincident.BusinessType: incidents}
}

// NewWorkflowListeners 装配流程事件监听器（业务类型 → 监听器）：基于流程引擎的业务域
// 在此登记，实例结束时在引擎的同一事务内收到回调。
func NewWorkflowListeners(tickets *bizticket.StatusSyncer, jobs *bizjob.ApprovalSyncer, k8sChanges *bizk8s.ChangeSyncer) bizworkflow.Listeners {
	return bizworkflow.Listeners{
		// 工单流程结束时除了同步工单状态，再把审批结果落到容器平台的变更单上（见 k8s_change.go）。
		bizticket.BusinessType: ticketWithK8sChange{StatusSyncer: tickets, changes: k8sChanges},
		// 作业审批：流程结束时把结果落到执行上（通过 → 排队，否则 → 审批未通过）。
		jobcontract.ApprovalBusinessType: jobApprovalListener{syncer: jobs},
	}
}

// NewWorkflowViewers 装配流程实例的业务侧查看权限（业务类型 → 判定）。
func NewWorkflowViewers(tickets *bizticket.Access, authz *bizsystem.AuthzUsecase) bizworkflow.Viewers {
	return bizworkflow.Viewers{
		bizticket.BusinessType:           tickets,
		jobcontract.ApprovalBusinessType: jobApprovalViewer{authz: authz},
	}
}

// NewWorkflowDefinitionReferrers 装配「谁还在引用某个流程定义」的业务域：流程被停用 / 删除
// 之前引擎逐个询问，仍被引用即拒绝。基于流程引擎的业务域在此登记自己的实现——须是不依赖
// 流程用例的类型（如只读仓储），否则与 DefinitionUsecase 互相依赖成环。
func NewWorkflowDefinitionReferrers(tickets bizticket.WorkflowReferences) bizworkflow.DefinitionReferrers {
	return bizworkflow.DefinitionReferrers{tickets}
}

// ticketSupervisor 以接口授权判定「能否查看全部工单」：持有「查询全部工单」接口权限
// （超管恒为 true）。操作码与授权中间件同一口径。
type ticketSupervisor struct {
	authz *bizsystem.AuthzUsecase
}

// NewTicketSupervisor 构造工单域的管理视图判定器。
func NewTicketSupervisor(authz *bizsystem.AuthzUsecase) bizticket.Supervisor {
	return ticketSupervisor{authz: authz}
}

// CanViewAll implements bizticket.Supervisor.
func (s ticketSupervisor) CanViewAll(ctx context.Context, userID string) (bool, error) {
	return s.authz.Allowed(ctx, userID, bizsystem.OperationCode(ticketv1.OperationTicketServiceListTickets))
}

// NewTicketCatalogManager 构造工单域的完整目录查看判定器：持有「查询完整服务目录」接口权限
// （超管恒为 true）者可读已停用的模板。
func NewTicketCatalogManager(authz *bizsystem.AuthzUsecase) bizticket.CatalogManager {
	return ticketSupervisor{authz: authz}
}

// CanManageCatalog implements bizticket.CatalogManager.
func (s ticketSupervisor) CanManageCatalog(ctx context.Context, userID string) (bool, error) {
	return s.authz.Allowed(ctx, userID, bizsystem.OperationCode(ticketv1.OperationTicketCatalogServiceListManagedCatalog))
}

// ticketDeadlines 把流程引擎的工作日历适配为工单的时限推算（SLA 与流程时限用同一份日历）。
type ticketDeadlines struct {
	calendar *bizworkflow.CalendarUsecase
}

// NewTicketDeadlines 构造工单域的时限推算器。
func NewTicketDeadlines(calendar *bizworkflow.CalendarUsecase) bizticket.Deadlines {
	return ticketDeadlines{calendar: calendar}
}

// Due implements bizticket.Deadlines.
func (d ticketDeadlines) Due(ctx context.Context, from time.Time, dur bizticket.Duration) (time.Time, error) {
	return d.calendar.Due(ctx, from, bizworkflow.Duration{Value: int(dur.Value), Unit: dur.Unit, WorkTime: dur.WorkTime})
}

// ticketResources 把 CMDB 的资源查询适配为工单的资源解析：只返回用户有权查看的资源
// （持有资源列表接口权限，且资源在他的数据范围内）。
type ticketResources struct {
	cis    *bizcmdb.CIUsecase
	scopes *bizcmdb.ScopeUsecase
	authz  *bizsystem.AuthzUsecase
}

// NewTicketResources 构造工单域的资源解析器。
func NewTicketResources(cis *bizcmdb.CIUsecase, scopes *bizcmdb.ScopeUsecase, authz *bizsystem.AuthzUsecase) bizticket.Resources {
	return ticketResources{cis: cis, scopes: scopes, authz: authz}
}

// Resolve implements bizticket.Resources.
func (r ticketResources) Resolve(ctx context.Context, userID string, ids []string) ([]bizticket.Resource, error) {
	ok, err := r.authz.Allowed(ctx, userID, bizsystem.OperationCode(cmdbv1.OperationCmdbCIServiceListCIs))
	if err != nil || !ok {
		return nil, err
	}
	scope, err := r.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]bizticket.Resource, 0, len(ids))
	for _, id := range ids {
		ci, err := r.cis.GetByID(ctx, id)
		if errors.Is(err, bizcmdb.ErrCINotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if scope.Allows(ci.ModelCode, ci.Env) {
			out = append(out, bizticket.Resource{ID: ci.ID, Name: ci.Name, ModelName: ci.ModelName})
		}
	}
	return out, nil
}

// workflowUsers 把 system 域的组织目录适配为流程引擎的用户目录（两域各自的展示
// 模型不互相引用，转换只出现在组合根）。
type workflowUsers struct {
	dir *bizsystem.DirectoryUsecase
}

// NewWorkflowUsers 构造流程引擎的用户目录适配器。
func NewWorkflowUsers(dir *bizsystem.DirectoryUsecase) bizworkflow.Users {
	return workflowUsers{dir: dir}
}

// UserNames implements bizworkflow.Users.
func (w workflowUsers) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	return w.dir.UserNames(ctx, ids)
}

// Candidates implements bizworkflow.Users.
func (w workflowUsers) Candidates(ctx context.Context, keyword string, limit int64) ([]bizworkflow.UserInfo, error) {
	users, err := w.dir.CandidateUsers(ctx, keyword, limit)
	if err != nil {
		return nil, err
	}
	out := make([]bizworkflow.UserInfo, 0, len(users))
	for _, u := range users {
		out = append(out, bizworkflow.UserInfo{ID: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return out, nil
}

// PrimaryDept implements bizworkflow.Users.
func (w workflowUsers) PrimaryDept(ctx context.Context, userID string) (string, []string, error) {
	return w.dir.PrimaryDept(ctx, userID)
}

// monitorRoles 把 system 域的角色列表适配为监控域的角色目录（分组授权页展示角色名与启用状态）。
type monitorRoles struct {
	roles *bizsystem.RoleUsecase
}

// NewMonitorRoles 构造监控域的角色目录适配器。
func NewMonitorRoles(roles *bizsystem.RoleUsecase) bizmonitor.RoleDirectory {
	return monitorRoles{roles: roles}
}

// ListRoles implements bizmonitor.RoleDirectory（角色为小集合，按页上限循环取全）。
func (m monitorRoles) ListRoles(ctx context.Context) ([]bizmonitor.RoleRef, error) {
	var out []bizmonitor.RoleRef
	for page := int64(1); ; page++ {
		roles, total, err := m.roles.List(ctx, bizsystem.RoleFilter{}, page, paging.MaxPageSize)
		if err != nil {
			return nil, err
		}
		for _, r := range roles {
			out = append(out, bizmonitor.RoleRef{ID: r.ID, Name: r.Name, Enabled: r.Status == bizsystem.RoleStatusEnabled})
		}
		if int64(len(out)) >= total || len(roles) == 0 {
			return out, nil
		}
	}
}

// monitorSchedules 把 oncall 域的值班表列表适配为监控域的值班表名称查询。
type monitorSchedules struct {
	schedules *bizoncall.ScheduleUsecase
}

// NewMonitorSchedules 构造监控域的值班表名称适配器。
func NewMonitorSchedules(schedules *bizoncall.ScheduleUsecase) bizmonitor.Schedules {
	return monitorSchedules{schedules: schedules}
}

// monitorScheduleLookups 逐个读取值班表的个数上限：超过它改为一次读全部。值班域没有按 ID 批量取名称的
// 查询，而「全部值班表」会连同轮转名单一起读出来——保存规则、派发通知时通常只涉及一两张值班表，
// 不值得为它们把所有值班表都读一遍。
const monitorScheduleLookups = 4

// ScheduleNames implements bizmonitor.Schedules：只返回启用的值班表（停用的不会有值班人）。
func (m monitorSchedules) ScheduleNames(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if len(ids) <= monitorScheduleLookups {
		for _, id := range ids {
			s, err := m.schedules.Get(ctx, id)
			if errors.Is(err, bizoncall.ErrScheduleNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if s.Status == bizoncall.StatusEnabled {
				out[s.ID] = s.Name
			}
		}
		return out, nil
	}
	all, err := m.schedules.List(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, s := range all {
		if s.Status == bizoncall.StatusEnabled && slices.Contains(ids, s.ID) {
			out[s.ID] = s.Name
		}
	}
	return out, nil
}

// monitorTargets 监控域通知对象选择器的候选项来源：值班表来自 oncall 域，部门与用户来自 system 域
// （单向依赖 monitor → oncall / system 的只读查询）。
type monitorTargets struct {
	schedules *bizoncall.ScheduleUsecase
	depts     *bizsystem.DeptUsecase
	dir       *bizsystem.DirectoryUsecase
}

// NewMonitorTargets 构造监控域的通知对象候选项适配器。
func NewMonitorTargets(schedules *bizoncall.ScheduleUsecase, depts *bizsystem.DeptUsecase, dir *bizsystem.DirectoryUsecase) bizmonitor.TargetSource {
	return monitorTargets{schedules: schedules, depts: depts, dir: dir}
}

// Schedules implements bizmonitor.TargetSource：启用的值班表（oncall 的列表已按值班组、名称排序）。
func (m monitorTargets) Schedules(ctx context.Context) ([]bizmonitor.TargetSchedule, error) {
	all, err := m.schedules.List(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.TargetSchedule, 0, len(all))
	for _, s := range all {
		if s.Status == bizoncall.StatusEnabled {
			out = append(out, bizmonitor.TargetSchedule{ID: s.ID, Name: s.Name, GroupName: s.GroupName})
		}
	}
	return out, nil
}

// Depts implements bizmonitor.TargetSource：全部部门（system 的列表已按层级排序，上级在前）。
func (m monitorTargets) Depts(ctx context.Context) ([]bizmonitor.TargetDept, error) {
	all, err := m.depts.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.TargetDept, 0, len(all))
	for _, d := range all {
		out = append(out, bizmonitor.TargetDept{ID: d.ID, Name: d.Name, ParentID: d.ParentID})
	}
	return out, nil
}

// Users implements bizmonitor.TargetSource：可用用户的最小披露视图。
func (m monitorTargets) Users(ctx context.Context, keyword string, limit int64) ([]bizmonitor.TargetUser, error) {
	users, err := m.dir.CandidateUsers(ctx, keyword, limit)
	if err != nil {
		return nil, err
	}
	out := make([]bizmonitor.TargetUser, 0, len(users))
	for _, u := range users {
		out = append(out, bizmonitor.TargetUser{ID: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return out, nil
}

// monitorPermissions 以接口授权回答监控域的两处权限判定（操作码与授权中间件同一口径；超管恒为 true，
// 非可用用户恒为 false）。
type monitorPermissions struct {
	authz *bizsystem.AuthzUsecase
}

// NewMonitorPermissions 构造监控域的接口权限判定器。
func NewMonitorPermissions(authz *bizsystem.AuthzUsecase) bizmonitor.Permissions {
	return monitorPermissions{authz: authz}
}

// CanReadAlerts implements bizmonitor.Permissions：持有「查询告警事件」接口权限。告警详情免接口授权
// （通知收件人要能打开），非收件人能否查看由它与数据范围共同决定。
func (p monitorPermissions) CanReadAlerts(ctx context.Context, userID string) (bool, error) {
	return p.authz.Allowed(ctx, userID, bizsystem.OperationCode(monitorv1.OperationMonitorAlertServiceListAlerts))
}

// CanEditDatasource implements bizmonitor.Permissions：持有「编辑监控数据源」接口权限。
func (p monitorPermissions) CanEditDatasource(ctx context.Context, userID string) (bool, error) {
	return p.authz.Allowed(ctx, userID, bizsystem.OperationCode(monitorv1.OperationMonitorDatasourceServiceUpdateDatasource))
}
