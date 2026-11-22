package biz

import (
	"fmt"
	"slices"
	"sync"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// ErrGrantInvalid 哨兵：授权请求携带未登记在权限目录内的菜单/元素码（400）。
var ErrGrantInvalid = errors.BadRequest(v1.ErrorReason_GRANT_INVALID.String(), "unknown menu or element code")

// MenuSpec 权限目录的菜单（页面）规格：页面默认接口 + 页面元素及其接口。
type MenuSpec struct {
	// Code 菜单码，与前端路由对齐（如 system.user）。
	Code string
	// Name 中文展示名。
	Name string
	// DefaultOps 页面默认接口（进页面即调用、不归属任何元素）的 proto full method。
	DefaultOps []string
	// Elements 页面内功能元素清单。
	Elements []ElementSpec
}

// ElementSpec 权限目录的页面元素规格：页面内一个功能标签（如「新建用户」
// 按钮）及其绑定的接口。
type ElementSpec struct {
	// Code 元素码，以所属菜单码为前缀（如 system.user.create）。
	Code string
	// Name 中文展示名。
	Name string
	// Ops 该元素从属的接口 proto full method（页面结构说明，授权不级联）。
	Ops []string
}

// permissionCatalog 权限目录注册表：菜单→页面元素→接口的结构描述（单一
// 事实源，代码内置随版本演进，不落库）。仅供两处消费：授权页渲染
// （ListPermissionCatalog）与菜单/元素码合法性校验（ValidateGrantCodes）。
// 目录中登记的接口绑定关系是「页面结构与接口的从属说明」，不参与授权
// 计算——菜单/元素授权只控前端可见性，接口权限一律来自 apis 直接授权。
// 引用的接口必须在 proto 反射枚举内且带中文展示名（service 层测试锁死）。
// 新增页面/元素的流程：前端落地路由与元素 → 在此登记目录。
var permissionCatalog = []MenuSpec{
	{
		Code: "system.user",
		Name: "用户管理",
		// 部门列表：用户页的部门筛选与所属部门选择依赖部门树。
		DefaultOps: []string{
			"/system.v1.UserService/ListUsers",
			"/system.v1.DeptService/ListDepts",
		},
		Elements: []ElementSpec{
			{
				Code: "system.user.create",
				Name: "新建用户",
				Ops:  []string{"/system.v1.UserService/CreateUser"},
			},
			{
				Code: "system.user.machine",
				Name: "新建机器账号",
				Ops: []string{
					"/system.v1.UserService/CreateMachineUser",
					"/system.v1.RoleService/ListRoles",
				},
			},
			{
				Code: "system.user.edit",
				Name: "编辑用户",
				Ops: []string{
					"/system.v1.UserService/GetUser",
					"/system.v1.UserService/UpdateUser",
				},
			},
			{
				Code: "system.user.status",
				Name: "变更用户状态",
				Ops:  []string{"/system.v1.UserService/UpdateUserStatus"},
			},
			{
				Code: "system.user.reset_password",
				Name: "重置用户密码",
				Ops:  []string{"/system.v1.UserService/ResetUserPassword"},
			},
			{
				Code: "system.user.bind_role",
				Name: "绑定用户角色",
				Ops: []string{
					"/system.v1.UserService/ListUserRoles",
					"/system.v1.UserService/BindUserRoles",
					"/system.v1.RoleService/ListRoles",
				},
			},
			{
				Code: "system.user.delete",
				Name: "删除用户",
				Ops:  []string{"/system.v1.UserService/DeleteUser"},
			},
		},
	},
	{
		Code: "system.role",
		Name: "角色管理",
		DefaultOps: []string{
			"/system.v1.RoleService/ListRoles",
		},
		Elements: []ElementSpec{
			{
				Code: "system.role.create",
				Name: "新建角色",
				Ops:  []string{"/system.v1.RoleService/CreateRole"},
			},
			{
				Code: "system.role.edit",
				Name: "编辑角色",
				Ops: []string{
					"/system.v1.RoleService/GetRole",
					"/system.v1.RoleService/UpdateRole",
				},
			},
			{
				Code: "system.role.grant",
				Name: "角色授权",
				Ops: []string{
					"/system.v1.RoleService/GetRolePermissions",
					"/system.v1.RoleService/SetRolePermissions",
					"/system.v1.RoleService/ListPermissionCatalog",
					"/system.v1.RoleService/ListOperations",
					// 容器平台数据范围（角色在哪些集群的哪些命名空间有什么档位）与选择集群用的集群列表。
					"/k8s.v1.K8sScopeService/GetRoleScope",
					"/k8s.v1.K8sScopeService/ReplaceRoleScope",
					"/k8s.v1.K8sClusterService/ListClusters",
				},
			},
			{
				Code: "system.role.delete",
				Name: "删除角色",
				Ops:  []string{"/system.v1.RoleService/DeleteRole"},
			},
		},
	},
	{
		Code: "system.dept",
		Name: "部门管理",
		// 成员抽屉与负责人/成员选择走部门域自己的最小披露接口，不附带完整用户列表。
		DefaultOps: []string{
			"/system.v1.DeptService/ListDepts",
			"/system.v1.DeptService/ListDeptMembers",
			"/system.v1.DeptService/ListUserCandidates",
		},
		Elements: []ElementSpec{
			{
				Code: "system.dept.create",
				Name: "新建部门",
				Ops:  []string{"/system.v1.DeptService/CreateDept"},
			},
			{
				Code: "system.dept.edit",
				Name: "编辑部门",
				// 编辑表单可同时调整上级部门，移动与更新一并授权
				Ops: []string{
					"/system.v1.DeptService/GetDept",
					"/system.v1.DeptService/UpdateDept",
					"/system.v1.DeptService/MoveDept",
				},
			},
			{
				Code: "system.dept.member",
				Name: "管理部门成员",
				Ops: []string{
					"/system.v1.DeptService/AddDeptMembers",
					"/system.v1.DeptService/RemoveDeptMember",
				},
			},
			{
				Code: "system.dept.delete",
				Name: "删除部门",
				Ops:  []string{"/system.v1.DeptService/DeleteDept"},
			},
		},
	},
	{
		Code: "workflow.task",
		Name: "我的流程",
		// 个人工作台：待办 / 已办 / 抄送 / 我发起的；看得到待办即可处理，处理动作随页面默认授权。
		DefaultOps: []string{
			"/workflow.v1.WorkflowTaskService/ListTodoTasks",
			"/workflow.v1.WorkflowTaskService/ListDoneTasks",
			"/workflow.v1.WorkflowTaskService/ListCcTasks",
			"/workflow.v1.WorkflowInstanceService/ListMyInstances",
			"/workflow.v1.WorkflowInstanceService/GetInstance",
			"/workflow.v1.WorkflowInstanceService/CancelInstance",
			"/workflow.v1.WorkflowTaskService/ApproveTask",
			"/workflow.v1.WorkflowTaskService/RejectTask",
			"/workflow.v1.WorkflowTaskService/ReturnTask",
			"/workflow.v1.WorkflowTaskService/ListReturnTargets",
			"/workflow.v1.WorkflowTaskService/TransferTask",
			"/workflow.v1.WorkflowTaskService/AddSignTask",
			"/workflow.v1.WorkflowTaskService/DelegateTask",
			"/workflow.v1.WorkflowTaskService/ListUserCandidates",
			// 个人代理设置（请假期间把新到的待办交给代理人）。
			"/workflow.v1.WorkflowConfigService/ListMyDelegations",
			"/workflow.v1.WorkflowConfigService/CreateDelegation",
			"/workflow.v1.WorkflowConfigService/DeleteDelegation",
		},
		Elements: []ElementSpec{
			{
				Code: "workflow.task.start",
				Name: "发起流程",
				Ops: []string{
					"/workflow.v1.WorkflowInstanceService/StartInstance",
					"/workflow.v1.WorkflowDefinitionService/ListDefinitions",
				},
			},
		},
	},
	{
		Code: "workflow.definition",
		Name: "流程设计",
		DefaultOps: []string{
			"/workflow.v1.WorkflowDefinitionService/ListDefinitions",
		},
		Elements: []ElementSpec{
			{
				Code: "workflow.definition.create",
				Name: "新建流程",
				Ops:  []string{"/workflow.v1.WorkflowDefinitionService/CreateDefinition"},
			},
			{
				Code: "workflow.definition.edit",
				Name: "编辑流程",
				// 设计器需要：读写草稿、模拟运行、查看历史版本，以及选人 / 选角色 / 选部门 /
				// 选 Webhook 端点的候选数据。
				Ops: []string{
					"/workflow.v1.WorkflowDefinitionService/GetDefinition",
					"/workflow.v1.WorkflowDefinitionService/UpdateDefinition",
					"/workflow.v1.WorkflowDefinitionService/SaveDefinitionDraft",
					"/workflow.v1.WorkflowDefinitionService/SimulateDefinition",
					"/workflow.v1.WorkflowDefinitionService/ListDefinitionVersions",
					"/workflow.v1.WorkflowDefinitionService/GetDefinitionVersion",
					"/workflow.v1.WorkflowConfigService/ListWebhooks",
					"/workflow.v1.WorkflowTaskService/ListUserCandidates",
					"/system.v1.RoleService/ListRoles",
					"/system.v1.DeptService/ListDepts",
					// 值班人规则：选值班组与其下的值班表。
					"/oncall.v1.OncallGroupService/ListGroups",
					"/oncall.v1.OncallScheduleService/ListSchedules",
				},
			},
			{
				Code: "workflow.definition.publish",
				Name: "发布流程",
				Ops:  []string{"/workflow.v1.WorkflowDefinitionService/PublishDefinition"},
			},
			{
				Code: "workflow.definition.delete",
				Name: "删除流程",
				Ops:  []string{"/workflow.v1.WorkflowDefinitionService/DeleteDefinition"},
			},
		},
	},
	{
		Code: "workflow.instance",
		Name: "流程监控",
		// 管理视图：可查看全部流程实例（持有 ListInstances 即可看任意实例详情）。
		DefaultOps: []string{
			"/workflow.v1.WorkflowInstanceService/ListInstances",
			"/workflow.v1.WorkflowInstanceService/GetInstance",
		},
		Elements: []ElementSpec{
			{
				Code: "workflow.instance.terminate",
				Name: "终止流程",
				Ops:  []string{"/workflow.v1.WorkflowInstanceService/TerminateInstance"},
			},
			{
				Code: "workflow.instance.reassign",
				Name: "改派任务",
				Ops: []string{
					"/workflow.v1.WorkflowTaskService/ReassignTask",
					"/workflow.v1.WorkflowTaskService/ListUserCandidates",
				},
			},
			{
				Code: "workflow.instance.wait",
				Name: "处理等待节点",
				// 自动任务进入死信后的重试 / 跳过、跳过定时节点、投递外部信号。
				Ops: []string{
					"/workflow.v1.WorkflowInstanceService/RetryInstanceJob",
					"/workflow.v1.WorkflowInstanceService/SkipInstanceJob",
					"/workflow.v1.WorkflowInstanceService/SignalInstance",
				},
			},
			{
				Code: "workflow.instance.handover",
				Name: "交接待办",
				// 离职 / 调岗：把某人名下全部待办换给另一个人。
				Ops: []string{
					"/workflow.v1.WorkflowInstanceService/HandoverTasks",
					"/workflow.v1.WorkflowTaskService/ListUserCandidates",
				},
			},
		},
	},
	{
		Code: "workflow.config",
		Name: "流程配置",
		// 工作日历与自动任务可调用的 Webhook 端点。
		DefaultOps: []string{
			"/workflow.v1.WorkflowConfigService/GetCalendar",
			"/workflow.v1.WorkflowConfigService/ListWebhooks",
			"/workflow.v1.WorkflowConfigService/ListClients",
		},
		Elements: []ElementSpec{
			{
				Code: "workflow.config.calendar",
				Name: "编辑工作日历",
				Ops:  []string{"/workflow.v1.WorkflowConfigService/UpdateCalendar"},
			},
			{
				Code: "workflow.config.webhook",
				Name: "维护 Webhook 端点",
				Ops: []string{
					"/workflow.v1.WorkflowConfigService/CreateWebhook",
					"/workflow.v1.WorkflowConfigService/UpdateWebhook",
					"/workflow.v1.WorkflowConfigService/DeleteWebhook",
				},
			},
			{
				Code: "workflow.config.client",
				Name: "维护接入凭据",
				// 外部系统的接入令牌：创建 / 重置时令牌明文只返回一次。表单里要选代表用户与允许的流程。
				Ops: []string{
					"/workflow.v1.WorkflowConfigService/CreateClient",
					"/workflow.v1.WorkflowConfigService/UpdateClient",
					"/workflow.v1.WorkflowConfigService/RotateClientToken",
					"/workflow.v1.WorkflowConfigService/DeleteClient",
					"/workflow.v1.WorkflowTaskService/ListUserCandidates",
					"/workflow.v1.WorkflowDefinitionService/ListDefinitions",
				},
			},
		},
	},
	{
		Code: "ticket.mine",
		Name: "我的工单",
		// 个人视图：提单、我提交的、待我处理的；处理动作走流程引擎的任务接口，随页面默认授权。
		DefaultOps: []string{
			"/ticket.v1.TicketService/ListMyTickets",
			"/ticket.v1.TicketService/GetTicket",
			"/ticket.v1.TicketService/ListTicketComments",
			"/ticket.v1.TicketService/AddTicketComment",
			"/ticket.v1.TicketService/UpdateTicketForm",
			"/ticket.v1.TicketService/RateTicket",
			"/workflow.v1.WorkflowTaskService/ListTodoTasks",
			"/workflow.v1.WorkflowTaskService/ListDoneTasks",
			// 抄送给我的工单（工单列表的「抄送我的」页签）。
			"/workflow.v1.WorkflowTaskService/ListCcTasks",
			"/workflow.v1.WorkflowInstanceService/GetInstance",
			"/workflow.v1.WorkflowInstanceService/CancelInstance",
			"/workflow.v1.WorkflowTaskService/ApproveTask",
			"/workflow.v1.WorkflowTaskService/RejectTask",
			"/workflow.v1.WorkflowTaskService/ReturnTask",
			"/workflow.v1.WorkflowTaskService/ListReturnTargets",
			"/workflow.v1.WorkflowTaskService/TransferTask",
			"/workflow.v1.WorkflowTaskService/AddSignTask",
			"/workflow.v1.WorkflowTaskService/DelegateTask",
			"/workflow.v1.WorkflowTaskService/ListUserCandidates",
			// 个人代理设置（工单列表里的入口；请假期间把新到的待办交给代理人）。
			"/workflow.v1.WorkflowConfigService/ListMyDelegations",
			"/workflow.v1.WorkflowConfigService/CreateDelegation",
			"/workflow.v1.WorkflowConfigService/DeleteDelegation",
			// 处理人修改表单里的部门字段时要加载部门树（不只是提单时）。
			"/system.v1.DeptService/ListDepts",
		},
		Elements: []ElementSpec{
			{
				Code: "ticket.mine.create",
				Name: "提交工单",
				// 提单页需要：服务目录、模板表单，以及表单里选部门 / 选人的候选数据。
				Ops: []string{
					"/ticket.v1.TicketService/CreateTicket",
					"/ticket.v1.TicketCatalogService/ListCatalog",
					"/ticket.v1.TicketCatalogService/GetTemplate",
					"/system.v1.DeptService/ListDepts",
				},
			},
		},
	},
	{
		Code: "ticket.catalog",
		Name: "服务目录",
		// 管理页看完整目录（含已停用的模板）。
		DefaultOps: []string{
			"/ticket.v1.TicketCatalogService/ListManagedCatalog",
		},
		Elements: []ElementSpec{
			{
				Code: "ticket.catalog.category",
				Name: "维护工单分类",
				Ops: []string{
					"/ticket.v1.TicketCatalogService/CreateCategory",
					"/ticket.v1.TicketCatalogService/UpdateCategory",
					"/ticket.v1.TicketCatalogService/DeleteCategory",
				},
			},
			{
				Code: "ticket.catalog.template",
				Name: "维护工单模板",
				// 模板表单里要选择所绑定的流程，以及 SLA 超时的升级通知对象（选人）。
				Ops: []string{
					"/ticket.v1.TicketCatalogService/GetTemplate",
					"/ticket.v1.TicketCatalogService/CreateTemplate",
					"/ticket.v1.TicketCatalogService/UpdateTemplate",
					"/ticket.v1.TicketCatalogService/DeleteTemplate",
					"/workflow.v1.WorkflowDefinitionService/ListDefinitions",
					// 改字段码 / 删字段前提醒「所绑定的流程里哪些节点引用了它」：需要读流程图。
					"/workflow.v1.WorkflowDefinitionService/GetDefinition",
					"/workflow.v1.WorkflowTaskService/ListUserCandidates",
				},
			},
		},
	},
	{
		Code: "ticket.all",
		Name: "全部工单",
		// 管理视图：持有 ListTickets 即可查看任意工单（详情、评论、附件）。
		DefaultOps: []string{
			"/ticket.v1.TicketService/ListTickets",
			"/ticket.v1.TicketService/GetTicket",
			"/ticket.v1.TicketService/ListTicketComments",
			"/ticket.v1.TicketService/AddTicketComment",
			"/ticket.v1.TicketCatalogService/ListManagedCatalog",
			"/workflow.v1.WorkflowInstanceService/GetInstance",
		},
	},
	{
		Code: "ticket.stats",
		Name: "工单统计",
		DefaultOps: []string{
			"/ticket.v1.TicketService/GetTicketStats",
		},
	},
	{
		Code: "system.login_log",
		Name: "登录日志",
		DefaultOps: []string{
			"/system.v1.LoginLogService/ListLoginLogs",
		},
		// 纯审计查询页：append-only 无写接口，页内无功能元素
	},
	{
		Code: "system.config",
		Name: "系统配置",
		// 纯可见性菜单：GetSystemConfig/GetSystemLogo 匿名免检、不在
		// 可授权枚举内，无页面默认接口
		Elements: []ElementSpec{
			{
				Code: "system.config.edit",
				Name: "编辑系统配置",
				// 更新配置与上传 logo 同属编辑动作，一并授权
				Ops: []string{
					"/system.v1.SystemConfigService/UpdateSystemConfig",
					"/system.v1.SystemConfigService/UploadSystemLogo",
				},
			},
		},
	},
	{
		Code: "notify.channel",
		Name: "通知渠道",
		DefaultOps: []string{
			"/notify.v1.NotifyChannelService/ListChannels",
			"/notify.v1.NotifyChannelService/ListChannelHealth",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.channel.create",
				Name: "新建通知渠道",
				Ops:  []string{"/notify.v1.NotifyChannelService/CreateChannel"},
			},
			{
				Code: "notify.channel.edit",
				Name: "编辑通知渠道",
				Ops: []string{
					"/notify.v1.NotifyChannelService/GetChannel",
					"/notify.v1.NotifyChannelService/UpdateChannel",
				},
			},
			{
				Code: "notify.channel.delete",
				Name: "删除通知渠道",
				Ops:  []string{"/notify.v1.NotifyChannelService/DeleteChannel"},
			},
			{
				Code: "notify.channel.test",
				Name: "发送测试事件",
				Ops:  []string{"/notify.v1.NotifyChannelService/SendTestEvent"},
			},
		},
	},
	{
		Code: "notify.template",
		Name: "通知模板",
		DefaultOps: []string{
			"/notify.v1.NotifyTemplateService/ListTemplates",
			"/notify.v1.NotifyTemplateService/RenderTemplate",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.template.create",
				Name: "新建通知模板",
				Ops:  []string{"/notify.v1.NotifyTemplateService/CreateTemplate"},
			},
			{
				Code: "notify.template.edit",
				Name: "编辑通知模板",
				Ops: []string{
					"/notify.v1.NotifyTemplateService/GetTemplate",
					"/notify.v1.NotifyTemplateService/UpdateTemplate",
				},
			},
			{
				Code: "notify.template.delete",
				Name: "删除通知模板",
				Ops:  []string{"/notify.v1.NotifyTemplateService/DeleteTemplate"},
			},
		},
	},
	{
		Code: "notify.route",
		Name: "通知路由",
		DefaultOps: []string{
			"/notify.v1.NotifyRouteService/ListRoutes",
			"/notify.v1.NotifyRouteService/PreviewRouting",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.route.create",
				Name: "新建通知路由",
				Ops:  []string{"/notify.v1.NotifyRouteService/CreateRoute"},
			},
			{
				Code: "notify.route.edit",
				Name: "编辑通知路由",
				Ops: []string{
					"/notify.v1.NotifyRouteService/GetRoute",
					"/notify.v1.NotifyRouteService/UpdateRoute",
				},
			},
			{
				Code: "notify.route.delete",
				Name: "删除通知路由",
				Ops:  []string{"/notify.v1.NotifyRouteService/DeleteRoute"},
			},
		},
	},
	{
		Code: "notify.silence",
		Name: "通知静默",
		DefaultOps: []string{
			"/notify.v1.NotifySilenceService/ListSilences",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.silence.create",
				Name: "新建通知静默",
				Ops:  []string{"/notify.v1.NotifySilenceService/CreateSilence"},
			},
			{
				Code: "notify.silence.edit",
				Name: "编辑通知静默",
				Ops: []string{
					"/notify.v1.NotifySilenceService/GetSilence",
					"/notify.v1.NotifySilenceService/UpdateSilence",
				},
			},
			{
				Code: "notify.silence.delete",
				Name: "删除通知静默",
				Ops:  []string{"/notify.v1.NotifySilenceService/DeleteSilence"},
			},
		},
	},
	{
		Code: "notify.subscription",
		Name: "订阅中心",
		// 自助页：目标用户缺省=本人（指定他人须 is_admin，service 层强制）；
		// 菜单可见性仍按角色授权，授予后用户可管理自己的订阅。
		DefaultOps: []string{
			"/notify.v1.NotifySubscriptionService/ListSubscriptions",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.subscription.upsert",
				Name: "保存订阅",
				Ops:  []string{"/notify.v1.NotifySubscriptionService/UpsertSubscription"},
			},
			{
				Code: "notify.subscription.delete",
				Name: "删除订阅",
				Ops:  []string{"/notify.v1.NotifySubscriptionService/DeleteSubscription"},
			},
		},
	},
	{
		Code: "notify.producer",
		Name: "生产者管理",
		// 接入凭据页：token 明文仅创建/轮换响应出现一次，读接口不回显；
		// 轮换是敏感操作单独成元素，可独立授予。
		DefaultOps: []string{
			"/notify.v1.NotifyProducerService/ListProducers",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.producer.create",
				Name: "新建生产者",
				Ops:  []string{"/notify.v1.NotifyProducerService/CreateProducer"},
			},
			{
				Code: "notify.producer.edit",
				Name: "编辑生产者",
				Ops: []string{
					"/notify.v1.NotifyProducerService/GetProducer",
					"/notify.v1.NotifyProducerService/UpdateProducer",
				},
			},
			{
				Code: "notify.producer.rotate",
				Name: "轮换令牌",
				Ops:  []string{"/notify.v1.NotifyProducerService/RotateProducerToken"},
			},
			{
				Code: "notify.producer.delete",
				Name: "删除生产者",
				Ops:  []string{"/notify.v1.NotifyProducerService/DeleteProducer"},
			},
		},
	},
	{
		Code: "notify.event",
		Name: "事件查询",
		// 排障查询页：事件是 append-only 运行数据（管道写 / core 读）；下钻为只读，
		// 事件级重发会真实再投递——单独元素授权（同 notify.dlq.replay 先例）。
		DefaultOps: []string{
			"/notify.v1.NotifyEventService/ListEvents",
			"/notify.v1.NotifyEventService/GetEvent",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.event.replay",
				Name: "重发事件",
				Ops:  []string{"/notify.v1.NotifyEventService/ReplayEvent"},
			},
		},
	},
	{
		Code: "notify.dispatch",
		Name: "发送任务查询",
		// 纯排障查询页：发送任务是 append-only 运行数据（管道 router 生成 /
		// core 读），状态机由管道 sender 推进，无管理写接口，页内无功能
		// 元素（同 system.login_log / notify.event 先例）。
		DefaultOps: []string{
			"/notify.v1.NotifyDispatchService/ListDispatches",
			"/notify.v1.NotifyDispatchService/GetDispatch",
		},
	},
	{
		Code: "notify.delivery_log",
		Name: "投递明细查询",
		// 纯排障查询页：投递明细是 append-only 运行数据（管道 sender 每次
		// 尝试后追加 / core 读），无管理写接口，页内无功能元素（同
		// system.login_log / notify.event 先例）。
		DefaultOps: []string{
			"/notify.v1.NotifyDeliveryLogService/ListDeliveryLogs",
		},
	},
	{
		Code: "notify.dlq",
		Name: "死信重放",
		// 查询 + 标记重放：死信是运行数据（sender 在 dispatch 重试超限后
		// 写入），重放是触发实际重发的敏感操作——单独元素授权（同
		// notify.producer.rotate 先例），仅 none 状态可标记（§4.7 状态机）。
		DefaultOps: []string{
			"/notify.v1.NotifyDeadLetterService/ListDeadLetters",
			"/notify.v1.NotifyDeadLetterService/GetDeadLetter",
		},
		Elements: []ElementSpec{
			{
				Code: "notify.dlq.replay",
				Name: "标记重放",
				Ops:  []string{"/notify.v1.NotifyDeadLetterService/ReplayDeadLetter"},
			},
		},
	},
	{
		Code: "assistant",
		Name: "智能助手",
		DefaultOps: []string{
			"/assistant.v1.AssistantChatService/Chat",
			"/assistant.v1.AssistantChatService/Confirm",
			"/assistant.v1.AssistantService/ListConversations",
			"/assistant.v1.AssistantService/ListMessages",
			"/assistant.v1.AssistantService/DeleteConversation",
		},
	},
	{
		Code: "knowledge",
		Name: "知识库",
		// 首页 / 搜索 / 空间与文档浏览 / 版本 / 相关知识 / 检索反馈：进知识库即需要。
		DefaultOps: []string{
			"/knowledge.v1.KnowledgeSpaceService/ListSpaces",
			"/knowledge.v1.KnowledgeSpaceService/GetSpace",
			"/knowledge.v1.KnowledgeService/Search",
			"/knowledge.v1.KnowledgeService/ListDocuments",
			"/knowledge.v1.KnowledgeService/GetDocument",
			"/knowledge.v1.KnowledgeService/ListVersions",
			"/knowledge.v1.KnowledgeService/GetVersion",
			"/knowledge.v1.KnowledgeService/ListRelated",
			"/knowledge.v1.KnowledgeService/GetOverview",
			"/knowledge.v1.KnowledgeService/SubmitSearchFeedback",
		},
		Elements: []ElementSpec{
			{
				Code: "knowledge.write",
				Name: "编写文档",
				Ops: []string{
					"/knowledge.v1.KnowledgeService/CreateDocument",
					"/knowledge.v1.KnowledgeService/UpdateDocument",
					"/knowledge.v1.KnowledgeService/PublishDocument",
					"/knowledge.v1.KnowledgeService/ConfirmDocumentReview",
					"/knowledge.v1.KnowledgeService/AddLink",
					"/knowledge.v1.KnowledgeService/RemoveLink",
					"/knowledge.v1.KnowledgeService/RollbackDocument",
					"/knowledge.v1.KnowledgeIngestService/CreateIngestJob",
					"/knowledge.v1.KnowledgeIngestService/ListIngestJobs",
					"/knowledge.v1.KnowledgeIngestService/RetryIngestJob",
				},
			},
			{
				Code: "knowledge.review",
				Name: "审核文档",
				Ops:  []string{"/knowledge.v1.KnowledgeService/ReviewDocument"},
			},
			{
				Code: "knowledge.archive",
				Name: "归档与恢复文档",
				Ops: []string{
					"/knowledge.v1.KnowledgeService/ArchiveDocument",
					"/knowledge.v1.KnowledgeService/RestoreDocument",
				},
			},
			{
				Code: "knowledge.delete",
				Name: "删除文档",
				Ops:  []string{"/knowledge.v1.KnowledgeService/DeleteDocument"},
			},
			{
				Code: "knowledge.space",
				Name: "管理知识空间",
				Ops: []string{
					"/knowledge.v1.KnowledgeSpaceService/CreateSpace",
					"/knowledge.v1.KnowledgeSpaceService/UpdateSpace",
					"/knowledge.v1.KnowledgeSpaceService/DeleteSpace",
					"/knowledge.v1.KnowledgeSpaceService/GetSpaceAcl",
					"/knowledge.v1.KnowledgeSpaceService/SetSpaceAcl",
				},
			},
		},
	},
	{
		Code: "knowledge.admin",
		Name: "知识库管理",
		DefaultOps: []string{
			"/knowledge.v1.KnowledgeAdminService/GetIndexStatus",
			"/knowledge.v1.KnowledgeAdminService/ListSearchLogs",
			"/knowledge.v1.KnowledgeAdminService/GetStats",
		},
		Elements: []ElementSpec{
			{
				Code: "knowledge.admin.rebuild",
				Name: "重建知识索引",
				Ops:  []string{"/knowledge.v1.KnowledgeAdminService/RebuildIndex"},
			},
		},
	},
	{
		Code: "cmdb.model",
		Name: "资源模型",
		DefaultOps: []string{
			"/cmdb.v1.CmdbModelService/ListModels",
			"/cmdb.v1.CmdbModelService/GetModel",
			"/cmdb.v1.CmdbModelService/ListRelationTypes",
			"/cmdb.v1.CmdbModelService/GetModelView",
			"/cmdb.v1.CmdbInsightService/ListModelGroups",
		},
		Elements: []ElementSpec{
			{Code: "cmdb.model.create", Name: "新建资源类型", Ops: []string{"/cmdb.v1.CmdbModelService/CreateModel", "/cmdb.v1.CmdbInsightService/CreateModelGroup"}},
			{Code: "cmdb.model.view", Name: "配置详情布局", Ops: []string{"/cmdb.v1.CmdbModelService/UpdateModelView"}},
			{
				Code: "cmdb.model.scope",
				Name: "配置数据范围",
				Ops: []string{
					"/cmdb.v1.CmdbScopeService/ListRoleScopes",
					"/cmdb.v1.CmdbScopeService/SetRoleScope",
				},
			},
			{Code: "cmdb.model.edit", Name: "编辑资源类型", Ops: []string{"/cmdb.v1.CmdbModelService/UpdateModel", "/cmdb.v1.CmdbInsightService/UpdateModelGroup"}},
			{Code: "cmdb.model.delete", Name: "删除资源类型", Ops: []string{"/cmdb.v1.CmdbModelService/DeleteModel", "/cmdb.v1.CmdbInsightService/DeleteModelGroup"}},
			{
				Code: "cmdb.model.field",
				Name: "维护类型字段",
				Ops: []string{
					"/cmdb.v1.CmdbModelService/CreateField",
					"/cmdb.v1.CmdbModelService/UpdateField",
					"/cmdb.v1.CmdbModelService/DeleteField",
					"/cmdb.v1.CmdbModelService/CreateUniqueRule",
					"/cmdb.v1.CmdbModelService/UpdateUniqueRule",
					"/cmdb.v1.CmdbModelService/DeleteUniqueRule",
					"/cmdb.v1.CmdbModelService/CheckUniqueRule",
				},
			},
			{
				Code: "cmdb.model.relation_type",
				Name: "维护关系类型",
				Ops: []string{
					"/cmdb.v1.CmdbModelService/CreateRelationType",
					"/cmdb.v1.CmdbModelService/UpdateRelationType",
					"/cmdb.v1.CmdbModelService/DeleteRelationType",
				},
			},
		},
	},
	{
		Code: "cmdb.ci",
		Name: "资源管理",
		DefaultOps: []string{
			"/cmdb.v1.CmdbCIService/ListCIs",
			"/cmdb.v1.CmdbCIService/GetCI",
			"/cmdb.v1.CmdbCIService/ListCIHistory",
			"/cmdb.v1.CmdbCIService/GetCIAt",
			"/cmdb.v1.CmdbCIService/SearchCIs",
			"/k8s.v1.K8sInsightService/LocateHost",
			"/cmdb.v1.CmdbCIService/ListOwnerCandidates",
			"/cmdb.v1.CmdbRelationService/ListRelated",
			"/cmdb.v1.CmdbRelationService/Neighbors",
			"/cmdb.v1.CmdbModelService/ListModels",
			"/cmdb.v1.CmdbModelService/GetModel",
			"/cmdb.v1.CmdbModelService/ListRelationTypes",
			"/cmdb.v1.CmdbModelService/GetModelView",
			"/cmdb.v1.CmdbScopeService/GetMyScope",
			"/cmdb.v1.CmdbInsightService/ListModelGroups",
			"/cmdb.v1.CmdbInsightService/ListFavorites",
			"/cmdb.v1.CmdbInsightService/SetFavorite",
			"/cmdb.v1.CmdbInsightService/ListCIViews",
			"/cmdb.v1.CmdbInsightService/SaveCIView",
			"/cmdb.v1.CmdbInsightService/DeleteCIView",
			"/cmdb.v1.CmdbInsightService/GetOverview",
			"/cmdb.v1.CmdbInsightService/GetQuality",
		},
		Elements: []ElementSpec{
			{Code: "cmdb.ci.create", Name: "新建资源", Ops: []string{"/cmdb.v1.CmdbCIService/CreateCI"}},
			{Code: "cmdb.ci.edit", Name: "编辑资源", Ops: []string{"/cmdb.v1.CmdbCIService/UpdateCI", "/cmdb.v1.CmdbCIService/BatchUpdateCIs", "/cmdb.v1.CmdbCIService/UnclaimCI"}},
			{Code: "cmdb.ci.transition", Name: "变更资源状态", Ops: []string{"/cmdb.v1.CmdbCIService/TransitionCI", "/cmdb.v1.CmdbCIService/BatchTransitionCIs"}},
			{Code: "cmdb.ci.delete", Name: "删除资源", Ops: []string{"/cmdb.v1.CmdbCIService/DeleteCI", "/cmdb.v1.CmdbCIService/BatchDeleteCIs", "/cmdb.v1.CmdbCIService/RestoreCI"}},
			{Code: "cmdb.ci.import", Name: "导入资源", Ops: []string{"/cmdb.v1.CmdbCIService/ImportCIs"}},
			{Code: "cmdb.ci.export", Name: "导出资源", Ops: []string{"/cmdb.v1.CmdbCIService/ExportCIs"}},
			{
				Code: "cmdb.ci.link",
				Name: "维护资源关联",
				Ops: []string{
					"/cmdb.v1.CmdbRelationService/LinkCIs",
					"/cmdb.v1.CmdbRelationService/UnlinkCI",
					"/cmdb.v1.CmdbRelationService/MoveCI",
				},
			},
		},
	},
	{
		Code: "cmdb.sync",
		Name: "资源同步",
		DefaultOps: []string{
			"/cmdb.v1.CmdbSyncService/ListSources",
			"/cmdb.v1.CmdbSyncService/GetSource",
			"/cmdb.v1.CmdbSyncService/ListRuns",
			"/cmdb.v1.CmdbSyncService/ListConflicts",
			"/cmdb.v1.CmdbModelService/ListModels",
		},
		Elements: []ElementSpec{
			{Code: "cmdb.sync.create", Name: "新建采集源", Ops: []string{"/cmdb.v1.CmdbSyncService/CreateSource"}},
			{Code: "cmdb.sync.edit", Name: "编辑采集源", Ops: []string{"/cmdb.v1.CmdbSyncService/UpdateSource", "/cmdb.v1.CmdbSyncService/ClaimConflict"}},
			{Code: "cmdb.sync.delete", Name: "删除采集源", Ops: []string{"/cmdb.v1.CmdbSyncService/DeleteSource"}},
			{Code: "cmdb.sync.trigger", Name: "触发同步", Ops: []string{"/cmdb.v1.CmdbSyncService/TriggerSync"}},
			{
				Code: "cmdb.sync.webhook",
				Name: "管理回调令牌",
				Ops: []string{
					"/cmdb.v1.CmdbSyncService/RotateWebhook",
					"/cmdb.v1.CmdbSyncService/DisableWebhook",
				},
			},
		},
	},
	{
		Code: "cmdb.audit",
		Name: "变更审计",
		DefaultOps: []string{
			"/cmdb.v1.CmdbCIService/ListHistory",
			"/cmdb.v1.CmdbCIService/GetCI",
			"/cmdb.v1.CmdbModelService/ListModels",
			"/cmdb.v1.CmdbInsightService/ListSchemaAudit",
			"/cmdb.v1.CmdbInsightService/GetOverview",
			"/cmdb.v1.CmdbInsightService/GetQuality",
			"/cmdb.v1.CmdbInsightService/ListModelGroups",
		},
	},
	{
		Code: "oncall.calendar",
		Name: "值班日历",
		DefaultOps: []string{
			"/oncall.v1.OncallScheduleService/ListShifts",
			"/oncall.v1.OncallScheduleService/GetCurrentDuty",
			"/oncall.v1.OncallScheduleService/ListSchedules",
			"/oncall.v1.OncallGroupService/ListGroups",
			"/oncall.v1.OncallGroupService/ListGroupMembers",
			"/oncall.v1.OncallGroupService/ListUserCandidates",
		},
		Elements: []ElementSpec{
			{
				Code: "oncall.calendar.override",
				Name: "替班",
				Ops: []string{
					"/oncall.v1.OncallScheduleService/CreateOverride",
					"/oncall.v1.OncallScheduleService/RevokeOverride",
				},
			},
			{
				Code: "oncall.calendar.share",
				Name: "我的公共地址",
				Ops: []string{
					"/oncall.v1.OncallShareService/ListShareLinks",
					"/oncall.v1.OncallShareService/CreateShareLink",
					"/oncall.v1.OncallShareService/GetShareLinkToken",
					"/oncall.v1.OncallShareService/ResetShareLink",
					"/oncall.v1.OncallShareService/DeleteShareLink",
				},
			},
		},
	},
	{
		Code: "oncall.group",
		Name: "值班分组",
		DefaultOps: []string{
			"/oncall.v1.OncallGroupService/ListGroups",
			"/oncall.v1.OncallGroupService/GetGroup",
			"/oncall.v1.OncallGroupService/ListGroupMembers",
			"/oncall.v1.OncallGroupService/ListChangeLogs",
			"/oncall.v1.OncallScheduleService/ListSchedules",
			"/oncall.v1.OncallScheduleService/GetSchedule",
			"/oncall.v1.OncallScheduleService/GetCurrentDuty",
			"/oncall.v1.OncallScheduleService/PreviewSchedule",
			"/oncall.v1.OncallScheduleService/GetNotificationStatus",
			"/oncall.v1.OncallShareService/ListShareLinks",
		},
		Elements: []ElementSpec{
			{Code: "oncall.group.create", Name: "新建分组", Ops: []string{"/oncall.v1.OncallGroupService/CreateGroup"}},
			{Code: "oncall.group.edit", Name: "编辑分组", Ops: []string{"/oncall.v1.OncallGroupService/UpdateGroup"}},
			{Code: "oncall.group.delete", Name: "删除分组", Ops: []string{"/oncall.v1.OncallGroupService/DeleteGroup"}},
			{
				Code: "oncall.group.member",
				Name: "管理成员",
				Ops: []string{
					"/oncall.v1.OncallGroupService/ListUserCandidates",
					"/oncall.v1.OncallGroupService/AddGroupMembers",
					"/oncall.v1.OncallGroupService/UpdateGroupMember",
					"/oncall.v1.OncallGroupService/RemoveGroupMember",
				},
			},
			{
				Code: "oncall.group.schedule",
				Name: "管理值班表",
				Ops: []string{
					"/oncall.v1.OncallScheduleService/CreateSchedule",
					"/oncall.v1.OncallScheduleService/UpdateSchedule",
					"/oncall.v1.OncallScheduleService/UpdateScheduleStatus",
					"/oncall.v1.OncallScheduleService/DeleteSchedule",
					"/oncall.v1.OncallScheduleService/SendScheduleNotification",
					"/oncall.v1.OncallScheduleService/ListScheduleNotificationResults",
				},
			},
			{
				Code: "oncall.group.share",
				Name: "管理公共地址",
				Ops: []string{
					"/oncall.v1.OncallShareService/CreateShareLink",
					"/oncall.v1.OncallShareService/GetShareLinkToken",
					"/oncall.v1.OncallShareService/ResetShareLink",
					"/oncall.v1.OncallShareService/DeleteShareLink",
				},
			},
		},
	},
	{
		Code: "monitor.alert",
		Name: "告警事件",
		// 分组树与数据源选项：列表的分组 / 数据源筛选。告警详情（GetAlert）免接口授权、不在此登记：
		// 通知收件人要能打开，访问控制在监控域内（收件人，或持有 ListAlerts 权限且在数据范围内）。
		// 值班班次告警数（ListShiftAlertCounts）同样免接口授权：值班日历页也要调，持有本菜单或值班日历的
		// 接口权限即可，判定在监控域内。
		DefaultOps: []string{
			"/monitor.v1.MonitorAlertService/ListAlerts",
			"/monitor.v1.MonitorAlertService/AggregateAlerts",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
			"/monitor.v1.MonitorAlertViewService/ListAlertViews",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.alert.ack",
				Name: "认领 / 转交",
				Ops: []string{
					"/monitor.v1.MonitorAlertService/AckAlert",
					"/monitor.v1.MonitorAlertService/UnackAlert",
					"/monitor.v1.MonitorAlertService/TransferAlert",
					"/monitor.v1.MonitorAlertService/BatchAckAlerts",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
				},
			},
			{
				Code: "monitor.alert.comment",
				Name: "备注",
				Ops: []string{
					"/monitor.v1.MonitorAlertService/CommentAlert",
				},
			},
			{
				Code: "monitor.alert.close",
				Name: "手动关闭",
				Ops: []string{
					"/monitor.v1.MonitorAlertService/CloseAlert",
					"/monitor.v1.MonitorAlertService/BatchCloseAlerts",
				},
			},
			{
				Code: "monitor.alert.ticket",
				Name: "转工单",
				Ops: []string{
					"/monitor.v1.MonitorAlertService/CreateAlertTicket",
					"/monitor.v1.MonitorRuleService/ListMonitorRuleOptions",
				},
			},
			{
				Code: "monitor.alert.view",
				Name: "保存视图",
				Ops: []string{
					"/monitor.v1.MonitorAlertViewService/CreateAlertView",
					"/monitor.v1.MonitorAlertViewService/UpdateAlertView",
					"/monitor.v1.MonitorAlertViewService/DeleteAlertView",
				},
			},
		},
	},
	{
		Code: "monitor.rule",
		Name: "告警规则",
		DefaultOps: []string{
			"/monitor.v1.MonitorRuleService/ListRules",
			"/monitor.v1.MonitorRuleService/GetRule",
			"/monitor.v1.MonitorRuleService/ListRuleVersions",
			"/monitor.v1.MonitorRuleService/GetRuleVersion",
			"/monitor.v1.MonitorRuleService/ListRuleEvalRecords",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.rule.create",
				Name: "新建规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/CreateRule",
					"/monitor.v1.MonitorRuleService/CloneRule",
					"/monitor.v1.MonitorRuleService/ValidateRuleExprs",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
					"/monitor.v1.MonitorRuleService/ListMonitorRuleOptions",
					"/monitor.v1.MonitorEscalationService/ListEscalationOptions",
					"/monitor.v1.MonitorDatasourceService/QueryDatasource",
					"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange",
				},
			},
			{
				Code: "monitor.rule.edit",
				Name: "编辑规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/UpdateRule",
					"/monitor.v1.MonitorRuleService/BatchRules",
					"/monitor.v1.MonitorRuleService/RollbackRule",
					"/monitor.v1.MonitorRuleService/ValidateRuleExprs",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
					"/monitor.v1.MonitorRuleService/ListMonitorRuleOptions",
					"/monitor.v1.MonitorEscalationService/ListEscalationOptions",
					"/monitor.v1.MonitorDatasourceService/QueryDatasource",
					"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange",
				},
			},
			{
				Code: "monitor.rule.delete",
				Name: "删除规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/DeleteRule",
				},
			},
			{
				Code: "monitor.rule.toggle",
				Name: "启停规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/ToggleRule",
				},
			},
			{
				Code: "monitor.rule.import",
				Name: "导入规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/PreviewImportRules",
					"/monitor.v1.MonitorRuleService/ImportRules",
					"/k8s.v1.K8sGovernanceService/GetAlertRuleTemplate",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
					"/monitor.v1.MonitorRuleService/ListRuleTemplatePacks",
					"/monitor.v1.MonitorRuleService/GetRuleTemplatePack",
				},
			},
			{
				Code: "monitor.rule.export",
				Name: "导出规则",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/ExportRules",
				},
			},
			{
				Code: "monitor.rule.group",
				Name: "分组维护",
				Ops: []string{
					"/monitor.v1.MonitorRuleGroupService/CreateRuleGroup",
					"/monitor.v1.MonitorRuleGroupService/UpdateRuleGroup",
					"/monitor.v1.MonitorRuleGroupService/DeleteRuleGroup",
				},
			},
			{
				Code: "monitor.rule.scope",
				Name: "分组授权",
				Ops: []string{
					"/monitor.v1.MonitorRuleGroupService/ListGroupRoleScopes",
					"/monitor.v1.MonitorRuleGroupService/SetGroupRoleScopes",
				},
			},
			{
				Code: "monitor.rule.simulate",
				Name: "模拟触发",
				Ops: []string{
					"/monitor.v1.MonitorRuleService/SimulateRule",
					"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
				},
			},
		},
	},
	{
		// 屏蔽规则与抑制规则两个页签。
		Code: "monitor.mute",
		Name: "降噪规则",
		DefaultOps: []string{
			"/monitor.v1.MonitorMuteService/ListMutes",
			"/monitor.v1.MonitorMuteService/GetMute",
			"/monitor.v1.MonitorInhibitService/ListInhibits",
			"/monitor.v1.MonitorInhibitService/GetInhibit",
			"/monitor.v1.MonitorRuleService/ListRules",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.mute.create",
				Name: "新建屏蔽规则",
				Ops: []string{
					"/monitor.v1.MonitorMuteService/CreateMute",
					"/monitor.v1.MonitorMuteService/PreviewMute",
				},
			},
			{
				Code: "monitor.mute.edit",
				Name: "编辑屏蔽规则",
				Ops: []string{
					"/monitor.v1.MonitorMuteService/UpdateMute",
					"/monitor.v1.MonitorMuteService/PreviewMute",
				},
			},
			{
				Code: "monitor.mute.delete",
				Name: "删除屏蔽规则",
				Ops: []string{
					"/monitor.v1.MonitorMuteService/DeleteMute",
				},
			},
			{
				Code: "monitor.mute.inhibit_create",
				Name: "新建抑制规则",
				Ops: []string{
					"/monitor.v1.MonitorInhibitService/CreateInhibit",
					"/monitor.v1.MonitorInhibitService/PreviewInhibit",
				},
			},
			{
				Code: "monitor.mute.inhibit_edit",
				Name: "编辑抑制规则",
				Ops: []string{
					"/monitor.v1.MonitorInhibitService/UpdateInhibit",
					"/monitor.v1.MonitorInhibitService/PreviewInhibit",
				},
			},
			{
				Code: "monitor.mute.inhibit_delete",
				Name: "删除抑制规则",
				Ops: []string{
					"/monitor.v1.MonitorInhibitService/DeleteInhibit",
				},
			},
		},
	},
	{
		Code: "monitor.subscription",
		Name: "告警订阅",
		DefaultOps: []string{
			"/monitor.v1.MonitorSubscriptionService/ListAlertSubscriptions",
			"/monitor.v1.MonitorSubscriptionService/GetAlertSubscription",
			"/monitor.v1.MonitorRuleService/ListRules",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.subscription.create",
				Name: "新建订阅",
				Ops: []string{
					"/monitor.v1.MonitorSubscriptionService/CreateAlertSubscription",
					"/monitor.v1.MonitorSubscriptionService/PreviewAlertSubscription",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
				},
			},
			{
				Code: "monitor.subscription.edit",
				Name: "编辑订阅",
				Ops: []string{
					"/monitor.v1.MonitorSubscriptionService/UpdateAlertSubscription",
					"/monitor.v1.MonitorSubscriptionService/PreviewAlertSubscription",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
				},
			},
			{
				Code: "monitor.subscription.delete",
				Name: "删除订阅",
				Ops: []string{
					"/monitor.v1.MonitorSubscriptionService/DeleteAlertSubscription",
				},
			},
		},
	},
	{
		Code: "monitor.escalation",
		Name: "升级策略",
		DefaultOps: []string{
			"/monitor.v1.MonitorEscalationService/ListEscalations",
			"/monitor.v1.MonitorEscalationService/GetEscalation",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.escalation.create",
				Name: "新建升级策略",
				Ops: []string{
					"/monitor.v1.MonitorEscalationService/CreateEscalation",
					"/monitor.v1.MonitorEscalationService/EstimateEscalation",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
					"/monitor.v1.MonitorEscalationService/ListOncallGroupOptions",
				},
			},
			{
				Code: "monitor.escalation.edit",
				Name: "编辑升级策略",
				Ops: []string{
					"/monitor.v1.MonitorEscalationService/UpdateEscalation",
					"/monitor.v1.MonitorEscalationService/EstimateEscalation",
					"/monitor.v1.MonitorRuleService/ListMonitorTargetOptions",
					"/monitor.v1.MonitorEscalationService/ListOncallGroupOptions",
				},
			},
			{
				Code: "monitor.escalation.delete",
				Name: "删除升级策略",
				Ops: []string{
					"/monitor.v1.MonitorEscalationService/DeleteEscalation",
				},
			},
		},
	},
	{
		Code: "monitor.record",
		Name: "记录规则",
		DefaultOps: []string{
			"/monitor.v1.MonitorRecordRuleService/ListRecordRules",
			"/monitor.v1.MonitorRecordRuleService/GetRecordRule",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.record.create",
				Name: "新建记录规则",
				Ops: []string{
					"/monitor.v1.MonitorRecordRuleService/CreateRecordRule",
					"/monitor.v1.MonitorRecordRuleService/ValidateRecordRule",
					"/monitor.v1.MonitorDatasourceService/QueryDatasource",
					"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange",
				},
			},
			{
				Code: "monitor.record.edit",
				Name: "编辑记录规则",
				Ops: []string{
					"/monitor.v1.MonitorRecordRuleService/UpdateRecordRule",
					"/monitor.v1.MonitorRecordRuleService/ToggleRecordRule",
					"/monitor.v1.MonitorRecordRuleService/ValidateRecordRule",
					"/monitor.v1.MonitorDatasourceService/QueryDatasource",
					"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange",
				},
			},
			{
				Code: "monitor.record.delete",
				Name: "删除记录规则",
				Ops: []string{
					"/monitor.v1.MonitorRecordRuleService/DeleteRecordRule",
				},
			},
		},
	},
	{
		Code: "monitor.datasource",
		Name: "数据源",
		DefaultOps: []string{
			"/monitor.v1.MonitorDatasourceService/ListDatasources",
			"/monitor.v1.MonitorDatasourceService/GetDatasource",
		},
		Elements: []ElementSpec{
			{
				Code: "monitor.datasource.create",
				Name: "新建数据源",
				Ops: []string{
					"/monitor.v1.MonitorDatasourceService/CreateDatasource",
					"/monitor.v1.MonitorDatasourceService/TestDatasourceConfig",
				},
			},
			{
				Code: "monitor.datasource.edit",
				Name: "编辑数据源",
				Ops: []string{
					"/monitor.v1.MonitorDatasourceService/UpdateDatasource",
					"/monitor.v1.MonitorDatasourceService/TestDatasourceConfig",
				},
			},
			{
				Code: "monitor.datasource.delete",
				Name: "删除数据源",
				Ops: []string{
					"/monitor.v1.MonitorDatasourceService/DeleteDatasource",
				},
			},
			{
				Code: "monitor.datasource.test",
				Name: "测试连接",
				Ops: []string{
					"/monitor.v1.MonitorDatasourceService/TestDatasource",
				},
			},
		},
	},
	{
		Code: "monitor.explore",
		Name: "即时查询",
		DefaultOps: []string{
			"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
			"/monitor.v1.MonitorDatasourceService/QueryDatasource",
			"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange",
		},
	},
	{
		Code: "monitor.stats",
		Name: "告警统计",
		DefaultOps: []string{
			"/monitor.v1.MonitorStatsService/GetAlertOverview",
			"/monitor.v1.MonitorStatsService/GetAlertTrend",
			"/monitor.v1.MonitorStatsService/GetAlertTop",
			"/monitor.v1.MonitorStatsService/GetAlertEfficiency",
			"/monitor.v1.MonitorStatsService/GetAlertNoise",
			"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
		},
	},
	{
		Code: "monitor.engine",
		Name: "引擎状态",
		DefaultOps: []string{
			"/monitor.v1.MonitorEngineService/GetEngineStatus",
		},
	},
	{
		Code: "job.adhoc",
		Name: "快速执行",
		// 表单要选凭据、脚本库脚本、执行节点区域，并从 CMDB 选资源类型 / 地址字段 / 资源；提交前检查不落库。
		DefaultOps: []string{
			"/job.v1.JobExecutionService/CheckJobAdhoc",
			"/job.v1.JobTemplateService/PreviewJobTarget",
			"/job.v1.JobCredentialService/ListJobCredentialOptions",
			"/job.v1.JobRunnerService/ListJobRunnerZones",
			"/job.v1.JobScriptService/ListJobScripts",
			"/job.v1.JobScriptService/GetJobScript",
			"/cmdb.v1.CmdbModelService/ListModels",
			"/cmdb.v1.CmdbModelService/GetModel",
			"/cmdb.v1.CmdbCIService/ListCIs",
		},
		Elements: []ElementSpec{
			{
				Code: "job.adhoc.run",
				Name: "执行",
				// 提交后跳到执行详情：能执行的人也要能看到自己这次执行的状态、主机结果与日志。
				Ops: []string{
					"/job.v1.JobExecutionService/SubmitJobAdhoc",
					"/job.v1.JobExecutionService/GetJobExecution",
					"/job.v1.JobExecutionService/ListJobExecutionHosts",
					"/job.v1.JobExecutionService/GetJobExecutionLog",
				},
			},
		},
	},
	{
		Code: "job.script",
		Name: "脚本库",
		DefaultOps: []string{
			"/job.v1.JobScriptService/ListJobScripts",
			"/job.v1.JobScriptService/GetJobScript",
		},
		Elements: []ElementSpec{
			{
				Code: "job.script.create",
				Name: "新建脚本",
				Ops: []string{
					"/job.v1.JobScriptService/CreateJobScript",
				},
			},
			{
				Code: "job.script.edit",
				Name: "编辑脚本",
				Ops: []string{
					"/job.v1.JobScriptService/UpdateJobScript",
				},
			},
			{
				Code: "job.script.delete",
				Name: "删除脚本",
				Ops: []string{
					"/job.v1.JobScriptService/DeleteJobScript",
				},
			},
		},
	},
	{
		Code: "job.schedule",
		Name: "定时作业",
		// 模板列表：列表的模板筛选。
		DefaultOps: []string{
			"/job.v1.JobScheduleService/ListJobSchedules",
			"/job.v1.JobTemplateService/ListJobTemplates",
		},
		Elements: []ElementSpec{
			{
				Code: "job.schedule.create",
				Name: "新建定时作业",
				Ops: []string{
					"/job.v1.JobScheduleService/CreateJobSchedule",
					"/job.v1.JobScheduleService/PreviewJobSchedule",
					"/job.v1.JobTemplateService/ListJobTemplates",
					"/job.v1.JobTemplateService/GetJobTemplate",
				},
			},
			{
				Code: "job.schedule.edit",
				Name: "编辑 / 启停定时作业",
				Ops: []string{
					"/job.v1.JobScheduleService/UpdateJobSchedule",
					"/job.v1.JobScheduleService/PreviewJobSchedule",
					"/job.v1.JobTemplateService/ListJobTemplates",
					"/job.v1.JobTemplateService/GetJobTemplate",
				},
			},
			{
				Code: "job.schedule.delete",
				Name: "删除定时作业",
				Ops: []string{
					"/job.v1.JobScheduleService/DeleteJobSchedule",
				},
			},
		},
	},
	{
		Code: "job.trigger",
		Name: "触发器",
		DefaultOps: []string{
			"/job.v1.JobTriggerService/ListJobTriggers",
			"/job.v1.JobTriggerService/ListJobTriggerFires",
			"/job.v1.JobTemplateService/ListJobTemplates",
		},
		Elements: []ElementSpec{
			{
				Code: "job.trigger.create",
				Name: "新建触发器",
				// 告警触发器的表单要选告警规则。
				Ops: []string{
					"/job.v1.JobTriggerService/CreateJobTrigger",
					"/job.v1.JobTemplateService/ListJobTemplates",
					"/job.v1.JobTemplateService/GetJobTemplate",
					"/monitor.v1.MonitorRuleService/ListRules",
					"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
				},
			},
			{
				Code: "job.trigger.edit",
				Name: "编辑 / 启停触发器",
				Ops: []string{
					"/job.v1.JobTriggerService/UpdateJobTrigger",
					"/job.v1.JobTemplateService/ListJobTemplates",
					"/job.v1.JobTemplateService/GetJobTemplate",
					"/monitor.v1.MonitorRuleService/ListRules",
					"/monitor.v1.MonitorRuleGroupService/ListRuleGroups",
				},
			},
			{
				Code: "job.trigger.delete",
				Name: "删除触发器",
				Ops: []string{
					"/job.v1.JobTriggerService/DeleteJobTrigger",
				},
			},
			{
				Code: "job.trigger.token",
				Name: "重置令牌",
				Ops: []string{
					"/job.v1.JobTriggerService/ResetJobTriggerToken",
				},
			},
		},
	},
	{
		Code: "job.policy",
		Name: "安全策略",
		DefaultOps: []string{
			"/job.v1.JobPolicyService/ListJobRiskRules",
			"/job.v1.JobPolicyService/GetJobPolicy",
			"/job.v1.JobPolicyService/TestJobRiskRules",
		},
		Elements: []ElementSpec{
			{
				Code: "job.policy.rule",
				Name: "维护高危命令规则",
				Ops: []string{
					"/job.v1.JobPolicyService/CreateJobRiskRule",
					"/job.v1.JobPolicyService/UpdateJobRiskRule",
					"/job.v1.JobPolicyService/DeleteJobRiskRule",
				},
			},
			{
				Code: "job.policy.edit",
				Name: "修改全局策略",
				// 审批流程从流程定义里选。
				Ops: []string{
					"/job.v1.JobPolicyService/UpdateJobPolicy",
					"/workflow.v1.WorkflowDefinitionService/ListDefinitions",
				},
			},
		},
	},
	{
		Code: "job.stats",
		Name: "执行统计",
		DefaultOps: []string{
			"/job.v1.JobExecutionService/GetJobStats",
		},
	},
	{
		Code: "job.template",
		Name: "作业模板",
		// 项目选项：列表的项目筛选。
		DefaultOps: []string{
			"/job.v1.JobTemplateService/ListJobTemplates",
			"/job.v1.JobTemplateService/GetJobTemplate",
			"/job.v1.JobProjectService/ListJobProjectOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "job.template.create",
				Name: "新建模板",
				Ops: []string{
					"/job.v1.JobTemplateService/CreateJobTemplate",
					"/job.v1.JobTemplateService/PreviewJobTarget",
					"/job.v1.JobProjectService/ListJobProjectOptions",
					"/job.v1.JobCredentialService/ListJobCredentialOptions",
					"/job.v1.JobRunnerService/ListJobRunnerZones",
					"/cmdb.v1.CmdbModelService/ListModels",
					"/cmdb.v1.CmdbModelService/GetModel",
					"/cmdb.v1.CmdbCIService/ListCIs",
				},
			},
			{
				Code: "job.template.edit",
				Name: "编辑模板",
				Ops: []string{
					"/job.v1.JobTemplateService/UpdateJobTemplate",
					"/job.v1.JobTemplateService/PreviewJobTarget",
					"/job.v1.JobProjectService/ListJobProjectOptions",
					"/job.v1.JobCredentialService/ListJobCredentialOptions",
					"/job.v1.JobRunnerService/ListJobRunnerZones",
					"/cmdb.v1.CmdbModelService/ListModels",
					"/cmdb.v1.CmdbModelService/GetModel",
					"/cmdb.v1.CmdbCIService/ListCIs",
				},
			},
			{
				Code: "job.template.delete",
				Name: "删除模板",
				Ops: []string{
					"/job.v1.JobTemplateService/DeleteJobTemplate",
				},
			},
			{
				Code: "job.template.run",
				Name: "执行作业",
				// 提交后跳到执行详情：能执行的人也要能看到自己这次执行的状态、主机结果与日志。
				Ops: []string{
					"/job.v1.JobExecutionService/SubmitJobExecution",
					"/job.v1.JobTemplateService/GetJobTemplate",
					"/job.v1.JobTemplateService/ListJobTemplateHosts",
					"/job.v1.JobExecutionService/GetJobExecution",
					"/job.v1.JobExecutionService/ListJobExecutionHosts",
					"/job.v1.JobExecutionService/GetJobExecutionLog",
				},
			},
		},
	},
	{
		Code: "job.execution",
		Name: "执行记录",
		// 模板列表：列表的模板筛选。
		DefaultOps: []string{
			"/job.v1.JobExecutionService/ListJobExecutions",
			"/job.v1.JobExecutionService/GetJobExecution",
			"/job.v1.JobExecutionService/ListJobExecutionHosts",
			"/job.v1.JobExecutionService/GetJobExecutionLog",
			"/job.v1.JobTemplateService/ListJobTemplates",
		},
		Elements: []ElementSpec{
			{
				Code: "job.execution.cancel",
				Name: "取消执行",
				Ops: []string{
					"/job.v1.JobExecutionService/CancelJobExecution",
				},
			},
		},
	},
	{
		Code: "job.project",
		Name: "项目",
		DefaultOps: []string{
			"/job.v1.JobProjectService/ListJobProjects",
			"/job.v1.JobProjectService/GetJobProject",
		},
		Elements: []ElementSpec{
			{
				Code: "job.project.create",
				Name: "新建项目",
				Ops: []string{
					"/job.v1.JobProjectService/CreateJobProject",
				},
			},
			{
				Code: "job.project.edit",
				Name: "编辑项目",
				Ops: []string{
					"/job.v1.JobProjectService/UpdateJobProject",
				},
			},
			{
				Code: "job.project.delete",
				Name: "删除项目",
				Ops: []string{
					"/job.v1.JobProjectService/DeleteJobProject",
				},
			},
		},
	},
	{
		Code: "job.credential",
		Name: "凭据",
		DefaultOps: []string{
			"/job.v1.JobCredentialService/ListJobCredentials",
			"/job.v1.JobCredentialService/GetJobCredential",
		},
		Elements: []ElementSpec{
			{
				Code: "job.credential.create",
				Name: "新建凭据",
				Ops: []string{
					"/job.v1.JobCredentialService/CreateJobCredential",
				},
			},
			{
				Code: "job.credential.edit",
				Name: "编辑凭据",
				Ops: []string{
					"/job.v1.JobCredentialService/UpdateJobCredential",
				},
			},
			{
				Code: "job.credential.delete",
				Name: "删除凭据",
				Ops: []string{
					"/job.v1.JobCredentialService/DeleteJobCredential",
				},
			},
		},
	},
	{
		Code: "job.runner",
		Name: "执行节点",
		DefaultOps: []string{
			"/job.v1.JobRunnerService/ListJobRunners",
			"/job.v1.JobRunnerService/ListJobRunnerZones",
		},
		Elements: []ElementSpec{
			{
				Code: "job.runner.create",
				Name: "登记节点",
				Ops: []string{
					"/job.v1.JobRunnerService/CreateJobRunner",
				},
			},
			{
				Code: "job.runner.edit",
				Name: "编辑节点",
				Ops: []string{
					"/job.v1.JobRunnerService/UpdateJobRunner",
				},
			},
			{
				Code: "job.runner.delete",
				Name: "删除节点",
				Ops: []string{
					"/job.v1.JobRunnerService/DeleteJobRunner",
				},
			},
			{
				Code: "job.runner.token",
				Name: "重置令牌",
				Ops: []string{
					"/job.v1.JobRunnerService/ResetJobRunnerToken",
				},
			},
		},
	},
	// 容器平台：下列接口由 core 完成登录与接口授权后转给数据面；能否对具体集群 / 命名空间里的资源操作成功，
	// 由集群按用户的数据范围判定（数据范围在「角色授权」里配置）。通用的预检 / 提交 / 删除接口被多个页面的元素共用。
	{
		Code: "k8s.cluster",
		Name: "集群管理",
		// 集群列表与详情，以及节点、命名空间两个页签的列表与详情。
		DefaultOps: []string{
			"/k8s.v1.K8sClusterService/ListClusters",
			"/k8s.v1.K8sClusterService/GetCluster",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
			"/k8s.v1.K8sHelmReleaseService/ListNamespaceQuotas",
			"/k8s.v1.K8sResourceService/ListNamespaceOptions",
			"/k8s.v1.K8sResourceService/ListAPIResources",
			"/k8s.v1.K8sResourceService/ListResources",
			"/k8s.v1.K8sResourceService/GetObject",
			"/k8s.v1.K8sResourceService/ListObjectEvents",
			"/k8s.v1.K8sStreamService/WatchResources",
			"/k8s.v1.K8sInsightService/GetPodSelector",
			"/k8s.v1.K8sInsightService/QueryMetrics",
			"/k8s.v1.K8sInsightService/GetNodeHost",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.cluster.create",
				Name: "接入集群",
				Ops: []string{
					"/k8s.v1.K8sClusterService/CreateCluster",
					"/k8s.v1.K8sClusterService/ParseKubeconfig",
					"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
					// 审批用的工单模板选项。
					"/ticket.v1.TicketCatalogService/ListCatalog",
					"/k8s.v1.K8sClusterService/TestCluster",
				},
			},
			{
				Code: "k8s.cluster.edit",
				Name: "编辑集群",
				Ops: []string{
					"/k8s.v1.K8sClusterService/UpdateCluster",
					"/k8s.v1.K8sClusterService/ParseKubeconfig",
					"/monitor.v1.MonitorDatasourceService/ListDatasourceOptions",
					// 审批用的工单模板选项；自助申请所用模板的设置。
					"/ticket.v1.TicketCatalogService/ListCatalog",
					"/k8s.v1.K8sChangeService/GetChangeSettings",
					"/k8s.v1.K8sChangeService/UpdateChangeSettings",
					"/k8s.v1.K8sClusterService/TestCluster",
				},
			},
			{
				Code: "k8s.cluster.delete",
				Name: "移除集群",
				Ops: []string{
					"/k8s.v1.K8sClusterService/DeleteCluster",
				},
			},
			{
				Code: "k8s.cluster.agent",
				Name: "管理 Agent",
				// 生成安装命令（含一次性的注册令牌）与吊销令牌。
				Ops: []string{
					"/k8s.v1.K8sClusterService/InstallAgent",
					"/k8s.v1.K8sClusterService/RevokeAgent",
				},
			},
			{
				Code: "k8s.cluster.node_schedule",
				Name: "节点调度开关",
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/CordonNode",
				},
			},
			{
				Code: "k8s.cluster.node_drain",
				Name: "排空节点",
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/PreviewDrainNode",
					"/k8s.v1.K8sStreamService/DrainNode",
				},
			},
			{
				Code: "k8s.cluster.node_edit",
				Name: "编辑节点",
				// 标签与污点。
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
				},
			},
			{
				Code: "k8s.cluster.namespace_create",
				Name: "新建命名空间",
				// 可同时创建 ResourceQuota 与 LimitRange。
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sResourceService/PreviewManifest",
					"/k8s.v1.K8sResourceService/ApplyManifest",
				},
			},
			{
				Code: "k8s.cluster.namespace_edit",
				Name: "编辑命名空间",
				// 含配额的增删改。
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sResourceService/DeleteObject",
				},
			},
			{
				Code: "k8s.cluster.namespace_delete",
				Name: "删除命名空间",
				Ops: []string{
					"/k8s.v1.K8sResourceService/DeleteObject",
				},
			},
		},
	},
	{
		Code: "k8s.workload",
		Name: "工作负载",
		// Deployment / StatefulSet / DaemonSet / Job / CronJob / Pod 的列表、详情与历史版本。
		DefaultOps: []string{
			"/k8s.v1.K8sClusterService/ListClusterOptions",
			"/k8s.v1.K8sResourceService/ListNamespaceOptions",
			"/k8s.v1.K8sResourceService/ListAPIResources",
			"/k8s.v1.K8sResourceService/ListResources",
			"/k8s.v1.K8sResourceService/GetObject",
			"/k8s.v1.K8sResourceService/ListObjectEvents",
			"/k8s.v1.K8sStreamService/WatchResources",
			"/k8s.v1.K8sInsightService/GetPodSelector",
			"/k8s.v1.K8sInsightService/QueryMetrics",
			"/k8s.v1.K8sInsightService/DiagnosePod",
			"/k8s.v1.K8sInsightService/DiagnoseWorkload",
			"/k8s.v1.K8sWorkloadService/ListWorkloadRevisions",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.workload.create",
				Name: "新建工作负载",
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sResourceService/PreviewManifest",
					"/k8s.v1.K8sResourceService/ApplyManifest",
				},
			},
			{
				Code: "k8s.workload.edit",
				Name: "编辑工作负载",
				// 编辑 YAML、更新镜像、暂停 / 恢复发布、CronJob 的暂停与手动触发。
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sWorkloadService/SetWorkloadImage",
					"/k8s.v1.K8sWorkloadService/PauseWorkload",
					"/k8s.v1.K8sWorkloadService/SuspendCronJob",
					"/k8s.v1.K8sWorkloadService/TriggerCronJob",
				},
			},
			{
				Code: "k8s.workload.delete",
				Name: "删除工作负载",
				// 含删除与驱逐 Pod。
				Ops: []string{
					"/k8s.v1.K8sResourceService/DeleteObject",
					"/k8s.v1.K8sWorkloadService/EvictPod",
				},
			},
			{
				Code: "k8s.workload.scale",
				Name: "伸缩",
				// 含原地调整 Pod 的容器资源（纵向伸缩）。
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/ScaleWorkload",
					"/k8s.v1.K8sWorkloadService/ResizePod",
				},
			},
			{
				Code: "k8s.workload.restart",
				Name: "重启",
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/RestartWorkload",
				},
			},
			{
				Code: "k8s.workload.rollback",
				Name: "回滚",
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/RollbackWorkload",
				},
			},
			{
				Code: "k8s.workload.log",
				Name: "查看日志",
				Ops: []string{
					"/k8s.v1.K8sStreamService/StreamPodLog",
					"/k8s.v1.K8sStreamService/StreamWorkloadLog",
				},
			},
			{
				Code: "k8s.workload.exec",
				Name: "Web 终端",
				// 签发终端票据；随后的 WebSocket 连接凭票据认证。
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/OpenExecTicket",
				},
			},
			{
				Code: "k8s.workload.debug",
				Name: "调试容器",
				// 在 Pod 里启动临时容器并进入（同一个票据接口，kind = 2）；另需持有专门的调试容器权限接口。
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/OpenExecTicket",
					"/k8s.v1.K8sGovernanceService/CheckDebugContainer",
				},
			},
			{
				Code: "k8s.workload.file",
				Name: "文件传输",
				Ops: []string{
					"/k8s.v1.K8sFileService/DownloadPodFile",
					"/k8s.v1.K8sFileService/UploadPodFile",
				},
			},
		},
	},
	{
		Code: "k8s.config",
		Name: "网络与配置",
		// Service / Ingress / ConfigMap / Secret / 存储的列表与详情（Secret 的值恒为掩码）。
		DefaultOps: []string{
			"/k8s.v1.K8sClusterService/ListClusterOptions",
			"/k8s.v1.K8sResourceService/ListNamespaceOptions",
			"/k8s.v1.K8sResourceService/ListAPIResources",
			"/k8s.v1.K8sResourceService/ListResources",
			"/k8s.v1.K8sResourceService/GetObject",
			"/k8s.v1.K8sResourceService/ListObjectEvents",
			"/k8s.v1.K8sStreamService/WatchResources",
			"/k8s.v1.K8sInsightService/GetPodSelector",
			"/k8s.v1.K8sInsightService/GetRelations",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.config.create",
				Name: "新建网络与配置",
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sResourceService/PreviewManifest",
					"/k8s.v1.K8sResourceService/ApplyManifest",
				},
			},
			{
				Code: "k8s.config.edit",
				Name: "编辑网络与配置",
				// 修改配置后可重启引用它的工作负载。
				Ops: []string{
					"/k8s.v1.K8sWorkloadService/RestartWorkload",
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sWorkloadService/RestartWorkload",
				},
			},
			{
				Code: "k8s.config.delete",
				Name: "删除网络与配置",
				Ops: []string{
					"/k8s.v1.K8sResourceService/DeleteObject",
				},
			},
			{
				Code: "k8s.config.secret_reveal",
				Name: "查看 Secret 明文",
				// 每次查看都记入审计。
				Ops: []string{
					"/k8s.v1.K8sResourceService/RevealSecret",
				},
			},
		},
	},
	{
		Code: "k8s.resource",
		Name: "资源浏览器",
		// 任意资源类型（含 CRD）的列表、详情与 YAML。
		DefaultOps: []string{
			"/k8s.v1.K8sClusterService/ListClusterOptions",
			"/k8s.v1.K8sResourceService/ListNamespaceOptions",
			"/k8s.v1.K8sResourceService/ListAPIResources",
			"/k8s.v1.K8sResourceService/ListResources",
			"/k8s.v1.K8sResourceService/GetObject",
			"/k8s.v1.K8sResourceService/ListObjectEvents",
			"/k8s.v1.K8sStreamService/WatchResources",
			"/k8s.v1.K8sInsightService/GetPodSelector",
			"/k8s.v1.K8sInsightService/GetRelations",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.resource.create",
				Name: "新建资源",
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
					"/k8s.v1.K8sResourceService/PreviewManifest",
					"/k8s.v1.K8sResourceService/ApplyManifest",
				},
			},
			{
				Code: "k8s.resource.edit",
				Name: "编辑资源",
				Ops: []string{
					"/k8s.v1.K8sResourceService/PreviewObject",
					"/k8s.v1.K8sResourceService/ApplyObject",
				},
			},
			{
				Code: "k8s.resource.delete",
				Name: "删除资源",
				Ops: []string{
					"/k8s.v1.K8sResourceService/DeleteObject",
				},
			},
		},
	},
	{
		Code: "k8s.app",
		Name: "应用",
		// Helm Release 的列表与详情、Helm 仓库列表；Release 里的资源可跳到资源详情。
		DefaultOps: []string{
			"/k8s.v1.K8sClusterService/ListClusterOptions",
			"/k8s.v1.K8sResourceService/ListNamespaceOptions",
			"/k8s.v1.K8sHelmReleaseService/ListHelmReleases",
			"/k8s.v1.K8sHelmReleaseService/GetHelmRelease",
			"/k8s.v1.K8sHelmRepoService/ListHelmRepos",
			"/k8s.v1.K8sHelmRepoService/ListHelmRepoOptions",
			"/k8s.v1.K8sResourceService/GetObject",
			"/k8s.v1.K8sResourceService/ListObjectEvents",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.app.repo",
				Name: "管理 Helm 仓库",
				Ops: []string{
					"/k8s.v1.K8sHelmRepoService/CreateHelmRepo",
					"/k8s.v1.K8sHelmRepoService/UpdateHelmRepo",
					"/k8s.v1.K8sHelmRepoService/DeleteHelmRepo",
					"/k8s.v1.K8sHelmRepoService/TestHelmRepo",
					"/k8s.v1.K8sHelmRepoService/ListHelmCharts",
					"/k8s.v1.K8sHelmRepoService/GetHelmChart",
				},
			},
			{
				Code: "k8s.app.install",
				Name: "安装应用",
				Ops: []string{
					"/k8s.v1.K8sHelmRepoService/ListHelmRepoOptions",
					"/k8s.v1.K8sHelmRepoService/ListHelmCharts",
					"/k8s.v1.K8sHelmRepoService/GetHelmChart",
					"/k8s.v1.K8sHelmReleaseService/PreviewHelmRelease",
					"/k8s.v1.K8sHelmReleaseService/InstallHelmRelease",
				},
			},
			{
				Code: "k8s.app.upgrade",
				Name: "升级与回滚应用",
				Ops: []string{
					"/k8s.v1.K8sHelmRepoService/ListHelmRepoOptions",
					"/k8s.v1.K8sHelmRepoService/ListHelmCharts",
					"/k8s.v1.K8sHelmRepoService/GetHelmChart",
					"/k8s.v1.K8sHelmReleaseService/PreviewHelmRelease",
					"/k8s.v1.K8sHelmReleaseService/UpgradeHelmRelease",
					"/k8s.v1.K8sHelmReleaseService/RollbackHelmRelease",
				},
			},
			{
				Code: "k8s.app.uninstall",
				Name: "卸载应用",
				Ops: []string{
					"/k8s.v1.K8sHelmReleaseService/UninstallHelmRelease",
				},
			},
		},
	},
	{
		Code: "k8s.overview",
		Name: "总览",
		// 各集群的健康状态、资源水位摘要、最近的 Warning 事件与指标概览；集群的监控数据源选项。
		DefaultOps: []string{
			"/k8s.v1.K8sInsightService/GetOverview",
			"/k8s.v1.K8sInsightService/QueryMetrics",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
	},
	{
		Code: "k8s.event",
		Name: "事件",
		// 跨集群的 Warning 事件检索；集群选项用于筛选。
		DefaultOps: []string{
			"/k8s.v1.K8sInsightService/ListEvents",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
	},
	{
		Code: "k8s.change",
		Name: "变更单",
		// 变更单列表与详情；自助申请（命名空间 / 集群权限）的入口与提交。
		DefaultOps: []string{
			"/k8s.v1.K8sChangeService/ListChanges",
			"/k8s.v1.K8sChangeService/GetChange",
			"/k8s.v1.K8sChangeService/GetChangeSettings",
			"/k8s.v1.K8sChangeService/ListRequestClusters",
			"/k8s.v1.K8sChangeService/RequestNamespace",
			"/k8s.v1.K8sChangeService/RequestPermission",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.change.cancel",
				Name: "撤回变更单",
				Ops:  []string{"/k8s.v1.K8sChangeService/CancelChange"},
			},
			{
				Code: "k8s.change.bypass",
				Name: "紧急变更（免审批）",
				// 持有该接口权限即可在需审批的集群上直接执行，审计标记为紧急变更。
				Ops: []string{"/k8s.v1.K8sChangeService/CheckChangeBypass"},
			},
		},
	},
	{
		Code: "k8s.audit",
		Name: "操作审计",
		// 变更记录与终端会话记录；集群选项用于筛选。
		DefaultOps: []string{
			"/k8s.v1.K8sAuditService/ListAudits",
			"/k8s.v1.K8sAuditService/GetAudit",
			"/k8s.v1.K8sAuditService/ListExecSessions",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.audit.replay",
				Name: "回放终端录制",
				Ops: []string{
					"/k8s.v1.K8sGovernanceService/GetExecRecord",
				},
			},
		},
	},
	{
		Code: "k8s.inspection",
		Name: "巡检",
		// 巡检记录与发现项；集群选项用于选择集群。
		DefaultOps: []string{
			"/k8s.v1.K8sGovernanceService/ListInspectionRuns",
			"/k8s.v1.K8sGovernanceService/GetInspectionRun",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.inspection.run",
				Name: "立即巡检",
				Ops: []string{
					"/k8s.v1.K8sGovernanceService/RunInspection",
				},
			},
		},
	},
	{
		Code: "k8s.kubeconfig",
		Name: "访问凭证",
		// 自己的限时 kubeconfig 列表与吊销；集群选项用于选择集群。吊销他人的令牌由用例限定为超管。
		DefaultOps: []string{
			"/k8s.v1.K8sGovernanceService/ListKubeTokens",
			"/k8s.v1.K8sGovernanceService/RevokeKubeToken",
			"/k8s.v1.K8sGovernanceService/RevokeUserKubeTokens",
			"/k8s.v1.K8sClusterService/ListClusterOptions",
		},
		Elements: []ElementSpec{
			{
				Code: "k8s.kubeconfig.issue",
				Name: "签发 kubeconfig",
				Ops: []string{
					"/k8s.v1.K8sGovernanceService/IssueKubeconfig",
				},
			},
		},
	},
	{
		Code: "incident.list",
		Name: "故障列表",
		// 列表、候选故障与详情页各页签的只读接口。告警所属故障（ListIncidentsByAlert）免接口授权、不在此登记：
		// 告警详情页要展示它，访问控制在故障域内按成员 / 接口权限过滤。
		DefaultOps: []string{
			"/incident.v1.IncidentService/ListIncidents",
			"/incident.v1.IncidentService/GetIncident",
			"/incident.v1.IncidentService/SearchIncidents",
			"/incident.v1.IncidentService/ListCandidates",
			"/incident.v1.IncidentService/ListTimeline",
			"/incident.v1.IncidentService/ListIncidentAlerts",
			"/incident.v1.IncidentService/GetImpact",
			"/incident.v1.IncidentService/ListChanges",
			"/incident.v1.IncidentService/GetIncidentContext",
			"/incident.v1.IncidentService/ListIncidentOptions",
			"/incident.v1.IncidentProposalService/ListProposals",
			"/incident.v1.IncidentUpdateService/ListUpdates",
			"/incident.v1.IncidentUpdateService/ListShareLinks",
			"/incident.v1.IncidentPostmortemService/GetPostmortem",
			"/incident.v1.IncidentPostmortemService/ListPostmortemVersions",
			"/incident.v1.IncidentPostmortemService/ListActionItems",
			"/incident.v1.IncidentStatsService/GetOverview",
			// 故障详情「AI 诊断」页签：运维 Agent 的运行摘要（完整转录另由 agent.task 菜单控制）。
			"/agent.v1.AgentTaskService/ListIncidentRuns",
			"/agent.v1.AgentTaskService/GetRun",
		},
		Elements: []ElementSpec{
			{
				Code: "incident.list.diagnose",
				Name: "让 Agent 分析",
				Ops: []string{
					"/agent.v1.AgentTaskService/RunForIncident",
					"/agent.v1.AgentTaskService/CancelTask",
					"/agent.v1.AgentTaskService/RateRun",
				},
			},
			{
				Code: "incident.list.create",
				Name: "新建 / 升级为故障",
				Ops: []string{
					"/incident.v1.IncidentService/CreateIncident",
					"/incident.v1.IncidentService/PromoteCandidate",
					"/incident.v1.IncidentService/DismissCandidate",
				},
			},
			{
				Code: "incident.list.handle",
				Name: "处置",
				// 状态流转、改等级、改指挥官 / 成员、评论、关联告警与变更、合并、开工单、重生成 AI 摘要；
				// 是否有权操作这一条由故障域按角色（指挥官 / 处理人 / 管理员）再判一次。
				Ops: []string{
					"/incident.v1.IncidentService/UpdateIncident",
					"/incident.v1.IncidentService/TransitionIncident",
					"/incident.v1.IncidentService/ChangeSeverity",
					"/incident.v1.IncidentService/AssignCommander",
					"/incident.v1.IncidentService/UpdateMembers",
					"/incident.v1.IncidentService/AddComment",
					"/incident.v1.IncidentService/PinEvent",
					"/incident.v1.IncidentService/AttachAlerts",
					"/incident.v1.IncidentService/DetachAlert",
					"/incident.v1.IncidentService/MarkChange",
					"/incident.v1.IncidentService/MergeIncident",
					"/incident.v1.IncidentService/CreateIncidentTicket",
					"/incident.v1.IncidentService/RegenerateSummary",
				},
			},
			{
				Code: "incident.list.proposal",
				Name: "处置建议",
				Ops: []string{
					"/incident.v1.IncidentProposalService/CreateProposal",
					"/incident.v1.IncidentProposalService/DecideProposal",
					"/incident.v1.IncidentProposalService/CancelProposal",
					"/incident.v1.IncidentProposalService/ResetAutoApprove",
					"/incident.v1.IncidentConfigService/ListPlaybookTargets",
				},
			},
			{
				Code: "incident.list.update",
				Name: "发布公告",
				Ops: []string{
					"/incident.v1.IncidentUpdateService/CreateUpdate",
					"/incident.v1.IncidentUpdateService/DraftUpdate",
				},
			},
			{
				Code: "incident.list.share",
				Name: "分享链接",
				Ops: []string{
					"/incident.v1.IncidentUpdateService/CreateShareLink",
					"/incident.v1.IncidentUpdateService/GetShareLinkToken",
					"/incident.v1.IncidentUpdateService/ResetShareLink",
					"/incident.v1.IncidentUpdateService/DeleteShareLink",
				},
			},
			{
				Code: "incident.list.postmortem",
				Name: "编写复盘",
				Ops: []string{
					"/incident.v1.IncidentPostmortemService/SavePostmortem",
					"/incident.v1.IncidentPostmortemService/CompletePostmortem",
					"/incident.v1.IncidentPostmortemService/RevisePostmortem",
					"/incident.v1.IncidentPostmortemService/DraftPostmortem",
					"/incident.v1.IncidentPostmortemService/GenerateTimeline",
					"/incident.v1.IncidentPostmortemService/SaveActionItem",
					"/incident.v1.IncidentPostmortemService/DeleteActionItem",
					"/incident.v1.IncidentPostmortemService/CreateActionItemTicket",
				},
			},
			{
				Code: "incident.list.agent",
				Name: "AI Agent 接入",
				// 运维 AI Agent 的机器账号持有：占用 / 释放处置租约、写入发现。
				Ops: []string{
					"/incident.v1.IncidentService/AcquireAgentLease",
					"/incident.v1.IncidentService/ReleaseAgentLease",
					"/incident.v1.IncidentService/AddFinding",
				},
			},
		},
	},
	{
		Code: "incident.review",
		Name: "复盘看板",
		DefaultOps: []string{
			"/incident.v1.IncidentService/ListIncidents",
			"/incident.v1.IncidentService/ListIncidentOptions",
			"/incident.v1.IncidentPostmortemService/ListActionItems",
			"/incident.v1.IncidentPostmortemService/GetPostmortem",
		},
	},
	{
		Code: "incident.stats",
		Name: "故障统计",
		DefaultOps: []string{
			"/incident.v1.IncidentStatsService/GetOverview",
			"/incident.v1.IncidentStatsService/GetTrend",
			"/incident.v1.IncidentStatsService/GetEfficiency",
			"/incident.v1.IncidentStatsService/GetDistribution",
			"/incident.v1.IncidentConfigService/ListBusinesses",
		},
	},
	{
		Code: "incident.rule",
		Name: "触发规则",
		DefaultOps: []string{
			"/incident.v1.IncidentConfigService/ListRules",
			"/incident.v1.IncidentConfigService/GetRule",
			"/incident.v1.IncidentConfigService/ListBusinesses",
			"/incident.v1.IncidentConfigService/ListPlaybooks",
			"/incident.v1.IncidentService/ListIncidentOptions",
		},
		Elements: []ElementSpec{
			{Code: "incident.rule.create", Name: "新建规则", Ops: []string{"/incident.v1.IncidentConfigService/CreateRule"}},
			{Code: "incident.rule.update", Name: "编辑规则", Ops: []string{"/incident.v1.IncidentConfigService/UpdateRule"}},
			{Code: "incident.rule.delete", Name: "删除规则", Ops: []string{"/incident.v1.IncidentConfigService/DeleteRule"}},
			{Code: "incident.rule.test", Name: "回放测试", Ops: []string{"/incident.v1.IncidentConfigService/TestRule"}},
		},
	},
	{
		Code: "incident.playbook",
		Name: "处置预案",
		DefaultOps: []string{
			"/incident.v1.IncidentConfigService/ListPlaybooks",
			"/incident.v1.IncidentConfigService/GetPlaybook",
			"/incident.v1.IncidentConfigService/ListPlaybookTargets",
			"/incident.v1.IncidentConfigService/ListBusinesses",
			"/incident.v1.IncidentService/ListIncidentOptions",
		},
		Elements: []ElementSpec{
			{Code: "incident.playbook.create", Name: "新建预案", Ops: []string{"/incident.v1.IncidentConfigService/CreatePlaybook"}},
			{Code: "incident.playbook.update", Name: "编辑预案", Ops: []string{"/incident.v1.IncidentConfigService/UpdatePlaybook"}},
			{Code: "incident.playbook.delete", Name: "删除预案", Ops: []string{"/incident.v1.IncidentConfigService/DeletePlaybook"}},
		},
	},
	{
		Code: "incident.business",
		Name: "业务字典",
		DefaultOps: []string{
			"/incident.v1.IncidentConfigService/ListBusinesses",
			"/incident.v1.IncidentService/ListIncidentOptions",
		},
		Elements: []ElementSpec{
			{Code: "incident.business.create", Name: "新建业务", Ops: []string{"/incident.v1.IncidentConfigService/CreateBusiness"}},
			{Code: "incident.business.update", Name: "编辑业务", Ops: []string{"/incident.v1.IncidentConfigService/UpdateBusiness"}},
			{Code: "incident.business.delete", Name: "删除业务", Ops: []string{"/incident.v1.IncidentConfigService/DeleteBusiness"}},
		},
	},
	{
		Code: "incident.source",
		Name: "接入与 Webhook",
		// 开放接口本身（OpenCreateIncident 等）凭接入令牌认证、免接口授权，不在此登记。
		DefaultOps: []string{
			"/incident.v1.IncidentConfigService/ListSources",
			"/incident.v1.IncidentConfigService/ListWebhooks",
			"/incident.v1.IncidentConfigService/ListBusinesses",
		},
		Elements: []ElementSpec{
			{
				Code: "incident.source.manage",
				Name: "新建 / 编辑接入源",
				Ops: []string{
					"/incident.v1.IncidentConfigService/CreateSource",
					"/incident.v1.IncidentConfigService/UpdateSource",
				},
			},
			{Code: "incident.source.reset", Name: "重置接入令牌", Ops: []string{"/incident.v1.IncidentConfigService/ResetSourceToken"}},
			{Code: "incident.source.delete", Name: "删除接入源", Ops: []string{"/incident.v1.IncidentConfigService/DeleteSource"}},
			{
				Code: "incident.source.webhook",
				Name: "管理 Webhook",
				Ops: []string{
					"/incident.v1.IncidentConfigService/CreateWebhook",
					"/incident.v1.IncidentConfigService/UpdateWebhook",
					"/incident.v1.IncidentConfigService/DeleteWebhook",
				},
			},
		},
	},
	{
		Code: "incident.setting",
		Name: "故障设置",
		DefaultOps: []string{
			"/incident.v1.IncidentConfigService/GetSettings",
			"/incident.v1.IncidentService/ListIncidentOptions",
		},
		Elements: []ElementSpec{
			{Code: "incident.setting.update", Name: "保存设置", Ops: []string{"/incident.v1.IncidentConfigService/UpdateSettings"}},
		},
	},
	{
		Code: "agent.task",
		Name: "Agent 运行记录",
		// 任务列表 / 详情 / 运行详情（含完整转录）；Profile 名称用于筛选。
		DefaultOps: []string{
			"/agent.v1.AgentTaskService/ListTasks",
			"/agent.v1.AgentTaskService/GetTask",
			"/agent.v1.AgentTaskService/GetRun",
			"/agent.v1.AgentTaskService/ListIncidentRuns",
			"/agent.v1.AgentProfileService/ListProfiles",
		},
		Elements: []ElementSpec{
			{
				Code: "agent.task.run",
				Name: "召唤 / 取消任务",
				Ops: []string{
					"/agent.v1.AgentTaskService/RunForIncident",
					"/agent.v1.AgentTaskService/CancelTask",
				},
			},
			{
				Code: "agent.task.rate",
				Name: "评价运行",
				Ops:  []string{"/agent.v1.AgentTaskService/RateRun"},
			},
		},
	},
	{
		Code: "agent.profile",
		Name: "Agent Profile",
		DefaultOps: []string{
			"/agent.v1.AgentProfileService/ListProfiles",
			"/agent.v1.AgentProfileService/GetProfile",
			"/agent.v1.AgentProfileService/ListProfileHistory",
			"/agent.v1.AgentProfileService/ListProfileTools",
			"/system.v1.UserService/ListUsers",
			"/incident.v1.IncidentConfigService/ListBusinesses",
		},
		Elements: []ElementSpec{
			{
				Code: "agent.profile.create",
				Name: "新建 Profile",
				Ops:  []string{"/agent.v1.AgentProfileService/CreateProfile"},
			},
			{
				Code: "agent.profile.update",
				Name: "编辑 Profile",
				Ops:  []string{"/agent.v1.AgentProfileService/UpdateProfile"},
			},
			{
				Code: "agent.profile.delete",
				Name: "删除 Profile",
				Ops:  []string{"/agent.v1.AgentProfileService/DeleteProfile"},
			},
			{
				Code: "agent.profile.test",
				Name: "干跑 Profile",
				Ops: []string{
					"/agent.v1.AgentProfileService/TestProfile",
					"/agent.v1.AgentTaskService/GetTask",
					"/incident.v1.IncidentService/ListIncidents",
				},
			},
		},
	},
	{
		Code: "agent.stats",
		Name: "Agent 统计",
		DefaultOps: []string{
			"/agent.v1.AgentStatsService/GetAgentOverview",
			"/agent.v1.AgentProfileService/ListProfiles",
		},
	},
}

// catalogIndexes 是目录的码索引（注册表不可变，进程内构建一次）。
type catalogIndexes struct {
	menus    map[string]MenuSpec
	elements map[string]ElementSpec
}

// catalogIndex 惰性构建菜单码/元素码索引，供校验与级联展开查询。
var catalogIndex = sync.OnceValue(func() catalogIndexes {
	idx := catalogIndexes{
		menus:    make(map[string]MenuSpec, len(permissionCatalog)),
		elements: make(map[string]ElementSpec, 16),
	}
	for _, menu := range permissionCatalog {
		idx.menus[menu.Code] = menu
		for _, el := range menu.Elements {
			idx.elements[el.Code] = el
		}
	}
	return idx
})

// Catalog 返回权限目录的深拷贝（注册表是包内不可变数据，禁止外泄内部
// slice 引用，调用方可安全改写返回值）。
func Catalog() []MenuSpec {
	out := make([]MenuSpec, 0, len(permissionCatalog))
	for _, menu := range permissionCatalog {
		cp := MenuSpec{
			Code:       menu.Code,
			Name:       menu.Name,
			DefaultOps: slices.Clone(menu.DefaultOps),
			Elements:   make([]ElementSpec, 0, len(menu.Elements)),
		}
		for _, el := range menu.Elements {
			cp.Elements = append(cp.Elements, ElementSpec{
				Code: el.Code,
				Name: el.Name,
				Ops:  slices.Clone(el.Ops),
			})
		}
		out = append(out, cp)
	}
	return out
}

// ValidateGrantCodes 校验菜单/元素码全部登记在权限目录内（拒绝拼写错误/
// 越权试探）；未命中返回 ErrGrantInvalid。接口码（api 类型）不在此列，
// 由 service 层对 proto 反射枚举校验。
func ValidateGrantCodes(menus, elements []string) error {
	idx := catalogIndex()
	for _, code := range menus {
		if _, ok := idx.menus[code]; !ok {
			return fmt.Errorf("%w: %s", ErrGrantInvalid, code)
		}
	}
	for _, code := range elements {
		if _, ok := idx.elements[code]; !ok {
			return fmt.Errorf("%w: %s", ErrGrantInvalid, code)
		}
	}
	return nil
}
