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
