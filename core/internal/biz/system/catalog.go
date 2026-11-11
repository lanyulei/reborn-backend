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
		DefaultOps: []string{
			"/system.v1.UserService/ListUsers",
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
		// 纯排障查询页：事件是 append-only 运行数据（管道写 / core 读），
		// 无管理写接口，页内无功能元素（同 system.login_log 先例）。
		DefaultOps: []string{
			"/notify.v1.NotifyEventService/ListEvents",
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
