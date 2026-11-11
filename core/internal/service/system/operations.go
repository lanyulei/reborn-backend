package service

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	assistantv1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"
	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/genproto/googleapis/api/annotations"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// operationFiles 是参与接口授权的 proto 文件描述符（编译期引用，不走
// protoregistry 字符串路径查找）；可授权操作全集 = 这些文件内全部 RPC 的
// proto full method（扣除 authzExemptOperations），与鉴权/授权中间件看到
// 的 operation 同一口径。auth.proto 保留在列：未来新增需门禁的认证类 RPC
// 会自动进入枚举（是否免检由 authzExemptOperations 单独决定）。notify 域
// 各资源 proto 逐个追加在列。
var operationFiles = []protoreflect.FileDescriptor{
	v1.File_system_v1_user_proto,
	v1.File_system_v1_role_proto,
	v1.File_system_v1_auth_proto,
	v1.File_system_v1_system_config_proto,
	v1.File_system_v1_login_log_proto,
	notifyv1.File_notify_v1_channel_proto,
	notifyv1.File_notify_v1_template_proto,
	notifyv1.File_notify_v1_route_proto,
	notifyv1.File_notify_v1_silence_proto,
	notifyv1.File_notify_v1_subscription_proto,
	notifyv1.File_notify_v1_producer_proto,
	notifyv1.File_notify_v1_event_proto,
	notifyv1.File_notify_v1_dispatch_proto,
	notifyv1.File_notify_v1_delivery_log_proto,
	notifyv1.File_notify_v1_dead_letter_proto,
	assistantv1.File_assistant_v1_assistant_proto,
}

// authzExemptOperations 是免授权检查的操作集合（proto full method），为鉴权
// 白名单的扩集：Login/RefreshToken 无 claims 可判；Logout 与 GetLoginProfile
// 是已认证用户的自助身份接口。此集合是 authz 中间件与操作枚举的共同事实源：
// 中间件对其跳过权限判定，ListOperations 不枚举它们（对免检操作授权无意义）。
var authzExemptOperations = map[string]struct{}{
	v1.OperationAuthServiceLogin:           {},
	v1.OperationAuthServiceRefreshToken:    {},
	v1.OperationAuthServiceLogout:          {},
	v1.OperationAuthServiceGetLoginProfile: {},
	// 全局展示配置（标题/logo 等）：登录页需在登录前拉取，匿名可读；
	// 随 authExempt 同步免 JWT。写接口 UpdateSystemConfig/UploadSystemLogo 不免检。
	v1.OperationSystemConfigServiceGetSystemConfig: {},
	// logo 文件读取（静态资源语义）：登录页 <img> 直载，匿名可读；
	// 随 authExempt 同步免 JWT。
	v1.OperationSystemConfigServiceGetSystemLogo: {},
}

// IsAuthzExempt 报告操作是否免授权检查；供 server 层 authz 中间件使用，
// 与 ListOperations 的排除口径共享同一集合，杜绝两处定义漂移。
func IsAuthzExempt(operation string) bool {
	_, ok := authzExemptOperations[operation]
	return ok
}

// operation 是可授权操作条目：Code 是授权存储与 authz 判定的唯一口径
// （md5(name)），Name/Title/Path/Method 仅供展示与排查。
type operation struct {
	Name   string
	Code   string
	Title  string
	Path   string
	Method string
}

// listOperations 枚举全部可授权接口操作，按 Name 字典序；sync.OnceValue
// 缓存（描述符不可变，重复枚举无意义）。
var listOperations = sync.OnceValue(func() []operation {
	ops := make([]operation, 0, 32)
	for _, fd := range operationFiles {
		services := fd.Services()
		for i := range services.Len() {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := range methods.Len() {
				m := methods.Get(j)
				name := fmt.Sprintf("/%s/%s", svc.FullName(), m.Name())
				if IsAuthzExempt(name) {
					continue // 免授权操作不可授权，不进枚举
				}
				method, path := httpRoute(m)
				ops = append(ops, operation{
					Name:   name,
					Code:   biz.OperationCode(name),
					Title:  title(m),
					Path:   path,
					Method: method,
				})
			}
		}
	}
	slices.SortFunc(ops, func(a, b operation) int { return cmp.Compare(a.Name, b.Name) })
	return ops
})

// httpRoute 读 rpc 的 google.api.http 注解，返回 (HTTP 方法, 接口地址)；
// 多重绑定（additional_bindings）不拆分，仅取主绑定；未标注注解返回空串。
func httpRoute(m protoreflect.MethodDescriptor) (method, path string) {
	opts := m.Options()
	if opts == nil || !proto.HasExtension(opts, annotations.E_Http) {
		return "", ""
	}
	rule, ok := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule)
	if !ok || rule == nil {
		return "", ""
	}
	switch pattern := rule.Pattern.(type) {
	case *annotations.HttpRule_Get:
		return "GET", pattern.Get
	case *annotations.HttpRule_Put:
		return "PUT", pattern.Put
	case *annotations.HttpRule_Post:
		return "POST", pattern.Post
	case *annotations.HttpRule_Delete:
		return "DELETE", pattern.Delete
	case *annotations.HttpRule_Patch:
		return "PATCH", pattern.Patch
	case *annotations.HttpRule_Custom:
		return pattern.Custom.GetKind(), pattern.Custom.GetPath()
	default:
		return "", ""
	}
}

// title 读 rpc 的 MethodOptions 扩展（system.v1.title）；option 嵌入生成
// 代码（proto 注释则会被剥掉，运行时不可读），未标注返回空串。
func title(m protoreflect.MethodDescriptor) string {
	if opts := m.Options(); opts != nil {
		if name, ok := proto.GetExtension(opts, v1.E_Title).(string); ok {
			return name
		}
	}
	return ""
}

// ErrOperationInvalid 哨兵：授权请求携带未枚举的操作 code（400）。
var ErrOperationInvalid = errors.BadRequest(v1.ErrorReason_OPERATION_INVALID.String(), "unknown operation")

// validateOperations 校验操作 code 全部在可枚举集合内（拒绝拼写错误/越权
// 试探）；未命中返回 ErrOperationInvalid。策略 matcher 为精确相等，垃圾
// code 存库也永不命中，但入口校验保证配置面干净、问题尽早暴露。
func validateOperations(operations []string) error {
	known := make(map[string]struct{}, 64)
	for _, op := range listOperations() {
		known[op.Code] = struct{}{}
	}
	for _, op := range operations {
		if _, ok := known[op]; !ok {
			return fmt.Errorf("%w: %s", ErrOperationInvalid, op)
		}
	}
	return nil
}

// AuthorizableMethod 是可授权 RPC 的方法级视图：供 assistant 域生成模型工具
// 目录（与 listOperations 同源、同口径，避免两处维护 proto 文件清单）。
type AuthorizableMethod struct {
	// Name proto full method；Code = md5(Name)。
	Name string
	Code string
	// Title 中文显示名（system.v1.title 注解）。
	Title string
	// HTTPMethod / Path / Body 来自 google.api.http 主绑定；Body 为 "" 或 "*"。
	HTTPMethod string
	Path       string
	Body       string
	// Input 请求消息描述符（生成入参 JSON Schema 用）。
	Input protoreflect.MessageDescriptor
}

// AuthorizableMethods 枚举全部可授权 RPC（扣除免授权操作），按 Name 字典序；
// 描述符不可变，结果缓存。
var AuthorizableMethods = sync.OnceValue(func() []AuthorizableMethod {
	out := make([]AuthorizableMethod, 0, 64)
	for _, fd := range operationFiles {
		services := fd.Services()
		for i := range services.Len() {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := range methods.Len() {
				m := methods.Get(j)
				name := fmt.Sprintf("/%s/%s", svc.FullName(), m.Name())
				if IsAuthzExempt(name) {
					continue
				}
				method, path := httpRoute(m)
				out = append(out, AuthorizableMethod{
					Name:       name,
					Code:       biz.OperationCode(name),
					Title:      title(m),
					HTTPMethod: method,
					Path:       path,
					Body:       httpBody(m),
					Input:      m.Input(),
				})
			}
		}
	}
	slices.SortFunc(out, func(a, b AuthorizableMethod) int { return cmp.Compare(a.Name, b.Name) })
	return out
})

// httpBody 读 rpc 的 google.api.http 注解 body 字段；未标注返回空串。
func httpBody(m protoreflect.MethodDescriptor) string {
	opts := m.Options()
	if opts == nil || !proto.HasExtension(opts, annotations.E_Http) {
		return ""
	}
	rule, ok := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule)
	if !ok || rule == nil {
		return ""
	}
	return rule.GetBody()
}
