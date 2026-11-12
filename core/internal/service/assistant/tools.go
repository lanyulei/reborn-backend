package service

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// pathVarPattern 匹配 HTTP 路径模板里的 {field} / {field=**} 占位。
var pathVarPattern = regexp.MustCompile(`\{([a-zA-Z0-9_.]+)(?:=[^}]*)?\}`)

// excludedOperations 不暴露给模型的 RPC：助手自身接口（递归无意义）与二进制
// HttpBody 接口（无法以 JSON 工具表达）。
var excludedOperations = map[string]struct{}{
	"/assistant.v1.AssistantChatService/Chat":           {},
	"/assistant.v1.AssistantChatService/Confirm":        {},
	"/assistant.v1.AssistantService/ListConversations":  {},
	"/assistant.v1.AssistantService/ListMessages":       {},
	"/assistant.v1.AssistantService/DeleteConversation": {},
	"/system.v1.SystemConfigService/UploadSystemLogo":   {},
	"/system.v1.SystemConfigService/GetSystemLogo":      {},
	"/system.v1.AuthService/UploadMyAvatar":             {},
	"/system.v1.AuthService/GetUserAvatar":              {},
	"/system.v1.RoleService/ListOperations":             {},
	"/system.v1.RoleService/ListPermissionCatalog":      {},
	// CMDB 批量导入/导出：HttpBody 与大批量载荷不适合作为对话工具。
	"/cmdb.v1.CmdbCIService/ImportCIs": {},
	"/cmdb.v1.CmdbCIService/ExportCIs": {},
	// 采集源凭据明文不经过模型。
	"/cmdb.v1.CmdbSyncService/CreateSource":  {},
	"/cmdb.v1.CmdbSyncService/UpdateSource":  {},
	"/cmdb.v1.CmdbSyncService/RotateWebhook": {},
}

// NewToolCatalog 从可授权 RPC 枚举生成模型工具目录（启动期一次性反射；运行
// 期 biz 只读纯数据）。HTTP body 绑定仅支持 "" 与 "*"（项目内全部如此），
// 出现其它绑定形式在装配阶段报错，防止回环执行器静默错传参数。
func NewToolCatalog() (biz.ToolCatalog, error) {
	methods := systemservice.AuthorizableMethods()
	catalog := make(biz.ToolCatalog, 0, len(methods))
	for _, m := range methods {
		if _, skip := excludedOperations[m.Name]; skip {
			continue
		}
		if m.HTTPMethod == "" || m.Path == "" {
			return nil, fmt.Errorf("assistant tool %s: missing http route", m.Name)
		}
		if m.Body != "" && m.Body != "*" {
			return nil, fmt.Errorf("assistant tool %s: unsupported http body binding %q", m.Name, m.Body)
		}
		spec, err := buildToolSpec(m)
		if err != nil {
			return nil, err
		}
		catalog = append(catalog, spec)
	}
	return catalog, nil
}

func buildToolSpec(m systemservice.AuthorizableMethod) (biz.ToolSpec, error) {
	svc, method, _ := strings.Cut(strings.TrimPrefix(m.Name, "/"), "/")
	short := svc[strings.LastIndexByte(svc, '.')+1:]
	name := short + "_" + method
	pathFields, err := pathFieldJSONNames(m.Path, m.Input)
	if err != nil {
		return biz.ToolSpec{}, fmt.Errorf("assistant tool %s: %w", m.Name, err)
	}
	schema, err := json.Marshal(messageSchema(m.Input, pathFields))
	if err != nil {
		return biz.ToolSpec{}, fmt.Errorf("assistant tool %s: schema: %w", m.Name, err)
	}
	desc := m.Title
	if desc == "" {
		desc = method
	}
	desc += fmt.Sprintf("（%s %s）", m.HTTPMethod, m.Path)
	write := isWriteMethod(method)
	if write {
		desc += "；写操作，执行前需用户确认"
	}
	return biz.ToolSpec{
		Name:         name,
		Operation:    m.Name,
		Code:         m.Code,
		Title:        m.Title,
		Description:  desc,
		Write:        write,
		HTTPMethod:   m.HTTPMethod,
		PathTemplate: m.Path,
		PathFields:   pathFields,
		BodyAll:      m.Body == "*",
		InputSchema:  schema,
	}, nil
}

// isWriteMethod 方法名不以 Get/List 开头即视为写操作（含 SendTestEvent /
// RotateProducerToken / SetRolePermissions 等有副作用的 POST）。
func isWriteMethod(method string) bool {
	return !strings.HasPrefix(method, "Get") && !strings.HasPrefix(method, "List")
}

// pathFieldJSONNames 把路径模板占位（proto 字段名）映射为 protojson 字段名，
// 按出现顺序返回；占位字段不存在于入参消息时报错。
func pathFieldJSONNames(path string, input protoreflect.MessageDescriptor) ([]string, error) {
	matches := pathVarPattern.FindAllStringSubmatch(path, -1)
	names := make([]string, 0, len(matches))
	for _, mt := range matches {
		fd := input.Fields().ByName(protoreflect.Name(mt[1]))
		if fd == nil {
			fd = input.Fields().ByJSONName(mt[1])
		}
		if fd == nil {
			return nil, fmt.Errorf("path var %q not found in %s", mt[1], input.FullName())
		}
		names = append(names, fd.JSONName())
	}
	return names, nil
}
