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
	// 流程引擎的签名密钥与接入令牌明文不经过模型。
	"/workflow.v1.WorkflowConfigService/CreateWebhook":     {},
	"/workflow.v1.WorkflowConfigService/UpdateWebhook":     {},
	"/workflow.v1.WorkflowConfigService/CreateClient":      {},
	"/workflow.v1.WorkflowConfigService/RotateClientToken": {},
	// 监控数据源的凭据（Basic 密码 / Bearer 令牌）明文不经过模型。
	"/monitor.v1.MonitorDatasourceService/CreateDatasource":     {},
	"/monitor.v1.MonitorDatasourceService/UpdateDatasource":     {},
	"/monitor.v1.MonitorDatasourceService/TestDatasourceConfig": {},
	// 作业中心：凭据 / 仓库令牌的明文、执行节点接入令牌的明文不经过模型。
	"/job.v1.JobCredentialService/CreateJobCredential": {},
	"/job.v1.JobCredentialService/UpdateJobCredential": {},
	"/job.v1.JobProjectService/CreateJobProject":       {},
	"/job.v1.JobProjectService/UpdateJobProject":       {},
	"/job.v1.JobRunnerService/CreateJobRunner":         {},
	"/job.v1.JobRunnerService/ResetJobRunnerToken":     {},
	// 提交执行会直接改动目标主机：只允许人在界面上看过主机清单后发起，不作为对话工具。
	"/job.v1.JobExecutionService/SubmitJobExecution": {},
	"/job.v1.JobExecutionService/SubmitJobAdhoc":     {},
	// 触发器的调用令牌明文不经过模型。
	"/job.v1.JobTriggerService/CreateJobTrigger":     {},
	"/job.v1.JobTriggerService/ResetJobTriggerToken": {},
	// 知识库导入需要先上传文件，模型拿不到文件标识；索引重建是管理员的运维动作。
	"/knowledge.v1.KnowledgeIngestService/CreateIngestJob": {},
	"/knowledge.v1.KnowledgeAdminService/RebuildIndex":     {},
	// 故障管理：分享链接令牌、接入源令牌、Webhook 签名密钥的明文不经过模型；凭令牌鉴权的公开页与开放接口
	// 是给外部系统 / 匿名访客用的，不作为对话工具。
	"/incident.v1.IncidentUpdateService/CreateShareLink":   {},
	"/incident.v1.IncidentUpdateService/GetShareLinkToken": {},
	"/incident.v1.IncidentUpdateService/ResetShareLink":    {},
	"/incident.v1.IncidentUpdateService/GetPublicIncident": {},
	"/incident.v1.IncidentConfigService/CreateSource":      {},
	"/incident.v1.IncidentConfigService/ResetSourceToken":  {},
	"/incident.v1.IncidentConfigService/CreateWebhook":     {},
	"/incident.v1.IncidentConfigService/UpdateWebhook":     {},
	"/incident.v1.IncidentOpenService/OpenCreateIncident":  {},
	"/incident.v1.IncidentOpenService/OpenGetIncident":     {},
	"/incident.v1.IncidentOpenService/OpenAppendNote":      {},
	"/incident.v1.IncidentOpenService/OpenResolveIncident": {},
}

// residentOperations 常驻工具：工具按模块懒加载时也始终下发（跨模块的通用能力）。
var residentOperations = map[string]struct{}{
	"/knowledge.v1.KnowledgeService/Search": {},
}

// readOnlyOperations 用 POST 承载但不写任何数据的 RPC（预览 / 预检）：按读操作处理，
// 不需要用户确认。只列确认过无副作用的接口；新接口默认仍按 HTTP 方法判定。
var readOnlyOperations = map[string]struct{}{
	"/oncall.v1.OncallScheduleService/PreviewSchedule": {},
	"/notify.v1.NotifyTemplateService/RenderTemplate":  {},
	"/cmdb.v1.CmdbModelService/CheckUniqueRule":        {},
	// 模拟运行只在内存里把流程图走一遍，不落库、不发通知、不调用外部系统。
	"/workflow.v1.WorkflowDefinitionService/SimulateDefinition": {},
	// 预览目标主机只解析 CMDB 资源，不落库。
	"/job.v1.JobTemplateService/PreviewJobTarget": {},
	// 快速执行的提交前检查、规则试匹配、Cron 预览：只计算，不落库。
	"/job.v1.JobExecutionService/CheckJobAdhoc":     {},
	"/job.v1.JobPolicyService/TestJobRiskRules":     {},
	"/job.v1.JobScheduleService/PreviewJobSchedule": {},
	// 指标面板查询：POST 只为承载目标参数，服务端固定模板查询监控数据源，无副作用。
	"/k8s.v1.K8sInsightService/QueryMetrics": {},
	// 知识库检索：POST 只为承载检索词与过滤条件，只读（会记一条检索日志，不改业务数据）。
	"/knowledge.v1.KnowledgeService/Search": {},
	// 监控数据源即时查询 / 区间查询：POST 只为承载查询表达式与时间窗，查的是时序库，不写任何数据。
	"/monitor.v1.MonitorDatasourceService/QueryDatasource":      {},
	"/monitor.v1.MonitorDatasourceService/QueryDatasourceRange": {},
}

// k8sToolOperations 容器平台域允许作为对话工具的接口：全部只读。助手只诊断、不动手——新增的容器平台
// 接口默认不是工具，确认只读且不返回凭据 / Secret 明文后才登记到这里。GetObject 返回的 Secret 已掩码。
var k8sToolOperations = map[string]struct{}{
	"/k8s.v1.K8sClusterService/ListClusterOptions":     {},
	"/k8s.v1.K8sResourceService/ListNamespaceOptions":  {},
	"/k8s.v1.K8sResourceService/ListAPIResources":      {},
	"/k8s.v1.K8sResourceService/ListResources":         {},
	"/k8s.v1.K8sResourceService/GetObject":             {},
	"/k8s.v1.K8sResourceService/ListObjectEvents":      {},
	"/k8s.v1.K8sWorkloadService/ListWorkloadRevisions": {},
	"/k8s.v1.K8sInsightService/GetOverview":            {},
	"/k8s.v1.K8sInsightService/ListEvents":             {},
	"/k8s.v1.K8sInsightService/QueryMetrics":           {},
	"/k8s.v1.K8sInsightService/GetRelations":           {},
	"/k8s.v1.K8sInsightService/DiagnosePod":            {},
	"/k8s.v1.K8sInsightService/DiagnoseWorkload":       {},
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
		// 运维 Agent 域自身的接口（Profile / 任务 / 运行 / 统计）不作为工具：模型调度 Agent 没有意义，也避免递归。
		if strings.HasPrefix(m.Name, "/agent.v1.") {
			continue
		}
		// 容器平台接口只把确认过的只读接口作为对话工具（k8sToolOperations）：写接口会直接改动集群
		// （必须由人在界面上看过 Diff 后提交），集群凭据、Agent 注册令牌与 Secret 明文不经过模型，终端不可用。
		if strings.HasPrefix(m.Name, "/k8s.v1.") {
			if _, ok := k8sToolOperations[m.Name]; !ok {
				continue
			}
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
	write := isWriteOperation(m.Name, m.HTTPMethod)
	if write {
		desc += "；写操作，执行前需用户确认"
	}
	_, resident := residentOperations[m.Name]
	return biz.ToolSpec{
		Name:         name,
		Module:       svc[:strings.IndexByte(svc+".", '.')],
		Operation:    m.Name,
		Code:         m.Code,
		Title:        m.Title,
		Description:  desc,
		Write:        write,
		Resident:     resident,
		HTTPMethod:   m.HTTPMethod,
		PathTemplate: m.Path,
		PathFields:   pathFields,
		BodyAll:      m.Body == "*",
		InputSchema:  schema,
	}, nil
}

// isWriteOperation 写操作判定：HTTP GET 一律是读（本项目的 GET 接口都不改数据，如全局
// 搜索、拓扑查询）；其余方法视为写（含 SendTestEvent / RotateProducerToken /
// SetRolePermissions 等有副作用的 POST、PUT），除非列入 readOnlyOperations。
// 写操作执行前须用户确认，所以拿不准的一律算写。
func isWriteOperation(operation, httpMethod string) bool {
	if httpMethod == "GET" {
		return false
	}
	_, readOnly := readOnlyOperations[operation]
	return !readOnly
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
