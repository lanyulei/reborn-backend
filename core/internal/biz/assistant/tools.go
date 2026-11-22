package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// maxToolOutputBytes 回传给模型/落审计的工具输出上限；超出截断并提示缩小分页。
const maxToolOutputBytes = 32 << 10

// sensitiveKeyParts 入参/出参脱敏的键名片段（大小写不敏感，子串匹配）；与
// kit/httpx 日志脱敏口径一致，宁可误打码不可漏打码。
var sensitiveKeyParts = []string{
	"password", "passwd", "secret", "token", "authorization", "api_key", "apikey",
	"private_key", "privatekey", "access_key", "accesskey", "cookie", "signature", "kubeconfig",
}

// maskedValue 脱敏替换值。
const maskedValue = "***"

// MaskJSON 对 JSON 对象/数组中键名含敏感片段的值做脱敏（递归）；非法 JSON
// 原样返回（不阻断调用，审计仍可落库）。
func MaskJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	if !maskValue(v) {
		return raw
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

// maskValue 递归脱敏，返回是否发生替换（无替换时调用方可跳过重编码）。
func maskValue(v any) bool {
	changed := false
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isSensitiveKey(k) {
				if s, ok := val.(string); !ok || s != "" {
					t[k] = maskedValue
					changed = true
				}
				continue
			}
			if maskValue(val) {
				changed = true
			}
		}
	case []any:
		for _, item := range t {
			if maskValue(item) {
				changed = true
			}
		}
	}
	return changed
}

func isSensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	for _, part := range sensitiveKeyParts {
		if strings.Contains(lk, part) {
			return true
		}
	}
	return false
}

// TruncateOutput 把超长工具输出截断为提示文本（保持合法 JSON），避免撑爆模型上下文。
// 调用方须先脱敏再截断：截断后的 preview 是一段普通字符串，按键名的脱敏对它不再生效。
func TruncateOutput(raw json.RawMessage) json.RawMessage {
	if len(raw) <= maxToolOutputBytes {
		return raw
	}
	msg := map[string]any{
		"truncated": true,
		"message":   "工具返回内容过长已截断，请缩小 page_size 或增加过滤条件后重试",
		"preview":   strings.ToValidUTF8(string(raw[:1024]), ""),
	}
	out, _ := json.Marshal(msg)
	return out
}

// filterTools 按用户有效授权过滤工具目录；超管直通全集。allowed 为授权 code 集合。
func filterTools(catalog ToolCatalog, isAdmin bool, allowed []string) []ToolSpec {
	if isAdmin {
		return append([]ToolSpec(nil), catalog...)
	}
	set := make(map[string]struct{}, len(allowed))
	for _, code := range allowed {
		set[code] = struct{}{}
	}
	out := make([]ToolSpec, 0, len(allowed))
	for _, spec := range catalog {
		if _, ok := set[spec.Code]; ok {
			out = append(out, spec)
		}
	}
	return out
}

// 工具按模块加载：可用工具很多时，每次模型请求都带全量工具目录既慢又贵，还会让模型
// 更容易选错工具。此时只下发一个「加载模块」的元工具和已加载模块的工具；模型按需
// 加载。可用工具不多（普通用户通常如此）时不启用，行为与全量下发完全一致。
const (
	// LoadToolsName 元工具名：加载功能模块的工具。
	LoadToolsName = "load_tools"
	// moduleRoutingMinTools 可用工具数超过它才按模块加载。
	moduleRoutingMinTools = 40
)

// loadToolsSchema 元工具入参。必须对所有用户相同：模型客户端按工具名缓存入参结构，
// 所以各用户可加载的模块只写在说明里，不写进 enum；加载了无权的模块按未知模块处理。
var loadToolsSchema = json.RawMessage(`{"type":"object","properties":{"modules":{"type":"array","description":"要加载的模块名","items":{"type":"string"}}},"required":["modules"],"additionalProperties":false}`)

// moduleLabels 模块的中文说明（给模型判断该加载哪个模块）；未登记的模块只显示模块名。
var moduleLabels = map[string]string{
	"cmdb":      "资源管理（CMDB）：模型、属性、配置项、资源关联与拓扑、唯一规则、采集同步、统计",
	"knowledge": "知识库：知识空间、知识文档（手册 / 规范 / 排查经验 / 故障复盘）的查看、新建草稿、版本、关联对象与导入任务；检索工具 KnowledgeService_Search 常驻，不需要加载本模块",
	"k8s":       "容器平台（只读）：集群与命名空间、资源列表与对象详情、事件、总览、指标、关联链路，以及 Pod / 工作负载诊断摘要（排查 Pod 起不来、反复重启、副本不足时优先用诊断摘要）",
	"monitor":   "监控告警：数据源与即时查询、规则分组与数据范围、告警规则（导入导出、变更历史、内置模板、模拟触发）、告警事件（认领、转交、备注、关闭、转工单）、屏蔽规则与抑制规则、告警订阅、升级策略、记录规则、告警统计、评估引擎状态",
	"notify":    "通知管理：渠道、通知规则（路由）、模板、静默、订阅、生产者、事件与投递追踪、死信",
	"oncall":    "值班管理：值班分组与成员、值班表、班次与值班日历、替班、当前值班人、公共日历地址、值班通知",
	"system":    "系统管理：用户、角色与权限、登录日志、系统配置、个人资料",
}

// initModules 判断本轮是否按模块加载，并从历史恢复已加载的模块（模型加载过的、
// 或调用过其工具的）——同一会话的后续轮次不必重新加载。
func (r *run) initModules() {
	if len(r.tools) <= moduleRoutingMinTools {
		return
	}
	var modules []string
	for _, spec := range r.tools {
		if !slices.Contains(modules, spec.Module) {
			modules = append(modules, spec.Module)
		}
	}
	if len(modules) < 2 {
		return
	}
	slices.Sort(modules)
	r.routed, r.modules, r.active = true, modules, make(map[string]bool, len(modules))
	for _, m := range r.messages {
		if m.Role != RoleAssistant {
			continue
		}
		for _, ref := range m.ToolCalls {
			if ref.Name == LoadToolsName {
				for _, name := range requestedModules(ref.Arguments) {
					if slices.Contains(modules, name) {
						r.active[name] = true
					}
				}
			} else if spec := r.findTool(ref.Name); spec != nil {
				r.active[spec.Module] = true
			}
		}
	}
}

// offeredTools 本次模型请求下发的工具：不按模块加载时为全部可用工具；否则为元工具
// 加已加载模块的工具。
func (r *run) offeredTools() []ToolSpec {
	if !r.routed {
		return r.tools
	}
	out := make([]ToolSpec, 0, len(r.tools)+1)
	out = append(out, r.loadToolsSpec())
	for _, spec := range r.tools {
		if spec.Resident || r.active[spec.Module] {
			out = append(out, spec)
		}
	}
	return out
}

// loadToolsSpec 元工具定义：列出本用户可加载的模块及各自的工具数。
func (r *run) loadToolsSpec() ToolSpec {
	counts := make(map[string]int, len(r.modules))
	for _, spec := range r.tools {
		counts[spec.Module]++
	}
	var desc strings.Builder
	desc.WriteString("加载功能模块的工具，加载后才能调用该模块的具体工具。可用模块：")
	for i, name := range r.modules {
		if i > 0 {
			desc.WriteString("；")
		}
		label := moduleLabels[name]
		if label == "" {
			label = name
		}
		// 说明不带"已加载"之类随对话变化的内容：工具定义保持稳定才能命中模型侧的前缀缓存
		//（加载结果已在对话历史的 tool 消息里）。
		fmt.Fprintf(&desc, "%s = %s（%d 个工具）", name, label, counts[name])
	}
	desc.WriteString("。一次可以加载多个模块。")
	return ToolSpec{Name: LoadToolsName, Description: desc.String(), InputSchema: loadToolsSchema}
}

// loadModules 执行元工具：把请求的模块标记为已加载并回灌结果。它不调用任何接口、
// 不涉及业务数据，所以不建审计记录、不向客户端推送步骤事件；只在对话历史里留下
// 调用与结果（协议要求每个 tool_call 都有对应的 tool 消息）。
func (r *run) loadModules(ctx context.Context, ref ToolCallRef) error {
	var loaded, unknown []string
	for _, name := range requestedModules(ref.Arguments) {
		if slices.Contains(r.modules, name) {
			r.active[name] = true
			loaded = append(loaded, name)
		} else {
			unknown = append(unknown, name)
		}
	}
	result := map[string]any{"available_modules": r.modules}
	switch {
	case len(loaded) > 0:
		result["loaded"] = loaded
		result["message"] = "模块工具已加载，现在可以直接调用"
	default:
		result["error"] = "没有加载任何模块：请从 available_modules 里选择"
	}
	if len(unknown) > 0 {
		result["unknown"] = unknown
	}
	content, _ := json.Marshal(result)
	return r.append(ctx, &Message{Role: RoleTool, Content: string(content), ToolCallID: ref.ID})
}

// requestedModules 解析元工具参数里的模块名（非法 JSON 视为没有）。
func requestedModules(arguments string) []string {
	var args struct {
		Modules []string `json:"modules"`
	}
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return nil
	}
	return args.Modules
}
