package service

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestNewToolCatalog 锁死工具目录契约：覆盖全部可授权接口（扣除助手自身与
// 二进制接口）、读写分类、路径字段与 JSON Schema 形态。
func TestNewToolCatalog(t *testing.T) {
	t.Parallel()
	catalog, err := NewToolCatalog()
	if err != nil {
		t.Fatalf("NewToolCatalog() error = %v", err)
	}
	if len(catalog) < 50 {
		t.Fatalf("catalog too small: %d", len(catalog))
	}
	names := make(map[string]bool, len(catalog))
	for _, spec := range catalog {
		if names[spec.Name] {
			t.Fatalf("duplicate tool name %s", spec.Name)
		}
		names[spec.Name] = true
		if spec.Name == "" || spec.Operation == "" || len(spec.Code) != 32 || spec.HTTPMethod == "" || !strings.HasPrefix(spec.PathTemplate, "/api/") {
			t.Fatalf("incomplete spec: %+v", spec)
		}
		var schema map[string]any
		if err := json.Unmarshal(spec.InputSchema, &schema); err != nil || schema["type"] != "object" {
			t.Fatalf("tool %s schema invalid: %s", spec.Name, spec.InputSchema)
		}
		if strings.Contains(spec.Operation, "Assistant") {
			t.Fatalf("assistant own rpc must be excluded: %s", spec.Operation)
		}
	}
	cases := map[string]struct {
		write      bool
		method     string
		pathFields []string
		bodyAll    bool
	}{
		"UserService_ListUsers":                    {false, "GET", nil, false},
		"UserService_GetUser":                      {false, "GET", []string{"id"}, false},
		"UserService_CreateUser":                   {true, "POST", nil, true},
		"UserService_DeleteUser":                   {true, "DELETE", []string{"id"}, false},
		"NotifyDeadLetterService_ReplayDeadLetter": {true, "POST", []string{"id"}, true},
		"NotifyChannelService_SendTestEvent":       {true, "POST", []string{"id"}, true},
		"RoleService_SetRolePermissions":           {true, "PUT", []string{"id"}, true},
		// 预览 / 预检不写数据：按读处理，不需要用户确认；GET 一律是读。
		"OncallScheduleService_PreviewSchedule": {false, "POST", nil, true},
		"NotifyTemplateService_RenderTemplate":  {false, "POST", nil, true},
		"CmdbCIService_SearchCIs":               {false, "GET", nil, false},
		// 有副作用的 POST 仍是写。
		"OncallScheduleService_SendScheduleNotification": {true, "POST", []string{"id"}, true},
		"OncallScheduleService_CreateOverride":           {true, "POST", nil, true},
	}
	byName := make(map[string]int, len(catalog))
	for i, spec := range catalog {
		byName[spec.Name] = i
	}
	for name, want := range cases {
		i, ok := byName[name]
		if !ok {
			t.Fatalf("tool %s missing", name)
		}
		spec := catalog[i]
		if spec.Write != want.write || spec.HTTPMethod != want.method || spec.BodyAll != want.bodyAll {
			t.Fatalf("tool %s = %+v, want %+v", name, spec, want)
		}
		if strings.Join(spec.PathFields, ",") != strings.Join(want.pathFields, ",") {
			t.Fatalf("tool %s path fields = %v, want %v", name, spec.PathFields, want.pathFields)
		}
	}
	for _, excluded := range []string{"SystemConfigService_UploadSystemLogo", "SystemConfigService_GetSystemLogo", "RoleService_ListOperations"} {
		if names[excluded] {
			t.Fatalf("tool %s should be excluded", excluded)
		}
	}
	// 路径字段进入 required，便于模型必填。
	spec := catalog[byName["UserService_GetUser"]]
	if !strings.Contains(string(spec.InputSchema), `"required":["id"]`) {
		t.Fatalf("GetUser schema should require id: %s", spec.InputSchema)
	}
	// 枚举与时间字段的 schema 形态。
	spec = catalog[byName["NotifyChannelService_ListChannels"]]
	if !strings.Contains(string(spec.InputSchema), `"enum"`) && !strings.Contains(string(spec.InputSchema), "integer") {
		t.Fatalf("ListChannels schema unexpected: %s", spec.InputSchema)
	}
}

// 字段说明来自 proto 注释（make gen 生成 fielddoc.json）：模型靠它理解枚举取值与约束。
// 工具入参的每个顶层字段都要有说明——缺了说明多半是改了 proto 却没有重新生成。
func TestNewToolCatalog_FieldDocs(t *testing.T) {
	t.Parallel()
	catalog, err := NewToolCatalog()
	if err != nil {
		t.Fatalf("NewToolCatalog() error = %v", err)
	}
	modules := map[string]int{}
	for _, spec := range catalog {
		modules[spec.Module]++
		var schema struct {
			Properties map[string]struct {
				Description string `json:"description"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(spec.InputSchema, &schema); err != nil {
			t.Fatalf("tool %s schema: %v", spec.Name, err)
		}
		for field, prop := range schema.Properties {
			if prop.Description == "" {
				t.Errorf("tool %s field %s has no description; run `make gen` after editing proto comments", spec.Name, field)
			}
		}
	}
	for _, module := range []string{"cmdb", "notify", "oncall", "system"} {
		if modules[module] == 0 {
			t.Errorf("module %s has no tools: %v", module, modules)
		}
	}
	byName := map[string]string{}
	for _, spec := range catalog {
		byName[spec.Name] = string(spec.InputSchema)
	}
	// 注释开头重复的字段名被去掉；枚举取值与约束保留。
	if schema := byName["OncallScheduleService_SendScheduleNotification"]; !strings.Contains(schema, `"description":"通知类型：1=上班提醒 2=每日播报。"`) {
		t.Errorf("SendScheduleNotification schema lacks the kind description: %s", schema)
	}
	if schema := byName["OncallScheduleService_CreateOverride"]; !strings.Contains(schema, "交接时刻") || !strings.Contains(schema, "RFC3339") {
		t.Errorf("CreateOverride schema lacks the time constraints: %s", schema)
	}
}
