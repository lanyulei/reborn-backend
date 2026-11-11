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
