package service

import (
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/kit/httpx"
)

// 角色授权的数量上限必须容得下整个权限目录：上限是首个提交写下的（64 / 128 / 256），目录此后持续
// 增长（可授权接口已有六百多个），勾选较多权限的角色保存即 400，配不出宽权限的非超管角色。
func TestRolePermissionLimitsCoverCatalog(t *testing.T) {
	t.Parallel()
	var menus, elements []string
	for _, m := range biz.Catalog() {
		menus = append(menus, m.Code)
		for _, e := range m.Elements {
			elements = append(elements, e.Code)
		}
	}
	apis := make([]string, 0, 64)
	for _, op := range listOperations() {
		apis = append(apis, op.Code)
	}
	if err := httpx.ProtoValidate(&v1.SetRolePermissionsRequest{Id: "018f3e1a-0a10-7000-8000-000000000001", Menus: menus, Elements: elements, Apis: apis}); err != nil {
		t.Fatalf("a role granted the whole catalog (%d menus, %d elements, %d apis) must pass validation: %v",
			len(menus), len(elements), len(apis), err)
	}
}
