package biz

import (
	"slices"
	"testing"
)

// TestCatalog_ValidateGrantCodes 目录校验：已登记的菜单/元素码通过，
// 未登记码（含格式合法但不在目录内、元素码误作菜单码）拒绝。
func TestCatalog_ValidateGrantCodes(t *testing.T) {
	t.Parallel()
	if err := ValidateGrantCodes([]string{"system.user", "system.role"}, nil); err != nil {
		t.Fatalf("ValidateGrantCodes(known menus) error = %v", err)
	}
	if err := ValidateGrantCodes(nil, []string{"system.user.create", "system.role.grant"}); err != nil {
		t.Fatalf("ValidateGrantCodes(known elements) error = %v", err)
	}
	for _, bad := range []struct {
		name     string
		menus    []string
		elements []string
	}{
		{"unknown menu", []string{"system.nope"}, nil},
		{"unknown element", nil, []string{"system.user.nope"}},
		{"element as menu", []string{"system.user.create"}, nil},
		{"menu as element", nil, []string{"system.user"}},
	} {
		if err := ValidateGrantCodes(bad.menus, bad.elements); err == nil {
			t.Errorf("ValidateGrantCodes(%s) error = nil, want rejection", bad.name)
		}
	}
}

// TestCatalog_ReturnsCopy Catalog 返回深拷贝：改写返回值不得污染内置注册表。
func TestCatalog_ReturnsCopy(t *testing.T) {
	t.Parallel()
	menus := Catalog()
	if len(menus) == 0 {
		t.Fatal("Catalog() is empty")
	}
	menus[0].Name = "tampered"
	menus[0].DefaultOps[0] = "/tampered"
	menus[0].Elements[0].Code = "tampered"

	fresh := Catalog()
	for _, menu := range fresh {
		if menu.Name == "tampered" || slices.Contains(menu.DefaultOps, "/tampered") {
			t.Fatalf("Catalog() leaked internal state: %+v", menu)
		}
		for _, el := range menu.Elements {
			if el.Code == "tampered" {
				t.Fatalf("Catalog() leaked internal element state: %+v", el)
			}
		}
	}
	// 校验不受改写影响（索引读的是注册表，非拷贝）。
	if err := ValidateGrantCodes([]string{fresh[0].Code}, nil); err != nil {
		t.Fatalf("ValidateGrantCodes after tampering error = %v", err)
	}
}
