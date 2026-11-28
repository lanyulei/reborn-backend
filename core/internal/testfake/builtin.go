package testfake

import (
	"os"
	"path/filepath"
	"testing"
)

// BuiltinSeedSQL 内置流程与工单模板的迁移原文（core/migrations/0095）。集成测试的夹具每次都会清空流程 /
// 工单表，验证内置内容时把这份迁移重新执行一遍——它本来就可以重复执行。文件改名或改号时这里会直接报错。
func BuiltinSeedSQL(t testing.TB) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "0095_builtin_flows_tickets.up.sql"))
	if err != nil {
		t.Fatalf("read the built-in seed migration: %v", err)
	}
	return string(raw)
}
