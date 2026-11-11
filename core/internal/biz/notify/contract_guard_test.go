package biz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// TestProtoPatternsMatchContract proto 文件里重复出现的事件类型/模式正则字面量
// 必须与 kit/notifycontract 的常量一致（proto 无法引用 Go 常量，靠本测试守护）。
func TestProtoPatternsMatchContract(t *testing.T) {
	t.Parallel()
	dir := filepath.Join("..", "..", "..", "api", "notify", "v1")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	// proto 文本里反斜杠写两次。
	esc := func(s string) string { return strings.ReplaceAll(s, `\`, `\\`) }
	allowed := map[string]bool{
		esc(notifycontract.EventTypePatternLiteral):         true,
		esc(notifycontract.EventPatternPatternLiteral):      true,
		esc(notifycontract.TemplateEventTypePatternLiteral): true,
	}
	found := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".proto") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // 测试读取仓库内 proto 文件
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			i := strings.Index(line, `pattern: "`)
			if i < 0 || !strings.Contains(line, "a-z0-9._-") {
				continue
			}
			lit := line[i+len(`pattern: "`):]
			lit = lit[:strings.Index(lit, `"`)]
			found++
			if !allowed[lit] {
				t.Errorf("%s: proto pattern %q is not one of the notifycontract literals", e.Name(), lit)
			}
		}
	}
	if found < 8 {
		t.Fatalf("expected to find event patterns in protos, found %d", found)
	}
}
