package migrations

import (
	"io/fs"
	"os"
	"regexp"
	"strings"
	"testing"
)

// createTablePattern 匹配建表语句的表名（含分区子表与 IF NOT EXISTS 形式）。
var createTablePattern = regexp.MustCompile(`(?i)\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-z_][a-z0-9_]*)`)

// TestTableNamePrefixMatchesAPIDomain 锁死表命名契约（AGENTS.md §1）：
// 每张表的前缀必须是 api/<domain> 的域名（system_ / notify_ / assistant_ …），
// 新增域或新增表不符合规则在此失败。域名清单直接读取 api/ 目录，无需维护常量。
func TestTableNamePrefixMatchesAPIDomain(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir("../api")
	if err != nil {
		t.Fatalf("read api dir: %v", err)
	}
	domains := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			domains = append(domains, e.Name()+"_")
		}
	}
	if len(domains) == 0 {
		t.Fatal("no api domains found")
	}
	tables := 0
	err = fs.WalkDir(FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".up.sql") {
			return err
		}
		raw, err := fs.ReadFile(FS, path)
		if err != nil {
			return err
		}
		for _, m := range createTablePattern.FindAllStringSubmatch(string(raw), -1) {
			tables++
			name := strings.ToLower(m[1])
			ok := false
			for _, prefix := range domains {
				if strings.HasPrefix(name, prefix) {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("%s: table %q must be prefixed by its api domain (one of %v)", path, name, domains)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk migrations: %v", err)
	}
	if tables < 30 {
		t.Fatalf("expected to scan existing tables, found %d", tables)
	}
}
