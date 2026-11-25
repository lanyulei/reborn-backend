package data

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 启动检查只留痕：有仍在用预置初始口令的启用超管 → 一条 WARN（带用户名，不带口令 / 哈希）；
// 没有 → 不出声；查询失败 → 也只是一条 WARN，不向外抛。
func TestWarnPresetPasswordAdmins(t *testing.T) {
	t.Parallel()
	run := func(names []string, err error) string {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		warnPresetPasswordAdmins(context.Background(), logger, func(context.Context) ([]string, error) { return names, err })
		return buf.String()
	}
	out := run([]string{"admin"}, nil)
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "admin") || strings.Count(out, "\n") != 1 {
		t.Fatalf("log = %q, want exactly one WARN naming the account", out)
	}
	if strings.Contains(out, biz.PresetAdminPasswordHash) || strings.Contains(out, "argon2") {
		t.Fatalf("log leaks the password hash: %q", out)
	}
	if out := run(nil, nil); out != "" {
		t.Fatalf("log = %q, want silence when no account is on the preset password", out)
	}
	if out := run(nil, errors.New("db down")); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "db down") {
		t.Fatalf("log = %q, want a WARN for the failed check", out)
	}
}

// 没有连接池（单测里的空 Data）时构造仓储不触发检查、不 panic。
func TestNewUserRepoWithoutPoolSkipsPresetCheck(t *testing.T) {
	t.Parallel()
	if NewUserRepo(nil) == nil || NewUserRepo(&basedata.Data{}) == nil {
		t.Fatal("NewUserRepo() = nil")
	}
}
