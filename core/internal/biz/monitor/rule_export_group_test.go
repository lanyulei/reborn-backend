package biz_test

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

// 导出时分组不存在（含已删除）明确报错，而不是回一份 0 条规则的空文件（monitor 模块评审
// core-monitor-rules P3-2）：超级管理员对任意 ID 的级别都是「可管理」，此前能通过可读判定。
func TestRule_ExportOfAMissingGroupIsRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	rule := e.mustRule(t, uAlice, spec(gHost, "cpu"))

	for _, format := range []string{"", biz.ExportJSON} {
		for _, c := range []struct {
			what  string
			ids   []string
			group string
		}{
			{"by group", nil, "no-such-group"},
			{"by ids within the group", []string{rule.ID}, "no-such-group"},
		} {
			_, n, err := e.rules.Export(ctx, uAdmin, c.ids, c.group, format)
			wantErr(t, c.what+" ("+format+")", err, biz.ErrGroupNotFound)
			if n != 0 {
				t.Fatalf("%s: count = %d, want 0 alongside the error", c.what, n)
			}
		}
	}
	// 无权查看的分组仍按无权限处理，不泄露它是否存在（与新建 / 移动 / 导入同一口径）。
	_, _, err := e.rules.Export(ctx, uCarol, nil, "no-such-group", "")
	wantErr(t, "unreadable missing group", err, biz.ErrForbidden)

	// 刚被删除的分组同样 404。
	if err := e.groups.Delete(ctx, uAdmin, gDB); err != nil {
		t.Fatalf("delete the empty group: %v", err)
	}
	_, _, err = e.rules.Export(ctx, uAdmin, nil, gDB, "")
	wantErr(t, "deleted group", err, biz.ErrGroupNotFound)

	// 现存的分组照常导出。
	if _, n, err := e.rules.Export(ctx, uAdmin, nil, gHost, ""); err != nil || n != 1 {
		t.Fatalf("export of an existing group = %d, %v; want 1", n, err)
	}
}
