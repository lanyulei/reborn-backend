package biz_test

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 批量操作不回显数据范围外规则的名称（monitor 模块评审 core-monitor-rules P3-1）：对调用方完全不可读的
// 分组里的规则，此前结果里带着 `{"id":"…","name":"cpu-high","ok":false,"reason":"MONITOR_FORBIDDEN"}`。
func TestRule_BatchHidesNamesOfRulesOutsideTheDataScope(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// bob 对 db 分组可管理、对 host 分组只读、对 app 分组没有任何授权。
	e.store.SeedRoleScope(rInfraRead, gDB, mc.ScopeManage)
	own := e.mustRule(t, uAlice, spec(gDB, "own"))
	readOnly := e.mustRule(t, uAlice, spec(gHost, "read-only-rule"))
	hidden := e.mustRule(t, uCarol, spec(gApp, "secret-name"))

	res, err := e.rules.Batch(ctx, uBob, biz.BatchInput{IDs: []string{own.ID, readOnly.ID, hidden.ID, "no-such-rule"}, Action: biz.BatchDisable})
	if err != nil || len(res) != 4 {
		t.Fatalf("Batch = %v, %v", res, err)
	}
	// 自己的：名称照常回显。
	if !res[0].OK || res[0].Name != "own" {
		t.Errorf("own rule: %+v", res[0])
	}
	// 只读分组里的：无权修改，但名称可见（调用方本来就读得到这条规则）。
	if res[1].OK || res[1].Reason != biz.ErrForbidden.Reason || res[1].Name != "read-only-rule" {
		t.Errorf("read-only rule: %+v", res[1])
	}
	// 完全不可读的分组里的：只回报无权限，名称留空。
	if res[2].OK || res[2].Reason != biz.ErrForbidden.Reason || res[2].Name != "" {
		t.Errorf("rule outside the data scope leaks its name: %+v", res[2])
	}
	if res[3].OK || res[3].Reason != biz.ErrRuleNotFound.Reason || res[3].Name != "" {
		t.Errorf("missing rule: %+v", res[3])
	}
	if v, _ := e.rules.Get(ctx, uCarol, hidden.ID); v.Version != 1 {
		t.Fatal("the rule outside the data scope must not be modified")
	}
}
