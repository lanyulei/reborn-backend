package biz_test

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 一次编辑同时换分组并改通知 / 附加信息等字段时，变更历史记成 update 而不是 move（monitor 模块评审
// core-monitor-rules P3-4）：内容确实变了，此前审计上看起来只是一次移动。
func TestRule_UpdateMovingAndEditingIsRecordedAsUpdate(t *testing.T) {
	t.Parallel()
	e := newP2EnvD(t)
	ctx := context.Background()
	rule := e.mustRule(t, uAlice, spec(gHost, "cpu"))
	latest := func() string {
		t.Helper()
		versions, _, err := e.rules.ListVersions(ctx, uAlice, rule.ID, 1, 1)
		if err != nil || len(versions) != 1 {
			t.Fatalf("ListVersions: %v", err)
		}
		return versions[0].Action
	}
	moveTo := func(groupID string, mutate func(s *biz.RuleSpec)) *biz.RuleView {
		t.Helper()
		cur, err := e.rules.Get(ctx, uAlice, rule.ID)
		if err != nil {
			t.Fatal(err)
		}
		next := cur.Spec
		next.GroupID = groupID
		if mutate != nil {
			mutate(&next)
		}
		v, err := e.rules.Update(ctx, uAlice, rule.ID, next, cur.Version, "")
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		return v
	}

	// 单纯换分组：move。
	moveTo(gDB, nil)
	if got := latest(); got != mc.RuleActionMove {
		t.Fatalf("a pure move is recorded as %q", got)
	}
	// 换分组 + 改一项别的：update。
	for name, mutate := range map[string]func(s *biz.RuleSpec){
		"notify users":     func(s *biz.RuleSpec) { s.Notify.UserIDs = []string{uBob} },
		"notify recovery":  func(s *biz.RuleSpec) { s.NotifyRecovery = false },
		"repeat interval":  func(s *biz.RuleSpec) { s.RepeatIntervalM = 30 },
		"max notify":       func(s *biz.RuleSpec) { s.MaxNotify = 10 },
		"annotations":      func(s *biz.RuleSpec) { s.Annotations = map[string]string{"summary": "磁盘 {{ $value }}"} },
		"recovery expr":    func(s *biz.RuleSpec) { s.RecoveryExpr = "up" },
		"effective window": func(s *biz.RuleSpec) { s.Effective.Timezone = "Asia/Shanghai" },
		"topology inhibit": func(s *biz.RuleSpec) { s.TopologyInhibit = true },
		"escalation":       func(s *biz.RuleSpec) { s.EscalationID = escInfra },
		"ai summary":       func(s *biz.RuleSpec) { s.Actions.AISummary = true },
	} {
		// 换分组的同时改了一项别的：update。
		moveTo(gHost, mutate)
		if got := latest(); got != mc.RuleActionUpdate {
			t.Errorf("moving while changing %s is recorded as %q, want update", name, got)
		}
		// 只换分组：move（之前几轮留下的改动不影响这一次的判定）。
		moveTo(gDB, nil)
		if got := latest(); got != mc.RuleActionMove {
			t.Errorf("a pure move after changing %s is recorded as %q, want move", name, got)
		}
	}
}
