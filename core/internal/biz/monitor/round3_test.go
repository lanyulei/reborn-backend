package biz_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 评审第二轮（docs/monitor-review-round2.md）core 部分的回归。

// K1：同时给 group_id 与 ids 时，所选规则不能因为分组里的规则多于导出上限而被丢掉。修复前先按分组
// 取前 MaxExportRules+1 条再挑 ids：排在后面的所选规则不在这一批里，导出 0 条且不报错。
func TestRule_ExportSelectedRulesBeyondGroupLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	for i := range biz.MaxExportRules + 2 {
		e.mustRule(t, uAdmin, spec(gHost, fmt.Sprintf("bulk-%04d", i)))
	}
	// 按（分组, 名称）排序时排在最后。
	last := e.mustRule(t, uAdmin, spec(gHost, "zz-selected"))
	inDB := e.mustRule(t, uAdmin, spec(gDB, "db-selected"))
	inApp := e.mustRule(t, uAdmin, spec(gApp, "app-selected"))

	for _, format := range []string{biz.ExportYAML, biz.ExportJSON} {
		content, n, err := e.rules.Export(ctx, uAlice, []string{last.ID}, gHost, format)
		if err != nil || n != 1 || !strings.Contains(content, "zz-selected") || strings.Contains(content, "bulk-") {
			t.Fatalf("export(%s) selected rule in a %d-rule group: n=%d err=%v", format, biz.MaxExportRules+3, n, err)
		}
	}
	// 上级分组同样（含下级）。
	if _, n, err := e.rules.Export(ctx, uAlice, []string{last.ID, inDB.ID}, gInfra, ""); err != nil || n != 2 {
		t.Fatalf("export ids within the parent group: n=%d err=%v", n, err)
	}
	// 交集：不在所给分组（含下级）里的规则不导出；范围外的规则不导出。
	if content, n, err := e.rules.Export(ctx, uAlice, []string{last.ID, inDB.ID}, gHost, ""); err != nil || n != 1 || strings.Contains(content, "db-selected") {
		t.Fatalf("export ids ∩ group must drop rules of other groups: n=%d err=%v", n, err)
	}
	if _, n, err := e.rules.Export(ctx, uAdmin, []string{inApp.ID}, gInfra, ""); err != nil || n != 0 {
		t.Fatalf("a rule outside the given group must not be exported: n=%d err=%v", n, err)
	}
	if _, n, err := e.rules.Export(ctx, uAlice, []string{last.ID, inApp.ID}, "", ""); err != nil || n != 1 {
		t.Fatalf("export by ids keeps only readable rules: n=%d err=%v", n, err)
	}
	// 对分组没有范围：给了 ids 也一样 403。
	_, _, err := e.rules.Export(ctx, uCarol, []string{last.ID}, gHost, "")
	wantErr(t, "export ids in a group out of scope", err, biz.ErrForbidden)
	// 只给分组、规则数超过上限：明确报错，不截断。
	_, _, err = e.rules.Export(ctx, uAlice, nil, gHost, "")
	wantErr(t, "group over the export limit", err, biz.ErrRuleInvalid)
	// ids 超过上限：带不带分组都拒绝。
	tooMany := make([]string, biz.MaxBatchRules+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("id-%d", i)
	}
	for _, group := range []string{"", gHost} {
		_, _, err = e.rules.Export(ctx, uAlice, tooMany, group, "")
		wantErr(t, "too many ids", err, biz.ErrRuleInvalid)
	}
}

// K2：「通知过我的」可以含范围外的事件；这些事件只给收件人视图——不带屏蔽规则（与 Get 一致）。
// 修复前列表原样返回 MuteID / MuteName。
func TestAlert_MineHidesMuteOutsideScope(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	ctx := context.Background()
	mute, err := d.mutes.Create(ctx, uAlice, onceMute(gHost))
	if err != nil {
		t.Fatal(err)
	}
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	// 通知之后事件被屏蔽规则命中（引擎写入）。
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.Muted, a.MuteID = true, mute.ID })

	one := func(user string, q biz.AlertListQuery) *biz.AlertView {
		t.Helper()
		views, total, err := d.alerts.List(ctx, user, q, 1, 10)
		if err != nil || total != 1 || len(views) != 1 || views[0].ID != d.alert {
			t.Fatalf("list for %s = %d items (total %d) err=%v", user, len(views), total, err)
		}
		return views[0]
	}
	mine := biz.AlertListQuery{Mine: true}
	// carol 是接收方（部门负责人），对 host 没有范围。
	for _, q := range []biz.AlertListQuery{mine, {Mine: true, GroupID: gInfra}} {
		if v := one(uCarol, q); v.MuteID != "" || v.MuteName != "" {
			t.Fatalf("recipient outside the scope sees the mute rule in the list: id=%q name=%q", v.MuteID, v.MuteName)
		} else if !v.Muted || v.GroupName == "" {
			t.Fatalf("the recipient view keeps the alert itself: %+v", v)
		}
	}
	// 与详情同一口径。
	detail, err := d.alerts.Get(ctx, uCarol, d.alert)
	if err != nil || detail.Alert.MuteID != "" || detail.Alert.MuteName != "" {
		t.Fatalf("recipient detail = %+v err=%v", detail, err)
	}
	// 在范围内的接收方（bob 对 infra 只读）照常看到屏蔽规则，带不带 mine 都一样。
	for _, q := range []biz.AlertListQuery{mine, {}} {
		if v := one(uBob, q); v.MuteID != mute.ID || v.MuteName != "变更窗口" {
			t.Fatalf("in-scope recipient: mute = %q %q", v.MuteID, v.MuteName)
		}
	}
	// 仓储里的事件没有被改动。
	if a := d.store.Alert(d.alert); a.MuteID != mute.ID {
		t.Fatalf("stored alert mute id = %q", a.MuteID)
	}
}

// K5：规则列表 / 详情在「结果不完整」时带上说明。此时状态是评估结果（不是查询错误），修复前
// lastError 为空。
func TestRule_LastErrorCarriesIncompleteNote(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	const note = biz.IncompletePrefix + "query 0 truncated to 10000 series; series not returned were left unchanged"
	if biz.IncompletePrefix != "incomplete result: " {
		t.Fatalf("IncompletePrefix = %q: the literal is a contract with the engine and the frontend", biz.IncompletePrefix)
	}
	lastError := func(id string) (string, int32) {
		t.Helper()
		items, _, err := e.rules.List(ctx, uAlice, biz.RuleListQuery{}, 1, 50)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range items {
			if r.ID == id {
				got, err := e.rules.Get(ctx, uAlice, id)
				if err != nil || got.LastError != r.LastError || got.Status != r.Status {
					t.Fatalf("Get() = %q/%d err=%v, list = %q/%d", got.LastError, got.Status, err, r.LastError, r.Status)
				}
				return r.LastError, r.Status
			}
		}
		t.Fatalf("rule %s not listed", id)
		return "", 0
	}
	for name, c := range map[string]struct {
		tasks      []biz.EvalTask
		wantErr    string
		wantStatus int32
	}{
		"firing with a truncated result": {[]biz.EvalTask{{LastStatus: mc.EvalFiring, LastError: note}}, note, mc.EvalFiring},
		"ok with a partial result": {
			[]biz.EvalTask{{LastStatus: mc.EvalOK, LastError: biz.IncompletePrefix + "query 1 returned a partial result; series not returned were left unchanged"}},
			biz.IncompletePrefix + "query 1 returned a partial result; series not returned were left unchanged", mc.EvalOK,
		},
		"no data with a partial result": {[]biz.EvalTask{{LastStatus: mc.EvalNoData, LastError: note}}, note, mc.EvalNoData},
		// 查询错误优先于「结果不完整」，与数据源的先后无关。
		"query error wins": {
			[]biz.EvalTask{{LastStatus: mc.EvalFiring, LastError: note}, {LastStatus: mc.EvalError, LastError: "query 0: timeout"}}, "query 0: timeout", mc.EvalFiring,
		},
		"complete result":     {[]biz.EvalTask{{LastStatus: mc.EvalFiring}}, "", mc.EvalFiring},
		"one of two complete": {[]biz.EvalTask{{LastStatus: mc.EvalOK}, {LastStatus: mc.EvalOK, LastError: note}}, note, mc.EvalOK},
		// 非错误状态下只认约定的前缀：其他残留文字不当作错误展示。
		"unrelated text on a healthy task": {[]biz.EvalTask{{LastStatus: mc.EvalOK, LastError: "stale"}}, "", mc.EvalOK},
	} {
		r := e.mustRule(t, uAlice, spec(gHost, name))
		for i, task := range c.tasks {
			task.RuleID, task.DatasourceID = r.ID, fmt.Sprintf("ds-%d", i)
			e.store.SeedEvalTask(&task)
		}
		if got, status := lastError(r.ID); got != c.wantErr || status != c.wantStatus {
			t.Errorf("%s: lastError = %q status = %d, want %q / %d", name, got, status, c.wantErr, c.wantStatus)
		}
	}
	// 停用的规则不汇总。
	off := spec(gHost, "disabled")
	off.Enabled = false
	r := e.mustRule(t, uAlice, off)
	e.store.SeedEvalTask(&biz.EvalTask{RuleID: r.ID, DatasourceID: dsVM, LastStatus: mc.EvalFiring, LastError: note})
	if got, status := lastError(r.ID); got != "" || status != mc.EvalNever {
		t.Errorf("disabled rule: lastError = %q status = %d", got, status)
	}
}

// K5：判定记录里一条查询的「部分结果」并入该查询的 error（接口没有单独的字段）。
func TestQueryNote(t *testing.T) {
	t.Parallel()
	if biz.QueryPartialNote != "incomplete result: partial response from the datasource" {
		t.Fatalf("QueryPartialNote = %q: the literal is a contract with the frontend", biz.QueryPartialNote)
	}
	for name, c := range map[string]struct {
		q    mc.EvalQueryDetail
		want string
	}{
		"complete":              {mc.EvalQueryDetail{Series: 3}, ""},
		"partial":               {mc.EvalQueryDetail{Series: 3, Partial: true}, biz.QueryPartialNote},
		"truncated only":        {mc.EvalQueryDetail{Series: 3, Truncated: true}, ""},
		"truncated and partial": {mc.EvalQueryDetail{Series: 3, Truncated: true, Partial: true}, biz.QueryPartialNote},
		"failed":                {mc.EvalQueryDetail{Error: "timeout"}, "timeout"},
		"failed keeps its text": {mc.EvalQueryDetail{Error: "timeout", Partial: true}, "timeout"},
	} {
		if got := biz.QueryNote(&c.q); got != c.want {
			t.Errorf("%s: QueryNote() = %q, want %q", name, got, c.want)
		}
	}
}
