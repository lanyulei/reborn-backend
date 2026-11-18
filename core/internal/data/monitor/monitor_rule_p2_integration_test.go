//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// p2Spec 带齐第二、三期各列的规则内容。
func p2Spec(groupID, name, escalationID string) biz.RuleSpec {
	s := ruleSpec(groupID, name)
	s.Relabel = []mc.RelabelStep{
		{Action: mc.RelabelReplace, SourceLabels: []string{"instance"}, Regex: `(.+):\d+`, TargetLabel: "host"},
		{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"},
	}
	s.DropExpr = `{{ if eq $labels.ci_env "dev" }}true{{ end }}`
	s.CIMatch = mc.CIMatch{Label: "instance", By: mc.CIMatchByAttr, Attr: "ip", ModelCodes: []string{"host"}, EnrichTags: []string{"core"}}
	s.ResourceScope = mc.ResourceScope{Enabled: true, Envs: []string{"prod"}}
	s.TopologyInhibit, s.EscalationID = true, escalationID
	s.Effective = mc.Effective{Mode: mc.EffectiveModeWorkTime}
	s.Notify.CIOwner = true
	s.Actions = mc.RuleActions{
		Ticket:   mc.TicketAction{Enabled: true, TemplateID: p2TemplateID, AgentUserID: uAlice},
		Workflow: mc.WorkflowAction{Enabled: true, DefinitionKey: "restart", AgentUserID: uBob}, AISummary: true,
	}
	return s
}

// p2TemplateID 测试用的工单模板 ID（监控域只存 ID，不建外键）。
const p2TemplateID = "018f3e1a-0d09-7000-8000-000000000001"

// seedEscalation 建一条升级策略：规则的写事务会确认新引用的策略存在（与策略的删除互斥）。
func seedEscalation(t *testing.T, e *env, groupID string) string {
	t.Helper()
	p := &biz.Escalation{ID: newID(t), Spec: biz.EscalationSpec{
		GroupID: groupID, Name: "p2-" + newID(t)[24:], Mode: mc.EscalationModeStack, MinSeverity: 1, Enabled: true,
		Levels: []mc.EscalationLevel{{Trigger: mc.EscalationTriggerUnacked, AfterM: 10, Targets: mc.EscalationTargets{UserIDs: []string{uAlice}}}},
	}}
	if err := NewEscalationRepo(e.d).Create(context.Background(), p, uAdmin); err != nil {
		t.Fatalf("seed escalation: %v", err)
	}
	return p.ID
}

// 规则的第二、三期各列：写入、读回、版本快照、整体替换为零值、按升级策略过滤。
func TestRuleRepo_P2Columns(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(mc.RootGroupID, "p2")
	escID := seedEscalation(t, e, g.ID)
	r := &biz.Rule{ID: newID(t), Spec: p2Spec(g.ID, "full", escID)}
	if err := e.rules.Create(ctx, r, biz.RuleChange{Action: mc.RuleActionCreate, Summary: "新建", Operator: uAlice}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	check := func(what string, got, want *biz.RuleSpec) {
		t.Helper()
		if !slices.EqualFunc(got.Relabel, want.Relabel, func(a, b mc.RelabelStep) bool {
			return a.Action == b.Action && slices.Equal(a.SourceLabels, b.SourceLabels) && a.Regex == b.Regex && a.TargetLabel == b.TargetLabel
		}) || got.DropExpr != want.DropExpr || got.CIMatch.Label != want.CIMatch.Label || got.CIMatch.By != want.CIMatch.By ||
			got.CIMatch.Attr != want.CIMatch.Attr || !slices.Equal(got.CIMatch.ModelCodes, want.CIMatch.ModelCodes) ||
			!slices.Equal(got.CIMatch.EnrichTags, want.CIMatch.EnrichTags) || got.ResourceScope.Enabled != want.ResourceScope.Enabled ||
			!slices.Equal(got.ResourceScope.Envs, want.ResourceScope.Envs) || got.TopologyInhibit != want.TopologyInhibit ||
			got.EscalationID != want.EscalationID || got.Actions != want.Actions || got.Effective.Mode != want.Effective.Mode ||
			got.Notify.CIOwner != want.Notify.CIOwner {
			t.Fatalf("%s: got %+v\nwant %+v", what, got, want)
		}
	}
	got, err := e.rules.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	check("Get", &got.Spec, &r.Spec)
	snap, err := e.vers.Get(ctx, r.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	check("version snapshot", snap.Spec, &r.Spec)

	// 落库形状与引擎的契约一致（引擎直接读这些列）。
	var relabelType, actions, ciMatch string
	if err := e.pool.QueryRow(ctx, `SELECT jsonb_typeof(relabel), actions::text, ci_match::text FROM monitor_rule WHERE id = $1`, r.ID).
		Scan(&relabelType, &actions, &ciMatch); err != nil {
		t.Fatal(err)
	}
	if relabelType != "array" || e.count(`SELECT count(*) FROM monitor_rule WHERE id = $1 AND actions->'ticket'->>'agent_user_id' = $2
		AND actions->'workflow'->>'definition_key' = 'restart' AND (actions->>'ai_summary')::bool AND ci_match->>'by' = 'attr'
		AND resource_scope->'envs' = '["prod"]' AND effective->>'mode' = 'work_time' AND (notify->>'ci_owner')::bool
		AND relabel->0->>'target_label' = 'host' AND escalation_id = $3 AND topology_inhibit`, r.ID, uAlice, escID) != 1 {
		t.Fatalf("stored shape: relabel %s actions %s ci_match %s", relabelType, actions, ciMatch)
	}

	// 整体替换为零值：列回到缺省（relabel 是空数组不是 null，escalation_id 为 NULL）。
	plain := ruleSpec(g.ID, "full")
	got.Spec = plain
	if err := e.rules.Update(ctx, got, got.Version, biz.RuleChange{Action: mc.RuleActionUpdate, Summary: "清空", Operator: uAlice}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if e.count(`SELECT count(*) FROM monitor_rule WHERE id = $1 AND relabel = '[]' AND drop_expr = '' AND NOT topology_inhibit
		AND escalation_id IS NULL AND NOT (effective ? 'mode') AND NOT (notify ? 'ci_owner')`, r.ID) != 1 {
		t.Fatal("zero-valued P2 fields were not written back as defaults")
	}
	back, err := e.rules.Get(ctx, r.ID)
	if err != nil || back.Spec.EscalationID != "" || len(back.Spec.Relabel) != 0 || back.Spec.Actions != (mc.RuleActions{}) {
		t.Fatalf("after reset: %v %+v", err, back.Spec)
	}

	// 第一期写下的行（各列为缺省值）照常读得出来。
	old := e.rule(g.ID, "p1 row")
	if p1, err := e.rules.Get(ctx, old.ID); err != nil || p1.Spec.EscalationID != "" || p1.Spec.CIMatch.Label != "" {
		t.Fatalf("P1 row: %v %+v", err, p1)
	}

	// 按升级策略过滤。
	other := &biz.Rule{ID: newID(t), Spec: p2Spec(g.ID, "other", escID)}
	if err := e.rules.Create(ctx, other, biz.RuleChange{Action: mc.RuleActionCreate, Summary: "新建", Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	items, total, err := e.rules.List(ctx, biz.RuleFilter{Groups: biz.GroupFilter{All: true}, EscalationID: escID}, 1, 10)
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != other.ID {
		t.Fatalf("List(escalation): %v total %d", err, total)
	}
	if _, total, err = e.rules.List(ctx, biz.RuleFilter{Groups: biz.GroupFilter{All: true}, EscalationID: newID(t)}, 1, 10); err != nil || total != 0 {
		t.Fatalf("List(unknown escalation): %v total %d", err, total)
	}
	var plan string
	rows, err := e.pool.Query(ctx, `EXPLAIN SELECT id FROM monitor_rule r WHERE r.delete_time IS NULL AND r.escalation_id = $1::uuid`, escID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	rows.Close()
	t.Logf("EXPLAIN rule list by escalation_id:\n%s", plan)
}

// ci_envs 候选项：只取有效资源上不同的非空环境，有上限。
func TestCIEnvRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, "TRUNCATE TABLE monitor_ci_index"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = e.pool.Exec(context.Background(), "TRUNCATE TABLE monitor_ci_index") })
	for i, row := range []struct {
		env   string
		valid bool
	}{{"prod", true}, {"prod", true}, {"dev", true}, {"", true}, {"retired-env", false}, {"test", true}} {
		if _, err := e.pool.Exec(ctx, `INSERT INTO monitor_ci_index (ci_id, model_code, name, env, valid) VALUES ($1, 'host', $2, $3, $4)`,
			newID(t), fmt.Sprintf("ci-%d", i), row.env, row.valid); err != nil {
			t.Fatal(err)
		}
	}
	repo := NewCIEnvRepo(e.d)
	envs, err := repo.ListEnvs(ctx, 200)
	if err != nil || !slices.Equal(envs, []string{"dev", "prod", "test"}) {
		t.Fatalf("ListEnvs = %v, %v", envs, err)
	}
	if envs, err = repo.ListEnvs(ctx, 2); err != nil || !slices.Equal(envs, []string{"dev", "prod"}) {
		t.Fatalf("ListEnvs(limit 2) = %v, %v", envs, err)
	}
}

// 规则的第二、三期写路径与候选项查询在只属于 monitor_core_role 的登录账号下可执行。
func TestRuleP2_AsCoreLoginRole(t *testing.T) {
	admin := newEnv(t)
	ctx := context.Background()
	g := admin.group(mc.RootGroupID, "login")
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	// 标识符与口令都是本函数生成的十六进制串，不含需要转义的字符；口令不落日志。
	role, password := "monp2_d_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
	if _, err := admin.pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s' IN ROLE monitor_core_role", role, password)); err != nil {
		t.Fatalf("create login role: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("drop login role %s: %v", role, err)
		}
	})
	cfg := admin.pool.Config().ConnConfig
	d := openData(t, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, role, password, cfg.Database))
	var current string
	var super bool
	if err := d.Pool.QueryRow(ctx, "SELECT current_user::text, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)").Scan(&current, &super); err != nil || current != role || super {
		t.Fatalf("connected as %s super=%v err=%v", current, super, err)
	}
	limited := reposOf(t, d)
	escID := seedEscalation(t, admin, g.ID)
	r := &biz.Rule{ID: newID(t), Spec: p2Spec(g.ID, "as login", escID)}
	if err := limited.rules.Create(ctx, r, biz.RuleChange{Action: mc.RuleActionCreate, Summary: "新建", Operator: uAlice}); err != nil {
		t.Fatalf("Create as login role: %v", err)
	}
	r.Spec.Relabel, r.Spec.EscalationID = nil, ""
	if err := limited.rules.Update(ctx, r, r.Version, biz.RuleChange{Action: mc.RuleActionUpdate, Summary: "编辑", Operator: uAlice}); err != nil {
		t.Fatalf("Update as login role: %v", err)
	}
	if _, _, err := limited.rules.List(ctx, biz.RuleFilter{Groups: biz.GroupFilter{All: true}, EscalationID: escID}, 1, 10); err != nil {
		t.Fatalf("List as login role: %v", err)
	}
	if _, err := NewCIEnvRepo(d).ListEnvs(ctx, 10); err != nil {
		t.Fatalf("ListEnvs as login role: %v", err)
	}
	if err := limited.rules.Delete(ctx, r, biz.RuleChange{Action: mc.RuleActionDelete, Summary: "删除", Operator: uAlice}); err != nil {
		t.Fatalf("Delete as login role: %v", err)
	}
}
