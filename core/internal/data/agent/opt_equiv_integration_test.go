//go:build integration

package data

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 优化项的等价性集成测试：改写后的语句与改写前的语句在同一份数据上给出同样的结果。

// 任务列表按故障 / Profile 过滤（core-agent 优化项 B）：与原来的 ::text 比较命中同样的行——规范写法命中，
// 大写 / 不带连字符 / 非法值仍然查不到且不报错。
func TestTaskListUUIDFilterMatchesTextCompare(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	repo := NewTaskRepo(d)
	txm := basedata.NewTxManager(d)
	now := time.Now()
	profileA, profileB := mustID(t), mustID(t)
	incidents := []string{mustID(t), mustID(t), mustID(t)}
	for i, inc := range incidents {
		task := &biz.Task{
			ID: mustID(t), Kind: biz.KindDiagnose, Source: biz.SourceIncident, IncidentID: inc, IncidentNumber: "INC-1", IncidentTitle: "t",
			Severity: 2, Reason: biz.ReasonOpened, Status: biz.TaskQueued, NextTime: now, QueuedTime: now, UpdateTime: now,
		}
		if err := txm.WithinTx(ctx, func(tx txrun.Tx) error { return repo.EnqueueTx(ctx, tx, task) }); err != nil {
			t.Fatalf("EnqueueTx: %v", err)
		}
		profile := profileA
		if i == 2 {
			profile = profileB
		}
		if _, err := d.Pool.Exec(ctx, "UPDATE agent_task SET profile_id = $2::uuid WHERE id = $1::uuid", task.ID, profile); err != nil {
			t.Fatalf("set profile: %v", err)
		}
	}
	legacy := func(col, v string) int64 {
		t.Helper()
		var n int64
		if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_task t WHERE t."+col+"::text = $1", v).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	inputs := func(id string) []string {
		return []string{id, strings.ToUpper(id), strings.ReplaceAll(id, "-", ""), "{" + id + "}", "not-a-uuid", mustID(t)}
	}
	for _, v := range inputs(profileA) {
		items, total, err := repo.List(ctx, biz.TaskFilter{ProfileID: v}, 1, 20)
		if err != nil {
			t.Fatalf("List(profile %q) error = %v", v, err)
		}
		if want := legacy("profile_id", v); total != want || int64(len(items)) != want {
			t.Fatalf("List(profile %q) = %d items, total %d; text comparison selects %d", v, len(items), total, want)
		}
	}
	for _, v := range inputs(incidents[0]) {
		items, total, err := repo.List(ctx, biz.TaskFilter{IncidentID: v}, 1, 20)
		if err != nil {
			t.Fatalf("List(incident %q) error = %v", v, err)
		}
		if want := legacy("incident_id", v); total != want || int64(len(items)) != want {
			t.Fatalf("List(incident %q) = %d items, total %d; text comparison selects %d", v, len(items), total, want)
		}
	}
	// 两个条件一起、再叠加状态过滤。
	if _, total, err := repo.List(ctx, biz.TaskFilter{ProfileID: profileA, IncidentID: incidents[1], Status: biz.TaskQueued}, 1, 20); err != nil || total != 1 {
		t.Fatalf("List(profile + incident + status) total = %d, %v", total, err)
	}
	if _, total, err := repo.List(ctx, biz.TaskFilter{ProfileID: profileB, IncidentID: incidents[1]}, 1, 20); err != nil || total != 0 {
		t.Fatalf("List(mismatched profile + incident) total = %d, %v", total, err)
	}
}

// ListByIDs（core-agent 优化项 B）：与原来的 p.id::text = ANY($1) 取到同样的 Profile；混入非规范写法时整体
// 退回文本比较，仍不报错。
func TestProfileListByIDsMatchesTextCompare(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	repo := NewProfileRepo(d)
	a, b := sampleProfile(t, "a"), sampleProfile(t, "b")
	for _, p := range []*biz.Profile{a, b} {
		if err := repo.Create(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	cases := [][]string{
		{a.ID},
		{a.ID, b.ID},
		{a.ID, a.ID},
		{mustID(t)},
		{a.ID, mustID(t)},
		{strings.ToUpper(a.ID)},
		{a.ID, strings.ToUpper(b.ID)},
		{a.ID, "bogus"},
		{""},
		{},
	}
	for _, ids := range cases {
		var want []string
		rows, err := d.Pool.Query(ctx, "SELECT p.id::text FROM agent_profile p WHERE p.id::text = ANY($1) ORDER BY 1", ids)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			want = append(want, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		got, err := repo.ListByIDs(ctx, ids)
		if err != nil {
			t.Fatalf("ListByIDs(%v) error = %v", ids, err)
		}
		var gotIDs []string
		for _, p := range got {
			gotIDs = append(gotIDs, p.ID)
		}
		slices.Sort(gotIDs)
		if !slices.Equal(gotIDs, want) {
			t.Fatalf("ListByIDs(%v) = %v, text comparison selects %v", ids, gotIDs, want)
		}
	}
}

// RecentAutoOutcome 的证据连接（core-agent 优化项 C）：改写后的连接条件与原来的 e = 'agent_run:' || r.id::text
// 对各种写法的证据逐条给出同样的判定；再端到端核对总数 / 失败数。
func TestRecentAutoOutcomeEvidenceJoinMatchesLegacy(t *testing.T) {
	d := newIntegrationAgentData(t)
	ctx := context.Background()
	runs := NewRunRepo(d)
	profile, other := mustID(t), mustID(t)
	newRun := func(profileID string) string {
		t.Helper()
		start := time.Now().Add(-time.Minute)
		run := &biz.Run{
			ID: mustID(t), TaskID: mustID(t), ProfileID: profileID, IncidentID: mustID(t), AgentUserID: mustID(t), ConversationID: mustID(t),
			Status: biz.RunRunning, Stages: []biz.StageTrace{{Key: biz.StageCollect, Status: biz.StagePending}}, StartTime: start, CreateTime: start,
		}
		if err := runs.Create(ctx, run); err != nil {
			t.Fatalf("Create run: %v", err)
		}
		return run.ID
	}
	mine, theirs := newRun(profile), newRun(other)
	evidence := []string{
		"agent_run:" + mine, "agent_run:" + theirs, "agent_run:" + mustID(t),
		"agent_run:" + strings.ToUpper(mine), "agent_run:" + strings.ReplaceAll(mine, "-", ""), "agent_run:{" + mine + "}",
		"agent_run:" + mine + " ", " agent_run:" + mine, "agent_run:" + mine + "\n", "agent_run:", "agent_run:not-a-uuid",
		"alert:" + mine, mine, "",
	}
	rows, err := d.Pool.Query(ctx,
		`SELECT e,
		        EXISTS (SELECT 1 FROM agent_run r WHERE e = 'agent_run:' || r.id::text AND r.profile_id = $1::uuid),
		        EXISTS (SELECT 1 FROM agent_run r
		                WHERE r.id = CASE WHEN e ~ '`+agentRunEvidenceRE+`' THEN substring(e FROM 11)::uuid END AND r.profile_id = $1::uuid)
		 FROM unnest($2::text[]) AS e`, profile, evidence)
	if err != nil {
		t.Fatalf("compare join predicates: %v", err)
	}
	hits := 0
	for rows.Next() {
		var e string
		var legacy, rewritten bool
		if err := rows.Scan(&e, &legacy, &rewritten); err != nil {
			t.Fatal(err)
		}
		if legacy != rewritten {
			t.Errorf("evidence %q: legacy join = %v, rewritten join = %v", e, legacy, rewritten)
		}
		if rewritten {
			hits++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if hits != 1 {
		t.Fatalf("exactly the canonical evidence of this profile's run must join, got %d", hits)
	}

	// 端到端：自动批准、已出结果的建议里，证据指向本 Profile 运行的才计入；失败 = 状态 6 或验证结果变差。
	incident := mustID(t)
	if _, err := d.Pool.Exec(ctx,
		`INSERT INTO incident_record (id, number, title, severity, status, occur_time) VALUES ($1::uuid, $2::text, 'title', 2, 1, now())`,
		incident, "INC-OPT-"+incident[len(incident)-8:]); err != nil {
		t.Fatalf("seed incident: %v", err)
	}
	t.Cleanup(func() {
		_, _ = d.Pool.Exec(context.Background(), "DELETE FROM incident_proposal WHERE incident_id = $1::uuid", incident)
		_, _ = d.Pool.Exec(context.Background(), "DELETE FROM incident_record WHERE id = $1::uuid", incident)
	})
	seeds := []struct {
		status   int
		auto     bool
		proposer int
		evidence string
		verify   string
	}{
		{5, true, 3, `["agent_run:` + mine + `"]`, `{"worse": false}`},           // 计入，成功
		{6, true, 3, `["note", "agent_run:` + mine + `"]`, `null`},               // 计入，失败
		{5, true, 3, `["agent_run:` + mine + `"]`, `{"worse": true}`},            // 计入，验证变差 = 失败
		{5, true, 3, `["agent_run:` + theirs + `"]`, `null`},                     // 别的 Profile 的运行
		{5, true, 3, `["agent_run:` + strings.ToUpper(mine) + `", "x"]`, `null`}, // 非规范写法：不命中、不报错
		{5, true, 3, `[]`, `null`},                                               // 没有证据
		{5, false, 3, `["agent_run:` + mine + `"]`, `null`},                      // 非自动批准
		{5, true, 1, `["agent_run:` + mine + `"]`, `null`},                       // 不是 Agent 提出的
		{4, true, 3, `["agent_run:` + mine + `"]`, `null`},                       // 还没出结果
	}
	for i, s := range seeds {
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO incident_proposal (id, incident_id, title, action, idem_key, status, auto, proposer_type, evidence, verify_result, expire_time)
			 VALUES ($1::uuid, $2::uuid, 'step', '{}'::jsonb, $3::text, $4::smallint, $5::boolean, $6::smallint, $7::jsonb, NULLIF($8::jsonb, 'null'::jsonb),
			         now() + interval '1 hour')`,
			mustID(t), incident, "idem-"+string(rune('a'+i)), s.status, s.auto, s.proposer, s.evidence, s.verify); err != nil {
			t.Fatalf("seed proposal %d: %v", i, err)
		}
	}
	var wantTotal, wantFailed int
	if err := d.Pool.QueryRow(ctx,
		`SELECT count(*)::int, count(*) FILTER (WHERE p.status = 6 OR (p.verify_result ->> 'worse')::boolean)::int
		 FROM incident_proposal p
		 WHERE p.proposer_type = 3 AND p.auto AND p.status IN (5, 6) AND p.create_time >= now() - make_interval(secs => $2::float8)
		   AND jsonb_typeof(p.evidence) = 'array'
		   AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(p.evidence) e JOIN agent_run r ON e = 'agent_run:' || r.id::text WHERE r.profile_id = $1)`,
		profile, (24*time.Hour).Seconds()).Scan(&wantTotal, &wantFailed); err != nil {
		t.Fatalf("legacy statement: %v", err)
	}
	total, failed, err := runs.RecentAutoOutcome(ctx, profile, 24*time.Hour)
	if err != nil {
		t.Fatalf("RecentAutoOutcome: %v", err)
	}
	if total != wantTotal || failed != wantFailed || total != 3 || failed != 2 {
		t.Fatalf("RecentAutoOutcome = %d / %d, legacy statement = %d / %d, want 3 / 2", total, failed, wantTotal, wantFailed)
	}
}
