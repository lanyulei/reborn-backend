//go:build integration

package data

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 评审第二轮 K5 / R6：评估引擎把「结果不完整」写在 monitor_eval_record.detail（monitorcontract.EvalDetail
// 的 JSON，查询上的 partial / truncated）与 monitor_eval_task.last_error（"incomplete result: " 前缀，
// 状态仍是评估结果）；core 从真实的 jsonb 列读回同一份结构。
func TestEvalRepo_ReadsIncompleteResult(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group("", "team-incomplete")
	ds := e.datasource("vm-incomplete", true)
	r := e.rule(g.ID, "many-series")
	const note = biz.IncompletePrefix + "query 0 truncated to 10000 series and partial; series not returned were left unchanged"
	// 与引擎同一条路径：EvalDetail 经 encoding/json 写入 jsonb。
	detail, err := json.Marshal(mc.EvalDetail{
		Queries: []mc.EvalQueryDetail{
			{Index: 0, Severity: 3, Series: 10000, Truncated: true, Partial: true},
			{Index: 1, Severity: 2, Series: 3, Partial: true},
			{Index: 2, Severity: 1, Series: 1},
		},
		Points: []mc.EvalPoint{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, status, error, series, anomalies, detail)
		 VALUES ($1, $2, $3, now(), $4, $5, 10004, 1, $6)`, newID(t), r.ID, ds.ID, mc.EvalFiring, note, detail); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx,
		"INSERT INTO monitor_eval_task (rule_id, datasource_id, last_status, last_error, firing_count) VALUES ($1, $2, $3, $4, 1)",
		r.ID, ds.ID, mc.EvalFiring, note); err != nil {
		t.Fatal(err)
	}

	since := time.Now().Add(-time.Hour)
	recs, total, err := e.evals.ListRecords(ctx, biz.EvalRecordFilter{RuleID: r.ID, Start: &since}, 1, 10)
	if err != nil || total != 1 || len(recs) != 1 || recs[0].Error != note || recs[0].Status != mc.EvalFiring || len(recs[0].Detail.Queries) != 3 {
		t.Fatalf("records = %+v total=%d err=%v", recs, total, err)
	}
	q := recs[0].Detail.Queries
	if !q[0].Partial || !q[0].Truncated || !q[1].Partial || q[1].Truncated || q[2].Partial || q[2].Truncated {
		t.Fatalf("partial / truncated flags read back = %+v", q)
	}
	for i, want := range []string{biz.QueryPartialNote, biz.QueryPartialNote, ""} {
		if got := biz.QueryNote(&q[i]); got != want {
			t.Errorf("query %d: note = %q, want %q", i, got, want)
		}
	}
	tasks, err := e.evals.ListTasks(ctx, []string{r.ID})
	if err != nil || len(tasks[r.ID]) != 1 || tasks[r.ID][0].LastStatus != mc.EvalFiring || tasks[r.ID][0].LastError != note {
		t.Fatalf("tasks = %+v err=%v", tasks[r.ID], err)
	}
}
