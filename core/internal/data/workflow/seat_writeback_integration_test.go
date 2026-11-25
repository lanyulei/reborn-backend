//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// 会签任务上处理一次只回写变化的席位：变化的席位落库为新值，未变化的席位行不被重写（行版本 xmin
// 不变）且内容仍是原值；转办新增的席位整行插入。随后的命令在重新读出的状态上照常推进到流程结束
// ——库内状态与「整张任务回写」时一致。
func TestIntegration_SeatWritebackOnlyChanged(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("countersign", `{"nodes":[{"id":"start","type":"start"},
		{"id":"ops","type":"userTask","name":"运维会签","assignee":{"mode":"all","rules":[{"kind":"user","userIds":["`+uOps1+`","`+uOps2+`","`+uLead+`"]}]}},
		{"id":"end","type":"end"}],
		"edges":[{"id":"e1","source":"start","target":"ops"},{"id":"e2","source":"ops","target":"end"}],"settings":{}}`)
	in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "countersign", Title: "会签", Initiator: uInit})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	type seat struct {
		xmin    string
		status  int32
		comment string
		acted   bool
	}
	seats := func() map[string]seat {
		t.Helper()
		rows, err := e.d.Pool.Query(ctx,
			`SELECT user_id::text, xmin::text, status, comment, action_time IS NOT NULL
			 FROM workflow_task_assignee WHERE instance_id = $1::uuid`, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]seat{}
		for rows.Next() {
			var user string
			var s seat
			if err := rows.Scan(&user, &s.xmin, &s.status, &s.comment, &s.acted); err != nil {
				t.Fatal(err)
			}
			out[user] = s
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := seats()
	if len(before) != 3 {
		t.Fatalf("seats after start = %+v, want 3", before)
	}
	taskID := e.todo(uOps1)[0].TaskID

	if _, err := e.rt.Approve(ctx, taskID, uOps1, "同意"); err != nil {
		t.Fatalf("Approve(ops1) error = %v", err)
	}
	after := seats()
	if s := after[uOps1]; s.status != biz.AssigneeApproved || s.comment != "同意" || !s.acted || s.xmin == before[uOps1].xmin {
		t.Fatalf("approved seat = %+v (before %+v)", s, before[uOps1])
	}
	for _, u := range []string{uOps2, uLead} {
		if after[u] != before[u] {
			t.Fatalf("untouched seat of %s rewritten: before %+v, after %+v", u, before[u], after[u])
		}
	}

	// 转办：原席位改写、新席位插入，第三个席位仍不动。
	if _, err := e.rt.Transfer(ctx, taskID, uOps2, uBoss, "出差"); err != nil {
		t.Fatalf("Transfer() error = %v", err)
	}
	moved := seats()
	if len(moved) != 4 || moved[uOps2].status != biz.AssigneeTransferred || moved[uBoss].status != biz.AssigneePending {
		t.Fatalf("seats after transfer = %+v", moved)
	}
	if moved[uLead] != before[uLead] || moved[uOps1] != after[uOps1] {
		t.Fatalf("untouched seats rewritten by transfer: %+v", moved)
	}

	for _, u := range []string{uBoss, uLead} {
		if _, err := e.rt.Approve(ctx, taskID, u, ""); err != nil {
			t.Fatalf("Approve(%s) error = %v", u, err)
		}
	}
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE id = $1::uuid AND status = $2", in.ID, biz.InstanceCompleted); n != 1 {
		t.Fatalf("instance not completed after all seats signed; seats = %+v", seats())
	}
	if n := e.count("SELECT count(*) FROM workflow_task_assignee WHERE instance_id = $1::uuid AND status = $2", in.ID, biz.AssigneeApproved); n != 3 {
		t.Fatalf("approved seats = %d, want 3", n)
	}
}
