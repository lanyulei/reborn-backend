package biz

import (
	"context"
	"maps"
	"testing"
)

// seatStore 模拟 workflow_task_assignee 表的 upsert 语义：新席位整行插入，已有席位只更新
// status / comment / action_time / owner_id（数据层 writeState 的 DO UPDATE 列）。
type seatStore map[string]TaskAssignee

func (st seatStore) upsert(a *TaskAssignee) {
	row, ok := st[a.ID]
	if !ok {
		st[a.ID] = *a
		return
	}
	row.Status, row.Comment, row.ActionTime, row.OwnerID = a.Status, a.Comment, a.ActionTime, a.OwnerID
	st[a.ID] = row
}

// 只回写变化的席位与「变化的任务整张回写」落库结果相同：一个 12 人会签任务上依次同意、转办、
// 前加签、后加签、委派、改派、交接，每条命令之后把两种回写各自应用到一张模拟的席位表上，两张表
// 始终逐行相等；而变化的席位数远小于任务的席位总数（同意一次只写一个席位）。
func TestState_ChangedAssigneesEquivalentToFullRewrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	users := []string{"u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "u9", "u10", "u11", "u12"}
	h := start(t, linearGraph(task("a", "all", users...), task("b", "any", "v1")), nil, nil)
	full, delta := seatStore{}, seatStore{}
	// flush 把本条命令的回写分别按两种方式落到两张表上并比对；返回只写变化席位时写出的行数。
	flush := func(step string) int {
		t.Helper()
		written := 0
		for _, tk := range h.s.ChangedTasks() {
			for _, a := range tk.Assignees {
				full.upsert(a)
			}
			for _, a := range h.s.ChangedAssignees(tk) {
				delta.upsert(a)
				written++
			}
		}
		if !maps.Equal(full, delta) {
			t.Fatalf("%s: seat table differs between full rewrite and changed-only write", step)
		}
		return written
	}
	if n := flush("start"); n != len(users) {
		t.Fatalf("start wrote %d seats, want %d (all new)", n, len(users))
	}
	id, _ := h.pending("a")
	steps := []struct {
		name string
		run  func() error
		max  int // 该命令至多写出的席位数
	}{
		{"approve", func() error { return h.exec().Approve(ctx, id, "u1", "ok") }, 1},
		{"transfer", func() error { return h.exec().Transfer(ctx, id, "u2", "x1", "busy") }, 2},
		{"add-sign before", func() error { return h.exec().AddSign(ctx, id, "u3", []string{"x2", "x3"}, OriginBefore, "") }, 3},
		{"add-sign after", func() error { return h.exec().AddSign(ctx, id, "u4", []string{"x4"}, OriginAfter, "看过") }, 2},
		{"delegate", func() error { return h.exec().Delegate(ctx, id, "u5", "x5", "") }, 2},
		{"delegate opinion", func() error { return h.exec().Approve(ctx, id, "x5", "可以") }, 2},
		{"added approves", func() error { return h.exec().Approve(ctx, id, "x2", "") }, 2},
		// 交接：u3（被前加签挂起）的席位换给 x9，名下加签席位的归属随之改写。
		{"handover", func() error { _, err := h.exec().Handover(ctx, "u3", "x9", "admin", "调岗"); return err }, 4},
		{"approve again", func() error { return h.exec().Approve(ctx, id, "u6", "") }, 1},
	}
	for _, st := range steps {
		if err := st.run(); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if n := flush(st.name); n == 0 || n > st.max {
			t.Errorf("%s wrote %d seats, want 1..%d", st.name, n, st.max)
		}
	}
	// 改派：全部未了结的席位作废、换上新的处理人——变化的席位多，但两种回写依旧一致。
	if err := h.exec().Reassign(ctx, id, "admin", []string{"z1", "z2"}, "重排"); err != nil {
		t.Fatalf("reassign: %v", err)
	}
	flush("reassign")
	for _, u := range []string{"z1", "z2"} {
		if err := h.exec().Approve(ctx, id, u, ""); err != nil {
			t.Fatalf("approve by %s: %v", u, err)
		}
		flush("approve by " + u)
	}
	if got := h.pendingNodes(); len(got) != 1 || got[0] != "b" {
		t.Fatalf("pending nodes = %v, want [b]", got)
	}
	// 没有任务变化的命令不写任何席位；无基线的状态（未经 NewExecution）全部视为变化。
	h.exec()
	if n := flush("noop"); n != 0 {
		t.Errorf("no-op wrote %d seats", n)
	}
	fresh := &State{Tasks: h.s.Tasks}
	if got := fresh.ChangedAssignees(h.s.Tasks[0]); len(got) != len(h.s.Tasks[0].Assignees) {
		t.Errorf("state without baseline: %d changed seats, want all %d", len(got), len(h.s.Tasks[0].Assignees))
	}
}
