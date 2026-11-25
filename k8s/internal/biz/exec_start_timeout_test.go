package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// deadlineExec 记下会话记录写入（Start）拿到的上下文截止时间；fail 时写入失败。
type deadlineExec struct {
	memExec
	fail      bool
	deadline  time.Time
	hasDeadln bool
}

func (d *deadlineExec) Start(ctx context.Context, s *ExecSession) error {
	deadline, ok := ctx.Deadline()
	d.mu.Lock()
	d.deadline, d.hasDeadln = deadline, ok
	d.mu.Unlock()
	if d.fail {
		return errors.New("db down")
	}
	return d.memExec.Start(ctx, s)
}

// 会话记录的写入有时限：数据库卡住时接入不能一直等——会话已登记却没有管理循环（空闲 / 最长时长靠它收尾）。
// 写入失败只记日志，终端照常可用、照常收尾。
func TestExecSessionStartHasDeadline(t *testing.T) {
	t.Parallel()
	c1 := &Cluster{ID: "c1", Name: "c1"}
	m, _, _, audits, _ := newExecEnv(t, c1)
	repo := &deadlineExec{}
	m.repo = repo
	alice := Identity{UserID: "alice"}
	params := k8scontract.ExecParams{Namespace: "ns", Pod: "p"}

	first, err := m.Attach(context.Background(), "c1", alice, params, (&client{}).sink)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	repo.mu.Lock()
	deadline, has := repo.deadline, repo.hasDeadln
	repo.mu.Unlock()
	wait := time.Until(deadline)
	if !has || wait <= 0 || wait > 5*time.Second {
		t.Fatalf("Start must get a deadline within 5 seconds: has=%v wait=%v", has, wait)
	}

	// 写入失败：接入照样成功，会话能在客户端关闭后正常结束（记录了结束原因与一条审计）。
	repo.mu.Lock()
	repo.fail = true
	repo.mu.Unlock()
	h, err := m.Attach(context.Background(), "c1", alice, params, (&client{}).sink)
	if err != nil {
		t.Fatal(err)
	}
	h.Close()
	select {
	case <-h.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the session must still be managed and end on close")
	}
	if got := repo.reason(h.SessionID()); got != k8scontract.ExecEndClientClose {
		t.Fatalf("end reason = %s", got)
	}
	if n := len(audits.rows); n != 2 || audits.rows[1].Result != k8scontract.AuditOK {
		t.Fatalf("exec audits = %+v", audits.rows)
	}
}
