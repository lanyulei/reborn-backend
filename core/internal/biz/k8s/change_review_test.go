package biz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"

	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// submit 提一张变更单（工单 ticket-1 挂在其上），返回变更单 ID。
func submit(t *testing.T, f *changeFakes, planes *PlaneUsecase) string {
	t.Helper()
	_, err := planes.Call(context.Background(), alice, "c1", kc.OpScale, kc.Empty{})
	id, _, _, ok := Submitted(err)
	if !ok {
		t.Fatalf("want K8S_CHANGE_SUBMITTED, got %v", err)
	}
	return id
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// finishedTickets 提单后复查时报告「流程已通过」的工单查看适配器。
type finishedTickets struct{}

func (finishedTickets) CanViewTicket(context.Context, string, string) (bool, error) {
	return false, nil
}

func (finishedTickets) TicketFinished(context.Context, string, string) (int32, bool, error) {
	return TicketApproved, true, nil
}

type notesRecorder struct{ comments *[]string }

func (n notesRecorder) Comment(_ context.Context, ticketID, _, content string) error {
	*n.comments = append(*n.comments, ticketID+": "+content)
	return nil
}

// 流程以「通过」结束 ≠ 有人审批过。流程引擎对解析不出处理人的审批节点是自动跳过的（角色暂时没有成员、
// 部门没有负责人……），跳过之后流程照样以通过结束：这时变更单不能被批准，更不能以发起人身份执行。
func TestChangeResolveNeedsHumanApproval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("有人批准：照常批准", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		// 另一个审批环节被跳过也无妨：只要有人点过同意。
		f.evidence = ApprovalEvidence{Skipped: 1, Approved: 1}
		_, planes, syncer := newChangeUC(f)
		id := submit(t, f, planes)
		if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err != nil {
			t.Fatal(err)
		}
		if c := f.changes[id]; c.Status != kc.ChangeApproved || c.Message != "" || f.requester[id] == nil || f.requester[id].RoleIDs[0] != "r1" {
			t.Fatalf("change = %+v requester = %+v", c, f.requester[id])
		}
	})

	t.Run("审批环节全部被跳过：不批准、不执行", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		f.evidence = ApprovalEvidence{Skipped: 2}
		_, planes, syncer := newChangeUC(f)
		id := submit(t, f, planes)
		if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err != nil {
			t.Fatal(err)
		}
		c := f.changes[id]
		if c.Status != kc.ChangeRejected || c.Message != msgUnreviewed {
			t.Fatalf("change = %+v, want rejected with the reason", c)
		}
		if f.requester[id] != nil || f.ttl[id] != 0 {
			t.Fatalf("a rejected change must not carry an execution identity: %+v", f.requester[id])
		}
		// 没有任何东西可执行：后台循环领不到它，也不会发「已执行」的通知或评论。
		events, comments := &fakeEvents{}, &[]string{}
		w := NewChangeWorker(f, f, fakeScopeRepo{f}, fakeSelfRoles{}, notesRecorder{comments}, events, f, f, f, fakeTxm{}, "", discardLog())
		if n, err := w.Run(ctx); err != nil || n != 0 {
			t.Fatalf("Run() = %d, %v", n, err)
		}
		if len(events.events) != 0 || len(*comments) != 0 || f.changes[id].Status != kc.ChangeRejected {
			t.Fatalf("a rejected change must stay rejected and must not be executed: events = %d comments = %v status = %d",
				len(events.events), *comments, f.changes[id].Status)
		}
	})

	t.Run("流程本来没有审批环节：不算被跳过，维持原行为", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		_, planes, syncer := newChangeUC(f)
		id := submit(t, f, planes)
		if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err != nil {
			t.Fatal(err)
		}
		if f.changes[id].Status != kc.ChangeApproved {
			t.Fatalf("a pass without any skipped approval must not be rejected: %+v", f.changes[id])
		}
	})

	t.Run("没有证据来源：无法证明有人审批过，不批准", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		f.noProof = true
		_, planes, syncer := newChangeUC(f)
		id := submit(t, f, planes)
		if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err != nil {
			t.Fatal(err)
		}
		if f.changes[id].Status != kc.ChangeRejected {
			t.Fatalf("without an evidence source a pass must not be approved: %+v", f.changes[id])
		}
	})

	t.Run("证据读不出来：整个命令失败，变更单原地不动", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		f.proofErr = errors.New("database is down")
		_, planes, syncer := newChangeUC(f)
		id := submit(t, f, planes)
		if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err == nil || !strings.Contains(err.Error(), "database is down") {
			t.Fatalf("Resolve() error = %v, want the evidence error", err)
		}
		if c := f.changes[id]; c.Status != kc.ChangePending {
			t.Fatalf("change = %+v, want still pending (the engine command rolls back)", c)
		}
	})

	t.Run("发起时即结束的分支不受影响", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		// 发起时不可能有人审批：不论证据如何，一律按未通过处理（原有行为）。
		f.ticketStatus, f.evidence = TicketApproved, ApprovalEvidence{Approved: 3}
		_, planes, _ := newChangeUC(f)
		id := submit(t, f, planes)
		if c := f.changes[id]; c.Status != kc.ChangeRejected || !strings.Contains(c.Message, "未经任何人审批") {
			t.Fatalf("change = %+v, want the at-start rejection", c)
		}
	})

	t.Run("提单与挂接之间流程走完：同样要有人审批过", func(t *testing.T) {
		t.Parallel()
		f := newChangeFakes()
		f.settings[SettingNamespaceTemplate] = "tpl"
		f.evidence = ApprovalEvidence{Skipped: 1}
		planes := NewPlaneUsecase(fakeChangePlane{f}, f, f, f, fakeChangeClusters{f}, nil)
		syncer := NewChangeSyncer(f, f)
		uc := NewChangeUsecase(NewChangeDeps(f, f, f, fakeSettings{f}, fakeChangeClusters{f}, fakeChangePlane{f}, f, fakeChangeTickets{f}, f, f, finishedTickets{}, fakeTxm{}),
			planes, &ClusterUsecase{}, syncer, "https://ops.example.com/", discardLog())
		err := uc.RequestNamespace(ctx, alice, "c1", kc.NamespaceRequest{Name: "team-a"}, "")
		id, _, _, ok := Submitted(err)
		if !ok {
			t.Fatalf("want K8S_CHANGE_SUBMITTED, got %v", err)
		}
		if c := f.changes[id]; c.Status != kc.ChangeRejected || c.Message != msgUnreviewed {
			t.Fatalf("change = %+v, want rejected with the reason", c)
		}
	})
}

// 变更单列表的分页参数与本域其余列表同一口径：超大页码夹到上限，不会让 OFFSET 溢出成负数被 PG 拒绝成 500。
func TestChangeListNormalizesPaging(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	f.admin = true
	uc, _, _ := newChangeUC(f)
	if _, _, err := uc.List(ctx, alice, false, ChangeFilter{}, math.MaxInt64, 100); err != nil {
		t.Fatal(err)
	}
	if f.listPage != paging.MaxPage || f.listSize != paging.MaxPageSize {
		t.Fatalf("page, pageSize = %d, %d; want %d, %d", f.listPage, f.listSize, paging.MaxPage, paging.MaxPageSize)
	}
	if _, _, err := uc.List(ctx, alice, true, ChangeFilter{}, 0, 0); err != nil {
		t.Fatal(err)
	}
	if f.listPage != 1 || f.listSize != paging.DefaultPageSize {
		t.Fatalf("page, pageSize = %d, %d; want 1, %d", f.listPage, f.listSize, paging.DefaultPageSize)
	}
}
