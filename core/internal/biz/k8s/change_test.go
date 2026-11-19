package biz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// changeFakes 变更审批用例的全部依赖（内存实现）。
type changeFakes struct {
	approval  ClusterApproval
	settings  map[string]string
	changes   map[string]*Change
	execs     map[string][]byte
	requester map[string]*kc.ChangeRequester
	ttl       map[string]time.Duration

	planCalls []string // 发给数据面的 op
	plan      kc.ChangePlanResponse
	planErr   error

	tickets      []ChangeTicket
	ticketErr    error
	ticketStatus int32 // 非 0：流程在发起时即结束
	cancelled    []string
	templateErr  error

	admin    bool
	roleIDs  []string
	grantNS  string
	inactive bool
	noDebug  bool
	// refreshed 变更单 ID → 最近一次刷新的发起人身份。
	refreshed map[string]*kc.ChangeRequester
	bypass    bool
	comments  []string
	scopes    map[string][]*RoleScope
	audits    []*AuditEntry
}

func newChangeFakes() *changeFakes {
	return &changeFakes{
		approval: ClusterApproval{Name: "prod-1", Enabled: true, TemplateID: "tpl"}, settings: map[string]string{},
		changes: map[string]*Change{}, execs: map[string][]byte{}, requester: map[string]*kc.ChangeRequester{}, ttl: map[string]time.Duration{},
		roleIDs: []string{"r1"}, scopes: map[string][]*RoleScope{},
		plan: kc.ChangePlanResponse{
			Display: kc.ChangeDisplay{
				Action: kc.ActionScale, Kind: "Deployment", Summary: "Deployment web：副本数 2 → 5",
				Target: kc.Ref{Group: "apps", Version: "v1", Resource: "deployments", Namespace: "a", Name: "web"},
				Items: []kc.ChangeItemDisplay{{
					Kind: "Deployment", Action: kc.ActionScale, Ref: kc.Ref{Namespace: "a", Name: "web"},
					Fields: []kc.ChangeField{{Path: "spec.replicas", Before: "2", After: "5"}},
				}},
			},
			Exec: kc.ChangeExec{Op: kc.OpScale},
		},
	}
}

func (f *changeFakes) Create(_ context.Context, c *Change, exec []byte) error {
	cp := *c
	f.changes[c.ID], f.execs[c.ID] = &cp, exec
	return nil
}

func (f *changeFakes) AttachTicket(_ context.Context, id, ticketID, number string) error {
	if ticketID == "" {
		delete(f.changes, id)
		return nil
	}
	f.changes[id].TicketID, f.changes[id].TicketNumber = ticketID, number
	return nil
}

func (f *changeFakes) Approval(context.Context, string) (*ClusterApproval, error) {
	a := f.approval
	return &a, nil
}

func (f *changeFakes) List(_ context.Context, filter ChangeFilter, _, _ int64) ([]*Change, int64, error) {
	var out []*Change
	for _, c := range f.changes {
		if filter.RequesterID != "" && c.RequesterID != filter.RequesterID {
			continue
		}
		if filter.ClusterIDs != nil && !contains(filter.ClusterIDs, c.ClusterID) {
			continue
		}
		if filter.Access != nil && c.RequesterID != filter.OrRequesterID && !filter.Access.AllowsObject(c.ClusterID, c.Namespace) {
			continue
		}
		out = append(out, c)
	}
	return out, int64(len(out)), nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (f *changeFakes) Get(_ context.Context, id string) (*Change, error) {
	if c, ok := f.changes[id]; ok {
		cp := *c
		return &cp, nil
	}
	return nil, ErrChangeNotFound
}

func (f *changeFakes) PendingByTicketTx(_ context.Context, _ txrun.Tx, ticketID string) (*Change, error) {
	for _, c := range f.changes {
		if c.TicketID == ticketID && c.Status == kc.ChangePending {
			return c, nil
		}
	}
	return nil, nil
}

func (f *changeFakes) ResolveTx(_ context.Context, _ txrun.Tx, id string, status int32, message string, requester *kc.ChangeRequester, ttl time.Duration) error {
	c := f.changes[id]
	if c.Status != kc.ChangePending {
		return nil
	}
	c.Status, c.Message = status, message
	f.requester[id], f.ttl[id] = requester, ttl
	return nil
}

func (f *changeFakes) Finish(_ context.Context, id string, status int32, message string) error {
	f.changes[id].Status, f.changes[id].Message = status, message
	return nil
}

func (f *changeFakes) ClaimPermission(context.Context) (*Change, error) {
	for _, c := range f.changes {
		if c.Kind == kc.ChangeKindPermission && c.Status == kc.ChangeApproved {
			c.Status = kc.ChangeExecuting
			return c, nil
		}
	}
	return nil, nil
}

func (f *changeFakes) Expire(context.Context) (int, error) { return 0, nil }

func (f *changeFakes) NoteTx(context.Context, txrun.Tx) (*Change, error) {
	for _, c := range f.changes {
		if kc.ChangeFinal(c.Status) && c.Namespace != "noted" {
			cp := *c
			c.Namespace = "noted"
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *changeFakes) Get0(context.Context) (map[string]string, error) { return f.settings, nil }

type fakeSettings struct{ f *changeFakes }

func (s fakeSettings) Get(context.Context) (map[string]string, error) { return s.f.settings, nil }
func (s fakeSettings) Set(_ context.Context, v map[string]string, _ string) error {
	for k, x := range v {
		s.f.settings[k] = x
	}
	return nil
}

type fakeChangePlane struct{ f *changeFakes }

func (p fakeChangePlane) Call(_ context.Context, _ kc.Identity, _, op string, req any) ([]byte, error) {
	p.f.planCalls = append(p.f.planCalls, op)
	if op == kc.OpChangePlan {
		if p.f.planErr != nil {
			return nil, p.f.planErr
		}
		return json.Marshal(p.f.plan)
	}
	return []byte(`{}`), nil
}

func (fakeChangePlane) Stream(context.Context, kc.Identity, string, string, any) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (fakeChangePlane) DialExec(context.Context, kc.Identity, string, kc.ExecParams) (ExecConn, error) {
	return nil, errors.New("unused")
}

type fakeChangeTickets struct{ f *changeFakes }

func (t fakeChangeTickets) CheckTemplate(context.Context, string) error { return t.f.templateErr }

func (t fakeChangeTickets) Create(_ context.Context, in ChangeTicket) (string, string, bool, int32, error) {
	if t.f.ticketErr != nil {
		return "", "", false, 0, t.f.ticketErr
	}
	t.f.tickets = append(t.f.tickets, in)
	return "ticket-1", "CHG-1", t.f.ticketStatus != 0, t.f.ticketStatus, nil
}

func (t fakeChangeTickets) Cancel(_ context.Context, ticketID, userID string) error {
	t.f.cancelled = append(t.f.cancelled, ticketID+"/"+userID)
	return nil
}

func (f *changeFakes) CanBypass(context.Context, string) (bool, error) { return f.bypass, nil }
func (f *changeFakes) CanDebug(context.Context, string) (bool, error)  { return !f.noDebug, nil }

// Approved implements ChangeRefresher.
func (f *changeFakes) Approved(context.Context, int) ([]ApprovedChange, error) {
	var out []ApprovedChange
	for _, c := range f.changes {
		if c.Status == kc.ChangeApproved {
			out = append(out, ApprovedChange{ID: c.ID, RequesterID: c.RequesterID})
		}
	}
	return out, nil
}

// RefreshRequester implements ChangeRefresher.
func (f *changeFakes) RefreshRequester(_ context.Context, id string, r *kc.ChangeRequester) error {
	f.refreshed[id] = r
	return nil
}

func (f *changeFakes) EnabledRoleIDs(context.Context, string) (bool, []string, error) {
	return f.admin, f.roleIDs, nil
}
func (f *changeFakes) Active(context.Context, string) (bool, error) { return !f.inactive, nil }

// Grants implements GrantSource：有角色的用户在 c1 上有 grantNS 的授权（缺省全部命名空间）。
func (f *changeFakes) Grants(_ context.Context, roleIDs []string) ([]*RoleScope, error) {
	if len(roleIDs) == 0 {
		return nil, nil
	}
	ns := f.grantNS
	if ns == "" {
		ns = kc.AllNamespaces
	}
	return []*RoleScope{{ClusterID: "c1", Namespace: ns, Level: kc.LevelEdit}}, nil
}

func (f *changeFakes) UserNames(_ context.Context, ids []string) (map[string]string, error) {
	return map[string]string{ids[0]: "alice"}, nil
}
func (f *changeFakes) Seal(p string) ([]byte, error) { return []byte("sealed:" + p), nil }

type fakeChangeClusters struct{ f *changeFakes }

func (fakeChangeClusters) List(context.Context, ClusterFilter, int64, int64) ([]*Cluster, int64, error) {
	return nil, 0, nil
}

func (fakeChangeClusters) Get(context.Context, string) (*Cluster, error) {
	return &Cluster{Enabled: true}, nil
}

func (c fakeChangeClusters) Options(_ context.Context, all bool, _ []string) ([]*ClusterOption, error) {
	if all {
		return []*ClusterOption{{ID: "c1"}, {ID: "c2"}}, nil
	}
	return []*ClusterOption{{ID: "c1"}}, nil
}

type fakeSelfRoles struct{}

func (fakeSelfRoles) Ensure(_ context.Context, userID, _ string) (string, error) {
	return "self-" + userID, nil
}

type fakeNotes struct{ f *changeFakes }

func (n fakeNotes) Comment(_ context.Context, ticketID, _, content string) error {
	n.f.comments = append(n.f.comments, ticketID+": "+content)
	return nil
}

type fakeScopeRepo struct{ f *changeFakes }

func (s fakeScopeRepo) Get(_ context.Context, roleID string) ([]*RoleScope, error) {
	return s.f.scopes[roleID], nil
}

func (s fakeScopeRepo) Replace(_ context.Context, roleID string, items []*RoleScope, a *AuditEntry) error {
	s.f.scopes[roleID] = items
	s.f.audits = append(s.f.audits, a)
	return nil
}

func newChangeUC(f *changeFakes) (*ChangeUsecase, *PlaneUsecase, *ChangeSyncer) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	plane := fakeChangePlane{f}
	planes := NewPlaneUsecase(plane, f, f, f, fakeChangeClusters{f}, nil)
	NewAccessResolver(f, f, planes)
	clusterUC := &ClusterUsecase{}
	syncer := NewChangeSyncer(f, f)
	uc := NewChangeUsecase(NewChangeDeps(f, f, f, fakeSettings{f}, fakeChangeClusters{f}, plane, f, fakeChangeTickets{f}, f, f, nil, fakeTxm{}),
		planes, clusterUC, syncer, "https://ops.example.com/", log)
	return uc, planes, syncer
}

var alice = Caller{UserID: "018f3e1a-0000-7000-8000-000000000001", ClientIP: "10.0.0.1"}

func TestChangeGateSubmitsInsteadOfExecuting(t *testing.T) {
	t.Parallel()
	f := newChangeFakes()
	_, planes, syncer := newChangeUC(f)
	_, err := planes.Call(context.Background(), alice, "c1", kc.OpScale, kc.ScaleRequest{Replicas: 5})
	changeID, ticketID, number, ok := Submitted(err)
	if !ok || changeID == "" || ticketID != "ticket-1" || number != "CHG-1" {
		t.Fatalf("want K8S_CHANGE_SUBMITTED, got %v", err)
	}
	if e := kerrors.FromError(err); e.Code != 428 || e.Reason != v1.ErrorReason_K8S_CHANGE_SUBMITTED.String() {
		t.Fatalf("error = %+v", e)
	}
	// 只向数据面要了计划：写操作本身没有发出去。
	if strings.Join(f.planCalls, ",") != kc.OpChangePlan {
		t.Fatalf("plane calls = %v", f.planCalls)
	}
	c := f.changes[changeID]
	if c.Status != kc.ChangePending || c.RequesterID != alice.UserID || c.RequesterName != "alice" || c.Name != "web" || c.Namespace != "a" || c.Op != kc.OpScale {
		t.Fatalf("change = %+v", c)
	}
	if !strings.HasPrefix(string(f.execs[changeID]), "sealed:") {
		t.Fatal("the execution plan must be sealed")
	}
	tk := f.tickets[0]
	if tk.Creator != alice.UserID || tk.TemplateID != "tpl" || tk.RequestID != changeID ||
		!strings.Contains(tk.Fields[TicketFieldDetail], "spec.replicas: 2 → 5") || tk.Fields[TicketFieldLink] != "https://ops.example.com/k8s/change?id="+changeID {
		t.Fatalf("ticket = %+v", tk)
	}

	// 审批通过：记下发起人此刻的角色；有效期为缺省值。
	if err := syncer.Resolve(context.Background(), nil, "ticket-1", TicketApproved); err != nil {
		t.Fatal(err)
	}
	if c.Status != kc.ChangeApproved || f.requester[changeID].RoleIDs[0] != "r1" || f.requester[changeID].IsAdmin || f.ttl[changeID] != kc.DefaultChangeTTL {
		t.Fatalf("approved change = %+v requester = %+v ttl = %v", c, f.requester[changeID], f.ttl[changeID])
	}
	// 同一张工单再来一次结果：变更单已不是待审批，不再变化。
	_ = syncer.Resolve(context.Background(), nil, "ticket-1", TicketRejected)
	if c.Status != kc.ChangeApproved {
		t.Fatal("a resolved change must not be resolved again")
	}
}

func TestChangeGatePassThrough(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// 读操作、预览不拦截。
	f := newChangeFakes()
	_, planes, _ := newChangeUC(f)
	for _, op := range []string{kc.OpList, kc.OpGet, kc.OpPreview, kc.OpManifestPreview, kc.OpHelmPreview, kc.OpDrainPreview, kc.OpSecretReveal} {
		if _, err := planes.Call(ctx, alice, "c1", op, kc.Empty{}); err != nil {
			t.Errorf("%s must not be gated: %v", op, err)
		}
	}
	if len(f.changes) != 0 {
		t.Fatal("reads must not create changes")
	}
	// 每个写操作都拦截。
	for _, op := range []string{
		kc.OpApply, kc.OpDelete, kc.OpManifestApply, kc.OpScale, kc.OpRestart, kc.OpSetImage, kc.OpRollback, kc.OpPause,
		kc.OpCronTrigger, kc.OpCronSuspend, kc.OpPodEvict, kc.OpPodResize, kc.OpNodeCordon, kc.OpHelmInstall, kc.OpHelmUpgrade, kc.OpHelmRollback, kc.OpHelmUninstall,
	} {
		if _, err := planes.Call(ctx, alice, "c1", op, kc.Empty{}); kerrors.FromError(err).Reason != kc.ReasonChangeSubmitted {
			t.Errorf("%s must be gated: %v", op, err)
		}
	}
	if _, err := planes.Stream(ctx, alice, "c1", kc.StreamDrain, kc.DrainRequest{}); kerrors.FromError(err).Reason != kc.ReasonChangeSubmitted {
		t.Errorf("drain must be gated: %v", err)
	}
	if _, err := planes.Stream(ctx, alice, "c1", kc.StreamPodLog, kc.Empty{}); err != nil {
		t.Errorf("log stream must not be gated: %v", err)
	}
	// 集群未配置审批模板：直接执行。
	f2 := newChangeFakes()
	f2.approval.TemplateID = ""
	_, planes2, _ := newChangeUC(f2)
	if _, err := planes2.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{}); err != nil || strings.Join(f2.planCalls, ",") != kc.OpScale {
		t.Fatalf("no approval template: err = %v calls = %v", err, f2.planCalls)
	}
}

func TestChangeBypass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	emergency := alice
	emergency.Emergency = true
	// 没有免审批权限：请求免审批被拒绝，也不生成变更单。
	f := newChangeFakes()
	_, planes, _ := newChangeUC(f)
	if _, err := planes.Call(ctx, emergency, "c1", kc.OpScale, kc.Empty{}); !errors.Is(err, ErrBypassForbidden) {
		t.Fatalf("bypass without permission: %v", err)
	}
	if len(f.changes) != 0 || len(f.planCalls) != 0 {
		t.Fatal("a refused bypass must not reach the plane")
	}
	// 有权限：直接执行。
	f.bypass = true
	if _, err := planes.Call(ctx, emergency, "c1", kc.OpScale, kc.Empty{}); err != nil || strings.Join(f.planCalls, ",") != kc.OpScale {
		t.Fatalf("bypass with permission: err = %v calls = %v", err, f.planCalls)
	}
	// 有权限但没有请求免审批：照常走审批（免审批必须是明确的选择）。
	if _, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{}); kerrors.FromError(err).Reason != kc.ReasonChangeSubmitted {
		t.Fatalf("no emergency flag: %v", err)
	}
	// 超管恒可免审批。
	f3 := newChangeFakes()
	f3.admin = true
	_, planes3, _ := newChangeUC(f3)
	if _, err := planes3.Call(ctx, emergency, "c1", kc.OpScale, kc.Empty{}); err != nil {
		t.Fatalf("admin bypass: %v", err)
	}
}

func TestChangeSubmitFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// 计划失败（如无权限、预检被拒绝）：错误原样返回，不留变更单。
	f := newChangeFakes()
	f.planErr = kerrors.Forbidden(kc.ReasonForbidden, "no")
	_, planes, _ := newChangeUC(f)
	if _, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{}); kerrors.FromError(err).Reason != kc.ReasonForbidden || len(f.changes) != 0 {
		t.Fatalf("plan failure: %v changes = %d", err, len(f.changes))
	}
	// 提单失败：变更单被清掉。
	f.planErr, f.ticketErr = nil, ErrChangeTemplate
	if _, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{}); !errors.Is(err, ErrChangeTemplate) || len(f.changes) != 0 {
		t.Fatalf("ticket failure: %v changes = %d", err, len(f.changes))
	}
	// 流程在发起时即结束（没有审批节点）：没有任何人审批过，不算通过——变更单按未通过处理，不会执行。
	f.ticketErr, f.ticketStatus = nil, TicketApproved
	_, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{})
	id, _, _, ok := Submitted(err)
	if !ok || f.changes[id].Status != kc.ChangeRejected {
		t.Fatalf("a flow that finishes at start must not approve the change: %v status = %d", err, f.changes[id].Status)
	}
}

func TestChangeStateMachine(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	want := map[int32]int32{TicketApproved: kc.ChangeApproved, TicketRejected: kc.ChangeRejected, TicketCancelled: kc.ChangeCancelled, TicketTerminated: kc.ChangeRejected}
	for ticketStatus, changeStatus := range want {
		f := newChangeFakes()
		uc, planes, syncer := newChangeUC(f)
		_, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{})
		id, _, _, _ := Submitted(err)
		if err := syncer.Resolve(ctx, nil, "ticket-1", ticketStatus); err != nil {
			t.Fatal(err)
		}
		if f.changes[id].Status != changeStatus {
			t.Errorf("ticket status %d → change status %d, want %d", ticketStatus, f.changes[id].Status, changeStatus)
		}
		if changeStatus != kc.ChangeApproved && f.requester[id] != nil {
			t.Error("only approved changes get a requester identity")
		}
		// 已有结果的变更单不能撤回。
		if err := uc.Cancel(ctx, alice, id); !errors.Is(err, ErrChangeState) {
			t.Errorf("cancel after resolution: %v", err)
		}
	}
	// 没有对应变更单的工单：什么都不做。
	f := newChangeFakes()
	_, _, syncer := newChangeUC(f)
	if err := syncer.Resolve(ctx, nil, "some-other-ticket", TicketApproved); err != nil {
		t.Fatal(err)
	}
	if !kc.ChangeFinal(kc.ChangeExpired) || kc.ChangeFinal(kc.ChangeExecuting) || kc.ChangeFinal(kc.ChangeApproved) {
		t.Fatal("ChangeFinal is wrong")
	}
}

func TestChangeCancelAndVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	uc, planes, _ := newChangeUC(f)
	_, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{})
	id, _, _, _ := Submitted(err)
	bob := Caller{UserID: "018f3e1a-0000-7000-8000-000000000002"}
	// 别人不能撤回；对该集群有数据范围的人能看。
	if err := uc.Cancel(ctx, bob, id); !errors.Is(err, ErrChangeNotFound) {
		t.Fatalf("cancel by another user: %v", err)
	}
	if _, err := uc.Get(ctx, bob, id); err != nil {
		t.Fatalf("user with scope on the cluster must see the change: %v", err)
	}
	// 只有别的命名空间的授权：看不到（变更单里有对象内容）；发起人本人始终能看。
	f.grantNS = "some-other-namespace"
	if _, err := uc.Get(ctx, bob, id); !errors.Is(err, ErrChangeNotFound) {
		t.Fatalf("user scoped to another namespace must not see the change: %v", err)
	}
	if _, err := uc.Get(ctx, alice, id); err != nil {
		t.Fatalf("requester must always see the change: %v", err)
	}
	f.grantNS = ""
	f.changes[id].ClusterID = "c2" // bob 的数据范围只有 c1
	if _, err := uc.Get(ctx, bob, id); !errors.Is(err, ErrChangeNotFound) {
		t.Fatalf("user without scope must not see the change: %v", err)
	}
	if _, err := uc.Get(ctx, alice, id); err != nil {
		t.Fatalf("requester must see own change: %v", err)
	}
	if items, _, _ := uc.List(ctx, bob, false, ChangeFilter{}, 1, 20); len(items) != 0 {
		t.Fatal("list must be limited to clusters in scope")
	}
	if items, _, _ := uc.List(ctx, alice, true, ChangeFilter{}, 1, 20); len(items) != 1 {
		t.Fatal("mine must list own changes")
	}
	// 发起人撤回：以发起人的名义撤销工单流程。
	if err := uc.Cancel(ctx, alice, id); err != nil || f.cancelled[0] != "ticket-1/"+alice.UserID {
		t.Fatalf("cancel: %v %v", err, f.cancelled)
	}
}

func TestValidateNamespaceRequest(t *testing.T) {
	t.Parallel()
	ok := kc.NamespaceRequest{
		Name: "team-a", Quota: map[string]string{"requests.cpu": "4", "limits.memory": "8Gi", "pods": "50"},
		LimitDefault: map[string]string{"cpu": "500m"}, LimitDefaultRequest: map[string]string{"memory": "128Mi"},
	}
	if err := ValidateNamespaceRequest(&ok); err != nil {
		t.Fatal(err)
	}
	bad := []kc.NamespaceRequest{
		{Name: "Team"},
		{Name: "kube-system"},
		{Name: "kube-x"},
		{Name: "default"},
		{Name: "reborn-system"},
		{Name: ""},
		{Name: "a", Labels: map[string]string{"pod-security.kubernetes.io/enforce": "privileged"}},
		{Name: "a", Quota: map[string]string{"count/secrets": "1"}},
		{Name: "a", Quota: map[string]string{"pods": "many"}},
		{Name: "a", Quota: map[string]string{"pods": "-1"}},
		{Name: "a", LimitDefault: map[string]string{"nvidia.com/gpu": "1"}},
		{Name: "a", LimitDefaultRequest: map[string]string{"cpu": "1; rm -rf"}},
	}
	for i := range bad {
		if err := ValidateNamespaceRequest(&bad[i]); !errors.Is(err, ErrChangeInvalid) {
			t.Errorf("%+v must be rejected, got %v", bad[i], err)
		}
	}
}

func TestSelfServiceRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	uc, _, syncer := newChangeUC(f)
	// 未开放。
	if err := uc.RequestNamespace(ctx, alice, "c1", kc.NamespaceRequest{Name: "team-a"}, ""); !errors.Is(err, ErrRequestDisabled) {
		t.Fatalf("disabled namespace request: %v", err)
	}
	if items, _ := uc.RequestClusters(ctx); len(items) != 0 {
		t.Fatal("no clusters may be requested while the entry is disabled")
	}
	if err := uc.UpdateSettings(ctx, alice, "tpl-ns", "tpl-perm"); err != nil {
		t.Fatal(err)
	}
	f.templateErr = ErrChangeTemplate
	if err := uc.UpdateSettings(ctx, alice, "bad", ""); !errors.Is(err, ErrChangeTemplate) {
		t.Fatalf("unusable template: %v", err)
	}
	f.templateErr = nil

	err := uc.RequestNamespace(ctx, alice, "c1", kc.NamespaceRequest{Name: "team-a", Quota: map[string]string{"pods": "10"}}, "new team")
	id, _, _, ok := Submitted(err)
	if !ok {
		t.Fatalf("namespace request: %v", err)
	}
	c := f.changes[id]
	var exec kc.ChangeExec
	_ = json.Unmarshal([]byte(strings.TrimPrefix(string(f.execs[id]), "sealed:")), &exec)
	if c.Kind != kc.ChangeKindNamespace || exec.Namespace == nil || exec.Namespace.Name != "team-a" || exec.Op != "" || len(exec.Items) != 0 {
		t.Fatalf("namespace change = %+v exec = %+v", c, exec)
	}
	if tk := f.tickets[len(f.tickets)-1]; tk.TemplateID != "tpl-ns" || tk.Fields[TicketFieldReason] != "new team" {
		t.Fatalf("ticket = %+v", tk)
	}

	// 权限申请：集群管理员不可申请。
	if err := uc.RequestPermission(ctx, alice, "c1", kc.PermissionRequest{Namespace: "*", Level: kc.LevelClusterAdmin}); !errors.Is(err, ErrChangeInvalid) {
		t.Fatalf("cluster-admin request: %v", err)
	}
	err = uc.RequestPermission(ctx, alice, "c1", kc.PermissionRequest{Namespace: "team-a", Level: kc.LevelEdit})
	pid, _, _, ok := Submitted(err)
	if !ok || f.execs[pid] != nil {
		t.Fatalf("permission request: %v", err)
	}
	f.changes[id].Status = kc.ChangeRejected // 只留权限申请待审批
	if err := syncer.Resolve(ctx, nil, "ticket-1", TicketApproved); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	events := &fakeEvents{}
	w := NewChangeWorker(f, f, fakeScopeRepo{f}, fakeSelfRoles{}, fakeNotes{f}, events, nil, f, f, fakeTxm{}, "", log)
	if _, err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	got := f.scopes["self-"+alice.UserID]
	if f.changes[pid].Status != kc.ChangeSucceeded || len(got) != 1 || got[0].ClusterID != "c1" || got[0].Namespace != "team-a" || got[0].Level != kc.LevelEdit {
		t.Fatalf("grant: status = %d scopes = %+v", f.changes[pid].Status, got)
	}
	if a := f.audits[0]; a.UserID != alice.UserID || a.RequestID != pid {
		t.Fatalf("audit = %+v", a)
	}
	// 结果通知发给发起人，并写回工单；审批未通过的不另发通知。
	if len(events.events) != 1 || events.events[0].EventType != kc.NotifyChangeSucceeded || events.events[0].Recipients[0] != alice.UserID {
		t.Fatalf("events = %+v", events.events)
	}
	if len(f.comments) != 1 || !strings.Contains(f.comments[0], "已执行成功") {
		t.Fatalf("comments = %v", f.comments)
	}
}

func TestMergeScope(t *testing.T) {
	t.Parallel()
	cur := []*RoleScope{{ClusterID: "c1", Namespace: "a", Level: kc.LevelAdmin}, {ClusterID: "c1", Namespace: "b", Level: kc.LevelView}}
	out := MergeScope(cur, "c1", "a", kc.LevelView)
	if len(out) != 2 || out[0].Level != kc.LevelAdmin {
		t.Fatalf("a lower level must not downgrade: %+v", out[0])
	}
	out = MergeScope(cur, "c1", "b", kc.LevelEdit)
	if out[1].Level != kc.LevelEdit || cur[1].Level != kc.LevelView {
		t.Fatal("must upgrade in the copy and leave the input alone")
	}
	if out = MergeScope(cur, "c2", "*", kc.LevelView); len(out) != 3 {
		t.Fatalf("new entry: %+v", out)
	}
}

func TestChangeDetailText(t *testing.T) {
	t.Parallel()
	d := &kc.ChangeDisplay{Items: []kc.ChangeItemDisplay{
		{Kind: "Deployment", Action: "update", Ref: kc.Ref{Namespace: "a", Name: "web"}, Fields: []kc.ChangeField{
			{Path: "spec.replicas", Before: "2", After: "5"}, {Path: "metadata.labels.x", After: `"y"`}, {Path: "spec.paused", Before: "true"},
		}},
		{Kind: "Namespace", Action: "create", Ref: kc.Ref{Name: "n"}},
	}}
	want := "[update] Deployment a/web\n  spec.replicas: 2 → 5\n  metadata.labels.x: \"y\"\n  spec.paused: true → (删除)\n[create] Namespace n"
	if got := changeDetailText(d); got != want {
		t.Fatalf("detail =\n%s", got)
	}
}

// 四眼原则：发起人不能同意自己的变更，超时自动同意也不算；他人可以。工单没有挂变更单时不干预。
func TestChangeCheckApprover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	_, planes, syncer := newChangeUC(f)
	_, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{})
	id, _, _, _ := Submitted(err)
	ticket := f.changes[id].TicketID
	if err := syncer.CheckApprover(ctx, nil, ticket, alice.UserID); !errors.Is(err, ErrSelfApproval) {
		t.Fatalf("requester approving own change: %v", err)
	}
	if err := syncer.CheckApprover(ctx, nil, ticket, ""); !errors.Is(err, ErrSelfApproval) {
		t.Fatalf("timeout auto-approval: %v", err)
	}
	if err := syncer.CheckApprover(ctx, nil, ticket, "018f3e1a-0000-7000-8000-000000000009"); err != nil {
		t.Fatalf("another user: %v", err)
	}
	if err := syncer.CheckApprover(ctx, nil, "ticket-without-change", alice.UserID); err != nil {
		t.Fatalf("ordinary ticket: %v", err)
	}
}

// 自助申请权限的范围：全部命名空间只能申请只读；系统命名空间不接受。
func TestSelfServiceScope(t *testing.T) {
	t.Parallel()
	ok := []kc.PermissionRequest{
		{Namespace: "team-a", Level: kc.LevelAdmin}, {Namespace: kc.AllNamespaces, Level: kc.LevelView},
	}
	bad := []kc.PermissionRequest{
		{Namespace: kc.AllNamespaces, Level: kc.LevelEdit},
		{Namespace: kc.AllNamespaces, Level: kc.LevelAdmin},
		{Namespace: "kube-system", Level: kc.LevelView},
		{Namespace: "reborn-system", Level: kc.LevelEdit},
		{Namespace: "Bad_NS", Level: kc.LevelView},
	}
	for _, r := range ok {
		if err := checkSelfServiceScope(r); err != nil {
			t.Errorf("%+v: %v", r, err)
		}
	}
	for _, r := range bad {
		if err := checkSelfServiceScope(r); !errors.Is(err, ErrChangeInvalid) {
			t.Errorf("%+v must be rejected, got %v", r, err)
		}
	}
}

// 数据范围对「对象内容」的判定：命名空间内的对象按命名空间；集群级对象要全部命名空间；无集群的只有超管。
func TestAccessAllowsObject(t *testing.T) {
	t.Parallel()
	a := NewAccess([]*RoleScope{{ClusterID: "c1", Namespace: "a"}, {ClusterID: "c2", Namespace: kc.AllNamespaces}})
	cases := []struct {
		cluster, ns string
		want        bool
	}{
		{"c1", "a", true}, {"c1", "b", false}, {"c1", "", false}, {"c2", "x", true}, {"c2", "", true}, {"c3", "a", false}, {"", "", false},
	}
	for _, c := range cases {
		if got := a.AllowsObject(c.cluster, c.ns); got != c.want {
			t.Errorf("AllowsObject(%q, %q) = %v, want %v", c.cluster, c.ns, got, c.want)
		}
	}
	if !(Access{All: true}).AllowsObject("", "") {
		t.Error("admin sees everything")
	}
}

// 总览摘要按数据范围裁剪：只留范围内命名空间的条目，节点清单去掉，总量保留。
func TestScopeSummary(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`{"nodeTotal":3,"abnormalWorkloads":[{"namespace":"a","name":"x"},{"namespace":"kube-system","name":"dns"}],"restartPods":[{"namespace":"b","name":"p"}],"abnormalNodes":[{"name":"n1"}]}`)
	scoped := NewAccess([]*RoleScope{{ClusterID: "c1", Namespace: "a"}})
	out := string(scopeSummary(raw, scoped, "c1"))
	for _, leak := range []string{"kube-system", "dns", `"p"`, "n1"} {
		if strings.Contains(out, leak) {
			t.Errorf("scoped summary leaks %s: %s", leak, out)
		}
	}
	if !strings.Contains(out, `"name":"x"`) || !strings.Contains(out, `"nodeTotal":3`) {
		t.Errorf("scoped summary lost own items or totals: %s", out)
	}
	all := NewAccess([]*RoleScope{{ClusterID: "c1", Namespace: kc.AllNamespaces}})
	if string(scopeSummary(raw, all, "c1")) != string(raw) || string(scopeSummary(raw, Access{All: true}, "c1")) != string(raw) {
		t.Error("all-namespaces grant and admin must see the full summary")
	}
}

// 代理地址的账号：响应里打码；表单原样带回时沿用已保存的，改了地址或自带账号则按输入。
func TestProxyUserinfo(t *testing.T) {
	t.Parallel()
	stored := "http://user:p%40ss@proxy.local:3128" //nolint:gosec // 测试数据
	masked := MaskProxyURL(stored)
	if strings.Contains(masked, "user") || strings.Contains(masked, "ss") || masked != "http://REDACTED@proxy.local:3128" {
		t.Fatalf("masked = %q", masked)
	}
	if MaskProxyURL("http://proxy.local:3128") != "http://proxy.local:3128" || MaskProxyURL("") != "" {
		t.Fatal("URLs without userinfo must stay unchanged")
	}
	for in, want := range map[string]string{ //nolint:gosec // 测试数据
		masked:                             stored,
		"http://proxy.local:3128":          stored,
		"http://new:pw@proxy.local:3128":   "http://new:pw@proxy.local:3128",
		"http://other.local:3128":          "http://other.local:3128",
		"http://REDACTED@other.local:3128": "http://REDACTED@other.local:3128",
		"":                                 "",
	} {
		if got := keepProxyUserinfo(in, stored); got != want {
			t.Errorf("keepProxyUserinfo(%q) = %q, want %q", in, got, want)
		}
	}
}

// 已批准未执行的变更单：每轮按发起人此刻的状态刷新身份；账号已不可用则直接置为失败，不会执行。
func TestChangeWorkerRefreshesRequester(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	f.refreshed = map[string]*kc.ChangeRequester{}
	_, planes, syncer := newChangeUC(f)
	_, err := planes.Call(ctx, alice, "c1", kc.OpScale, kc.Empty{})
	id, _, _, _ := Submitted(err)
	if err := syncer.Resolve(ctx, nil, f.changes[id].TicketID, TicketApproved); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := NewChangeWorker(f, f, fakeScopeRepo{f}, fakeSelfRoles{}, fakeNotes{f}, &fakeEvents{}, f, f, f, fakeTxm{}, "", log)
	// 角色在批准之后被换掉：快照跟着换。
	f.roleIDs = []string{"r9"}
	if err := w.refreshRequesters(ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.refreshed[id]; got == nil || len(got.RoleIDs) != 1 || got.RoleIDs[0] != "r9" || got.IsAdmin {
		t.Fatalf("refreshed requester = %+v", got)
	}
	// 账号被停用：变更单置为失败。
	f.inactive = true
	if err := w.refreshRequesters(ctx); err != nil {
		t.Fatal(err)
	}
	if c := f.changes[id]; c.Status != kc.ChangeFailed || !strings.Contains(c.Message, "停用") {
		t.Fatalf("change of a disabled requester: status = %d message = %q", c.Status, c.Message)
	}
}

// 调试容器要单独的权限：只有终端权限的人打开 kind = 2 的票据被拒绝。
func TestDebugTicketNeedsPermission(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newChangeFakes()
	f.noDebug = true
	_, planes, _ := newChangeUC(f)
	if _, err := planes.OpenExecTicket(ctx, alice, "c1", kc.ExecParams{Namespace: "default", Pod: "p", Kind: kc.ExecKindDebug}); !errors.Is(err, ErrDebugForbidden) {
		t.Fatalf("debug ticket without the permission: %v", err)
	}
}
