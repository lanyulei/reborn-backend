package biz

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// treeLookup 实例查询桩：GetInstance 按 instances 返回（in 非空时恒返回它）；SeatsInTree 返回用户
// 持有席位的实例（只列未被取消的席位，与 data 层同口径）；InstancesWithSeats 返回 listed 并记下游标。
type treeLookup struct {
	InstanceLookup
	in        *Instance
	instances map[string]*Instance
	seats     map[string][]string
	listed    []string
	after     string
}

func (l *treeLookup) GetInstance(_ context.Context, id string) (*Instance, error) {
	if l.in != nil {
		return l.in, nil
	}
	if in := l.instances[id]; in != nil {
		return in, nil
	}
	return nil, ErrInstanceNotFound
}

func (l *treeLookup) SeatsInTree(_ context.Context, _ string, userID string) ([]SeatRef, error) {
	var refs []SeatRef
	for _, id := range l.seats[userID] {
		refs = append(refs, SeatRef{InstanceID: id})
	}
	return refs, nil
}

func (l *treeLookup) InstancesWithSeats(_ context.Context, _, after string, _ int) ([]string, error) {
	l.after = after
	return l.listed, nil
}

func (l *treeLookup) Children(context.Context, string) ([]*ChildRef, error) { return nil, nil }

func handoverIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("018f3e1a-0c00-7000-8000-%012x", i+1)
	}
	return out
}

const (
	hoFrom = "018f3e1a-0c00-7000-8000-0000000000f1"
	hoTo   = "018f3e1a-0c00-7000-8000-0000000000f2"
)

func handoverRT(lookup *treeLookup, txs *txManagerFake) *RuntimeUsecase {
	return NewRuntimeUsecase(nil, nil, nil, lookup, &dirStub{}, nil, nil, nil, txs, nil, nil, nil, nil)
}

// 请求的时间在批中途用尽：被打断的那个实例不算失败，游标停在最后一个处理过的实例上；带着游标
// 再来时从断点之后继续。修复前超时后的实例全部计为失败、且被游标越过，再也不会被处理。
func TestHandoverStopsAtDeadlineWithoutSkipping(t *testing.T) {
	t.Parallel()
	ids := handoverIDs(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookup := &treeLookup{listed: ids}
	var txs *txManagerFake
	txs = &txManagerFake{onTx: func(ctx context.Context) error {
		if txs.calls == 3 {
			cancel() // 处理第三个实例时请求超时
		}
		return ctx.Err()
	}}

	moved, failed, next, err := handoverRT(lookup, txs).Handover(ctx, hoFrom, hoTo, "adm", "调岗", "")
	if err != nil || failed != 0 || moved != 0 || next != ids[1] {
		t.Fatalf("Handover() = %d moved, %d failed, next %q, err %v; want 0, 0, %q, nil", moved, failed, next, err, ids[1])
	}
	if txs.calls != 3 {
		t.Errorf("commands attempted = %d, want 3", txs.calls)
	}

	// 续传：游标原样交给查询，从断点之后继续；处理完剩下的即结束。
	lookup2 := &treeLookup{listed: ids[2:]}
	txs2 := &txManagerFake{}
	moved, failed, next, err = handoverRT(lookup2, txs2).Handover(context.Background(), hoFrom, hoTo, "adm", "调岗", next)
	if err != nil || failed != 0 || next != "" || lookup2.after != ids[1] || txs2.calls != 3 {
		t.Fatalf("Handover(resume) = %d moved, %d failed, next %q, after %q, %d commands, err %v",
			moved, failed, next, lookup2.after, txs2.calls, err)
	}
}

// 一个实例都没来得及处理：有游标就原样带回（more 为真）；没有游标可返回时如实报错——不能回一个
// more=false 让调用方以为交接已经做完。
func TestHandoverCursorWhenNothingProcessed(t *testing.T) {
	t.Parallel()
	ids := handoverIDs(3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lookup := &treeLookup{listed: ids}
	txs := &txManagerFake{}

	_, failed, next, err := handoverRT(lookup, txs).Handover(ctx, hoFrom, hoTo, "adm", "调岗", ids[0])
	if err != nil || failed != 0 || next != ids[0] || txs.calls != 0 {
		t.Fatalf("Handover(cancelled, cursor) = %d failed, next %q, %d commands, err %v; want 0, %q, 0, nil",
			failed, next, txs.calls, err, ids[0])
	}
	_, _, next, err = handoverRT(lookup, txs).Handover(ctx, hoFrom, hoTo, "adm", "调岗", "")
	if !errors.Is(err, context.Canceled) || next != "" {
		t.Fatalf("Handover(cancelled, no cursor) = next %q, err %v; want an error and no cursor", next, err)
	}
}

// 离截止不到余量时不再开始处理新的实例（把应答的时间留出来）；时间充裕则整批处理完。
func TestHandoverReservesTimeForReply(t *testing.T) {
	t.Parallel()
	ids := handoverIDs(3)
	short, cancelShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShort()
	lookup := &treeLookup{listed: ids}
	txs := &txManagerFake{}
	if _, _, next, err := handoverRT(lookup, txs).Handover(short, hoFrom, hoTo, "adm", "调岗", ids[0]); err != nil || next != ids[0] || txs.calls != 0 {
		t.Fatalf("Handover(almost out of time) = next %q, %d commands, err %v; want no command and the cursor back", next, txs.calls, err)
	}

	long, cancelLong := context.WithTimeout(context.Background(), time.Minute)
	defer cancelLong()
	lookup = &treeLookup{listed: ids}
	txs = &txManagerFake{}
	if _, _, next, err := handoverRT(lookup, txs).Handover(long, hoFrom, hoTo, "adm", "调岗", ids[0]); err != nil || next != "" || txs.calls != 3 {
		t.Fatalf("Handover(time left) = next %q, %d commands, err %v; want all three processed", next, txs.calls, err)
	}
}

// 多于一批：处理一批，游标指向这一批的最后一个；真正失败的实例照常计数并被越过（原有行为）。
func TestHandoverPaginationAndFailedInstances(t *testing.T) {
	t.Parallel()
	ids := handoverIDs(handoverBatch + 3)
	lookup := &treeLookup{listed: ids}
	var txs *txManagerFake
	txs = &txManagerFake{onTx: func(context.Context) error {
		if txs.calls == 2 {
			return errors.New("boom")
		}
		return nil
	}}

	_, failed, next, err := handoverRT(lookup, txs).Handover(context.Background(), hoFrom, hoTo, "adm", "调岗", "")
	if err != nil || failed != 1 || next != ids[handoverBatch-1] || txs.calls != handoverBatch {
		t.Fatalf("Handover() = %d failed, next %q, %d commands, err %v; want 1, %q, %d, nil",
			failed, next, txs.calls, err, ids[handoverBatch-1], handoverBatch)
	}
}

// 流程树：根流程 R → 子流程 A → 孙流程 C；S 是 R 的另一个子流程。
const (
	uidR = "018f3e1a-0000-7000-8000-000000000001"
	uidA = "018f3e1a-0000-7000-8000-00000000000a"
	uidC = "018f3e1a-0000-7000-8000-00000000000c"
	uidS = "018f3e1a-0000-7000-8000-00000000000b"
)

// versionStub 只实现 GetVersion：返回一张最简单的流程图（详情页要把它渲染出来）。
type versionStub struct{ VersionReader }

func (versionStub) GetVersion(context.Context, string) (*DefinitionVersion, error) {
	return &DefinitionVersion{Graph: []byte(linearGraph(task("a", "single", "child-handler")))}, nil
}

type readerFake struct {
	InstanceReader
	s *State
}

func (r readerFake) Load(context.Context, string) (*State, error) { return r.s, nil }

type usersFake struct{ Users }

func (usersFake) UserNames(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type viewerFake func(ctx context.Context, userID, businessID string) (bool, error)

func (f viewerFake) CanViewInstance(ctx context.Context, userID, businessID string) (bool, error) {
	return f(ctx, userID, businessID)
}

// detailRT 以 C（孙流程）的详情为目标装配运行时用例。
func detailRT(t *testing.T, s *State, seats map[string][]string, viewers Viewers) *RuntimeUsecase {
	t.Helper()
	defs := NewDefinitionUsecase(nil, nil, nil, versionStub{}, nil, nil)
	lookup := &treeLookup{instances: map[string]*Instance{
		uidR: {ID: uidR, RootID: uidR, Initiator: "init"},
		uidA: {ID: uidA, ParentInstanceID: uidR, RootID: uidR, Initiator: "init"},
		uidC: {ID: uidC, ParentInstanceID: uidA, RootID: uidR, Initiator: "init"},
		uidS: {ID: uidS, ParentInstanceID: uidR, RootID: uidR, Initiator: "init"},
	}, seats: seats}
	return NewRuntimeUsecase(defs, nil, readerFake{s: s}, lookup, &dirStub{}, usersFake{}, nil, nil, nil, nil, nil, viewers, nil)
}

func childDetailState() *State {
	return &State{Instance: Instance{
		ID: uidC, ParentInstanceID: uidA, RootID: uidR, Depth: 2, Initiator: "init", Status: InstanceRunning, VersionID: "v1",
	}, Tasks: []*Task{{
		ID: "t1", NodeID: "a", Status: TaskPending,
		Assignees: []*TaskAssignee{{UserID: "child-handler", Status: AssigneePending}},
	}}}
}

// 不挂业务对象的流程：子流程的详情对祖先实例（父流程及其上级）里处理过任务的人开放，只读——
// 他们在子流程上没有席位，按本实例的参与关系原本会被拒。同级子流程的人、无关的人仍然打不开；
// 顶层实例不因此对子流程的人开放（只放宽「父看子」这一个方向）。
func TestGetSubProcessDetailForAncestorParticipants(t *testing.T) {
	t.Parallel()
	s := childDetailState()
	seats := map[string][]string{
		"parent-handler":  {uidA},
		"root-handler":    {uidR},
		"sibling-handler": {uidS},
		"child-handler":   {uidC},
	}
	rt := detailRT(t, s, seats, nil)
	for user, want := range map[string]bool{
		"parent-handler": true, "root-handler": true, "child-handler": true, "init": true,
		"sibling-handler": false, "outsider": false,
	} {
		d, err := rt.Get(context.Background(), uidC, user, false)
		switch {
		case want && err != nil:
			t.Errorf("Get(child, %s) error = %v, want allowed", user, err)
		case !want && !errors.Is(err, ErrInstanceForbidden):
			t.Errorf("Get(child, %s) error = %v, want ErrInstanceForbidden", user, err)
		case want && user != "child-handler" && d.MyTaskID != "":
			t.Errorf("Get(child, %s) must be read-only, got a task to handle %q", user, d.MyTaskID)
		}
	}
	if d, err := rt.Get(context.Background(), uidC, "child-handler", false); err != nil || d.MyTaskID != "t1" {
		t.Errorf("the child's own handler keeps handling his task: %+v, %v", d, err)
	}

	// 顶层实例：子流程的人看不到（不在放宽范围内）。
	rt = detailRT(t, &State{Instance: Instance{ID: uidR, RootID: uidR, Initiator: "init", Status: InstanceRunning, VersionID: "v1"}}, seats, nil)
	if _, err := rt.Get(context.Background(), uidR, "child-handler", false); !errors.Is(err, ErrInstanceForbidden) {
		t.Errorf("Get(root, child handler) error = %v, want ErrInstanceForbidden", err)
	}
	// 被改派走（席位已取消）的人不算参与者：席位查询里没有他。
	rt = detailRT(t, childDetailState(), map[string][]string{"reassigned": {}}, nil)
	if _, err := rt.Get(context.Background(), uidC, "reassigned", false); !errors.Is(err, ErrInstanceForbidden) {
		t.Errorf("Get(child, user whose seat was cancelled) error = %v, want ErrInstanceForbidden", err)
	}
}

// 挂了业务对象的流程仍由业务域的查看规则说了算：引擎不再另行放行，也不替业务域拒绝。
func TestGetSubProcessDetailDefersToBusinessViewer(t *testing.T) {
	t.Parallel()
	for _, allow := range []bool{false, true} {
		var asked string
		viewers := Viewers{"ticket": viewerFake(func(_ context.Context, userID, businessID string) (bool, error) {
			asked = userID + "@" + businessID
			return allow, nil
		})}
		s := childDetailState()
		s.Instance.RootBusinessType, s.Instance.RootBusinessID = "ticket", "T-1"
		_, err := detailRT(t, s, map[string][]string{"parent-handler": {uidA}}, viewers).Get(context.Background(), uidC, "parent-handler", false)
		if asked != "parent-handler@T-1" {
			t.Errorf("the business viewer must be consulted, got %q", asked)
		}
		if allow != (err == nil) {
			t.Errorf("viewer allows = %v but Get() error = %v", allow, err)
		}
	}
}
