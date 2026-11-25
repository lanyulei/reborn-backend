package biz

import (
	"context"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// txAwareMembers 记下每次 List 时 ctx 是否带着事务句柄。
type txAwareMembers struct {
	MemberRepo
	inTx []bool
}

func (m *txAwareMembers) List(ctx context.Context, _ string) ([]*Member, error) {
	m.inTx = append(m.inTx, txrun.FromContext(ctx) != nil)
	return []*Member{{UserID: "u9", Role: RoleResponder}}, nil
}

// 写事务里读成员带着本事务的句柄（data 层据此复用事务连接，不再向连接池要第二条），读到的成员照常交给 fn；
// 交给 fn 的 ctx 不变——fn 里其余的读取仍按原来的方式走。
func TestMutateReadsMembersOnTheTransaction(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResponding}
	members := &txAwareMembers{}
	d := &Deps{Txm: fakeTxm{}, Writer: &mapWriter{byID: map[string]*Incident{inc.ID: inc}}, Members: members, Logger: testLogger()}
	uc := NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))

	ctx := context.Background()
	var seen []*Member
	if _, err := uc.mutate(ctx, "i1", ActorSystem, "", func(m *mutation) error {
		seen = m.members
		return nil
	}); err != nil {
		t.Fatalf("mutate() error = %v", err)
	}
	if len(members.inTx) != 1 || !members.inTx[0] {
		t.Fatalf("member reads carried the transaction: %v, want exactly one read on the transaction", members.inTx)
	}
	if len(seen) != 1 || seen[0].UserID != "u9" {
		t.Fatalf("members handed to fn = %+v, want the ones read", seen)
	}
	if txrun.FromContext(ctx) != nil {
		t.Fatal("the caller's ctx must stay free of the transaction handle")
	}
}
