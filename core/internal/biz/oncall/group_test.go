package biz

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// memberTx 成员用例的事务桩：只实现移出成员用到的方法（pending 为 HasPendingOverrides 的返回值）。
type memberTx struct {
	Tx
	members []*Member
	pending bool
	removed []string
}

func (t *memberTx) LockGroup(_ context.Context, id string) (*Group, error) {
	return &Group{ID: id}, nil
}

func (t *memberTx) ListMembers(context.Context, string) ([]*Member, error) { return t.members, nil }

func (t *memberTx) HasPendingOverrides(context.Context, string, string) (bool, error) {
	return t.pending, nil
}

func (t *memberTx) DeleteMember(_ context.Context, _, userID string) error {
	t.members = slices.DeleteFunc(t.members, func(m *Member) bool { return m.UserID == userID })
	t.removed = append(t.removed, userID)
	return nil
}

func (t *memberTx) InsertChangeLog(context.Context, *ChangeLog) error { return nil }

type memberStore struct{ tx *memberTx }

func (s memberStore) WithinTx(_ context.Context, fn func(tx Tx) error) error { return fn(s.tx) }

// 名下还有未结束的替班班次的成员不能移出：否则这些班次悬在一个已不在组里的人名下。与「仍在
// 轮转名单」同一个错误码，但说明里提示先撤销或改派；替班都结束了才能移出。
func TestRemoveMemberWithPendingOverride(t *testing.T) {
	t.Parallel()
	bob := &Member{UserID: "bob", Role: MemberRoleMember}
	tx := &memberTx{members: []*Member{{UserID: "alice", Role: MemberRoleAdmin}, bob}, pending: true}
	uc := NewGroupUsecase(memberStore{tx}, nil, nil)

	err := uc.RemoveMember(context.Background(), memAdmin, "g", "bob")
	if !errors.Is(err, ErrMemberInUse) || len(tx.removed) != 0 {
		t.Fatalf("RemoveMember(pending override) error = %v, removed = %v; want ErrMemberInUse and nothing removed", err, tx.removed)
	}
	if !strings.Contains(err.Error(), "override") {
		t.Errorf("the error should tell the admin what to do first, got %q", err.Error())
	}
	// 仍在轮转名单的成员照旧被拒（返回的就是哨兵本身）。
	bob.InRotation = true
	if err := uc.RemoveMember(context.Background(), memAdmin, "g", "bob"); err != ErrMemberInUse || len(tx.removed) != 0 {
		t.Fatalf("RemoveMember(rotating) error = %v, removed = %v; want the ErrMemberInUse sentinel", err, tx.removed)
	}

	bob.InRotation, tx.pending = false, false
	if err := uc.RemoveMember(context.Background(), memAdmin, "g", "bob"); err != nil || !slices.Equal(tx.removed, []string{"bob"}) {
		t.Fatalf("RemoveMember(no pending override) error = %v, removed = %v", err, tx.removed)
	}
}
