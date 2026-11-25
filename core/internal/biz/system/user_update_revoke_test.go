package biz

import (
	"context"
	"errors"
	"testing"
)

// 请求里原样回填 is_admin=false（值没变）不能吊销普通用户的会话：此前只要请求带了 false 就吊销，
// 管理员改个昵称就把对方的所有设备踢下线。
func TestUserUsecase_UpdateUnchangedAdminFlagKeepsSessions(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	u, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Nickname: "A", Gender: GenderFemale, Status: StatusEnabled})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	no, nick := false, "Alice"
	if err := uc.Update(ctx, u.ID, UserUpdate{IsAdmin: &no}); err != nil || len(stub.revoked) != 0 {
		t.Fatalf("Update(is_admin=false, unchanged) error = %v, revoked = %d; want nil, 0", err, len(stub.revoked))
	}
	if err := uc.Update(ctx, u.ID, UserUpdate{Nickname: &nick, IsAdmin: &no}); err != nil || len(stub.revoked) != 0 {
		t.Fatalf("Update(nickname + is_admin=false) error = %v, revoked = %d; want nil, 0", err, len(stub.revoked))
	}
	// 目标不存在：不吊销。
	if err := uc.Update(ctx, "missing", UserUpdate{IsAdmin: &no}); !errors.Is(err, ErrUserNotFound) || len(stub.revoked) != 0 {
		t.Fatalf("Update(missing) error = %v, revoked = %d; want ErrUserNotFound, 0", err, len(stub.revoked))
	}
}
