package service

import (
	"context"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
)

// 非超管只能重置「权限不高于自己」的账号的密码：此前守卫只保护超管账号，持「重置密码」权限的角色
// 可以重置权限更高的普通用户（如容器平台管理员）的密码，再用对方的身份登录。
func TestUserService_ResetPasswordCeiling(t *testing.T) {
	t.Parallel()
	svc, store := newUserService(t)
	const adminID, opID, lowID, highID = "u-admin", "u-op", "u-low", "u-high"
	seedUserRow(t, store, adminID, "root", true)
	seedUserRow(t, store, opID, "operator", false)
	seedUserRow(t, store, lowID, "low", false)
	seedUserRow(t, store, highID, "high", false)
	for _, r := range []*biz.Role{{ID: "role-held", Name: "持有", Code: "held", Status: biz.RoleStatusEnabled}, {ID: "role-high", Name: "高权限", Code: "high", Status: biz.RoleStatusEnabled}} {
		if err := store.CreateRole(context.Background(), r); err != nil {
			t.Fatalf("seed role %s: %v", r.Code, err)
		}
	}
	for user, roles := range map[string][]string{opID: {"role-held"}, lowID: {"role-held"}, highID: {"role-high"}} {
		if err := store.BindUserRoles(context.Background(), user, roles); err != nil {
			t.Fatalf("bind %s: %v", user, err)
		}
	}
	const pw = "new-secret-123"
	opCtx, adminCtx := claimsContext(opID), claimsContext(adminID)

	if _, err := svc.ResetUserPassword(opCtx, &v1.ResetUserPasswordRequest{Id: lowID, Password: pw}); err != nil {
		t.Fatalf("operator resets a user who holds only roles the operator holds: %v", err)
	}
	_, err := svc.ResetUserPassword(opCtx, &v1.ResetUserPasswordRequest{Id: highID, Password: pw})
	if errors.Code(err) != 403 || errors.Reason(err) != v1.ErrorReason_PERMISSION_DENIED.String() {
		t.Fatalf("operator resets a user who holds a role the operator lacks: error = %v, want 403 PERMISSION_DENIED", err)
	}
	if _, err := svc.ResetUserPassword(adminCtx, &v1.ResetUserPasswordRequest{Id: highID, Password: pw}); err != nil {
		t.Fatalf("admin resets any non-admin user: %v", err)
	}
}

// 超管用自己的大写 ID 提交也绕不过「不能改自己的 is_admin」：UUID 大小写不敏感，守卫此前按原样比较。
func TestUserService_SelfDemotionUppercaseID(t *testing.T) {
	t.Parallel()
	svc, store := newUserService(t)
	seedUserRow(t, store, "u-admin", "root", true)
	no := false
	_, err := svc.UpdateUser(claimsContext("u-admin"), &v1.UpdateUserRequest{Id: "U-ADMIN", IsAdmin: &no})
	if !errors.Is(err, biz.ErrUserSelfOperation) {
		t.Fatalf("admin demotes self with an uppercase id: error = %v, want ErrUserSelfOperation", err)
	}
}
