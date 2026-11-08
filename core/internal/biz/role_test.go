package biz

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

func TestRoleUsecase_Create(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := NewRoleUsecase(stub, stub, newTestAuthorizer(t, &policyStoreStub{}, time.Minute))

	role, err := uc.Create(context.Background(), &Role{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(role.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", role.ID)
	}
	if role.CreateTime.IsZero() || role.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}
	if stub.roles[role.ID] == nil {
		t.Fatal("Create() not persisted")
	}
}

func TestRoleUsecase_CreateDuplicateCode(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := NewRoleUsecase(stub, stub, newTestAuthorizer(t, &policyStoreStub{}, time.Minute))
	ctx := context.Background()

	if _, err := uc.Create(ctx, &Role{Name: "管理员", Code: "admin"}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	_, err := uc.Create(ctx, &Role{Name: "管理员2", Code: "admin"})
	if !errors.Is(err, ErrRoleCodeExists) {
		t.Fatalf("Create(duplicate code) error = %v, want ErrRoleCodeExists", err)
	}
}

func TestRoleUsecase_Update(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := NewRoleUsecase(stub, stub, newTestAuthorizer(t, &policyStoreStub{}, time.Minute))
	ctx := context.Background()

	role, err := uc.Create(ctx, &Role{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	name := "超级管理员"
	if err := uc.Update(ctx, role.ID, RoleUpdate{Name: &name}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if stub.roles[role.ID].Name != name {
		t.Fatalf("Update() name = %q, want %q", stub.roles[role.ID].Name, name)
	}
	if err := uc.Update(ctx, "missing", RoleUpdate{Name: &name}); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrRoleNotFound", err)
	}
}

func TestRoleUsecase_Delete(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := NewRoleUsecase(stub, stub, newTestAuthorizer(t, &policyStoreStub{}, time.Minute))
	ctx := context.Background()

	role, err := uc.Create(ctx, &Role{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := uc.Delete(ctx, role.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok := stub.roles[role.ID]; ok {
		t.Fatal("Delete() role still exists")
	}
	if err := uc.Delete(ctx, role.ID); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("Delete(missing) error = %v, want ErrRoleNotFound", err)
	}
}

func TestRoleUsecase_GetAndList(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := NewRoleUsecase(stub, stub, newTestAuthorizer(t, &policyStoreStub{}, time.Minute))
	ctx := context.Background()

	created, err := uc.Create(ctx, &Role{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	got, err := uc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.ID != created.ID || got.Code != "admin" {
		t.Fatalf("Get() = %+v", got)
	}
	if _, err := uc.Get(ctx, "missing"); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrRoleNotFound", err)
	}

	roles, total, err := uc.List(ctx, RoleFilter{}, 1, 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 1 || len(roles) != 1 {
		t.Fatalf("List() total = %d, len = %d, want 1/1", total, len(roles))
	}
}
