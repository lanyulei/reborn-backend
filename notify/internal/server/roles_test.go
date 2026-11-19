package server

import (
	"testing"

	"github.com/lanyulei/reborn/backend/notify/internal/conf"
)

func TestNewRoles(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	roles, err := NewRoles(&conf.Pipeline{Roles: []string{" API", "sender "}})
	if err != nil || !roles.Has(RoleAPI) || !roles.Has(RoleSender) || roles.Has(RoleRouter) {
		t.Fatalf("NewRoles = %v, %v", roles, err)
	}
	if roles, err := NewRoles(&conf.Pipeline{}); err != nil || len(roles) != 5 || roles.Has(RoleRelay) {
		t.Fatalf("empty list must enable all non-relay roles, got %v, %v", roles, err)
	}
	if _, err := NewRoles(&conf.Pipeline{Roles: []string{"sendr"}}); err == nil {
		t.Fatal("unknown role must fail assembly, not silently start an HTTP-only process")
	}
}

func TestNewRoles_NatsDefaultIncludesRelay(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	roles, err := NewRoles(&conf.Pipeline{Backbone: "nats"})
	if err != nil || !roles.Has(RoleRelay) || !roles.Has(RoleSender) {
		t.Fatalf("nats default roles = %v, %v", roles, err)
	}
	roles, err = NewRoles(&conf.Pipeline{Backbone: "pg"})
	if err != nil || roles.Has(RoleRelay) {
		t.Fatalf("pg default roles must not include relay: %v, %v", roles, err)
	}
	roles, err = NewRoles(&conf.Pipeline{Backbone: "nats", Roles: []string{"sender"}})
	if err != nil || roles.Has(RoleRelay) {
		t.Fatalf("explicit roles must be respected: %v, %v", roles, err)
	}
}
