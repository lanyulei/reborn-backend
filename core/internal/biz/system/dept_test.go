package biz

import (
	"slices"
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
)

func TestDeptConstants(t *testing.T) {
	t.Parallel()
	if DeptStatusEnabled != 1 || DeptStatusDisabled != 0 {
		t.Fatalf("dept status constants drifted: enabled=%d disabled=%d, want 1/0", DeptStatusEnabled, DeptStatusDisabled)
	}
}

func TestUserDeptAssignment_Normalized(t *testing.T) {
	t.Parallel()
	tooMany := make([]string, 0, MaxUserDepts+1)
	for i := range MaxUserDepts + 1 {
		tooMany = append(tooMany, string(rune('a'+i)))
	}
	tests := []struct {
		name        string
		in          UserDeptAssignment
		wantIDs     []string
		wantPrimary string
		wantErr     error
	}{
		{name: "empty clears", in: UserDeptAssignment{PrimaryDeptID: "x"}},
		{name: "default primary is first given", in: UserDeptAssignment{DeptIDs: []string{"b", "a"}}, wantIDs: []string{"a", "b"}, wantPrimary: "b"},
		{name: "explicit primary", in: UserDeptAssignment{DeptIDs: []string{"b", "a"}, PrimaryDeptID: "a"}, wantIDs: []string{"a", "b"}, wantPrimary: "a"},
		{name: "duplicates collapsed", in: UserDeptAssignment{DeptIDs: []string{"a", "a", "b"}}, wantIDs: []string{"a", "b"}, wantPrimary: "a"},
		{name: "primary outside list", in: UserDeptAssignment{DeptIDs: []string{"a"}, PrimaryDeptID: "z"}, wantErr: ErrDeptNotFound},
		{name: "over limit", in: UserDeptAssignment{DeptIDs: tooMany}, wantErr: ErrDeptLimitExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.in.normalized()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("normalized() error = %v, want %v", err, tt.wantErr)
			}
			if !slices.Equal(got.DeptIDs, tt.wantIDs) || got.PrimaryDeptID != tt.wantPrimary {
				t.Fatalf("normalized() = %+v, want ids %v primary %q", got, tt.wantIDs, tt.wantPrimary)
			}
		})
	}
}
