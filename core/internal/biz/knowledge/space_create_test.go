package biz

import (
	"context"
	"errors"
	"testing"
)

// recordingSpaceRepo 在内存空间仓储之上记录落库调用（Create / Update 传进来的空间）。
type recordingSpaceRepo struct {
	fakeSpaceRepo
	created []*Space
	updated []*Space
}

func (f *recordingSpaceRepo) Create(_ context.Context, s *Space) error {
	f.created = append(f.created, s)
	return nil
}

func (f *recordingSpaceRepo) Update(_ context.Context, s *Space) error {
	f.updated = append(f.updated, s)
	return nil
}

func newRecordingSpaceUsecase() (*SpaceUsecase, *recordingSpaceRepo) {
	_, base := newSpaceUsecaseForTest()
	repo := &recordingSpaceRepo{fakeSpaceRepo: *base}
	return NewSpaceUsecase(repo, fakeRoles{}, fakeDirectory{}, fakeRoleDir{}), repo
}

// 空间名称不能是纯空白：proto 的 min_len 校验的是原始字符串，"   " 能通过，去掉首尾空白后为空应拒绝，
// 且不落库、不覆盖已有名称。
func TestSpaceNameCannotBeBlank(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, name := range []string{"   ", "\t\n", " 　 "} {
		uc, repo := newRecordingSpaceUsecase()
		_, err := uc.Create(ctx, tOwner, CreateSpaceInput{Code: "ops", Name: name, Type: "general", Visibility: VisibilityPublic})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Create(name = %q) error = %v, want ErrInvalidArgument", name, err)
		}
		if len(repo.created) != 0 {
			t.Errorf("Create(name = %q) persisted %d spaces", name, len(repo.created))
		}

		uc, repo = newRecordingSpaceUsecase()
		blank := name
		if _, err := uc.Update(ctx, tOwner, tPriv, SpacePatch{Name: &blank}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Update(name = %q) error = %v, want ErrInvalidArgument", name, err)
		}
		if len(repo.updated) != 0 {
			t.Errorf("Update(name = %q) persisted %d spaces", name, len(repo.updated))
		}
	}

	// 正常名称：首尾空白照旧去掉后保存。
	uc, repo := newRecordingSpaceUsecase()
	s, err := uc.Create(ctx, tOwner, CreateSpaceInput{Code: "ops", Name: "  运维手册  ", Type: "general", Visibility: VisibilityPublic})
	if err != nil || s.Name != "运维手册" || len(repo.created) != 1 || repo.created[0].Name != "运维手册" {
		t.Fatalf("Create() = %+v, %v; persisted %+v", s, err, repo.created)
	}
	renamed := " 新名称 "
	if _, err := uc.Update(ctx, tOwner, tPriv, SpacePatch{Name: &renamed}); err != nil || len(repo.updated) != 1 || repo.updated[0].Name != "新名称" {
		t.Fatalf("Update() error = %v; persisted %+v", err, repo.updated)
	}
}

// 新建空间返回的 level 是创建者对它的实际级别，而不是恒为管理级别：前端据此渲染管理入口，报高了随后都会 403 / 404。
func TestCreateSpaceReturnsCreatorLevel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name       string
		user       string
		owner      string
		visibility int32
		want       int32
	}{
		{name: "owner", user: tOwner, owner: "", visibility: VisibilityRestricted, want: LevelManage},
		{name: "admin, someone else's restricted space", user: tAdmin, owner: tOwner, visibility: VisibilityRestricted, want: LevelManage},
		{name: "non-owner, public space", user: tReader, owner: tOwner, visibility: VisibilityPublic, want: LevelRead},
		{name: "non-owner, restricted space", user: tReader, owner: tOwner, visibility: VisibilityRestricted, want: LevelNone},
	}
	for _, tc := range cases {
		uc, repo := newRecordingSpaceUsecase()
		s, err := uc.Create(ctx, tc.user, CreateSpaceInput{Code: "ops", Name: "运维", Type: "general", Visibility: tc.visibility, OwnerID: tc.owner})
		if err != nil {
			t.Fatalf("%s: Create() error = %v", tc.name, err)
		}
		if s.Level != tc.want {
			t.Errorf("%s: Level = %d, want %d", tc.name, s.Level, tc.want)
		}
		if len(repo.created) != 1 || repo.created[0].OwnerID != firstNonEmpty(tc.owner, tc.user) {
			t.Errorf("%s: persisted = %+v", tc.name, repo.created)
		}
	}
}
