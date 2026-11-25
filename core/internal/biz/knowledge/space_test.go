package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

const (
	tOwner  = "018f3e1a-0000-7000-8000-000000000001"
	tReader = "018f3e1a-0000-7000-8000-000000000002"
	tAdmin  = "018f3e1a-0000-7000-8000-000000000009"
	tRole   = "018f3e1a-0000-7000-8000-0000000000aa"
	tPub    = "018f3e1a-0000-7000-8000-0000000000b1"
	tPriv   = "018f3e1a-0000-7000-8000-0000000000b2"
)

// fakeSpaceRepo 内存空间仓储：只实现 Resolve / Require 需要的部分。
type fakeSpaceRepo struct {
	spaces map[string]*Space
	acl    map[string][]*AclEntry
}

func (f *fakeSpaceRepo) Create(context.Context, *Space) error                { return nil }
func (f *fakeSpaceRepo) Update(context.Context, *Space) error                { return nil }
func (f *fakeSpaceRepo) SoftDelete(context.Context, string, time.Time) error { return nil }
func (f *fakeSpaceRepo) DocumentCounts(context.Context) (map[string]int32, error) {
	return map[string]int32{}, nil
}

func (f *fakeSpaceRepo) ListAcl(_ context.Context, id string) ([]*AclEntry, error) {
	return f.acl[id], nil
}

func (f *fakeSpaceRepo) ReplaceAcl(_ context.Context, id string, entries []*AclEntry, _ string) error {
	f.acl[id] = entries
	return nil
}

func (f *fakeSpaceRepo) Get(_ context.Context, id string) (*Space, error) {
	s, ok := f.spaces[id]
	if !ok {
		return nil, ErrSpaceNotFound
	}
	cp := *s
	return &cp, nil
}

func (f *fakeSpaceRepo) List(context.Context, string, string) ([]*Space, error) {
	out := make([]*Space, 0, len(f.spaces))
	for _, s := range f.spaces {
		cp := *s
		out = append(out, &cp)
	}
	return out, nil
}

func (f *fakeSpaceRepo) Names(_ context.Context, ids []string) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range ids {
		if s, ok := f.spaces[id]; ok {
			out[id] = s.Name
		}
	}
	return out, nil
}

// Levels 复刻 SQL 口径：公开 ≥1、负责人 3、ACL 取最大。
func (f *fakeSpaceRepo) Levels(_ context.Context, userID string, roleIDs []string) (map[string]int32, error) {
	out := map[string]int32{}
	for id, s := range f.spaces {
		lv := int32(0)
		if s.Visibility == VisibilityPublic {
			lv = LevelRead
		}
		if s.OwnerID == userID {
			lv = LevelManage
		}
		for _, e := range f.acl[id] {
			match := (e.SubjectType != SubjectRole && e.SubjectID == userID)
			for _, r := range roleIDs {
				if e.SubjectType == SubjectRole && e.SubjectID == r {
					match = true
				}
			}
			if match && e.Level > lv {
				lv = e.Level
			}
		}
		if lv > 0 {
			out[id] = lv
		}
	}
	return out, nil
}

type fakeRoles struct{}

func (fakeRoles) EnabledRoleIDs(_ context.Context, userID string) (bool, []string, error) {
	if userID == tAdmin {
		return true, nil, nil
	}
	if userID == tReader {
		return false, []string{tRole}, nil
	}
	return false, nil, nil
}

type fakeDirectory struct{}

func (fakeDirectory) UserNames(_ context.Context, ids []string) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range ids {
		if id == tOwner || id == tReader || id == tAdmin {
			out[id] = "user-" + id[len(id)-2:]
		}
	}
	return out, nil
}

type fakeRoleDir struct{}

func (fakeRoleDir) ListRoles(context.Context) ([]RoleRef, error) {
	return []RoleRef{{ID: tRole, Name: "运维", Enabled: true}}, nil
}

func newSpaceUsecaseForTest() (*SpaceUsecase, *fakeSpaceRepo) {
	repo := &fakeSpaceRepo{
		spaces: map[string]*Space{
			tPub:  {ID: tPub, Code: "pub", Name: "公开", Visibility: VisibilityPublic, OwnerID: tOwner},
			tPriv: {ID: tPriv, Code: "priv", Name: "受限", Visibility: VisibilityRestricted, OwnerID: tOwner},
		},
		acl: map[string][]*AclEntry{tPriv: {{SubjectType: SubjectRole, SubjectID: tRole, Level: LevelWrite}}},
	}
	return NewSpaceUsecase(repo, fakeRoles{}, fakeDirectory{}, fakeRoleDir{}), repo
}

func TestAccessResolve(t *testing.T) {
	t.Parallel()
	uc, _ := newSpaceUsecaseForTest()
	ctx := context.Background()

	admin, err := uc.Resolve(ctx, tAdmin)
	if err != nil || !admin.Unrestricted() || admin.Level(tPriv) != LevelManage || admin.IDs(LevelRead) != nil {
		t.Fatalf("admin access = %+v, %v", admin, err)
	}
	owner, err := uc.Resolve(ctx, tOwner)
	if err != nil || owner.Level(tPub) != LevelManage || owner.Level(tPriv) != LevelManage {
		t.Fatalf("owner access = %+v, %v", owner, err)
	}
	reader, err := uc.Resolve(ctx, tReader)
	if err != nil || reader.Level(tPub) != LevelRead || reader.Level(tPriv) != LevelWrite || !reader.Can(tPriv, LevelWrite) || reader.Can(tPriv, LevelManage) {
		t.Fatalf("reader access = %+v, %v", reader, err)
	}
	if ids := reader.IDs(LevelWrite); len(ids) != 1 || ids[0] != tPriv {
		t.Fatalf("reader write ids = %v", ids)
	}
	stranger, err := uc.Resolve(ctx, "018f3e1a-0000-7000-8000-0000000000ff")
	if err != nil || stranger.Level(tPub) != LevelRead || stranger.Level(tPriv) != LevelNone {
		t.Fatalf("stranger access = %+v, %v", stranger, err)
	}
}

func TestRequireHidesInvisibleSpaces(t *testing.T) {
	t.Parallel()
	uc, _ := newSpaceUsecaseForTest()
	ctx := context.Background()
	stranger := "018f3e1a-0000-7000-8000-0000000000ff"
	if _, _, err := uc.Require(ctx, stranger, tPriv, LevelRead); !errors.Is(err, ErrSpaceNotFound) {
		t.Fatalf("invisible space must look absent, got %v", err)
	}
	if _, _, err := uc.Require(ctx, stranger, tPub, LevelWrite); !errors.Is(err, ErrForbidden) {
		t.Fatalf("insufficient level must be forbidden, got %v", err)
	}
	if s, acc, err := uc.Require(ctx, tReader, tPriv, LevelWrite); err != nil || s.Level != LevelWrite || acc.Level(tPriv) != LevelWrite {
		t.Fatalf("reader write on priv = %+v, %v", s, err)
	}
}

func TestSetAclValidation(t *testing.T) {
	t.Parallel()
	uc, repo := newSpaceUsecaseForTest()
	ctx := context.Background()
	// 非负责人不能改 ACL。
	if _, err := uc.SetAcl(ctx, tReader, tPriv, nil); !errors.Is(err, ErrForbidden) {
		t.Fatalf("reader set acl = %v", err)
	}
	bad := [][]*AclEntry{
		{{SubjectType: SubjectUser, SubjectID: "not-a-uuid", Level: LevelRead}},
		{{SubjectType: SubjectUser, SubjectID: "018f3e1a-0000-7000-8000-0000000000ee", Level: LevelRead}}, // 用户不存在
		{{SubjectType: SubjectRole, SubjectID: tReader, Level: LevelRead}},                                // 角色不存在
		{{SubjectType: SubjectUser, SubjectID: tReader, Level: 9}},
		{{SubjectType: SubjectUser, SubjectID: tReader, Level: LevelRead}, {SubjectType: SubjectUser, SubjectID: tReader, Level: LevelWrite}}, // 重复
	}
	for i, entries := range bad {
		if _, err := uc.SetAcl(ctx, tOwner, tPriv, entries); !errors.Is(err, ErrACLInvalid) {
			t.Errorf("case %d: err = %v, want ErrACLInvalid", i, err)
		}
	}
	got, err := uc.SetAcl(ctx, tOwner, tPriv, []*AclEntry{
		{SubjectType: SubjectUser, SubjectID: tReader, Level: LevelRead},
		{SubjectType: SubjectRole, SubjectID: tRole, Level: LevelManage},
	})
	if err != nil || len(got) != 2 || got[0].SubjectName == "" || got[1].SubjectName != "运维" {
		t.Fatalf("set acl = %+v, %v", got, err)
	}
	if len(repo.acl[tPriv]) != 2 {
		t.Fatalf("acl not persisted: %v", repo.acl[tPriv])
	}
}

func TestScopeSpaces(t *testing.T) {
	t.Parallel()
	admin := &Access{Admin: true}
	if unrestricted, ids, ok := scopeSpaces(admin, nil); !unrestricted || ids != nil || !ok {
		t.Fatalf("admin unscoped = %v %v %v", unrestricted, ids, ok)
	}
	if unrestricted, ids, ok := scopeSpaces(admin, []string{tPub}); unrestricted || len(ids) != 1 || !ok {
		t.Fatalf("admin scoped = %v %v %v", unrestricted, ids, ok)
	}
	user := &Access{levels: map[string]int32{tPub: LevelRead}}
	if _, ids, ok := scopeSpaces(user, []string{tPriv}); ok || len(ids) != 0 {
		t.Fatalf("requested invisible space must yield nothing: %v %v", ids, ok)
	}
	if _, ids, ok := scopeSpaces(user, []string{tPub, tPriv}); !ok || len(ids) != 1 || ids[0] != tPub {
		t.Fatalf("intersection = %v %v", ids, ok)
	}
	if _, _, ok := scopeSpaces(&Access{levels: map[string]int32{}}, nil); ok {
		t.Fatal("no visible spaces must yield nothing")
	}
}

func TestCheckContent(t *testing.T) {
	t.Parallel()
	if err := checkContent("正常内容"); err != nil {
		t.Fatal(err)
	}
	err := checkContent("line\npassword=RealPassw0rd!")
	if !errors.Is(err, ErrSecretDetected) {
		t.Fatalf("secret must be rejected, got %v", err)
	}
	if md := kerrors.FromError(err).GetMetadata(); md["line"] != "2" || md["kind"] != "password_assignment" {
		t.Fatalf("metadata = %v", md)
	}
	if err := checkContent(string(make([]byte, MaxContentBytes+1))); !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("oversize = %v", err)
	}
}

func TestParseSummary(t *testing.T) {
	t.Parallel()
	s, tags := parseSummary("好的，结果如下：{\"summary\": \" 这是摘要 \", \"tags\": [\"nginx\", \"nginx\", \"\", \"排障\"]}")
	if s != "这是摘要" || len(tags) != 2 || tags[0] != "nginx" || tags[1] != "排障" {
		t.Fatalf("parse = %q %v", s, tags)
	}
	s, tags = parseSummary("模型没有按格式输出，只给了一段话。")
	if s != "模型没有按格式输出，只给了一段话。" || tags != nil {
		t.Fatalf("fallback = %q %v", s, tags)
	}
	if s, _ := parseSummary("{}"); s != "{}" {
		t.Fatalf("empty json falls back to raw text, got %q", s)
	}
}
