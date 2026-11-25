package biz

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// Access 一个主体对全部空间的权限视图。
type Access struct {
	// Admin 超级管理员：全部空间管理级别。
	Admin  bool
	levels map[string]int32
}

// Level 对某空间的级别。
func (a *Access) Level(spaceID string) int32 {
	if a.Admin {
		return LevelManage
	}
	return a.levels[spaceID]
}

// Can 是否至少具备 level。
func (a *Access) Can(spaceID string, level int32) bool { return a.Level(spaceID) >= level }

// IDs 级别 ≥ min 的空间 ID（超管返回 nil，配合 Unrestricted 使用）。
func (a *Access) IDs(minLevel int32) []string {
	if a.Admin {
		return nil
	}
	out := make([]string, 0, len(a.levels))
	for id, lv := range a.levels {
		if lv >= minLevel {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Unrestricted 是否不限空间。
func (a *Access) Unrestricted() bool { return a.Admin }

// SpaceUsecase 知识空间用例：权限解析、空间与 ACL 管理。
type SpaceUsecase struct {
	repo  SpaceRepo
	roles RoleSource
	dir   Directory
	rdir  RoleDirectory
	now   func() time.Time
}

// NewSpaceUsecase new a SpaceUsecase.
func NewSpaceUsecase(repo SpaceRepo, roles RoleSource, dir Directory, rdir RoleDirectory) *SpaceUsecase {
	return &SpaceUsecase{repo: repo, roles: roles, dir: dir, rdir: rdir, now: time.Now}
}

// Resolve 解析用户的权限视图：超管直通；否则按公开空间 / 负责人 / ACL（用户 + 启用角色）取最大级别。
func (uc *SpaceUsecase) Resolve(ctx context.Context, userID string) (*Access, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	if isAdmin {
		return &Access{Admin: true}, nil
	}
	levels, err := uc.repo.Levels(ctx, userID, roleIDs)
	if err != nil {
		return nil, err
	}
	if levels == nil {
		levels = map[string]int32{}
	}
	return &Access{levels: levels}, nil
}

// Require 取空间并要求至少 level；不可见按不存在处理（不暴露存在性），可见但级别不足返回 ErrForbidden。
func (uc *SpaceUsecase) Require(ctx context.Context, userID, spaceID string, level int32) (*Space, *Access, error) {
	acc, err := uc.Resolve(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	s, err := uc.repo.Get(ctx, spaceID)
	if err != nil {
		return nil, nil, err
	}
	lv := acc.Level(s.ID)
	if lv == LevelNone {
		return nil, nil, ErrSpaceNotFound
	}
	if lv < level {
		return nil, nil, ErrForbidden
	}
	s.Level = lv
	return s, acc, nil
}

// List 当前用户可见的空间（带级别、已发布文档数、负责人名）。
func (uc *SpaceUsecase) List(ctx context.Context, userID, keyword, typ string) ([]*Space, error) {
	acc, err := uc.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	all, err := uc.repo.List(ctx, keyword, typ)
	if err != nil {
		return nil, err
	}
	counts, err := uc.repo.DocumentCounts(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*Space, 0, len(all))
	for _, s := range all {
		if lv := acc.Level(s.ID); lv > LevelNone {
			s.Level = lv
			s.DocumentCount = counts[s.ID]
			out = append(out, s)
		}
	}
	if err := uc.fillOwnerNames(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// Get 单个空间（需可见）。
func (uc *SpaceUsecase) Get(ctx context.Context, userID, id string) (*Space, error) {
	s, _, err := uc.Require(ctx, userID, id, LevelRead)
	if err != nil {
		return nil, err
	}
	counts, err := uc.repo.DocumentCounts(ctx)
	if err != nil {
		return nil, err
	}
	s.DocumentCount = counts[s.ID]
	if err := uc.fillOwnerNames(ctx, []*Space{s}); err != nil {
		return nil, err
	}
	return s, nil
}

// CreateSpaceInput 新建空间入参。
type CreateSpaceInput struct {
	Code, Name, Type, Description string
	Visibility                    int32
	OwnerID                       string
	EmbeddingEnabled              bool
	ReviewRequired                bool
	ReviewDays                    int32
}

// Create 新建空间；负责人缺省为创建者。
func (uc *SpaceUsecase) Create(ctx context.Context, userID string, in CreateSpaceInput) (*Space, error) {
	if _, ok := spaceTypes[in.Type]; !ok {
		return nil, ErrInvalidArgument
	}
	if in.Visibility != VisibilityPublic && in.Visibility != VisibilityRestricted {
		return nil, ErrInvalidArgument
	}
	// proto 的 min_len 校验的是原始字符串，纯空白能通过；名称去掉首尾空白后不能为空。
	if strings.TrimSpace(in.Name) == "" {
		return nil, ErrInvalidArgument
	}
	isAdmin, _, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	owner := in.OwnerID
	if owner == "" {
		owner = userID
	}
	if err := uc.checkUsers(ctx, []string{owner}); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := uc.now()
	s := &Space{
		ID: id, Code: strings.ToLower(in.Code), Name: strings.TrimSpace(in.Name), Type: in.Type, Description: in.Description,
		Visibility: in.Visibility, OwnerID: owner, EmbeddingEnabled: in.EmbeddingEnabled, ReviewRequired: in.ReviewRequired,
		ReviewDays: in.ReviewDays, CreateUser: userID, CreateTime: now, UpdateTime: now,
	}
	if err := uc.repo.Create(ctx, s); err != nil {
		return nil, err
	}
	s.Level = creatorLevel(isAdmin, owner == userID, s.Visibility)
	return s, nil
}

// creatorLevel 新建空间的创建者对它的级别，口径同数据层的 Levels：超级管理员与负责人是管理级别；否则公开空间
// 可读、受限空间不可见（刚建的空间还没有 ACL）。前端按这个级别渲染管理入口，不能恒报管理级别。
func creatorLevel(isAdmin, isOwner bool, visibility int32) int32 {
	switch {
	case isAdmin || isOwner:
		return LevelManage
	case visibility == VisibilityPublic:
		return LevelRead
	}
	return LevelNone
}

// SpacePatch 修改空间入参（nil = 不改）。
type SpacePatch struct {
	Name, Type, Description *string
	Visibility              *int32
	OwnerID                 *string
	EmbeddingEnabled        *bool
	ReviewRequired          *bool
	ReviewDays              *int32
}

// Update 修改空间（需管理级别）。
func (uc *SpaceUsecase) Update(ctx context.Context, userID, id string, p SpacePatch) (*Space, error) {
	s, _, err := uc.Require(ctx, userID, id, LevelManage)
	if err != nil {
		return nil, err
	}
	if p.Name != nil {
		name := strings.TrimSpace(*p.Name)
		if name == "" {
			return nil, ErrInvalidArgument
		}
		s.Name = name
	}
	if p.Type != nil {
		if _, ok := spaceTypes[*p.Type]; !ok {
			return nil, ErrInvalidArgument
		}
		s.Type = *p.Type
	}
	if p.Description != nil {
		s.Description = *p.Description
	}
	if p.Visibility != nil {
		if *p.Visibility != VisibilityPublic && *p.Visibility != VisibilityRestricted {
			return nil, ErrInvalidArgument
		}
		s.Visibility = *p.Visibility
	}
	if p.OwnerID != nil {
		if err := uc.checkUsers(ctx, []string{*p.OwnerID}); err != nil {
			return nil, err
		}
		s.OwnerID = *p.OwnerID
	}
	if p.EmbeddingEnabled != nil {
		s.EmbeddingEnabled = *p.EmbeddingEnabled
	}
	if p.ReviewRequired != nil {
		s.ReviewRequired = *p.ReviewRequired
	}
	if p.ReviewDays != nil {
		if *p.ReviewDays < 0 {
			return nil, ErrInvalidArgument
		}
		s.ReviewDays = *p.ReviewDays
	}
	s.UpdateTime = uc.now()
	if err := uc.repo.Update(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

// Delete 软删空间（需管理级别；仍有文档时拒绝）。
func (uc *SpaceUsecase) Delete(ctx context.Context, userID, id string) error {
	if _, _, err := uc.Require(ctx, userID, id, LevelManage); err != nil {
		return err
	}
	return uc.repo.SoftDelete(ctx, id, uc.now())
}

// GetAcl 空间 ACL（需管理级别），带主体显示名。
func (uc *SpaceUsecase) GetAcl(ctx context.Context, userID, id string) ([]*AclEntry, error) {
	if _, _, err := uc.Require(ctx, userID, id, LevelManage); err != nil {
		return nil, err
	}
	entries, err := uc.repo.ListAcl(ctx, id)
	if err != nil {
		return nil, err
	}
	return entries, uc.fillSubjectNames(ctx, entries)
}

// SetAcl 整组替换 ACL（需管理级别）：主体须存在、不得重复。
func (uc *SpaceUsecase) SetAcl(ctx context.Context, userID, id string, entries []*AclEntry) ([]*AclEntry, error) {
	if _, _, err := uc.Require(ctx, userID, id, LevelManage); err != nil {
		return nil, err
	}
	if len(entries) > maxACLEntries {
		return nil, ErrACLInvalid
	}
	seen := make(map[string]struct{}, len(entries))
	var users, roles []string
	for _, e := range entries {
		if e.Level < LevelRead || e.Level > LevelManage || !idgen.IsUUID(e.SubjectID) {
			return nil, ErrACLInvalid
		}
		e.SubjectID = strings.ToLower(e.SubjectID)
		key := string(rune('0'+e.SubjectType)) + e.SubjectID
		if _, dup := seen[key]; dup {
			return nil, ErrACLInvalid
		}
		seen[key] = struct{}{}
		switch e.SubjectType {
		case SubjectUser:
			users = append(users, e.SubjectID)
		case SubjectRole:
			roles = append(roles, e.SubjectID)
		case SubjectMachine:
			// 机器账号由 system 域后续提供；现阶段按用户校验（机器账号即用户的一个类型）。
			users = append(users, e.SubjectID)
		default:
			return nil, ErrACLInvalid
		}
	}
	if err := uc.checkUsers(ctx, users); err != nil {
		return nil, err
	}
	if len(roles) > 0 {
		all, err := uc.rdir.ListRoles(ctx)
		if err != nil {
			return nil, err
		}
		for _, rid := range roles {
			if !slices.ContainsFunc(all, func(r RoleRef) bool { return r.ID == rid }) {
				return nil, ErrACLInvalid
			}
		}
	}
	if err := uc.repo.ReplaceAcl(ctx, id, entries, userID); err != nil {
		return nil, err
	}
	return entries, uc.fillSubjectNames(ctx, entries)
}

// checkUsers 校验用户存在（经组织目录的显示名查询）。
func (uc *SpaceUsecase) checkUsers(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	names, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, ok := names[id]; !ok {
			return ErrACLInvalid
		}
	}
	return nil
}

// fillOwnerNames 补负责人显示名。
func (uc *SpaceUsecase) fillOwnerNames(ctx context.Context, spaces []*Space) error {
	ids := make([]string, 0, len(spaces))
	for _, s := range spaces {
		ids = append(ids, s.OwnerID)
	}
	if len(ids) == 0 {
		return nil
	}
	names, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, s := range spaces {
		s.OwnerName = names[s.OwnerID]
	}
	return nil
}

// fillSubjectNames 补 ACL 主体显示名。
func (uc *SpaceUsecase) fillSubjectNames(ctx context.Context, entries []*AclEntry) error {
	var users []string
	hasRole := false
	for _, e := range entries {
		if e.SubjectType == SubjectRole {
			hasRole = true
		} else {
			users = append(users, e.SubjectID)
		}
	}
	names := map[string]string{}
	if len(users) > 0 {
		var err error
		if names, err = uc.dir.UserNames(ctx, users); err != nil {
			return err
		}
	}
	roleNames := map[string]string{}
	if hasRole {
		all, err := uc.rdir.ListRoles(ctx)
		if err != nil {
			return err
		}
		for _, r := range all {
			roleNames[r.ID] = r.Name
		}
	}
	for _, e := range entries {
		if e.SubjectType == SubjectRole {
			e.SubjectName = roleNames[e.SubjectID]
		} else {
			e.SubjectName = names[e.SubjectID]
		}
	}
	return nil
}
