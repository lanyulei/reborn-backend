package biz

import (
	"context"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// validIDs 小写、去重并丢弃不是 UUID 的项（保持首次出现顺序）。目录查询的入参来自流程定义
// 与表单取值（不可信），查询时要转成 uuid：非法形式在此丢弃，而不是让语句报错、连带回滚
// 调用方的事务。
func validIDs(ids []string) []string {
	out := uniqueLowerIDs(ids)
	return slices.DeleteFunc(out, func(id string) bool { return !idgen.IsUUID(id) })
}

// DirectoryRepo 组织目录只读仓储接口，由 data 层实现（依赖倒置）；供流程引擎等
// 跨域消费方解析处理人。全部为批量查询，无逐条 N+1。
type DirectoryRepo interface {
	// ListUserBriefs 给定 ID 中存在的用户（ID / 登录名 / 昵称）；activeOnly 时只含可用状态的。
	ListUserBriefs(ctx context.Context, ids []string, activeOnly bool) ([]UserBrief, error)
	// ListActiveUserIDsByRoles 持有任一给定角色（未删除且启用）的全部可用用户 ID。
	ListActiveUserIDsByRoles(ctx context.Context, roleCodes []string) ([]string, error)
	// ListDeptNames 给定 ID 中未删除部门的名称（ID → 名称）。
	ListDeptNames(ctx context.Context, ids []string) (map[string]string, error)
	// ListDeptChainLeaders 部门及其全部上级部门的可用负责人：按「由近及远」返回每一级
	// （chain[0] 为 deptID 自身），级内按负责人顺序。部门不存在返回空。
	ListDeptChainLeaders(ctx context.Context, deptID string) ([][]string, error)
}

// UserBrief 用户展示信息的最小视图。
type UserBrief struct {
	ID       string
	Username string
	Nickname string
}

// DirectoryUsecase 组织目录用例：跨域窄接口的实现方（流程引擎的 Directory / Users）。
type DirectoryUsecase struct {
	repo  DirectoryRepo
	users UserReader
	depts DeptMemberRepo
}

// NewDirectoryUsecase new a DirectoryUsecase.
func NewDirectoryUsecase(repo DirectoryRepo, users UserReader, depts DeptMemberRepo) *DirectoryUsecase {
	return &DirectoryUsecase{repo: repo, users: users, depts: depts}
}

// ActiveUserIDs 过滤出存在且为可用状态的用户，保持入参顺序并去重。
func (uc *DirectoryUsecase) ActiveUserIDs(ctx context.Context, ids []string) ([]string, error) {
	ids = validIDs(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	briefs, err := uc.repo.ListUserBriefs(ctx, ids, true)
	if err != nil {
		return nil, err
	}
	active := make(map[string]struct{}, len(briefs))
	for _, b := range briefs {
		active[b.ID] = struct{}{}
	}
	out := make([]string, 0, len(briefs))
	for _, id := range ids {
		if _, ok := active[id]; ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// UserIDsByRoles 持有任一给定角色（启用）的全部可用用户。
func (uc *DirectoryUsecase) UserIDsByRoles(ctx context.Context, roleCodes []string) ([]string, error) {
	if len(roleCodes) == 0 {
		return nil, nil
	}
	return uc.repo.ListActiveUserIDsByRoles(ctx, roleCodes)
}

// DeptLeaderIDs 部门负责人：从 deptID 向上第 level 级部门（1 = 自身）取可用的负责人
// （排除 exclude）；该级为空则继续向上，直到根部门。部门不存在或整条链为空返回空。
func (uc *DirectoryUsecase) DeptLeaderIDs(ctx context.Context, deptID string, level int, exclude string) ([]string, error) {
	deptID = strings.ToLower(deptID)
	if !idgen.IsUUID(deptID) {
		return nil, nil
	}
	chain, err := uc.repo.ListDeptChainLeaders(ctx, deptID)
	if err != nil {
		return nil, err
	}
	exclude = strings.ToLower(exclude)
	for i := max(level, 1) - 1; i < len(chain); i++ {
		leaders := make([]string, 0, len(chain[i]))
		for _, id := range chain[i] {
			if id != exclude {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) > 0 {
			return leaders, nil
		}
	}
	return nil, nil
}

// UserNames 批量取用户显示名（昵称优先，其次登录名）；不存在或非可用的用户不在结果中
// ——调用方对缺失项回落显示原始 ID。
func (uc *DirectoryUsecase) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	ids = validIDs(ids)
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	// 显示名用于历史记录：停用 / 离职的用户仍要能显示名字（已删除的除外）。
	briefs, err := uc.repo.ListUserBriefs(ctx, ids, false)
	if err != nil {
		return nil, err
	}
	names := make(map[string]string, len(briefs))
	for _, b := range briefs {
		names[b.ID] = b.displayName()
	}
	return names, nil
}

func (b UserBrief) displayName() string {
	if b.Nickname != "" {
		return b.Nickname
	}
	return b.Username
}

// DeptNames 批量取部门名称；不存在的部门不在结果中。
func (uc *DirectoryUsecase) DeptNames(ctx context.Context, ids []string) (map[string]string, error) {
	ids = validIDs(ids)
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	return uc.repo.ListDeptNames(ctx, ids)
}

// CandidateUsers 可用用户按关键字（登录名 / 昵称）匹配，至多 limit 个（最小披露）。
func (uc *DirectoryUsecase) CandidateUsers(ctx context.Context, keyword string, limit int64) ([]UserBrief, error) {
	enabled := StatusEnabled
	users, _, err := uc.users.ListUsers(ctx, UserFilter{Keyword: keyword, NameOnly: true, Status: &enabled}, 1, limit)
	if err != nil {
		return nil, err
	}
	out := make([]UserBrief, 0, len(users))
	for _, u := range users {
		out = append(out, UserBrief{ID: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return out, nil
}

// PrimaryDept 用户的主部门 ID 与其全部所属部门 ID；无部门返回空。
func (uc *DirectoryUsecase) PrimaryDept(ctx context.Context, userID string) (string, []string, error) {
	depts, err := uc.depts.ListUserDepts(ctx, []string{userID})
	if err != nil {
		return "", nil, err
	}
	var primary string
	all := make([]string, 0, len(depts[userID]))
	for _, d := range depts[userID] {
		all = append(all, d.ID)
		if d.Primary {
			primary = d.ID
		}
	}
	return primary, all, nil
}
