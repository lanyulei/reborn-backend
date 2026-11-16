package biz

import (
	"context"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 部门状态常量（部门仅启用/停用两态，与角色同口径）。
const (
	// DeptStatusDisabled 停用。
	DeptStatusDisabled int32 = 0
	// DeptStatusEnabled 启用。
	DeptStatusEnabled int32 = 1
)

// 组织架构容量上限：部门列表不分页（前端整树渲染），总数与层级必须有界；
// 单用户所属部门数、单部门负责人数同样有界（与 proto 校验一致）。
const (
	// MaxDeptDepth 部门层级上限（根部门为第 1 级）。
	MaxDeptDepth = 10
	// MaxDeptCount 未删除部门总数上限。
	MaxDeptCount = 2000
	// MaxUserDepts 单个用户所属部门数上限。
	MaxUserDepts = 20
	// deptCandidateLimit 可选用户单次返回上限（靠关键字缩小范围，不翻页）。
	deptCandidateLimit = 50
)

// DeptLeader 是部门负责人的只读视图（用户展示信息）。
type DeptLeader struct {
	UserID   string
	Username string
	Nickname string
	Avatar   string
	// Status 用户状态；非可用的负责人在流程解析处理人时被跳过。
	Status int32
}

// Dept 是部门领域模型（DO）；与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
// ParentID 空串表示根部门；Path 为祖先链（含自身）的物化路径，由 data 层在
// 结构锁内计算，调用方只读。Leaders/MemberCount 仅读路径填充。
type Dept struct {
	ID          string
	ParentID    string
	Path        string
	Name        string
	Code        string
	Sort        int32
	Status      int32
	Description string
	Leaders     []DeptLeader
	MemberCount int32
	CreateTime  time.Time
	UpdateTime  time.Time
}

// DeptUpdate 是部门部分更新；nil 字段不更新。LeaderIDs 非 nil 时整组替换
// （空切片 = 清空负责人）。上级部门不在此更新，见 DeptMover。
type DeptUpdate struct {
	Name        *string
	Code        *string
	Sort        *int32
	Status      *int32
	Description *string
	LeaderIDs   *[]string
}

// UserDept 是用户所属部门的只读视图。
type UserDept struct {
	ID      string
	Name    string
	Primary bool
}

// UserDeptAssignment 是用户所属部门的整组替换入参：DeptIDs 为目标全集（空 =
// 不属于任何部门），PrimaryDeptID 为主部门（须在 DeptIDs 内）。
type UserDeptAssignment struct {
	DeptIDs       []string
	PrimaryDeptID string
}

// lowerIDs 返回 ID 的小写副本（保序）。UUID 的文本形式不区分大小写，而去重、
// 定序加锁与路径拼接都按字符串进行：入口处统一为库内的规范（小写）形式。
func lowerIDs(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strings.ToLower(id)
	}
	return out
}

// uniqueLowerIDs 返回小写、去重且保持首次出现顺序的 ID（负责人顺序有业务含义）。
func uniqueLowerIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.ToLower(id)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// normalized 返回规整后的副本：ID 统一小写、去重排序；未指定主部门时取入参的
// 第一个部门；主部门不在 DeptIDs 内返回 ErrDeptNotFound（proto 层已校验，
// 此处为内部调用兜底）。
func (a UserDeptAssignment) normalized() (UserDeptAssignment, error) {
	if len(a.DeptIDs) == 0 {
		return UserDeptAssignment{}, nil
	}
	if len(a.DeptIDs) > MaxUserDepts {
		return UserDeptAssignment{}, ErrDeptLimitExceeded
	}
	ids := lowerIDs(a.DeptIDs)
	out := UserDeptAssignment{DeptIDs: SortedUnique(ids), PrimaryDeptID: strings.ToLower(a.PrimaryDeptID)}
	if out.PrimaryDeptID == "" {
		out.PrimaryDeptID = ids[0]
		return out, nil
	}
	for _, id := range out.DeptIDs {
		if id == out.PrimaryDeptID {
			return out, nil
		}
	}
	return UserDeptAssignment{}, ErrDeptNotFound
}

// 部门域哨兵错误：reason 与 api/system/v1 的 ErrorReason 枚举对齐。
var (
	// ErrDeptNotFound 目标部门（或指定的上级部门）不存在。
	ErrDeptNotFound = errors.NotFound(v1.ErrorReason_DEPT_NOT_FOUND.String(), "department not found")
	// ErrDeptNameExists 同一上级部门下名称已被占用。
	ErrDeptNameExists = errors.Conflict(v1.ErrorReason_DEPT_NAME_EXISTS.String(), "department name already exists under the same parent")
	// ErrDeptCodeExists 部门编码已被占用。
	ErrDeptCodeExists = errors.Conflict(v1.ErrorReason_DEPT_CODE_EXISTS.String(), "department code already exists")
	// ErrDeptHasChildren 部门下仍有下级部门，不可删除。
	ErrDeptHasChildren = errors.Conflict(v1.ErrorReason_DEPT_HAS_CHILDREN.String(), "department still has sub-departments")
	// ErrDeptHasMembers 部门下仍有成员，不可删除。
	ErrDeptHasMembers = errors.Conflict(v1.ErrorReason_DEPT_HAS_MEMBERS.String(), "department still has members")
	// ErrDeptMoveInvalid 不能把部门移到自身或自身的下级部门之下。
	ErrDeptMoveInvalid = errors.BadRequest(v1.ErrorReason_DEPT_MOVE_INVALID.String(), "cannot move a department under itself or its descendants")
	// ErrDeptDepthExceeded 部门层级超过上限。
	ErrDeptDepthExceeded = errors.BadRequest(v1.ErrorReason_DEPT_DEPTH_EXCEEDED.String(), "department depth limit exceeded")
	// ErrDeptLimitExceeded 部门总数或单个用户所属部门数超过上限。
	ErrDeptLimitExceeded = errors.BadRequest(v1.ErrorReason_DEPT_LIMIT_EXCEEDED.String(), "department limit exceeded")
)

// DeptReader 是部门只读仓储接口，由 data 层实现（依赖倒置）。
type DeptReader interface {
	// GetDept 按 ID 查询未删除部门（含负责人与直属成员数）；不存在返回 ErrDeptNotFound。
	GetDept(ctx context.Context, id string) (*Dept, error)
	// ListDepts 查询全部未删除部门（含负责人与直属成员数）：上级在前，同级按 sort、
	// 创建时间升序；总数受 MaxDeptCount 约束，不分页。
	ListDepts(ctx context.Context) ([]*Dept, error)
}

// DeptWriter 是部门写仓储接口，由 data 层实现（依赖倒置）。结构性写（新建/
// 删除，以及 DeptMover 的移动）在实现内经同一把跨实例锁串行。
type DeptWriter interface {
	// CreateDept 创建部门并写入负责人（同事务）：上级不存在返回 ErrDeptNotFound，
	// 负责人不存在返回 ErrUserNotFound，名称/编码冲突、层级/总数超限返回对应哨兵。
	// 成功后回填 dept.Path。
	CreateDept(ctx context.Context, dept *Dept, leaderIDs []string) error
	// UpdateDept 按 ID 部分更新未删除部门（含负责人整组替换，同事务）；
	// 不存在返回 ErrDeptNotFound。
	UpdateDept(ctx context.Context, id string, upd DeptUpdate) error
	// DeleteDept 软删部门并清除其负责人；不存在返回 ErrDeptNotFound，仍有下级
	// 部门/成员返回 ErrDeptHasChildren/ErrDeptHasMembers。
	DeleteDept(ctx context.Context, id string) error
}

// DeptMover 是部门移动仓储接口，由 data 层实现（依赖倒置）。
type DeptMover interface {
	// MoveDept 把部门连同子树移到 parentID 之下（空串 = 移为根部门）：部门或
	// 目标上级不存在返回 ErrDeptNotFound，目标为自身或其下级返回 ErrDeptMoveInvalid，
	// 移动后层级超限返回 ErrDeptDepthExceeded，目标上级下重名返回 ErrDeptNameExists。
	MoveDept(ctx context.Context, id, parentID string) error
}

// DeptMemberRepo 是部门成员仓储接口，由 data 层实现（依赖倒置）。
type DeptMemberRepo interface {
	// AddDeptMembers 把用户加入部门（已在部门内的跳过）；用户尚无主部门时该
	// 部门成为其主部门。部门不存在返回 ErrDeptNotFound，任一用户不存在返回
	// ErrUserNotFound，任一用户所属部门数将超限返回 ErrDeptLimitExceeded。
	AddDeptMembers(ctx context.Context, deptID string, userIDs []string) error
	// RemoveDeptMember 把用户移出部门（不在部门内为幂等 no-op）；移出的是主部门
	// 且用户仍属其他部门时，最早加入的部门顶替为主部门。
	RemoveDeptMember(ctx context.Context, deptID, userID string) error
	// ListUserDepts 批量查询用户所属的未删除部门（主部门在前）；无部门的用户不出现在结果中。
	ListUserDepts(ctx context.Context, userIDs []string) (map[string][]UserDept, error)
}

// DeptUsecase 是部门用例：承载业务规则；树结构不变式（路径、层级、无环）
// 需与写入同事务判定，收敛在仓储实现的结构锁内。
type DeptUsecase struct {
	reader  DeptReader
	writer  DeptWriter
	mover   DeptMover
	members DeptMemberRepo
	users   UserReader
}

// NewDeptUsecase new a DeptUsecase；users 供成员列表与可选用户查询。
func NewDeptUsecase(reader DeptReader, writer DeptWriter, mover DeptMover, members DeptMemberRepo, users UserReader) *DeptUsecase {
	return &DeptUsecase{reader: reader, writer: writer, mover: mover, members: members, users: users}
}

// Create 创建部门：生成 ID、填充审计时间后连同负责人落库；返回含负责人的完整实体。
func (uc *DeptUsecase) Create(ctx context.Context, dept *Dept, leaderIDs []string) (*Dept, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	dept.ID = id
	dept.Status = DeptStatusEnabled
	dept.CreateTime, dept.UpdateTime = now, now
	dept.ParentID = strings.ToLower(dept.ParentID)
	if err := uc.writer.CreateDept(ctx, dept, uniqueLowerIDs(leaderIDs)); err != nil {
		return nil, err
	}
	return uc.reader.GetDept(ctx, id)
}

// Update 部分更新部门并返回更新后的实体；不存在返回 ErrDeptNotFound。
func (uc *DeptUsecase) Update(ctx context.Context, id string, upd DeptUpdate) (*Dept, error) {
	if upd.LeaderIDs != nil {
		leaders := uniqueLowerIDs(*upd.LeaderIDs)
		upd.LeaderIDs = &leaders
	}
	if err := uc.writer.UpdateDept(ctx, id, upd); err != nil {
		return nil, err
	}
	return uc.reader.GetDept(ctx, id)
}

// Move 调整部门的上级（parentID 空串 = 移为根部门）。
func (uc *DeptUsecase) Move(ctx context.Context, id, parentID string) error {
	if strings.EqualFold(id, parentID) {
		return ErrDeptMoveInvalid
	}
	return uc.mover.MoveDept(ctx, id, strings.ToLower(parentID))
}

// Delete 软删部门；仍有下级部门或成员时拒绝。
func (uc *DeptUsecase) Delete(ctx context.Context, id string) error {
	return uc.writer.DeleteDept(ctx, id)
}

// Get 查询部门详情。
func (uc *DeptUsecase) Get(ctx context.Context, id string) (*Dept, error) {
	return uc.reader.GetDept(ctx, id)
}

// List 查询全部部门（平铺）。
func (uc *DeptUsecase) List(ctx context.Context) ([]*Dept, error) {
	return uc.reader.ListDepts(ctx)
}

// AddMembers 把用户加入部门。
func (uc *DeptUsecase) AddMembers(ctx context.Context, deptID string, userIDs []string) error {
	return uc.members.AddDeptMembers(ctx, deptID, SortedUnique(lowerIDs(userIDs)))
}

// RemoveMember 把用户移出部门；部门不存在返回 ErrDeptNotFound。
func (uc *DeptUsecase) RemoveMember(ctx context.Context, deptID, userID string) error {
	if _, err := uc.reader.GetDept(ctx, deptID); err != nil {
		return err
	}
	return uc.members.RemoveDeptMember(ctx, deptID, userID)
}

// Members 分页查询部门成员（includeSub 时含全部下级部门）及其所属部门；
// 部门不存在返回 ErrDeptNotFound。
func (uc *DeptUsecase) Members(ctx context.Context, deptID string, includeSub bool, keyword string, page, pageSize int64) ([]*User, map[string][]UserDept, int64, error) {
	if _, err := uc.reader.GetDept(ctx, deptID); err != nil {
		return nil, nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	users, total, err := uc.users.ListUsers(ctx, UserFilter{Keyword: keyword, NameOnly: true, DeptID: deptID, IncludeSubDepts: includeSub}, page, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}
	ids := make([]string, 0, len(users))
	for _, u := range users {
		ids = append(ids, u.ID)
	}
	depts, err := uc.UserDepts(ctx, ids)
	if err != nil {
		return nil, nil, 0, err
	}
	return users, depts, total, nil
}

// Candidates 可选用户：可用状态的用户按关键字匹配，最多 deptCandidateLimit 个。
func (uc *DeptUsecase) Candidates(ctx context.Context, keyword string) ([]*User, error) {
	enabled := StatusEnabled
	users, _, err := uc.users.ListUsers(ctx, UserFilter{Keyword: keyword, NameOnly: true, Status: &enabled}, 1, deptCandidateLimit)
	return users, err
}

// UserDepts 批量查询用户所属部门（主部门在前）；供用户详情/列表填充。
func (uc *DeptUsecase) UserDepts(ctx context.Context, userIDs []string) (map[string][]UserDept, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	return uc.members.ListUserDepts(ctx, userIDs)
}
