//go:build integration

package data

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
)

// deptTestID 生成固定前缀的测试部门 ID。
func deptTestID(n int) string {
	return fmt.Sprintf("018f3e1a-00d0-7000-8000-%012d", n)
}

// userTestID 生成固定前缀的测试用户 ID。
func userTestID(n int) string {
	return fmt.Sprintf("018f3e1a-00e0-7000-8000-%012d", n)
}

func newTestDept(n int, name, parentID string) *biz.Dept {
	now := time.Now().UTC()
	return &biz.Dept{
		ID: deptTestID(n), ParentID: parentID, Name: name, Status: biz.DeptStatusEnabled,
		CreateTime: now, UpdateTime: now,
	}
}

// seedUsers 建 n 个测试用户，返回其 ID（升序）。
func seedUsers(t *testing.T, repo *userRepo, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		u := newTestUser(userTestID(i), fmt.Sprintf("dept-user-%d", i))
		if err := repo.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("CreateUser() error = %v", err)
		}
		ids = append(ids, u.ID)
	}
	return ids
}

func TestIntegration_DeptTree(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	users := seedUsers(t, NewUserRepo(d), 2)
	ctx := context.Background()

	root := newTestDept(1, "研发中心", "")
	root.Code = "rd"
	if err := repo.CreateDept(ctx, root, []string{users[1], users[0]}); err != nil {
		t.Fatalf("CreateDept(root) error = %v", err)
	}
	if root.Path != "/"+root.ID+"/" {
		t.Fatalf("root path = %q", root.Path)
	}
	child := newTestDept(2, "后端组", root.ID)
	if err := repo.CreateDept(ctx, child, nil); err != nil {
		t.Fatalf("CreateDept(child) error = %v", err)
	}
	grand := newTestDept(3, "支付小组", child.ID)
	if err := repo.CreateDept(ctx, grand, nil); err != nil {
		t.Fatalf("CreateDept(grand) error = %v", err)
	}
	other := newTestDept(4, "运维中心", "")
	if err := repo.CreateDept(ctx, other, nil); err != nil {
		t.Fatalf("CreateDept(other) error = %v", err)
	}

	// 约束映射：同级重名 / 根部门之间重名 / 编码冲突 / 上级不存在 / 负责人不存在。
	if err := repo.CreateDept(ctx, newTestDept(10, "后端组", root.ID), nil); !errors.Is(err, biz.ErrDeptNameExists) {
		t.Fatalf("CreateDept(dup name) error = %v, want ErrDeptNameExists", err)
	}
	if err := repo.CreateDept(ctx, newTestDept(11, "研发中心", ""), nil); !errors.Is(err, biz.ErrDeptNameExists) {
		t.Fatalf("CreateDept(dup root name) error = %v, want ErrDeptNameExists", err)
	}
	dupCode := newTestDept(12, "另一个", "")
	dupCode.Code = "rd"
	if err := repo.CreateDept(ctx, dupCode, nil); !errors.Is(err, biz.ErrDeptCodeExists) {
		t.Fatalf("CreateDept(dup code) error = %v, want ErrDeptCodeExists", err)
	}
	if err := repo.CreateDept(ctx, newTestDept(13, "孤儿", missingID), nil); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("CreateDept(missing parent) error = %v, want ErrDeptNotFound", err)
	}
	if err := repo.CreateDept(ctx, newTestDept(14, "无主", ""), []string{missingID}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("CreateDept(missing leader) error = %v, want ErrUserNotFound", err)
	}

	// 读取：负责人按配置顺序；列表上级在前。
	got, err := repo.GetDept(ctx, root.ID)
	if err != nil {
		t.Fatalf("GetDept() error = %v", err)
	}
	if got.Code != "rd" || len(got.Leaders) != 2 || got.Leaders[0].UserID != users[1] || got.Leaders[1].UserID != users[0] {
		t.Fatalf("GetDept() = %+v", got)
	}
	if _, err := repo.GetDept(ctx, missingID); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("GetDept(missing) error = %v, want ErrDeptNotFound", err)
	}
	list, err := repo.ListDepts(ctx)
	if err != nil {
		t.Fatalf("ListDepts() error = %v", err)
	}
	if len(list) != 4 || list[2].ID != child.ID || list[3].ID != grand.ID {
		t.Fatalf("ListDepts() order = %v", deptIDs(list))
	}

	// 更新：字段 + 负责人整组替换；清空编码；不存在。
	name, code := "研发部", ""
	leaders := []string{users[0]}
	if err := repo.UpdateDept(ctx, root.ID, biz.DeptUpdate{Name: &name, Code: &code, LeaderIDs: &leaders}); err != nil {
		t.Fatalf("UpdateDept() error = %v", err)
	}
	if got, _ = repo.GetDept(ctx, root.ID); got.Name != name || got.Code != "" || len(got.Leaders) != 1 || got.Leaders[0].UserID != users[0] {
		t.Fatalf("UpdateDept() result = %+v", got)
	}
	if err := repo.UpdateDept(ctx, missingID, biz.DeptUpdate{Name: &name}); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("UpdateDept(missing) error = %v, want ErrDeptNotFound", err)
	}
	badLeaders := []string{missingID}
	if err := repo.UpdateDept(ctx, root.ID, biz.DeptUpdate{LeaderIDs: &badLeaders}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("UpdateDept(missing leader) error = %v, want ErrUserNotFound", err)
	}
	if got, _ = repo.GetDept(ctx, root.ID); len(got.Leaders) != 1 {
		t.Fatalf("leaders after failed replace = %+v, want rollback to 1", got.Leaders)
	}

	// 移动：成环拒绝；合法移动整体改写子树路径；回根；幂等。
	if err := repo.MoveDept(ctx, root.ID, grand.ID); !errors.Is(err, biz.ErrDeptMoveInvalid) {
		t.Fatalf("MoveDept(under descendant) error = %v, want ErrDeptMoveInvalid", err)
	}
	if err := repo.MoveDept(ctx, child.ID, other.ID); err != nil {
		t.Fatalf("MoveDept() error = %v", err)
	}
	if got, _ = repo.GetDept(ctx, grand.ID); got.Path != "/"+other.ID+"/"+child.ID+"/"+grand.ID+"/" {
		t.Fatalf("grandchild path after move = %q", got.Path)
	}
	if got, _ = repo.GetDept(ctx, child.ID); got.ParentID != other.ID {
		t.Fatalf("child parent after move = %q", got.ParentID)
	}
	if err := repo.MoveDept(ctx, child.ID, other.ID); err != nil {
		t.Fatalf("MoveDept(same parent) error = %v, want no-op", err)
	}
	if err := repo.MoveDept(ctx, child.ID, ""); err != nil {
		t.Fatalf("MoveDept(to root) error = %v", err)
	}
	if got, _ = repo.GetDept(ctx, grand.ID); got.Path != "/"+child.ID+"/"+grand.ID+"/" {
		t.Fatalf("grandchild path after move to root = %q", got.Path)
	}
	if err := repo.MoveDept(ctx, missingID, ""); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("MoveDept(missing) error = %v, want ErrDeptNotFound", err)
	}

	// 删除守卫与软删后名称可复用。
	if err := repo.DeleteDept(ctx, child.ID); !errors.Is(err, biz.ErrDeptHasChildren) {
		t.Fatalf("DeleteDept(with children) error = %v, want ErrDeptHasChildren", err)
	}
	if err := repo.DeleteDept(ctx, grand.ID); err != nil {
		t.Fatalf("DeleteDept() error = %v", err)
	}
	if err := repo.DeleteDept(ctx, grand.ID); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("DeleteDept(again) error = %v, want ErrDeptNotFound", err)
	}
	if err := repo.CreateDept(ctx, newTestDept(5, "支付小组", child.ID), nil); err != nil {
		t.Fatalf("CreateDept(reuse name after delete) error = %v", err)
	}
}

func deptIDs(depts []*biz.Dept) []string {
	ids := make([]string, 0, len(depts))
	for _, d := range depts {
		ids = append(ids, d.ID)
	}
	return ids
}

func TestIntegration_DeptDepthLimit(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	ctx := context.Background()

	// 建一条满深度的链。
	parent := ""
	for i := 1; i <= biz.MaxDeptDepth; i++ {
		dept := newTestDept(i, fmt.Sprintf("L%d", i), parent)
		if err := repo.CreateDept(ctx, dept, nil); err != nil {
			t.Fatalf("CreateDept(level %d) error = %v", i, err)
		}
		parent = dept.ID
	}
	if err := repo.CreateDept(ctx, newTestDept(50, "too-deep", parent), nil); !errors.Is(err, biz.ErrDeptDepthExceeded) {
		t.Fatalf("CreateDept(beyond depth) error = %v, want ErrDeptDepthExceeded", err)
	}
	// 两级子树挂到第 9 级之下将达到 11 级：拒绝；挂到第 8 级之下刚好 10 级：允许。
	a := newTestDept(60, "A", "")
	b := newTestDept(61, "B", a.ID)
	for _, dept := range []*biz.Dept{a, b} {
		if err := repo.CreateDept(ctx, dept, nil); err != nil {
			t.Fatalf("CreateDept(%s) error = %v", dept.Name, err)
		}
	}
	if err := repo.MoveDept(ctx, a.ID, deptTestID(9)); !errors.Is(err, biz.ErrDeptDepthExceeded) {
		t.Fatalf("MoveDept(subtree too deep) error = %v, want ErrDeptDepthExceeded", err)
	}
	if err := repo.MoveDept(ctx, a.ID, deptTestID(8)); err != nil {
		t.Fatalf("MoveDept(subtree fits) error = %v", err)
	}
}

func TestIntegration_DeptMembers(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	userRepo := NewUserRepo(d)
	users := seedUsers(t, userRepo, 3)
	ctx := context.Background()

	rd := newTestDept(1, "研发中心", "")
	backend := newTestDept(2, "后端组", rd.ID)
	ops := newTestDept(3, "运维中心", "")
	for _, dept := range []*biz.Dept{rd, backend, ops} {
		if err := repo.CreateDept(ctx, dept, nil); err != nil {
			t.Fatalf("CreateDept(%s) error = %v", dept.Name, err)
		}
	}
	userDepts := func(userID string) []biz.UserDept {
		t.Helper()
		m, err := repo.ListUserDepts(ctx, []string{userID})
		if err != nil {
			t.Fatalf("ListUserDepts() error = %v", err)
		}
		return m[userID]
	}

	// 加入：首个部门成为主部门；重复加入幂等；后续加入不改主部门。
	if err := repo.AddDeptMembers(ctx, backend.ID, users[:2]); err != nil {
		t.Fatalf("AddDeptMembers() error = %v", err)
	}
	if err := repo.AddDeptMembers(ctx, backend.ID, users[:1]); err != nil {
		t.Fatalf("AddDeptMembers(again) error = %v", err)
	}
	if err := repo.AddDeptMembers(ctx, ops.ID, users[:1]); err != nil {
		t.Fatalf("AddDeptMembers(second dept) error = %v", err)
	}
	if got := userDepts(users[0]); len(got) != 2 || got[0].ID != backend.ID || !got[0].Primary || got[1].Primary {
		t.Fatalf("user depts = %+v, want primary 后端组 first", got)
	}
	if err := repo.AddDeptMembers(ctx, missingID, users[:1]); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("AddDeptMembers(missing dept) error = %v, want ErrDeptNotFound", err)
	}
	if err := repo.AddDeptMembers(ctx, ops.ID, []string{missingID}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("AddDeptMembers(missing user) error = %v, want ErrUserNotFound", err)
	}
	if got, _ := repo.GetDept(ctx, backend.ID); got.MemberCount != 2 {
		t.Fatalf("member count = %d, want 2", got.MemberCount)
	}

	// 用户列表按部门过滤：直属 / 含下级。
	if _, total, err := userRepo.ListUsers(ctx, biz.UserFilter{DeptID: rd.ID}, 1, 10); err != nil || total != 0 {
		t.Fatalf("ListUsers(direct of 研发中心) total = %d err = %v, want 0", total, err)
	}
	if _, total, err := userRepo.ListUsers(ctx, biz.UserFilter{DeptID: rd.ID, IncludeSubDepts: true}, 1, 10); err != nil || total != 2 {
		t.Fatalf("ListUsers(subtree of 研发中心) total = %d err = %v, want 2", total, err)
	}
	if _, total, err := userRepo.ListUsers(ctx, biz.UserFilter{DeptID: missingID, IncludeSubDepts: true}, 1, 10); err != nil || total != 0 {
		t.Fatalf("ListUsers(missing dept) total = %d err = %v, want 0", total, err)
	}

	// 整组替换（经用户更新，同事务）：保留未变化归属，切换主部门；部门不存在整体回滚。
	nick := "renamed"
	assign := biz.UserDeptAssignment{DeptIDs: sorted(ops.ID, rd.ID), PrimaryDeptID: rd.ID}
	if err := userRepo.UpdateUser(ctx, users[0], biz.UserUpdate{Nickname: &nick, Depts: &assign}); err != nil {
		t.Fatalf("UpdateUser(depts) error = %v", err)
	}
	if got := userDepts(users[0]); len(got) != 2 || got[0].ID != rd.ID || !got[0].Primary || got[1].ID != ops.ID {
		t.Fatalf("user depts after replace = %+v", got)
	}
	nick2 := "must-rollback"
	bad := biz.UserDeptAssignment{DeptIDs: []string{missingID}, PrimaryDeptID: missingID}
	if err := userRepo.UpdateUser(ctx, users[0], biz.UserUpdate{Nickname: &nick2, Depts: &bad}); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("UpdateUser(missing dept) error = %v, want ErrDeptNotFound", err)
	}
	if u, _ := userRepo.GetUser(ctx, users[0]); u.Nickname != nick {
		t.Fatalf("nickname = %q, want %q (rolled back)", u.Nickname, nick)
	}
	if err := userRepo.UpdateUser(ctx, missingID, biz.UserUpdate{Depts: &assign}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("UpdateUser(missing user, depts) error = %v, want ErrUserNotFound", err)
	}

	// 建号同事务写部门；部门不存在则不建号。
	withDepts := newTestUser(userTestID(10), "with-depts")
	withDepts.Depts = &biz.UserDeptAssignment{DeptIDs: []string{backend.ID}, PrimaryDeptID: backend.ID}
	if err := userRepo.CreateUserWithRoles(ctx, withDepts, nil); err != nil {
		t.Fatalf("CreateUserWithRoles(depts) error = %v", err)
	}
	if got := userDepts(withDepts.ID); len(got) != 1 || !got[0].Primary {
		t.Fatalf("new user depts = %+v", got)
	}
	orphan := newTestUser(userTestID(11), "orphan")
	orphan.Depts = &bad
	if err := userRepo.CreateUserWithRoles(ctx, orphan, nil); !errors.Is(err, biz.ErrDeptNotFound) {
		t.Fatalf("CreateUserWithRoles(missing dept) error = %v, want ErrDeptNotFound", err)
	}
	if _, err := userRepo.GetUser(ctx, orphan.ID); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("GetUser(orphan) error = %v, want ErrUserNotFound (rolled back)", err)
	}

	// 移出主部门：最早加入的其余部门顶替；不在部门内幂等。
	if err := repo.RemoveDeptMember(ctx, rd.ID, users[0]); err != nil {
		t.Fatalf("RemoveDeptMember() error = %v", err)
	}
	if got := userDepts(users[0]); len(got) != 1 || got[0].ID != ops.ID || !got[0].Primary {
		t.Fatalf("user depts after removing primary = %+v", got)
	}
	if err := repo.RemoveDeptMember(ctx, rd.ID, users[0]); err != nil {
		t.Fatalf("RemoveDeptMember(again) error = %v", err)
	}

	// 有成员不可删；删除用户清归属与负责人身份后可删。
	if err := repo.DeleteDept(ctx, backend.ID); !errors.Is(err, biz.ErrDeptHasMembers) {
		t.Fatalf("DeleteDept(with members) error = %v, want ErrDeptHasMembers", err)
	}
	leaders := []string{users[1]}
	if err := repo.UpdateDept(ctx, backend.ID, biz.DeptUpdate{LeaderIDs: &leaders}); err != nil {
		t.Fatalf("UpdateDept(leaders) error = %v", err)
	}
	for _, id := range []string{users[1], withDepts.ID} {
		if err := userRepo.DeleteUser(ctx, id); err != nil {
			t.Fatalf("DeleteUser() error = %v", err)
		}
	}
	if got, _ := repo.GetDept(ctx, backend.ID); got.MemberCount != 0 || len(got.Leaders) != 0 {
		t.Fatalf("dept after deleting users = %+v, want no members/leaders", got)
	}
	if err := repo.DeleteDept(ctx, backend.ID); err != nil {
		t.Fatalf("DeleteDept(emptied) error = %v", err)
	}

	// 清空归属。
	if err := userRepo.UpdateUser(ctx, users[0], biz.UserUpdate{Depts: &biz.UserDeptAssignment{}}); err != nil {
		t.Fatalf("UpdateUser(clear depts) error = %v", err)
	}
	if got := userDepts(users[0]); len(got) != 0 {
		t.Fatalf("user depts after clear = %+v", got)
	}
}

func sorted(ids ...string) []string {
	slices.Sort(ids)
	return ids
}

// 多实例并发：同一用户被并发加入多个部门，主部门恒只有一个；互相移入对方之下
// 的并发移动不会成环。
func TestIntegration_DeptConcurrency(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	users := seedUsers(t, NewUserRepo(d), 1)
	ctx := context.Background()

	const n = 8
	for i := 1; i <= n; i++ {
		if err := repo.CreateDept(ctx, newTestDept(i, fmt.Sprintf("D%d", i), ""), nil); err != nil {
			t.Fatalf("CreateDept() error = %v", err)
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { errs[i] = repo.AddDeptMembers(ctx, deptTestID(i+1), users) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent AddDeptMembers[%d] error = %v", i, err)
		}
	}
	m, err := repo.ListUserDepts(ctx, users)
	if err != nil {
		t.Fatalf("ListUserDepts() error = %v", err)
	}
	primaries := 0
	for _, dept := range m[users[0]] {
		if dept.Primary {
			primaries++
		}
	}
	if len(m[users[0]]) != n || primaries != 1 {
		t.Fatalf("after concurrent adds: %d depts, %d primaries; want %d / 1", len(m[users[0]]), primaries, n)
	}

	// D1 → D2 之下 与 D2 → D1 之下并发：恰一个成功，另一个因成环被拒。
	var moveErrs [2]error
	wg.Go(func() { moveErrs[0] = repo.MoveDept(ctx, deptTestID(1), deptTestID(2)) })
	wg.Go(func() { moveErrs[1] = repo.MoveDept(ctx, deptTestID(2), deptTestID(1)) })
	wg.Wait()
	ok, invalid := 0, 0
	for _, err := range moveErrs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, biz.ErrDeptMoveInvalid):
			invalid++
		default:
			t.Fatalf("concurrent MoveDept error = %v", err)
		}
	}
	if ok != 1 || invalid != 1 {
		t.Fatalf("concurrent cross moves: ok=%d invalid=%d, want 1/1", ok, invalid)
	}
}

// 加锁顺序回归（全域统一「先用户、后部门」）：下列并发组合曾因顺序相反而互相等待，
// 任一迭代出现死锁（PG 40P01）即以非预期错误失败。
func TestIntegration_DeptLockOrdering(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	userRepo := NewUserRepo(d)
	users := seedUsers(t, userRepo, 2)
	ctx := context.Background()

	parent := newTestDept(1, "P", "")
	a := newTestDept(2, "A", parent.ID)
	b := newTestDept(3, "B", parent.ID)
	other := newTestDept(4, "O", "")
	for _, dept := range []*biz.Dept{parent, a, b, other} {
		if err := repo.CreateDept(ctx, dept, nil); err != nil {
			t.Fatalf("CreateDept(%s) error = %v", dept.Name, err)
		}
	}

	const rounds = 40
	run := func(name string, fns ...func(i int) error) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make([]error, len(fns))
		for k, fn := range fns {
			wg.Go(func() {
				for i := range rounds {
					if err := fn(i); err != nil {
						errs[k] = err
						return
					}
				}
			})
		}
		wg.Wait()
		for k, err := range errs {
			if err != nil {
				t.Fatalf("%s: worker %d error = %v", name, k, err)
			}
		}
	}

	// 1) 改用户并替换其部门归属 ↔ 改部门并替换其负责人（同一对 用户/部门）。
	assign := biz.UserDeptAssignment{DeptIDs: []string{a.ID}, PrimaryDeptID: a.ID}
	leaders := []string{users[0]}
	nick := "n"
	run("user depts vs dept leaders",
		func(int) error {
			return userRepo.UpdateUser(ctx, users[0], biz.UserUpdate{Nickname: &nick, Depts: &assign})
		},
		func(int) error { return repo.UpdateDept(ctx, a.ID, biz.DeptUpdate{LeaderIDs: &leaders}) },
	)

	// 2) 用户归属跨多个部门 ↔ 移动包含这些部门的子树（成批改写路径）↔ 往其中一个部门加成员。
	both := biz.UserDeptAssignment{DeptIDs: sorted(a.ID, b.ID), PrimaryDeptID: a.ID}
	run("user depts vs subtree move vs add members",
		func(int) error { return userRepo.UpdateUser(ctx, users[0], biz.UserUpdate{Depts: &both}) },
		func(i int) error {
			target := other.ID
			if i%2 == 1 {
				target = ""
			}
			return repo.MoveDept(ctx, parent.ID, target)
		},
		func(int) error { return repo.AddDeptMembers(ctx, b.ID, users) },
	)

	// 3) 删除用户 ↔ 改部门并保留该用户为负责人：改部门要么成功、要么因用户已删返回
	//    ErrUserNotFound；无论先后，都不得留下指向已删除用户的负责人行。
	for i := range rounds {
		victim := newTestUser(userTestID(100+i), fmt.Sprintf("victim-%03d", i))
		victim.Phone = fmt.Sprintf("139%08d", i)
		if err := userRepo.CreateUser(ctx, victim); err != nil {
			t.Fatalf("CreateUser(victim) error = %v", err)
		}
		keep := []string{victim.ID}
		if err := repo.UpdateDept(ctx, a.ID, biz.DeptUpdate{LeaderIDs: &keep}); err != nil {
			t.Fatalf("UpdateDept(seed leader) error = %v", err)
		}
		var wg sync.WaitGroup
		var delErr, updErr error
		wg.Go(func() { delErr = userRepo.DeleteUser(ctx, victim.ID) })
		wg.Go(func() { updErr = repo.UpdateDept(ctx, a.ID, biz.DeptUpdate{LeaderIDs: &keep}) })
		wg.Wait()
		if delErr != nil {
			t.Fatalf("round %d: DeleteUser error = %v", i, delErr)
		}
		if updErr != nil && !errors.Is(updErr, biz.ErrUserNotFound) {
			t.Fatalf("round %d: UpdateDept error = %v, want nil or ErrUserNotFound", i, updErr)
		}
	}
	var dangling int
	if err := d.Pool.QueryRow(ctx,
		`SELECT count(*) FROM system_dept_leader l JOIN "system_user" u ON u.id = l.user_id WHERE u.delete_time IS NOT NULL`).Scan(&dangling); err != nil {
		t.Fatalf("count dangling leaders: %v", err)
	}
	if dangling != 0 {
		t.Fatalf("dangling leader rows for deleted users = %d, want 0", dangling)
	}

	// 路径始终是规范形式：大写 ID 移动不会把大写段写进路径。
	if err := repo.MoveDept(ctx, strings.ToUpper(a.ID), strings.ToUpper(other.ID)); err != nil {
		t.Fatalf("MoveDept(upper-case ids) error = %v", err)
	}
	got, err := repo.GetDept(ctx, a.ID)
	if err != nil {
		t.Fatalf("GetDept() error = %v", err)
	}
	if got.Path != strings.ToLower(got.Path) || !strings.HasSuffix(got.Path, "/"+a.ID+"/") {
		t.Fatalf("path after upper-case move = %q, want canonical lower-case", got.Path)
	}
}
