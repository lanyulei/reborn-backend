package testfake

import (
	"cmp"
	"context"
	"slices"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// userDeptEntry 是用户的一条部门归属（切片顺序即加入顺序）。
type userDeptEntry struct {
	deptID  string
	primary bool
}

// deptDepth 由物化路径得到层级（根部门为 1），与 data 层同口径。
func deptDepth(path string) int { return strings.Count(path, "/") - 1 }

// deptViewLocked 返回部门的读视图副本（回填负责人与直属成员数）；调用方须持锁。
func (s *Store) deptViewLocked(d *biz.Dept) *biz.Dept {
	cp := *d
	cp.Leaders = nil
	for _, userID := range s.deptLeaders[d.ID] {
		u, ok := s.users[userID]
		if !ok {
			continue
		}
		cp.Leaders = append(cp.Leaders, biz.DeptLeader{
			UserID: u.ID, Username: u.Username, Nickname: u.Nickname, Avatar: u.Avatar, Status: u.Status,
		})
	}
	cp.MemberCount = 0
	for _, entries := range s.userDepts {
		if slices.ContainsFunc(entries, func(e userDeptEntry) bool { return e.deptID == d.ID }) {
			cp.MemberCount++
		}
	}
	return &cp
}

// GetDept implements biz.DeptReader.
func (s *Store) GetDept(_ context.Context, id string) (*biz.Dept, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.depts[id]
	if !ok {
		return nil, biz.ErrDeptNotFound
	}
	return s.deptViewLocked(d), nil
}

// ListDepts implements biz.DeptReader：上级在前，同级按 sort、创建时间升序。
func (s *Store) ListDepts(_ context.Context) ([]*biz.Dept, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*biz.Dept, 0, len(s.depts))
	for _, d := range s.depts {
		out = append(out, s.deptViewLocked(d))
	}
	slices.SortFunc(out, func(a, b *biz.Dept) int {
		return cmp.Or(
			cmp.Compare(len(a.Path), len(b.Path)),
			cmp.Compare(a.Sort, b.Sort),
			a.CreateTime.Compare(b.CreateTime),
			strings.Compare(a.ID, b.ID),
		)
	})
	return out, nil
}

// deptConflictLocked 校验名称（同一上级下）与编码唯一性，skipID 为被更新的部门自身。
func (s *Store) deptConflictLocked(skipID, parentID, name, code string) error {
	for _, d := range s.depts {
		if d.ID == skipID {
			continue
		}
		if d.ParentID == parentID && d.Name == name {
			return biz.ErrDeptNameExists
		}
		if code != "" && d.Code == code {
			return biz.ErrDeptCodeExists
		}
	}
	return nil
}

// leadersExistLocked 校验负责人全部存在。
func (s *Store) leadersExistLocked(leaderIDs []string) error {
	for _, id := range leaderIDs {
		if _, ok := s.users[id]; !ok {
			return biz.ErrUserNotFound
		}
	}
	return nil
}

// CreateDept implements biz.DeptWriter.
func (s *Store) CreateDept(_ context.Context, dept *biz.Dept, leaderIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := "/" + dept.ID + "/"
	if dept.ParentID != "" {
		parent, ok := s.depts[dept.ParentID]
		if !ok {
			return biz.ErrDeptNotFound
		}
		if deptDepth(parent.Path)+1 > biz.MaxDeptDepth {
			return biz.ErrDeptDepthExceeded
		}
		path = parent.Path + dept.ID + "/"
	}
	if len(s.depts) >= biz.MaxDeptCount {
		return biz.ErrDeptLimitExceeded
	}
	if err := s.deptConflictLocked("", dept.ParentID, dept.Name, dept.Code); err != nil {
		return err
	}
	if err := s.leadersExistLocked(leaderIDs); err != nil {
		return err
	}
	dept.Path = path
	cp := *dept
	cp.Leaders = nil
	s.depts[cp.ID] = &cp
	s.deptLeaders[cp.ID] = slices.Clone(leaderIDs)
	return nil
}

// UpdateDept implements biz.DeptWriter.
func (s *Store) UpdateDept(_ context.Context, id string, upd biz.DeptUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.depts[id]
	if !ok {
		return biz.ErrDeptNotFound
	}
	name, code := d.Name, d.Code
	if upd.Name != nil {
		name = *upd.Name
	}
	if upd.Code != nil {
		code = *upd.Code
	}
	if err := s.deptConflictLocked(id, d.ParentID, name, code); err != nil {
		return err
	}
	if upd.LeaderIDs != nil {
		if err := s.leadersExistLocked(*upd.LeaderIDs); err != nil {
			return err
		}
		s.deptLeaders[id] = slices.Clone(*upd.LeaderIDs)
	}
	d.Name, d.Code = name, code
	if upd.Sort != nil {
		d.Sort = *upd.Sort
	}
	if upd.Status != nil {
		d.Status = *upd.Status
	}
	if upd.Description != nil {
		d.Description = *upd.Description
	}
	return nil
}

// MoveDept implements biz.DeptMover.
func (s *Store) MoveDept(_ context.Context, id, parentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.depts[id]
	if !ok {
		return biz.ErrDeptNotFound
	}
	oldPath := d.Path
	newPath := "/" + id + "/"
	if parentID != "" {
		parent, ok := s.depts[parentID]
		if !ok {
			return biz.ErrDeptNotFound
		}
		if strings.HasPrefix(parent.Path, oldPath) {
			return biz.ErrDeptMoveInvalid
		}
		newPath = parent.Path + id + "/"
	}
	if newPath == oldPath {
		return nil
	}
	deepest := 0
	for _, other := range s.depts {
		if strings.HasPrefix(other.Path, oldPath) {
			deepest = max(deepest, deptDepth(other.Path))
		}
	}
	if deepest-deptDepth(oldPath)+deptDepth(newPath) > biz.MaxDeptDepth {
		return biz.ErrDeptDepthExceeded
	}
	if err := s.deptConflictLocked(id, parentID, d.Name, ""); err != nil {
		return err
	}
	d.ParentID = parentID
	for _, other := range s.depts {
		if strings.HasPrefix(other.Path, oldPath) {
			other.Path = newPath + other.Path[len(oldPath):]
		}
	}
	return nil
}

// DeleteDept implements biz.DeptWriter.
func (s *Store) DeleteDept(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.depts[id]; !ok {
		return biz.ErrDeptNotFound
	}
	for _, d := range s.depts {
		if d.ParentID == id {
			return biz.ErrDeptHasChildren
		}
	}
	for _, entries := range s.userDepts {
		if slices.ContainsFunc(entries, func(e userDeptEntry) bool { return e.deptID == id }) {
			return biz.ErrDeptHasMembers
		}
	}
	delete(s.depts, id)
	delete(s.deptLeaders, id)
	return nil
}

// AddDeptMembers implements biz.DeptMemberRepo.
func (s *Store) AddDeptMembers(_ context.Context, deptID string, userIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.depts[deptID]; !ok {
		return biz.ErrDeptNotFound
	}
	inDept := func(userID string) bool {
		return slices.ContainsFunc(s.userDepts[userID], func(e userDeptEntry) bool { return e.deptID == deptID })
	}
	for _, userID := range userIDs {
		if _, ok := s.users[userID]; !ok {
			return biz.ErrUserNotFound
		}
		if len(s.userDepts[userID]) >= biz.MaxUserDepts && !inDept(userID) {
			return biz.ErrDeptLimitExceeded
		}
	}
	for _, userID := range userIDs {
		if inDept(userID) {
			continue
		}
		hasPrimary := slices.ContainsFunc(s.userDepts[userID], func(e userDeptEntry) bool { return e.primary })
		s.userDepts[userID] = append(s.userDepts[userID], userDeptEntry{deptID: deptID, primary: !hasPrimary})
	}
	return nil
}

// RemoveDeptMember implements biz.DeptMemberRepo.
func (s *Store) RemoveDeptMember(_ context.Context, deptID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.userDepts[userID]
	idx := slices.IndexFunc(entries, func(e userDeptEntry) bool { return e.deptID == deptID })
	if idx < 0 {
		return nil
	}
	wasPrimary := entries[idx].primary
	entries = slices.Delete(entries, idx, idx+1)
	if wasPrimary && len(entries) > 0 {
		entries[0].primary = true
	}
	s.userDepts[userID] = entries
	return nil
}

// ListUserDepts implements biz.DeptMemberRepo：主部门在前，其余按加入顺序。
func (s *Store) ListUserDepts(_ context.Context, userIDs []string) (map[string][]biz.UserDept, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]biz.UserDept, len(userIDs))
	for _, userID := range userIDs {
		var primary, rest []biz.UserDept
		for _, e := range s.userDepts[userID] {
			d, ok := s.depts[e.deptID]
			if !ok {
				continue
			}
			view := biz.UserDept{ID: d.ID, Name: d.Name, Primary: e.primary}
			if e.primary {
				primary = append(primary, view)
			} else {
				rest = append(rest, view)
			}
		}
		if depts := append(primary, rest...); len(depts) > 0 {
			out[userID] = depts
		}
	}
	return out, nil
}

// checkDeptsExist 校验部门全部存在（建号前置，对齐 data 层事务内判定）。
func (s *Store) checkDeptsExist(deptIDs []string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, id := range deptIDs {
		if _, ok := s.depts[id]; !ok {
			return biz.ErrDeptNotFound
		}
	}
	return nil
}

// replaceUserDeptsLocked 整组替换用户所属部门：保留未变化归属的加入顺序，新增的
// 追加在后；调用方须持写锁且已校验部门存在。
func (s *Store) replaceUserDeptsLocked(userID string, assign biz.UserDeptAssignment) {
	kept := make([]userDeptEntry, 0, len(assign.DeptIDs))
	for _, e := range s.userDepts[userID] {
		if slices.Contains(assign.DeptIDs, e.deptID) {
			kept = append(kept, e)
		}
	}
	for _, deptID := range assign.DeptIDs {
		if !slices.ContainsFunc(kept, func(e userDeptEntry) bool { return e.deptID == deptID }) {
			kept = append(kept, userDeptEntry{deptID: deptID})
		}
	}
	for i := range kept {
		kept[i].primary = kept[i].deptID == assign.PrimaryDeptID
	}
	if len(kept) == 0 {
		delete(s.userDepts, userID)
		return
	}
	s.userDepts[userID] = kept
}

// deptUserFilter 构造「属于某部门」的用户判定（未指定部门返回 nil）；includeSub
// 时按物化路径前缀匹配整棵子树。返回的闭包在调用时自行加读锁。
func (s *Store) deptUserFilter(filter biz.UserFilter) func(userID string) bool {
	if filter.DeptID == "" {
		return nil
	}
	return func(userID string) bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		target, ok := s.depts[filter.DeptID]
		for _, e := range s.userDepts[userID] {
			if e.deptID == filter.DeptID {
				return true
			}
			if d := s.depts[e.deptID]; filter.IncludeSubDepts && ok && d != nil && strings.HasPrefix(d.Path, target.Path) {
				return true
			}
		}
		return false
	}
}
