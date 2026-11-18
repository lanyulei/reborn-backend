package biz

import (
	"context"
	"slices"
	"strings"
)

// 通知对象候选项的种类。
const (
	TargetKindSchedule = "schedule"
	TargetKindDept     = "dept"
	TargetKindUser     = "user"
	// maxTargetUsers 一次返回的候选用户数上限。
	maxTargetUsers = 50
)

// TargetOptions 通知对象的候选项。未请求的种类为 nil。
type TargetOptions struct {
	Schedules []TargetSchedule
	Depts     []TargetDept
	Users     []TargetUser
}

// TargetUsecase 通知对象候选项（规则的通知对象、订阅的接收方选择器用）。值班表、部门、用户分别属于
// 值班与组织架构域，那边的列表接口挂在各自页面的权限下；监控域只需要「可选项」这一最小视图，所以
// 自带一个只读接口，随规则 / 订阅的新建、编辑权限开放。
type TargetUsecase struct {
	src TargetSource
}

// NewTargetUsecase new a TargetUsecase.
func NewTargetUsecase(src TargetSource) *TargetUsecase { return &TargetUsecase{src: src} }

// Options 取候选项：kinds 为空 = 三种都要；keyword 只影响用户（登录名 / 昵称模糊匹配，至多
// maxTargetUsers 个）。
func (uc *TargetUsecase) Options(ctx context.Context, keyword string, kinds []string) (*TargetOptions, error) {
	want := func(kind string) bool { return len(kinds) == 0 || slices.Contains(kinds, kind) }
	out := &TargetOptions{}
	var err error
	if want(TargetKindSchedule) {
		if out.Schedules, err = uc.src.Schedules(ctx); err != nil {
			return nil, err
		}
	}
	if want(TargetKindDept) {
		if out.Depts, err = uc.src.Depts(ctx); err != nil {
			return nil, err
		}
	}
	if want(TargetKindUser) {
		if out.Users, err = uc.src.Users(ctx, strings.TrimSpace(keyword), maxTargetUsers); err != nil {
			return nil, err
		}
	}
	return out, nil
}
