package service

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 值班域入参里的用户 ID 一律转小写：数据库返回的 ID 恒为小写，biz 按请求原值做 map 查找 /
// 相等比较。protovalidate 的 string.uuid 放行大写，不转的话成员、替班人、过滤条件都会被判成
// 「不存在」。

const (
	lowerAlice = "018f3e1a-0c00-7000-8000-00000000000a"
	lowerBob   = "018f3e1a-0c00-7000-8000-00000000000b"
	lowerAdmin = "018f3e1a-0c00-7000-8000-0000000000ff"
	lowerGroup = "018f3e1a-0c00-7000-8000-00000000aa01"
	lowerSched = "018f3e1a-0c00-7000-8000-00000000bb01"
)

func upper(s string) string { return strings.ToUpper(s) }

// upperp 大写后取指针（proto 的 optional 字段）。
func upperp(s string) *string { return proto.String(upper(s)) }

type adminAll struct{}

func (adminAll) IsAdminUser(context.Context, string) (bool, error) { return true, nil }

// userCtx 模拟鉴权中间件注入 claims 之后的请求 ctx。
func userCtx(userID string) context.Context {
	return jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: userID},
	})
}

// svcSchedules 只实现 List / Get。
type svcSchedules struct {
	biz.ScheduleRepo
	sched *biz.Schedule
}

func (r *svcSchedules) List(context.Context, string) ([]*biz.Schedule, error) {
	s := *r.sched
	return []*biz.Schedule{&s}, nil
}

func (r *svcSchedules) Get(context.Context, string) (*biz.Schedule, error) {
	s := *r.sched
	return &s, nil
}

// svcShifts 只实现 ListRange（原样返回，不按范围过滤）。
type svcShifts struct {
	biz.ShiftRepo
	shifts []*biz.Shift
}

func (r *svcShifts) ListRange(context.Context, time.Time, time.Time) ([]*biz.Shift, error) {
	return r.shifts, nil
}

// svcTx 值班域事务桩：只实现替班 / 加成员用到的方法，其余走嵌入的 nil 接口（调用即 panic）。
type svcTx struct {
	biz.Tx
	sched    *biz.Schedule
	members  []*biz.Member
	inserted []*biz.Shift
	added    []string
}

func (t *svcTx) LockGroup(_ context.Context, id string) (*biz.Group, error) {
	return &biz.Group{ID: id}, nil
}

func (t *svcTx) LockSchedule(context.Context, string) (*biz.Schedule, error) {
	s := *t.sched
	return &s, nil
}

func (t *svcTx) ListMembers(context.Context, string) ([]*biz.Member, error) { return t.members, nil }

func (t *svcTx) ListShiftsFrom(context.Context, string, time.Time) ([]*biz.Shift, error) {
	return nil, nil
}

func (t *svcTx) InsertShifts(_ context.Context, shifts []*biz.Shift, _ string) error {
	t.inserted = append(t.inserted, shifts...)
	return nil
}

func (t *svcTx) InsertMembers(_ context.Context, _ string, userIDs []string, _ int32, _ string) (int, error) {
	t.added = append(t.added, userIDs...)
	return len(userIDs), nil
}

func (t *svcTx) InsertChangeLog(context.Context, *biz.ChangeLog) error { return nil }

type svcStore struct{ tx *svcTx }

func (s *svcStore) WithinTx(_ context.Context, fn func(tx biz.Tx) error) error { return fn(s.tx) }

// svcUsers 用户目录桩：ID 恒为小写（与数据库一致）。
type svcUsers struct{ biz.UserDirectory }

func (svcUsers) Lookup(_ context.Context, ids []string) (map[string]*biz.User, error) {
	out := make(map[string]*biz.User, len(ids))
	for _, id := range ids {
		out[id] = &biz.User{ID: id, Available: true}
	}
	return out, nil
}

type noopWriter struct{}

func (noopWriter) NotifyEvent(_ context.Context, _ txrun.Tx, _ biznotify.Event) (*biznotify.Event, error) {
	return &biznotify.Event{ID: "ev"}, nil
}

func TestFromProtoConfigLowersUserIDs(t *testing.T) {
	t.Parallel()
	cfg := fromProtoConfig(&v1.ScheduleConfig{MemberIds: []string{upper(lowerAlice), upper(lowerBob)}, StartUserId: upperp(lowerBob)})
	if !slices.Equal(cfg.MemberIDs, []string{lowerAlice, lowerBob}) || cfg.StartUserID != lowerBob {
		t.Errorf("fromProtoConfig() members = %v, start user = %q", cfg.MemberIDs, cfg.StartUserID)
	}
	if got := lowerAll([]string{upper(lowerAlice), "", lowerBob}); !slices.Equal(got, []string{lowerAlice, "", lowerBob}) {
		t.Errorf("lowerAll() = %v", got)
	}
}

// 大写的 user_id / group_id / schedule_id 过滤条件同样能查到班次（biz 在内存里按 ID 相等比较）。
func TestListShiftsLowersFilterIDs(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tx := &svcTx{sched: &biz.Schedule{ID: lowerSched, GroupID: lowerGroup, Status: biz.StatusEnabled}}
	uc := biz.NewScheduleUsecase(&svcStore{tx}, nil, &svcSchedules{sched: tx.sched},
		&svcShifts{shifts: []*biz.Shift{{ScheduleID: lowerSched, GroupID: lowerGroup, UserID: lowerAlice, Start: now.Add(-time.Hour), End: now.Add(time.Hour)}}},
		biz.NewNotifier(noopWriter{}))
	svc := &OncallScheduleService{uc: uc, admin: adminAll{}}

	out, err := svc.ListShifts(userCtx(lowerAdmin), &v1.ListShiftsRequest{
		StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute)),
		GroupId: upperp(lowerGroup), ScheduleId: upperp(lowerSched), UserId: upperp(lowerAlice),
	})
	if err != nil || len(out.GetItems()) != 1 || out.GetItems()[0].GetUserId() != lowerAlice {
		t.Fatalf("ListShifts(upper-case filters) = %+v, err %v; want the one shift of %s", out.GetItems(), err, lowerAlice)
	}
}

// 大写的替班人 ID 能通过「必须是本组可用成员」的校验，落库的班次记的是小写 ID。
func TestCreateOverrideLowersUserIDs(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	anchor := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	until := now.Add(72 * time.Hour)
	tx := &svcTx{
		sched:   &biz.Schedule{ID: lowerSched, GroupID: lowerGroup, Timezone: "UTC", AnchorTime: anchor, Status: biz.StatusEnabled, GeneratedUntil: &until},
		members: []*biz.Member{{UserID: lowerAlice, Available: true}, {UserID: lowerBob, Available: true}},
	}
	uc := biz.NewScheduleUsecase(&svcStore{tx}, nil, &svcSchedules{sched: tx.sched}, &svcShifts{}, biz.NewNotifier(noopWriter{}))
	svc := &OncallScheduleService{uc: uc, admin: adminAll{}}

	// 从明天 0 点起替一整天（起止都落在交接时刻上，且在已排班范围内）。
	start := anchor.Add(24 * time.Hour)
	out, err := svc.CreateOverride(userCtx(lowerAdmin), &v1.CreateOverrideRequest{
		ScheduleId: upper(lowerSched), Role: biz.RolePrimary, UserId: upper(lowerAlice),
		StartTime: timestamppb.New(start), EndTime: timestamppb.New(start.Add(24 * time.Hour)),
	})
	if err != nil {
		t.Fatalf("CreateOverride(upper-case user id) error = %v", err)
	}
	if len(out.GetItems()) != 1 || out.GetItems()[0].GetUserId() != lowerAlice || len(tx.inserted) != 1 || tx.inserted[0].UserID != lowerAlice {
		t.Errorf("created = %+v, inserted = %+v; want the substitute recorded as %s", out.GetItems(), tx.inserted, lowerAlice)
	}
}

// 大写的 user_ids 加成员：用户目录里的 ID 是小写，不转会被判成「用户不存在或不可用」。
func TestAddGroupMembersLowersUserIDs(t *testing.T) {
	t.Parallel()
	tx := &svcTx{members: []*biz.Member{{UserID: lowerAdmin, Role: biz.MemberRoleAdmin}}}
	svc := &OncallGroupService{uc: biz.NewGroupUsecase(&svcStore{tx}, nil, svcUsers{}), admin: adminAll{}}

	out, err := svc.AddGroupMembers(userCtx(lowerAdmin), &v1.AddGroupMembersRequest{
		GroupId: upper(lowerGroup), UserIds: []string{upper(lowerAlice), upper(lowerBob)}, Role: biz.MemberRoleMember,
	})
	if err != nil || out.GetAdded() != 2 || !slices.Equal(tx.added, []string{lowerAlice, lowerBob}) {
		t.Fatalf("AddGroupMembers(upper-case ids) = %+v, err %v, added = %v", out, err, tx.added)
	}
}
