package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 值班域仓储的内存桩：班次适配器只走 Get / List / ListRange 三条路径，其余方法不会被调用。
type shiftStubSchedules struct {
	bizoncall.ScheduleRepo
	byID map[string]*bizoncall.Schedule
}

func (s shiftStubSchedules) List(context.Context, string) ([]*bizoncall.Schedule, error) {
	out := make([]*bizoncall.Schedule, 0, len(s.byID))
	for _, sc := range s.byID {
		out = append(out, sc)
	}
	return out, nil
}

func (s shiftStubSchedules) Get(_ context.Context, id string) (*bizoncall.Schedule, error) {
	if sc := s.byID[id]; sc != nil {
		return sc, nil
	}
	return nil, bizoncall.ErrScheduleNotFound
}

type shiftStubRepo struct {
	bizoncall.ShiftRepo
	shifts []*bizoncall.Shift
}

func (r shiftStubRepo) ListRange(context.Context, time.Time, time.Time) ([]*bizoncall.Shift, error) {
	return r.shifts, nil
}

// 班次告警数：时间范围内的班次超过上限时明确报错，而不是静默只给前 limit 个（monitor 模块评审
// core-monitor-config P3）：窗口后段的班次此前看起来像「没有排班」。
func TestMonitorShifts_OverTheLimitIsAnExplicitError(t *testing.T) {
	const sched = "sched-1"
	start := time.Now().Add(24 * time.Hour)
	stub := &shiftStubRepo{}
	for i := range 3 {
		stub.shifts = append(stub.shifts, &bizoncall.Shift{
			ScheduleID: sched, Role: bizoncall.RolePrimary, UserID: "u-1", UserName: "张三",
			Start: start.Add(time.Duration(i) * time.Hour), End: start.Add(time.Duration(i+1) * time.Hour),
		})
	}
	schedules := shiftStubSchedules{byID: map[string]*bizoncall.Schedule{
		sched: {ID: sched, GroupID: "g-1", Name: "一线值班", Timezone: "UTC", RotationInterval: 1, Status: bizoncall.StatusEnabled},
	}}
	shifts := NewMonitorShifts(bizoncall.NewScheduleUsecase(nil, nil, schedules, stub, nil))
	ctx := context.Background()
	from, to := start.Add(-time.Hour), start.Add(4*time.Hour)

	// 上限之内：照常返回，按开始时间升序。
	slots, exists, err := shifts.Shifts(ctx, sched, from, to, 3)
	if err != nil || !exists || len(slots) != 3 || slots[0].Start.After(slots[2].Start) || slots[0].UserName != "张三" {
		t.Fatalf("Shifts(limit 3) = %+v, %v, %v", slots, exists, err)
	}
	// 超过上限：400（提示缩小时间范围），一条都不给。
	slots, exists, err = shifts.Shifts(ctx, sched, from, to, 2)
	if len(slots) != 0 || !exists || !kerrors.Is(err, bizmonitor.ErrTimeRangeInvalid) {
		t.Fatalf("Shifts(limit 2) = %+v, %v, %v; want an explicit too-many-shifts error", slots, exists, err)
	}
	if msg := kerrors.FromError(err).Message; !strings.Contains(msg, "2") || !strings.Contains(msg, "narrow") {
		t.Fatalf("error message = %q, want it to name the limit and ask for a narrower range", msg)
	}
	// 值班表不存在：与出错区分开（exists = false，无错误）。
	if _, exists, err := shifts.Shifts(ctx, "no-such-schedule", from, to, 10); err != nil || exists {
		t.Fatalf("Shifts(missing schedule) exists = %v, err = %v", exists, err)
	}
}
