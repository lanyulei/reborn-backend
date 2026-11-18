package biz_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

func newViews(e *env) *biz.AlertViewUsecase {
	repo := testfake.NewMonitorViews()
	return biz.NewAlertViewUsecase(e.scopes, repo, repo, e.org)
}

func TestAlertView_OwnershipMatrix(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := newViews(e)
	ctx := context.Background()
	mine, err := uc.Create(ctx, uAlice, biz.SavedViewSpec{Name: "  我的视图 ", By: "label:instance", Filters: mc.AlertViewFilter{GroupID: gHost, Labels: []string{"env=prod"}}})
	if err != nil || mine.Spec.Name != "我的视图" || !mine.Mine || !mine.CanEdit || mine.OwnerName != "Alice" || mine.Spec.IsPublic {
		t.Fatalf("Create = %+v, %v", mine, err)
	}
	// 公共视图：只有超级管理员能建。
	_, err = uc.Create(ctx, uAlice, biz.SavedViewSpec{Name: "公共", By: "rule", IsPublic: true})
	wantErr(t, "non-admin public", err, biz.ErrForbidden)
	pub, err := uc.Create(ctx, uAdmin, biz.SavedViewSpec{Name: "公共", By: "rule", IsPublic: true})
	if err != nil {
		t.Fatal(err)
	}
	// 同一所有者下重名；不同所有者可以同名。
	_, err = uc.Create(ctx, uAlice, biz.SavedViewSpec{Name: "我的视图", By: "rule"})
	wantErr(t, "duplicate", err, biz.ErrAlertViewNameExists)
	bobs, err := uc.Create(ctx, uBob, biz.SavedViewSpec{Name: "我的视图", By: "severity"})
	if err != nil {
		t.Fatal(err)
	}

	// 列表：公共在前，然后是自己的；看不到别人的个人视图。
	list, err := uc.List(ctx, uAlice)
	if err != nil || len(list) != 2 || list[0].ID != pub.ID || list[0].CanEdit || list[0].Mine || list[1].ID != mine.ID {
		t.Fatalf("List(alice) = %+v, %v", list, err)
	}
	if list, _ = uc.List(ctx, uAdmin); len(list) != 1 || !list[0].CanEdit || !list[0].Mine {
		t.Fatalf("List(admin) = %+v", list)
	}

	edit := biz.SavedViewSpec{Name: "改名", By: "group"}
	// 别人的个人视图：对谁都是 404——包括超级管理员。
	for _, user := range []string{uBob, uAdmin} {
		_, err = uc.Update(ctx, user, mine.ID, edit)
		wantErr(t, "update other's view as "+user, err, biz.ErrAlertViewNotFound)
		wantErr(t, "delete other's view as "+user, uc.Delete(ctx, user, mine.ID), biz.ErrAlertViewNotFound)
	}
	// 公共视图：非管理员 404。
	_, err = uc.Update(ctx, uAlice, pub.ID, edit)
	wantErr(t, "update public as alice", err, biz.ErrAlertViewNotFound)
	wantErr(t, "delete public as alice", uc.Delete(ctx, uAlice, pub.ID), biz.ErrAlertViewNotFound)
	_, err = uc.Update(ctx, uAlice, "no-such-view", edit)
	wantErr(t, "update missing", err, biz.ErrAlertViewNotFound)

	// 本人可以改自己的，但不能把它设成公共。
	got, err := uc.Update(ctx, uAlice, mine.ID, edit)
	if err != nil || got.Spec.Name != "改名" || got.Spec.By != "group" || got.OwnerID != uAlice {
		t.Fatalf("Update = %+v, %v", got, err)
	}
	edit.IsPublic = true
	_, err = uc.Update(ctx, uAlice, mine.ID, edit)
	wantErr(t, "make public as alice", err, biz.ErrForbidden)
	// 改成别人已有的名字不冲突（唯一性按所有者），改成自己已有的名字才冲突。
	second, err := uc.Create(ctx, uAlice, biz.SavedViewSpec{Name: "第二个", By: "rule"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = uc.Update(ctx, uAlice, second.ID, biz.SavedViewSpec{Name: "改名", By: "rule"})
	wantErr(t, "rename to own existing", err, biz.ErrAlertViewNameExists)

	// 管理员可以改 / 删公共视图。
	if _, err = uc.Update(ctx, uAdmin, pub.ID, biz.SavedViewSpec{Name: "公共 2", By: "severity", IsPublic: true}); err != nil {
		t.Fatal(err)
	}
	if err := uc.Delete(ctx, uAdmin, pub.ID); err != nil {
		t.Fatal(err)
	}
	if err := uc.Delete(ctx, uBob, bobs.ID); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "delete twice", uc.Delete(ctx, uBob, bobs.ID), biz.ErrAlertViewNotFound)
}

func TestAlertView_Validation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	uc := newViews(e)
	ctx := context.Background()
	for name, spec := range map[string]biz.SavedViewSpec{
		"blank name":       {Name: "   ", By: "rule"},
		"bad by":           {Name: "a", By: "owner"},
		"bad label by":     {Name: "a", By: "label:9x"},
		"missing group":    {Name: "a", By: "rule", Filters: mc.AlertViewFilter{GroupID: "g-none"}},
		"unreadable group": {Name: "a", By: "rule", Filters: mc.AlertViewFilter{GroupID: gApp}},
		"bad severity":     {Name: "a", By: "rule", Filters: mc.AlertViewFilter{Severities: []int32{5}}},
		"dup severity":     {Name: "a", By: "rule", Filters: mc.AlertViewFilter{Severities: []int32{1, 1}}},
		"bad label":        {Name: "a", By: "rule", Filters: mc.AlertViewFilter{Labels: []string{"=x"}}},
		"too many labels":  {Name: "a", By: "rule", Filters: mc.AlertViewFilter{Labels: []string{"a=1", "b=1", "c=1", "d=1", "e=1", "f=1"}}},
		"bad datasource":   {Name: "a", By: "rule", Filters: mc.AlertViewFilter{DatasourceID: "ds"}},
	} {
		_, err := uc.Create(ctx, uAlice, spec)
		wantErr(t, name, err, biz.ErrAlertViewInvalid)
	}
	// 每人至多 50 个个人视图；公共视图不占名额。
	for i := range biz.MaxPersonalViews {
		if _, err := uc.Create(ctx, uAdmin, biz.SavedViewSpec{Name: fmt.Sprintf("v%d", i), By: "rule"}); err != nil {
			t.Fatalf("create #%d: %v", i, err)
		}
	}
	_, err := uc.Create(ctx, uAdmin, biz.SavedViewSpec{Name: "one too many", By: "rule"})
	wantErr(t, "51st personal view", err, biz.ErrAlertViewInvalid)
	if _, err := uc.Create(ctx, uAdmin, biz.SavedViewSpec{Name: "public", By: "rule", IsPublic: true}); err != nil {
		t.Fatalf("public view beyond the personal cap: %v", err)
	}
}

// fakeShifts 值班表班次与值班日历权限的桩（由组合根适配 oncall / system 域）。
type fakeShifts struct {
	slots    map[string][]biz.ShiftSlot
	calendar []string
	limit    int
}

func (f *fakeShifts) Shifts(_ context.Context, scheduleID string, _, _ time.Time, limit int) ([]biz.ShiftSlot, bool, error) {
	f.limit = limit
	slots, ok := f.slots[scheduleID]
	return slots, ok, nil
}

func (f *fakeShifts) CanViewOncallCalendar(_ context.Context, userID string) (bool, error) {
	for _, u := range f.calendar {
		if u == userID {
			return true, nil
		}
	}
	return false, nil
}

func TestShiftCount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	base := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	day := func(n int) time.Time { return base.AddDate(0, 0, n) }
	shifts := &fakeShifts{
		slots: map[string][]biz.ShiftSlot{schedOps: {
			{UserID: uBob, UserName: "Bob", Role: 1, Start: day(-1), End: day(1)},
			{UserID: uCarol, UserName: "Carol", Role: 2, Start: day(-1), End: day(1)},
			{UserID: uCarol, UserName: "Carol", Role: 1, Start: day(1), End: day(2)},
		}},
		calendar: []string{uDave},
	}
	// alice 没有任何一项权限；bob 有告警权限；dave 只有值班日历权限；uGone 已停用。
	e.org.AlertReaders = []string{uBob, uGone}
	uc := biz.NewShiftCountUsecase(e.org, shifts, e.org, shifts, e.store.ShiftCounter())
	log := func(id, alertID string, at time.Time, recipients ...string) {
		e.store.SeedNotifyLog(&biz.NotifyLog{ID: id, AlertID: alertID, Source: mc.NotifySourceRule, Recipients: recipients, CreateTime: at})
	}
	log("l1", "a1", day(0).Add(time.Hour), uBob)
	log("l2", "a1", day(0).Add(2*time.Hour), uBob, uCarol) // 同一事件再次通知：bob 仍只计一次
	log("l3", "a2", day(0).Add(3*time.Hour), uBob)
	log("l4", "a3", day(-1).Add(time.Hour), uBob)      // 在班次内，但在查询范围之前
	log("l5", "a4", day(1).Add(time.Hour), uCarol)     // 第三个班次
	log("l6", "a5", day(1).Add(2*time.Hour), uBob)     // bob 已不当班
	log("l7", "a6", day(0).Add(time.Hour), "$group:x") // 群通知不计到人

	for _, user := range []string{uAlice, uGone, "u-nobody"} {
		_, err := uc.List(ctx, user, schedOps, day(0), day(2))
		wantErr(t, "List as "+user, err, biz.ErrForbidden)
	}
	for _, user := range []string{uBob, uDave, uAdmin} {
		items, err := uc.List(ctx, user, schedOps, day(0), day(2))
		if err != nil || len(items) != 3 {
			t.Fatalf("List as %s = %+v, %v", user, items, err)
		}
		if items[0].AlertCount != 2 || items[0].UserName != "Bob" || !items[0].Start.Equal(day(-1)) ||
			items[1].AlertCount != 1 || items[1].Role != 2 || items[2].AlertCount != 1 {
			t.Fatalf("counts as %s = %d / %d / %d", user, items[0].AlertCount, items[1].AlertCount, items[2].AlertCount)
		}
	}
	if shifts.limit != 500 {
		t.Fatalf("shift limit = %d", shifts.limit)
	}
	// 不存在的值班表：空列表。
	if items, err := uc.List(ctx, uBob, "sched-none", day(0), day(2)); err != nil || items == nil || len(items) != 0 {
		t.Fatalf("unknown schedule = %v, %v", items, err)
	}
	_, err := uc.List(ctx, uBob, schedOps, day(0), day(63))
	wantErr(t, "range too long", err, biz.ErrTimeRangeInvalid)
	_, err = uc.List(ctx, uBob, schedOps, day(1), day(1))
	wantErr(t, "empty range", err, biz.ErrTimeRangeInvalid)
}
