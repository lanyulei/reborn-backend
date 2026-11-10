package service

import (
	"testing"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newSilenceService(t *testing.T) (*NotifySilenceService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	return NewNotifySilenceService(biz.NewSilenceUsecase(store), testfake.Unrestricted()), store
}

// newSilenceSpec 返回合法静默规则载荷（无窗口=全程静默）。
func newSilenceSpec() *notifyv1.SilenceSpec {
	return &notifyv1.SilenceSpec{
		EventPattern: "monitor.alert.*",
		SeverityMax:  i32ptr(3),
	}
}

func TestNotifySilenceService_CreateRoundtrip(t *testing.T) {
	t.Parallel()
	svc, _ := newSilenceService(t)
	ctx := testCtx()

	// 带时间窗 + 时区：字段透传。
	resp, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "monitor.alert.*",
		SeverityMax:  i32ptr(3),
		TimeWindow: &notifyv1.SilenceWindow{
			Start: "22:00", End: "08:00", Tz: sptr("Asia/Shanghai"),
		},
	}})
	if err != nil {
		t.Fatalf("CreateSilence() error = %v", err)
	}
	silence := resp.GetSilence()
	if silence.GetId() == "" || silence.GetBizId() != biz.DefaultBizID {
		t.Fatalf("CreateSilence() = %+v, want id and default biz", silence)
	}
	if !silence.GetEnabled() {
		t.Fatal("CreateSilence() enabled = false, want default true")
	}
	window := silence.GetTimeWindow()
	if window.GetStart() != "22:00" || window.GetEnd() != "08:00" || window.GetTz() != "Asia/Shanghai" {
		t.Fatalf("CreateSilence() window = %+v", window)
	}

	// 缺省调优项走默认：severity_max 2、无窗口（protojson 省略 time_window）。
	bare, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "permission.change",
	}})
	if err != nil {
		t.Fatalf("CreateSilence(defaults) error = %v", err)
	}
	if bare.GetSilence().GetSeverityMax() != 2 {
		t.Fatalf("CreateSilence(defaults) severityMax = %d, want 2", bare.GetSilence().GetSeverityMax())
	}
	if bare.GetSilence().GetTimeWindow() != nil {
		t.Fatalf("CreateSilence(defaults) window = %+v, want nil", bare.GetSilence().GetTimeWindow())
	}

	// 显式停用入参。
	off := false
	created, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: newSilenceSpec(), Enabled: &off})
	if err != nil {
		t.Fatalf("CreateSilence(disabled) error = %v", err)
	}
	if created.GetSilence().GetEnabled() {
		t.Fatal("CreateSilence(disabled) enabled = true")
	}
}

func TestNotifySilenceService_CreateErrors(t *testing.T) {
	t.Parallel()
	svc, _ := newSilenceService(t)
	ctx := testCtx()

	// 非法 event_pattern（protovalidate 在传输层拦截，service 层直断 biz 哨兵）。
	if _, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "Monitor.*.Bad",
	}}); !errors.Is(err, biz.ErrSilenceInvalid) {
		t.Fatalf("CreateSilence(bad pattern) error = %v, want ErrSilenceInvalid", err)
	}

	// 非法时间窗：零长（起点等于终点）。
	if _, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "monitor.alert.*",
		TimeWindow:   &notifyv1.SilenceWindow{Start: "09:00", End: "09:00"},
	}}); !errors.Is(err, biz.ErrSilenceInvalid) {
		t.Fatalf("CreateSilence(zero-length window) error = %v, want ErrSilenceInvalid", err)
	}

	// 未知时区。
	if _, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "monitor.alert.*",
		TimeWindow:   &notifyv1.SilenceWindow{Start: "22:00", End: "08:00", Tz: sptr("Mars/Olympus")},
	}}); !errors.Is(err, biz.ErrSilenceInvalid) {
		t.Fatalf("CreateSilence(bad tz) error = %v, want ErrSilenceInvalid", err)
	}
}

func TestNotifySilenceService_UpdateSilence(t *testing.T) {
	t.Parallel()
	svc, _ := newSilenceService(t)
	ctx := testCtx()

	created, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
		EventPattern: "monitor.alert.*",
		TimeWindow:   &notifyv1.SilenceWindow{Start: "22:00", End: "08:00"},
	}})
	if err != nil {
		t.Fatalf("CreateSilence() error = %v", err)
	}
	id := created.GetSilence().GetId()

	// spec 整组替换：换模式 + 清除时间窗（不传 time_window 即全程）。
	if _, err := svc.UpdateSilence(ctx, &notifyv1.UpdateSilenceRequest{
		Id:   id,
		Spec: &notifyv1.SilenceSpec{EventPattern: "permission.change", SeverityMax: i32ptr(4)},
	}); err != nil {
		t.Fatalf("UpdateSilence(spec) error = %v", err)
	}
	got, err := svc.GetSilence(ctx, &notifyv1.GetSilenceRequest{Id: id})
	if err != nil {
		t.Fatalf("GetSilence() error = %v", err)
	}
	s := got.GetSilence()
	if s.GetEventPattern() != "permission.change" || s.GetSeverityMax() != 4 {
		t.Fatalf("UpdateSilence(spec) result = %+v", s)
	}
	if s.GetTimeWindow() != nil {
		t.Fatalf("UpdateSilence(spec) window = %+v, want nil（整组替换清除）", s.GetTimeWindow())
	}

	// 独立启停不动规则。
	disabled := false
	if _, err := svc.UpdateSilence(ctx, &notifyv1.UpdateSilenceRequest{Id: id, Enabled: &disabled}); err != nil {
		t.Fatalf("UpdateSilence(enabled) error = %v", err)
	}
	got, _ = svc.GetSilence(ctx, &notifyv1.GetSilenceRequest{Id: id})
	if got.GetSilence().GetEnabled() || got.GetSilence().GetEventPattern() != "permission.change" {
		t.Fatalf("UpdateSilence(enabled) result = %+v", got.GetSilence())
	}

	// 不存在静默。
	if _, err := svc.UpdateSilence(ctx, &notifyv1.UpdateSilenceRequest{
		Id: missingChannelID, Enabled: &disabled,
	}); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("UpdateSilence(missing) error = %v, want ErrSilenceNotFound", err)
	}
}

func TestNotifySilenceService_DeleteAndList(t *testing.T) {
	t.Parallel()
	svc, _ := newSilenceService(t)
	ctx := testCtx()

	for _, pattern := range []string{"monitor.alert.*", "permission.change"} {
		if _, err := svc.CreateSilence(ctx, &notifyv1.CreateSilenceRequest{Spec: &notifyv1.SilenceSpec{
			EventPattern: pattern,
		}}); err != nil {
			t.Fatalf("CreateSilence(%q) error = %v", pattern, err)
		}
	}

	list, err := svc.ListSilences(ctx, &notifyv1.ListSilencesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListSilences() error = %v", err)
	}
	if list.GetTotal() != 2 || len(list.GetItems()) != 2 {
		t.Fatalf("ListSilences() = %d items, total %d, want 2/2", len(list.GetItems()), list.GetTotal())
	}

	// keyword 模糊匹配 event_pattern。
	kw := "permission"
	list, err = svc.ListSilences(ctx, &notifyv1.ListSilencesRequest{Page: 1, PageSize: 10, Keyword: &kw})
	if err != nil {
		t.Fatalf("ListSilences(keyword) error = %v", err)
	}
	if list.GetTotal() != 1 || list.GetItems()[0].GetEventPattern() != "permission.change" {
		t.Fatalf("ListSilences(keyword) = %+v", list.GetItems())
	}

	// enabled 过滤。
	off := false
	list, err = svc.ListSilences(ctx, &notifyv1.ListSilencesRequest{Page: 1, PageSize: 10, Enabled: &off})
	if err != nil {
		t.Fatalf("ListSilences(enabled) error = %v", err)
	}
	if list.GetTotal() != 0 {
		t.Fatalf("ListSilences(enabled) total = %d, want 0", list.GetTotal())
	}

	// 删除后不可见。
	all, err := svc.ListSilences(ctx, &notifyv1.ListSilencesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListSilences(all) error = %v", err)
	}
	victim := all.GetItems()[0].GetId()
	if _, err := svc.DeleteSilence(ctx, &notifyv1.DeleteSilenceRequest{Id: victim}); err != nil {
		t.Fatalf("DeleteSilence() error = %v", err)
	}
	if _, err := svc.GetSilence(ctx, &notifyv1.GetSilenceRequest{Id: victim}); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("GetSilence(deleted) error = %v, want ErrSilenceNotFound", err)
	}
	list, err = svc.ListSilences(ctx, &notifyv1.ListSilencesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListSilences(after delete) error = %v", err)
	}
	if list.GetTotal() != 1 {
		t.Fatalf("ListSilences(after delete) total = %d, want 1", list.GetTotal())
	}
}
