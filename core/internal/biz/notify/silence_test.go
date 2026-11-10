package biz

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// silenceRepoStub 静默仓储内存桩：承载用例断言所需的最小语义（软删不可见、
// Rule 整组替换、NotFound）。
type silenceRepoStub struct {
	silences map[string]*Silence
}

func newSilenceRepoStub() *silenceRepoStub {
	return &silenceRepoStub{silences: make(map[string]*Silence)}
}

func (s *silenceRepoStub) CreateSilence(_ context.Context, silence *Silence) error {
	cp := *silence
	if silence.Window != nil {
		window := *silence.Window
		cp.Window = &window
	}
	s.silences[cp.ID] = &cp
	return nil
}

func (s *silenceRepoStub) UpdateSilence(_ context.Context, id string, upd SilenceUpdate) error {
	silence, ok := s.silences[id]
	if !ok {
		return ErrSilenceNotFound
	}
	if upd.Rule != nil {
		silence.EventPattern = upd.Rule.EventPattern
		silence.SeverityMax = upd.Rule.SeverityMax
		silence.Window = upd.Rule.Window
	}
	if upd.Enabled != nil {
		silence.Enabled = *upd.Enabled
	}
	silence.UpdateTime = time.Now()
	return nil
}

func (s *silenceRepoStub) DeleteSilence(_ context.Context, id string) error {
	if _, ok := s.silences[id]; !ok {
		return ErrSilenceNotFound
	}
	delete(s.silences, id)
	return nil
}

func (s *silenceRepoStub) GetSilence(_ context.Context, id string) (*Silence, error) {
	silence, ok := s.silences[id]
	if !ok {
		return nil, ErrSilenceNotFound
	}
	cp := *silence
	return &cp, nil
}

func (s *silenceRepoStub) ListSilences(_ context.Context, filter SilenceFilter, page, pageSize int64) ([]*Silence, int64, error) {
	var matched []*Silence
	for _, silence := range s.silences {
		if filter.Enabled != nil && silence.Enabled != *filter.Enabled {
			continue
		}
		cp := *silence
		matched = append(matched, &cp)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// newSilenceRule 返回合法静默规则入参（无窗口=全程静默）。
func newSilenceRule() *SilenceRule {
	return &SilenceRule{EventPattern: "monitor.alert.*", SeverityMax: 2}
}

// newSilenceUC 组装静默用例。
func newSilenceUC() (*SilenceUsecase, *silenceRepoStub) {
	repo := newSilenceRepoStub()
	return NewSilenceUsecase(repo), repo
}

func TestSilenceUsecase_Create(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()

	silence := &Silence{Enabled: true}
	silence.SilenceRule = *newSilenceRule()
	out, err := uc.Create(context.Background(), silence)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(out.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", out.ID)
	}
	if out.BizID != DefaultBizID {
		t.Fatalf("Create() bizID = %q, want default", out.BizID)
	}
	if out.CreateTime.IsZero() || out.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}

	// 带跨午夜窗口 + 时区：合法。
	nightly := &Silence{Enabled: true}
	nightly.SilenceRule = *newSilenceRule()
	nightly.Window = &SilenceWindow{Start: "22:00", End: "08:00", Tz: "Asia/Shanghai"}
	if _, err := uc.Create(context.Background(), nightly); err != nil {
		t.Fatalf("Create(nightly) error = %v", err)
	}
	// 设计即无唯一约束：同 pattern 重复建两条均成功。
	if _, err := uc.Create(context.Background(), silence); err != nil {
		t.Fatalf("Create(duplicate pattern) error = %v", err)
	}
}

func TestSilenceUsecase_CreatePatternInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()

	for _, pattern := range []string{
		"",                   // 空
		"Monitor.Alert",      // 大写
		"monitor.*.firing",   // 中间通配（仅允许尾部）
		"monitor.alert.**",   // 双星
		"monitor.alert.*sub", // 星后缀非独立
		".alert",             // 前导点
		"monitor alert",      // 空格
	} {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.EventPattern = pattern
		if _, err := uc.Create(context.Background(), silence); !errors.Is(err, ErrSilenceInvalid) {
			t.Errorf("Create(pattern %q) error = %v, want ErrSilenceInvalid", pattern, err)
		}
	}
	for _, pattern := range []string{"*", "monitor.alert.firing", "monitor.alert.*"} {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.EventPattern = pattern
		if _, err := uc.Create(context.Background(), silence); err != nil {
			t.Errorf("Create(pattern %q) error = %v, want nil", pattern, err)
		}
	}
}

func TestSilenceUsecase_CreateWindowInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()

	cases := []struct {
		name   string
		window SilenceWindow
	}{
		{"bad start", SilenceWindow{Start: "9:00", End: "18:00"}},     // 非 HH:MM
		{"bad end", SilenceWindow{Start: "09:00", End: "24:00"}},      // 越界小时
		{"start minutes", SilenceWindow{Start: "09:0", End: "18:00"}}, // 补零缺失
		{"zero-length", SilenceWindow{Start: "09:00", End: "09:00"}},  // 起点等于终点
		{"bad tz", SilenceWindow{Start: "22:00", End: "08:00", Tz: "Not/AZone"}},
	}
	for _, tc := range cases {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.Window = &tc.window
		if _, err := uc.Create(context.Background(), silence); !errors.Is(err, ErrSilenceInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrSilenceInvalid", tc.name, err)
		}
	}
	// 合法边界：跨午夜（start > end）、UTC 显式、tz 缺省。
	for _, window := range []SilenceWindow{
		{Start: "22:00", End: "08:00"},
		{Start: "00:00", End: "23:59", Tz: "UTC"},
		{Start: "09:30", End: "18:00", Tz: "America/New_York"},
	} {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.Window = &window
		if _, err := uc.Create(context.Background(), silence); err != nil {
			t.Errorf("Create(window %v) error = %v, want nil", window, err)
		}
	}
}

func TestSilenceUsecase_CreateSeverityInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()

	for _, severity := range []int32{0, 5, -1} {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.SeverityMax = severity
		if _, err := uc.Create(context.Background(), silence); !errors.Is(err, ErrSilenceInvalid) {
			t.Errorf("Create(severity %d) error = %v, want ErrSilenceInvalid", severity, err)
		}
	}
	for _, severity := range []int32{1, 4} {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.SeverityMax = severity
		if _, err := uc.Create(context.Background(), silence); err != nil {
			t.Errorf("Create(severity %d) error = %v, want nil", severity, err)
		}
	}
}

func TestSilenceUsecase_Update(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()
	ctx := context.Background()

	silence := &Silence{Enabled: true}
	silence.SilenceRule = *newSilenceRule()
	created, err := uc.Create(ctx, silence)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Rule 整组替换：新窗口生效。
	replaced := &SilenceRule{
		EventPattern: "permission.change",
		SeverityMax:  3,
		Window:       &SilenceWindow{Start: "12:00", End: "13:00"},
	}
	if err := uc.Update(ctx, created.ID, SilenceUpdate{Rule: replaced}); err != nil {
		t.Fatalf("Update(rule) error = %v", err)
	}
	got, err := uc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.EventPattern != "permission.change" || got.SeverityMax != 3 ||
		got.Window == nil || got.Window.Start != "12:00" {
		t.Fatalf("Update(rule) result = %+v", got)
	}

	// Rule 替换为无窗口：清除时间窗（全程静默）。
	nowindow := &SilenceRule{EventPattern: "permission.change", SeverityMax: 3}
	if err := uc.Update(ctx, created.ID, SilenceUpdate{Rule: nowindow}); err != nil {
		t.Fatalf("Update(rule nowindow) error = %v", err)
	}
	if got, _ = uc.Get(ctx, created.ID); got.Window != nil {
		t.Fatalf("Update(rule nowindow) window = %+v, want nil", got.Window)
	}

	// 独立启停。
	disabled := false
	if err := uc.Update(ctx, created.ID, SilenceUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("Update(enabled) error = %v", err)
	}
	if got, _ = uc.Get(ctx, created.ID); got.Enabled {
		t.Fatal("Update(enabled) not applied")
	}

	// 非法新规则拒绝。
	bad := newSilenceRule()
	bad.SeverityMax = 9
	if err := uc.Update(ctx, created.ID, SilenceUpdate{Rule: bad}); !errors.Is(err, ErrSilenceInvalid) {
		t.Fatalf("Update(bad rule) error = %v, want ErrSilenceInvalid", err)
	}
	// 不存在静默。
	if err := uc.Update(ctx, "missing-id", SilenceUpdate{Rule: replaced}); !errors.Is(err, ErrSilenceNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrSilenceNotFound", err)
	}
	// 删除后不可见。
	if err := uc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := uc.Get(ctx, created.ID); !errors.Is(err, ErrSilenceNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrSilenceNotFound", err)
	}
	if err := uc.Delete(ctx, created.ID); !errors.Is(err, ErrSilenceNotFound) {
		t.Fatalf("Delete(again) error = %v, want ErrSilenceNotFound", err)
	}
}

func TestSilenceUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	uc, _ := newSilenceUC()
	ctx := context.Background()

	patterns := []string{"monitor.alert.*", "permission.change", "*"}
	for _, pattern := range patterns {
		silence := &Silence{Enabled: true}
		silence.SilenceRule = *newSilenceRule()
		silence.EventPattern = pattern
		if _, err := uc.Create(ctx, silence); err != nil {
			t.Fatalf("Create(%q) error = %v", pattern, err)
		}
	}

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, SilenceFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 3/3", len(items), total)
	}

	// enabled 过滤透传仓储：停用一条。
	disabled := false
	for _, item := range items {
		if item.EventPattern == "*" {
			if err := uc.Update(ctx, item.ID, SilenceUpdate{Enabled: &disabled}); err != nil {
				t.Fatalf("Update() error = %v", err)
			}
		}
	}
	items, total, err = uc.List(ctx, SilenceFilter{Enabled: &disabled}, 1, 20)
	if err != nil {
		t.Fatalf("List(enabled filter) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "*" {
		t.Fatalf("List(enabled filter) = %+v", items)
	}
}
