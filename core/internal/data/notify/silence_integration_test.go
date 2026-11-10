//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/go-kratos/kratos/v3/errors"
)

// newSilenceDO 返回带固定 ID 与审计时间的静默 DO（默认无窗口=全程静默）。
func newSilenceDO(id, eventPattern string) *biz.Silence {
	return &biz.Silence{
		ID:    id,
		BizID: biz.DefaultBizID,
		SilenceRule: biz.SilenceRule{
			EventPattern: eventPattern,
			SeverityMax:  2,
		},
		Enabled:    true,
		CreateTime: time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime: time.Now().UTC().Truncate(time.Microsecond),
	}
}

// silenceCfgVersion 读回 notify_silence 的配置版本号。
func silenceCfgVersion(t *testing.T, d *basedata.Data) int64 {
	t.Helper()
	var version int64
	if err := d.Pool.QueryRow(context.Background(),
		"SELECT version FROM notify_cfg_version WHERE table_name = 'notify_silence'").Scan(&version); err != nil {
		t.Fatalf("read silence cfg version: %v", err)
	}
	return version
}

func TestIntegration_SilenceCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewSilenceRepo(d)
	ctx := context.Background()

	// 创建：带跨午夜时间窗（含时区）落 jsonb。
	alert := newSilenceDO("018f3e1a-0303-7000-8000-000000000001", "monitor.alert.*")
	alert.Window = &biz.SilenceWindow{Start: "22:00", End: "08:00", Tz: "Asia/Shanghai"}
	if err := repo.CreateSilence(ctx, alert); err != nil {
		t.Fatalf("CreateSilence() error = %v", err)
	}
	if v := silenceCfgVersion(t, d); v < 1 {
		t.Fatalf("cfg version after create = %d, want >= 1", v)
	}

	// 无窗口静默：time_window 落 NULL 并往返为 nil。
	bare := newSilenceDO("018f3e1a-0303-7000-8000-000000000002", "*")
	if err := repo.CreateSilence(ctx, bare); err != nil {
		t.Fatalf("CreateSilence(bare) error = %v", err)
	}

	// 读路径全字段回填：jsonb 窗口结构与 NULL 语义。
	got, err := repo.GetSilence(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetSilence() error = %v", err)
	}
	if got.BizID != biz.DefaultBizID || got.EventPattern != "monitor.alert.*" ||
		got.SeverityMax != 2 || !got.Enabled {
		t.Fatalf("GetSilence() = %+v", got)
	}
	if got.Window == nil || got.Window.Start != "22:00" || got.Window.End != "08:00" ||
		got.Window.Tz != "Asia/Shanghai" {
		t.Fatalf("GetSilence() window = %+v", got.Window)
	}
	bareGot, err := repo.GetSilence(ctx, bare.ID)
	if err != nil {
		t.Fatalf("GetSilence(bare) error = %v", err)
	}
	if bareGot.Window != nil {
		t.Fatalf("GetSilence(bare) window = %+v, want nil (NULL roundtrip)", bareGot.Window)
	}
	if _, err := repo.GetSilence(ctx, missingChannelID); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("GetSilence(missing) error = %v, want ErrSilenceNotFound", err)
	}

	// Rule 整组替换：窗口清除 → 落 NULL；同事务 bump。
	vBefore := silenceCfgVersion(t, d)
	if err := repo.UpdateSilence(ctx, alert.ID, biz.SilenceUpdate{Rule: &biz.SilenceRule{
		EventPattern: "permission.change",
		SeverityMax:  3,
	}}); err != nil {
		t.Fatalf("UpdateSilence(rule) error = %v", err)
	}
	if vAfter := silenceCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after update = %d, want %d (same-tx bump)", vAfter, vBefore+1)
	}
	got, err = repo.GetSilence(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetSilence() error = %v", err)
	}
	if got.EventPattern != "permission.change" || got.SeverityMax != 3 {
		t.Fatalf("UpdateSilence(rule) result = %+v", got)
	}
	if got.Window != nil {
		t.Fatalf("UpdateSilence(rule) window = %+v, want nil (cleared to NULL)", got.Window)
	}

	// 独立启停 + 不存在静默。
	disabled := false
	if err := repo.UpdateSilence(ctx, alert.ID, biz.SilenceUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("UpdateSilence(enabled) error = %v", err)
	}
	if got, _ := repo.GetSilence(ctx, alert.ID); got.Enabled {
		t.Fatal("UpdateSilence(enabled) not applied")
	}
	if err := repo.UpdateSilence(ctx, missingChannelID, biz.SilenceUpdate{Enabled: &disabled}); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("UpdateSilence(missing) error = %v, want ErrSilenceNotFound", err)
	}

	// 列表：keyword（event_pattern 模糊）/ enabled 过滤与分页。
	items, total, err := repo.ListSilences(ctx, biz.SilenceFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListSilences() error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListSilences() = %d items, total %d, want 2/2", len(items), total)
	}
	items, total, err = repo.ListSilences(ctx, biz.SilenceFilter{Keyword: "permission"}, 1, 20)
	if err != nil {
		t.Fatalf("ListSilences(keyword) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "permission.change" {
		t.Fatalf("ListSilences(keyword) = %+v", items)
	}
	enabled := true
	items, total, err = repo.ListSilences(ctx, biz.SilenceFilter{Enabled: &enabled}, 1, 20)
	if err != nil {
		t.Fatalf("ListSilences(enabled) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "*" {
		t.Fatalf("ListSilences(enabled) = %+v", items)
	}
	items, total, err = repo.ListSilences(ctx, biz.SilenceFilter{}, 2, 1)
	if err != nil {
		t.Fatalf("ListSilences(paged) error = %v", err)
	}
	if total != 2 || len(items) != 1 {
		t.Fatalf("ListSilences(paged) = %d items, total %d, want 1/2", len(items), total)
	}

	// 软删：读不可见、版本继续 bump、同 pattern 重建无冲突（无唯一约束）。
	vBefore = silenceCfgVersion(t, d)
	if err := repo.DeleteSilence(ctx, alert.ID); err != nil {
		t.Fatalf("DeleteSilence() error = %v", err)
	}
	if vAfter := silenceCfgVersion(t, d); vAfter != vBefore+1 {
		t.Fatalf("cfg version after delete = %d, want %d", vAfter, vBefore+1)
	}
	if _, err := repo.GetSilence(ctx, alert.ID); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("GetSilence(deleted) error = %v, want ErrSilenceNotFound", err)
	}
	if err := repo.DeleteSilence(ctx, alert.ID); !errors.Is(err, biz.ErrSilenceNotFound) {
		t.Fatalf("DeleteSilence(twice) error = %v, want ErrSilenceNotFound", err)
	}
	rebuild := newSilenceDO("018f3e1a-0303-7000-8000-000000000003", "permission.change")
	if err := repo.CreateSilence(ctx, rebuild); err != nil {
		t.Fatalf("CreateSilence(rebuild same pattern) error = %v", err)
	}
}
