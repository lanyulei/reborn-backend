package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// setMaintenanceRepo 维护仓储桩：分区维护恒成功（记下删除分区的截止时刻），清理方法记下保留期。
type setMaintenanceRepo struct {
	dropped  []time.Time
	purgedAt []time.Duration
}

func (r *setMaintenanceRepo) EnsurePartition(context.Context, string, time.Time) error { return nil }

func (r *setMaintenanceRepo) DropPartitionsBefore(_ context.Context, _ string, before time.Time) (int, error) {
	r.dropped = append(r.dropped, before)
	return 0, nil
}

func (r *setMaintenanceRepo) PurgeRuns(_ context.Context, olderThan time.Duration, _ int) (int, error) {
	r.purgedAt = append(r.purgedAt, olderThan)
	return 0, nil
}

func (r *setMaintenanceRepo) PurgeTasks(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

// retentionDur：未设置取缺省；显式 0 与负值按原值下发（= 不删除 / 不清理），不像 dur 那样被当成缺省。
func TestRetentionDur(t *testing.T) {
	t.Parallel()
	const def = 9 * time.Hour
	cases := []struct {
		name string
		d    *durationpb.Duration
		want time.Duration
	}{
		{name: "unset", d: nil, want: def},
		{name: "explicit zero is kept", d: durationpb.New(0), want: 0},
		{name: "negative is kept", d: durationpb.New(-time.Hour), want: -time.Hour},
		{name: "explicit value", d: durationpb.New(72 * time.Hour), want: 72 * time.Hour},
	}
	for _, tc := range cases {
		if got := retentionDur(tc.d, def); got != tc.want {
			t.Errorf("%s: retentionDur() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// NewSet 把配置注入维护用例：run_retention 未设置取 180 天，显式设置按配置值，显式 0 = 不清理；history_retention
// 的既有口径不变（未设置取 365 天，显式 0 = 不删除分区）。
func TestNewSet_InjectsRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		history     *durationpb.Duration
		run         *durationpb.Duration
		wantDrop    *time.Time // nil = 不删分区
		wantPurgeAt []time.Duration
	}{
		{name: "defaults", wantDrop: ptrOf(now.Add(-365 * 24 * time.Hour)), wantPurgeAt: []time.Duration{biz.DefaultRunRetention}},
		{name: "run retention zero disables purge", run: durationpb.New(0), wantDrop: ptrOf(now.Add(-365 * 24 * time.Hour))},
		{name: "history retention zero disables drop", history: durationpb.New(0), wantPurgeAt: []time.Duration{biz.DefaultRunRetention}},
		{
			name: "explicit values", history: durationpb.New(48 * time.Hour), run: durationpb.New(72 * time.Hour),
			wantDrop: ptrOf(now.Add(-48 * time.Hour)), wantPurgeAt: []time.Duration{72 * time.Hour},
		},
	}
	for _, tc := range cases {
		repo := &setMaintenanceRepo{}
		maintenance := biz.NewMaintenanceUsecase(repo, repo, testLogger())
		if _, err := NewSet(&conf.Sync{Maintenance: &conf.Sync_Maintenance{HistoryRetention: tc.history, RunRetention: tc.run}},
			biz.NewSchedulerUsecase(nil, nil, testLogger()), biz.NewWorkerUsecase(nil, nil, nil, nil, nil, nil, nil, nil, testLogger()),
			maintenance, nil, nil, testLogger()); err != nil {
			t.Fatalf("%s: NewSet() error = %v", tc.name, err)
		}
		if err := maintenance.RunOnce(ctx, now); err != nil {
			t.Fatalf("%s: RunOnce() error = %v", tc.name, err)
		}
		if (len(repo.dropped) == 0) != (tc.wantDrop == nil) || (tc.wantDrop != nil && !repo.dropped[0].Equal(*tc.wantDrop)) {
			t.Errorf("%s: partitions dropped before %v, want %v", tc.name, repo.dropped, tc.wantDrop)
		}
		if len(repo.purgedAt) != len(tc.wantPurgeAt) || (len(tc.wantPurgeAt) == 1 && repo.purgedAt[0] != tc.wantPurgeAt[0]) {
			t.Errorf("%s: PurgeRuns called with %v, want %v", tc.name, repo.purgedAt, tc.wantPurgeAt)
		}
	}
}

func ptrOf(t time.Time) *time.Time { return &t }
