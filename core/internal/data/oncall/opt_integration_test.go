//go:build integration

package data

import (
	"context"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
)

// 事务内读班次（Tx.ListShiftsRange，每日播报用）与 ShiftRepo.ListRange 读到同一批、同样顺序的班次；
// OnDuty 改为按值班表下推后，结果与从全量班次里逐行判定一致。
func TestIntegration_ShiftReadsEquivalent(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	cfg := dailyConfig(now.Add(-time.Hour), uA, uB, uC)
	s1, err := e.schedules.Create(ctx, e.admin, g.ID, cfg)
	if err != nil {
		t.Fatalf("create s1: %v", err)
	}
	cfg.Name, cfg.MemberIDs = "夜间值班", []string{uB, uC, uA}
	s2, err := e.schedules.Create(ctx, e.admin, g.ID, cfg)
	if err != nil {
		t.Fatalf("create s2: %v", err)
	}

	ids := func(shifts []*biz.Shift) []string {
		out := make([]string, 0, len(shifts))
		for _, sh := range shifts {
			out = append(out, sh.ID)
		}
		return out
	}
	for _, w := range [][2]time.Time{{now, now.Add(time.Second)}, {now, now.AddDate(0, 0, 10)}} {
		viaPool, err := e.shiftRepo.ListRange(ctx, w[0], w[1])
		if err != nil {
			t.Fatalf("ListRange: %v", err)
		}
		var viaTx []*biz.Shift
		if err := e.store.WithinTx(ctx, func(tx biz.Tx) error {
			var err error
			viaTx, err = tx.ListShiftsRange(ctx, w[0], w[1])
			return err
		}); err != nil {
			t.Fatalf("ListShiftsRange: %v", err)
		}
		if len(viaPool) == 0 || !slices.Equal(ids(viaPool), ids(viaTx)) {
			t.Fatalf("window %v: pool %v, tx %v", w, ids(viaPool), ids(viaTx))
		}
	}

	duty := biz.NewDutyUsecase(e.schedRepo, e.shiftRepo)
	all, err := e.shiftRepo.ListRange(ctx, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*biz.Schedule{s1, s2} {
		var want []string
		for _, role := range []int32{biz.RolePrimary, biz.RoleBackup} {
			for _, sh := range all {
				if sh.ScheduleID == s.ID && sh.Role == role && !sh.Start.After(now) && sh.End.After(now) {
					want = append(want, sh.UserID)
				}
			}
		}
		got, err := duty.OnDuty(ctx, s.ID, now)
		if err != nil || len(want) != 2 || !slices.Equal(got, want) {
			t.Fatalf("OnDuty(%s) = %v, %v, want %v", s.Name, got, err, want)
		}
	}
}
