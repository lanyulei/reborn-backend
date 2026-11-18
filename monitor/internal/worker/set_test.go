package worker

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

type nopLocker struct{}

func (nopLocker) TryLock(context.Context, string) (func(), bool, error) { return nil, false, nil }

type nopObserver struct{}

func (nopObserver) ObserveEval(context.Context, string, time.Duration, time.Duration) {}
func (nopObserver) AddOutbox(context.Context, int32, int)                             {}
func (nopObserver) AddMutesTruncated(context.Context)                                 {}
func (nopObserver) AddTruncated(context.Context, string)                              {}
func (nopObserver) AddInhibitCycles(context.Context, int)                             {}
func (nopObserver) ObserveRecord(context.Context, string, time.Duration, int)         {}
func (nopObserver) AddCommand(context.Context, string)                                {}
func (nopObserver) AddEngineEvents(context.Context, string, int)                      {}

func build(t *testing.T, c *conf.Engine) *Set {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache, cleanup := biz.NewConfigCache(nil, nil, nopObserver{}, logger)
	t.Cleanup(cleanup)
	obs := nopObserver{}
	inhibits, stats := biz.NewInhibitCache(nil, cache, obs, logger), biz.NewInstanceStats()
	info := biz.InstanceInfo{ID: "host-abc", Roles: "planner,evaluator"}
	heartbeat, _ := biz.NewHeartbeatUsecase(nil, info, stats, logger)
	return NewSet(c, info, NewUsecases(cache, inhibits, stats, biz.NewPlannerUsecase(nil, cache, obs, logger),
		biz.NewEvaluatorUsecase(nil, nil, nil, cache, inhibits, stats, obs, logger),
		biz.NewMaintenanceUsecase(nil, nil, logger), biz.NewCommandUsecase(nil, obs, logger),
		biz.NewRecordUsecase(nil, cache, stats, obs, logger), biz.NewWatchUsecase(nil, obs, logger), heartbeat),
		nopLocker{}, nil, logger)
}

func names(set *Set) string {
	var out []string
	for _, l := range set.Planner {
		out = append(out, l.Name())
	}
	for _, l := range set.Evaluators {
		out = append(out, l.Name())
	}
	for _, l := range set.Always {
		out = append(out, l.Name())
	}
	return strings.Join(out, ",")
}

func TestNewSetAppliesDefaults(t *testing.T) {
	// 空配置（含 nil）：conf.proto 注明的缺省值——规划器四条循环；4 条评估循环、1 条记录规则循环、
	// 1 条事件指令循环；心跳不分角色。
	for _, c := range []*conf.Engine{nil, {}, {Evaluator: &conf.Engine_Evaluator{Concurrency: -1}}} {
		if got := names(build(t, c)); got != "materialize,probe,maintenance,watch,evaluator-0,evaluator-1,evaluator-2,evaluator-3,recorder-0,command,heartbeat" {
			t.Fatalf("loops = %s", got)
		}
	}
	set := build(t, &conf.Engine{
		Evaluator:   &conf.Engine_Evaluator{Concurrency: 2, Batch: 5, Lease: durationpb.New(90 * time.Second)},
		Maintenance: &conf.Engine_Maintenance{AlertRetention: durationpb.New(0)},
		Recorder:    &conf.Engine_Recorder{Concurrency: 2},
	})
	if got := names(set); got != "materialize,probe,maintenance,watch,evaluator-0,evaluator-1,recorder-0,recorder-1,command,heartbeat" {
		t.Fatalf("loops = %s", got)
	}
}

func TestDefaultHelpers(t *testing.T) {
	if dur(nil, time.Second) != time.Second || dur(durationpb.New(0), time.Second) != time.Second ||
		dur(durationpb.New(-time.Minute), time.Second) != time.Second || dur(durationpb.New(time.Minute), time.Second) != time.Minute {
		t.Fatal("dur() must fall back to the default for unset or non-positive values")
	}
	if num(0, 7) != 7 || num(-3, 7) != 7 || num(2, 7) != 2 {
		t.Fatal("num() must fall back to the default for non-positive values")
	}
}
