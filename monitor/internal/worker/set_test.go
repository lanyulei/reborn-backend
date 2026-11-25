package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
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

func build(t *testing.T, c *conf.Engine) *Set { return buildWith(t, c, nil) }

// failingBeats 恒失败的心跳仓储，记调用次数。
type failingBeats struct{ calls atomic.Int32 }

func (f *failingBeats) Beat(context.Context, *biz.InstanceBeat) error {
	f.calls.Add(1)
	return errors.New("db down")
}

func (*failingBeats) Remove(context.Context, string) error { return nil }

func buildWith(t *testing.T, c *conf.Engine, beats biz.InstanceRepo) *Set {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache, cleanup := biz.NewConfigCache(nil, nil, nopObserver{}, logger)
	t.Cleanup(cleanup)
	obs := nopObserver{}
	inhibits, stats := biz.NewInhibitCache(nil, cache, obs, logger), biz.NewInstanceStats()
	info := biz.InstanceInfo{ID: "host-abc", Roles: "planner,evaluator"}
	heartbeat, _ := biz.NewHeartbeatUsecase(beats, info, stats, logger)
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

// 心跳循环的失败退避不超过一个心跳周期（monitor-module-biz 优化项 5）。周期 20ms、连续失败时：封顶后每
// 20ms 重试一次；kit/loop 缺省的指数退避是 40 / 80 / 160 / 320 / 640ms，700ms 内至多 5 次。
func TestHeartbeatBackoffIsCappedAtItsInterval(t *testing.T) {
	beats := &failingBeats{}
	set := buildWith(t, &conf.Engine{HeartbeatInterval: durationpb.New(20 * time.Millisecond)}, beats)
	if len(set.Always) != 1 || set.Always[0].Name() != "heartbeat" {
		t.Fatalf("always loops = %v", set.Always)
	}
	hb := set.Always[0]
	done := make(chan error, 1)
	go func() { done <- hb.Start(context.Background()) }()
	time.Sleep(700 * time.Millisecond)
	if err := hb.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := beats.calls.Load(); got < 10 {
		t.Fatalf("heartbeat attempts in 700ms = %d: the failure backoff is not capped at the 20ms interval", got)
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
