package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/loop"
)

// scheduleBatchSize 测试队列每轮交出的到期计划数。
const scheduleBatchSize = 3

// dueQueue 到期的定时作业队列：每轮交出至多一批到期计划，记下每轮读取的时刻。
type dueQueue struct {
	mu     sync.Mutex
	left   int
	rounds []time.Time
}

// Due implements bizjob.ScheduleQueue.
func (q *dueQueue) Due(context.Context, int) ([]*bizjob.Schedule, time.Time, error) {
	q.mu.Lock()
	q.rounds = append(q.rounds, time.Now())
	n := min(q.left, scheduleBatchSize)
	q.left -= n
	first := len(q.rounds) * scheduleBatchSize
	q.mu.Unlock()
	now := time.Date(2026, 11, 18, 10, 0, 30, 0, time.UTC)
	items := make([]*bizjob.Schedule, 0, n)
	for i := range n {
		items = append(items, &bizjob.Schedule{
			ID: fmt.Sprintf("018f3e1a-0000-7000-8000-%012d", first+i+1), TemplateID: "018f3e1a-0000-7000-8000-0000000000aa",
			Cron: "0 * * * *", Timezone: "UTC", Misfire: jc.MisfireSkip, NextTime: &now, Owner: "u",
		})
	}
	return items, now, nil
}

// Fire implements bizjob.ScheduleQueue：一律认领成功。
func (q *dueQueue) Fire(_ context.Context, id string, planned time.Time, fn func(context.Context, *bizjob.Schedule) (bizjob.ScheduleRun, error)) (bool, error) {
	_, err := fn(context.Background(), &bizjob.Schedule{
		ID: id, TemplateID: "018f3e1a-0000-7000-8000-0000000000aa", Cron: "0 * * * *", Timezone: "UTC", Misfire: jc.MisfireSkip,
		NextTime: &planned, Owner: "u",
	})
	return err == nil, err
}

func (q *dueQueue) reads() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.rounds)
}

// 同一时刻到期的计划多于一批：定时循环触发完一批立即续跑下一批，不再每批固定等 10 秒的轮询周期；
// 队列取空后回到周期轮询。
func TestScheduleLoopDrainsBacklogWithoutWaiting(t *testing.T) {
	t.Parallel()
	js := testfake.NewJobStore()
	org := &testfake.MonitorOrg{}
	execs := bizjob.NewExecutionUsecase(js.Executions(), js.Executions(), js.Executions(), js.Templates(), js.Projects(), js.Credentials(), js.Runners(), nil,
		org, bizjob.NewExecutionDeps(testfake.NewTxManager(), js.Policies(), js.RiskRules(), js.Scripts(), &testfake.JobFiles{},
			&testfake.JobApprovals{}, testfake.JobSources{}, nil, testfake.JobRuns{}))
	// 三批到期计划。模板不存在：每次触发都被业务规则拒绝、记为失败并照常推进，没有别的副作用。
	q := &dueQueue{left: 3 * scheduleBatchSize}
	schedules := bizjob.NewScheduleUsecase(nil, q, js.Templates(), execs, org)

	var schedule *loop.Loop
	for _, s := range NewJobLoops(schedules, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil).Servers() {
		if lp, ok := s.(*loop.Loop); ok && lp.Name() == "job-schedule" {
			schedule = lp
		}
	}
	if schedule == nil {
		t.Fatal("the job-schedule loop is not registered")
	}
	// 循环脱离传入 ctx 的取消（kit/loop），由 Stop 停止。
	go func() { _ = schedule.Start(context.Background()) }()
	t.Cleanup(func() {
		if err := schedule.Stop(context.Background()); err != nil {
			t.Errorf("stop the loop: %v", err)
		}
	})

	// 三批各读一次，再加一次读到空的：都远少于 10 秒的轮询周期。每批之后等一个周期的话，3 秒内只读得到第一批。
	deadline := time.Now().Add(3 * time.Second)
	for q.reads() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := q.reads(); n != 4 {
		t.Fatalf("the loop read the queue %d times in 3s, want 3 batches plus one empty round: it waits the polling interval between batches", n)
	}
	// 队列已空：读不到到期的计划，回到周期轮询，不会连续空转。
	time.Sleep(300 * time.Millisecond)
	if n := q.reads(); n != 4 {
		t.Fatalf("the idle loop kept polling: %d reads", n)
	}
}
