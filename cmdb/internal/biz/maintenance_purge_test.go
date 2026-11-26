package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
)

// purgeRecorder 按脚本返回每批的条数（脚本用尽后返回 0），记录每次调用的保留期与批大小。
type purgeRecorder struct {
	retentions []time.Duration
	limits     []int
	script     []int
	err        error // 脚本用尽后返回的错误
	calls      int
}

func (p *purgeRecorder) purge(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	p.calls++
	p.retentions = append(p.retentions, olderThan)
	p.limits = append(p.limits, limit)
	if len(p.script) == 0 {
		return 0, p.err
	}
	n := p.script[0]
	p.script = p.script[1:]
	return n, nil
}

// 分批清理的停止条件：两张表都不足一批即停（已清完）；任一张删满一批就继续，语句条数用尽即停；运行记录已清完而任务
// 还有积压时只继续删任务；运行记录又删满一批时，已判「清完」的任务重新参与（任务在运行记录删掉之后才可删）；
// 时间预算用尽即停；ctx 结束与出错原样返回，返回值始终是已删除的条数。
func TestPurgeBatched_StopConditions(t *testing.T) {
	t.Parallel()
	const batch = 10
	o := loop.PurgeOptions{Batch: batch, MaxStatements: 6, Budget: time.Minute}
	pair := func(runs, tasks *purgeRecorder) []func(context.Context) (int, error) {
		return []func(context.Context) (int, error){
			func(ctx context.Context) (int, error) { return runs.purge(ctx, 0, 0) },
			func(ctx context.Context) (int, error) { return tasks.purge(ctx, 0, 0) },
		}
	}

	runs, tasks := &purgeRecorder{script: []int{3}}, &purgeRecorder{script: []int{2}}
	if total, err := loop.PurgeBatched(context.Background(), o, pair(runs, tasks)...); err != nil || total != 5 || runs.calls != 1 || tasks.calls != 1 {
		t.Fatalf("drained: total = %d err = %v calls = %d/%d; want 5 after one statement each", total, err, runs.calls, tasks.calls)
	}

	// 两张表都有积压：交替各发一条，发满语句条数即停。
	runs, tasks = &purgeRecorder{}, &purgeRecorder{}
	for range 10 {
		runs.script = append(runs.script, batch)
		tasks.script = append(tasks.script, batch)
	}
	if total, err := loop.PurgeBatched(context.Background(), o, pair(runs, tasks)...); err != nil || total != batch*6 || runs.calls != 3 || tasks.calls != 3 || len(runs.script) != 7 {
		t.Fatalf("both backlogged: total = %d err = %v calls = %d/%d; want %d statements in all (3 each)", total, err, runs.calls, tasks.calls, 6)
	}

	// 运行记录已清完、任务还有上一轮留下的积压：运行记录那一句不再重复，只继续删任务。
	runs, tasks = &purgeRecorder{}, &purgeRecorder{}
	for range 10 {
		tasks.script = append(tasks.script, batch)
	}
	if total, err := loop.PurgeBatched(context.Background(), o, pair(runs, tasks)...); err != nil || total != batch*5 || runs.calls != 1 || tasks.calls != 5 {
		t.Fatalf("tasks only: total = %d err = %v calls = %d/%d; want 1 run statement then 5 task statements", total, err, runs.calls, tasks.calls)
	}

	// 依赖：运行记录每轮都删满一批，任务第一轮不足一批（这一批运行记录多属同一任务的多次尝试），之后又有可删的——
	// 已判「清完」的任务必须重新参与，而不是留到下一轮。
	runs, tasks = &purgeRecorder{script: []int{batch, batch, batch}}, &purgeRecorder{script: []int{2, 4, 6, 9}}
	// 不重新参与的话任务只会在第一轮删掉 2 条（total = 32、1 次调用）。
	if total, err := loop.PurgeBatched(context.Background(), loop.PurgeOptions{Batch: batch, MaxStatements: 10, Budget: time.Minute}, pair(runs, tasks)...); err != nil ||
		total != batch*3+2+4+6+9 || tasks.calls != 4 {
		t.Fatalf("dependent step re-enabled: total = %d err = %v tasks calls = %d; want the task statement to follow every full run batch", total, err, tasks.calls)
	}

	// 预算已过：一轮只跑一遍（两条语句）。
	runs, tasks = &purgeRecorder{}, &purgeRecorder{}
	for range 10 {
		runs.script = append(runs.script, batch)
		tasks.script = append(tasks.script, batch)
	}
	if total, err := loop.PurgeBatched(context.Background(), loop.PurgeOptions{Batch: batch, MaxStatements: 6, Budget: -time.Second}, pair(runs, tasks)...); err != nil || total != batch*2 {
		t.Fatalf("time Budget: total = %d err = %v, want one pass over both statements (%d)", total, err, batch*2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runs, tasks = &purgeRecorder{script: []int{batch}}, &purgeRecorder{script: []int{batch}}
	if total, err := loop.PurgeBatched(ctx, o, pair(runs, tasks)...); !errors.Is(err, context.Canceled) || total != batch*2 {
		t.Fatalf("ctx done: total = %d err = %v, want %d + context.Canceled", total, err, batch*2)
	}

	boom := errors.New("db down")
	runs, tasks = &purgeRecorder{script: []int{batch}}, &purgeRecorder{err: boom}
	if total, err := loop.PurgeBatched(context.Background(), o, pair(runs, tasks)...); !errors.Is(err, boom) || total != batch {
		t.Fatalf("step error: total = %d err = %v, want %d + boom", total, err, batch)
	}
}

// purgeRepo 维护仓储桩：分区维护恒成功，清理方法委托给两个 recorder。
type purgeRepo struct {
	fakeMaintenance
	runs, tasks *purgeRecorder
}

func (r *purgeRepo) PurgeRuns(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return r.runs.purge(ctx, olderThan, limit)
}

func (r *purgeRepo) PurgeTasks(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return r.tasks.purge(ctx, olderThan, limit)
}

func newPurgeRepo() *purgeRepo {
	return &purgeRepo{runs: &purgeRecorder{}, tasks: &purgeRecorder{}}
}

// 维护循环里的同步记录清理：缺省保留期 180 天（不调 WithOptions 也是它）、按批大小下发、运行记录在前任务在后；
// 保留期 0 不发语句；账号没有 DELETE 权限（ErrPurgeDenied）只告警、不算这一轮失败；其它错误原样返回。
func TestMaintenance_RunOncePurge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

	repo := newPurgeRepo()
	if err := NewMaintenanceUsecase(repo, repo, testLogger()).RunOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if repo.runs.calls != 1 || repo.tasks.calls != 1 || repo.runs.retentions[0] != DefaultRunRetention || repo.tasks.retentions[0] != DefaultRunRetention {
		t.Fatalf("default options: runs = %+v tasks = %+v; want one statement each at the default retention %v", *repo.runs, *repo.tasks, DefaultRunRetention)
	}
	if repo.runs.limits[0] != runPurgeBatch || repo.tasks.limits[0] != runPurgeBatch {
		t.Fatalf("batch size = %d / %d, want %d", repo.runs.limits[0], repo.tasks.limits[0], runPurgeBatch)
	}

	// 显式保留期：下发给两条语句。
	repo = newPurgeRepo()
	if err := NewMaintenanceUsecase(repo, repo, testLogger()).WithOptions(MaintenanceOptions{RunRetention: 48 * time.Hour}).RunOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if repo.runs.retentions[0] != 48*time.Hour || repo.tasks.retentions[0] != 48*time.Hour {
		t.Fatalf("explicit retention not passed down: runs = %v tasks = %v", repo.runs.retentions, repo.tasks.retentions)
	}

	// 保留期 0 = 不清理：分区维护照常，清理一条语句都不发。
	repo = newPurgeRepo()
	if err := NewMaintenanceUsecase(repo, repo, testLogger()).WithOptions(MaintenanceOptions{RunRetention: 0}).RunOnce(ctx, now); err != nil {
		t.Fatal(err)
	}
	if len(repo.ensured) != 3 || repo.runs.calls != 0 || repo.tasks.calls != 0 {
		t.Fatalf("retention 0: ensured = %v runs = %d tasks = %d; want partitions maintained and no purge statement", repo.ensured, repo.runs.calls, repo.tasks.calls)
	}

	// 账号没有 DELETE 权限：不算这一轮的失败（循环不该为此退避、反复重跑分区维护）。
	repo = newPurgeRepo()
	repo.runs.err = ErrPurgeDenied
	if err := NewMaintenanceUsecase(repo, repo, testLogger()).RunOnce(ctx, now); err != nil {
		t.Fatalf("ErrPurgeDenied must not fail the round, got %v", err)
	}
	if repo.runs.calls != 1 || repo.tasks.calls != 0 {
		t.Fatalf("purge denied: runs = %d tasks = %d; want the failing statement to stop the purge for this round", repo.runs.calls, repo.tasks.calls)
	}

	// 其它错误：原样返回，分区维护的结论不受影响。
	boom := errors.New("db down")
	repo = newPurgeRepo()
	repo.tasks.err = boom
	if err := NewMaintenanceUsecase(repo, repo, testLogger()).RunOnce(ctx, now); !errors.Is(err, boom) {
		t.Fatalf("purge error = %v, want boom", err)
	}
}
