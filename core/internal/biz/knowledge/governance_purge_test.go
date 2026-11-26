package biz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 导入任务保留期：未设置取缺省（30 天），显式设置按配置值，显式 0（及负值）= 不清理。
func TestNewConfig_IngestJobRetention(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		c    *conf.Knowledge
		want time.Duration
	}{
		{name: "nil config", c: nil, want: defaultIngestJobRetention},
		{name: "unset", c: &conf.Knowledge{}, want: defaultIngestJobRetention},
		{name: "explicit zero disables", c: &conf.Knowledge{IngestJobRetention: durationpb.New(0)}, want: 0},
		{name: "negative disables", c: &conf.Knowledge{IngestJobRetention: durationpb.New(-time.Hour)}, want: 0},
		{name: "explicit value", c: &conf.Knowledge{IngestJobRetention: durationpb.New(48 * time.Hour)}, want: 48 * time.Hour},
	}
	for _, tc := range cases {
		if got := NewConfig(tc.c).IngestJobRetention; got != tc.want {
			t.Errorf("%s: IngestJobRetention = %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := NewConfig(nil).LogRetention; got != defaultLogRetention {
		t.Fatalf("LogRetention default changed: %v", got)
	}
}

// purgeIngestRepo 记录 PurgeFinished 的实参，并按脚本返回每批的条数（用尽后返回 0）。
type purgeIngestRepo struct {
	fakeIngestRepo
	calls  []time.Duration
	limits []int
	script []int
}

func (f *purgeIngestRepo) PurgeFinished(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	f.calls = append(f.calls, olderThan)
	f.limits = append(f.limits, limit)
	if len(f.script) == 0 {
		return 0, nil
	}
	n := f.script[0]
	f.script = f.script[1:]
	return n, nil
}

// purgeLogs / purgeDocs 只记录 Purge 的调用并返回预置条数。
type purgeLogs struct {
	SearchLogRepo
	n int
}

func (p *purgeLogs) Purge(context.Context, time.Time, int) (int, error) { return p.n, nil }

type purgeDocs struct {
	*fakeDocumentRepo
	n int
}

func (p *purgeDocs) PurgeDeleted(context.Context, time.Time, int) (int, error) { return p.n, nil }

// 分批清理的停止条件：不足一批即停（已清完）、语句条数用尽即停、时间预算用尽即停、ctx 结束与出错原样返回，
// 返回值始终是已删除的条数。
func TestPurgeBatched_StopConditions(t *testing.T) {
	t.Parallel()
	batch := 10
	scripted := func(results ...int) func(context.Context) (int, error) {
		return func(context.Context) (int, error) {
			if len(results) == 0 {
				t.Fatal("step called after it should have stopped")
			}
			n := results[0]
			results = results[1:]
			return n, nil
		}
	}
	full := func(context.Context) (int, error) { return batch, nil }
	o := loop.PurgeOptions{Batch: batch, MaxStatements: 4, Budget: time.Minute}

	if total, err := loop.PurgeBatched(context.Background(), o, scripted(batch, batch, 3)); err != nil || total != 23 {
		t.Fatalf("drained: total = %d err = %v, want 23 (stops on a partial batch)", total, err)
	}
	if total, err := loop.PurgeBatched(context.Background(), o, scripted(0)); err != nil || total != 0 {
		t.Fatalf("nothing to delete: total = %d err = %v, want 0 after one statement", total, err)
	}
	// 一直删满一批：发满语句条数即停，积压留给下一轮。
	if total, err := loop.PurgeBatched(context.Background(), o, full); err != nil || total != batch*4 {
		t.Fatalf("statement Budget: total = %d err = %v, want %d", total, err, batch*4)
	}
	// 预算已过（负预算）：删满第一批后不再发第二条。
	if total, err := loop.PurgeBatched(context.Background(), loop.PurgeOptions{Batch: batch, MaxStatements: 4, Budget: -time.Second}, full); err != nil || total != batch {
		t.Fatalf("time Budget: total = %d err = %v, want %d", total, err, batch)
	}
	// ctx 结束：删完手上这一批后停，并返回 ctx 的错误。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if total, err := loop.PurgeBatched(ctx, loop.PurgeOptions{Batch: batch, MaxStatements: 4, Budget: time.Minute}, func(context.Context) (int, error) {
		return batch, nil
	}); !errors.Is(err, context.Canceled) || total != batch {
		t.Fatalf("ctx done: total = %d err = %v, want %d + context.Canceled", total, err, batch)
	}
	// 出错：带上出错前已删的条数返回。
	boom := errors.New("db down")
	if total, err := loop.PurgeBatched(context.Background(), o, func(context.Context) (int, error) {
		return batch, boom
	}); !errors.Is(err, boom) || total != batch {
		t.Fatalf("step error: total = %d err = %v, want %d + boom", total, err, batch)
	}
}

// Purge 里的导入任务清理：缺省保留期 30 天、按批大小下发；保留期 0 时一条语句都不发；清理条数不计入 Purge 的
// 返回值（否则积压会让循环连续空转），积压时一轮至多发满语句条数；仓储出错原样返回，前两步的条数不丢。
func TestGovernancePurge_IngestJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	newGov := func(jobs IngestRepo, retention time.Duration) *GovernanceUsecase {
		return NewGovernanceUsecase(&purgeDocs{}, &fakeSpaceRepo{}, &purgeLogs{n: 2}, jobs, fakeChunkRepo{}, fakeTxm{}, nil, fakeDirectory{},
			Config{LogRetention: defaultLogRetention, IngestJobRetention: retention}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}

	jobs := &purgeIngestRepo{script: []int{3}}
	if n, err := newGov(jobs, defaultIngestJobRetention).Purge(ctx); err != nil || n != 2 {
		t.Fatalf("Purge() = %d, %v; want 2 (search logs + deleted docs only)", n, err)
	}
	// 缺省值：NewConfig 未设置时的 30 天；批大小是导入任务清理自己的批大小（不是日志 / 文档的 purgeBatch）。
	jobs = &purgeIngestRepo{}
	if n, err := newGov(jobs, NewConfig(nil).IngestJobRetention).Purge(ctx); err != nil || n != 2 || len(jobs.calls) != 1 {
		t.Fatalf("Purge() = %d, %v calls = %v; want one PurgeFinished call", n, err, jobs.calls)
	}
	if jobs.calls[0] != 30*24*time.Hour || jobs.limits[0] != ingestPurgeBatch || jobs.limits[0] == purgeBatch {
		t.Fatalf("PurgeFinished called with retention = %v limit = %d", jobs.calls[0], jobs.limits[0])
	}
	// 显式 0 = 不清理：不发语句。
	jobs = &purgeIngestRepo{}
	if n, err := newGov(jobs, 0).Purge(ctx); err != nil || n != 2 || len(jobs.calls) != 0 {
		t.Fatalf("retention 0: Purge() = %d, %v calls = %v; want no PurgeFinished call", n, err, jobs.calls)
	}
	// 积压：每批都删满时一轮至多发 ingestPurgeStatements 条，返回值仍不含导入任务的条数。
	jobs = &purgeIngestRepo{script: slices.Repeat([]int{ingestPurgeBatch}, ingestPurgeStatements+3)}
	if n, err := newGov(jobs, defaultIngestJobRetention).Purge(ctx); err != nil || n != 2 || len(jobs.calls) != ingestPurgeStatements {
		t.Fatalf("backlog: Purge() = %d, %v calls = %d; want %d statements and the loop count unaffected", n, err, len(jobs.calls), ingestPurgeStatements)
	}
	// 仓储出错：返回错误，前两步已清的条数照常返回。
	failing := &failingPurgeIngestRepo{}
	if n, err := newGov(failing, defaultIngestJobRetention).Purge(ctx); !errors.Is(err, errPurgeBoom) || n != 2 || failing.calls != 1 {
		t.Fatalf("repo error: Purge() = %d, %v calls = %d; want 2 + errPurgeBoom", n, err, failing.calls)
	}
}

var errPurgeBoom = errors.New("purge boom")

type failingPurgeIngestRepo struct {
	fakeIngestRepo
	calls int
}

func (f *failingPurgeIngestRepo) PurgeFinished(context.Context, time.Duration, int) (int, error) {
	f.calls++
	return 0, errPurgeBoom
}
