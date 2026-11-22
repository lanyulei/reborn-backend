package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingOverview struct{ calls int }

func (c *countingOverview) Stats(context.Context, OverviewFilter) (*Overview, error) {
	return &Overview{}, nil
}

func (c *countingOverview) Quality(context.Context, OverviewFilter, time.Time) ([]*QualityRow, error) {
	c.calls++
	return []*QualityRow{{ModelCode: "host", Total: int64(c.calls)}}, nil
}

// 数据质量按数据范围缓存：同一范围在有效期内只算一次；不同范围各算各的；返回值是副本。
func TestInsightUsecase_QualityCache(t *testing.T) {
	t.Parallel()
	repo := &countingOverview{}
	uc := NewInsightUsecase(nil, nil, repo, nil, nil, nil)
	ctx := context.Background()
	a1, err := uc.Quality(ctx, OverviewFilter{})
	if err != nil {
		t.Fatal(err)
	}
	a1[0].Total = 99 // 改写返回值不得污染缓存
	a2, _ := uc.Quality(ctx, OverviewFilter{})
	if repo.calls != 1 || a2[0].Total != 1 {
		t.Fatalf("same scope must hit the cache: calls=%d row=%+v", repo.calls, a2[0])
	}
	// 不限（nil）与"什么都看不到"（空集合）是不同的范围；顺序不同的同一集合是同一范围。
	if _, _ = uc.Quality(ctx, OverviewFilter{ModelCodes: []string{}}); repo.calls != 2 {
		t.Fatalf("empty scope must not share the unrestricted entry: calls=%d", repo.calls)
	}
	_, _ = uc.Quality(ctx, OverviewFilter{ModelCodes: []string{"a", "b"}})
	_, _ = uc.Quality(ctx, OverviewFilter{ModelCodes: []string{"b", "a"}})
	if repo.calls != 3 {
		t.Fatalf("scope key must be order-insensitive: calls=%d", repo.calls)
	}
	// 并发未命中合并为一次计算。
	slow := &slowOverview{release: make(chan struct{})}
	uc2 := NewInsightUsecase(nil, nil, slow, nil, nil, nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = uc2.Quality(ctx, OverviewFilter{})
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(slow.release)
	wg.Wait()
	if n := slow.calls.Load(); n != 1 {
		t.Fatalf("concurrent misses must be coalesced: calls=%d", n)
	}
	// 请求已取消也能算完并入缓存（计算脱离请求取消）。
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := uc.Quality(cctx, OverviewFilter{Envs: []string{"prod"}}); err != nil || repo.calls != 4 {
		t.Fatalf("cancelled request: err=%v calls=%d", err, repo.calls)
	}
}

type slowOverview struct {
	calls   atomic.Int32
	release chan struct{}
}

func (s *slowOverview) Stats(context.Context, OverviewFilter) (*Overview, error) {
	return &Overview{}, nil
}

func (s *slowOverview) Quality(context.Context, OverviewFilter, time.Time) ([]*QualityRow, error) {
	s.calls.Add(1)
	<-s.release
	return []*QualityRow{{ModelCode: "host"}}, nil
}

// 运营概览按（范围，天数）缓存：同一组合在有效期内只算一次；天数不同各算各的；返回值是副本。
func TestInsightUsecase_OverviewCache(t *testing.T) {
	t.Parallel()
	repo := &countingStats{}
	uc := NewInsightUsecase(nil, nil, repo, nil, nil, nil)
	ctx := context.Background()
	a1, err := uc.Overview(ctx, OverviewFilter{}, 7)
	if err != nil {
		t.Fatal(err)
	}
	a1.ByModel[0].Count = 99 // 改写返回值不得污染缓存
	a2, _ := uc.Overview(ctx, OverviewFilter{}, 7)
	if repo.calls != 1 || a2.ByModel[0].Count != 1 || a2.Days != 7 {
		t.Fatalf("same scope and days must hit the cache: calls=%d overview=%+v", repo.calls, a2)
	}
	if _, _ = uc.Overview(ctx, OverviewFilter{}, 30); repo.calls != 2 {
		t.Fatalf("different days must not share the entry: calls=%d", repo.calls)
	}
	if _, _ = uc.Overview(ctx, OverviewFilter{ModelCodes: []string{}}, 7); repo.calls != 3 {
		t.Fatalf("empty scope must not share the unrestricted entry: calls=%d", repo.calls)
	}
}

type countingStats struct{ calls int }

func (c *countingStats) Stats(context.Context, OverviewFilter) (*Overview, error) {
	c.calls++
	return &Overview{ByModel: []OverviewCount{{Key: "host", Count: int64(c.calls)}}}, nil
}

func (c *countingStats) Quality(context.Context, OverviewFilter, time.Time) ([]*QualityRow, error) {
	return nil, nil
}
