package biz

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// batchCardsCMDB 模拟 CMDB 适配器：Cards 一次至多返回 MaxCardsPerLookup 个存在的资源。
type batchCardsCMDB struct {
	CMDB
	exists map[string]bool
	calls  []int
}

func (c *batchCardsCMDB) Cards(_ context.Context, ids []string) ([]*CICard, error) {
	c.calls = append(c.calls, len(ids))
	out := make([]*CICard, 0, len(ids))
	for _, id := range ids[:min(len(ids), MaxCardsPerLookup)] {
		if c.exists[id] {
			out = append(out, &CICard{ID: id})
		}
	}
	return out, nil
}

// 靶标去重后超过 Cards 的单次上限时必须分批校验：此前整批比较长度，上游并集一超过 200 个，
// 处置建议就恒报「不存在于 CMDB」。
func TestProposalAllowedTargets_BatchedCardLookup(t *testing.T) {
	t.Parallel()
	affected := make([]string, 0, 450)
	for i := range 450 {
		affected = append(affected, fmt.Sprintf("018f3e1a-0a30-7000-8000-%012x", i))
	}
	exists := map[string]bool{}
	for _, id := range affected {
		exists[id] = true
	}
	cmdb := &batchCardsCMDB{exists: exists}
	d := &Deps{CMDB: cmdb, Logger: testLogger()}
	uc := NewProposalUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)
	inc := &Incident{AffectedCIIDs: slices.Clone(affected)}

	if err := uc.allowedTargets(context.Background(), inc, affected); err != nil {
		t.Fatalf("allowedTargets() with 450 existing targets: error = %v", err)
	}
	if !slices.Equal(cmdb.calls, []int{MaxCardsPerLookup, MaxCardsPerLookup, 50}) {
		t.Fatalf("Cards batch sizes = %v, want [%d %d 50]", cmdb.calls, MaxCardsPerLookup, MaxCardsPerLookup)
	}

	delete(exists, affected[449]) // 最后一批里的一个靶标不存在
	if err := uc.allowedTargets(context.Background(), inc, affected); !ErrProposalInvalid.Is(err) {
		t.Fatalf("allowedTargets() with one missing target: error = %v, want ErrProposalInvalid", err)
	}
}
