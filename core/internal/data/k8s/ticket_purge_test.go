package data

import (
	"sync"
	"testing"
	"time"
)

// 清理节流：实例的第一次签发会清；之后一个间隔内不再清；间隔过后恰好再清一次；
// 并发的签发里只有一个去清；清理失败后把节流点退回去，下一次照旧先清。
func TestTicketPurgeThrottle(t *testing.T) {
	t.Parallel()
	r := &ticketRepo{}
	due, prev := r.purgeDue()
	if !due || prev != 0 {
		t.Fatalf("first purgeDue() = %v, %d, want true, 0", due, prev)
	}
	for range 5 {
		if due, _ := r.purgeDue(); due {
			t.Fatal("purgeDue() = true within the interval, want false")
		}
	}

	// 间隔已过（把节流点拨回过去），并发调用里恰好一个轮到。
	r.nextPurge.Store(int64(time.Since(purgeEpoch)) - 1)
	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for range 16 {
		wg.Go(func() {
			if due, _ := r.purgeDue(); due {
				mu.Lock()
				won++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if won != 1 {
		t.Fatalf("concurrent purgeDue() winners = %d, want 1", won)
	}

	// 清理失败的回退：Create 把节流点存回 prev，下一次仍轮到。
	r.nextPurge.Store(int64(time.Since(purgeEpoch)) - 1)
	due, prev = r.purgeDue()
	if !due {
		t.Fatal("purgeDue() = false after the interval elapsed")
	}
	r.nextPurge.Store(prev)
	if due, _ := r.purgeDue(); !due {
		t.Fatal("purgeDue() = false after a failed purge was rolled back, want true")
	}
}
