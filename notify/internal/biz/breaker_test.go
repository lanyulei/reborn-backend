package biz

import (
	"testing"
	"time"
)

func TestBreaker(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	b := NewBreaker()
	b.now = func() time.Time { return now }
	const ch = "c1"
	for i := 0; i < 9; i++ {
		b.Record(ch, false)
	}
	if !b.Allow(ch) {
		t.Fatal("below min samples must stay closed")
	}
	b.Record(ch, false) // 10 样本，100% 失败 → 打开
	if b.Allow(ch) || !b.Open(ch) {
		t.Fatal("breaker should be open")
	}
	now = now.Add(61 * time.Second)
	if !b.Allow(ch) {
		t.Fatal("half-open must allow one probe")
	}
	if b.Allow(ch) {
		t.Fatal("half-open must allow only one probe")
	}
	b.Record(ch, false) // 探测失败 → 再开 60s
	if b.Allow(ch) {
		t.Fatal("failed probe must reopen")
	}
	now = now.Add(61 * time.Second)
	if !b.Allow(ch) {
		t.Fatal("second probe expected")
	}
	b.Record(ch, true) // 探测成功 → 关闭
	if !b.Allow(ch) || b.Open(ch) {
		t.Fatal("successful probe must close breaker")
	}
	// 窗口滑动：旧失败样本过期后不触发。
	for i := 0; i < 6; i++ {
		b.Record(ch, false)
	}
	now = now.Add(2 * time.Minute)
	for i := 0; i < 10; i++ {
		b.Record(ch, true)
	}
	if !b.Allow(ch) {
		t.Fatal("old failures outside window must not count")
	}
}

func TestBackoffSchedule(t *testing.T) {
	t.Parallel()
	bases := []time.Duration{30 * time.Second, 2 * time.Minute, 8 * time.Minute, 30 * time.Minute, 2 * time.Hour}
	for i, base := range bases {
		for j := 0; j < 20; j++ {
			d := Backoff(int32(i + 1))
			if d < base || d > base+base/5 {
				t.Fatalf("Backoff(%d) = %v, want [%v, %v]", i+1, d, base, base+base/5)
			}
		}
	}
	if d := Backoff(99); d < 2*time.Hour {
		t.Fatalf("Backoff(99) = %v, want capped at last entry", d)
	}
	if d := Backoff(0); d < 30*time.Second {
		t.Fatalf("Backoff(0) = %v", d)
	}
}
