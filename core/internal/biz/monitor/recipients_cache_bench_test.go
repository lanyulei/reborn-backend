package biz

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// 基准（AGENTS §6）：收件人解析的目录往返——HEAD 的「每条订阅各查一次目录」对当前的「一次预取 +
// 会话内缓存」。不连库：目录桩每次调用固定耗时（模拟一次数据库往返），上报每个操作的目录调用次数。
// 只比较，不断言性能阈值。

// benchDirDelay 目录桩每次调用的固定耗时（一次数据库往返的量级）。
const benchDirDelay = 100 * time.Microsecond

// stallDir 目录桩：沿用 countingDir 的目录口径（转小写、去重、丢掉非法 ID、按顺序筛出可用用户），
// 每次 ActiveUserIDs 自旋等待 delay。用自旋而不是 Sleep：100µs 级别的睡眠实际时长受调度器影响，
// 噪声会盖过被测的差异。
type stallDir struct {
	*countingDir
	delay time.Duration
}

func (d *stallDir) ActiveUserIDs(ctx context.Context, ids []string) ([]string, error) {
	start := time.Now()
	for time.Since(start) < d.delay {
	}
	return d.countingDir.ActiveUserIDs(ctx, ids)
}

// subscriptionLegacy HEAD:core/internal/biz/monitor/recipients.go 的 Subscription 与它调用的 active
// （逐字拼接）：每条订阅把候选用户去重后整个交给目录，没有会话内缓存、没有预取。
func subscriptionLegacy(ctx context.Context, sess *ResolveSession, sub *Subscription) ([]string, error) {
	r := sub.Spec.Receivers
	users := slices.Clone(r.UserIDs)
	if r.Self {
		users = append(users, sub.OwnerID)
	}
	duty, err := sess.usersOnDuty(ctx, r.ScheduleIDs, nil)
	if err != nil {
		return nil, err
	}
	want := uniq(append(users, duty...))
	if len(want) == 0 {
		return nil, nil
	}
	return sess.r.dir.ActiveUserIDs(ctx, want)
}

// benchSubscriptions 一行发件箱的候选订阅：与等价性测试同一形态（所有者本人、与相邻订阅重叠的
// 指定用户、重复的 ID、每 5 条一条带值班表），ID 全部是库里读出来的规范写法。
func benchSubscriptions(n int) []*Subscription {
	subs := make([]*Subscription, 0, n)
	for i := 1; i <= n; i++ {
		var schedules []string
		if i%5 == 0 {
			schedules = []string{"sch-1"}
		}
		subs = append(subs, cacheSub(cacheUser(i), i%3 != 0, schedules, cacheUser(i+1), cacheUser(i+1), cacheUser(n+1-i)))
	}
	return subs
}

// BenchmarkRecipientsSubscriptionCandidates 一个 op = 一行发件箱把 n 条候选订阅的接收方解析完
// （dispatch.go：先 Prefetch，再逐条 Subscription）。legacy = HEAD 的逐条查询；current = 预取 + 缓存。
// n 的上界取 maxSubscriptionCandidates（一次匹配至多考察这么多条订阅）。
func BenchmarkRecipientsSubscriptionCandidates(b *testing.B) {
	ctx := context.Background()
	duty := fixedDuty{"sch-1": {cacheUser(3), cacheUser(1 << 20)}}
	for _, n := range []int{100, maxSubscriptionCandidates} {
		subs := benchSubscriptions(n)
		active := map[string]bool{}
		for i := 1; i <= n+1; i++ {
			active[cacheUser(i)] = i%4 != 0 // 每 4 个里有 1 个已停用
		}
		active[cacheUser(1<<20)] = true

		// 开始计时之前核对：两种做法对每条订阅解析出的接收方相同。
		baseDir := &countingDir{active: active}
		want := make([][]string, len(subs))
		base := NewRecipientResolver(baseDir, nil, duty, nil, nil).Session(time.Now())
		for i, sub := range subs {
			users, err := subscriptionLegacy(ctx, base, sub)
			if err != nil {
				b.Fatal(err)
			}
			want[i] = users
		}
		curDir := &countingDir{active: active}
		cur := NewRecipientResolver(curDir, nil, duty, nil, nil).Session(time.Now())
		if err := cur.Prefetch(ctx, subs); err != nil {
			b.Fatal(err)
		}
		for i, sub := range subs {
			got, err := cur.Subscription(ctx, sub)
			if err != nil {
				b.Fatal(err)
			}
			if !slices.Equal(got, want[i]) {
				b.Fatalf("subscription %d: current = %v, legacy = %v", i, got, want[i])
			}
		}

		dir := &stallDir{countingDir: &countingDir{active: active}, delay: benchDirDelay}
		resolver := NewRecipientResolver(dir, nil, duty, nil, nil)
		b.Run(fmt.Sprintf("subscriptions=%d", n), func(b *testing.B) {
			for _, v := range []struct {
				name string
				run  func() error
			}{
				{"legacy=per-subscription", func() error {
					sess := resolver.Session(time.Now())
					for _, sub := range subs {
						if _, err := subscriptionLegacy(ctx, sess, sub); err != nil {
							return err
						}
					}
					return nil
				}},
				{"current=prefetch+cache", func() error {
					sess := resolver.Session(time.Now())
					if err := sess.Prefetch(ctx, subs); err != nil {
						return err
					}
					for _, sub := range subs {
						if _, err := sess.Subscription(ctx, sub); err != nil {
							return err
						}
					}
					return nil
				}},
			} {
				b.Run(v.name, func(b *testing.B) {
					dir.calls = nil
					b.ResetTimer()
					for range b.N {
						if err := v.run(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(len(dir.calls))/float64(b.N), "dir-calls/op")
				})
			}
		})
	}
}
