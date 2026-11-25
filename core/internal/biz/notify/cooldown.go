package biz

import (
	"fmt"
	"sync"
	"time"
)

// cooldown 按键的最小间隔（渠道连通性测试按渠道、事件重发按源事件）：两者都会把一条事件真实投递到
// 收件人，不节流即成免费发信口。进程内状态：多副本各自计数（人工按钮场景足够）；渠道表与事件表
// 都没有可用来记「上次测试 / 重发时间」的列，做成库级节流需要加列，暂不引入。
type cooldown struct {
	d    time.Duration
	now  func() time.Time
	mu   sync.Mutex
	last map[string]time.Time
	// aliases 派生键 → 所属的冷却键（重发出的事件 → 源事件）：对派生键的操作与源共享同一条冷却，
	// 否则「重发刚重发出的事件」可以链式绕过冷却连续刷信。随源键的冷却期一起过期。
	aliases map[string]string
}

func newCooldown(d time.Duration) *cooldown {
	return &cooldown{d: d, now: time.Now, last: map[string]time.Time{}, aliases: map[string]string{}}
}

// keyOf 返回 key 所属的冷却键（没有被别名化就是它自己）。
func (c *cooldown) keyOf(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rootLocked(key)
}

// alias 把派生键登记到 key 所属的冷却键下。d ≤ 0 时不登记。
func (c *cooldown) alias(derived, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.d <= 0 {
		return
	}
	c.aliases[derived] = c.rootLocked(key)
}

// rootLocked 沿别名找到 key 所属的冷却键（别名总是指向根，链长为 1）。调用方须持锁。
func (c *cooldown) rootLocked(key string) string {
	if root, ok := c.aliases[key]; ok {
		return root
	}
	return key
}

// reserve 间隔不足返回 base 包装的错误（带剩余等待时间），否则占用本次名额；顺带清掉已过冷却期的旧
// 条目（map 不随键数无界增长）。d ≤ 0 关闭节流。
func (c *cooldown) reserve(key string, base error) error {
	if c.d <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key = c.rootLocked(key)
	now := c.now()
	if last, ok := c.last[key]; ok && now.Sub(last) < c.d {
		return fmt.Errorf("%w: retry after %s", base, (c.d - now.Sub(last)).Round(time.Second))
	}
	for id, at := range c.last {
		if now.Sub(at) >= c.d {
			delete(c.last, id)
		}
	}
	for derived, root := range c.aliases {
		if _, live := c.last[root]; !live {
			delete(c.aliases, derived)
		}
	}
	c.last[key] = now
	return nil
}
