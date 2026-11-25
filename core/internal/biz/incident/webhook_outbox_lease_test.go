package biz

import (
	"testing"
	"time"
)

// 出站箱一批是串行投递的：租约必须覆盖「批大小 × 单次投递上限」。此前固定 60 秒，一批最坏要 200 秒，
// 多副本下还没轮到的行会在租约到期后被别的副本重新领取并重复投递。
func TestOutboxLeaseCoversWholeBatch(t *testing.T) {
	t.Parallel()
	worst := time.Duration(outboxBatch) * WebhookSendMax
	if outboxLease <= worst {
		t.Fatalf("outboxLease = %v, must exceed a whole batch of worst-case deliveries (%v)", outboxLease, worst)
	}
}
