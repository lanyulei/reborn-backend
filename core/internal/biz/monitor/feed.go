package biz

import (
	"context"
	"time"
)

// 告警事件的对外只读视图（故障管理域经组合根消费）：按全局序号 tail 告警时间线、批量取告警。
// 与引擎共享的是表与 kit/monitorcontract；本文件只增接口，不改既有仓储契约。

// AlertFeedItem 告警时间线上的一条触发 / 恢复 / 解除屏蔽 / 解除抑制 / 关闭事件及其告警当前快照。
type AlertFeedItem struct {
	Seq   int64
	Type  string
	Time  time.Time
	Alert *Alert
}

// AlertFeedReader 按 monitor_alert_event.seq 顺序读取告警事件（迁移 0080 为该表加的全局序号）。
type AlertFeedReader interface {
	// ListAfter seq 之后的触发 / 恢复 / 解除屏蔽 / 解除抑制 / 关闭事件（按 seq 升序），至多 limit 条。
	ListAfter(ctx context.Context, seq int64, limit int) ([]*AlertFeedItem, error)
	// ListBetween seq 在 (afterSeq, untilSeq] 内的事件（类型同 ListAfter，按 seq 升序），至多 limit 条；
	// 消费方回看「取号早、提交晚」的事务用，晚提交的恢复 / 关闭同样要补处理。
	ListBetween(ctx context.Context, afterSeq, untilSeq int64, limit int) ([]*AlertFeedItem, error)
}

// AlertLookup 批量取告警事件（不存在的不在结果中）。
type AlertLookup interface {
	ListByIDs(ctx context.Context, ids []string) ([]*Alert, error)
	// CountFiring ids 里当前仍在告警中的条数：status = 1 且未被屏蔽、未被抑制——与故障关联器判定
	// 「告警是否算在触发」的口径一致（被屏蔽 / 抑制的告警不参与归并，也不算故障上的活跃告警）。
	CountFiring(ctx context.Context, ids []string) (int, error)
}
