package biz

import (
	"context"
	"fmt"
	"time"
)

// 同步相关通知事件类型（notify 路由按模式 cmdb.sync.* 订阅）。
const (
	EventSyncFailed      = "cmdb.sync.failed"
	EventSyncDisappeared = "cmdb.sync.disappeared"
	EventSyncStale       = "cmdb.sync.stale"
	EventSyncChanged     = "cmdb.sync.changed"
)

// 事件严重级（与 notify 域一致：1 info / 2 warning / 3 critical / 4 fatal）。
const (
	SeverityInfo     int32 = 1
	SeverityWarning  int32 = 2
	SeverityCritical int32 = 3
)

// SyncEvent 待发出的通知事件；DedupKey 非空时在 DedupTTL 内只发一次。
type SyncEvent struct {
	Type     string
	Severity int32
	Title    string
	Payload  map[string]any
	DedupKey string
	DedupTTL time.Duration
}

// Notifier 通知事件写入（data 层以 kit/notifycontract 直插 notify_event）；失败只记日志，
// 不影响同步结果。
type Notifier interface {
	Notify(ctx context.Context, event SyncEvent) error
}

// failedEvent 同步失败事件：同一采集源在一个周期内只通知一次。
func failedEvent(src *Source, runID, reason string) SyncEvent {
	return SyncEvent{
		Type: EventSyncFailed, Severity: SeverityCritical,
		Title:    fmt.Sprintf("采集源 %s 同步失败", src.Name),
		Payload:  map[string]any{"source_id": src.ID, "source_name": src.Name, "provider": src.Provider, "run_id": runID, "error": reason},
		DedupKey: EventSyncFailed + ":" + src.ID, DedupTTL: time.Duration(src.IntervalSeconds) * time.Second,
	}
}

// changedEvent 资源新增 / 更新事件（info）。
func changedEvent(src *Source, runID string, c Counts) SyncEvent {
	return SyncEvent{
		Type: EventSyncChanged, Severity: SeverityInfo,
		Title: fmt.Sprintf("采集源 %s：新增 %d 个资源，更新 %d 个", src.Name, c.Created, c.Updated),
		Payload: map[string]any{
			"source_id": src.ID, "source_name": src.Name, "provider": src.Provider, "run_id": runID,
			"created": c.Created, "updated": c.Updated, "discovered": c.Discovered,
		},
	}
}

// disappearedEvent 资源消失 / 退役事件：每次运行有变化即通知。
func disappearedEvent(src *Source, runID string, c Counts) SyncEvent {
	return SyncEvent{
		Type: EventSyncDisappeared, Severity: SeverityWarning,
		Title: fmt.Sprintf("采集源 %s：%d 个资源消失，%d 个已退役", src.Name, c.Disappeared, c.Retired),
		Payload: map[string]any{
			"source_id": src.ID, "source_name": src.Name, "provider": src.Provider, "run_id": runID,
			"disappeared": c.Disappeared, "retired": c.Retired, "discovered": c.Discovered,
		},
	}
}

// staleEvent 陈旧资源事件：每小时最多一次。
func staleEvent(count int) SyncEvent {
	return SyncEvent{
		Type: EventSyncStale, Severity: SeverityWarning,
		Title:    fmt.Sprintf("%d 个同步资源超过周期未更新，已标记为陈旧", count),
		Payload:  map[string]any{"count": count},
		DedupKey: EventSyncStale, DedupTTL: time.Hour,
	}
}
