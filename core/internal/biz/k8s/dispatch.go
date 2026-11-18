package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 发件箱状态（k8s_notify_outbox.status）与派发参数。
const (
	OutboxPending int32 = 1
	OutboxDone    int32 = 2
	OutboxDropped int32 = 3
	// OutboxMaxAttempts 派发尝试次数上限，超过即丢弃。
	OutboxMaxAttempts int32 = 10

	dispatchBatch          = 100
	maxConsecutiveFailures = 5
	// maxNotifyKeyLen notify 的 dedup_key / group_key 上限。
	maxNotifyKeyLen = 128
	// maxNotifyRecipients 单个通知事件的显式收件人上限，超出的分片。
	maxNotifyRecipients = 20
)

// EventWriter 通知事件写入（notify 域 NotificationWriter）。
type EventWriter = biznotify.NotificationWriter

// DispatchUsecase 把数据面写入的通知发件箱派发为通知事件：领取、写事件、置为已派发在同一事务。
// 多实例并发安全：行锁 SKIP LOCKED 领取，notify 的 dedup_key 兜底幂等。
type DispatchUsecase struct {
	txm    txrun.Manager
	outbox OutboxRepo
	events EventWriter
	log    *slog.Logger
}

// NewDispatchUsecase new a DispatchUsecase.
func NewDispatchUsecase(txm txrun.Manager, outbox OutboxRepo, events EventWriter, logger *slog.Logger) *DispatchUsecase {
	return &DispatchUsecase{txm: txm, outbox: outbox, events: events, log: logger}
}

var errOutboxEmpty = errors.New("k8s: notify outbox is empty")

// Run 处理一批发件箱行，返回处理（派发或丢弃）的行数。单行失败只记账与日志，不拖慢其余行；
// 返回 error 只代表系统性失败（领取不了、记不了账、连续多行失败）。
func (uc *DispatchUsecase) Run(ctx context.Context) (int, error) {
	var failed []string
	var last error
	done, streak := 0, 0
	for done+len(failed) < dispatchBatch && streak < maxConsecutiveFailures {
		var item *OutboxItem
		err := uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			if item, err = uc.outbox.ClaimTx(ctx, tx, failed); err != nil {
				return err
			}
			if item == nil {
				return errOutboxEmpty
			}
			return uc.process(ctx, tx, item)
		})
		switch {
		case errors.Is(err, errOutboxEmpty):
			return done, nil
		case err == nil:
			done++
			streak = 0
		case item == nil || ctx.Err() != nil:
			return done, err
		default:
			failed = append(failed, item.ID)
			streak++
			last = err
			if err := uc.fail(ctx, item, err); err != nil {
				return done, err
			}
		}
	}
	if streak >= maxConsecutiveFailures {
		return done, fmt.Errorf("k8s notify dispatch: %d consecutive rows failed, last: %w", streak, last)
	}
	return done, nil
}

// fail 事务已回滚后独立记一次失败（不受取消影响：停机中途失败的行也要留下次数）。
func (uc *DispatchUsecase) fail(ctx context.Context, item *OutboxItem, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	reason := cause.Error()
	if len(reason) > 500 {
		reason = reason[:500]
	}
	dropped, err := uc.outbox.Fail(ctx, item.ID, reason, OutboxMaxAttempts)
	if err != nil {
		return fmt.Errorf("dispatch k8s outbox %s: %w (record failure: %w)", item.ID, cause, err)
	}
	if dropped {
		uc.log.ErrorContext(ctx, "k8s notify outbox row dropped after too many failures",
			"outbox_id", item.ID, "event_type", item.EventType, "error", cause)
		return nil
	}
	uc.log.WarnContext(ctx, "k8s notify outbox row failed, will retry", "outbox_id", item.ID, "event_type", item.EventType, "error", cause)
	return nil
}

// notifyKey 把键收进 notify 的长度上限：超长时保留前缀并接上摘要，仍然稳定且不易碰撞。
func notifyKey(key string) string {
	if len(key) <= maxNotifyKeyLen {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return key[:maxNotifyKeyLen-17] + ":" + hex.EncodeToString(sum[:8])
}

// eventTitle 通知标题：内容体自带 title 时用它，否则按事件类型给出缺省标题。
func eventTitle(eventType string, payload json.RawMessage) string {
	var p struct {
		Title       string `json:"title"`
		ClusterName string `json:"cluster_name"`
	}
	_ = json.Unmarshal(payload, &p)
	title := p.Title
	if title == "" {
		switch eventType {
		case kc.NotifyClusterUnreachable:
			title = "集群不可达：" + p.ClusterName
		case kc.NotifyClusterRecovered:
			title = "集群恢复：" + p.ClusterName
		default:
			title = eventType
			if p.ClusterName != "" {
				title += "：" + p.ClusterName
			}
		}
	}
	if r := []rune(title); len(r) > 200 {
		title = string(r[:200])
	}
	return title
}

// process 事务内派发一行：没有指定收件人时写一条不带收件人的事件（由通知路由与订阅决定谁收）；
// 指定了收件人则每 20 人一条。
func (uc *DispatchUsecase) process(ctx context.Context, tx txrun.Tx, item *OutboxItem) error {
	payload := item.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	title := eventTitle(item.EventType, payload)
	chunks := [][]string{nil}
	if len(item.Recipients) > 0 {
		chunks = chunks[:0]
		for i := 0; i < len(item.Recipients); i += maxNotifyRecipients {
			chunks = append(chunks, item.Recipients[i:min(i+maxNotifyRecipients, len(item.Recipients))])
		}
	}
	for i, recipients := range chunks {
		dedup := item.DedupKey
		if i > 0 {
			dedup = fmt.Sprintf("%s:%d", dedup, i)
		}
		if _, err := uc.events.NotifyEvent(ctx, tx, biznotify.Event{
			EventType: item.EventType, Severity: item.Severity, Title: title, Payload: payload,
			DedupKey: notifyKey(dedup), GroupKey: notifyKey(item.GroupKey), Recipients: recipients,
		}); err != nil {
			if errors.Is(err, biznotify.ErrNotificationInvalid) {
				// 内容本身不合法，重试无益：直接丢弃并留下原因。
				return uc.outbox.FinishTx(ctx, tx, item.ID, OutboxDropped, truncateReason(err.Error()))
			}
			return err
		}
	}
	return uc.outbox.FinishTx(ctx, tx, item.ID, OutboxDone, "")
}

func truncateReason(s string) string {
	if len(s) > 500 {
		return s[:500]
	}
	return s
}
