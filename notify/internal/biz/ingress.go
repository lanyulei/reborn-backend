package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/pkg/idgen"
)

// IngressResult 单条落库结果。
type IngressResult struct {
	EventID      string
	Deduplicated bool
}

// IngressRepo 接入写仓储：一个事务内先按生产者扣减配额（秒窗/日窗条件 upsert，
// 0 行 = 超限 → ErrQuotaExceeded 整批回滚），再对每条事件执行 dedup 门闩（单语句
// upsert RETURNING，评审 #15）+ INSERT notify_event；全部成功才提交。
type IngressRepo interface {
	SaveEvents(ctx context.Context, producer *Producer, events []*Event, dedupTTL time.Duration) ([]IngressResult, error)
}

// 事件约束（与 core NotificationWriter 同口径）。
const (
	maxEventPayloadBytes = 32 * 1024
	maxEventRecipients   = 20
	maxEventTitleLen     = 255
	maxEventKeyLen       = 128
)

// IngressUsecase 接入用例：scope 校验 → biz 校验补全 → 先落库后应答。
type IngressUsecase struct {
	repo     IngressRepo
	dedupTTL time.Duration
}

// NewIngressUsecase new an IngressUsecase（缺省 dedup 24h）。
func NewIngressUsecase(repo IngressRepo) *IngressUsecase {
	return &IngressUsecase{repo: repo, dedupTTL: 24 * time.Hour}
}

// WithDedupTTL 覆盖门闩有效期。
func (uc *IngressUsecase) WithDedupTTL(d time.Duration) *IngressUsecase {
	if d > 0 {
		uc.dedupTTL = d
	}
	return uc
}

// Accept 校验并落库一批事件（≤500，单事务）；任一校验失败整批拒绝。
func (uc *IngressUsecase) Accept(ctx context.Context, producer *Producer, events []*Event) ([]IngressResult, error) {
	now := time.Now().UTC()
	for i, ev := range events {
		if err := producer.Authorize(ev.BizID, ev.EventType); err != nil {
			return nil, err
		}
		if err := validateEvent(ev); err != nil {
			return nil, fmt.Errorf("events[%d]: %w", i, err)
		}
		id, err := idgen.NewID()
		if err != nil {
			return nil, err
		}
		ev.ID = id
		ev.Source = EventSourceAPI
		ev.Status = EventStatusReceived
		ev.PublishState = PublishPending
		ev.PublishAttempts = 0
		ev.NextPublishTime = now
		ev.CreateTime = now
	}
	return uc.repo.SaveEvents(ctx, producer, events, uc.dedupTTL)
}

func validateEvent(ev *Event) error {
	switch {
	case ev.Severity < 1 || ev.Severity > 4:
		return fmt.Errorf("%w: severity out of range", ErrEventInvalid)
	case ev.Title == "" || len(ev.Title) > maxEventTitleLen:
		return fmt.Errorf("%w: title length", ErrEventInvalid)
	case len(ev.Payload) > maxEventPayloadBytes:
		return fmt.Errorf("%w: payload exceeds 32KB", ErrEventInvalid)
	case len(ev.Payload) > 0 && !json.Valid(ev.Payload):
		return fmt.Errorf("%w: payload is not valid JSON", ErrEventInvalid)
	case len(ev.DedupKey) > maxEventKeyLen || len(ev.GroupKey) > maxEventKeyLen:
		return fmt.Errorf("%w: dedup/group key too long", ErrEventInvalid)
	case len(ev.Recipients) > maxEventRecipients:
		return fmt.Errorf("%w: too many recipients", ErrEventInvalid)
	}
	return nil
}
