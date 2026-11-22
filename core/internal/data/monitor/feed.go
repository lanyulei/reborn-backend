package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
)

var (
	_ biz.AlertFeedReader = (*alertFeedRepo)(nil)
	_ biz.AlertLookup     = (*alertFeedRepo)(nil)
)

// FeedProviderSet 告警事件对外只读视图（故障管理域消费）的 data providers。
var FeedProviderSet = wire.NewSet(
	NewAlertFeedRepo,
	wire.Bind(new(biz.AlertFeedReader), new(*alertFeedRepo)),
	wire.Bind(new(biz.AlertLookup), new(*alertFeedRepo)),
)

// alertFeedRepo 按 seq tail 告警时间线（idx_monitor_alert_event_seq），连同告警当前状态一起返回。
type alertFeedRepo struct {
	data *basedata.Data
}

// NewAlertFeedRepo new an alertFeedRepo.
func NewAlertFeedRepo(data *basedata.Data) *alertFeedRepo { return &alertFeedRepo{data: data} }

// feedTypes 消费方关心的事件类型：触发 / 恢复 / 解除屏蔽 / 解除抑制 / 关闭。
var feedTypes = []string{mc.AlertEventFire, mc.AlertEventResolve, mc.AlertEventUnmute, mc.AlertEventUninhibit, mc.AlertEventClose}

// ListAfter implements biz.AlertFeedReader.
func (r *alertFeedRepo) ListAfter(ctx context.Context, seq int64, limit int) ([]*biz.AlertFeedItem, error) {
	return r.feed(ctx, "WHERE e.seq > $1 AND e.type = ANY($2::text[]) ORDER BY e.seq LIMIT $3", seq, feedTypes, limit)
}

// ListBetween implements biz.AlertFeedReader.
func (r *alertFeedRepo) ListBetween(ctx context.Context, afterSeq, untilSeq int64, limit int) ([]*biz.AlertFeedItem, error) {
	if untilSeq <= afterSeq {
		return []*biz.AlertFeedItem{}, nil
	}
	return r.feed(ctx, "WHERE e.seq > $1 AND e.seq <= $2 AND e.type = ANY($3::text[]) ORDER BY e.seq LIMIT $4", afterSeq, untilSeq, feedTypes, limit)
}

func (r *alertFeedRepo) feed(ctx context.Context, tail string, args ...any) ([]*biz.AlertFeedItem, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT e.seq, e.type, e.create_time, "+alertColumns+" FROM monitor_alert_event e JOIN monitor_alert a ON a.id = e.alert_id "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("list monitor alert feed: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.AlertFeedItem, 0, 64)
	for rows.Next() {
		it := &biz.AlertFeedItem{}
		var al biz.Alert
		var labels, annotations []byte
		if err := rows.Scan(&it.Seq, &it.Type, &it.Time,
			&al.ID, &al.Fingerprint, &al.RuleID, &al.RuleName, &al.GroupID, &al.DatasourceID, &al.DatasourceName,
			&al.Severity, &al.Status, &labels, &annotations, &al.Expr, &al.Value, &al.RecoveryValue, &al.FirstFireTime, &al.LastFireTime,
			&al.ResolveTime, &al.Muted, &al.MuteID, &al.NotifyCount, &al.LastNotifyTime, &al.GroupKey,
			&al.AckUser, &al.AckTime, &al.ClosedBy, &al.CloseReason, &al.Inhibited, &al.InhibitKind,
			&al.InhibitID, &al.InhibitAlertID, &al.CIID, &al.EscalationLevel,
			&al.TicketID, &al.WorkflowInstanceID, &al.AISummary); err != nil {
			return nil, fmt.Errorf("scan monitor alert feed: %w", err)
		}
		if err := decodeJSON(labels, &al.Labels, "alert labels"); err != nil {
			return nil, err
		}
		if err := decodeJSON(annotations, &al.Annotations, "alert annotations"); err != nil {
			return nil, err
		}
		it.Alert = &al
		out = append(out, it)
	}
	return out, rows.Err()
}

// ListByIDs implements biz.AlertLookup.
func (r *alertFeedRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Alert, error) {
	if len(ids) == 0 {
		return []*biz.Alert{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+alertColumns+" FROM monitor_alert a WHERE a.id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor alerts by ids: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Alert, 0, len(ids))
	for rows.Next() {
		al, err := scanAlert(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan monitor alert: %w", err)
		}
		out = append(out, al)
	}
	return out, rows.Err()
}

// CountFiring implements biz.AlertLookup：告警中且未被屏蔽 / 抑制（与故障关联器的「触发」口径一致）。
func (r *alertFeedRepo) CountFiring(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_alert WHERE id = ANY($1::uuid[]) AND status = 1 AND NOT muted AND NOT inhibited", ids).Scan(&n); err != nil {
		return 0, fmt.Errorf("count firing monitor alerts: %w", err)
	}
	return n, nil
}
