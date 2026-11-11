package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.SubscriptionRepo = (*subscriptionRepo)(nil)

// subscriptionRepo 订阅仓储 PostgreSQL 实现。channel_prefs 落库为设计 §5.1
// 紧凑形态（{"email":true,"dingtalk":"$group:ops"}）；订阅不参与管道配置
// 轮询（§4.1 缓存集不含订阅），写路径不 bump notify_cfg_version。
type subscriptionRepo struct {
	data *basedata.Data
}

// NewSubscriptionRepo new a subscriptionRepo.
func NewSubscriptionRepo(data *basedata.Data) *subscriptionRepo {
	return &subscriptionRepo{data: data}
}

// subscriptionSelectColumns 是订阅查询列序，与 scanSubscription 对应。
const subscriptionSelectColumns = "id, user_id, biz_id, event_pattern, channel_prefs, enabled, create_time, update_time"

// decodeChannelPrefs 从 jsonb 紧凑形态解码偏好（bool→开关、string→目标群）。
func decodeChannelPrefs(raw []byte) (biz.ChannelPrefs, error) {
	var compact map[string]any
	if err := json.Unmarshal(raw, &compact); err != nil {
		return nil, fmt.Errorf("decode channel_prefs: %w", err)
	}
	prefs := make(biz.ChannelPrefs, len(compact))
	for channelType, value := range compact {
		switch v := value.(type) {
		case bool:
			prefs[channelType] = biz.SubscriptionPref{Subscribed: v}
		case string:
			prefs[channelType] = biz.SubscriptionPref{Target: v}
		default:
			return nil, fmt.Errorf("decode channel_prefs: %s pref is %T, want bool or string", channelType, value)
		}
	}
	return prefs, nil
}

// encodeChannelPrefs 编码为 jsonb 紧凑形态：target 非空落字符串、否则落开关。
func encodeChannelPrefs(prefs biz.ChannelPrefs) ([]byte, error) {
	compact := make(map[string]any, len(prefs))
	for channelType, pref := range prefs {
		if pref.Target != "" {
			compact[channelType] = pref.Target
		} else {
			compact[channelType] = pref.Subscribed
		}
	}
	raw, err := json.Marshal(compact)
	if err != nil {
		return nil, fmt.Errorf("encode channel_prefs: %w", err)
	}
	return raw, nil
}

// scanSubscription 按列序扫描一行订阅数据并解码为 DO。
func scanSubscription(scan func(dest ...any) error) (*biz.Subscription, error) {
	var (
		id, userID, bizID, eventPattern string
		prefsRaw                        []byte
		enabled                         bool
		createTime, updateTime          time.Time
	)
	if err := scan(&id, &userID, &bizID, &eventPattern, &prefsRaw,
		&enabled, &createTime, &updateTime); err != nil {
		return nil, err
	}
	prefs, err := decodeChannelPrefs(prefsRaw)
	if err != nil {
		return nil, err
	}
	return &biz.Subscription{
		ID:           id,
		UserID:       userID,
		BizID:        bizID,
		EventPattern: eventPattern,
		ChannelPrefs: prefs,
		Enabled:      enabled,
		CreateTime:   createTime,
		UpdateTime:   updateTime,
	}, nil
}

// UpsertSubscription implements biz.SubscriptionRepo：同自然键（未删除行，
// 部分唯一索引）整组替换偏好与启停，否则新建；单条语句 RETURNING 权威行。
func (r *subscriptionRepo) UpsertSubscription(ctx context.Context, sub *biz.Subscription) (*biz.Subscription, error) {
	prefs, err := encodeChannelPrefs(sub.ChannelPrefs)
	if err != nil {
		return nil, err
	}
	row := r.data.Pool.QueryRow(ctx,
		`INSERT INTO notify_subscription
		     (id, user_id, biz_id, event_pattern, channel_prefs, enabled, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (user_id, biz_id, event_pattern) WHERE delete_time IS NULL
		 DO UPDATE SET channel_prefs = EXCLUDED.channel_prefs,
		               enabled       = EXCLUDED.enabled,
		               update_time   = now()
		 RETURNING `+subscriptionSelectColumns,
		sub.ID, sub.UserID, sub.BizID, sub.EventPattern, prefs,
		sub.Enabled, sub.CreateTime, sub.UpdateTime)
	out, err := scanSubscription(row.Scan)
	if err != nil {
		return nil, fmt.Errorf("upsert subscription: %w", err)
	}
	return out, nil
}

// DeleteSubscription implements biz.SubscriptionRepo：软删。
func (r *subscriptionRepo) DeleteSubscription(ctx context.Context, id string) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE notify_subscription SET delete_time = now(), update_time = now()"+
			" WHERE id = $1 AND delete_time IS NULL", id)
	if err != nil {
		return fmt.Errorf("soft delete subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSubscriptionNotFound
	}
	return nil
}

// GetSubscription implements biz.SubscriptionRepo。
func (r *subscriptionRepo) GetSubscription(ctx context.Context, id string) (*biz.Subscription, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+subscriptionSelectColumns+" FROM notify_subscription WHERE id = $1 AND delete_time IS NULL", id)
	sub, err := scanSubscription(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrSubscriptionNotFound
		}
		return nil, fmt.Errorf("get subscription: %w", err)
	}
	return sub, nil
}

// ListSubscriptions implements biz.SubscriptionRepo：按目标用户过滤 + 分页，
// 两条参数化查询。
func (r *subscriptionRepo) ListSubscriptions(ctx context.Context, filter biz.SubscriptionFilter, page, pageSize int64) ([]*biz.Subscription, int64, error) {
	where := []string{"delete_time IS NULL", "user_id = $1"}
	args := make([]any, 0, 4)
	args = append(args, filter.UserID)
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		where = append(where, "event_pattern ILIKE $"+strconv.Itoa(len(args)))
	}
	if filter.Enabled != nil {
		args = append(args, *filter.Enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_subscription WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count subscriptions: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + subscriptionSelectColumns + " FROM notify_subscription WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()

	subs := make([]*biz.Subscription, 0, pageSize)
	for rows.Next() {
		sub, err := scanSubscription(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan subscription: %w", err)
		}
		subs = append(subs, sub)
	}
	return subs, total, rows.Err()
}
