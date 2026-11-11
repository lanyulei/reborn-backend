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
var _ biz.RouteRepo = (*routeRepo)(nil)

// routeRepo 路由仓储 PostgreSQL 实现；每次写操作同事务 bump
// notify_cfg_version('notify_route')（管道 10s 轮询刷新路由缓存）。
// tag_match/recipient_policy 以 jsonb 落库（键值与策略结构即 biz 层 json 标签），
// channel_ids 为 uuid[]（pgx v5 与 []string 双向直传）。
type routeRepo struct {
	data *basedata.Data
}

// NewRouteRepo new a routeRepo.
func NewRouteRepo(data *basedata.Data) *routeRepo {
	return &routeRepo{data: data}
}

// routeSelectColumns 是路由查询列序，与 scanRoute 对应。
const routeSelectColumns = "id, biz_id, event_pattern, severity_min, tag_match, priority, " +
	"channel_ids, recipient_policy, aggregate_window_s, suppress_threshold, suppress_duration_m, " +
	"enabled, create_time, update_time"

// scanRoute 按列序扫描一行路由数据并解码为 DO（jsonb 列经字节中转反序列化）。
func scanRoute(scan func(dest ...any) error) (*biz.Route, error) {
	var (
		id, bizID, eventPattern string
		severityMin, priority   int32
		tagMatch, recipientRaw  []byte
		channelIDs              []string
		recipient               biz.RouteRecipient
		windowS, threshold      int32
		durationM               int32
		enabled                 bool
		createTime, updateTime  time.Time
	)
	if err := scan(&id, &bizID, &eventPattern, &severityMin, &tagMatch, &priority,
		&channelIDs, &recipientRaw, &windowS, &threshold, &durationM,
		&enabled, &createTime, &updateTime); err != nil {
		return nil, err
	}
	if len(recipientRaw) > 0 {
		if err := json.Unmarshal(recipientRaw, &recipient); err != nil {
			return nil, fmt.Errorf("decode recipient_policy: %w", err)
		}
	}
	var tags map[string]string
	if len(tagMatch) > 0 {
		if err := json.Unmarshal(tagMatch, &tags); err != nil {
			return nil, fmt.Errorf("decode tag_match: %w", err)
		}
	}
	return &biz.Route{
		ID:      id,
		BizID:   bizID,
		Enabled: enabled,
		RouteRule: biz.RouteRule{
			EventPattern:      eventPattern,
			SeverityMin:       severityMin,
			TagMatch:          tags,
			Priority:          priority,
			ChannelIDs:        channelIDs,
			Recipient:         recipient,
			AggregateWindowS:  windowS,
			SuppressThreshold: threshold,
			SuppressDurationM: durationM,
		},
		CreateTime: createTime,
		UpdateTime: updateTime,
	}, nil
}

// encodeRouteTags 编码标签匹配条件；nil/空 map 落 NULL（无条件）。编码失败
// 作为错误返回，而非把 error 值当 SQL 实参塞给驱动。
func encodeRouteTags(tags map[string]string) (any, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("encode tag_match: %w", err)
	}
	return raw, nil
}

// encodeRouteRecipient 编码收件策略。
func encodeRouteRecipient(r biz.RouteRecipient) ([]byte, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("encode recipient_policy: %w", err)
	}
	return raw, nil
}

// CreateRoute implements biz.RouteRepo：落库并同事务 bump 版本。
func (r *routeRepo) CreateRoute(ctx context.Context, route *biz.Route) error {
	tags, err := encodeRouteTags(route.TagMatch)
	if err != nil {
		return err
	}
	recipient, err := encodeRouteRecipient(route.Recipient)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_route
			     (id, biz_id, event_pattern, severity_min, tag_match, priority,
			      channel_ids, recipient_policy, aggregate_window_s, suppress_threshold,
			      suppress_duration_m, enabled, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			route.ID, route.BizID, route.EventPattern, route.SeverityMin,
			tags, route.Priority,
			route.ChannelIDs, recipient,
			route.AggregateWindowS, route.SuppressThreshold, route.SuppressDurationM,
			route.Enabled, route.CreateTime, route.UpdateTime); err != nil {
			return fmt.Errorf("insert route: %w", err)
		}
		return bumpCfgVersion(ctx, tx, "notify_route")
	})
}

// UpdateRoute implements biz.RouteRepo：Rule 整组替换规则字段或 Enabled 启停；
// update_time 恒刷新。
func (r *routeRepo) UpdateRoute(ctx context.Context, id string, upd biz.RouteUpdate) error {
	sets := make([]string, 0, 12)
	args := make([]any, 0, 13)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Rule != nil {
		tags, err := encodeRouteTags(upd.Rule.TagMatch)
		if err != nil {
			return err
		}
		recipient, err := encodeRouteRecipient(upd.Rule.Recipient)
		if err != nil {
			return err
		}
		add("event_pattern", upd.Rule.EventPattern)
		add("severity_min", upd.Rule.SeverityMin)
		add("tag_match", tags)
		add("priority", upd.Rule.Priority)
		add("channel_ids", upd.Rule.ChannelIDs)
		add("recipient_policy", recipient)
		add("aggregate_window_s", upd.Rule.AggregateWindowS)
		add("suppress_threshold", upd.Rule.SuppressThreshold)
		add("suppress_duration_m", upd.Rule.SuppressDurationM)
	}
	if upd.Enabled != nil {
		add("enabled", *upd.Enabled)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	query := "UPDATE notify_route SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("update route: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrRouteNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_route")
	})
}

// DeleteRoute implements biz.RouteRepo：软删并同事务 bump 版本。
func (r *routeRepo) DeleteRoute(ctx context.Context, id string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE notify_route SET delete_time = now(), update_time = now()"+
				" WHERE id = $1 AND delete_time IS NULL", id)
		if err != nil {
			return fmt.Errorf("soft delete route: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrRouteNotFound
		}
		return bumpCfgVersion(ctx, tx, "notify_route")
	})
}

// GetRoute implements biz.RouteRepo。
func (r *routeRepo) GetRoute(ctx context.Context, id string) (*biz.Route, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+routeSelectColumns+" FROM notify_route WHERE id = $1 AND delete_time IS NULL", id)
	route, err := scanRoute(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrRouteNotFound
		}
		return nil, fmt.Errorf("get route: %w", err)
	}
	return route, nil
}

// ListRoutes implements biz.RouteRepo：条件过滤 + 分页，两条参数化查询。
func (r *routeRepo) ListRoutes(ctx context.Context, filter biz.RouteFilter, page, pageSize int64) ([]*biz.Route, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 2)
	if filter.BizID != "" {
		args = append(args, filter.BizID)
		where = append(where, "biz_id = $"+strconv.Itoa(len(args)))
	}
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
		"SELECT count(*) FROM notify_route WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count routes: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + routeSelectColumns + " FROM notify_route WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list routes: %w", err)
	}
	defer rows.Close()

	routes := make([]*biz.Route, 0, pageSize)
	for rows.Next() {
		route, err := scanRoute(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan route: %w", err)
		}
		routes = append(routes, route)
	}
	return routes, total, rows.Err()
}

// CountRoutesUsingChannel implements biz.ChannelReferenceChecker：未删除路由中
// channel_ids 包含该渠道的条数（删除渠道前校验）。
func (r *routeRepo) CountRoutesUsingChannel(ctx context.Context, channelID string) (int64, error) {
	var n int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_route WHERE delete_time IS NULL AND $1::uuid = ANY(channel_ids)", channelID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count routes using channel: %w", err)
	}
	return n, nil
}
