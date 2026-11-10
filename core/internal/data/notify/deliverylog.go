package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 编译期断言锁住 biz 契约。
var _ biz.DeliveryLogRepo = (*deliveryLogRepo)(nil)

// deliveryLogRepo 投递明细仓储 PostgreSQL 实现：只读查询运行表
// notify_delivery_log（管道写 / core 读，设计 §5.2），无写路径、不 bump
// cfg_version。全列为标量（无 jsonb）；provider_msg_id/error 可空，经二级
// 指针接收 NULL。
type deliveryLogRepo struct {
	data *basedata.Data
}

// NewDeliveryLogRepo new a deliveryLogRepo.
func NewDeliveryLogRepo(data *basedata.Data) *deliveryLogRepo {
	return &deliveryLogRepo{data: data}
}

// deliveryLogSelectColumns 是投递明细查询列序，与 scanDeliveryLog 对应。
const deliveryLogSelectColumns = "id, dispatch_id, channel_id, attempt, ok, " +
	"latency_ms, provider_msg_id, error, create_time"

// scanDeliveryLog 按列序扫描一行投递明细数据并解码为 DO（可空列
// provider_msg_id/error 经二级指针）。
func scanDeliveryLog(scan func(dest ...any) error) (*biz.DeliveryLog, error) {
	var (
		id, dispatchID, channelID string
		providerMsgID, logErr     *string
		attempt, latencyMS        int32
		ok                        bool
		createTime                time.Time
	)
	if err := scan(&id, &dispatchID, &channelID, &attempt, &ok, &latencyMS,
		&providerMsgID, &logErr, &createTime); err != nil {
		return nil, err
	}
	log := &biz.DeliveryLog{
		ID:         id,
		DispatchID: dispatchID,
		ChannelID:  channelID,
		Attempt:    attempt,
		OK:         ok,
		LatencyMS:  latencyMS,
		CreateTime: createTime,
	}
	if providerMsgID != nil {
		log.ProviderMsgID = *providerMsgID
	}
	if logErr != nil {
		log.Error = *logErr
	}
	return log, nil
}

// ListDeliveryLogs implements biz.DeliveryLogRepo：四维过滤 + 分页，两条参数
// 化查询；按落库时间倒序（分区裁剪 + idx_notify_delivery_query）。
func (r *deliveryLogRepo) ListDeliveryLogs(ctx context.Context, filter biz.DeliveryLogFilter, page, pageSize int64) ([]*biz.DeliveryLog, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 5)
	add := func(condition string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(condition, "$"+strconv.Itoa(len(args))))
	}
	if filter.DispatchID != "" {
		add("dispatch_id = %s", filter.DispatchID)
	}
	if filter.ChannelID != "" {
		add("channel_id = %s", filter.ChannelID)
	}
	if filter.OK != nil {
		add("ok = %s", *filter.OK)
	}
	if filter.CreateTimeStart != nil {
		add("create_time >= %s", *filter.CreateTimeStart)
	}
	if filter.CreateTimeEnd != nil {
		add("create_time <= %s", *filter.CreateTimeEnd)
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_delivery_log WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count delivery logs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + deliveryLogSelectColumns + " FROM notify_delivery_log WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list delivery logs: %w", err)
	}
	defer rows.Close()

	logs := make([]*biz.DeliveryLog, 0, pageSize)
	for rows.Next() {
		log, err := scanDeliveryLog(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan delivery log: %w", err)
		}
		logs = append(logs, log)
	}
	return logs, total, rows.Err()
}
