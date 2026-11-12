package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

var _ biz.ChannelHealthRepo = (*channelHealthRepo)(nil)

// channelHealthRepo 渠道健康只读仓储（notify_channel_health ⋈ notify_channel；
// 迁移 0031 授予 notify_core_role SELECT）。
type channelHealthRepo struct {
	data *basedata.Data
}

// NewChannelHealthRepo new a channelHealthRepo.
func NewChannelHealthRepo(data *basedata.Data) *channelHealthRepo {
	return &channelHealthRepo{data: data}
}

// ListChannelHealth implements biz.ChannelHealthRepo：打开优先（open_until 倒序，
// NULL 最后）、最近更新倒序；配置表规模不分页。
func (r *channelHealthRepo) ListChannelHealth(ctx context.Context, bizID string) ([]*biz.ChannelHealth, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT h.channel_id::text, c.name, c.type, c.biz_id, h.samples, h.failures, h.open_until, h.probing, h.update_time
		 FROM notify_channel_health h
		 JOIN notify_channel c ON c.id = h.channel_id AND c.delete_time IS NULL
		 WHERE $1 = '' OR c.biz_id = $1
		 ORDER BY h.open_until DESC NULLS LAST, h.update_time DESC`, bizID)
	if err != nil {
		return nil, fmt.Errorf("list channel health: %w", err)
	}
	defer rows.Close()
	var out []*biz.ChannelHealth
	for rows.Next() {
		h := &biz.ChannelHealth{}
		if err := rows.Scan(&h.ChannelID, &h.ChannelName, &h.ChannelType, &h.BizID, &h.Samples, &h.Failures, &h.OpenUntil, &h.Probing, &h.UpdateTime); err != nil {
			return nil, fmt.Errorf("scan channel health: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
