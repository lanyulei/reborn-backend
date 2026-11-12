package service

import (
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// 运行表列表时间窗口：缺省回看 7 天、跨度上限 90 天。运行表按月分区，无界查询
// 的 count(*) + 排序会扫全部分区；控制台排障以"最近"为主，更早的数据分段查。
const (
	defaultListWindow = 7 * 24 * time.Hour
	maxListWindow     = 90 * 24 * time.Hour
)

// listTimeRange 规整时间过滤：end 缺省开放（按 now 计跨度）；start 缺省 = 有效
// end − 7 天；跨度 > 90 天返回 ErrTimeRangeInvalid。
func listTimeRange(start, end *timestamppb.Timestamp, now time.Time) (*time.Time, *time.Time, error) {
	var endT *time.Time
	effectiveEnd := now
	if end != nil {
		t := end.AsTime()
		endT = &t
		effectiveEnd = t
	}
	var startT time.Time
	if start != nil {
		startT = start.AsTime()
	} else {
		startT = effectiveEnd.Add(-defaultListWindow)
	}
	if span := effectiveEnd.Sub(startT); span > maxListWindow {
		return nil, nil, fmt.Errorf("%w: %s", biz.ErrTimeRangeInvalid, span.Round(time.Hour))
	}
	return &startT, endT, nil
}
