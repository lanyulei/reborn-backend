package biz

import "context"

// TableStats 增长观测数据源（只读）：返回增长观测清单内各表的估算行数（表名 → 估算行数）。
// 清单与查询都在 data 层（internal/data/table_stats.go）；server 的指标在每次抓取时现查。
// 返回值只用内建类型，不为一个指标再造一个跨层结构体。
type TableStats interface {
	TableRowEstimates(ctx context.Context) (map[string]int64, error)
}
