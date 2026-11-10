package data

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// bumpCfgVersion 同事务递增指定配置表的版本号（upsert 幂等，种子行缺失亦可
// 自举）。notify 域全部配置表（channel/template/…）的写路径共用：管道服务
// 10s 轮询 notify_cfg_version 比对失效刷新内存缓存（设计文档 §5.3）。
func bumpCfgVersion(ctx context.Context, tx pgx.Tx, table string) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO notify_cfg_version (table_name, version, update_time)
		 VALUES ($1, 1, now())
		 ON CONFLICT (table_name)
		 DO UPDATE SET version = notify_cfg_version.version + 1, update_time = now()`, table); err != nil {
		return fmt.Errorf("bump notify_cfg_version(%s): %w", table, err)
	}
	return nil
}
