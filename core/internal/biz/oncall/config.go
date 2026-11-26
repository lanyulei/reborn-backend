package biz

import (
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// 值班域运行参数缺省值。
const (
	// DefaultDeletedShiftRetention 已删除值班表的班次的缺省保留期。
	DefaultDeletedShiftRetention = 30 * 24 * time.Hour
)

// Config 值班域运行参数（由 conf.Oncall 规整）。
type Config struct {
	// DeletedShiftRetention 已删除值班表的班次保留期；0 = 不清理。
	DeletedShiftRetention time.Duration
}

// NewConfig 从配置规整；nil 取缺省。保留期「未设置取缺省，显式设置按配置值」：0（及负值）= 不清理。
func NewConfig(c *conf.Oncall) Config {
	cfg := Config{DeletedShiftRetention: DefaultDeletedShiftRetention}
	if c == nil {
		return cfg
	}
	if d := c.GetDeletedScheduleShiftRetention(); d != nil {
		cfg.DeletedShiftRetention = max(d.AsDuration(), 0)
	}
	return cfg
}
