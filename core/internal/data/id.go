package data

import (
	"fmt"

	"github.com/google/uuid"
)

// NewID 生成业务主键（UUIDv7，时间有序利于 B-tree 聚簇），供 data 层需要
// 自行生成主键的仓储复用（biz 侧 ID 由各域 biz 包自行生成，方向禁止反置）。
func NewID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate uuid v7: %w", err)
	}
	return id.String(), nil
}
