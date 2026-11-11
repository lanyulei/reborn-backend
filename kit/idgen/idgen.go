// Package idgen 提供 UUIDv7 主键生成（时间有序，B-tree 右侧追加；跨服务无需协调）。
package idgen

import (
	"fmt"

	"github.com/google/uuid"
)

// NewID 生成全局唯一 UUIDv7。
func NewID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate uuid v7: %w", err)
	}
	return id.String(), nil
}
