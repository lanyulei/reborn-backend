// Package idgen 提供跨域共享的 ID 生成工具（biz/service 层中性包，落位规则
// 见 core README「共享工具提取规则」：第二个域需要时才提取，不预先抽象）。
package idgen

import (
	"fmt"

	"github.com/google/uuid"
)

// NewID 生成全局唯一 UUIDv7：时间有序（B-tree 右侧追加、索引局部性好），
// 跨微服务无需协调。
func NewID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate uuid v7: %w", err)
	}
	return id.String(), nil
}
