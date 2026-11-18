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

// IsUUID 报告 s 是否为 UUID 的规范文本形式（8-4-4-4-12、小写十六进制）。来自请求的 ID 在进入
// SQL（转 uuid 列）或参与路径拼接之前用它把关；本项目生成与存储的 ID 都是小写。
func IsUUID(s string) bool { return isUUID(s, false) }

// IsUUIDFold 同 IsUUID，但十六进制字母不区分大小写（外部系统给的 ID 可能是大写；调用方
// 通常随后转成小写再用）。
func IsUUIDFold(s string) bool { return isUUID(s, true) }

func isUUID(s string, fold bool) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return false
			}
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		case fold && c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}
