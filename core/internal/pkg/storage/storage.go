// Package storage 定义对象存储的最小抽象：按键写入 / 读取 / 删除一个不可变对象。
// 中性包：不依赖任何业务域与驱动，biz 可 import；驱动实现在 data 层（本地磁盘、
// S3 兼容对象存储；再加驱动只需新增实现并在装配处登记）。
package storage

import (
	"context"
	"errors"
)

// ErrObjectNotFound 对象不存在。
var ErrObjectNotFound = errors.New("storage: object not found")

// ObjectStore 是对象存储驱动接口。键由调用方生成，须为相对路径形式的受控字符串
// （不得含用户输入）；实现须拒绝越出自身根目录的键。
type ObjectStore interface {
	// Put 写入对象（整体原子可见；同键覆盖）。
	Put(ctx context.Context, key string, data []byte) error
	// Get 读取对象全部内容；不存在返回 ErrObjectNotFound。
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete 删除对象；不存在视为成功（幂等，供清理任务重试）。
	Delete(ctx context.Context, key string) error
}
