// Package paging 提供分页参数规整（各模块、各域统一口径）。
package paging

import "math"

const (
	// DefaultPageSize 未指定或非法时的默认页大小。
	DefaultPageSize int64 = 20
	// MaxPageSize 页大小上限（与 proto 的 page_size lte 对齐）。
	MaxPageSize int64 = 100
	// MaxPage 页码上限：保证 (page-1)*pageSize 不溢出 int64（溢出为负会被 PG 以
	// OFFSET must not be negative 拒绝成 500）；超出按上限取空页。
	MaxPage = math.MaxInt64 / MaxPageSize
)

// Normalize 规整分页参数：page 夹在 [1, MaxPage]，pageSize 夹在 [1, MaxPageSize]。
func Normalize(page, pageSize int64) (int64, int64) {
	switch {
	case page < 1:
		page = 1
	case page > MaxPage:
		page = MaxPage
	}
	switch {
	case pageSize < 1:
		pageSize = DefaultPageSize
	case pageSize > MaxPageSize:
		pageSize = MaxPageSize
	}
	return page, pageSize
}
