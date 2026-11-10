package paging

import "testing"

// TestNormalize 分页规整：页码下限 1、上限 MaxPage（防 OFFSET 溢出为负），
// 页大小缺省 20、上限 100。
func TestNormalize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		page, size, wantPage, wantSize int64
	}{
		{0, 0, 1, DefaultPageSize},
		{-5, 500, 1, MaxPageSize},
		{3, 10, 3, 10},
		{1 << 62, 100, MaxPage, 100},
	}
	for _, tc := range cases {
		page, size := Normalize(tc.page, tc.size)
		if page != tc.wantPage || size != tc.wantSize {
			t.Errorf("Normalize(%d, %d) = (%d, %d), want (%d, %d)", tc.page, tc.size, page, size, tc.wantPage, tc.wantSize)
		}
		if (page-1)*size < 0 {
			t.Errorf("Normalize(%d, %d) offset overflowed negative", tc.page, tc.size)
		}
	}
}
