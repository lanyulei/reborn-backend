package paging

import "testing"

func TestNormalize(t *testing.T) {
	t.Parallel()
	cases := []struct{ page, size, wantPage, wantSize int64 }{
		{0, 0, 1, DefaultPageSize}, {-5, 500, 1, MaxPageSize}, {3, 10, 3, 10}, {1 << 62, 100, MaxPage, 100},
	}
	for _, tc := range cases {
		page, size := Normalize(tc.page, tc.size)
		if page != tc.wantPage || size != tc.wantSize || (page-1)*size < 0 {
			t.Errorf("Normalize(%d, %d) = (%d, %d), want (%d, %d)", tc.page, tc.size, page, size, tc.wantPage, tc.wantSize)
		}
	}
}
