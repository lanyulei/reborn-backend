package biz_test

import (
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

// 统计时区的解析改用带缓存的加载（core-monitor-alert 优化项 7）后结果不变：与 time.LoadLocation 得到的时区
// 等效（同一时刻的本地时间与偏移相同）、规范名不变；留空取缺省；"Local" 与不认识的名称仍被拒绝。
func TestStatsLocationMatchesTimeLoadLocation(t *testing.T) {
	at := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	for _, name := range []string{"Asia/Shanghai", "UTC", "America/New_York", "Europe/London", " Asia/Tokyo "} {
		for range 2 { // 第二次命中缓存
			loc, canonical, err := biz.StatsLocation(name)
			if err != nil {
				t.Fatalf("StatsLocation(%q) error = %v", name, err)
			}
			want, err := time.LoadLocation(canonical)
			if err != nil {
				t.Fatal(err)
			}
			_, gotOff := at.In(loc).Zone()
			_, wantOff := at.In(want).Zone()
			if loc.String() != want.String() || gotOff != wantOff || at.In(loc).Format(time.RFC3339) != at.In(want).Format(time.RFC3339) {
				t.Fatalf("StatsLocation(%q) = %v (offset %d), want %v (offset %d)", name, loc, gotOff, want, wantOff)
			}
		}
	}
	if loc, name, err := biz.StatsLocation(""); err != nil || loc == nil || name == "" {
		t.Fatalf("StatsLocation(\"\") = %v, %q, %v", loc, name, err)
	}
	for _, bad := range []string{"Local", "Mars/Olympus", "../etc/passwd"} {
		if _, _, err := biz.StatsLocation(bad); err == nil {
			t.Fatalf("StatsLocation(%q) must fail", bad)
		}
	}
}
