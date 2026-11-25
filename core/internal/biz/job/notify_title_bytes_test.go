package biz

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 通知域按字节限制标题（255）：名称里有 4 字节字符（emoji、扩展区汉字）时，按字符截到 80 个仍会超限，
// 通知被通知域拒绝后静默丢弃（执行人收不到失败通知）。标题须落在 255 字节以内，且不切开字符。
func TestNoticeTitleFitsNotifyByteLimit(t *testing.T) {
	t.Parallel()
	uc := NewNotifyUsecase(nil, nil, nil, "")
	for name, execName := range map[string]string{
		"emoji at the template-name limit": strings.Repeat("🚀", 64),
		"extended hanzi":                   strings.Repeat("𠀀", 64),
		"long quick-run name":              strings.Repeat("🔴", 300),
		"3-byte chars at the limit":        strings.Repeat("中", 64),
	} {
		n := uc.notice(&FinishedExecution{
			Name: execName, Status: jc.StatusFailed, Trigger: jc.TriggerSchedule, FinishTime: time.Unix(1, 0),
		})
		if size := len(n.Title); size > noticeTitleMaxBytes {
			t.Errorf("%s: title is %d bytes, want at most %d: %q", name, size, noticeTitleMaxBytes, n.Title)
		}
		if !utf8.ValidString(n.Title) || utf8.RuneCountInString(n.Title) > 80 {
			t.Errorf("%s: title split a character or exceeded the rune limit: %q", name, n.Title)
		}
		if !strings.HasPrefix(n.Title, "作业执行失败：") {
			t.Errorf("%s: title = %q, want the status prefix kept", name, n.Title)
		}
	}
	// 不超限的标题原样保留。
	n := uc.notice(&FinishedExecution{Name: "🚀生产发布🔴", Status: jc.StatusFailed, Trigger: jc.TriggerSchedule, FinishTime: time.Unix(1, 0)})
	if n.Title != "作业执行失败：🚀生产发布🔴" {
		t.Fatalf("short title changed: %q", n.Title)
	}
}

func TestTruncateBytes(t *testing.T) {
	t.Parallel()
	exact := strings.Repeat("中", 85) // 恰好 255 字节
	if got := truncateBytes(exact, 255); got != exact {
		t.Fatalf("a title within the limit was cut: %q", got)
	}
	over := strings.Repeat("🚀", 64) // 256 字节：去掉边界上的那一个字符
	if got := truncateBytes(over, 255); len(got) != 63*4 || !utf8.ValidString(got) || got != strings.Repeat("🚀", 63) {
		t.Fatalf("cut inside a 4-byte character: %q (%d bytes)", got, len(got))
	}
	if got := truncateBytes("a中b", 2); got != "a" {
		t.Fatalf("cut inside a 3-byte character: %q", got)
	}
	if got := truncateBytes("abc", 2); got != "ab" {
		t.Fatalf("ascii cut = %q", got)
	}
}
