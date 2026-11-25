package biz

import "testing"

// 链接目标按 base 为根计算：靠多级 .. 逃出 base 的链接（解析后仍落在归档根之内）也要丢弃，
// 否则用户解包后它指向目标目录之外的位置。
func TestSafeTarLinkRootedAtBase(t *testing.T) {
	t.Parallel()
	for _, c := range [][2]string{
		{"log/sub/l", "../../x"}, // → 归档根下的 x：在 base 之外
		{"log/sub/deep/l", "../../../x"},
		{"log/a", "../x"}, // 上一级就是 base 之外
		{"log/sub/l", ".././../x"},
		{"log/sub/l", "a/../../../x"},
		{"log/sub/l", "a/../../.."},
		{"log/a", ".."},
	} {
		if SafeTarLink(c[0], c[1], "log") {
			t.Errorf("link %s -> %s escapes the base and must be dropped", c[0], c[1])
		}
	}
	for _, c := range [][2]string{
		{"log/sub/l", "../a"},         // → log/a
		{"log/sub/deep/l", "../../a"}, // → log/a
		{"log/sub/l", "a/../../b"},    // → log/b
		{"log/a", "b"},                // → log/b
		{"log/a", "./b/c"},
	} {
		if !SafeTarLink(c[0], c[1], "log") {
			t.Errorf("link %s -> %s stays inside the base and must be kept", c[0], c[1])
		}
	}
}
