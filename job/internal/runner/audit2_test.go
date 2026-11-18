package runner

import (
	"strings"
	"testing"
)

// 一个工作目录只给一个节点进程用：第二个实例启动即被拒绝，不会把第一个的在途作业当成残留清掉。
func TestWorkDirIsExclusive(t *testing.T) {
	cfg := testConfig(t)
	first := newRunner(cfg, newFakeEngine(), testLogger(&syncBuffer{}))
	if err := first.prepareWorkDir(); err != nil {
		t.Fatal(err)
	}
	second := newRunner(cfg, newFakeEngine(), testLogger(&syncBuffer{}))
	if err := second.prepareWorkDir(); err == nil || !strings.Contains(err.Error(), "in use by another runner") {
		t.Fatalf("second runner on the same work dir: %v", err)
	}
}
