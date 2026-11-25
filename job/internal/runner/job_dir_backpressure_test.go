package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 缓冲写满且引擎不可达时作业被中断：子进程已被杀，但读输出的 goroutine 还阻塞在背压上，wait() 要等引擎
// 恢复才返回。作业目录（含口令文件 secrets.json）不陪绑——子进程一结束就删，与引擎是否可达无关。
func TestJobDirRemovedWhenInterruptedUnderBackpressure(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.reportDown = true
	cfg := testConfig(t)
	j := newTestJob(cfg, eng, nil, gitTask(g, "flood.yml"))
	j.buf = newLogBuffer(4<<10, 64<<10) // 小缓冲：断连期间必然写满，子进程被背压挡住
	// 断言失败时也让作业能收尾（引擎恢复 → 阻塞解除），不留残余的子进程与 goroutine。
	t.Cleanup(func() { eng.set(func(e *fakeEngine) { e.reportDown = false }) })
	done := make(chan struct{})
	go func() { defer close(done); j.run(context.Background()) }()

	secrets := filepath.Join(j.dir, secretsName)
	waitFor(t, "buffer to fill", func() bool { return j.buf.sizeForTest() >= 64<<10 })
	time.Sleep(200 * time.Millisecond) // 让子进程把管道写满、挡在写上
	if _, err := os.Stat(secrets); err != nil {
		t.Fatalf("the job dir must hold the password file while the child runs: %v", err)
	}

	j.interrupt(causeCancel)

	// 子进程结束后作业目录立即删除，而作业仍卡在 wait()（引擎不可达，没有任何确认）。
	waitFor(t, "job dir removed while the job is blocked on backpressure", func() bool {
		entries, err := os.ReadDir(cfg.WorkDir)
		return err == nil && len(entries) == 0
	})
	select {
	case <-done:
		t.Fatal("job returned although the engine never acknowledged anything")
	default:
	}
	// 引擎恢复：阻塞解除，作业按「已取消」正常收尾——提前清理不影响其后的流程。
	eng.set(func(e *fakeEngine) { e.reportDown = false })
	if fin := eng.finish(t); fin.Outcome != jobcontract.OutcomeCanceled || fin.Error != "" {
		t.Fatalf("finish: %+v", fin)
	}
	<-done
	if entries, err := os.ReadDir(cfg.WorkDir); err != nil || len(entries) != 0 {
		t.Fatalf("work dir not empty after the job: %v %v", entries, err)
	}
}

// purgeDirs 只删文件、只删本作业建的目录：不改 madeDir / madeCtl——run() 末尾的 cleanup 仍据此清理脱离了
// 进程组的后代，并清掉标记（幂等）。
func TestPurgeDirsLeavesProcessCleanupToCleanup(t *testing.T) {
	j := newTestJob(testConfig(t), newFakeEngine(), nil, adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"}))
	dirs := []string{j.dir, j.ctlDir}
	// 目录不是它建的：什么都不删。
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	j.purgeDirs()
	for _, dir := range dirs {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("purgeDirs removed a directory the job did not create: %v", err)
		}
	}
	// 自己建的目录：立即删除，标记保持为真。
	j.madeDir, j.madeCtl = true, true
	j.purgeDirs()
	for _, dir := range dirs {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("purgeDirs left %s (%v)", dir, err)
		}
	}
	if !j.madeDir || !j.madeCtl {
		t.Fatal("purgeDirs must leave the process cleanup to cleanup()")
	}
	j.cleanup()
	j.cleanup()
	if j.madeDir || j.madeCtl {
		t.Fatal("cleanup must clear the markers")
	}
}
