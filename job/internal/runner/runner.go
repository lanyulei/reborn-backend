package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// claimBackoffMax 领取失败后的退避上限。
	claimBackoffMax = 30 * time.Second
	// finalizeSlack 节点停止、等待期已过并取消在途作业之后，再等它们收尾（终止进程、交日志、报结束）
	// 的时长 = cancel_grace + finalizeSlack；之后硬停止。
	finalizeSlack = 30 * time.Second
)

// Runner 执行节点：心跳循环 + 领取循环 + 每个作业一组 goroutine。实现 kratos transport.Server。
type Runner struct {
	cfg *Config
	eng engine
	log *slog.Logger

	// enabled 最近一次心跳得到的启用状态；从未成功心跳时为 false（不领取）。
	enabled atomic.Bool

	// mu 保护 jobs、cancel、done。不变式：jobs 里是已领取、尚未收尾完的作业。
	mu     sync.Mutex
	jobs   map[string]*job
	cancel context.CancelFunc // 停止心跳与领取循环
	hard   context.CancelFunc // 硬停止：放弃在途作业的一切等待
	done   chan struct{}      // Start 返回后关闭
	wg     sync.WaitGroup     // 在途作业
	lock   *os.File           // 工作目录的独占锁
}

// New 构造执行节点（未启用 runner 角色时不会被调用，见 server.NewRunner）。
func New(cfg *Config, logger *slog.Logger) (*Runner, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return newRunner(cfg, c, logger), nil
}

func newRunner(cfg *Config, eng engine, logger *slog.Logger) *Runner {
	return &Runner{cfg: cfg, eng: eng, log: logger, jobs: map[string]*job{}}
}

// running 在途作业数。
func (r *Runner) running() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.jobs)
}

// Start implements transport.Server：阻塞运行直至 Stop。
func (r *Runner) Start(ctx context.Context) error {
	if err := r.prepareWorkDir(); err != nil {
		return err
	}
	// 循环与作业的 ctx 都脱离 kratos 的启动 ctx：停止由 Stop 编排（先停领取，再等作业）。
	base := context.WithoutCancel(ctx)
	loopCtx, cancel := context.WithCancel(base)
	hardCtx, hard := context.WithCancel(base)
	done := make(chan struct{})
	r.mu.Lock()
	r.cancel, r.hard, r.done = cancel, hard, done
	r.mu.Unlock()
	defer close(done)
	defer hard()

	r.log.Info("runner started", "engine", r.cfg.EngineURL, "capacity", r.cfg.Capacity, "work_dir", r.cfg.WorkDir, "version", r.cfg.Version)
	hbDone := make(chan struct{})
	// 心跳 goroutine：hardCtx 取消即结束（停止领取之后仍要心跳，引擎才看得到在途作业数）。
	go func() {
		defer close(hbDone)
		r.heartbeatLoop(hardCtx)
	}()
	r.claimLoop(loopCtx, hardCtx)
	// 领取已停：等在途作业收尾（Stop 负责等待期与取消），再停心跳。
	r.wg.Wait()
	hard()
	<-hbDone
	r.log.Info("runner stopped")
	return nil
}

// Stop implements transport.Server：停止领取 → 等在途作业自行结束至多 drain_timeout → 取消它们
// （以「已取消」结束并注明节点已停止）→ 再等收尾 → 硬停止。ctx 到期则立即硬停止。
func (r *Runner) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel, hard, done := r.cancel, r.hard, r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	if n := r.running(); n > 0 {
		r.log.Info("runner stopping: waiting for running jobs", "jobs", n, "drain_timeout", r.cfg.DrainTimeout)
	}
	if waitDone(ctx, done, r.cfg.DrainTimeout) {
		return nil
	}
	r.mu.Lock()
	for _, j := range r.jobs {
		j.interrupt(causeShutdown)
	}
	n := len(r.jobs)
	r.mu.Unlock()
	r.log.Warn("runner stopping: drain timeout reached, canceling running jobs", "jobs", n)
	if waitDone(ctx, done, r.cfg.CancelGrace+finalizeSlack) {
		return nil
	}
	r.log.Error("runner stopping: jobs did not finalize in time, forcing stop")
	hard()
	<-done
	return nil
}

// Wait 等执行节点完全停止（Start 返回）或 ctx 到期；尚未启动时立即返回。同一进程里同时运行引擎时，
// 引擎的协议监听用它排在执行节点之后关闭：在途作业收尾时还要向它上报。
func (r *Runner) Wait(ctx context.Context) {
	r.mu.Lock()
	done := r.done
	r.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// waitDone 等 done 关闭，至多 d；ctx 到期同样返回 false。
func waitDone(ctx context.Context, done <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
	case <-ctx.Done():
	}
	return false
}

// prepareWorkDir 建工作目录并清掉上次进程崩溃（或被强杀）留下的东西：先杀掉残留作业目录里登记的
// 进程组（ansible-playbook、ssh-agent 及其后代——它们可能仍在目标主机上执行），再删目录（其中可能有
// 口令文件）。工作目录由一个节点进程独占：不要让两个节点共用。
func (r *Runner) prepareWorkDir() error {
	if err := os.MkdirAll(r.cfg.WorkDir, 0o700); err != nil {
		return fmt.Errorf("create runner.work_dir: %w", err)
	}
	// 独占锁（进程退出即释放）：第二个节点进程用同一个工作目录启动，会把第一个的在途作业当成残留杀掉、删掉。
	lock, err := os.OpenFile(filepath.Join(r.cfg.WorkDir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open runner.work_dir lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return fmt.Errorf("runner.work_dir %s is in use by another runner process: %w", r.cfg.WorkDir, err)
	}
	r.lock = lock // 持有到进程退出
	entries, err := os.ReadDir(r.cfg.WorkDir)
	if err != nil {
		return fmt.Errorf("read runner.work_dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() || (!idgen.IsUUID(e.Name()) && !controlDirPattern.MatchString(e.Name())) {
			continue
		}
		dir := filepath.Join(r.cfg.WorkDir, e.Name())
		if killed := killStaleGroups(dir); killed > 0 {
			r.log.Warn("killed process groups left by a previous run", "dir", e.Name(), "killed", killed)
		}
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove stale job dir: %w", err)
		}
		r.log.Warn("removed stale job dir left by a previous run", "dir", e.Name())
	}
	return nil
}

// killStaleGroups 杀掉残留作业目录 dir 的 pgids 文件里登记的进程组，返回杀掉的个数。逐个核对它们确实
// 还是当初的那些进程（见各平台的 killStaleGroup）——PID 可能已被无关进程复用。
func killStaleGroups(dir string) int {
	data, err := os.ReadFile(filepath.Join(dir, pgidsName)) //nolint:gosec // 路径由平台构造
	if err != nil {
		return 0
	}
	killed := 0
	for _, line := range strings.Split(string(data), "\n") {
		var pgid int
		var started int64
		if n, _ := fmt.Sscanf(line, "%d %d", &pgid, &started); n != 2 || pgid <= 1 || pgid == os.Getpid() || pgid == syscall.Getpgrp() {
			continue
		}
		killed += killStaleGroup(pgid, time.Unix(started, 0), dir)
	}
	return killed
}

// heartbeatLoop 周期心跳并记下引擎答复的启用状态。心跳失败保持上一次的状态。
func (r *Runner) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	failures := 0
	for {
		resp, err := r.eng.Heartbeat(ctx, &jobcontract.HeartbeatRequest{
			AgentVersion: r.cfg.Version, Hostname: r.cfg.Hostname,
			Capacity: int32(r.cfg.Capacity), Running: int32(r.running()), //nolint:gosec // 并发数为个位到两位数
		})
		switch {
		case err == nil:
			if failures > 0 {
				r.log.Info("heartbeat recovered", "failures", failures)
			}
			failures = 0
			if was := r.enabled.Swap(resp.Enabled); was != resp.Enabled {
				r.log.Info("runner enabled state changed", "enabled", resp.Enabled)
			}
		case ctx.Err() != nil:
			return
		default:
			if errors.Is(err, errUnauthorized) {
				// 令牌被拒（节点被删除或令牌已重置）：不再领取；在途作业照常进行。
				r.enabled.Store(false)
			}
			if failures++; failures == 1 || failures%30 == 0 {
				r.log.Error("heartbeat failed", "error", err, "failures", failures)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// claimLoop 周期领取：有空位且最近一次心跳说节点已启用才领取，领到即起一个作业。
// loopCtx 取消即停止领取；作业运行在 hardCtx 上。
func (r *Runner) claimLoop(loopCtx, hardCtx context.Context) {
	failures := 0
	for {
		wait := r.cfg.PollInterval
		if failures > 0 {
			wait = min(r.cfg.PollInterval<<min(failures, 5), claimBackoffMax)
		}
		select {
		case <-loopCtx.Done():
			return
		case <-time.After(wait):
		}
		for loopCtx.Err() == nil && r.enabled.Load() && r.running() < r.cfg.Capacity {
			// 领取请求不随 loopCtx 取消：请求已发出而应答被丢弃，引擎那边的执行就成了没人跑的「执行中」。
			task, err := r.eng.Claim(hardCtx)
			if errors.Is(err, errDisabled) {
				r.enabled.Store(false)
				r.log.Info("runner is disabled, claiming paused")
				break
			}
			if err != nil {
				if failures++; failures == 1 || failures%30 == 0 {
					r.log.Error("claim failed", "error", err, "failures", failures)
				}
				break
			}
			failures = 0
			if task == nil {
				break
			}
			r.launch(hardCtx, task)
		}
	}
}

// launch 登记并启动一个作业。同一执行 ID 已在运行（不应出现）时不重复启动。
// 执行 ID 或领取令牌不合法的任务直接丢弃：没法为它建目录，也没法拿一个假 ID 去报结束。
func (r *Runner) launch(ctx context.Context, task *jobcontract.Task) {
	j, err := newJob(r.cfg, r.eng, r.log, task)
	if err != nil {
		r.log.Error("claimed task dropped", "error", err, "execution_id_len", len(task.ExecutionID))
		return
	}
	r.mu.Lock()
	if _, dup := r.jobs[task.ExecutionID]; dup {
		r.mu.Unlock()
		r.log.Error("claimed an execution that is already running here, ignored", "execution_id", task.ExecutionID)
		return
	}
	r.jobs[task.ExecutionID] = j
	r.wg.Add(1)
	r.mu.Unlock()
	r.log.Info("job claimed", "execution_id", task.ExecutionID, "hosts", len(task.Hosts), "playbook", task.Playbook)
	// 作业 goroutine：run 在作业收尾完成或硬停止后返回。
	go func() {
		defer r.wg.Done()
		defer func() {
			r.mu.Lock()
			delete(r.jobs, task.ExecutionID)
			r.mu.Unlock()
		}()
		j.run(ctx)
	}()
}
