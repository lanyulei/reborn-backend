package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/pkg/textx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// 短命令的时限。
	cloneTimeout  = 10 * time.Minute
	gitTimeout    = 30 * time.Second
	sshAddTimeout = 15 * time.Second
	agentWait     = 5 * time.Second
	// agentLifeMargin ssh-agent 里私钥的寿命在作业时限之外留的余量（终止宽限期、收尾）。
	agentLifeMargin = 5 * time.Minute
	// pgidsName 作业目录里记录子进程进程组的文件。
	pgidsName = "pgids"
	// shortGrace 短命令（git、ssh-add）被终止时 SIGTERM 与 SIGKILL 之间的宽限期。
	shortGrace = 3 * time.Second
	// 上报节奏：flushInterval 封块周期；重试退避从 retryBase 翻倍到 retryMax。
	flushInterval = time.Second
	retryBase     = time.Second
	retryMax      = 15 * time.Second
	// finishGiveUp 结束请求连续被引擎以「不可重试」的错误拒绝（令牌被拒、请求被判不合法）多少次后放弃。
	// 网络错误与 5xx 不计入，一直重试。
	finishGiveUp = 10
	// maxErrorDetail 错误信息里引用的命令输出长度上限（字符）。
	maxErrorDetail = 600
	// defaultLeaseSeconds 任务未带租约秒数时的保守取值。
	defaultLeaseSeconds = 60
)

// commitPattern git rev-parse HEAD 的输出：SHA-1 或 SHA-256 的十六进制。
var commitPattern = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

// cause 作业被中断的原因。
type cause int

const (
	causeNone     cause = iota
	causeCancel         // 引擎转达的取消请求
	causeTimeout        // 超过 options.timeout_s
	causeShutdown       // 节点进程停止，等待期已过
	causeLost           // 租约失效：作业作废，不再上报
)

// outcome 一次执行的结果（finish 请求的内容）。
type outcome struct {
	kind     string // jobcontract.Outcome*
	exitCode int32
	err      string
}

// job 一次执行：独占一个工作目录、一个日志缓冲、一条上报 goroutine。
type job struct {
	cfg  *Config
	eng  engine
	log  *slog.Logger
	task *jobcontract.Task
	dir  string
	// ctlDir ssh 连接复用的控制目录（见 controlDirFor）；与作业目录一起建、一起删。
	ctlDir string
	mask   *masker
	buf    *logBuffer
	// timeout 执行时限（options.timeout_s），从作业开始（领取之后）算起，含拉取代码的时间。
	timeout time.Duration
	// flushEvery / retryBase 可由测试调小。
	flushEvery time.Duration
	retryBase  time.Duration

	// mu 保护 why、intrClosed、commitID。
	mu         sync.Mutex
	why        cause
	intrClosed bool
	commitID   string
	// madeDir / madeCtl 作业目录与控制目录是否由本作业建成：清理时只删自己建的。只由运行作业的 goroutine 读写。
	madeDir, madeCtl bool
	// intr 作业被中断时关闭（只关一次）。
	intr chan struct{}
	// rp PLAY RECAP 解析器；只由运行作业的那个 goroutine 读写（execute 设置、finish 读取）。
	rp *recapParser
}

// errBadTaskID 任务的执行 ID 或领取令牌不合法：无法为它建目录，也无法向引擎报结束。
var errBadTaskID = errors.New("task has an invalid execution id or no lease token")

// newJob 构造作业。执行 ID 会拼进作业目录的路径（结束时整个删除），所以在拼路径之前就校验：
// 不是 UUID（如 `./././././././.` 会让作业目录等于工作目录）一律拒绝。
func newJob(cfg *Config, eng engine, logger *slog.Logger, task *jobcontract.Task) (*job, error) {
	if !idgen.IsUUID(task.ExecutionID) || task.LeaseToken == "" {
		return nil, errBadTaskID
	}
	timeout := time.Duration(task.Options.TimeoutS) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(jobcontract.DefaultTimeoutS) * time.Second
	}
	return &job{
		cfg: cfg, eng: eng, task: task, timeout: timeout,
		log:        logger.With("execution_id", task.ExecutionID),
		dir:        filepath.Join(cfg.WorkDir, task.ExecutionID),
		ctlDir:     controlDirFor(cfg.WorkDir, task.ExecutionID),
		mask:       newMasker(taskSecrets(task)...),
		buf:        newLogBuffer(chunkTarget, bufferLimit),
		flushEvery: flushInterval,
		retryBase:  retryBase,
		intr:       make(chan struct{}),
	}, nil
}

// cleanup 删除本作业建的目录（其中可能有口令文件、脚本、待分发的文件）。幂等。
func (j *job) cleanup() {
	if j.madeDir {
		// 脱离了进程组的后代（ssh 的连接复用主进程会 setsid）不受进程组信号管：按 HOME 找出来杀掉，
		// 不让它们带着已认证的连接留在节点上。
		if n := killJobProcesses(j.dir); n > 0 {
			j.log.Info("killed processes left outside the job's process groups", "killed", n)
		}
		if err := os.RemoveAll(j.dir); err != nil {
			j.log.Error("remove job dir failed", "error", err)
		} else {
			j.madeDir = false
		}
	}
	if j.madeCtl {
		if err := os.RemoveAll(j.ctlDir); err != nil {
			j.log.Error("remove job control dir failed", "error", err)
		} else {
			j.madeCtl = false
		}
	}
}

// track 把子进程的进程组记进作业目录的 pgids 文件（每行「进程组号 启动时刻」）：节点崩溃或被强杀后，
// 下次启动据此清掉残留的进程组（Runner.prepareWorkDir）。写失败只记日志，不影响作业。
func (j *job) track(pgid int) {
	f, err := os.OpenFile(filepath.Join(j.dir, pgidsName), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		j.log.Warn("record child process group failed", "error", err)
		return
	}
	_, werr := fmt.Fprintf(f, "%d %d\n", pgid, time.Now().Unix())
	if cerr := f.Close(); werr != nil || cerr != nil {
		j.log.Warn("record child process group failed", "error", errors.Join(werr, cerr))
	}
}

// interrupt 中断作业。第一个原因生效；租约失效例外——它覆盖其它原因（作业已作废，无需再上报）。
func (j *job) interrupt(c cause) {
	j.mu.Lock()
	if j.why == causeNone || c == causeLost {
		j.why = c
	}
	first := !j.intrClosed
	j.intrClosed = true
	j.mu.Unlock()
	if first {
		close(j.intr)
	}
}

func (j *job) cause() cause {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.why
}

func (j *job) setCommit(id string) {
	j.mu.Lock()
	j.commitID = id
	j.mu.Unlock()
}

func (j *job) commit() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.commitID
}

// run 执行整个作业：准备 → 运行 → 把日志交完 → 上报结束 → 清理。ctx 只在节点硬停止时取消
// （此时放弃一切等待）；正常的取消、超时、节点停止都经 interrupt 走终止序列。
func (j *job) run(ctx context.Context) {
	// 作业目录无论如何都要删除（其中可能有口令文件）。
	defer j.cleanup()

	rctx, stopReporter := context.WithCancel(ctx)
	reporterDone := make(chan struct{})
	// 上报 goroutine：rctx 取消或租约失效即结束。
	go func() {
		defer close(reporterDone)
		j.reportLoop(rctx)
	}()

	timer := time.AfterFunc(j.timeout, func() { j.interrupt(causeTimeout) })
	out := j.execute(ctx)
	timer.Stop()
	// 子进程都已结束：立刻删掉作业目录，不等日志交完与结束上报——引擎不可达时那两步会一直重试，
	// 口令文件、脚本与待分发的文件不应陪着留在盘上。
	j.cleanup()

	// 结束之前把日志交完：上报 goroutine 继续工作，直到全部确认、租约失效或硬停止。
	j.buf.Flush()
	j.waitDrained(ctx)
	stopReporter()
	<-reporterDone

	if j.cause() == causeLost {
		j.log.Warn("execution lease lost, job discarded")
		return
	}
	j.finish(ctx, out)
}

// waitDrained 等全部日志被引擎确认。
func (j *job) waitDrained(ctx context.Context) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	acked, progress := j.buf.Acked(), time.Now()
	for !j.buf.Drained() {
		if j.cause() == causeLost {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// 作业已经跑完，引擎却长时间一块日志都不确认（接入令牌被重置、引擎的确认序号倒退）：不再等，
		// 丢掉剩余日志去上报结果——否则这个作业永远占着节点的一个并发位。
		if a := j.buf.Acked(); a != acked {
			acked, progress = a, time.Now()
		} else if time.Since(progress) > drainStall {
			j.log.Error("engine has not acknowledged any log for too long after the job ended, remaining log dropped", "stalled_for", drainStall)
			j.buf.Discard()
			return
		}
	}
}

// drainStall 作业结束后等引擎确认日志的最长无进展时间。
var drainStall = 30 * time.Minute

// reportLoop 周期上报：有封好的日志块就发，没有也至少每 min(lease/4, 3s) 发一次（续租 + 探知取消）。失败后按退避重试，
// 从引擎确认的序号之后重传；引擎答复租约失效则中断作业并丢弃缓冲。
func (j *job) reportLoop(ctx context.Context) {
	lease := time.Duration(j.task.LeaseSeconds) * time.Second
	if lease <= 0 {
		lease = defaultLeaseSeconds * time.Second
	}
	// 没有新日志时的上报间隔：不超过 lease/4（续租），也不超过 reportEvery（取消请求只能随上报的应答
	// 送达，间隔太长取消就迟钝）。
	keepalive := max(min(lease/4, j.cfg.reportEvery), j.flushEvery)
	tick := time.NewTicker(j.flushEvery)
	defer tick.Stop()

	var (
		lastOK   = time.Now()
		retryAt  time.Time
		failures int
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-j.buf.Ready():
		case <-tick.C:
			j.buf.Flush()
		}
		now := time.Now()
		if now.Before(retryAt) {
			continue
		}
		for {
			chunks := j.buf.Pending(jobcontract.MaxLogChunks, reportMaxBytes)
			if len(chunks) == 0 && time.Since(lastOK) < keepalive {
				break
			}
			resp, err := j.eng.Report(ctx, &jobcontract.ReportRequest{
				ExecutionID: j.task.ExecutionID, LeaseToken: j.task.LeaseToken,
				CommitID: j.commit(), Chunks: chunks,
			})
			if errors.Is(err, errLeaseLost) {
				j.buf.Discard()
				j.interrupt(causeLost)
				return
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				failures++
				wait := min(j.retryBase<<min(failures-1, 6), retryMax)
				retryAt = time.Now().Add(wait)
				// 引擎不可达不放弃作业：继续跑、继续重试。首次与此后每 10 次记一条日志。
				if failures == 1 || failures%10 == 0 {
					j.log.Warn("report to engine failed, will retry", "error", err, "failures", failures, "retry_in", wait)
				}
				break
			}
			if failures > 0 {
				j.log.Info("report to engine recovered", "failures", failures)
			}
			failures, lastOK, retryAt = 0, time.Now(), time.Time{}
			before := j.buf.Acked()
			j.buf.Ack(resp.AckSeq)
			if resp.Cancel {
				j.interrupt(causeCancel)
			}
			// 没有待发的块，或引擎没有确认任何新块（不应出现）：等下一个节拍，不原地空转。
			if len(chunks) == 0 || j.buf.Acked() == before {
				break
			}
		}
	}
}

// finish 上报结束，直到引擎答复成功或租约失效。网络错误与 5xx 一直重试；被引擎以不可重试的错误
// 连续拒绝 finishGiveUp 次则放弃（该执行会在租约到期后显示为结果未知）。
func (j *job) finish(ctx context.Context, out outcome) {
	req := &jobcontract.FinishRequest{
		ExecutionID: j.task.ExecutionID, LeaseToken: j.task.LeaseToken,
		Outcome: out.kind, ExitCode: out.exitCode,
		Error:    textx.TruncateRunes(j.mask.Mask(textx.Clean(out.err)), jobcontract.MaxErrorLen),
		CommitID: j.commit(),
	}
	if j.rp != nil {
		req.Hosts = j.rp.Results()
	}
	rejected := 0
	for attempt := 0; ; attempt++ {
		err := j.eng.Finish(ctx, req)
		switch {
		case err == nil:
			j.log.Info("job finished", "outcome", out.kind, "exit_code", out.exitCode)
			return
		case errors.Is(err, errLeaseLost):
			j.log.Warn("finish rejected: execution lease lost, job discarded")
			return
		case ctx.Err() != nil:
			j.log.Error("runner stopped before the job result was delivered", "outcome", out.kind)
			return
		case permanent(err):
			if rejected++; rejected >= finishGiveUp {
				j.log.Error("finish keeps being rejected by engine, giving up", "error", err, "outcome", out.kind)
				return
			}
		}
		wait := min(j.retryBase<<min(attempt, 6), retryMax)
		if attempt == 0 || attempt%10 == 9 {
			j.log.Warn("finish failed, will retry", "error", err, "retry_in", wait)
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
}

// say 往作业日志里写一行平台消息（用户在执行日志里看到进度）。
func (j *job) say(ctx context.Context, format string, args ...any) {
	_ = j.buf.Append(ctx, "[runner] "+j.mask.Mask(fmt.Sprintf(format, args...))+"\n")
}

// failure 构造「未能启动」的结果并把原因写进作业日志。
func (j *job) failure(ctx context.Context, format string, args ...any) outcome {
	msg := j.mask.Mask(fmt.Sprintf(format, args...))
	j.say(ctx, "%s", msg)
	return outcome{kind: jobcontract.OutcomeError, exitCode: -1, err: msg}
}

// interrupted 作业被中断后的结果。
func (j *job) interrupted(ctx context.Context) outcome {
	switch j.cause() {
	case causeTimeout:
		msg := fmt.Sprintf("执行超时（%d 秒），已终止", j.task.Options.TimeoutS)
		j.say(ctx, "%s", msg)
		return outcome{kind: jobcontract.OutcomeTimeout, exitCode: -1, err: msg}
	case causeShutdown:
		msg := "执行节点已停止，作业被取消"
		j.say(ctx, "%s", msg)
		return outcome{kind: jobcontract.OutcomeCanceled, exitCode: -1, err: msg}
	case causeLost:
		return outcome{kind: jobcontract.OutcomeError, exitCode: -1}
	}
	j.say(ctx, "已按取消请求终止")
	return outcome{kind: jobcontract.OutcomeCanceled, exitCode: -1}
}

// isInterrupted 报告作业是否已被中断（各步骤开始前检查）。
func (j *job) isInterrupted() bool {
	select {
	case <-j.intr:
		return true
	default:
		return false
	}
}

// detail 把命令输出整理成可以放进错误信息的一小段：掩码、压成一行、截断。
func (j *job) detail(output string) string {
	s := strings.Join(strings.Fields(j.mask.Mask(output)), " ")
	if s == "" {
		return "（无输出）"
	}
	if r := []rune(s); len(r) > maxErrorDetail {
		s = "…" + string(r[len(r)-maxErrorDetail:])
	}
	return s
}

// execute 准备并运行 ansible-playbook，返回结果。任何一步之前或之中被中断都走 interrupted。
func (j *job) execute(ctx context.Context) outcome {
	t := j.task
	if err := validateTask(t); err != nil {
		return j.failure(ctx, "任务内容不合法，拒绝执行：%s", err)
	}
	// 作业目录：全新、私有。残留的同名目录（上次进程崩溃）先清掉。
	if err := os.RemoveAll(j.dir); err != nil {
		return j.failure(ctx, "清理作业目录失败")
	}
	if err := os.MkdirAll(j.cfg.WorkDir, 0o700); err != nil {
		return j.failure(ctx, "创建工作目录失败")
	}
	if err := os.Mkdir(j.dir, 0o700); err != nil {
		return j.failure(ctx, "创建作业目录失败")
	}
	j.madeDir = true
	if err := os.RemoveAll(j.ctlDir); err != nil {
		return j.failure(ctx, "清理作业目录失败")
	}
	if err := os.Mkdir(j.ctlDir, 0o700); err != nil {
		return j.failure(ctx, "创建作业目录失败")
	}
	j.madeCtl = true

	// 作业模板：拉取代码，Playbook 在仓库里，工作目录 = 仓库目录。
	// 快速执行：不拉取代码，Playbook 由平台生成在作业目录里，工作目录 = 作业目录。
	var playbook, workDir, title string
	if taskKind(t) == jobcontract.KindPlaybook {
		if out, ok := j.checkout(ctx); !ok {
			return out
		}
		workDir = filepath.Join(j.dir, repoDirName)
		if err := resolvePlaybook(workDir, t.Playbook); err != nil {
			return j.failure(ctx, "%s：%s", err, t.Playbook)
		}
		playbook = t.Playbook
		title = fmt.Sprintf("开始执行 %s（主机 %d 台，并发 %d）", t.Playbook, len(t.Hosts), t.Options.Forks)
	} else {
		var err error
		if playbook, err = writeAdhocFiles(t, j.dir); err != nil {
			return j.failure(ctx, "%s", err)
		}
		workDir, title = j.dir, adhocTitle(t)
	}

	inventory, err := buildInventory(t, j.cfg.KnownHosts)
	if err != nil {
		return j.failure(ctx, "生成主机清单失败")
	}
	vars, err := buildVars(t)
	if err != nil {
		return j.failure(ctx, "生成参数文件失败")
	}
	secrets, err := buildSecrets(t)
	if err != nil {
		return j.failure(ctx, "生成凭据文件失败")
	}
	files := map[string][]byte{inventoryName: inventory, varsName: vars, ansibleCfg: buildAnsibleCfg(j.ctlDir)}
	if secrets != nil {
		files[secretsName] = secrets
	}
	for name, data := range files {
		if err := writePrivate(filepath.Join(j.dir, name), data); err != nil {
			return j.failure(ctx, "写入作业文件失败：%s", name)
		}
	}

	sock := ""
	if t.Credential.Type == jobcontract.CredentialSSHKey {
		sock = filepath.Join(j.dir, agentSock)
		stop, err := j.startAgent(sock, t.Credential.Secret, j.timeout)
		if err != nil {
			if errors.Is(err, errInterrupted) {
				return j.interrupted(ctx)
			}
			return j.failure(ctx, "加载登录私钥失败：%s", err)
		}
		defer stop()
	}
	if j.isInterrupted() {
		return j.interrupted(ctx)
	}

	j.say(ctx, "%s", title)
	j.rp = newRecapParser(t.Hosts)
	return j.runBatches(ctx, playbook, workDir, sock, secrets != nil)
}

// runBatches 运行 ansible-playbook：不分批时一次跑完全部主机；options.batch_size > 0 时按主机顺序切成若干批
// 依次执行（每批一次 ansible-playbook，以 --limit 限定到这一批；清单始终是完整的，Playbook 里引用其它主机的
// 变量不受影响）。各批的 PLAY RECAP 汇总到同一个解析器。
//
// 滚动执行每跑完一批就核对失败阈值：已执行的主机里失败与不可达的占比超过 max_fail_percent（0 = 出现任何
// 失败）即中止，剩余批次不执行（那些主机不上报，保持「未执行」），并以非零退出码结束。某一批以非零退出码
// 结束、却没有任何主机被记为失败或不可达（Playbook 自身出错等）同样中止——后面的批次只会重复同一个错误。
// 取消、超时与节点停止对整个作业生效：在批次之间与批次之中都会停下。
func (j *job) runBatches(ctx context.Context, playbook, workDir, sock string, withSecrets bool) outcome {
	t := j.task
	batches := splitBatches(t.Hosts, int(t.Options.BatchSize))
	rolling := len(batches) > 1
	var (
		executed int   // 已执行批次里的主机数
		lastCode int32 // 最后一个非零的批次退出码
	)
	for i, batch := range batches {
		if j.isInterrupted() {
			return j.interrupted(ctx)
		}
		limitFile := ""
		if rolling {
			limitFile = filepath.Join(j.dir, limitFileName(i+1))
			if err := writePrivate(limitFile, buildLimit(batch)); err != nil {
				return j.failure(ctx, "写入作业文件失败：%s", limitFileName(i+1))
			}
			j.say(ctx, "第 %d/%d 批（%d 台）", i+1, len(batches), len(batch))
		}
		failedBefore, unreachableBefore := countBad(j.rp.Results())
		code, interrupted, err := j.runAnsible(ctx, ansibleArgs(t, j.dir, playbook, withSecrets, limitFile), workDir, sock)
		if err != nil {
			return j.failure(ctx, "无法启动 ansible-playbook：%s", err)
		}
		if interrupted {
			return j.interrupted(ctx)
		}
		if code != 0 {
			lastCode = code
		}
		executed += len(batch)
		remaining := len(t.Hosts) - executed
		if !rolling || remaining == 0 {
			continue
		}
		failed, unreachable := countBad(j.rp.Results())
		bad := failed + unreachable
		switch {
		case bad*100 > int(t.Options.MaxFailPercent)*executed:
			j.say(ctx, "失败比例超过阈值（%d%% > %d%%），剩余 %d 台主机未执行", bad*100/executed, t.Options.MaxFailPercent, remaining)
			return outcome{
				kind: jobcontract.OutcomeExited, exitCode: nonZero(lastCode),
				err: fmt.Sprintf("滚动执行中止：%d 台主机失败，%d 台不可达，剩余 %d 台未执行", failed, unreachable, remaining),
			}
		case code != 0 && failed == failedBefore && unreachable == unreachableBefore:
			j.say(ctx, "第 %d 批异常结束（退出码 %d），剩余 %d 台主机未执行", i+1, code, remaining)
			return outcome{
				kind: jobcontract.OutcomeExited, exitCode: code,
				err: fmt.Sprintf("滚动执行中止：第 %d 批异常结束（ansible-playbook 退出码 %d），剩余 %d 台未执行", i+1, code, remaining),
			}
		}
	}
	out := outcome{kind: jobcontract.OutcomeExited, exitCode: lastCode}
	if lastCode != 0 {
		out.err = exitError(lastCode, j.rp.Results())
	}
	j.say(ctx, "执行结束，退出码 %d", lastCode)
	return out
}

// nonZero 中止滚动执行时上报的退出码：引擎据退出码与主机结果推终态，中止必须落为失败 / 部分失败。
// 有失败主机而 ansible-playbook 却以 0 退出（Playbook 里 ignore_unreachable 等）时取 2（Ansible「有主机失败」的退出码）。
func nonZero(code int32) int32 {
	if code == 0 {
		return 2
	}
	return code
}

// countBad 统计主机结果里失败与不可达的台数（口径同主机结果：既失败又不可达的算失败）。
func countBad(hosts []jobcontract.HostResult) (failed, unreachable int) {
	for _, h := range hosts {
		switch h.Status() {
		case jobcontract.HostFailed:
			failed++
		case jobcontract.HostUnreachable:
			unreachable++
		}
	}
	return failed, unreachable
}

// runAnsible 运行一次 ansible-playbook 并等它结束，输出经统计解析与掩码进入日志缓冲。作业被中断（或节点
// 硬停止）时按终止序列杀掉进程组并返回 interrupted = true；err 非空表示进程没能启动。
func (j *job) runAnsible(ctx context.Context, args []string, workDir, sock string) (code int32, interrupted bool, err error) {
	// sink 在读输出的 goroutine 里顺序执行：先喂统计解析（原始输出），再掩码、入缓冲。
	// 缓冲满时 Append 阻塞，即对 ansible-playbook 施加背压；缓冲被丢弃（租约失效）后直接丢输出。
	sink := func(chunk []byte) {
		j.rp.Write(chunk)
		_ = j.buf.Append(ctx, j.mask.Write(chunk))
	}
	p, wait, err := streamProc(j.cfg.AnsiblePlaybook, args, workDir, ansibleEnv(j.dir, sock), sink, j.track)
	if err != nil {
		return 0, false, err
	}
	select {
	case <-p.done:
	case <-j.intr:
		interrupted = true
		p.stop(j.cfg.CancelGrace)
	case <-ctx.Done():
		// 节点硬停止：不再等宽限期。
		interrupted = true
		p.stop(0)
	}
	p.signal(syscall.SIGKILL) // 清掉进程组里残留的后代
	wait()
	j.rp.Close()
	_ = j.buf.Append(ctx, j.mask.Flush())
	if interrupted && j.cause() == causeNone {
		j.interrupt(causeShutdown)
	}
	return p.exitCode(), interrupted, nil
}

// exitError ansible-playbook 以非零退出码结束时的失败原因。解析到了 PLAY RECAP 且其中有失败或不可达的
// 主机时，按用户关心的口径说明（几台失败、几台不可达）；否则只给退出码。
func exitError(code int32, hosts []jobcontract.HostResult) string {
	failed, unreachable := countBad(hosts)
	if failed+unreachable == 0 {
		return fmt.Sprintf("ansible-playbook 以退出码 %d 结束", code)
	}
	return fmt.Sprintf("%d 台主机失败，%d 台不可达（ansible-playbook 退出码 %d）", failed, unreachable, code)
}

// checkout 拉取代码并记录提交号。ok = false 时 out 是应上报的结果。
func (j *job) checkout(ctx context.Context) (out outcome, ok bool) {
	t := j.task
	if j.isInterrupted() {
		return j.interrupted(ctx), false
	}
	sock := ""
	if t.Git.Type == jobcontract.GitAuthSSHKey {
		// 仓库私钥用单独的 agent，拉完即停：它不会被出示给目标主机。
		sock = filepath.Join(j.dir, gitAgentSock)
		stop, err := j.startAgent(sock, t.Git.Secret, cloneTimeout)
		if err != nil {
			if errors.Is(err, errInterrupted) {
				return j.interrupted(ctx), false
			}
			return j.failure(ctx, "加载仓库私钥失败：%s", err), false
		}
		defer stop()
	}
	j.say(ctx, "拉取代码 %s（%s）", t.RepoURL, t.Ref)
	env := gitEnv(j.cfg, j.dir, t.Git, sock)
	output, err := runShort(j.intr, cloneTimeout, shortGrace, j.cfg.Git, cloneArgs(t), j.dir, env, nil, j.track)
	if errors.Is(err, errInterrupted) {
		return j.interrupted(ctx), false
	}
	if err != nil {
		return j.failure(ctx, "拉取代码失败：%s", j.detail(output)), false
	}
	// rev-parse 不需要任何凭据：用不带认证信息的环境。
	head, err := runShort(j.intr, gitTimeout, shortGrace, j.cfg.Git,
		[]string{"-C", repoDirName, "rev-parse", "HEAD"}, j.dir, gitEnv(j.cfg, j.dir, jobcontract.GitAuth{}, ""), nil, j.track)
	if errors.Is(err, errInterrupted) {
		return j.interrupted(ctx), false
	}
	commit := strings.TrimSpace(head)
	if err != nil || !commitPattern.MatchString(commit) {
		return j.failure(ctx, "读取提交号失败：%s", j.detail(head)), false
	}
	j.setCommit(commit)
	j.say(ctx, "代码已就绪，提交 %s", commit)
	return outcome{}, true
}

// agentArgs ssh-agent 的参数：前台运行、指定套接字、身份寿命 = life + agentLifeMargin（秒）。
func agentArgs(sock string, life time.Duration) []string {
	return []string{"-D", "-a", sock, "-t", strconv.Itoa(int((life + agentLifeMargin) / time.Second))}
}

// startAgent 启动一个只属于本次执行的 ssh-agent（前台运行，由节点直接管理），并把私钥经标准输入
// 交给 ssh-add——私钥不落盘。返回的 stop 终止 agent。错误信息不含私钥。life 是这把私钥要用多久：agent 以
// `-t` 给身份设寿命（life + 余量），即便 agent 进程因节点崩溃而残留，里面的私钥也会自行过期。
func (j *job) startAgent(sock, key string, life time.Duration) (stop func(), err error) {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	agent, err := startProc(j.cfg.SSHAgent, agentArgs(sock, life), j.dir, baseEnv(j.dir), nil, devnull, j.track)
	_ = devnull.Close()
	if err != nil {
		return nil, errors.New("无法启动 ssh-agent")
	}
	stop = func() { agent.stop(shortGrace) }
	// 等 agent 建好套接字。
	deadline := time.NewTimer(agentWait)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		select {
		case <-agent.done:
			return nil, errors.New("ssh-agent 意外退出")
		case <-j.intr:
			stop()
			return nil, errInterrupted
		case <-deadline.C:
			stop()
			return nil, errors.New("ssh-agent 启动超时")
		case <-poll.C:
		}
	}
	if !strings.HasSuffix(key, "\n") {
		key += "\n" // OpenSSH 私钥文件缺少结尾换行会被判为格式错误
	}
	// SSH_ASKPASS_REQUIRE=never：带口令的私钥直接失败，而不是试图弹出口令输入。
	env := append(baseEnv(j.dir), "SSH_AUTH_SOCK="+sock, "SSH_ASKPASS_REQUIRE=never")
	output, err := runShort(j.intr, sshAddTimeout, shortGrace, j.cfg.SSHAdd, []string{"-q", "-"}, j.dir, env, strings.NewReader(key), j.track)
	if err != nil {
		stop()
		if errors.Is(err, errInterrupted) {
			return nil, errInterrupted
		}
		return nil, fmt.Errorf("ssh-add 拒绝了该私钥（私钥格式不对或带有口令）：%s", j.detail(output))
	}
	return stop, nil
}
