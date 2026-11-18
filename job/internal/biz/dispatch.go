package biz

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/pkg/textx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// DefaultLease 缺省租约；MinLease 租约下限（过短会让正常的网络抖动被判为节点失联）。
	DefaultLease = 60 * time.Second
	MinLease     = 10 * time.Second
	// maxDispatchAttempts 一次领取请求里最多连续处理多少条「领取后无法派发」的执行；
	// 用完仍没有可派发的就应答无任务，节点下一轮再来。
	maxDispatchAttempts = 20
	// maxBuildFailures / minStallAge 一条执行连续组装失败这么多次、且持续这么久之后置为失败（见 stalled）。
	maxBuildFailures = 5
	minStallAge      = time.Minute
	// maxStallEntries 组装故障计数表的容量上限。
	maxStallEntries = 1024
	// settleTimeout 领取后的撤销（置失败 / 放回队列）用的独立超时：请求 ctx 可能已被取消，
	// 而这一步不做完，执行会悬在「执行中」直到租约到期被判为结果未知。
	settleTimeout = 5 * time.Second
	// 心跳列宽（job_runner.agent_version / hostname）。
	maxAgentVersionLen = 32
	maxHostnameLen     = 255
)

// 派发失败原因（写入 job_execution.error，不含凭据）。
const (
	reasonBadSpec         = "执行内容无法解析"
	reasonNoHosts         = "没有目标主机"
	reasonLoginDeleted    = "凭据已被删除，无法执行"
	reasonProjectDeleted  = "项目已被删除，无法执行"
	reasonDecrypt         = "凭据解密失败：引擎的主密钥未配置或与管理面不一致"
	reasonRepoAuthMissing = "项目未配置仓库访问凭据"
	reasonAuthMismatch    = "项目的认证方式与仓库地址不匹配"
	reasonStalled         = "引擎多次无法读取该执行所需的数据，已放弃派发"
	reasonPayloadMissing  = "待分发的文件内容不存在"
	reasonPayloadMismatch = "待分发的文件内容与提交时记录的大小或摘要不一致"
)

// RunnerOptions 执行节点协议用例的参数。
type RunnerOptions struct {
	// Lease 执行租约；≤0 取 DefaultLease，低于 MinLease 取 MinLease。
	Lease time.Duration
	// Clock 取当前时刻（只用于组装故障的计时）；nil = time.Now。
	Clock func() time.Time
}

// stall 一条执行连续组装失败的记录。
type stall struct {
	count int
	first time.Time
}

// RunnerUsecase 执行节点协议的用例：鉴权、心跳、领取、上报、结束。无进程内状态，可多实例并发。
type RunnerUsecase struct {
	runners   RunnerRepo
	queue     QueueRepo
	materials MaterialRepo
	payloads  PayloadRepo
	progress  ProgressRepo
	secrets   SecretOpener
	obs       Observer
	log       *slog.Logger
	lease     time.Duration
	clock     func() time.Time

	// mu 保护 stalls：各执行连续组装失败的次数与首次失败时刻。只是本进程的计数（多实例各记各的），
	// 不承担正确性：它只决定「何时不再把一条组装不出来的执行放回队首」。
	mu     sync.Mutex
	stalls map[string]stall
}

// NewRunnerUsecase new a RunnerUsecase（租约取缺省值，WithOptions 覆盖）。
func NewRunnerUsecase(runners RunnerRepo, queue QueueRepo, materials MaterialRepo, payloads PayloadRepo, progress ProgressRepo,
	secrets SecretOpener, obs Observer, logger *slog.Logger,
) *RunnerUsecase {
	return &RunnerUsecase{
		runners: runners, queue: queue, materials: materials, payloads: payloads, progress: progress,
		secrets: secrets, obs: obs, log: logger, lease: DefaultLease,
		clock: time.Now, stalls: map[string]stall{},
	}
}

// WithOptions 注入参数（装配阶段调用一次，非并发安全）。
func (uc *RunnerUsecase) WithOptions(o RunnerOptions) {
	switch {
	case o.Lease <= 0:
		uc.lease = DefaultLease
	case o.Lease < MinLease:
		uc.lease = MinLease
	default:
		uc.lease = o.Lease
	}
	uc.clock = time.Now
	if o.Clock != nil {
		uc.clock = o.Clock
	}
}

// Lease 生效的执行租约。
func (uc *RunnerUsecase) Lease() time.Duration { return uc.lease }

// stalled 记一次组装故障，报告这条执行是否已「持续」组装不出来：连续 maxBuildFailures 次，且距首次失败
// 已过 minStallAge（多个节点同时轮询时次数涨得快，加上时长才不会把一次短暂的数据库抖动当成持续故障）。
func (uc *RunnerUsecase) stalled(executionID string) bool {
	now := uc.clock()
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if len(uc.stalls) >= maxStallEntries {
		clear(uc.stalls) // 只是计数：清空的代价是个别执行多重试几次
	}
	st := uc.stalls[executionID]
	if st.count == 0 {
		st.first = now
	}
	st.count++
	if st.count >= maxBuildFailures && now.Sub(st.first) >= minStallAge {
		delete(uc.stalls, executionID)
		return true
	}
	uc.stalls[executionID] = st
	return false
}

// unstall 清掉一条执行的组装故障计数（组装成功，或已判定无法派发）。
func (uc *RunnerUsecase) unstall(executionID string) {
	uc.mu.Lock()
	delete(uc.stalls, executionID)
	uc.mu.Unlock()
}

// Authenticate 校验接入令牌并返回节点。令牌格式不对、节点不存在、密钥不匹配一律返回
// ErrUnauthorized（对外不区分）；令牌不写入日志与错误。
func (uc *RunnerUsecase) Authenticate(ctx context.Context, token string) (*Runner, error) {
	id, secret, ok := jobcontract.ParseRunnerToken(token)
	if !ok || !idgen.IsUUID(id) {
		return nil, ErrUnauthorized
	}
	r, err := uc.runners.Find(ctx, id)
	if errors.Is(err, ErrNotFound) {
		// 节点不存在时也做一次等长的摘要比较，不让响应时间泄露节点 ID 是否存在。
		jobcontract.RunnerSecretMatches(secret, make([]byte, 32))
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, fmt.Errorf("find runner: %w", err)
	}
	if !jobcontract.RunnerSecretMatches(secret, r.TokenHash) {
		return nil, ErrUnauthorized
	}
	return r, nil
}

// Heartbeat 回写节点心跳并返回节点是否启用。
func (uc *RunnerUsecase) Heartbeat(ctx context.Context, r *Runner, req *jobcontract.HeartbeatRequest) (resp *jobcontract.HeartbeatResponse, err error) {
	defer func() { uc.obs.ObserveCall(ctx, OpHeartbeat, resultOf(err)) }()
	enabled, err := uc.runners.Heartbeat(ctx, r.ID, Heartbeat{
		AgentVersion: textx.TruncateRunes(textx.Clean(req.AgentVersion), maxAgentVersionLen),
		Hostname:     textx.TruncateRunes(textx.Clean(req.Hostname), maxHostnameLen),
		Capacity:     max(req.Capacity, 0),
		Running:      max(req.Running, 0),
	})
	if errors.Is(err, ErrNotFound) {
		// 鉴权之后节点被删除。
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, fmt.Errorf("write heartbeat: %w", err)
	}
	return &jobcontract.HeartbeatResponse{Enabled: enabled}, nil
}

// Claim 为节点领取一条执行并组装任务；没有可执行的返回 (nil, nil)。节点已停用返回 ErrRunnerDisabled。
//
// 领取与组装是两步：领取是单语句原子更新（多实例互斥），组装要读凭据 / 项目并解密。组装发现执行
// 永远无法派发（凭据或项目已删除、解密失败、内容不合法）→ 置为失败并继续领取下一条；组装遇到
// 瞬时故障（数据库错误）→ 放回队列并报错；同一条执行持续组装失败（见 stalled）→ 置为失败，不再堵住队首。
// 撤销都以领取令牌围栏。
func (uc *RunnerUsecase) Claim(ctx context.Context, r *Runner) (task *jobcontract.Task, err error) {
	defer func() {
		result := resultOf(err)
		if err == nil && task == nil {
			result = ResultEmpty
		}
		uc.obs.ObserveCall(ctx, OpClaim, result)
	}()
	if !r.Enabled {
		return nil, ErrRunnerDisabled
	}
	for range maxDispatchAttempts {
		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}
		c, err := uc.queue.ClaimNext(ctx, r.ID, r.Zone, token, uc.lease)
		if err != nil {
			return nil, fmt.Errorf("claim execution: %w", err)
		}
		if c == nil {
			return nil, nil
		}
		task, reason, err := uc.buildTask(ctx, c, token)
		if err != nil {
			// 队列严格按提交顺序领取：放回去的执行下次还是第一个被领到。它若一直组装不出来（比如这一行的
			// 数据让读取总是出错），整个区域的队列都会被它堵住——持续失败后把它置为失败，让出队首。
			// 请求被取消 / 超时（数据库变慢、节点断开）说明不了这条执行本身有问题：只放回，不计数——
			// 否则数据库一慢，队首的执行会被逐条判为失败。
			transient := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
			if !transient && uc.stalled(c.ID) {
				uc.log.ErrorContext(ctx, "execution keeps failing to build, marked failed", "execution_id", c.ID, "error", err)
				uc.settle(ctx, c.ID, "fail", func(sctx context.Context) error { return uc.queue.Fail(sctx, c.ID, token, reasonStalled) })
				uc.obs.AddExecutions(ctx, EventDispatchFailed, 1)
			} else {
				uc.settle(ctx, c.ID, "release", func(sctx context.Context) error { return uc.queue.Release(sctx, c.ID, token) })
			}
			return nil, fmt.Errorf("build task for execution %s: %w", c.ID, err)
		}
		uc.unstall(c.ID)
		if reason != "" {
			uc.log.WarnContext(ctx, "execution cannot be dispatched, marked failed", "execution_id", c.ID, "reason", reason)
			uc.settle(ctx, c.ID, "fail", func(sctx context.Context) error { return uc.queue.Fail(sctx, c.ID, token, reason) })
			uc.obs.AddExecutions(ctx, EventDispatchFailed, 1)
			continue
		}
		uc.obs.AddExecutions(ctx, EventClaimed, 1)
		uc.log.InfoContext(ctx, "execution dispatched", "execution_id", c.ID, "runner_id", r.ID, "hosts", len(task.Hosts))
		return task, nil
	}
	return nil, nil
}

// settle 执行领取后的撤销：脱离请求取消、独立超时。失败只记日志——该执行会在租约到期后被判为结果未知。
// Unclaim 把刚领取、但应答没能送达节点的执行放回队列（节点没拿到完整的任务，不可能已经启动它）。
// 否则它要等租约过期，然后被判为「结果未知」——而它其实从未运行。
func (uc *RunnerUsecase) Unclaim(ctx context.Context, executionID, leaseToken string) {
	uc.settle(ctx, executionID, "unclaim", func(sctx context.Context) error { return uc.queue.Release(sctx, executionID, leaseToken) })
}

func (uc *RunnerUsecase) settle(ctx context.Context, executionID, action string, fn func(ctx context.Context) error) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	if err := fn(sctx); err != nil {
		uc.log.ErrorContext(ctx, "settle claimed execution failed", "execution_id", executionID, "action", action, "error", err)
	}
}

// buildTask 由领取到的执行组装任务。reason 非空 = 该执行永远无法派发（调用方置为失败）；
// err 非空 = 瞬时故障（调用方放回队列）。两者都不含凭据明文。
//
// 作业模板（KindPlaybook）：仓库与 Playbook 取自固化的 spec，Git 认证取自当前的项目行。
// 快速执行（命令 / 脚本 / 文件）：没有项目，不读 job_project，任务里的仓库字段留空；内容取自 spec.adhoc，
// 分发文件另读 job_execution_payload 并核对大小与摘要。
func (uc *RunnerUsecase) buildTask(ctx context.Context, c *Claimed, leaseToken string) (task *jobcontract.Task, reason string, err error) {
	var spec jobcontract.Spec
	dec := json.NewDecoder(bytes.NewReader(c.Spec))
	dec.UseNumber() // 参数里的整数原样透传，不经 float64 丢精度
	if err := dec.Decode(&spec); err != nil {
		return nil, reasonBadSpec, nil
	}
	if reason := validateSpec(&spec); reason != "" {
		return nil, reason, nil
	}
	kind := spec.JobKind()

	rows, err := uc.materials.Hosts(ctx, c.ID)
	if err != nil {
		return nil, "", fmt.Errorf("load hosts: %w", err)
	}
	hosts, reason := taskHosts(rows)
	if reason != "" {
		return nil, reason, nil
	}

	cred, err := uc.materials.Credential(ctx, spec.CredentialID)
	if errors.Is(err, ErrNotFound) {
		return nil, reasonLoginDeleted, nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("load credential: %w", err)
	}
	tc, reason := uc.taskCredential(cred, spec.Options.Become)
	if reason != "" {
		return nil, reason, nil
	}

	task = &jobcontract.Task{
		ExecutionID:  c.ID,
		LeaseToken:   leaseToken,
		LeaseSeconds: int32(uc.lease / time.Second), //nolint:gosec // 租约为分钟量级，不会溢出
		Kind:         kind,
		Hosts:        hosts,
		Credential:   tc,
		ExtraVars:    map[string]any{},
		Options:      spec.Options,
	}
	if kind != jobcontract.KindPlaybook {
		adhoc := *spec.Adhoc
		task.Adhoc = &adhoc
		if kind == jobcontract.KindFile {
			if task.FileContent, reason, err = uc.filePayload(ctx, c.ID, &adhoc); err != nil || reason != "" {
				return nil, reason, err
			}
		}
		return task, "", nil
	}

	proj, err := uc.materials.Project(ctx, spec.ProjectID)
	if errors.Is(err, ErrNotFound) {
		return nil, reasonProjectDeleted, nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("load project: %w", err)
	}
	git, reason := uc.gitAuth(proj, jobcontract.ParseRepoURL(spec.RepoURL))
	if reason != "" {
		return nil, reason, nil
	}
	task.RepoURL, task.Ref, task.Playbook, task.Git = spec.RepoURL, spec.Ref, spec.Playbook, git
	if spec.ExtraVars != nil {
		task.ExtraVars = spec.ExtraVars
	}
	return task, "", nil
}

// filePayload 读分发文件的内容并核对它就是提交时固化的那一份（大小与 SHA-256 记在 spec.adhoc 里，
// 审批与审计看到的是它们）。
func (uc *RunnerUsecase) filePayload(ctx context.Context, executionID string, adhoc *jobcontract.Adhoc) (content []byte, reason string, err error) {
	content, err = uc.payloads.Payload(ctx, executionID)
	if errors.Is(err, ErrNotFound) {
		return nil, reasonPayloadMissing, nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("load file payload: %w", err)
	}
	sum := sha256.Sum256(content)
	if int64(len(content)) != adhoc.FileSize || !strings.EqualFold(hex.EncodeToString(sum[:]), adhoc.FileSHA256) {
		return nil, reasonPayloadMismatch, nil
	}
	return content, "", nil
}

// validateSpec 派发前按契约口径复核固化的执行内容（纵深防御：core 写入时已校验）；返回不合法的原因。
func validateSpec(spec *jobcontract.Spec) string {
	kind := spec.JobKind()
	switch {
	case !jobcontract.ValidKind(kind):
		return "执行内容不合法：作业类型"
	case !idgen.IsUUID(spec.CredentialID):
		return "执行内容不合法：凭据 ID"
	}
	if kind == jobcontract.KindPlaybook {
		switch {
		case jobcontract.ParseRepoURL(spec.RepoURL) == jobcontract.RepoInvalid:
			return "执行内容不合法：仓库地址"
		case !jobcontract.ValidRef(spec.Ref):
			return "执行内容不合法：分支或标签名"
		case !jobcontract.ValidPlaybook(spec.Playbook):
			return "执行内容不合法：Playbook 路径"
		case !idgen.IsUUID(spec.ProjectID):
			return "执行内容不合法：项目 ID"
		case len(spec.ExtraVars) > jobcontract.MaxParams:
			return "执行内容不合法：参数个数超出上限"
		}
		for name := range spec.ExtraVars {
			if !jobcontract.ValidVarName(name) {
				return "执行内容不合法：参数名"
			}
		}
	} else {
		switch {
		case spec.Adhoc == nil:
			return "执行内容不合法：缺少快速执行的内容"
		case spec.Adhoc.Validate(kind) != nil:
			return "执行内容不合法：" + adhocLabels[kind]
		case len(spec.ExtraVars) > 0:
			return "执行内容不合法：快速执行不接受执行参数"
		}
	}
	if err := spec.Options.Normalize(); err != nil {
		return "执行内容不合法：执行选项"
	}
	return ""
}

// adhocLabels 快速执行各类型的内容在错误信息里的叫法（下标 = 作业类型）。
var adhocLabels = [...]string{
	jobcontract.KindCommand: "命令",
	jobcontract.KindScript:  "脚本、解释器或脚本参数",
	jobcontract.KindFile:    "文件名、目标目录、权限或大小",
}

// taskHosts 把主机快照转成任务主机并复核地址与端口。
func taskHosts(rows []Host) ([]jobcontract.TaskHost, string) {
	switch {
	case len(rows) == 0:
		return nil, reasonNoHosts
	case len(rows) > jobcontract.MaxHosts:
		return nil, "执行内容不合法：目标主机数超出上限"
	}
	hosts := make([]jobcontract.TaskHost, 0, len(rows))
	for _, h := range rows {
		if addr, ok := jobcontract.NormalizeAddress(h.Address); !ok || addr != h.Address || h.Port < 1 || h.Port > 65535 {
			return nil, "执行内容不合法：主机地址或端口"
		}
		hosts = append(hosts, jobcontract.TaskHost{
			Address: h.Address,
			Port:    h.Port,
			Vars:    map[string]string{"cmdb_id": h.CIID, "cmdb_name": h.Name, "cmdb_env": h.Env},
		})
	}
	return hosts, ""
}

// taskCredential 解密登录凭据。未开启提权时不下发提权信息（密文不必要地离开引擎）。
func (uc *RunnerUsecase) taskCredential(cred *Credential, become bool) (jobcontract.TaskCredential, string) {
	if cred.Type != jobcontract.CredentialSSHKey && cred.Type != jobcontract.CredentialSSHPassword {
		return jobcontract.TaskCredential{}, "凭据不合法：类型"
	}
	if !textx.ValidLogin(cred.Username) || (cred.BecomeUser != "" && !textx.ValidLogin(cred.BecomeUser)) || !jobcontract.ValidBecomeMethod(cred.BecomeMethod) {
		return jobcontract.TaskCredential{}, "凭据不合法：用户名或提权方式"
	}
	secret, err := uc.secrets.Open(cred.Secret)
	if err != nil || secret == "" {
		return jobcontract.TaskCredential{}, reasonDecrypt
	}
	tc := jobcontract.TaskCredential{Type: cred.Type, Username: cred.Username, Secret: secret}
	if !become {
		return tc, ""
	}
	tc.BecomeMethod, tc.BecomeUser = cred.BecomeMethod, cred.BecomeUser
	if cred.BecomeSecret != nil {
		if tc.BecomeSecret, err = uc.secrets.Open(cred.BecomeSecret); err != nil {
			return jobcontract.TaskCredential{}, reasonDecrypt
		}
	}
	return tc, ""
}

// gitAuth 解密仓库凭据并复核认证方式与仓库地址的传输方式一致（令牌只走 http(s)，私钥只走 ssh）。
func (uc *RunnerUsecase) gitAuth(proj *Project, scheme jobcontract.RepoScheme) (jobcontract.GitAuth, string) {
	switch proj.AuthType {
	case jobcontract.GitAuthNone:
		return jobcontract.GitAuth{Type: jobcontract.GitAuthNone}, ""
	case jobcontract.GitAuthToken:
		if scheme != jobcontract.RepoHTTP {
			return jobcontract.GitAuth{}, reasonAuthMismatch
		}
	case jobcontract.GitAuthSSHKey:
		if scheme != jobcontract.RepoSSH {
			return jobcontract.GitAuth{}, reasonAuthMismatch
		}
	default:
		return jobcontract.GitAuth{}, "项目不合法：认证方式"
	}
	if proj.Secret == nil {
		return jobcontract.GitAuth{}, reasonRepoAuthMissing
	}
	secret, err := uc.secrets.Open(proj.Secret)
	if err != nil || secret == "" {
		return jobcontract.GitAuth{}, reasonDecrypt
	}
	if strings.ContainsAny(proj.Username, "\r\n") {
		return jobcontract.GitAuth{}, "项目不合法：用户名"
	}
	return jobcontract.GitAuth{Type: proj.AuthType, Username: proj.Username, Secret: secret}, ""
}

// newLeaseToken 生成领取令牌：32 字节随机数的十六进制（64 字符，job_execution.lease_token 的列宽）。
func newLeaseToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
