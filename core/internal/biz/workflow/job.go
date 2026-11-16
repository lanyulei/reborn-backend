package biz

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"maps"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// jobBatch 执行器单轮领取的引擎内部作业数（定时、超时、提醒、子流程衔接：只有数据库往返）。
	jobBatch = 20
	// serviceConcurrency 同时执行的自动任务（Webhook 调用）数，也是单轮领取的条数：领到的每一条
	// 立刻开始执行，不在别的调用后面排队——领取即计一次尝试，排队等待不应白白消耗尝试次数、
	// 也不应让租约在等待里过期。每条在回写结果时占用一个数据库连接，取值须远小于连接池上限。
	serviceConcurrency = 4
	// jobLease 领取后的租约：覆盖一次 Webhook 调用（含超时）加回写的时间；执行器崩溃后
	// 租约过期，作业可被再次领取。
	jobLease = 2 * time.Minute
	// maxServiceAttempts 自动任务的最大尝试次数，耗尽进死信。
	maxServiceAttempts = 5
	// maxJobAttempts 其余作业（引擎内部动作）的最大尝试次数；它们只会因暂时性故障失败。
	maxJobAttempts = 10
)

// ErrJobNotFound 作业不存在或已了结。
var ErrJobNotFound = errors.NotFound(v1.ErrorReason_WORKFLOW_JOB_NOT_FOUND.String(), "workflow job not found or already settled")

// errJobGone 作业在事务内复核时已不是待执行（被别的执行器处理了）：放弃本次执行。
var errJobGone = stderrors.New("workflow: job is no longer pending")

// JobQueue 作业队列（事务外）：领取到期作业、续租、登记失败重试、清理。
type JobQueue interface {
	// ClaimDue 领取至多 limit 条到期的待执行作业：续上租约并把尝试次数 +1（SKIP LOCKED，
	// 多实例不重复领取）。service 为 true 只领自动任务（JobService），为 false 只领其余的
	// 引擎内部作业——两类作业分开领取、分开执行，慢的外部调用不会挡住定时与超时。
	ClaimDue(ctx context.Context, service bool, limit int, lease time.Duration) ([]*Job, error)
	// Renew 续租：只有租约仍是自己领取时的那一个才成功（期间被别的执行器重新领走则返回 false）。
	Renew(ctx context.Context, j *Job, lease time.Duration) (bool, error)
	// Retry 登记一次失败：记下错误、推迟 delay 再试并释放租约（时间以数据库时钟为准）。只有
	// 租约仍是自己的才生效：自己卡过了租约、作业已被别的执行器接手时不去动它。
	Retry(ctx context.Context, j *Job, delay time.Duration, lastError string) error
	// Release 放回一条没有执行完的作业：租约立即释放、本次领取不计入尝试次数（执行器停机
	// 打断了调用，不是作业自身的失败）。只对仍持有租约的领取者生效。
	Release(ctx context.Context, j *Job) error
	// Purge 删除 before 之前了结（已完成 / 已取消）的作业，至多 limit 条；返回删除的条数。
	Purge(ctx context.Context, before time.Time, limit int) (int, error)
}

// JobBacklog 作业队列的积压概况（指标用）。
type JobBacklog struct {
	// Overdue 到期超过一分钟仍待执行的作业数；OldestSeconds 其中最早一条已经拖了多久。
	Overdue       int64
	OldestSeconds float64
	// Dead 处于死信、等待人工处理的作业数。
	Dead int64
}

// JobStats 作业队列的只读统计。
type JobStats interface {
	Backlog(ctx context.Context) (JobBacklog, error)
}

// JobTx 作业的事务内操作（与实例状态的回写同一事务）。
type JobTx interface {
	// LockTx 锁住作业行并读出；不存在返回 (nil, nil)。
	LockTx(ctx context.Context, tx txrun.Tx, id string) (*Job, error)
	// SettleTx 把作业置为给定状态并记下最后的错误。
	SettleTx(ctx context.Context, tx txrun.Tx, id string, status int32, lastError string) error
	// ReviveTx 把死信作业重新置为待执行（立即到期、尝试次数清零）。
	ReviveTx(ctx context.Context, tx txrun.Tx, id string) error
}

// ServiceCaller 调用一个 Webhook 端点。retryable 报告失败是否值得重试（网络错误、超时、
// 5xx / 408 / 429 可重试；其余 4xx、目标被安全策略拦截不可重试）。
type ServiceCaller interface {
	Call(ctx context.Context, hook *Webhook, jobID string, body []byte) (resp []byte, retryable bool, err error)
}

const (
	// crashLimit 同一条作业被领取超过这么多次仍无结果（每次都在处理它时执行器崩溃或卡死），
	// 不再执行，直接转死信（照常记历史、通知管理员）。
	crashLimit int32 = 20
	// writeTimeout 外部调用成功后回写结果的时限：回写不随执行器停机而取消，否则已经
	// 成功的调用会因为没来得及记下结果而被重复发起。
	writeTimeout = 10 * time.Second
	// jobRetention 已了结作业的保留时长；purgeBatch 单轮清理的条数。
	jobRetention = 30 * 24 * time.Hour
	purgeBatch   = 1000
)

// backoff 第 attempts 次尝试失败后的重试间隔：30s 起指数退避，封顶 30 分钟。
func backoff(attempts int32) time.Duration {
	return min(30*time.Second<<min(max(attempts-1, 0), 6), 30*time.Minute)
}

// permanent 报告错误是否为确定性的业务错误（重试也不会成功）：引擎的 4xx 类错误。
func permanent(err error) bool {
	var ke *errors.Error
	return stderrors.As(err, &ke) && ke.Code >= 400 && ke.Code < 500
}

// reasonOf 可以展示给流程参与者的失败原因：业务错误给出其说明，其余（数据库、网络等
// 内部故障）只给一句概括——细节在服务日志里，不外泄。
func reasonOf(err error) string {
	var ke *errors.Error
	if stderrors.As(err, &ke) && ke.Code >= 400 && ke.Code < 500 {
		return truncate(ke.Message, 500)
	}
	return "internal error, see the service log"
}

// JobUsecase 作业执行器：领取到期作业并逐条执行。每条作业只在自己所属的实例上开一个
// 事务（锁实例行 → 复核作业仍待执行 → 执行 → 回写 → 作业了结），天然幂等；
// 自动任务的外部调用在事务之外进行，投递语义为「至少一次」（接收端按作业 ID 去重）。
type JobUsecase struct {
	rt     *RuntimeUsecase
	queue  JobQueue
	jobs   JobTx
	hooks  WebhookResolver
	caller ServiceCaller
	log    *slog.Logger
}

// NewJobUsecase new a JobUsecase.
func NewJobUsecase(rt *RuntimeUsecase, queue JobQueue, jobs JobTx, hooks WebhookResolver, caller ServiceCaller, log *slog.Logger) *JobUsecase {
	return &JobUsecase{rt: rt, queue: queue, jobs: jobs, hooks: hooks, caller: caller, log: log}
}

// RunDue 领取并逐条执行一批到期的引擎内部作业（定时、超时、提醒、子流程衔接），返回处理的
// 条数（后台循环据此决定是否立即再跑一轮）。单条作业失败只登记重试，不影响同批其他作业。
// 自动任务（外部调用）不在这里执行，见 RunServices。
func (uc *JobUsecase) RunDue(ctx context.Context) (int, error) {
	jobs, err := uc.queue.ClaimDue(ctx, false, jobBatch, jobLease)
	if err != nil {
		return 0, err
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			break
		}
		// 同批靠后的作业开始前先续租：前面的作业若耗时很长（数据库抖动、锁等待），租约可能已
		// 过期并被别的执行器领走——续租失败就不再碰它，避免同一条作业被两边同时执行。
		if ok, err := uc.queue.Renew(ctx, j, jobLease); err != nil || !ok {
			continue
		}
		uc.process(ctx, j)
	}
	return len(jobs), nil
}

// RunServices 领取并并发执行一批到期的自动任务（Webhook 调用），返回处理的条数。与引擎内部
// 作业分开领取、分开执行：一个迟迟不响应的端点只占用这里的并发名额，不会让排在它后面的
// 定时节点、任务超时与子流程衔接一起停摆。领到的每一条立刻开始执行（见 serviceConcurrency）。
func (uc *JobUsecase) RunServices(ctx context.Context) (int, error) {
	jobs, err := uc.queue.ClaimDue(ctx, true, serviceConcurrency, jobLease)
	if err != nil {
		return 0, err
	}
	var g errgroup.Group
	for _, j := range jobs {
		g.Go(func() error {
			// 单条作业的 panic 不应带垮整个进程（errgroup 不兜 panic）：记日志，作业留给租约
			// 过期后重试（尝试次数已计，反复如此会按 crashLimit 转死信）。
			defer func() {
				if r := recover(); r != nil {
					uc.log.ErrorContext(ctx, "workflow service job panicked", "job", j.ID, "instance", j.InstanceID, "panic", r)
				}
			}()
			uc.process(ctx, j)
			return nil
		})
	}
	_ = g.Wait() // 各协程自行处置失败，恒返回 nil
	return len(jobs), nil
}

// process 执行一条已领取的作业；反复领取仍无结果的直接转死信。
func (uc *JobUsecase) process(ctx context.Context, j *Job) {
	if j.Attempts > crashLimit {
		uc.log.ErrorContext(ctx, "workflow job abandoned after repeated attempts", "job", j.ID, "kind", j.Kind, "instance", j.InstanceID)
		if err := uc.dead(ctx, j, "abandoned: the job never finished after repeated attempts"); err != nil && !stderrors.Is(err, errJobGone) {
			uc.log.ErrorContext(ctx, "dead-letter workflow job", "job", j.ID, "error", err)
		}
		return
	}
	uc.handle(ctx, j)
}

// handle 执行一条作业并处置其失败：确定性的业务错误直接转死信（记历史、通知管理员），
// 暂时性故障退避重试，重试耗尽同样转死信。
func (uc *JobUsecase) handle(ctx context.Context, j *Job) {
	err := uc.run(ctx, j)
	if err == nil || stderrors.Is(err, errJobGone) {
		return
	}
	uc.log.WarnContext(ctx, "workflow job failed", "job", j.ID, "kind", j.Kind, "instance", j.InstanceID, "attempts", j.Attempts, "error", err)
	if !permanent(err) && j.Attempts < maxJobAttempts {
		if rerr := uc.queue.Retry(ctx, j, backoff(j.Attempts), reasonOf(err)); rerr != nil {
			uc.log.ErrorContext(ctx, "record workflow job failure", "job", j.ID, "error", rerr)
		}
		return
	}
	if derr := uc.dead(ctx, j, reasonOf(err)); derr != nil && !stderrors.Is(derr, errJobGone) {
		uc.log.ErrorContext(ctx, "dead-letter workflow job", "job", j.ID, "error", derr)
	}
}

// dead 把作业转为死信：在实例上记历史并通知流程管理员（与作业状态同一事务）。
func (uc *JobUsecase) dead(ctx context.Context, j *Job, reason string) error {
	return uc.rt.jobCommand(ctx, uc.jobs, j, JobDead, reason, func(ctx context.Context, x *Execution) error {
		return x.JobDead(ctx, j, reason)
	})
}

// run 执行一条作业。返回的错误由 handle 统一处置；自动任务的调用失败在这里自行处置
// （它有自己的重试上限，且失败原因来自外部端点而非引擎）。
func (uc *JobUsecase) run(ctx context.Context, j *Job) error {
	if j.Kind != JobService {
		return uc.rt.jobCommand(ctx, uc.jobs, j, JobDone, "", func(ctx context.Context, x *Execution) error {
			return x.Fire(ctx, j)
		})
	}
	vars, retryable, callErr := uc.call(ctx, j)
	if stderrors.Is(callErr, errJobGone) {
		return callErr
	}
	// 外部调用已有结果：回写不随执行器停机而取消。
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	if callErr == nil {
		return uc.rt.jobCommand(wctx, uc.jobs, j, JobDone, "", func(ctx context.Context, x *Execution) error {
			// 业务对象名下的变量（如工单的表单字段）不由外部系统的返回改写：丢弃这些键并记日志，
			// 其余照常并入——调用已经成功，不因多返回了几个键就让流程停下。
			owned, err := uc.rt.ownedIn(ctx, x.Instance(), vars)
			if err != nil {
				return err
			}
			if len(owned) > 0 {
				vars = maps.Clone(vars)
				for _, name := range owned {
					delete(vars, name)
				}
				uc.log.WarnContext(ctx, "workflow service result tried to overwrite business-owned variables; ignored",
					"job", j.ID, "instance", j.InstanceID, "variables", owned)
			}
			return x.ServiceDone(ctx, j, vars)
		})
	}
	if ctx.Err() != nil {
		// 执行器正在停机，调用是被我们自己打断的：不算作业的一次失败（否则赶上最后一次尝试
		// 时，一次发布就能把作业送进死信），放回队列由别的执行器接手。
		return uc.queue.Release(wctx, j)
	}
	reason := truncate(callErr.Error(), 500)
	if retryable && j.Attempts < maxServiceAttempts {
		return uc.queue.Retry(wctx, j, backoff(j.Attempts), reason)
	}
	return uc.dead(wctx, j, reason)
}

// settle 在独立事务里把一条仍待执行的作业置为终态（不经引擎）；期间已被别处了结的不动。
func (uc *JobUsecase) settle(ctx context.Context, j *Job, status int32) error {
	return uc.rt.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		cur, err := uc.jobs.LockTx(ctx, tx, j.ID)
		if err != nil || cur == nil || cur.Status != JobPending {
			return err
		}
		return uc.jobs.SettleTx(ctx, tx, j.ID, status, "")
	})
}

// Purge 清理早已了结的作业（后台循环定期调用），返回删除的条数。
func (uc *JobUsecase) Purge(ctx context.Context) (int, error) {
	return uc.queue.Purge(ctx, time.Now().Add(-jobRetention), purgeBatch)
}

// servicePayload 自动任务发给 Webhook 端点的请求体。
type servicePayload struct {
	JobID          string         `json:"jobId"`
	Attempt        int32          `json:"attempt"`
	InstanceID     string         `json:"instanceId"`
	DefinitionKey  string         `json:"definitionKey"`
	DefinitionName string         `json:"definitionName"`
	NodeID         string         `json:"nodeId"`
	Title          string         `json:"title"`
	Initiator      string         `json:"initiator"`
	BusinessType   string         `json:"businessType,omitempty"`
	BusinessID     string         `json:"businessId,omitempty"`
	Variables      map[string]any `json:"variables"`
}

// call 调用自动任务的 Webhook 端点并解析它返回的流程变量。响应体可为空；非空时须是
// JSON 对象，其 variables 字段（对象）并入流程变量。
func (uc *JobUsecase) call(ctx context.Context, j *Job) (map[string]any, bool, error) {
	id, _ := j.Payload["webhookId"].(string)
	in, err := uc.rt.lookup.GetInstance(ctx, j.InstanceID)
	if err != nil {
		uc.log.WarnContext(ctx, "load workflow instance for a service job", "job", j.ID, "error", err)
		return nil, true, stderrors.New("internal error, see the service log")
	}
	if in.Status != InstanceRunning {
		// 实例已结束：不再对外调用；作业作废。
		if err := uc.settle(ctx, j, JobCancelled); err != nil {
			return nil, true, err
		}
		return nil, false, errJobGone
	}
	hook, err := uc.hooks.Resolve(ctx, id)
	if err != nil {
		// 端点被删除 / 停用是配置问题，重试无益；其余（数据库故障）可重试，原因不外泄。
		if errors.Is(err, ErrWebhookNotFound) {
			return nil, false, stderrors.New("the webhook endpoint does not exist or is disabled")
		}
		uc.log.WarnContext(ctx, "resolve workflow webhook", "job", j.ID, "error", err)
		return nil, true, stderrors.New("internal error, see the service log")
	}
	body, err := json.Marshal(servicePayload{
		JobID: j.ID, Attempt: j.Attempts, InstanceID: in.ID, DefinitionKey: in.DefinitionKey, DefinitionName: in.DefinitionName,
		NodeID: j.NodeID, Title: in.Title, Initiator: in.Initiator, BusinessType: in.RootBusinessType, BusinessID: in.RootBusinessID,
		Variables: nonNilVars(in.Variables),
	})
	if err != nil {
		return nil, false, err
	}
	resp, retryable, err := uc.caller.Call(ctx, hook, j.ID, body)
	if err != nil {
		return nil, retryable, err
	}
	if len(resp) == 0 {
		return nil, false, nil
	}
	var out struct {
		Variables map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		return nil, false, stderrors.New("webhook response is not a JSON object")
	}
	merged := nonNilVars(in.Variables)
	for k, v := range out.Variables {
		merged[k] = v
	}
	if err := checkVariables(merged); err != nil {
		return nil, false, stderrors.New("webhook response makes the workflow variables too large or invalid")
	}
	return out.Variables, false, nil
}

func nonNilVars(vars map[string]any) map[string]any {
	out := make(map[string]any, len(vars))
	for k, v := range vars {
		out[k] = v
	}
	return out
}

// jobCommand 在作业所属实例上执行一条由作业触发的命令：锁实例行 → 复核作业仍待执行 →
// 执行 → 回写 → 作业置为 settle 状态。实例已结束时作业直接作废。
func (uc *RuntimeUsecase) jobCommand(ctx context.Context, jobs JobTx, j *Job, settle int32, lastError string, fn func(ctx context.Context, x *Execution) error) error {
	return uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		ctx := txrun.NewContext(ctx, tx)
		s, err := uc.store.LoadForUpdateTx(ctx, tx, j.InstanceID)
		if err != nil {
			return err
		}
		cur, err := jobs.LockTx(ctx, tx, j.ID)
		if err != nil {
			return err
		}
		if cur == nil || cur.Status != JobPending {
			return errJobGone
		}
		if s.Instance.Status != InstanceRunning {
			return jobs.SettleTx(ctx, tx, j.ID, JobCancelled, "")
		}
		g, err := uc.defs.graph(ctx, s.Instance.VersionID)
		if errors.Is(err, ErrVersionInvalid) && (settle == JobDead || j.Kind == JobStop) {
			// 流程图已通不过校验：转死信与「父流程已结束，终止本实例」都不依赖流程图，照常
			// 执行（否则这条作业既完不成、也进不了死信，会被无休止地重新领取）。
			g, err = nil, nil
		}
		if err != nil {
			return err
		}
		x, err := uc.exec(ctx, g, s)
		if err != nil {
			return err
		}
		if err := uc.syncOwned(ctx, x, s); err != nil {
			return err
		}
		if err := fn(ctx, x); err != nil {
			return err
		}
		// 先了结作业再回写：实例若在这条命令里结束，回写会把它名下仍待执行的作业一并作废，
		// 不应波及刚被置为死信的这一条。
		if err := jobs.SettleTx(ctx, tx, j.ID, settle, lastError); err != nil {
			return err
		}
		if err := uc.store.SaveTx(ctx, tx, s); err != nil {
			return err
		}
		return uc.after(ctx, tx, s)
	})
}

// RetryJob 管理员重试一条死信作业：重新置为待执行，由执行器再次执行。实例已结束的不能重试。
func (uc *RuntimeUsecase) RetryJob(ctx context.Context, jobs JobTx, instanceID, jobID string) error {
	return uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		ctx := txrun.NewContext(ctx, tx)
		// 先锁实例再锁作业（与执行器同一加锁顺序）。
		st, err := uc.store.LoadForUpdateTx(ctx, tx, instanceID)
		if err != nil {
			return err
		}
		if st.Instance.Status != InstanceRunning {
			return ErrInstanceNotRunning
		}
		j, err := jobs.LockTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if j == nil || j.InstanceID != instanceID || j.Status != JobDead {
			return ErrJobNotFound
		}
		return jobs.ReviveTx(ctx, tx, jobID)
	})
}

// SkipJob 管理员跳过一个等待节点（死信的自动任务、未到期的定时节点）：流程从它之后继续。
func (uc *RuntimeUsecase) SkipJob(ctx context.Context, jobs JobTx, instanceID, jobID, operator, reason string) (int32, error) {
	return uc.command(ctx, instanceID, true, func(ctx context.Context, x *Execution) error {
		tx := txrun.FromContext(ctx)
		j, err := jobs.LockTx(ctx, tx, jobID)
		if err != nil {
			return err
		}
		if j == nil || j.InstanceID != instanceID || (j.Status != JobPending && j.Status != JobDead) ||
			(j.Kind != JobService && j.Kind != JobTimer) {
			return ErrJobNotFound
		}
		if err := x.SkipWait(ctx, j, operator, clean(reason)); err != nil {
			return err
		}
		return jobs.SettleTx(ctx, tx, jobID, JobCancelled, j.LastError)
	})
}
