package biz

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Change 一张已批准、待执行的变更单（k8s_change 的执行视图）。
type Change struct {
	ID        string
	ClusterID string
	Kind      int32
	// Exec 加密的执行计划（k8scontract.ChangeExec）；只有 data/kube 解密。
	Exec      []byte
	Requester k8scontract.ChangeRequester
	ClientIP  string
}

// ChangeRepo 变更单的领取与回写。数据面只动执行状态相关的列。
type ChangeRepo interface {
	// Claim 领取一张已批准且未过期的变更单（限 clusterIDs；FOR UPDATE SKIP LOCKED），置为执行中并写租约。
	// 租约过期的执行中变更单：尚未开始向集群提交的可被重新领取；已经开始的（result.started）直接记为失败
	// ——它可能已部分生效，重跑会让不可重入的操作执行两次。没有返回 nil。
	Claim(ctx context.Context, instance string, clusterIDs []string, lease time.Duration) (*Change, error)
	// MarkStarted 记下「已开始向集群提交」（以 instance 围栏）。写不进去则返回错误，调用方不得提交。
	MarkStarted(ctx context.Context, instance, id string) error
	// Finish 写入终态与结果（以 instance 围栏）。
	Finish(ctx context.Context, instance, id string, status int32, message string, result k8scontract.ChangeResult) error
	// Release 集群暂不可达：放回「已批准」，稍后重试（到失效时间为止）。
	Release(ctx context.Context, instance, id, message string) error
}

// ChangeExecutor 执行一张变更单（data/kube 实现）：按字段比对现状、预检、提交。
type ChangeExecutor interface {
	// started 在第一次真正改动集群之前被调用一次（预检与比对都已通过）；它返回错误时不得改动集群。
	ExecuteChange(ctx context.Context, c *Cluster, id Identity, kind int32, sealed []byte, started func() error) (k8scontract.ChangeResult, []*AuditEvent, error)
}

// ChangeRunner 执行已批准的变更单：只处理本实例负责且在线的集群。
//
// 以发起人的身份执行——角色取自批准时刻 core 写入的快照，集群据此重新判定权限：审批期间发起人
// 被收回了数据范围，变更即因无权限而失败。不自动重试冲突 / 无权限 / 被拒绝；只有集群暂不可达才
// 放回队列，到失效时间为止。
type ChangeRunner struct {
	registry *Registry
	prober   *Prober
	scopes   *ScopeSyncer
	repo     ChangeRepo
	exec     ChangeExecutor
	auditor  *Auditor
	log      *slog.Logger
	now      func() time.Time
}

// NewChangeRunner new a ChangeRunner.
func NewChangeRunner(registry *Registry, prober *Prober, scopes *ScopeSyncer, repo ChangeRepo, exec ChangeExecutor, auditor *Auditor, log *slog.Logger) *ChangeRunner {
	return &ChangeRunner{registry: registry, prober: prober, scopes: scopes, repo: repo, exec: exec, auditor: auditor, log: log, now: time.Now}
}

// changeBatch 单轮最多执行的变更单数。
const changeBatch = 5

// changeTimeout 单次变更执行的时限（含 Helm 钩子、排空），短于领取租约。
const changeTimeout = k8scontract.ChangeLease - time.Minute

// Run 执行一批已批准的变更单。恒返回 0（周期循环）。
func (r *ChangeRunner) Run(ctx context.Context) (int, error) {
	ready := map[string]*Cluster{}
	var ids []string
	for _, c := range r.registry.OwnedClusters() {
		if r.prober.Online(c.ID) && r.scopes.Ready(c) {
			ready[c.ID] = c
			ids = append(ids, c.ID)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	for range changeBatch {
		ch, err := r.repo.Claim(ctx, r.registry.InstanceID(), ids, k8scontract.ChangeLease)
		if err != nil {
			return 0, err
		}
		if ch == nil {
			return 0, nil
		}
		r.execute(ctx, ready[ch.ClusterID], ch)
	}
	return 0, nil
}

// ChangeIdentity 执行变更单时的身份：发起人 + 批准时刻的角色快照；审计带变更单 ID。
func ChangeIdentity(ch *Change, now time.Time) Identity {
	roles := ch.Requester.RoleIDs
	if roles == nil {
		roles = []string{}
	}
	return Identity{
		UserID: ch.Requester.UserID, Username: ch.Requester.Username, RoleIDs: roles, IsAdmin: ch.Requester.IsAdmin,
		ClientIP: ch.ClientIP, RequestID: ch.ID, ChangeID: ch.ID, IssuedAt: now.Unix(),
	}
}

// TransientChangeError 报告执行失败是否只是「集群现在不可用」：这类失败放回队列重试，其余一律记为失败。
func TransientChangeError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var be *Error
	if !errors.As(err, &be) {
		return false
	}
	switch be.Reason {
	case k8scontract.ReasonClusterOffline, k8scontract.ReasonNotOwner, k8scontract.ReasonCacheSyncing:
		return true
	}
	return be.Code == http.StatusServiceUnavailable
}

// ErrChangeInterrupted 执行在开始向集群提交之后中断：结果未知，不自动重跑。
const changeInterruptedMessage = "execution was interrupted after it started changing the cluster; the change may be partly applied — verify the resources and resubmit if needed"

// finishAttempts / finishBackoff 回写结果的重试：结果写不进去，变更单会停在执行中直到租约过期。
const (
	finishAttempts = 5
	finishBackoff  = 2 * time.Second
)

func (r *ChangeRunner) execute(ctx context.Context, c *Cluster, ch *Change) {
	id := ChangeIdentity(ch, r.now())
	instance := r.registry.InstanceID()
	started := false
	mark := func() error {
		if started {
			return nil
		}
		mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := r.repo.MarkStarted(mctx, instance, ch.ID); err != nil {
			return NewError(http.StatusServiceUnavailable, k8scontract.ReasonClusterOffline, "cannot record the start of the execution: %s", err.Error())
		}
		started = true
		return nil
	}
	ectx, cancel := context.WithTimeout(ctx, changeTimeout)
	result, audits, err := r.exec.ExecuteChange(ectx, c, id, ch.Kind, ch.Exec, mark)
	cancel()
	result.Started = started

	status, message := k8scontract.ChangeSucceeded, ""
	release := false
	switch {
	case err == nil:
	case TransientChangeError(err) && !started && len(result.Applied) == 0:
		// 还没有动过集群：放回队列，稍后重试。
		release = true
		message = truncate(AuditErrorText(err), k8scontract.MaxChangeMessage)
		r.log.Warn("change execution postponed: cluster unavailable", "change_id", ch.ID, "cluster", c.Name, "error", err)
	case started && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		status, message = k8scontract.ChangeFailed, changeInterruptedMessage
	default:
		status, message = k8scontract.ChangeFailed, truncate(AuditErrorText(err), k8scontract.MaxChangeMessage)
	}
	// 先回写结果、再写审计：结果丢失会让变更单停在执行中；审计写入慢（数据库抖动）不应挤掉它。
	// 用脱离取消的 ctx 并重试：停机中途完成的变更也要留下结果。
	var werr error
	for attempt := 1; attempt <= finishAttempts; attempt++ {
		wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if release {
			werr = r.repo.Release(wctx, instance, ch.ID, message)
		} else {
			werr = r.repo.Finish(wctx, instance, ch.ID, status, message, result)
		}
		wcancel()
		if werr == nil {
			break
		}
		r.log.Warn("write change result failed, retrying", "change_id", ch.ID, "attempt", attempt, "error", werr)
		time.Sleep(finishBackoff)
	}
	if werr != nil {
		// 仍写不进去：租约过期后，已开始的变更单会被记为「执行中断」而不是重跑（见 ChangeRepo.Claim）。
		r.log.Error("write change result failed", "change_id", ch.ID, "cluster", c.Name, "error", werr)
	}
	r.auditor.Record(context.WithoutCancel(ctx), c, id, audits)
}
