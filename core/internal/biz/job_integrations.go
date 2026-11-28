package biz

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	jobv1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 本文件是作业中心（job）对流程、通知、文件、监控四个域的适配器：都是只读或「调用对方已有用例」，
// 单向依赖 job → 其他域。对方域没有为作业中心新增任何接口；唯一的登记点是流程引擎的监听器与
// 查看权限注册表（NewWorkflowListeners / NewWorkflowViewers），做法与工单域相同。

// ---------------------------------------------------------------------------
// 审批：流程引擎
// ---------------------------------------------------------------------------

type jobApprovals struct {
	runtime     *bizworkflow.RuntimeUsecase
	definitions *bizworkflow.DefinitionUsecase
	evidence    bizjob.ApprovalEvidenceReader
}

// NewJobApprovals 构造作业审批的流程引擎适配器。
func NewJobApprovals(runtime *bizworkflow.RuntimeUsecase, definitions *bizworkflow.DefinitionUsecase, evidence bizjob.ApprovalEvidenceReader) bizjob.Approvals {
	return jobApprovals{runtime: runtime, definitions: definitions, evidence: evidence}
}

// jobApprovalEvidence 作业审批的审批证据查询：取自流程引擎的实例仓储。直接依赖仓储而不是运行时用例——
// 运行时用例的构造需要监听器，而作业审批的监听器又要用它查证据，经运行时取会成环。
type jobApprovalEvidence struct {
	lookup bizworkflow.InstanceLookup
}

// NewJobApprovalEvidence 构造作业审批的审批证据查询。
func NewJobApprovalEvidence(lookup bizworkflow.InstanceLookup) bizjob.ApprovalEvidenceReader {
	return jobApprovalEvidence{lookup: lookup}
}

// errNoApprovalEvidence 仓储没有提供审批证据（装配缺陷，不应发生）。
var errNoApprovalEvidence = errors.New("job: approval evidence is not available")

// ApprovalEvidenceTx implements bizjob.ApprovalEvidenceReader：只看这条执行名下的审批流程树。
func (e jobApprovalEvidence) ApprovalEvidenceTx(ctx context.Context, tx bizjob.Tx, businessID, except string) (bizjob.ApprovalEvidence, error) {
	r, ok := e.lookup.(bizworkflow.ApprovalEvidenceReader)
	if !ok {
		return bizjob.ApprovalEvidence{}, errNoApprovalEvidence
	}
	ev, err := r.ApprovalEvidenceTx(ctx, tx, jobcontract.ApprovalBusinessType, businessID, except)
	if err != nil {
		return bizjob.ApprovalEvidence{}, err
	}
	return bizjob.ApprovalEvidence{Skipped: ev.SkippedApprovals, Approved: ev.HumanApprovals}, nil
}

// Startable implements bizjob.Approvals.
func (a jobApprovals) Startable(ctx context.Context, key string) (name string, exists, startable bool, err error) {
	exists, startable, err = a.definitions.KeyStartable(ctx, key)
	if err != nil || !exists {
		return "", exists, startable, err
	}
	// 名称只用于展示：按标识搜一页，取标识完全相同的那个。
	items, _, err := a.definitions.List(ctx, bizworkflow.DefinitionFilter{Keyword: key}, 1, paging.MaxPageSize)
	if err != nil {
		return "", exists, startable, err
	}
	for _, d := range items {
		if d.Key == key {
			name = d.Name
		}
	}
	return name, exists, startable, nil
}

// Involved implements bizjob.Approvals。只在用户没有「执行记录」权限时才会问到这里，所以流程引擎
// 放行（参与者，或查看权限注册表放行——即能看全部执行的人）就等于他是参与者。
func (a jobApprovals) Involved(ctx context.Context, instanceID, userID string) (bool, error) {
	_, err := a.runtime.Get(ctx, instanceID, userID, false)
	switch {
	case err == nil:
		return true, nil
	case kerrors.Is(err, bizworkflow.ErrInstanceForbidden), kerrors.Is(err, bizworkflow.ErrInstanceNotFound):
		return false, nil
	}
	return false, err
}

// StartTx implements bizjob.Approvals.
func (a jobApprovals) StartTx(ctx context.Context, tx bizjob.Tx, p bizjob.ApprovalStart) (bizjob.ApprovalOutcome, error) {
	in, err := a.runtime.StartTx(ctx, tx, bizworkflow.StartParams{
		DefinitionKey: p.DefinitionKey, BusinessType: jobcontract.ApprovalBusinessType, BusinessID: p.BusinessID,
		Title: p.Title, Initiator: p.Initiator, Variables: p.Variables,
	})
	if err != nil {
		if kerrors.Is(err, bizworkflow.ErrDefinitionUnavailable) || kerrors.Is(err, bizworkflow.ErrDefinitionNotFound) {
			return bizjob.ApprovalOutcome{}, bizjob.ErrApprovalUnavailable
		}
		return bizjob.ApprovalOutcome{}, err
	}
	outcome := bizjob.ApprovalOutcome{
		InstanceID: in.ID, Finished: in.Status != bizworkflow.InstanceRunning, Approved: in.Status == bizworkflow.InstanceCompleted,
	}
	// 发起即结束且「通过」（审批节点被自动跳过时会这样）：与监听器同一口径看审批证据，没有人真的
	// 审批过就不算通过（见 ApprovalSyncer.Resolve）。
	if outcome.Finished && outcome.Approved {
		ev, err := a.evidenceOf(ctx, tx, in.BusinessID, in.Initiator)
		if err != nil {
			return bizjob.ApprovalOutcome{}, err
		}
		if !bizjob.ApprovalReviewed(ev) {
			outcome.Approved, outcome.Reason = false, bizjob.MsgApprovalUnreviewed
		}
	}
	return outcome, nil
}

// evidenceOf 读取这条执行的审批证据；没有证据来源时返回错误（无法证明有人审批过就不批准）。
func (a jobApprovals) evidenceOf(ctx context.Context, tx bizjob.Tx, businessID, submitter string) (bizjob.ApprovalEvidence, error) {
	if a.evidence == nil {
		return bizjob.ApprovalEvidence{}, errNoApprovalEvidence
	}
	return a.evidence.ApprovalEvidenceTx(ctx, tx, businessID, submitter)
}

// Cancel implements bizjob.Approvals.
func (a jobApprovals) Cancel(ctx context.Context, instanceID, userID string) error {
	return a.runtime.Cancel(ctx, instanceID, userID, "作业执行已取消")
}

// jobApprovalListener 审批流程结束时把结果落到执行上（流程引擎的监听器，在引擎的事务内调用）。
type jobApprovalListener struct {
	syncer *bizjob.ApprovalSyncer
}

// OnInstanceFinished implements bizworkflow.Listener.
func (l jobApprovalListener) OnInstanceFinished(ctx context.Context, tx txrun.Tx, in *bizworkflow.Instance) error {
	reason := ""
	switch in.Status {
	case bizworkflow.InstanceCompleted:
	case bizworkflow.InstanceRejected:
		reason = "审批被驳回"
	case bizworkflow.InstanceCancelled:
		reason = "审批已撤回"
	default:
		reason = "审批流程被终止"
	}
	return l.syncer.Resolve(ctx, tx, in.BusinessID, in.Status == bizworkflow.InstanceCompleted, in.Initiator, reason)
}

// jobApprovalViewer 谁能查看作业审批的流程实例（参与者之外）：能看全部执行记录的人——与执行详情同一口径
// （只有「执行」按钮权限的人只能看自己提交的执行，他是那些流程的发起人，本就是参与者）。
type jobApprovalViewer struct {
	authz *bizsystem.AuthzUsecase
}

// CanViewInstance implements bizworkflow.Viewer.
func (v jobApprovalViewer) CanViewInstance(ctx context.Context, userID, _ string) (bool, error) {
	return v.authz.Allowed(ctx, userID, bizsystem.OperationCode(jobv1.OperationJobExecutionServiceListJobExecutions))
}

type jobSignals struct {
	runtime *bizworkflow.RuntimeUsecase
}

// NewJobSignals 构造向流程实例回发信号的适配器。
func NewJobSignals(runtime *bizworkflow.RuntimeUsecase) bizjob.Signals {
	return jobSignals{runtime: runtime}
}

// Signal implements bizjob.Signals.
func (s jobSignals) Signal(ctx context.Context, instanceID, name string, vars map[string]any) (delivered, gone bool, err error) {
	_, err = s.runtime.Signal(ctx, instanceID, name, vars)
	switch {
	case err == nil:
		return true, false, nil
	case kerrors.Is(err, bizworkflow.ErrSignalNotAwaited):
		return false, false, nil
	case kerrors.Is(err, bizworkflow.ErrInstanceNotRunning), kerrors.Is(err, bizworkflow.ErrInstanceNotFound):
		return false, true, nil
	}
	return false, false, err
}

type jobRunPermission struct {
	authz *bizsystem.AuthzUsecase
}

// NewJobRunPermission 构造「执行作业」接口权限的判定（与授权中间件同一口径）。
func NewJobRunPermission(authz *bizsystem.AuthzUsecase) bizjob.RunPermission {
	return jobRunPermission{authz: authz}
}

// CanRun implements bizjob.RunPermission.
func (p jobRunPermission) CanRun(ctx context.Context, userID string) (bool, error) {
	return p.authz.Allowed(ctx, userID, bizsystem.OperationCode(jobv1.OperationJobExecutionServiceSubmitJobExecution))
}

// CanViewAll implements bizjob.RunPermission.
func (p jobRunPermission) CanViewAll(ctx context.Context, userID string) (bool, error) {
	return p.authz.Allowed(ctx, userID, bizsystem.OperationCode(jobv1.OperationJobExecutionServiceListJobExecutions))
}

// ---------------------------------------------------------------------------
// 通知
// ---------------------------------------------------------------------------

type jobNotifier struct {
	events biznotify.NotificationWriter
}

// NewJobNotifier 构造执行结束通知的写入适配器。
func NewJobNotifier(events biznotify.NotificationWriter) bizjob.Notifier {
	return jobNotifier{events: events}
}

// NewJobSiteURL 通知里详情链接的站点地址：与监控告警共用 monitor.site_url（它就是前端站点地址）。
func NewJobSiteURL(c *conf.Monitor) bizjob.SiteURL {
	return bizjob.SiteURL(strings.TrimRight(strings.TrimSpace(c.GetSiteUrl()), "/"))
}

// NotifyTx implements bizjob.Notifier。通知内容被通知域拒绝（4xx：标题过长等）只影响这一条通知，
// 不让它回滚「已通知」的标记而无限重试。
func (n jobNotifier) NotifyTx(ctx context.Context, tx bizjob.Tx, notice bizjob.Notice) error {
	payload, err := json.Marshal(notice.Payload)
	if err != nil {
		return errors.Join(errJobPayload, err)
	}
	_, err = n.events.NotifyEvent(ctx, tx, biznotify.Event{
		BizID: biznotify.DefaultBizID, EventType: jobcontract.EventExecutionFinished, Severity: notice.Severity,
		Title: notice.Title, Payload: payload, DedupKey: notice.DedupKey, Recipients: notice.Recipients,
	})
	if e := kerrors.FromError(err); err != nil && e != nil && e.Code >= 400 && e.Code < 500 {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// 文件
// ---------------------------------------------------------------------------

type jobFiles struct {
	files *bizfile.Usecase
}

// NewJobFiles 构造读取待分发文件的适配器。
func NewJobFiles(files *bizfile.Usecase) bizjob.Files { return jobFiles{files: files} }

// Read implements bizjob.Files：经文件域的 Download（带访问控制——临时文件只有上传者本人可读）。
func (f jobFiles) Read(ctx context.Context, userID, fileID string) (*bizjob.UploadedFile, error) {
	meta, data, err := f.files.Download(ctx, userID, fileID)
	if err != nil {
		if e := kerrors.FromError(err); e != nil && e.Code >= 400 && e.Code < 500 {
			return nil, kerrors.BadRequest(bizjob.ErrAdhocInvalid.Reason, "the file to distribute does not exist or was not uploaded by you")
		}
		return nil, err
	}
	return &bizjob.UploadedFile{Name: meta.Filename, Data: data}, nil
}

// ---------------------------------------------------------------------------
// 监控：告警事件与规则名称
// ---------------------------------------------------------------------------

type jobAlerts struct {
	alerts bizmonitor.AlertReader
	rules  bizmonitor.RuleLookup
}

// NewJobAlertFeed 构造告警事件来源（告警自愈）。
func NewJobAlertFeed(alerts bizmonitor.AlertReader, rules bizmonitor.RuleLookup) bizjob.AlertFeed {
	return jobAlerts{alerts: alerts, rules: rules}
}

// NewJobAlertRules 构造告警规则名称查询。
func NewJobAlertRules(alerts bizmonitor.AlertReader, rules bizmonitor.RuleLookup) bizjob.AlertRules {
	return jobAlerts{alerts: alerts, rules: rules}
}

// FiringAfter implements bizjob.AlertFeed：告警中、未被屏蔽、(首次触发时间, ID) 在游标之后的事件，按该序升序
// （监控域 Scan 带 FireAfter 时即按此序返回）。
func (j jobAlerts) FiringAfter(ctx context.Context, after bizjob.AlertCursor, limit int) ([]*bizjob.AlertEvent, error) {
	unmuted := false
	items, err := j.alerts.Scan(ctx, bizmonitor.AlertFilter{
		Groups: bizmonitor.GroupFilter{All: true}, Statuses: []int32{mc.AlertFiring}, Muted: &unmuted,
		FireAfter: &bizmonitor.AlertCursor{FirstFireTime: after.FirstFireTime, ID: after.ID},
	}, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*bizjob.AlertEvent, 0, len(items))
	for _, a := range items {
		out = append(out, &bizjob.AlertEvent{
			ID: a.ID, RuleID: a.RuleID, RuleName: a.RuleName, Severity: a.Severity, Labels: a.Labels, FirstFireTime: a.FirstFireTime,
		})
	}
	return out, nil
}

// RuleNames implements bizjob.AlertRules.
func (j jobAlerts) RuleNames(ctx context.Context, ids []string) (map[string]string, error) {
	return j.rules.Names(ctx, ids)
}

// errJobPayload 通知载荷无法编码（不应出现：载荷只含字符串与数字）。
var errJobPayload = errors.New("job notice payload is not encodable")
