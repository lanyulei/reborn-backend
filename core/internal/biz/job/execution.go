package biz

import (
	"context"
	"strings"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 读取上限。
const (
	// maxHostPageSize 执行主机列表的每页上限（一次执行至多 jobcontract.MaxHosts 台，可一页取完）。
	maxHostPageSize = jc.MaxHosts
	// logPageBytes 一次增量读取返回的日志内容上限（至少返回一块，实际可能略超）。
	logPageBytes = 1 << 20
	// problemsListed 拒绝提交时在说明里列出的问题主机个数。
	problemsListed = 5
)

// ExecutionUsecase 执行用例：提交（作业模板 / 快速执行）、查询、读日志、取消、统计。
// 执行状态由执行引擎推进，本用例只建单、发起审批与请求取消。
type ExecutionUsecase struct {
	reader      ExecutionReader
	logs        ExecutionLogReader
	writer      ExecutionWriter
	templates   TemplateReader
	projects    ProjectReader
	credentials CredentialReader
	runners     RunnerTokens
	targets     *TargetUsecase
	dir         Directory
	ext         ExecutionDeps
}

// ExecutionDeps 二期 / 三期引入的依赖（事务、策略、审批、脚本、文件、统计、触发来源名称）。
type ExecutionDeps struct {
	Txs       TxManager
	Policies  PolicyRepo
	Rules     RiskRuleRepo
	Scripts   ScriptRepo
	Files     Files
	Approvals Approvals
	Sources   TriggerNames
	Stats     StatsReader
	Runs      RunPermission
}

// NewExecutionDeps 聚合 ExecutionUsecase 的扩展依赖。
func NewExecutionDeps(txs TxManager, policies PolicyRepo, rules RiskRuleRepo, scripts ScriptRepo, files Files,
	approvals Approvals, sources TriggerNames, stats StatsReader, runs RunPermission,
) ExecutionDeps {
	return ExecutionDeps{
		Txs: txs, Policies: policies, Rules: rules, Scripts: scripts, Files: files,
		Approvals: approvals, Sources: sources, Stats: stats, Runs: runs,
	}
}

// NewExecutionUsecase new an ExecutionUsecase.
func NewExecutionUsecase(reader ExecutionReader, logs ExecutionLogReader, writer ExecutionWriter, templates TemplateReader,
	projects ProjectReader, credentials CredentialReader, runners RunnerTokens, targets *TargetUsecase, dir Directory,
	ext ExecutionDeps,
) *ExecutionUsecase {
	return &ExecutionUsecase{
		reader: reader, logs: logs, writer: writer, templates: templates, projects: projects,
		credentials: credentials, runners: runners, targets: targets, dir: dir, ext: ext,
	}
}

// Submit 按模板提交一次执行。提交即固化：模板版本、项目地址与分支、Playbook、凭据 ID、参数取值、
// 执行选项与目标主机快照都写进执行记录，之后编辑模板不影响本次执行。
//
// 目标主机按执行人的 CMDB 数据范围解析：模板指定而执行人看不到的主机不会进入本次执行；
// 入参点名的主机（in.CIIDs）必须全部在解析结果内，否则整体拒绝——绝不悄悄缩小或扩大范围。
// 有任何一台选中的主机地址不可用同样整体拒绝。
//
// 模板要求审批、或目标主机含受保护环境时（预检模式除外），执行进入「待审批」并同事务发起审批流程。
// 模板开启切片时，一次提交按每片主机数拆成多条执行，返回第一条。
func (uc *ExecutionUsecase) Submit(ctx context.Context, userID string, in SubmitInput) (*Execution, error) {
	// 非手动的提交（定时作业 / 触发器）没有经过接口授权中间件：以负责人身份执行，就按负责人此刻的权限判定。
	if in.Trigger != 0 && in.Trigger != jc.TriggerManual {
		if err := uc.RequireRun(ctx, userID); err != nil {
			return nil, err
		}
	}
	tpl, err := uc.templates.Get(ctx, in.TemplateID)
	if err != nil {
		return nil, err
	}
	if !tpl.Enabled {
		return nil, ErrTemplateDisabled
	}
	// 定时作业 / 触发器以负责人的身份与数据范围执行：模板在负责人确认之后被别人改过（换 Playbook、
	// 放宽目标、去掉审批），就不能再替负责人执行——须由负责人重新保存确认。
	// 手动执行同理：确认框里看到的是旧内容，就不能照着新内容执行。
	if in.TemplateVersion != 0 && tpl.Version != in.TemplateVersion {
		if in.Trigger == 0 || in.Trigger == jc.TriggerManual {
			return nil, detail(ErrTemplateInvalid, "the job template changed (v%d → v%d) while you were confirming; review it and run again", in.TemplateVersion, tpl.Version)
		}
		return nil, detail(ErrTemplateInvalid, "the job template changed (v%d → v%d) since this was last saved; open it, review the template and save again", in.TemplateVersion, tpl.Version)
	}
	// 模板保存时已校验过参数定义；提交时再校验一次：契约收紧（如保留变量名清单扩充）之后的存量模板
	// 在这里就给出明确的原因，而不是派发到执行节点后才被拒绝。
	if err := validateParams(tpl.Params); err != nil {
		return nil, detail(ErrTemplateInvalid, "the template's params are no longer valid (%s); please edit and save it again", err.Error())
	}
	project, err := uc.projects.Get(ctx, tpl.ProjectID)
	if err != nil {
		return nil, missingAs(err, ErrProjectNotFound, "the template's project no longer exists")
	}
	credential, err := uc.credentials.Get(ctx, tpl.CredentialID)
	if err != nil {
		return nil, missingAs(err, ErrCredentialNotFound, "the template's credential no longer exists")
	}
	vars, err := resolveParams(tpl.Params, in.Params)
	if err != nil {
		return nil, err
	}
	policy, err := uc.ext.Policies.Get(ctx)
	if err != nil {
		return nil, err
	}
	hosts, err := uc.selectHosts(ctx, userID, tpl, in.CIIDs, policy)
	if err != nil {
		return nil, err
	}
	options := tpl.Options
	if in.Check != nil {
		options.Check = *in.Check
	}
	// 模板保存时已校验过；这里再校验一次，防止直接改库或契约收紧后的存量数据进入执行。
	if err := options.Normalize(); err != nil {
		return nil, detail(ErrTemplateInvalid, "%s", err.Error())
	}
	if !jc.ValidPlaybook(tpl.Playbook) || jc.ParseRepoURL(project.RepoURL) == jc.RepoInvalid || !jc.ValidRef(project.Ref) {
		return nil, detail(ErrTemplateInvalid, "the template's playbook or project is no longer valid, please edit and save it again")
	}
	trigger := in.Trigger
	if trigger == 0 {
		trigger = jc.TriggerManual
	}
	return uc.submit(ctx, userID, policy, &submission{
		kind: jc.KindPlaybook, name: tpl.Name, templateID: tpl.ID, templateVersion: tpl.Version,
		spec: jc.Spec{
			Kind: jc.KindPlaybook, ProjectID: project.ID, ProjectName: project.Name, RepoURL: project.RepoURL, Ref: project.Ref,
			Playbook: tpl.Playbook, CredentialID: credential.ID, CredentialName: credential.Name,
			ExtraVars: vars, Options: options,
		},
		runnerZone: tpl.RunnerZone, exclusive: tpl.Exclusive, sliceSize: int(tpl.SliceSize), hosts: hosts,
		trigger: trigger, triggerRef: in.TriggerRef, requestID: in.RequestID,
		reasons: approvalReasons(policy, jc.KindPlaybook, tpl.RequireApproval, options.Check, hosts, nil),
	})
}

// RequireRun 用户须持有「执行作业」的接口权限，否则返回 ErrRunForbidden。
func (uc *ExecutionUsecase) RequireRun(ctx context.Context, userID string) error {
	ok, err := uc.ext.Runs.CanRun(ctx, userID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRunForbidden
	}
	return nil
}

// RequireVisible 用户须能查看这条执行：持有「执行记录」权限的人能看全部；只有「执行」按钮权限的人
// （提交后跳到详情页）只能看自己提交的——否则凭一个执行 ID 就能读到别人的脚本、参数与日志。
// 不可见时返回 ErrExecutionNotFound，不透露它是否存在。
func (uc *ExecutionUsecase) RequireVisible(ctx context.Context, userID, id string) error {
	all, err := uc.ext.Runs.CanViewAll(ctx, userID)
	if err != nil || all {
		return err
	}
	e, err := uc.reader.Get(ctx, id)
	if err != nil {
		return err
	}
	if e.Creator == userID {
		return nil
	}
	// 审批人要能看到自己正在审批的内容（脚本、参数、目标主机），哪怕他没有「执行记录」权限。
	if e.ApprovalInstanceID != "" {
		if ok, err := uc.ext.Approvals.Involved(ctx, e.ApprovalInstanceID, userID); err != nil || ok {
			return err
		}
	}
	return ErrExecutionNotFound
}

// TemplatePreflight 模板当前会选中的主机，以及实际执行时是否需要审批（执行前确认用）。
func (uc *ExecutionUsecase) TemplatePreflight(ctx context.Context, userID, templateID string) (*Preflight, error) {
	tpl, err := uc.templates.Get(ctx, templateID)
	if err != nil {
		return nil, err
	}
	policy, err := uc.ext.Policies.Get(ctx)
	if err != nil {
		return nil, err
	}
	limit := hostCap(policy, int(tpl.SliceSize))
	set, err := uc.targets.ResolveN(ctx, userID, tpl.Target, limit)
	if err != nil {
		return nil, err
	}
	pf := &Preflight{HostSet: set, MaxHosts: limit}
	pf.ApprovalReasons = approvalReasons(policy, jc.KindPlaybook, tpl.RequireApproval, false, set.Hosts, nil)
	pf.ApprovalReady, err = uc.approvalReady(ctx, policy)
	return pf, err
}

// approvalReady 报告全局策略里的审批流程当前能否发起。
func (uc *ExecutionUsecase) approvalReady(ctx context.Context, policy *Policy) (bool, error) {
	if policy.ApprovalWorkflowKey == "" {
		return false, nil
	}
	_, _, startable, err := uc.ext.Approvals.Startable(ctx, policy.ApprovalWorkflowKey)
	return startable, err
}

// hostCap 一次提交的主机数上限：不切片时是单条执行的上限；切片时是每片上限 × 最多片数。
func hostCap(policy *Policy, sliceSize int) int {
	limit := policy.HostLimit()
	if sliceSize > 0 {
		return min(sliceSize, limit) * jc.MaxSlices
	}
	return limit
}

// missingAs 把「引用对象不存在」翻译成模板不可用（其余错误原样返回）。
func missingAs(err error, notFound error, msg string) error {
	if errorsIs(err, notFound) {
		return detail(ErrTemplateInvalid, "%s", msg)
	}
	return err
}

// selectHosts 解析模板的目标主机并按入参收窄，返回可直接执行的主机列表。
func (uc *ExecutionUsecase) selectHosts(ctx context.Context, userID string, tpl *Template, only []string, policy *Policy) ([]Host, error) {
	limit := hostCap(policy, int(tpl.SliceSize))
	set, err := uc.targets.ResolveN(ctx, userID, tpl.Target, limit)
	if err != nil {
		return nil, err
	}
	hosts := set.Hosts
	if only = uniq(only); len(only) > 0 {
		byID := make(map[string]Host, len(hosts))
		for _, h := range hosts {
			byID[h.CIID] = h
		}
		hosts = make([]Host, 0, len(only))
		for _, id := range only {
			h, ok := byID[id]
			if !ok {
				return nil, detail(ErrTargetInvalid, "host %s is not among the template's target hosts or is outside your data scope", id)
			}
			hosts = append(hosts, h)
		}
		// 收窄后重新判定地址重复：重复只在同一次执行内有意义。
		hosts = recheckDuplicates(hosts)
	} else if set.Truncated {
		return nil, detail(ErrTargetInvalid, "the template matches more than %d hosts; narrow its target or run on a subset", limit)
	}
	return hosts, runnable(hosts)
}

// runnable 提交前的主机检查：至少一台，且每台都有可用的地址。
func runnable(hosts []Host) error {
	if len(hosts) == 0 {
		return detail(ErrTargetInvalid, "no target hosts: nothing matches within your data scope")
	}
	var problems []string
	for _, h := range hosts {
		if h.Problem != "" {
			problems = append(problems, h.Name+" ("+h.Problem+")")
		}
	}
	if n := len(problems); n > 0 {
		listed := problems[:min(n, problemsListed)]
		more := ""
		if n > len(listed) {
			more = ", ..."
		}
		return detail(ErrTargetInvalid, "%d host(s) have no usable address: %s%s", n, strings.Join(listed, ", "), more)
	}
	return nil
}

// recheckDuplicates 在收窄后的主机集合内重新判定地址重复（其余问题保持不变）。
func recheckDuplicates(hosts []Host) []Host {
	seen := make(map[string]struct{}, len(hosts))
	for i := range hosts {
		h := &hosts[i]
		if h.Problem == ProblemAddressDuplicate {
			h.Problem = ""
		}
		if h.Problem != "" {
			continue
		}
		if _, dup := seen[h.Address]; dup {
			h.Problem = ProblemAddressDuplicate
		}
		seen[h.Address] = struct{}{}
	}
	return hosts
}

// List 分页查询执行记录。
func (uc *ExecutionUsecase) List(ctx context.Context, filter ExecutionFilter, page, pageSize int64) ([]*Execution, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

// Get 查询执行详情。
func (uc *ExecutionUsecase) Get(ctx context.Context, id string) (*Execution, error) {
	e, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return e, uc.fillNames(ctx, []*Execution{e})
}

func (uc *ExecutionUsecase) fillNames(ctx context.Context, items []*Execution) error {
	if len(items) == 0 {
		return nil
	}
	users, runners := make([]string, 0, 2*len(items)), make([]string, 0, len(items))
	for _, e := range items {
		users, runners = append(users, e.Creator, e.CancelUser), append(runners, e.RunnerID)
	}
	userNames, err := uc.dir.UserNames(ctx, uniq(users))
	if err != nil {
		return err
	}
	runnerNames := map[string]string{}
	if runners = uniq(runners); len(runners) > 0 {
		if runnerNames, err = uc.runners.Names(ctx, runners); err != nil {
			return err
		}
	}
	var schedules, triggers []string
	for _, e := range items {
		switch {
		case e.TriggerRef == "":
		case e.Trigger == jc.TriggerSchedule:
			schedules = append(schedules, e.TriggerRef)
		default:
			triggers = append(triggers, e.TriggerRef)
		}
	}
	sourceNames := map[string]string{}
	if len(schedules)+len(triggers) > 0 {
		if sourceNames, err = uc.ext.Sources.SourceNames(ctx, uniq(schedules), uniq(triggers)); err != nil {
			return err
		}
	}
	for _, e := range items {
		e.CreatorName, e.CancelUserName, e.RunnerName = userNames[e.Creator], userNames[e.CancelUser], runnerNames[e.RunnerID]
		e.TriggerName = sourceNames[e.TriggerRef]
	}
	return nil
}

// ListHosts 分页查询一次执行的主机。
func (uc *ExecutionUsecase) ListHosts(ctx context.Context, id string, filter ExecutionHostFilter, page, pageSize int64) ([]*ExecutionHost, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = paging.DefaultPageSize
	}
	pageSize = min(pageSize, int64(maxHostPageSize))
	// 先确认执行存在：不存在的执行应当是 404，而不是一张空表。
	if _, err := uc.reader.Get(ctx, id); err != nil {
		return nil, 0, err
	}
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	return uc.reader.ListHosts(ctx, id, filter, page, pageSize)
}

// Log 增量读取执行日志：返回序号大于 afterSeq 的内容。
//
// 先读执行记录上的最大序号，再读日志块：引擎在同一事务里写日志块并推进最大序号，所以读到的最大序号
// 以内的块此时都已可见。日志被截断后，超限的块只推进序号、不落内容；因此读不到更多块时把游标
// 直接推到最大序号，避免客户端在一段不存在的序号上空转。
func (uc *ExecutionUsecase) Log(ctx context.Context, id string, afterSeq int64) (*LogPage, error) {
	e, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	page := &LogPage{LastSeq: afterSeq, Status: e.Status, Truncated: e.LogTruncated}
	if afterSeq >= e.LogSeq {
		return page, nil
	}
	content, lastSeq, err := uc.logs.ReadLog(ctx, id, afterSeq, logPageBytes)
	if err != nil {
		return nil, err
	}
	page.Content = content
	if len(content) >= logPageBytes && lastSeq < e.LogSeq {
		page.LastSeq, page.More = lastSeq, true
		return page, nil
	}
	page.LastSeq = max(lastSeq, e.LogSeq)
	return page, nil
}

// Cancel 取消执行：待审批的直接取消并撤回审批流程（切片时整组一并取消——审批是对整组的）；
// 排队中的立即取消；执行中的通知执行节点终止；结果未知（节点失联）的登记取消请求——作业可能仍在运行，
// 节点恢复连接后即终止。
func (uc *ExecutionUsecase) Cancel(ctx context.Context, userID, id string) (*Execution, error) {
	before, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := uc.writer.Cancel(ctx, id, userID); err != nil {
		return nil, err
	}
	if before.Status == jc.StatusPending && before.ApprovalInstanceID != "" {
		// 执行已取消（审批结果不会再改变它）；撤回流程只是为了不在审批人的待办里留下一条无效的任务，
		// 失败不影响取消的结果。以发起人的身份撤回：流程只允许发起人撤回。
		_ = uc.ext.Approvals.Cancel(ctx, before.ApprovalInstanceID, before.Creator)
	}
	return uc.Get(ctx, id)
}

// Stats 执行统计。
func (uc *ExecutionUsecase) Stats(ctx context.Context, days int32, timezone string) (*Stats, error) {
	if days <= 0 {
		days = 30
	}
	days = min(days, 366)
	if timezone == "" {
		timezone = defaultTimezone
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil || timezone == "Local" {
		return nil, detail(ErrScheduleInvalid, "unknown timezone %q", timezone)
	}
	now := time.Now().In(loc)
	// 从 (days-1) 天前的零点起算：「最近 7 天」含今天。
	since := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, -int(days-1))
	return uc.ext.Stats.Stats(ctx, since, timezone)
}
