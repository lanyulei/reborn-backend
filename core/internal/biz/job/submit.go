package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// defaultTimezone 未指定时区时的缺省值。
const defaultTimezone = "Asia/Shanghai"

// 快速执行的显示名长度（字符）。
const adhocNameLen = 48

// 需要审批的原因（Execution.ApprovalReason 的组成部分，亦作审批流程的 reason 变量）。
const (
	reasonTemplate = "作业模板要求审批"
	reasonAdhoc    = "快速执行须审批"
	reasonRisk     = "命中高危命令规则"
	reasonEnv      = "目标主机含受保护环境"
)

// submission 一次提交（作业模板或快速执行）在建单前的全部内容。
type submission struct {
	kind            int32
	name            string
	templateID      string
	templateVersion int32
	spec            jc.Spec
	runnerZone      string
	exclusive       bool
	// sliceSize 每片主机数；0 = 不切片。
	sliceSize  int
	hosts      []Host
	payload    []byte
	trigger    int32
	triggerRef string
	requestID  string
	riskHits   []jc.RiskHit
	// reasons 需要审批的原因；空 = 无需审批，直接排队。
	reasons []string
}

// approvalReasons 判定一次提交是否需要审批，返回原因（空 = 无需审批）。
// 预检模式（--check 不做实际变更）不需要审批；快速执行没有预检模式。
func approvalReasons(policy *Policy, kind int32, templateRequires, check bool, hosts []Host, hits []jc.RiskHit) []string {
	if check && kind == jc.KindPlaybook {
		return nil
	}
	var reasons []string
	if templateRequires {
		reasons = append(reasons, reasonTemplate)
	}
	if kind != jc.KindPlaybook && policy.AdhocApproval {
		reasons = append(reasons, reasonAdhoc)
	}
	var rules []string
	for _, h := range hits {
		if h.Action == jc.RiskApprove {
			rules = append(rules, h.Name)
		}
	}
	if len(rules) > 0 {
		reasons = append(reasons, reasonRisk+"（"+strings.Join(rules, "、")+"）")
	}
	if envs := protectedEnvs(policy, hosts); len(envs) > 0 {
		reasons = append(reasons, reasonEnv+"（"+strings.Join(envs, "、")+"）")
	}
	return reasons
}

// protectedEnvs 目标主机涉及的受保护环境（去重、排序）。
func protectedEnvs(policy *Policy, hosts []Host) []string {
	if len(policy.ProtectedEnvs) == 0 {
		return nil
	}
	var out []string
	for _, h := range hosts {
		if h.Env != "" && slices.Contains(policy.ProtectedEnvs, h.Env) && !slices.Contains(out, h.Env) {
			out = append(out, h.Env)
		}
	}
	sort.Strings(out)
	return out
}

// hostEnvs 目标主机的环境列表（去重、排序；审批流程的 envs 变量）。
func hostEnvs(hosts []Host) []any {
	var envs []string
	for _, h := range hosts {
		if h.Env != "" && !slices.Contains(envs, h.Env) {
			envs = append(envs, h.Env)
		}
	}
	sort.Strings(envs)
	out := make([]any, len(envs))
	for i, e := range envs {
		out[i] = e
	}
	return out
}

// submit 建单：按切片拆分、落库主机快照；需要审批时同事务发起审批流程（执行为「待审批」），
// 流程发起即通过的直接进入排队。返回第一条执行。
func (uc *ExecutionUsecase) submit(ctx context.Context, userID string, policy *Policy, sub *submission) (*Execution, error) {
	limit := policy.HostLimit()
	per := len(sub.hosts)
	if sub.sliceSize > 0 {
		per = min(sub.sliceSize, limit)
	}
	if per > limit {
		return nil, detail(ErrTargetInvalid, "%d hosts exceed the limit of %d per execution", len(sub.hosts), limit)
	}
	chunks := slices.Collect(slices.Chunk(sub.hosts, per))
	if len(chunks) > jc.MaxSlices {
		return nil, detail(ErrTargetInvalid, "%d hosts would need %d slices; at most %d slices of %d hosts are allowed", len(sub.hosts), len(chunks), jc.MaxSlices, per)
	}
	needApproval := len(sub.reasons) > 0
	if needApproval {
		ready, err := uc.approvalReady(ctx, policy)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, ErrApprovalUnavailable
		}
	}
	status, reason := jc.StatusQueued, ""
	if needApproval {
		status, reason = jc.StatusPending, truncateRunes(strings.Join(sub.reasons, "；"), 512)
	}
	groupID := ""
	if len(chunks) > 1 {
		id, err := idgen.NewID()
		if err != nil {
			return nil, err
		}
		groupID = id
	}
	items := make([]NewExecution, 0, len(chunks))
	for i, hosts := range chunks {
		id, err := idgen.NewID()
		if err != nil {
			return nil, err
		}
		e := &Execution{
			ID: id, Kind: sub.kind, TemplateID: sub.templateID, TemplateName: sub.name, TemplateVersion: sub.templateVersion,
			Trigger: sub.trigger, TriggerRef: sub.triggerRef, Creator: userID, Status: status, Spec: sub.spec,
			RunnerZone: sub.runnerZone, Exclusive: sub.exclusive, TimeoutS: sub.spec.Options.TimeoutS,
			HostTotal:      int32(len(hosts)), //nolint:gosec // 每片至多 jobcontract.MaxHosts 台
			ApprovalReason: reason, RiskHits: sub.riskHits, GroupID: groupID,
		}
		if groupID != "" {
			e.SliceNo, e.SliceTotal = int32(i+1), int32(len(chunks)) //nolint:gosec // 至多 MaxSlices 片
		}
		if i == 0 {
			e.RequestID = sub.requestID
		}
		items = append(items, NewExecution{Execution: e, Hosts: hosts, Payload: sub.payload})
	}
	first := items[0].Execution.ID
	err := uc.ext.Txs.WithinTx(ctx, func(tx Tx) error {
		id, created, err := uc.writer.CreateTx(ctx, tx, items)
		if err != nil {
			return err
		}
		first = id
		if !created || !needApproval {
			return nil // 幂等键命中：返回先前那条，不再发起审批
		}
		businessID := id
		if groupID != "" {
			businessID = groupID
		}
		out, err := uc.ext.Approvals.StartTx(ctx, tx, ApprovalStart{
			DefinitionKey: policy.ApprovalWorkflowKey, BusinessID: businessID, Initiator: userID,
			// 标题带上主机数：审批人在待办列表里就能看到影响面；完整内容（命令 / 脚本 / 主机清单）在执行详情页。
			Title: truncateRunes(fmt.Sprintf("作业审批：%s（%d 台主机）", sub.name, len(sub.hosts)), 200),
			Variables: map[string]any{
				"kind": sub.kind, "name": sub.name, "host_count": len(sub.hosts), "envs": hostEnvs(sub.hosts),
				"reason": reason, "check": sub.spec.Options.Check, "trigger": sub.trigger,
			},
		})
		if err != nil {
			return err
		}
		if err := uc.writer.SetApprovalTx(ctx, tx, businessID, out.InstanceID); err != nil {
			return err
		}
		if !out.Finished {
			return nil
		}
		// 流程发起即结束（节点全部自动通过或被跳过）：监听器触发时执行还没有记下流程实例，这里补上结果。
		reason := out.Reason
		if reason == "" {
			reason = rejectedReason(out.Approved)
		}
		_, err = uc.writer.ResolveApprovalTx(ctx, tx, businessID, out.Approved, reason)
		return err
	})
	if err != nil {
		return nil, err
	}
	return uc.Get(ctx, first)
}

// rejectedReason 审批未通过时记在执行上的原因。
func rejectedReason(approved bool) string {
	if approved {
		return ""
	}
	return "审批未通过"
}

// truncateRunes 按字符截断。
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// ---------------------------------------------------------------------------
// 快速执行
// ---------------------------------------------------------------------------

// prepareAdhoc 规整并校验快速执行的内容，返回其显示名、要匹配高危规则的文本与文件内容。
// withFile 为假时不读取文件（提交前检查）。
func (uc *ExecutionUsecase) prepareAdhoc(ctx context.Context, userID string, in *AdhocInput, withFile bool) (name, riskText string, payload []byte, err error) {
	a := &in.Adhoc
	switch in.Kind {
	case jc.KindCommand:
		*a = jc.Adhoc{Command: strings.TrimSpace(a.Command)}
		name, riskText = "命令："+firstLine(a.Command), a.Command
	case jc.KindScript:
		args := strings.TrimSpace(a.ScriptArgs)
		if a.ScriptID != "" {
			s, err := uc.ext.Scripts.Get(ctx, a.ScriptID)
			if err != nil {
				return "", "", nil, err
			}
			*a = jc.Adhoc{Script: s.Content, Interpreter: s.Interpreter, ScriptID: s.ID, ScriptName: s.Name, ScriptVersion: s.Version}
			name = "脚本：" + s.Name
		} else {
			*a = jc.Adhoc{Script: a.Script, Interpreter: a.Interpreter}
			name = "脚本：" + firstLine(strings.TrimPrefix(strings.TrimSpace(a.Script), "#!"))
		}
		a.Script = strings.ReplaceAll(a.Script, "\r\n", "\n")
		a.ScriptArgs = args
		riskText = a.Script + "\n" + args
	case jc.KindFile:
		mode := a.Mode
		if mode == "" {
			mode = "0644"
		}
		*a = jc.Adhoc{FileName: strings.TrimSpace(a.FileName), Dest: path.Clean(strings.TrimSpace(a.Dest)), Mode: mode}
		if withFile || in.FileID != "" {
			f, err := uc.ext.Files.Read(ctx, userID, in.FileID)
			if err != nil {
				return "", "", nil, err
			}
			if len(f.Data) > jc.MaxFileBytes {
				return "", "", nil, detail(ErrAdhocInvalid, "file is larger than %dMB", jc.MaxFileBytes>>20)
			}
			if a.FileName == "" {
				a.FileName = f.Name
			}
			sum := sha256.Sum256(f.Data)
			a.FileSize, a.FileSHA256, payload = int64(len(f.Data)), hex.EncodeToString(sum[:]), f.Data
		} else if a.FileName == "" {
			a.FileName = "file" // 仅检查目标与审批：文件名占位
		}
		name = "文件：" + a.FileName + " → " + a.Dest
		// 分发的文本文件（脚本、配置）同样按高危规则检查：否则被拦截的命令可以先当文件发下去再执行。
		riskText = a.Dest + "/" + a.FileName
		// 不以「是合法 UTF-8」为前提：否则在脚本里塞一个非法字节就能整份绕过检查，而 shell 照样执行它。
		riskText += "\n" + strings.ToValidUTF8(string(payload), "?")
	default:
		return "", "", nil, detail(ErrAdhocInvalid, "unknown quick execution kind")
	}
	if err := a.Validate(in.Kind); err != nil {
		return "", "", nil, detail(ErrAdhocInvalid, "%s", err.Error())
	}
	return truncateRunes(name, adhocNameLen), riskText, payload, nil
}

// firstLine 取文本的第一行（去掉首尾空白）。
func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(line)
}

// adhocPreflight 快速执行的提交前检查：内容校验、目标主机、高危规则、审批判定。
func (uc *ExecutionUsecase) adhocPreflight(ctx context.Context, userID string, in *AdhocInput, withFile bool) (*Preflight, *Policy, string, []byte, error) {
	name, riskText, payload, err := uc.prepareAdhoc(ctx, userID, in, withFile)
	if err != nil {
		return nil, nil, "", nil, err
	}
	policy, err := uc.ext.Policies.Get(ctx)
	if err != nil {
		return nil, nil, "", nil, err
	}
	limit := policy.HostLimit()
	set, err := uc.targets.ResolveN(ctx, userID, in.Target, limit)
	if err != nil {
		return nil, nil, "", nil, err
	}
	pf := &Preflight{HostSet: set, MaxHosts: limit}
	if riskText != "" {
		rules, err := uc.ext.Rules.List(ctx)
		if err != nil {
			return nil, nil, "", nil, err
		}
		pf.RiskHits = matchRisk(rules, riskText)
		pf.Blocked = slices.ContainsFunc(pf.RiskHits, func(h jc.RiskHit) bool { return h.Action == jc.RiskBlock })
	}
	pf.ApprovalReasons = approvalReasons(policy, in.Kind, false, false, set.Hosts, pf.RiskHits)
	if pf.ApprovalReady, err = uc.approvalReady(ctx, policy); err != nil {
		return nil, nil, "", nil, err
	}
	return pf, policy, name, payload, nil
}

// CheckAdhoc 提交前检查一次快速执行；不落库。
func (uc *ExecutionUsecase) CheckAdhoc(ctx context.Context, userID string, in AdhocInput) (*Preflight, error) {
	pf, _, _, _, err := uc.adhocPreflight(ctx, userID, &in, false)
	return pf, err
}

// SubmitAdhoc 快速执行：在选中的主机上执行一条命令、一段脚本或分发一个文件。
// 提交即固化内容（脚本取自脚本库时是当时的版本；文件内容存入执行记录）与主机快照。
func (uc *ExecutionUsecase) SubmitAdhoc(ctx context.Context, userID string, in AdhocInput) (*Execution, error) {
	if in.Kind == jc.KindFile && in.FileID == "" {
		return nil, detail(ErrAdhocInvalid, "upload the file to distribute first")
	}
	in.Options.Check, in.Options.Diff, in.Options.Verbosity = false, false, 0
	if err := in.Options.Normalize(); err != nil {
		return nil, detail(ErrAdhocInvalid, "%s", err.Error())
	}
	if !jc.ValidZone(in.RunnerZone) {
		return nil, detail(ErrAdhocInvalid, "invalid runner zone")
	}
	credential, err := uc.credentials.Get(ctx, in.CredentialID)
	if err != nil {
		return nil, err
	}
	pf, policy, name, payload, err := uc.adhocPreflight(ctx, userID, &in, true)
	if err != nil {
		return nil, err
	}
	if pf.Blocked {
		var names []string
		for _, h := range pf.RiskHits {
			if h.Action == jc.RiskBlock {
				names = append(names, h.Name)
			}
		}
		return nil, detail(ErrRiskBlocked, "blocked by high-risk command rule(s): %s", strings.Join(names, ", "))
	}
	if pf.Truncated {
		return nil, detail(ErrTargetInvalid, "the target matches more than %d hosts; narrow it", pf.MaxHosts)
	}
	if err := runnable(pf.Hosts); err != nil {
		return nil, err
	}
	target := in.Target
	adhoc := in.Adhoc
	return uc.submit(ctx, userID, policy, &submission{
		kind: in.Kind, name: name,
		spec: jc.Spec{
			Kind: in.Kind, Adhoc: &adhoc, Target: &target, CredentialID: credential.ID, CredentialName: credential.Name,
			ExtraVars: map[string]any{}, Options: in.Options,
		},
		runnerZone: in.RunnerZone, hosts: pf.Hosts, payload: payload,
		trigger: jc.TriggerManual, requestID: in.RequestID, riskHits: pf.RiskHits, reasons: pf.ApprovalReasons,
	})
}

// ApprovalSyncer 审批流程结束时把结果落到执行上。独立于 ExecutionUsecase：它被登记为流程引擎的监听器，
// 而 ExecutionUsecase 经 Approvals 依赖流程引擎——合在一起会成环。
type ApprovalSyncer struct {
	writer   ExecutionWriter
	evidence ApprovalEvidenceReader
}

// NewApprovalSyncer new an ApprovalSyncer。
func NewApprovalSyncer(writer ExecutionWriter, evidence ApprovalEvidenceReader) *ApprovalSyncer {
	return &ApprovalSyncer{writer: writer, evidence: evidence}
}

// Resolve 在流程引擎的事务内落审批结果：通过 → 排队中；驳回 / 撤回 / 终止 → 审批未通过。
// 只动仍是待审批的执行（已被取消的不受影响）。submitter 是执行的提交人（审批流程的发起人）。
//
// 「通过」不等于有人审批过：流程里的审批节点可以设置成处理人为空时自动跳过，流程会以通过结束。
// 所以落批准之前先看审批证据（ApprovalReviewed）：确有审批环节被跳过、又没有任何人点过同意 →
// 按未通过处理，作业不执行。没有证据来源、证据读不出来时同样不批准。
func (s *ApprovalSyncer) Resolve(ctx context.Context, tx Tx, businessID string, approved bool, submitter, reason string) error {
	if approved {
		reviewed, err := s.reviewed(ctx, tx, businessID, submitter)
		if err != nil {
			return err
		}
		if !reviewed {
			approved, reason = false, MsgApprovalUnreviewed
		}
	}
	if reason == "" {
		reason = rejectedReason(approved)
	}
	if _, err := s.writer.ResolveApprovalTx(ctx, tx, businessID, approved, truncateRunes(reason, jc.MaxErrorLen)); err != nil {
		return fmt.Errorf("resolve job approval: %w", err)
	}
	return nil
}

// reviewed 这一次「通过」能否落为批准（见 ApprovalReviewed）。
func (s *ApprovalSyncer) reviewed(ctx context.Context, tx Tx, businessID, submitter string) (bool, error) {
	if s.evidence == nil {
		return false, nil
	}
	ev, err := s.evidence.ApprovalEvidenceTx(ctx, tx, businessID, submitter)
	if err != nil {
		return false, fmt.Errorf("read job approval evidence: %w", err)
	}
	return ApprovalReviewed(ev), nil
}
