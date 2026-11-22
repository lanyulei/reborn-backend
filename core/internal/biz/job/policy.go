package biz

import (
	"context"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 上限。
const (
	// maxRiskRules 高危命令规则的条数上限。
	maxRiskRules = 200
	// riskMatchLen 命中片段的展示长度（字符）。
	riskMatchLen = 120
)

// compileRisk 编译规则的正则。Go 的 regexp 是 RE2：匹配耗时与输入长度成线性，不存在回溯爆炸，
// 用户提供的正则不会拖垮服务。
func compileRisk(pattern string) (*regexp.Regexp, error) {
	return regexp.Compile(pattern)
}

// riskRegexps 已编译规则的进程内缓存，键 = 规则 ID@版本（规则改动版本号递增，旧版本条目失效）。每次提交都
// 要拿全部规则匹配一遍，不缓存就是每条规则每次提交编译一次。条目数超过两倍规则上限时整体清空（只是缓存）。
var riskRegexps = struct {
	sync.Mutex
	m map[string]*regexp.Regexp
}{m: map[string]*regexp.Regexp{}}

// riskRegexp 取规则的已编译正则（没有 ID 的草稿不缓存）。
func riskRegexp(r *RiskRule) (*regexp.Regexp, error) {
	if r.ID == "" {
		return compileRisk(r.Pattern)
	}
	key := r.ID + "@" + strconv.Itoa(int(r.Version))
	riskRegexps.Lock()
	re, ok := riskRegexps.m[key]
	riskRegexps.Unlock()
	if ok {
		return re, nil
	}
	re, err := compileRisk(r.Pattern)
	if err != nil {
		return nil, err
	}
	riskRegexps.Lock()
	if len(riskRegexps.m) >= 2*maxRiskRules {
		clear(riskRegexps.m)
	}
	riskRegexps.m[key] = re
	riskRegexps.Unlock()
	return re, nil
}

// matchRisk 用启用的规则匹配一段命令 / 脚本内容，返回命中项（拦截在前、须审批次之、提醒最后）。
// 编译不过的存量规则跳过（保存时已校验，这里不让一条坏规则挡住全部提交）。
func matchRisk(rules []*RiskRule, content string) []jc.RiskHit {
	var hits []jc.RiskHit
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		re, err := riskRegexp(r)
		if err != nil {
			continue
		}
		loc := re.FindStringIndex(content)
		if loc == nil {
			continue
		}
		hits = append(hits, jc.RiskHit{
			RuleID: r.ID, Name: r.Name, Action: r.Action, Description: r.Description,
			Match: truncateRunes(printable(strings.TrimSpace(content[loc[0]:loc[1]]), riskMatchLen), riskMatchLen),
		})
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Action < hits[j].Action })
	return hits
}

// ScriptUsecase 脚本库用例。
type ScriptUsecase struct {
	repo ScriptRepo
	dir  Directory
}

// NewScriptUsecase new a ScriptUsecase.
func NewScriptUsecase(repo ScriptRepo, dir Directory) *ScriptUsecase {
	return &ScriptUsecase{repo: repo, dir: dir}
}

// List 分页查询脚本（不含内容）。
func (uc *ScriptUsecase) List(ctx context.Context, filter ScriptFilter, page, pageSize int64) ([]*Script, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.repo.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

// Get 查询脚本详情（含内容）。
func (uc *ScriptUsecase) Get(ctx context.Context, id string) (*Script, error) {
	s, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return s, uc.fillNames(ctx, []*Script{s})
}

func (uc *ScriptUsecase) fillNames(ctx context.Context, items []*Script) error {
	ids := make([]string, 0, len(items))
	for _, s := range items {
		ids = append(ids, s.UpdateUser)
	}
	if ids = uniq(ids); len(ids) == 0 {
		return nil
	}
	names, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, s := range items {
		s.UpdateUserName = names[s.UpdateUser]
	}
	return nil
}

func normalizeScript(s *Script) error {
	s.Name, s.Description = strings.TrimSpace(s.Name), strings.TrimSpace(s.Description)
	s.Content = strings.ReplaceAll(s.Content, "\r\n", "\n")
	switch {
	case s.Name == "":
		return detail(ErrScriptInvalid, "name must not be blank")
	case !jc.ValidInterpreter(s.Interpreter):
		return detail(ErrScriptInvalid, "unknown interpreter")
	case strings.TrimSpace(s.Content) == "" || len(s.Content) > jc.MaxScriptBytes || strings.ContainsRune(s.Content, 0):
		return detail(ErrScriptInvalid, "script content must be 1 byte to 256KB")
	}
	return nil
}

// Create 新建脚本。
func (uc *ScriptUsecase) Create(ctx context.Context, userID string, s *Script) (*Script, error) {
	if err := normalizeScript(s); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	s.ID = id
	if err := uc.repo.Write(ctx, OpCreate, s, 0, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑脚本（整体替换）；已提交的执行不受影响。
func (uc *ScriptUsecase) Update(ctx context.Context, userID string, s *Script, version int32) (*Script, error) {
	if err := normalizeScript(s); err != nil {
		return nil, err
	}
	if err := uc.repo.Write(ctx, OpUpdate, s, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, s.ID)
}

// Delete 删除脚本。
func (uc *ScriptUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.repo.Write(ctx, OpDelete, &Script{ID: id}, 0, userID)
}

// PolicyUsecase 安全策略用例：高危命令规则与全局策略。
type PolicyUsecase struct {
	rules     RiskRuleRepo
	policies  PolicyRepo
	approvals Approvals
	dir       Directory
}

// NewPolicyUsecase new a PolicyUsecase.
func NewPolicyUsecase(rules RiskRuleRepo, policies PolicyRepo, approvals Approvals, dir Directory) *PolicyUsecase {
	return &PolicyUsecase{rules: rules, policies: policies, approvals: approvals, dir: dir}
}

// ListRules 全部高危命令规则。
func (uc *PolicyUsecase) ListRules(ctx context.Context) ([]*RiskRule, error) {
	items, err := uc.rules.List(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(items))
	for _, r := range items {
		ids = append(ids, r.UpdateUser)
	}
	names, err := uc.dir.UserNames(ctx, uniq(ids))
	if err != nil {
		return nil, err
	}
	for _, r := range items {
		r.UpdateUserName = names[r.UpdateUser]
	}
	return items, nil
}

func normalizeRule(r *RiskRule) error {
	r.Name, r.Description = strings.TrimSpace(r.Name), strings.TrimSpace(r.Description)
	switch {
	case r.Name == "":
		return detail(ErrRiskRuleInvalid, "name must not be blank")
	case r.Action != jc.RiskBlock && r.Action != jc.RiskApprove && r.Action != jc.RiskWarn:
		return detail(ErrRiskRuleInvalid, "unknown action")
	case strings.TrimSpace(r.Pattern) == "":
		return detail(ErrRiskRuleInvalid, "pattern must not be blank")
	}
	re, err := compileRisk(r.Pattern)
	if err != nil {
		return detail(ErrRiskRuleInvalid, "pattern is not a valid regular expression: %s", err.Error())
	}
	// 能匹配空串的规则会命中一切内容（如 a* 或 x|）：几乎必然是写错了。
	if re.MatchString("") {
		return detail(ErrRiskRuleInvalid, "pattern matches the empty string and would hit everything")
	}
	return nil
}

// CreateRule 新建高危命令规则。
func (uc *PolicyUsecase) CreateRule(ctx context.Context, userID string, r *RiskRule) (*RiskRule, error) {
	if err := normalizeRule(r); err != nil {
		return nil, err
	}
	all, err := uc.rules.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(all) >= maxRiskRules {
		return nil, detail(ErrRiskRuleInvalid, "at most %d rules", maxRiskRules)
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	r.ID = id
	if err := uc.rules.Write(ctx, OpCreate, r, 0, userID); err != nil {
		return nil, err
	}
	return uc.rules.Get(ctx, id)
}

// UpdateRule 编辑高危命令规则（整体替换）。
func (uc *PolicyUsecase) UpdateRule(ctx context.Context, userID string, r *RiskRule, version int32) (*RiskRule, error) {
	if err := normalizeRule(r); err != nil {
		return nil, err
	}
	if err := uc.rules.Write(ctx, OpUpdate, r, version, userID); err != nil {
		return nil, err
	}
	return uc.rules.Get(ctx, r.ID)
}

// DeleteRule 删除高危命令规则。
func (uc *PolicyUsecase) DeleteRule(ctx context.Context, userID, id string) error {
	return uc.rules.Write(ctx, OpDelete, &RiskRule{ID: id}, 0, userID)
}

// TestRules 试匹配：draft 非空时只用它，否则用全部启用的规则。
func (uc *PolicyUsecase) TestRules(ctx context.Context, content string, draft *RiskRule) ([]jc.RiskHit, error) {
	if draft != nil {
		if draft.Name == "" {
			draft.Name = "draft"
		}
		if err := normalizeRule(draft); err != nil {
			return nil, err
		}
		draft.Enabled = true
		return matchRisk([]*RiskRule{draft}, content), nil
	}
	rules, err := uc.rules.List(ctx)
	if err != nil {
		return nil, err
	}
	return matchRisk(rules, content), nil
}

// GetPolicy 查询全局策略（带审批流程的名称与当前是否可发起）。
func (uc *PolicyUsecase) GetPolicy(ctx context.Context) (*Policy, error) {
	p, err := uc.policies.Get(ctx)
	if err != nil {
		return nil, err
	}
	if p.ApprovalWorkflowKey != "" {
		if p.ApprovalWorkflowName, _, p.ApprovalWorkflowReady, err = uc.approvals.Startable(ctx, p.ApprovalWorkflowKey); err != nil {
			return nil, err
		}
	}
	names, err := uc.dir.UserNames(ctx, uniq([]string{p.UpdateUser}))
	if err != nil {
		return nil, err
	}
	p.UpdateUserName = names[p.UpdateUser]
	return p, nil
}

// UpdatePolicy 更新全局策略。审批流程须存在且当前可发起：不让策略指向一个发不起来的流程
// （那样所有需要审批的提交都会被拒绝）。
func (uc *PolicyUsecase) UpdatePolicy(ctx context.Context, userID string, p *Policy) (*Policy, error) {
	envs := make([]string, 0, len(p.ProtectedEnvs))
	for _, e := range p.ProtectedEnvs {
		if e = strings.TrimSpace(e); e != "" && !slices.Contains(envs, e) {
			envs = append(envs, e)
		}
	}
	p.ProtectedEnvs = envs
	if p.MaxHosts < 0 || p.MaxHosts > jc.MaxHosts {
		return nil, detail(ErrPolicyInvalid, "max hosts must be between 0 and %d", jc.MaxHosts)
	}
	if p.ApprovalWorkflowKey != "" {
		_, exists, startable, err := uc.approvals.Startable(ctx, p.ApprovalWorkflowKey)
		if err != nil {
			return nil, err
		}
		switch {
		case !exists:
			return nil, detail(ErrPolicyInvalid, "approval workflow %q does not exist", p.ApprovalWorkflowKey)
		case !startable:
			return nil, detail(ErrPolicyInvalid, "approval workflow %q is not enabled or not published", p.ApprovalWorkflowKey)
		}
	}
	if err := uc.policies.Update(ctx, p, userID); err != nil {
		return nil, err
	}
	return uc.GetPolicy(ctx)
}
