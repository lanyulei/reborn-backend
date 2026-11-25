package biz

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 触发规则（功能设计 §3.2）：告警满足什么条件、怎样归并成故障。匹配与归并键计算是纯函数，关联器与试算共用。

var ruleKeyRE = regexp.MustCompile(`^(business|group|rule|ci|label:[a-zA-Z_][a-zA-Z0-9_]*)$`)

// 试算上限。
const (
	testScanLimit = 2000
	testGroups    = 50
)

// RuleUsecase 触发规则。
type RuleUsecase struct {
	d *Deps
}

// NewRuleUsecase new a RuleUsecase.
func NewRuleUsecase(d *Deps) *RuleUsecase { return &RuleUsecase{d: d} }

// RuleView 规则及其展示信息。
type RuleView struct {
	*Rule
	BusinessName   string
	PlaybookName   string
	ScheduleName   string
	CommanderName  string
	UpdateUserName string
	IncidentCount  int32
}

// compiledRule 已编译的规则（关联器缓存）。
type compiledRule struct {
	rule     *Rule
	matchers MatcherSet
}

// validateRule 不依赖外部数据的校验与归一化。
func (uc *RuleUsecase) validateRule(ctx context.Context, s *RuleSpec) (MatcherSet, error) {
	s.Name, s.Description = cleanText(s.Name), cleanText(s.Description)
	if s.Name == "" || len([]rune(s.Name)) > 128 {
		return nil, detail(ErrRuleInvalid, "name must be 1-128 characters")
	}
	if s.Match.MinSeverity < 0 || s.Match.MinSeverity > 4 {
		return nil, detail(ErrRuleInvalid, "match.min_severity must be 0-4")
	}
	s.Match.GroupIDs, s.Match.RuleIDs, s.Match.ModelCodes = uniq(s.Match.GroupIDs), uniq(s.Match.RuleIDs), uniq(s.Match.ModelCodes)
	matchers, err := uc.d.Matchers.Compile(s.Match.Matchers)
	if err != nil {
		return nil, detail(ErrRuleInvalid, "matchers: %v", err)
	}
	for _, k := range s.Correlation.Keys {
		if !ruleKeyRE.MatchString(k) {
			return nil, detail(ErrRuleInvalid, "unknown correlation key %q", k)
		}
	}
	s.Correlation.Keys = uniq(s.Correlation.Keys)
	if len(s.Correlation.Keys) > 5 {
		return nil, detail(ErrRuleInvalid, "at most 5 correlation keys")
	}
	if s.Correlation.MinAlerts < 1 {
		s.Correlation.MinAlerts = 1
	}
	if s.Correlation.MinCIs < 1 {
		s.Correlation.MinCIs = 1
	}
	if s.Correlation.WindowM < 0 || s.Correlation.WindowM > 1440 {
		return nil, detail(ErrRuleInvalid, "correlation.window_m must be 0-1440")
	}
	if s.Incident.Severity != 0 && !ValidSeverity(s.Incident.Severity) {
		return nil, detail(ErrRuleInvalid, "incident.severity must be 0-4")
	}
	s.Incident.TitleTemplate, s.Incident.BusinessLabel = cleanText(s.Incident.TitleTemplate), strings.TrimSpace(s.Incident.BusinessLabel)
	if s.Incident.BusinessID != "" {
		if _, err := uc.d.Businesses.Get(ctx, s.Incident.BusinessID); err != nil {
			return nil, err
		}
	}
	for _, g := range s.Response.Groups {
		if !strings.HasPrefix(g, "$") {
			return nil, detail(ErrRuleInvalid, "group identifier must start with $")
		}
	}
	s.Response.Groups, s.Response.ResponderUserIDs = uniq(s.Response.Groups), uniq(s.Response.ResponderUserIDs)
	if len(s.Response.Groups) > 10 || len(s.Response.ResponderUserIDs) > 20 {
		return nil, detail(ErrRuleInvalid, "too many groups or responders")
	}
	if ids := uniq(append(slices.Clone(s.Response.ResponderUserIDs), s.Response.CommanderUserID)); len(ids) > 0 {
		active, err := uc.d.Dir.ActiveUserIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		if len(active) != len(ids) {
			return nil, detail(ErrRuleInvalid, "some users are not active")
		}
	}
	if s.Response.CommanderScheduleID != "" {
		names, err := uc.d.Schedules.ScheduleNames(ctx, []string{s.Response.CommanderScheduleID})
		if err != nil {
			return nil, err
		}
		if len(names) != 1 {
			return nil, detail(ErrRuleInvalid, "commander schedule does not exist or is disabled")
		}
	}
	if s.Action.PlaybookMode < 0 || s.Action.PlaybookMode > PlaybookAuto || s.Action.AutoResolve < 0 || s.Action.AutoResolve > AutoResolveResolve {
		return nil, detail(ErrRuleInvalid, "action modes out of range")
	}
	if s.Action.PlaybookID != "" {
		pb, err := uc.d.Playbooks.Get(ctx, s.Action.PlaybookID)
		if err != nil {
			return nil, err
		}
		if s.Action.PlaybookMode >= PlaybookPropose && len(pb.Spec.Steps) == 0 {
			return nil, detail(ErrRuleInvalid, "the playbook has no executable step")
		}
	} else {
		s.Action.PlaybookMode = 0
	}
	return matchers, nil
}

func (uc *RuleUsecase) views(ctx context.Context, items []*Rule) ([]*RuleView, error) {
	var users, schedules, businesses, playbooks []string
	for _, r := range items {
		users = append(users, r.UpdateUser, r.Spec.Response.CommanderUserID)
		schedules = append(schedules, r.Spec.Response.CommanderScheduleID)
		businesses = append(businesses, r.Spec.Incident.BusinessID)
		playbooks = append(playbooks, r.Spec.Action.PlaybookID)
	}
	userNames, err := uc.d.Dir.UserNames(ctx, uniq(users))
	if err != nil {
		return nil, err
	}
	scheduleNames, err := uc.d.Schedules.ScheduleNames(ctx, uniq(schedules))
	if err != nil {
		return nil, err
	}
	businessNames, playbookNames := map[string]string{}, map[string]string{}
	for _, id := range uniq(businesses) {
		if b, err := uc.d.Businesses.Get(ctx, id); err == nil {
			businessNames[id] = b.Name
		}
	}
	if ids := uniq(playbooks); len(ids) > 0 {
		pbs, err := uc.d.Playbooks.ListByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for _, p := range pbs {
			playbookNames[p.ID] = p.Spec.Name
		}
	}
	since := time.Now().Add(-30 * 24 * time.Hour)
	out := make([]*RuleView, 0, len(items))
	for _, r := range items {
		n, err := uc.d.Incidents.CountRecentByRule(ctx, r.ID, since)
		if err != nil {
			return nil, err
		}
		out = append(out, &RuleView{
			Rule: r, BusinessName: businessNames[r.Spec.Incident.BusinessID], PlaybookName: playbookNames[r.Spec.Action.PlaybookID],
			ScheduleName: scheduleNames[r.Spec.Response.CommanderScheduleID], CommanderName: userNames[r.Spec.Response.CommanderUserID],
			UpdateUserName: userNames[r.UpdateUser], IncidentCount: i32(n),
		})
	}
	return out, nil
}

// List 分页。
func (uc *RuleUsecase) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*RuleView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Rules.List(ctx, cleanText(keyword), enabled, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, items)
	return views, total, err
}

// Get 详情。
func (uc *RuleUsecase) Get(ctx context.Context, id string) (*RuleView, error) {
	r, err := uc.d.Rules.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, []*Rule{r})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// Create 新建。
func (uc *RuleUsecase) Create(ctx context.Context, userID string, spec RuleSpec) (*RuleView, error) {
	if _, err := uc.validateRule(ctx, &spec); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	if err := uc.d.Rules.Create(ctx, &Rule{ID: id, Spec: spec}, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑。
func (uc *RuleUsecase) Update(ctx context.Context, userID, id string, spec RuleSpec, version int32) (*RuleView, error) {
	if _, err := uc.validateRule(ctx, &spec); err != nil {
		return nil, err
	}
	if err := uc.d.Rules.Update(ctx, &Rule{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Delete 删除。
func (uc *RuleUsecase) Delete(ctx context.Context, userID, id string) error {
	r, err := uc.d.Rules.Get(ctx, id)
	if err != nil {
		return err
	}
	return uc.d.Rules.Delete(ctx, id, r.Version, userID)
}

// ---------------------------------------------------------------------------
// 匹配与归并（纯函数）
// ---------------------------------------------------------------------------

// matchContext 匹配所需的外部数据：告警关联资源的模型。
type matchContext struct {
	ciModel map[string]string
}

// matches 告警是否满足规则的匹配条件。
func (c *compiledRule) matches(a *AlertRef, mc matchContext) bool {
	m := c.rule.Spec.Match
	if m.MinSeverity > 0 && a.Severity < m.MinSeverity {
		return false
	}
	if len(m.GroupIDs) > 0 && !slices.Contains(m.GroupIDs, a.GroupID) {
		return false
	}
	if len(m.RuleIDs) > 0 && !slices.Contains(m.RuleIDs, a.RuleID) {
		return false
	}
	if len(m.ModelCodes) > 0 {
		if a.CIID == "" || !slices.Contains(m.ModelCodes, mc.ciModel[a.CIID]) {
			return false
		}
	}
	return c.matchers == nil || c.matchers.Match(a.Labels)
}

// correlationKey 告警在规则下的归并键（空键列表 = 全部归并到一个故障）。businessID 为按标签匹配到的业务。
func correlationKey(spec *RuleSpec, a *AlertRef, businessID string, upstream []string) string {
	if len(spec.Correlation.Keys) == 0 {
		return "*"
	}
	parts := make([]string, 0, len(spec.Correlation.Keys))
	for _, k := range spec.Correlation.Keys {
		switch {
		case k == "business":
			parts = append(parts, "b="+businessID)
		case k == "group":
			parts = append(parts, "g="+a.GroupID)
		case k == "rule":
			parts = append(parts, "r="+a.RuleID)
		case k == "ci":
			ci := a.CIID
			// 有上游时按最顶层上游归并（上游故障把下游一起带上）。
			if len(upstream) > 0 {
				ci = upstream[len(upstream)-1]
			}
			parts = append(parts, "c="+ci)
		case strings.HasPrefix(k, "label:"):
			parts = append(parts, "l:"+k[6:]+"="+a.Labels[k[6:]])
		}
	}
	// 按字节截到列宽（varchar(255)）且不切开字符：按字节硬切会把中文标签值切成非法 UTF-8，
	// 落库报 22021，关联器同样卡在这条信号上。
	return truncateBytes(strings.Join(parts, "|"), 255)
}

// severityFor 规则开出故障的等级：固定或按告警级别映射（致命→S2、严重→S3、其余→S4）。
func severityFor(spec *RuleSpec, alerts []*AlertRef) int32 {
	if spec.Incident.Severity != 0 {
		return spec.Incident.Severity
	}
	maxSev := int32(0)
	for _, a := range alerts {
		maxSev = max(maxSev, a.Severity)
	}
	switch {
	case maxSev >= 4:
		return SeverityS2
	case maxSev == 3:
		return SeverityS3
	}
	return SeverityS4
}

// renderTitle 标题模板：{{rule}} {{labels.xxx}} {{business}} {{count}}。
func renderTitle(tpl string, a *AlertRef, business string, count int) string {
	if tpl == "" {
		tpl = "{{rule}}"
		if business != "" {
			tpl = "{{business}}：{{rule}}"
		}
	}
	out := tpl
	out = strings.ReplaceAll(out, "{{rule}}", a.RuleName)
	out = strings.ReplaceAll(out, "{{business}}", business)
	out = strings.ReplaceAll(out, "{{count}}", fmt.Sprint(count))
	for k, v := range a.Labels {
		out = strings.ReplaceAll(out, "{{labels."+k+"}}", v)
	}
	// 未命中的标签占位符清掉。
	for {
		i := strings.Index(out, "{{labels.")
		if i < 0 {
			break
		}
		j := strings.Index(out[i:], "}}")
		if j < 0 {
			break
		}
		out = out[:i] + out[i+j+2:]
	}
	out = cleanText(out)
	if out == "" {
		out = a.RuleName
	}
	return truncate(out, 255)
}

// ruleCreateInput 按规则属性组装开故障入参（关联器 / 候选转正共用）。
func (uc *IncidentUsecase) ruleCreateInput(ctx context.Context, rule *Rule, key string, alerts []*AlertRef) CreateInput {
	spec := &rule.Spec
	businessID, businessName := spec.Incident.BusinessID, ""
	if businessID == "" && spec.Incident.BusinessLabel != "" {
		if all, err := uc.d.Businesses.ListAll(ctx); err == nil {
			if b := matchBusiness(all, alerts[0].Labels[spec.Incident.BusinessLabel]); b != nil {
				businessID = b.ID
			}
		}
	}
	if businessID != "" {
		if b, err := uc.d.Businesses.Get(ctx, businessID); err == nil {
			businessName = b.Name
		}
	}
	in := CreateInput{
		Title: renderTitle(spec.Incident.TitleTemplate, alerts[0], businessName, len(alerts)), Severity: severityFor(spec, alerts),
		BusinessID: businessID, AlertRefs: alerts, Groups: spec.Response.Groups, IsDrill: spec.Incident.IsDrill,
		Source: SourceRule, SourceRef: rule.ID, RuleID: rule.ID, Correlation: key, LinkMethod: LinkRule, AISummary: spec.Action.AISummary, Agent: spec.Action.Agent,
		response: responseSpec{
			commanderUserID: spec.Response.CommanderUserID, commanderSchedule: spec.Response.CommanderScheduleID,
			responderBackup: spec.Response.ResponderBackup, responderGroupID: spec.Response.ResponderOncallGroupID,
			responderCIOwner: spec.Response.ResponderCIOwner, responderUserIDs: spec.Response.ResponderUserIDs,
		},
	}
	for _, a := range alerts {
		in.AlertIDs = append(in.AlertIDs, a.ID)
	}
	if spec.Action.PlaybookID != "" && spec.Action.PlaybookMode >= PlaybookPropose {
		in.Playbook = &playbookTrigger{PlaybookID: spec.Action.PlaybookID, Mode: spec.Action.PlaybookMode}
	}
	return in
}

// ---------------------------------------------------------------------------
// 试算
// ---------------------------------------------------------------------------

// TestGroup 试算出的一个归并组。
type TestGroup struct {
	Key       string
	Alerts    []*AlertRef
	CIs       int
	WouldOpen bool
	Title     string
	Severity  int32
}

// TestResult 试算结果。
type TestResult struct {
	Scanned int
	Matched int
	Groups  []*TestGroup
}

// Test 用近期历史告警试算规则（不落库）。
func (uc *RuleUsecase) Test(ctx context.Context, spec RuleSpec, hours int) (*TestResult, error) {
	matchers, err := uc.validateRule(ctx, &spec)
	if err != nil {
		return nil, err
	}
	if hours <= 0 {
		hours = 24
	}
	alerts, err := uc.d.Alerts.RecentFiring(ctx, time.Now().Add(-time.Duration(hours)*time.Hour), testScanLimit)
	if err != nil {
		return nil, err
	}
	cr := &compiledRule{rule: &Rule{Spec: spec}, matchers: matchers}
	mc, err := uc.matchContextFor(ctx, alerts)
	if err != nil {
		return nil, err
	}
	all, err := uc.d.Businesses.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	res := &TestResult{Scanned: len(alerts)}
	groups := map[string]*TestGroup{}
	for _, a := range alerts {
		if !cr.matches(a, mc) {
			continue
		}
		res.Matched++
		businessID := spec.Incident.BusinessID
		if businessID == "" && spec.Incident.BusinessLabel != "" {
			if b := matchBusiness(all, a.Labels[spec.Incident.BusinessLabel]); b != nil {
				businessID = b.ID
			}
		}
		key := correlationKey(&spec, a, businessID, nil)
		g := groups[key]
		if g == nil {
			g = &TestGroup{Key: key}
			groups[key] = g
		}
		g.Alerts = append(g.Alerts, a)
	}
	for _, g := range groups {
		cis := map[string]struct{}{}
		for _, a := range g.Alerts {
			if a.CIID != "" {
				cis[a.CIID] = struct{}{}
			}
		}
		g.CIs = len(cis)
		g.WouldOpen = len(g.Alerts) >= int(spec.Correlation.MinAlerts) && (spec.Correlation.MinCIs <= 1 || g.CIs >= int(spec.Correlation.MinCIs))
		g.Severity = severityFor(&spec, g.Alerts)
		g.Title = renderTitle(spec.Incident.TitleTemplate, g.Alerts[0], "", len(g.Alerts))
		if len(g.Alerts) > 20 {
			g.Alerts = g.Alerts[:20]
		}
		res.Groups = append(res.Groups, g)
	}
	sort.SliceStable(res.Groups, func(i, j int) bool { return len(res.Groups[i].Alerts) > len(res.Groups[j].Alerts) })
	if len(res.Groups) > testGroups {
		res.Groups = res.Groups[:testGroups]
	}
	return res, nil
}

// matchContextFor 读取告警关联资源的模型（匹配 model_codes 用）。
func (uc *RuleUsecase) matchContextFor(ctx context.Context, alerts []*AlertRef) (matchContext, error) {
	mc := matchContext{ciModel: map[string]string{}}
	var ids []string
	for _, a := range alerts {
		if a.CIID != "" {
			ids = append(ids, a.CIID)
		}
	}
	if ids = uniq(ids); len(ids) == 0 {
		return mc, nil
	}
	cards, err := uc.d.CMDB.Cards(ctx, ids)
	if err != nil {
		return mc, err
	}
	for _, c := range cards {
		mc.ciModel[c.ID] = c.ModelCode
	}
	return mc, nil
}
