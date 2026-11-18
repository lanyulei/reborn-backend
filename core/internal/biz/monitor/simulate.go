package biz

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 模拟触发（P2/P3 设计 §3.7）：按表单当前的规则内容在指定数据源上执行查询，再走与评估引擎相同的
// 处理流程，逐阶段给出结论。流水线各阶段（改写、指纹、关联 / 富化 / 丢弃 / 资源范围、抑制、生效时间）
// 调的是引擎所用的同一套 kit 纯函数；通知对象与订阅解析用的是通知派发的同一份实现（recipients.go）。
// 不写告警表、发件箱、时间线与通知记录；send 时只经通知域写一条带 [TEST] 的通知事件。
//
// 与引擎的已知差别（都只影响展示，不产生副作用）：
//   - 抑制快照取「此刻告警中且未被抑制」的事件（引擎落库的结论），至多 simulateSnapshotLimit 条；
//   - 引擎对上游序列的标签个数 / 长度有上限（其实现在引擎内，不在 kit），这里不裁剪；
//   - 不模拟持续时长（for）：直接当作已满足。

// 模拟触发的阶段（固定顺序）与结论。
const (
	StageQuery        = "query"
	StageEffective    = "effective"
	StageRelabel      = "relabel"
	StageCI           = "ci"
	StageDrop         = "drop"
	StageScope        = "scope"
	StageLevelInhibit = "level_inhibit"
	StageInhibit      = "inhibit"
	StageMute         = "mute"
	StageNotify       = "notify"
	StageSend         = "send"

	StagePass  = "pass"
	StageWarn  = "warn"
	StageBlock = "block"
	StageError = "error"
	StageSkip  = "skip"
)

// 事件的去向（SimulateEvent.Outcome）。
const (
	OutcomeNotify         = "notify"
	OutcomeDropped        = "dropped"
	OutcomeOutOfScope     = "out_of_scope"
	OutcomeLevelInhibited = "level_inhibited"
	OutcomeInhibited      = "inhibited"
	OutcomeMutedEvent     = "muted_event"
	OutcomeMutedNotify    = "muted_notify"
	OutcomeMaintenance    = "maintenance"
)

// 模拟触发的限额。
const (
	// simulateMaxEvents 返回的事件数上限。
	simulateMaxEvents = 20
	// simulateMaxSubscriptions 返回的命中订阅数上限。
	simulateMaxSubscriptions = 50
	// simulateSnapshotLimit 抑制快照装入的活跃告警数上限。
	simulateSnapshotLimit = 5000
	// simulateMaxInhibits / simulateMaxCIs 装入的抑制规则数与资源数上限。
	simulateMaxInhibits = 2000
	simulateMaxCIs      = 50000
	// simulateMaxSampleLabels 合成事件的样例标签个数上限（与接口校验同一口径）。
	simulateMaxSampleLabels = 30
	// simulateTestPrefix 测试通知标题的前缀。
	simulateTestPrefix = "[TEST] "
	// simulateRulePath 测试通知里的链接（没有对应的告警事件，指向规则页）。
	simulateRulePath = "/monitor/rule"
	// unsavedRuleID 规则尚未保存时参与指纹与订阅预筛的规则 ID。
	unsavedRuleID = "00000000-0000-0000-0000-000000000000"
	// templateErrorText 附加信息渲染失败时的取值（与引擎一致）。
	templateErrorText = "<template error>"
)

// RouteProbe 通知路由试算（组合根以 notify 域的 PreviewUsecase 适配）。
type RouteProbe interface {
	// RouteMatched 是否有启用的通知路由能匹配该类型、级别与标签的监控事件。
	RouteMatched(ctx context.Context, eventType string, severity int32, labels map[string]string) (bool, error)
}

// SimulateInput 模拟触发入参。
type SimulateInput struct {
	Spec         RuleSpec
	DatasourceID string
	SampleLabels map[string]string
	SampleValue  float64
	Send         bool
	// RuleID 正在编辑的规则（可空）：判断抑制时把它自己的活跃告警排除在外。
	RuleID string
}

// SimulateStage 一个阶段的结论。
type SimulateStage struct {
	Stage   string
	Status  string
	Message string
}

// SimulateQuery 一条查询的执行结果。
type SimulateQuery struct {
	Index      int32
	Severity   int32
	Series     int32
	Truncated  bool
	Error      string
	DurationMS int32
}

// SimulateEvent 一个事件及其去向。
type SimulateEvent struct {
	Labels         map[string]string
	SourceLabels   map[string]string
	Severity       int32
	Value          float64
	Fingerprint    string
	Annotations    map[string]string
	Outcome        string
	Reason         string
	CIStatus       string
	CI             *NamedRef
	InhibitID      string
	InhibitAlertID string
	MuteID         string

	// 以下供阶段说明与通知解析用，不出接口。
	relabeled   bool
	dropErr     error
	verdict     monitorpipeline.Verdict
	maintenance bool
	ci          *monitorpipeline.CI
	muteMode    int32
	muteName    string
	inhibitKind int32
}

// SimulateSubscription 命中的一条订阅。
type SimulateSubscription struct {
	ID         string
	Name       string
	Recipients []NamedRef
}

// SimulateLevel 升级策略一级此刻解析出的通知对象。
type SimulateLevel struct {
	Level      int32
	AfterM     int32
	Trigger    int32
	Recipients []NamedRef
}

// SimulateResult 模拟触发的结果。
type SimulateResult struct {
	Stages         []SimulateStage
	Queries        []SimulateQuery
	Synthetic      bool
	InEffect       bool
	Events         []*SimulateEvent
	EventTotal     int32
	Recipients     []NamedRef
	RecipientNotes []string
	Subscriptions  []SimulateSubscription
	Escalation     []SimulateLevel
	RouteMatched   bool
	Sent           bool
	NotifyEventIDs []string
}

// SimulateDeps 模拟触发的依赖（合成一组，免得构造函数的参数无限变长）。
type SimulateDeps struct {
	Scopes      *ScopeUsecase
	Rules       *RuleUsecase
	Sources     *DatasourceUsecase
	Mutes       MuteReader
	Alerts      AlertReader
	Inhibits    InhibitReader
	CIs         CIIndexReader
	Cfg         CfgVersionReader
	Calendar    CalendarReader
	Escalations EscalationReader
	Subs        SubscriptionReader
	Dir         Directory
	Leaders     DeptLeaders
	Duty        Duty
	Roster      DutyRoster
	Groups      OncallGroups
	Schedules   Schedules
	Routes      RouteProbe
	Events      EventWriter
	Txm         TxManager
}

// SimulateUsecase 模拟触发用例。进程内只有「资源索引」的只读缓存（按 monitor_cfg_version 的版本号
// 失效），不承载正确性；可并发使用。
type SimulateUsecase struct {
	d        SimulateDeps
	resolver *RecipientResolver
	matcher  *SubscriptionMatcher
	siteURL  string
	now      func() time.Time

	// mu 保护 ciVersion / ciIndex：资源索引缓存及其对应的配置版本。
	mu        sync.Mutex
	ciVersion int64
	ciIndex   *monitorpipeline.CIIndex
	ciLoaded  bool
}

// NewSimulateUsecase new a SimulateUsecase.
func NewSimulateUsecase(d SimulateDeps, logger *slog.Logger, c *conf.Monitor) (*SimulateUsecase, error) {
	siteURL, err := normalizeSiteURL(c.GetSiteUrl())
	if err != nil {
		return nil, err
	}
	return &SimulateUsecase{
		d: d, resolver: NewRecipientResolver(d.Dir, d.Leaders, d.Duty, d.Roster, d.Groups),
		matcher: NewSubscriptionMatcher(d.Subs, d.Scopes, logger), siteURL: siteURL, now: time.Now,
	}, nil
}

func simulateInvalid(format string, args ...any) error {
	return detail(ErrSimulateInvalid, format, args...)
}

// Simulate 执行一次模拟触发。要求对规则所在分组可管理。
func (uc *SimulateUsecase) Simulate(ctx context.Context, userID string, in SimulateInput) (*SimulateResult, error) {
	scope, err := uc.d.Scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	rules := uc.d.Rules
	if err := requireTargetGroup(scope, in.Spec.GroupID); err != nil {
		return nil, err
	}
	spec := in.Spec
	// 正在编辑的规则：保存前就在规则里的引用按保存时的口径豁免（与编辑保存一致）。
	var old *RuleSpec
	ruleID := unsavedRuleID
	if in.RuleID != "" {
		saved, err := rules.reader.Get(ctx, in.RuleID)
		switch {
		case kerrors.Is(err, ErrRuleNotFound):
		case err != nil:
			return nil, err
		case scope.CanManage(saved.Spec.GroupID):
			old, ruleID = &saved.Spec, saved.ID
		}
	}
	if err := rules.validate(ctx, scope, &spec, old); err != nil {
		return nil, err
	}
	if len(in.SampleLabels) > simulateMaxSampleLabels {
		return nil, simulateInvalid("at most %d sample labels", simulateMaxSampleLabels)
	}
	for name, value := range in.SampleLabels {
		if !mc.ValidLabelName(name) || !mc.ValidLabelValue(value) {
			return nil, simulateInvalid("sample label %q is invalid", truncate(name, 64))
		}
	}
	if spec.DatasourceScope == mc.DatasourceScopeSpecified && !slices.Contains(spec.DatasourceIDs, in.DatasourceID) {
		return nil, simulateInvalid("the datasource is not one of the rule's datasources")
	}
	ds, err := uc.d.Sources.reader.Get(ctx, in.DatasourceID)
	if kerrors.Is(err, ErrDatasourceNotFound) {
		return nil, simulateInvalid("the datasource does not exist")
	}
	if err != nil {
		return nil, err
	}
	if !ds.Enabled {
		return nil, simulateInvalid("the datasource is disabled")
	}
	pipe, err := monitorpipeline.Compile(pipelineSpec(&spec))
	if err != nil {
		return nil, ruleInvalid("%s", truncate(err.Error(), 300)) // validate 已通过，理论上不会到这里
	}
	now := uc.now()
	run := &simulation{uc: uc, scope: scope, spec: &spec, ruleID: ruleID, ds: ds, pipe: pipe, now: now, res: &SimulateResult{}}
	return run.run(ctx, in)
}

// simulation 一次模拟的上下文。
type simulation struct {
	uc     *SimulateUsecase
	scope  Scope
	spec   *RuleSpec
	ruleID string
	ds     *Datasource
	pipe   *monitorpipeline.Pipeline
	now    time.Time
	res    *SimulateResult
	cis    *monitorpipeline.CIIndex
}

func (s *simulation) stage(stage, status, format string, args ...any) {
	s.res.Stages = append(s.res.Stages, SimulateStage{Stage: stage, Status: status, Message: fmt.Sprintf(format, args...)})
}

// sample 一条查询结果序列（或合成的样本）。
type simSample struct {
	labels   map[string]string
	value    float64
	severity int32
}

// query 逐条执行规则的查询（复用即时查询的出站路径：同样的地址守卫、并发额度与凭据处理）。
func (s *simulation) query(ctx context.Context) ([]simSample, error) {
	var samples []simSample
	failed, truncated := 0, false
	var firstErr string
	for i, q := range s.spec.Queries {
		item := SimulateQuery{Index: int32(i), Severity: q.Severity} // #nosec G115 -- 至多 MaxRuleQueries 条
		res, err := s.uc.d.Sources.Query(ctx, s.ds.ID, q.Expr, time.Time{})
		switch {
		case err != nil && ctx.Err() != nil:
			return nil, ctx.Err()
		case err != nil && kerrors.FromError(err).Code >= 500 && !kerrors.Is(err, ErrDatasourceUnreachable):
			return nil, err
		case err != nil:
			item.Error = truncate(errorMessage(err), 512)
			if failed++; firstErr == "" {
				firstErr = item.Error
			}
		default:
			item.Series, item.Truncated = int32(len(res.Series)), res.Truncated // #nosec G115 -- 至多 maxQuerySeries
			item.DurationMS = int32(min(res.Duration.Milliseconds(), 1<<30))    // #nosec G115 -- 已夹紧
			truncated = truncated || res.Truncated
			for _, sr := range res.Series {
				if len(sr.Points) == 0 {
					continue
				}
				samples = append(samples, simSample{labels: sr.Labels, value: sr.Points[len(sr.Points)-1].Value, severity: q.Severity})
			}
		}
		s.res.Queries = append(s.res.Queries, item)
	}
	switch {
	case failed == len(s.spec.Queries):
		s.stage(StageQuery, StageError, "查询失败：%s", firstErr)
		return nil, nil
	case len(samples) == 0:
		s.res.Synthetic = true
		s.stage(StageQuery, StageWarn, "查询没有返回任何序列（当前没有满足条件的数据），已用样例标签合成一个事件继续模拟")
	case failed > 0:
		s.stage(StageQuery, StageWarn, "%d 条查询失败（%s），其余查询共返回 %d 条序列", failed, firstErr, len(samples))
	case truncated:
		s.stage(StageQuery, StageWarn, "查询返回的序列数超过上限已被截断，取到 %d 条序列", len(samples))
	default:
		s.stage(StageQuery, StagePass, "查询共返回 %d 条序列，每条序列对应一个告警事件", len(samples))
	}
	return samples, nil
}

func (s *simulation) run(ctx context.Context, in SimulateInput) (*SimulateResult, error) {
	samples, err := s.query(ctx)
	if err != nil {
		return nil, err
	}
	if len(s.res.Stages) == 1 && s.res.Stages[0].Status == StageError {
		for _, st := range []string{StageEffective, StageRelabel, StageCI, StageDrop, StageScope, StageLevelInhibit, StageInhibit, StageMute, StageNotify, StageSend} {
			s.stage(st, StageSkip, "查询失败，未执行")
		}
		return s.res, nil
	}
	if s.res.Synthetic {
		samples = []simSample{{labels: maps.Clone(in.SampleLabels), value: in.SampleValue, severity: s.spec.MaxSeverity()}}
	}
	if err := s.effective(ctx); err != nil {
		return nil, err
	}
	if s.cis, err = s.uc.ciIndexFor(ctx); err != nil {
		return nil, err
	}
	events := s.pipeline(samples)
	s.res.EventTotal = int32(min(len(events), 1<<30)) // #nosec G115 -- 已夹紧
	events = events[:min(len(events), simulateMaxEvents)]
	if err := s.suppress(ctx, events); err != nil {
		return nil, err
	}
	s.res.Events = events
	first := events[0]
	s.pipelineStages(first)
	if err := s.notify(ctx, first); err != nil {
		return nil, err
	}
	return s.res, s.send(ctx, in.Send, first)
}

// effective 生效时间：与引擎同一个判定函数（按工作日历的方式在日历缺失时恒为生效）。
func (s *simulation) effective(ctx context.Context) error {
	e := s.spec.Effective
	var cal *mc.Calendar
	note := ""
	if e.UsesCalendar() {
		snap, err := s.uc.d.Calendar.Snapshot(ctx)
		if err != nil {
			return err
		}
		if cal, err = snap.Compile(); err != nil {
			cal, note = nil, "；工作日历尚未同步或配置不合法，按全时段生效处理"
		}
	}
	s.res.InEffect = e.ContainsWithCalendar(s.now, cal)
	switch {
	case !s.res.InEffect:
		s.stage(StageEffective, StageWarn, "此刻不在规则的生效时间内，评估引擎不会评估；以下各阶段仍照常模拟")
	case note != "":
		s.stage(StageEffective, StageWarn, "此刻在生效时间内%s", note)
	default:
		s.stage(StageEffective, StagePass, "此刻在规则的生效时间内")
	}
	return nil
}

// ciIndexFor 取资源索引：按 monitor_cfg_version 的版本号缓存，版本不变不重读。从未同步过返回 nil
// （与引擎「索引尚未加载」同一语义）。
func (uc *SimulateUsecase) ciIndexFor(ctx context.Context) (*monitorpipeline.CIIndex, error) {
	version, err := uc.d.Cfg.CfgVersion(ctx, mc.CfgCIIndex)
	if err != nil {
		return nil, err
	}
	uc.mu.Lock()
	if uc.ciLoaded && uc.ciVersion == version {
		idx := uc.ciIndex
		uc.mu.Unlock()
		return idx, nil
	}
	uc.mu.Unlock()
	entries, err := uc.d.CIs.ListValid(ctx, simulateMaxCIs)
	if err != nil {
		return nil, err
	}
	var idx *monitorpipeline.CIIndex
	if len(entries) == 0 {
		_, lastSync, err := uc.d.CIs.Summary(ctx)
		if err != nil {
			return nil, err
		}
		if lastSync != nil {
			idx = monitorpipeline.NewCIIndex(nil)
		}
	} else {
		cis := make([]monitorpipeline.CI, 0, len(entries))
		for _, e := range entries {
			cis = append(cis, monitorpipeline.CI{
				ID: e.ID, ModelCode: e.ModelCode, Name: e.Name, Env: e.Env, Status: e.Status, Owner: e.Owner,
				Tags: e.Tags, Attrs: e.Attrs, UpstreamIDs: e.UpstreamIDs,
			})
		}
		idx = monitorpipeline.NewCIIndex(cis)
	}
	uc.mu.Lock()
	uc.ciVersion, uc.ciIndex, uc.ciLoaded = version, idx, true
	uc.mu.Unlock()
	return idx, nil
}

// pipeline 样本 → 事件：与引擎的 mergeSamples 同一顺序——去掉 __name__、合并规则附加标签 → 改写 →
// 指纹（开启级别抑制时不含级别）→ 关联 / 富化 / 丢弃 / 资源范围 → 级别抑制（同指纹只留最高级别）。
func (s *simulation) pipeline(samples []simSample) []*SimulateEvent {
	events := make([]*SimulateEvent, 0, len(samples))
	kept := map[string]*SimulateEvent{}
	templates := make(map[string]*monitorpipeline.Template, len(s.spec.Annotations))
	for name, text := range s.spec.Annotations {
		t, err := monitorpipeline.CompileTemplate(text)
		if err != nil {
			t = nil
		}
		templates[name] = t
	}
	for _, sm := range samples {
		// 克隆：查询结果的标签归查询方所有，这里要就地改写。
		labels := maps.Clone(sm.labels)
		if labels == nil {
			labels = map[string]string{}
		}
		delete(labels, "__name__")
		maps.Copy(labels, s.spec.Labels)
		ev := &SimulateEvent{SourceLabels: maps.Clone(labels), Severity: sm.severity, Value: sm.value, Outcome: OutcomeNotify}
		s.pipe.Relabel(labels)
		ev.relabeled = !maps.Equal(labels, ev.SourceLabels)
		fpSeverity := sm.severity
		if s.spec.LevelInhibit {
			fpSeverity = 0
		}
		ev.Fingerprint = mc.Fingerprint(s.ruleID, s.ds.ID, labels, fpSeverity)
		res := s.pipe.Resolve(labels, sm.value, s.cis)
		ev.Labels, ev.CIStatus, ev.dropErr, ev.verdict, ev.maintenance, ev.ci = labels, res.CIStatus.String(), res.DropErr, res.Verdict, res.Maintenance, res.CI
		if res.CI != nil {
			ev.CI = &NamedRef{ID: res.CI.ID, Name: res.CI.Name}
		}
		if res.DropErr != nil {
			ev.Reason = "条件丢弃表达式执行出错（事件不丢弃）：" + truncate(res.DropErr.Error(), 200)
		}
		ev.Annotations = make(map[string]string, len(templates))
		for name, t := range templates {
			ev.Annotations[name] = templateErrorText
			if t != nil {
				if text, err := t.Render(labels, sm.value); err == nil {
					ev.Annotations[name] = text
				}
			}
		}
		events = append(events, ev)
		switch res.Verdict {
		case monitorpipeline.Dropped:
			ev.Outcome, ev.Reason = OutcomeDropped, "条件丢弃表达式的结果为 true"
			continue
		case monitorpipeline.OutOfScope:
			ev.Outcome, ev.Reason = OutcomeOutOfScope, "事件没有关联到资源，或关联的资源不满足规则限定的模型 / 环境 / 标签"
			continue
		case monitorpipeline.Keep:
		}
		prev, dup := kept[ev.Fingerprint]
		switch {
		case !dup:
			kept[ev.Fingerprint] = ev
		case ev.Severity > prev.Severity:
			prev.Outcome, prev.Reason = OutcomeLevelInhibited, "同一序列同时命中了更高级别的查询"
			kept[ev.Fingerprint] = ev
		default:
			// 同级别的重复序列只留先出现的一条；低级别被抑制。
			ev.Outcome, ev.Reason = OutcomeLevelInhibited, "同一序列同时命中了更高级别的查询"
		}
	}
	return events
}

// compiledSimMute 一条对本规则此刻生效的屏蔽规则。
type compiledSimMute struct {
	mute *Mute
	c    criteria
}

// activeMutes 对「该规则 + 该数据源」此刻生效的屏蔽规则，强者在前（屏蔽事件先于只屏蔽通知，同一方式按
// ID 升序）——与引擎的判定同一口径：作用分组覆盖规则所在分组（含上级）或全局、限定的规则 / 数据源命中、
// 启用且在屏蔽时间内；匹配条件非法的不生效。
func (s *simulation) activeMutes(ctx context.Context) ([]compiledSimMute, error) {
	filter := MuteFilter{Groups: GroupFilter{IDs: s.scope.Ancestors(s.spec.GroupID)}, IncludeGlobal: true, Live: true}
	mutes, _, err := s.uc.d.Mutes.List(ctx, filter, 1, maxMuteScan)
	if err != nil {
		return nil, err
	}
	out := make([]compiledSimMute, 0, len(mutes))
	for _, m := range mutes {
		sp := &m.Spec
		if muteState(sp, s.now) != MuteStateActive ||
			(len(sp.RuleIDs) > 0 && !slices.Contains(sp.RuleIDs, s.ruleID)) ||
			(len(sp.DatasourceIDs) > 0 && !slices.Contains(sp.DatasourceIDs, s.ds.ID)) {
			continue
		}
		ms, err := mc.CompileMatchers(sp.Matchers)
		if err != nil {
			continue
		}
		out = append(out, compiledSimMute{mute: m, c: criteria{severities: sp.Severities, matchers: ms}})
	}
	slices.SortFunc(out, func(a, b compiledSimMute) int {
		return cmp.Or(cmp.Compare(b.mute.Spec.Mode, a.mute.Spec.Mode), cmp.Compare(a.mute.ID, b.mute.ID))
	})
	return out, nil
}

// inhibitSnapshot 抑制索引：全部启用的抑制规则 + 此刻告警中且未被抑制的事件（不含本规则自己的）。
func (s *simulation) inhibitSnapshot(ctx context.Context) (*monitorpipeline.InhibitSnapshot, map[string]string, error) {
	inhibits, err := s.uc.d.Inhibits.ListEnabled(ctx, simulateMaxInhibits)
	if err != nil {
		return nil, nil, err
	}
	rules := make([]monitorpipeline.InhibitRule, 0, len(inhibits))
	names := make(map[string]string, len(inhibits))
	for _, in := range inhibits {
		path := ""
		if in.Spec.GroupID != "" {
			g, ok := s.scope.Group(in.Spec.GroupID)
			if !ok {
				continue // 作用分组已删除
			}
			path = g.Path
		}
		names[in.ID] = in.Spec.Name
		rules = append(rules, monitorpipeline.InhibitRule{
			ID: in.ID, GroupPath: path, Source: in.Spec.Source, Target: in.Spec.Target, Equal: in.Spec.EqualLabels,
		})
	}
	notInhibited := false
	active, err := s.uc.d.Alerts.Scan(ctx, AlertFilter{
		Groups: GroupFilter{All: true}, Statuses: []int32{mc.AlertFiring}, Inhibited: &notInhibited,
	}, simulateSnapshotLimit)
	if err != nil {
		return nil, nil, err
	}
	refs := make([]monitorpipeline.AlertRef, 0, len(active))
	for _, a := range active {
		if a.RuleID == s.ruleID {
			continue
		}
		g, _ := s.scope.Group(a.GroupID)
		refs = append(refs, monitorpipeline.AlertRef{ID: a.ID, RuleID: a.RuleID, GroupPath: g.Path, Severity: a.Severity, Labels: a.Labels, CIID: a.CIID})
	}
	return monitorpipeline.CompileInhibits(rules).Snapshot(refs), names, nil
}

// suppress 对还会产生事件的样本判定抑制与屏蔽。去向的优先级与引擎一致：屏蔽事件 > 被抑制 > 只屏蔽
// 通知 > 资源维护态。
func (s *simulation) suppress(ctx context.Context, events []*SimulateEvent) error {
	mutes, err := s.activeMutes(ctx)
	if err != nil {
		return err
	}
	snap, inhibitNames, err := s.inhibitSnapshot(ctx)
	if err != nil {
		return err
	}
	group, _ := s.scope.Group(s.spec.GroupID)
	for _, ev := range events {
		if ev.Outcome != OutcomeNotify {
			continue
		}
		ref := monitorpipeline.AlertRef{RuleID: s.ruleID, GroupPath: group.Path, Severity: ev.Severity, Labels: ev.Labels}
		if ev.ci != nil {
			ref.CIID = ev.ci.ID
		}
		hit, inhibited := snap.Match(&ref)
		if !inhibited && s.spec.TopologyInhibit && ev.ci != nil {
			upstream := slices.DeleteFunc(slices.Clone(ev.ci.UpstreamIDs), func(id string) bool { return id == ev.ci.ID })
			hit, inhibited = snap.Upstream(upstream)
		}
		probe := &Alert{Severity: ev.Severity, Labels: ev.Labels}
		for _, m := range mutes {
			if m.c.match(probe) {
				ev.MuteID, ev.muteMode, ev.muteName = m.mute.ID, m.mute.Spec.Mode, m.mute.Spec.Name
				break
			}
		}
		if inhibited {
			ev.InhibitID, ev.InhibitAlertID, ev.inhibitKind = hit.InhibitID, hit.AlertID, hit.Kind
		}
		switch {
		case ev.muteMode == mc.MuteModeEvent:
			ev.Outcome, ev.Reason = OutcomeMutedEvent, "命中屏蔽规则「"+ev.muteName+"」（屏蔽事件：不产生事件）"
		case inhibited && hit.Kind == mc.InhibitTopology:
			ev.Outcome, ev.Reason = OutcomeInhibited, "关联资源的上游资源正在告警（拓扑抑制）"
		case inhibited:
			ev.Outcome, ev.Reason = OutcomeInhibited, "命中抑制规则「"+inhibitNames[hit.InhibitID]+"」"
		case ev.muteMode == mc.MuteModeNotify:
			ev.Outcome, ev.Reason = OutcomeMutedNotify, "命中屏蔽规则「"+ev.muteName+"」（只屏蔽通知）"
		case ev.maintenance:
			ev.Outcome, ev.Reason = OutcomeMaintenance, "关联的资源处于维护态，只屏蔽通知"
		}
	}
	return nil
}

// ciStatusText 关联结论的说明。
var ciStatusText = map[string]string{
	monitorpipeline.CIDisabled.String():    "规则关闭了资源关联",
	monitorpipeline.CINoLabel.String():     "事件上没有用于关联资源的标签",
	monitorpipeline.CINotFound.String():    "没有找到与标签取值对应的资源",
	monitorpipeline.CIAmbiguous.String():   "标签取值对应到多个资源，不关联（可在规则里限定模型来消歧）",
	monitorpipeline.CIUnavailable.String(): "资源索引尚未同步，暂时无法关联资源",
}

// pipelineStages 针对一个事件给出 relabel 至 mute 各阶段的结论。事件在某一步被拦下后，其后依赖它的阶段
// 记为未执行。
func (s *simulation) pipelineStages(ev *SimulateEvent) {
	switch {
	case len(s.spec.Relabel) == 0:
		s.stage(StageRelabel, StageSkip, "规则没有配置标签改写")
	case ev.relabeled:
		s.stage(StageRelabel, StagePass, "标签改写生效，事件标签已按规则改写（指纹按改写后的标签计算）")
	default:
		s.stage(StageRelabel, StagePass, "标签改写没有改动这个事件的标签")
	}
	switch ev.CIStatus {
	case monitorpipeline.CIMatched.String():
		s.stage(StageCI, StagePass, "已关联资源「%s」，并写入 ci_ 开头的富化标签", ev.ci.Name)
	case monitorpipeline.CIDisabled.String():
		s.stage(StageCI, StageSkip, "%s", ciStatusText[ev.CIStatus])
	default:
		s.stage(StageCI, StageWarn, "%s", ciStatusText[ev.CIStatus])
	}
	switch {
	case s.spec.DropExpr == "":
		s.stage(StageDrop, StageSkip, "规则没有配置条件丢弃")
	case ev.dropErr != nil:
		s.stage(StageDrop, StageError, "条件丢弃表达式执行出错，事件不丢弃：%s", truncate(ev.dropErr.Error(), 200))
	case ev.verdict == monitorpipeline.Dropped:
		s.stage(StageDrop, StageBlock, "条件丢弃表达式的结果为 true，事件被丢弃")
	default:
		s.stage(StageDrop, StagePass, "条件丢弃表达式的结果不是 true，事件保留")
	}
	blocked := ev.verdict == monitorpipeline.Dropped
	skip := func(stage string) { s.stage(stage, StageSkip, "事件已在前面的阶段被拦下，未执行") }
	switch {
	case !s.spec.ResourceScope.Enabled:
		s.stage(StageScope, StageSkip, "规则没有限定资源范围")
	case blocked:
		skip(StageScope)
	case ev.verdict == monitorpipeline.OutOfScope:
		s.stage(StageScope, StageBlock, "%s，不产生事件", ev.Reason)
	default:
		s.stage(StageScope, StagePass, "关联的资源在规则限定的资源范围内")
	}
	blocked = blocked || ev.verdict == monitorpipeline.OutOfScope
	switch {
	case !s.spec.LevelInhibit || len(s.spec.Queries) < 2:
		s.stage(StageLevelInhibit, StageSkip, "规则只有一档级别，或没有开启级别抑制")
	case blocked:
		skip(StageLevelInhibit)
	case ev.Outcome == OutcomeLevelInhibited:
		s.stage(StageLevelInhibit, StageBlock, "%s，只保留最高级别的事件", ev.Reason)
	default:
		s.stage(StageLevelInhibit, StagePass, "这是该序列命中的最高级别")
	}
	if blocked = blocked || ev.Outcome == OutcomeLevelInhibited; blocked {
		skip(StageInhibit)
		skip(StageMute)
		return
	}
	switch ev.inhibitKind {
	case mc.InhibitRule, mc.InhibitTopology:
		reason := ev.Reason
		if ev.Outcome != OutcomeInhibited {
			reason = "同时命中了抑制"
		}
		s.stage(StageInhibit, StageBlock, "%s：事件照常产生并留档，但不通知、不升级", reason)
	default:
		s.stage(StageInhibit, StagePass, "没有被抑制规则或上游资源的告警抑制")
	}
	switch {
	case ev.muteMode == mc.MuteModeEvent:
		s.stage(StageMute, StageBlock, "命中屏蔽规则「%s」（屏蔽事件）：不产生事件", ev.muteName)
	case ev.muteMode == mc.MuteModeNotify:
		s.stage(StageMute, StageBlock, "命中屏蔽规则「%s」（只屏蔽通知）：事件照常产生，但不通知", ev.muteName)
	case ev.maintenance:
		s.stage(StageMute, StageBlock, "关联的资源处于维护态：事件照常产生，但不通知")
	default:
		s.stage(StageMute, StagePass, "没有命中屏蔽规则")
	}
}

// userRefs 用户 ID → NamedRef（保持顺序）。
func userRefs(ids []string, names map[string]string) []NamedRef {
	out := make([]NamedRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, NamedRef{ID: id, Name: names[id]})
	}
	return out
}

// ciEntry 事件关联的资源（通知对象解析与通知内容用）。
func (ev *SimulateEvent) ciEntry() *CIEntry {
	if ev.ci == nil {
		return nil
	}
	return &CIEntry{ID: ev.ci.ID, Name: ev.ci.Name, Owner: ev.ci.Owner, Valid: true}
}

// notify 解析规则的通知对象、命中的订阅与升级策略各级的通知对象（与通知派发同一份实现），并试算通知路由。
func (s *simulation) notify(ctx context.Context, ev *SimulateEvent) error {
	sess := s.uc.resolver.Session(s.now)
	users, notes, err := sess.Rule(ctx, s.spec.Notify, ev.ciEntry())
	if err != nil {
		return err
	}
	subs, err := s.uc.matcher.Match(ctx, SubscriptionEvent{
		GroupID: s.spec.GroupID, RuleID: s.ruleID, DatasourceID: s.ds.ID, Severity: ev.Severity, Labels: ev.Labels,
	})
	if err != nil {
		return err
	}
	type subHit struct {
		sub        *Subscription
		recipients []string
	}
	var hits []subHit
	notified := slices.Clone(users)
	nameIDs := slices.Clone(users)
	for _, sub := range subs {
		if len(hits) >= simulateMaxSubscriptions {
			break
		}
		su, err := sess.Subscription(ctx, sub)
		if err != nil {
			return err
		}
		recipients := slices.DeleteFunc(slices.Concat(su, sub.Spec.Receivers.Groups), func(id string) bool { return slices.Contains(notified, id) })
		if len(recipients) == 0 {
			continue
		}
		notified = append(notified, recipients...)
		nameIDs = append(nameIDs, su...)
		hits = append(hits, subHit{sub: sub, recipients: recipients})
	}
	type levelHit struct {
		level mc.EscalationLevel
		users []string
	}
	var levels []levelHit
	if s.spec.EscalationID != "" {
		e, err := s.uc.d.Escalations.Get(ctx, s.spec.EscalationID)
		switch {
		case kerrors.Is(err, ErrEscalationNotFound):
		case err != nil:
			return err
		case e.Spec.Enabled && ev.Severity >= e.Spec.MinSeverity:
			for _, l := range e.Spec.Levels {
				lu, ln, err := sess.Escalation(ctx, l.Targets)
				if err != nil {
					return err
				}
				notes = append(notes, ln...)
				nameIDs = append(nameIDs, lu...)
				levels = append(levels, levelHit{level: l, users: lu})
			}
		}
	}
	userNames := map[string]string{}
	if nameIDs = uniq(nameIDs); len(nameIDs) > 0 {
		if userNames, err = s.uc.d.Dir.UserNames(ctx, nameIDs); err != nil {
			return err
		}
	}
	s.res.Recipients = userRefs(users, userNames)
	s.res.Subscriptions = make([]SimulateSubscription, 0, len(hits))
	for _, h := range hits {
		item := SimulateSubscription{ID: h.sub.ID, Name: h.sub.Spec.Name, Recipients: make([]NamedRef, 0, len(h.recipients))}
		for _, id := range h.recipients {
			name := userNames[id]
			if isGroupRecipient(id) {
				name = id
			}
			item.Recipients = append(item.Recipients, NamedRef{ID: id, Name: name})
		}
		s.res.Subscriptions = append(s.res.Subscriptions, item)
	}
	s.res.Escalation = make([]SimulateLevel, 0, len(levels))
	for i, l := range levels {
		s.res.Escalation = append(s.res.Escalation, SimulateLevel{
			Level: int32(i + 1), AfterM: l.level.AfterM, Trigger: l.level.Trigger, Recipients: userRefs(l.users, userNames), // #nosec G115 -- 至多 MaxEscalationLevels 级
		})
	}
	if s.res.RecipientNotes, err = s.noteTexts(ctx, notes, ev); err != nil {
		return err
	}
	if s.res.RouteMatched, err = s.uc.d.Routes.RouteMatched(ctx, mc.EventAlertFiring, ev.Severity, ev.Labels); err != nil {
		return err
	}
	switch {
	case ev.Outcome != OutcomeNotify:
		s.stage(StageNotify, StageSkip, "事件不会通知（%s）；下面列出的是假如通知时会解析出的对象", ev.Reason)
	case !s.res.RouteMatched:
		s.stage(StageNotify, StageWarn, "通知管理里没有路由能匹配这条规则的告警事件，通知不会被投递")
	case len(users) == 0:
		s.stage(StageNotify, StageWarn, "没有解析到通知对象：事件不带收件人，交由通知管理的路由与订阅决定发给谁")
	case len(notes) > 0:
		s.stage(StageNotify, StageWarn, "解析出 %d 个通知对象，命中 %d 条订阅；部分通知对象没有解析到人，见说明", len(users), len(hits))
	default:
		s.stage(StageNotify, StagePass, "解析出 %d 个通知对象，命中 %d 条订阅", len(users), len(hits))
	}
	return nil
}

// noteTexts 把解析通知对象时的说明写成中文（带对象名称；各类名称各查一次）。
func (s *simulation) noteTexts(ctx context.Context, notes []ResolveNote, ev *SimulateEvent) ([]string, error) {
	if len(notes) == 0 {
		return []string{}, nil
	}
	var scheduleIDs, deptIDs []string
	needGroups := false
	for _, n := range notes {
		switch n.Kind {
		case NoteScheduleEmpty:
			scheduleIDs = append(scheduleIDs, n.ID)
		case NoteDeptNoLeader:
			deptIDs = append(deptIDs, n.ID)
		case NoteOncallGroupNoAdmin:
			needGroups = true
		}
	}
	scheduleNames, deptNames, groupNames := map[string]string{}, map[string]string{}, map[string]string{}
	var err error
	if scheduleIDs = uniq(scheduleIDs); len(scheduleIDs) > 0 {
		if scheduleNames, err = s.uc.d.Schedules.ScheduleNames(ctx, scheduleIDs); err != nil {
			return nil, err
		}
	}
	if deptIDs = uniq(deptIDs); len(deptIDs) > 0 {
		if deptNames, err = s.uc.d.Dir.DeptNames(ctx, deptIDs); err != nil {
			return nil, err
		}
	}
	if needGroups && s.uc.d.Groups != nil {
		groups, err := s.uc.d.Groups.Groups(ctx)
		if err != nil {
			return nil, err
		}
		for _, g := range groups {
			groupNames[g.ID] = g.Name
		}
	}
	label := func(id string, names map[string]string) string {
		if name := names[id]; name != "" {
			return name
		}
		return id
	}
	out := make([]string, 0, len(notes))
	for _, n := range notes {
		text := ""
		switch n.Kind {
		case NoteScheduleEmpty:
			text = "值班表「" + label(n.ID, scheduleNames) + "」当前无人值班（或已停用）"
		case NoteDeptNoLeader:
			text = "部门「" + label(n.ID, deptNames) + "」及其上级部门没有可用的负责人"
		case NoteOncallGroupNoAdmin:
			text = "值班分组「" + label(n.ID, groupNames) + "」没有可用的管理员"
		case NoteCIOwnerNotUser:
			name := n.ID
			if ev.ci != nil {
				name = ev.ci.Name
			}
			text = "资源「" + name + "」的负责人不是可用的系统用户"
		case NoteCINotLinked:
			text = "规则要求通知资源负责人，但事件没有关联到资源"
		case NoteUsersInactive:
			text = "部分通知对象已停用或不存在，已略过"
		}
		if text != "" && !slices.Contains(out, text) {
			out = append(out, text)
		}
	}
	return out, nil
}

// send 真实发送一条带 [TEST] 的通知：只发给规则通知对象解析出的用户（不发给订阅者，不触发升级与事件
// 动作）；一个事务，经通知域写入。不写告警表、发件箱、时间线与通知记录。
func (s *simulation) send(ctx context.Context, want bool, ev *SimulateEvent) error {
	switch {
	case !want:
		s.stage(StageSend, StageSkip, "干跑：没有发送测试通知")
		return nil
	case ev.Outcome != OutcomeNotify:
		s.stage(StageSend, StageSkip, "事件不会通知，没有发送测试通知")
		return nil
	}
	value := ev.Value
	p := buildPayload(payloadInput{
		Alert: &Alert{
			RuleID: s.ruleID, RuleName: s.spec.Name, DatasourceName: s.ds.Name, Severity: ev.Severity,
			Labels: ev.Labels, Annotations: ev.Annotations, Expr: s.exprOf(ev.Severity), Value: &value, FirstFireTime: s.now,
		},
		Timezone: s.spec.Effective.Timezone, GroupName: s.scope.GroupName(s.spec.GroupID), SiteURL: s.uc.siteURL,
		Seq: 1, Now: s.now, CI: ev.ciEntry(),
	})
	// 没有对应的告警事件：链接指向规则页，不带事件 ID。
	p.Test, p.AlertID, p.AlertURL = true, "", s.uc.siteURL+simulateRulePath
	if s.ruleID == unsavedRuleID {
		p.RuleID = ""
	}
	raw, err := encodePayload(p)
	if err != nil {
		return fmt.Errorf("encode test notify payload: %w", err)
	}
	// 每次调用一个新的去重键：重复点击都会发出。
	nonce, err := idgen.NewID()
	if err != nil {
		return err
	}
	title := truncateBytes(simulateTestPrefix+alertTitle(s.spec.Name, ev.Labels), maxTitleBytes)
	recipients := make([]string, 0, len(s.res.Recipients))
	for _, r := range s.res.Recipients {
		recipients = append(recipients, r.ID)
	}
	chunks := [][]string{nil}
	if len(recipients) > 0 {
		chunks = slices.Collect(slices.Chunk(recipients, maxEventRecipients))
	}
	ids := make([]string, 0, len(chunks))
	err = s.uc.d.Txm.WithinTx(ctx, func(tx txrun.Tx) error {
		ids = ids[:0]
		for i, chunk := range chunks {
			out, err := s.uc.d.Events.NotifyEvent(ctx, tx, biznotify.Event{
				BizID: mc.NotifyBizID, EventType: mc.EventAlertFiring, Severity: ev.Severity, Title: title, Payload: raw,
				DedupKey: "monitor:test:" + strings.ReplaceAll(nonce, "-", "") + ":" + fmt.Sprint(i), Recipients: chunk,
			})
			if err != nil {
				return err
			}
			ids = append(ids, out.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.res.Sent, s.res.NotifyEventIDs = true, ids
	s.stage(StageSend, StagePass, "测试通知已写入通知管道（标题带 [TEST]），可到「通知管理 → 事件」查看投递情况")
	return nil
}

// exprOf 该级别对应的查询表达式。
func (s *simulation) exprOf(severity int32) string {
	for _, q := range s.spec.Queries {
		if q.Severity == severity {
			return q.Expr
		}
	}
	return ""
}
