package biz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 关联器（功能设计 §3.2 / §11.2）：单实例 tail 监控域的告警时间线（按 seq），按触发规则匹配，归并到活跃故障、
// 候选故障或开新故障；恢复事件触发「关联告警全部恢复」的检查；风暴保护。全部写幂等，崩溃后从游标重放不重复开故障。

const (
	feedCursor       = "alert_feed"
	feedBatch        = 500
	feedLookback     = 1000
	ruleCacheTTL     = 30 * time.Second
	stormWindow      = time.Minute
	feedLagThreshold = 10 * time.Minute
	engineLagDedup   = 30 * time.Minute
)

// CorrelatorUsecase 关联器后台用例。
type CorrelatorUsecase struct {
	d        *Deps
	inc      *IncidentUsecase
	settings *SettingsUsecase
	now      func() time.Time

	rulesAt time.Time
	rules   []*compiledRule
	// lagNotified 上次发出「关联器落后」自监控通知的时间（进程内）。
	lagNotified time.Time
}

// NewCorrelatorUsecase new a CorrelatorUsecase.
func NewCorrelatorUsecase(d *Deps, inc *IncidentUsecase, settings *SettingsUsecase) *CorrelatorUsecase {
	return &CorrelatorUsecase{d: d, inc: inc, settings: settings, now: time.Now}
}

// loadRules 启用的规则（带短缓存），编译匹配器。
func (uc *CorrelatorUsecase) loadRules(ctx context.Context) ([]*compiledRule, error) {
	if uc.rules != nil && uc.now().Sub(uc.rulesAt) < ruleCacheTTL {
		return uc.rules, nil
	}
	rules, err := uc.d.Rules.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*compiledRule, 0, len(rules))
	for _, r := range rules {
		set, err := uc.d.Matchers.Compile(r.Spec.Match.Matchers)
		if err != nil {
			uc.d.Logger.WarnContext(ctx, "incident correlator: skip rule with invalid matchers", "rule", r.ID, "error", err)
			continue
		}
		out = append(out, &compiledRule{rule: r, matchers: set})
	}
	uc.rules, uc.rulesAt = out, uc.now()
	return out, nil
}

// Run 处理一批告警事件，返回处理条数。由 loop.Leader 保证单实例执行。
//
// 两段读取：先回看游标之前 feedLookback 个序号内的事件（覆盖「取号早、提交晚」的事务），只处理 incident_feed_seen
// 里没有记录的序号——否则每轮重放会把解除关联的告警重新归并、把已关闭的故障重开、重建已忽略的候选并不断刷新候选的
// update_time 让它永不过期；再向前读游标之后的全部事件并据此推进游标——回看段不参与推进，否则回看窗口里堆满事件时
// 游标会原地不动。前向处理过的序号与游标在同一事务里记为已处理；回看补处理的逐条记录；滑出回看窗口的记录每轮清理。
// 单条处理失败：游标停在它之前（下一轮从它重试），错误上抛给循环记录与退避。
func (uc *CorrelatorUsecase) Run(ctx context.Context) (int, error) {
	cursor, err := uc.d.Cursor.Get(ctx, feedCursor)
	if err != nil {
		return 0, err
	}
	lookbackFrom := max(cursor-feedLookback, 0)
	lookback, err := uc.d.Alerts.FeedRange(ctx, lookbackFrom, cursor, feedLookback)
	if err != nil {
		return 0, err
	}
	forward, err := uc.d.Alerts.Feed(ctx, cursor, feedBatch)
	if err != nil {
		return 0, err
	}
	if len(lookback) == 0 && len(forward) == 0 {
		return 0, nil
	}
	seen, err := uc.d.Cursor.Seen(ctx, lookbackFrom, cursor)
	if err != nil {
		return 0, err
	}
	rules, err := uc.loadRules(ctx)
	if err != nil {
		return 0, err
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, s := range lookback {
		if _, ok := seen[s.Seq]; ok {
			continue
		}
		if err := uc.handle(ctx, s, rules, settings); err != nil {
			uc.d.Logger.ErrorContext(ctx, "incident correlator: handle signal", "seq", s.Seq, "alert", s.Alert.ID, "kind", s.Kind, "error", err)
			return processed, err
		}
		if err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error { return uc.d.Cursor.MarkSeenTx(ctx, tx, []int64{s.Seq}) }); err != nil {
			return processed, err
		}
		processed++
	}
	maxSeq := cursor
	done := make([]int64, 0, len(forward))
	var runErr error
	for _, s := range forward {
		if err := uc.handle(ctx, s, rules, settings); err != nil {
			uc.d.Logger.ErrorContext(ctx, "incident correlator: handle signal", "seq", s.Seq, "alert", s.Alert.ID, "kind", s.Kind, "error", err)
			runErr = err
			break
		}
		maxSeq = max(maxSeq, s.Seq)
		done = append(done, s.Seq)
		processed++
	}
	if maxSeq > cursor {
		err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
			if err := uc.d.Cursor.MarkSeenTx(ctx, tx, done); err != nil {
				return err
			}
			return uc.d.Cursor.SetTx(ctx, tx, feedCursor, maxSeq)
		})
		if err != nil {
			return processed, err
		}
	}
	if below := maxSeq - feedLookback; below > 0 {
		if err := uc.d.Cursor.PruneBelow(ctx, below); err != nil {
			uc.d.Logger.WarnContext(ctx, "incident correlator: prune feed seen", "error", err)
		}
	}
	if runErr != nil {
		return processed, runErr
	}
	if len(forward) == feedBatch {
		if last := forward[len(forward)-1]; uc.now().Sub(last.Time) > feedLagThreshold {
			uc.reportLag(ctx, last.Time)
		}
	}
	return processed, nil
}

// handle 处理一条信号。
func (uc *CorrelatorUsecase) handle(ctx context.Context, s *AlertSignal, rules []*compiledRule, settings *Settings) error {
	a := &s.Alert
	switch s.Kind {
	case "fire", "unmute", "uninhibit":
		if a.Status != 1 || a.Muted || a.Inhibited {
			return nil
		}
		return uc.onFire(ctx, a, rules, settings)
	case "resolve", "close":
		incidentID, err := uc.d.Links.ActiveIncident(ctx, a.ID)
		if err != nil || incidentID == "" {
			return err
		}
		return uc.inc.onAlertsResolved(ctx, incidentID, settings)
	}
	return nil
}

// onFire 告警触发：已属于活跃故障的不再归并；否则按顺序找到第一条命中的规则。
func (uc *CorrelatorUsecase) onFire(ctx context.Context, a *AlertRef, rules []*compiledRule, settings *Settings) error {
	if existing, err := uc.d.Links.ActiveIncident(ctx, a.ID); err != nil || existing != "" {
		return err
	}
	if len(rules) == 0 {
		return nil
	}
	mc := matchContext{ciModel: map[string]string{}}
	needModel := slices.ContainsFunc(rules, func(r *compiledRule) bool { return len(r.rule.Spec.Match.ModelCodes) > 0 })
	if needModel && a.CIID != "" {
		if cards, err := uc.d.CMDB.Cards(ctx, []string{a.CIID}); err == nil && len(cards) == 1 {
			mc.ciModel[a.CIID] = cards[0].ModelCode
		}
	}
	for _, r := range rules {
		if !r.matches(a, mc) {
			continue
		}
		return uc.correlate(ctx, r.rule, a, settings)
	}
	return nil
}

// correlate 把告警归并到规则下的活跃故障 / 候选桶，或开新故障。
func (uc *CorrelatorUsecase) correlate(ctx context.Context, rule *Rule, a *AlertRef, settings *Settings) error {
	spec := &rule.Spec
	businessID := spec.Incident.BusinessID
	if businessID == "" && spec.Incident.BusinessLabel != "" {
		if all, err := uc.d.Businesses.ListAll(ctx); err == nil {
			if b := matchBusiness(all, a.Labels[spec.Incident.BusinessLabel]); b != nil {
				businessID = b.ID
			}
		}
	}
	var upstream []string
	if a.CIID != "" && slices.Contains(spec.Correlation.Keys, "ci") {
		ups, err := uc.d.CMDB.Upstream(ctx, a.CIID)
		if err != nil {
			return err
		}
		upstream = ups
	}
	key := correlationKey(spec, a, businessID, upstream)
	// 1. 同规则同键的活跃故障：直接归并。
	if target, err := uc.findActive(ctx, rule.ID, key, spec.Correlation.WindowM, settings); err != nil {
		return err
	} else if target != nil {
		return uc.attachToIncident(ctx, target, a, rule)
	}
	// 2. 候选桶：达到门槛即开故障。
	var opened *Incident
	var c *Candidate
	err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		var err error
		if c, err = uc.d.Candidates.LockOpenTx(ctx, tx, rule.ID, key); err != nil {
			return err
		}
		if c == nil {
			id, err := idgen.NewID()
			if err != nil {
				return err
			}
			c = &Candidate{ID: id, RuleID: rule.ID, CorrelationKey: key, Status: CandidateOpen, FirstTime: uc.now()}
		}
		if !slices.Contains(c.AlertIDs, a.ID) {
			c.AlertIDs = append(c.AlertIDs, a.ID)
		}
		if a.CIID != "" && !slices.Contains(c.CIIDs, a.CIID) {
			c.CIIDs = append(c.CIIDs, a.CIID)
		}
		c.UpdateTime = uc.now()
		if len(c.AlertIDs) > 1000 {
			c.AlertIDs = c.AlertIDs[len(c.AlertIDs)-1000:]
		}
		return uc.d.Candidates.UpsertTx(ctx, tx, c)
	})
	if err != nil {
		return err
	}
	if len(c.AlertIDs) < int(spec.Correlation.MinAlerts) || (spec.Correlation.MinCIs > 1 && len(c.CIIDs) < int(spec.Correlation.MinCIs)) {
		return nil
	}
	// 3. 风暴保护：同规则一分钟内开出过多故障时不再开新的，挂到最近一条。
	recent, err := uc.d.Incidents.CountRecentByRule(ctx, rule.ID, uc.now().Add(-stormWindow))
	if err != nil {
		return err
	}
	if recent >= int(settings.StormThreshold) {
		return uc.storm(ctx, rule, a, c, settings)
	}
	refs, err := uc.inc.promotableRefs(ctx, c.AlertIDs, true)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		// 桶里的告警都已恢复或归了别的故障：关闭候选，下一条告警重新开桶。
		return uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
			return uc.d.Candidates.SetStatusTx(ctx, tx, c.ID, CandidateExpired, "")
		})
	}
	in := uc.inc.ruleCreateInput(ctx, rule, key, refs)
	opened, err = uc.inc.open(ctx, ActorSystem, "", in)
	if err != nil {
		if ErrVersionConflict.Is(err) || ErrAlertLinked.Is(err) {
			// 并发开出或告警已被别处关联：下一轮按活跃故障归并。
			return nil
		}
		return err
	}
	return uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		return uc.d.Candidates.SetStatusTx(ctx, tx, c.ID, CandidatePromoted, opened.ID)
	})
}

// findActive 规则 + 归并键对应的活跃故障（或恢复后仍在归并窗口内的）。
func (uc *CorrelatorUsecase) findActive(ctx context.Context, ruleID, key string, windowM int32, settings *Settings) (*Incident, error) {
	filter := IncidentFilter{RuleID: ruleID, CorrelationKey: key, Statuses: []int32{StatusOpen, StatusResponding, StatusMitigated, StatusResolved}}
	items, _, err := uc.d.Incidents.List(ctx, filter, 1, 10)
	if err != nil {
		return nil, err
	}
	var resolved *Incident
	for _, it := range items {
		if IsActive(it.Status) {
			return it, nil
		}
		if it.Status == StatusResolved && it.ResolveTime != nil && uc.now().Sub(*it.ResolveTime) <= time.Duration(windowM)*time.Minute {
			resolved = it
		}
	}
	if resolved != nil {
		// 归并窗口内再次触发：系统自动恢复的故障自动重开后归并；手动恢复的不重开（autoReopenIfNeeded 返回
		// false），落到下面的 nil——调用方按没有活跃故障走候选 / 开单。
		ok, err := uc.inc.autoReopenIfNeeded(ctx, resolved, settings)
		if err != nil {
			return nil, err
		}
		if ok {
			return uc.d.Incidents.Get(ctx, resolved.ID)
		}
	}
	return nil, nil
}

// attachToIncident 归并一条告警到故障。
func (uc *CorrelatorUsecase) attachToIncident(ctx context.Context, inc *Incident, a *AlertRef, rule *Rule) error {
	_, err := uc.inc.mutate(ctx, inc.ID, ActorSystem, "", func(m *mutation) error {
		if !IsActive(m.inc.Status) {
			return errNoOp
		}
		n, err := uc.d.Links.Count(ctx, m.inc.ID)
		if err != nil {
			return err
		}
		if n >= maxAlertsPerIncident {
			return errNoOp
		}
		ok, err := uc.inc.attachOne(ctx, m, a, LinkRule, "")
		if err != nil {
			if ErrAlertLinked.Is(err) {
				return errNoOp
			}
			return err
		}
		if !ok {
			return errNoOp
		}
		m.event(EventAlertAttach, EventDetail{AlertIDs: []string{a.ID}, AlertCount: 1, To: LinkRule, Text: "规则「" + rule.Spec.Name + "」归并"})
		m.hook("alert", map[string]any{"attached": []string{a.ID}})
		return nil
	})
	if errors.Is(err, errNoOp) {
		return nil
	}
	if err == nil {
		uc.inc.noteAlert(ctx, a.ID, "", inc)
	}
	return err
}

// storm 风暴归并：挂到规则最近开出的活跃故障并通知管理员。
func (uc *CorrelatorUsecase) storm(ctx context.Context, rule *Rule, a *AlertRef, c *Candidate, settings *Settings) error {
	items, _, err := uc.d.Incidents.List(ctx, IncidentFilter{RuleID: rule.ID, Statuses: []int32{StatusOpen, StatusResponding, StatusMitigated}}, 1, 1)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	target := items[0]
	err = uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		return uc.d.Candidates.SetStatusTx(ctx, tx, c.ID, CandidatePromoted, target.ID)
	})
	if err != nil {
		return err
	}
	_, err = uc.inc.mutate(ctx, target.ID, ActorSystem, "", func(m *mutation) error {
		// 通知里的关联告警数要是归并之后的。commit 取关联数走的是连接池，本事务提交前读不到刚写入的关联，
		// 所以自己记：事务开始后的第一条语句，读到的就是已提交的现有数（别的写入方都要先锁故障行）。
		existing, err := uc.d.Links.Count(ctx, m.inc.ID)
		if err != nil {
			return err
		}
		linked := 0
		for _, id := range c.AlertIDs {
			ref, err := uc.d.Alerts.Get(ctx, id)
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return err
			}
			ok, err := uc.inc.attachOne(ctx, m, ref, LinkRule, "")
			if err != nil && !ErrAlertLinked.Is(err) {
				return err
			}
			if ok {
				linked++
			}
		}
		m.alerts = existing + linked
		m.event(EventStorm, EventDetail{AlertCount: i32(len(c.AlertIDs)), Text: fmt.Sprintf("规则「%s」1 分钟内开出的故障超过 %d 条，后续告警归并到本故障", rule.Spec.Name, settings.StormThreshold)})
		m.notify(notice{
			action: NotifyStorm, dedup: fmt.Sprint(uc.now().Truncate(10 * time.Minute).Unix()), noGroups: true,
			extra: map[string]any{"rule_name": rule.Spec.Name}, users: uniq([]string{m.inc.CommanderID}),
		})
		return nil
	})
	_ = a
	return err
}

// reportLag 关联器落后告警（进程内去重 30 分钟）：写一条不带收件人的自监控通知，交由路由。
func (uc *CorrelatorUsecase) reportLag(ctx context.Context, last time.Time) {
	if uc.now().Sub(uc.lagNotified) < engineLagDedup {
		return
	}
	uc.lagNotified = uc.now()
	text := fmt.Sprintf("故障关联器落后告警事件 %s，请检查 core worker 负载", humanDuration(uc.now().Sub(last)))
	uc.d.Logger.WarnContext(ctx, "incident correlator lagging", "last_event_time", last)
	err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		_, err := uc.d.Notify.NotifyEvent(ctx, tx, NotifyEvent{
			BizID: notifyBizID, EventType: EventType(SeverityS3, NotifyEngine), Severity: 3, Title: "【故障关联器】落后告警事件",
			Payload:  jsonRaw(map[string]any{"text": text, "url": string(uc.d.Site) + "/incident"}),
			DedupKey: "incident:engine:lag:" + uc.now().Truncate(engineLagDedup).Format(time.RFC3339), GroupKey: "incident:engine",
		})
		return err
	})
	if err != nil {
		uc.d.Logger.ErrorContext(ctx, "incident correlator: report lag", "error", err)
	}
}
