package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
)

// ruleP2Providers 规则第二、三期的 service providers（由包的 ProviderSet 引用；ProvideRuleService 代替
// NewRuleService）。
var ruleP2Providers = wire.NewSet(ProvideRuleService)

// ruleP2 规则的第二、三期接口（内置模板、模拟触发、配置候选项）的实现，内嵌在 RuleService 里。
// 未经 ProvideRuleService 装配（options / simulate 为 nil）时模拟触发与候选项返回 501。
type ruleP2 struct {
	packs    *biz.RuleUsecase
	options  *biz.RuleOptionUsecase
	simulate *biz.SimulateUsecase
}

// ProvideRuleService 装配带第二、三期用例的 RuleService（wire 用它代替 NewRuleService）。
func ProvideRuleService(uc *biz.RuleUsecase, targets *biz.TargetUsecase, options *biz.RuleOptionUsecase, simulate *biz.SimulateUsecase) *RuleService {
	s := NewRuleService(uc, targets)
	s.ruleP2 = ruleP2{packs: uc, options: options, simulate: simulate}
	return s
}

// errNotWired 第二、三期的用例没有装配（只在以 NewRuleService 构造的测试装置里出现）。
func errNotWired() error { return v1.ErrorMonitorNotImplemented("not available in this deployment") }

func toProtoPack(p biz.TemplatePack) *v1.RuleTemplatePack {
	return &v1.RuleTemplatePack{Code: p.Code, Name: p.Name, Description: p.Description, Category: p.Category, RuleCount: p.RuleCount}
}

// ListRuleTemplatePacks implements proto MonitorRuleService/ListRuleTemplatePacks.
func (s *ruleP2) ListRuleTemplatePacks(context.Context, *v1.ListRuleTemplatePacksRequest) (*v1.ListRuleTemplatePacksResponse, error) {
	packs := s.packs.TemplatePacks()
	resp := &v1.ListRuleTemplatePacksResponse{Items: make([]*v1.RuleTemplatePack, 0, len(packs))}
	for _, p := range packs {
		resp.Items = append(resp.Items, toProtoPack(p))
	}
	return resp, nil
}

// GetRuleTemplatePack implements proto MonitorRuleService/GetRuleTemplatePack.
func (s *ruleP2) GetRuleTemplatePack(_ context.Context, req *v1.GetRuleTemplatePackRequest) (*v1.GetRuleTemplatePackResponse, error) {
	d, err := s.packs.TemplatePack(req.GetCode())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetRuleTemplatePackResponse{Pack: toProtoPack(d.TemplatePack), Content: d.Content, Rules: make([]*v1.RuleTemplateItem, 0, len(d.Rules))}
	for _, r := range d.Rules {
		resp.Rules = append(resp.Rules, &v1.RuleTemplateItem{Name: r.Name, Expr: r.Expr, Severity: r.Severity, ForS: r.ForS, Summary: r.Summary})
	}
	return resp, nil
}

// SimulateRule implements proto MonitorRuleService/SimulateRule.
func (s *ruleP2) SimulateRule(ctx context.Context, req *v1.SimulateRuleRequest) (*v1.SimulateRuleResponse, error) {
	if s.simulate == nil {
		return nil, errNotWired()
	}
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	res, err := s.simulate.Simulate(ctx, userID, biz.SimulateInput{
		Spec: toRuleSpec(req.GetSpec()), DatasourceID: req.GetDatasourceId(), SampleLabels: req.GetSampleLabels(),
		SampleValue: req.GetSampleValue(), Send: req.GetSend(), RuleID: req.GetRuleId(),
	})
	if err != nil {
		return nil, err
	}
	resp := &v1.SimulateRuleResponse{
		Synthetic: res.Synthetic, InEffect: res.InEffect, EventTotal: res.EventTotal, Recipients: toProtoRefs(res.Recipients),
		RecipientNotes: res.RecipientNotes, RouteMatched: res.RouteMatched, Sent: res.Sent, NotifyEventIds: res.NotifyEventIDs,
		Stages:        make([]*v1.SimulateStage, 0, len(res.Stages)),
		Queries:       make([]*v1.SimulateQueryResult, 0, len(res.Queries)),
		Events:        make([]*v1.SimulateEvent, 0, len(res.Events)),
		Subscriptions: make([]*v1.SimulateSubscription, 0, len(res.Subscriptions)),
		Escalation:    make([]*v1.SimulateEscalationLevel, 0, len(res.Escalation)),
	}
	for _, st := range res.Stages {
		resp.Stages = append(resp.Stages, &v1.SimulateStage{Stage: st.Stage, Status: st.Status, Message: st.Message})
	}
	for _, q := range res.Queries {
		resp.Queries = append(resp.Queries, &v1.SimulateQueryResult{
			Index: q.Index, Severity: q.Severity, Series: q.Series, Truncated: q.Truncated, Error: q.Error, DurationMs: q.DurationMS,
		})
	}
	for _, ev := range res.Events {
		item := &v1.SimulateEvent{
			Labels: ev.Labels, SourceLabels: ev.SourceLabels, Severity: ev.Severity, Value: ev.Value, Fingerprint: ev.Fingerprint,
			Annotations: ev.Annotations, Outcome: ev.Outcome, Reason: ev.Reason, CiStatus: ev.CIStatus,
			InhibitId: ev.InhibitID, InhibitAlertId: ev.InhibitAlertID, MuteId: ev.MuteID,
		}
		if ev.CI != nil {
			item.Ci = &v1.NamedRef{Id: ev.CI.ID, Name: ev.CI.Name}
		}
		resp.Events = append(resp.Events, item)
	}
	for _, sub := range res.Subscriptions {
		resp.Subscriptions = append(resp.Subscriptions, &v1.SimulateSubscription{Id: sub.ID, Name: sub.Name, Recipients: toProtoRefs(sub.Recipients)})
	}
	for _, l := range res.Escalation {
		resp.Escalation = append(resp.Escalation, &v1.SimulateEscalationLevel{
			Level: l.Level, AfterM: l.AfterM, Trigger: l.Trigger, Recipients: toProtoRefs(l.Recipients),
		})
	}
	if resp.RecipientNotes == nil {
		resp.RecipientNotes = []string{}
	}
	if resp.NotifyEventIds == nil {
		resp.NotifyEventIds = []string{}
	}
	return resp, nil
}

// ListMonitorRuleOptions implements proto MonitorRuleService/ListMonitorRuleOptions.
func (s *ruleP2) ListMonitorRuleOptions(ctx context.Context, _ *v1.ListMonitorRuleOptionsRequest) (*v1.ListMonitorRuleOptionsResponse, error) {
	if s.options == nil {
		return nil, errNotWired()
	}
	opts, err := s.options.Options(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListMonitorRuleOptionsResponse{
		CiEnvs: opts.CIEnvs, CiMatchAttrs: opts.CIMatchAttrs, AiAvailable: opts.AIAvailable,
		TicketTemplates: make([]*v1.MonitorTicketTemplateOption, 0, len(opts.TicketTemplates)),
		Workflows:       make([]*v1.MonitorWorkflowOption, 0, len(opts.Workflows)),
		CiModels:        make([]*v1.MonitorCIModelOption, 0, len(opts.CIModels)),
	}
	for _, t := range opts.TicketTemplates {
		resp.TicketTemplates = append(resp.TicketTemplates, &v1.MonitorTicketTemplateOption{Id: t.ID, Name: t.Name, CategoryName: t.CategoryName})
	}
	for _, w := range opts.Workflows {
		resp.Workflows = append(resp.Workflows, &v1.MonitorWorkflowOption{Key: w.Key, Name: w.Name})
	}
	for _, m := range opts.CIModels {
		resp.CiModels = append(resp.CiModels, &v1.MonitorCIModelOption{Code: m.Code, Name: m.Name})
	}
	return resp, nil
}
