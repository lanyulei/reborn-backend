package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ConfigService 实现 IncidentConfigServiceHTTPServer：触发规则、业务字典、预案、接入源、Webhook、设置。
type ConfigService struct {
	rules      *biz.RuleUsecase
	businesses *biz.BusinessUsecase
	playbooks  *biz.PlaybookUsecase
	sources    *biz.SourceUsecase
	webhooks   *biz.WebhookUsecase
	settings   *biz.SettingsUsecase
	incidents  *biz.IncidentUsecase
}

// NewConfigService new a ConfigService.
func NewConfigService(rules *biz.RuleUsecase, businesses *biz.BusinessUsecase, playbooks *biz.PlaybookUsecase, sources *biz.SourceUsecase,
	webhooks *biz.WebhookUsecase, settings *biz.SettingsUsecase, incidents *biz.IncidentUsecase,
) *ConfigService {
	return &ConfigService{rules: rules, businesses: businesses, playbooks: playbooks, sources: sources, webhooks: webhooks, settings: settings, incidents: incidents}
}

// ---- 触发规则 ----

func toRuleSpec(in *v1.RuleSpec) biz.RuleSpec {
	s := biz.RuleSpec{Name: in.GetName(), Description: in.GetDescription(), Sort: in.GetSort(), Enabled: in.GetEnabled()}
	if m := in.GetMatch(); m != nil {
		s.Match = biz.RuleMatch{MinSeverity: m.GetMinSeverity(), GroupIDs: m.GetGroupIds(), RuleIDs: m.GetRuleIds(), Matchers: toMatchers(m.GetMatchers()), ModelCodes: m.GetModelCodes()}
	}
	if c := in.GetCorrelation(); c != nil {
		s.Correlation = biz.RuleCorrelation{Keys: c.GetKeys(), WindowM: c.GetWindowM(), MinAlerts: c.GetMinAlerts(), MinCIs: c.GetMinCis()}
	}
	if i := in.GetIncident(); i != nil {
		s.Incident = biz.RuleIncident{Severity: i.GetSeverity(), TitleTemplate: i.GetTitleTemplate(), BusinessID: i.GetBusinessId(), BusinessLabel: i.GetBusinessLabel(), IsDrill: i.GetIsDrill()}
	}
	if r := in.GetResponse(); r != nil {
		s.Response = biz.RuleResponse{
			CommanderScheduleID: r.GetCommanderScheduleId(), CommanderUserID: r.GetCommanderUserId(), ResponderBackup: r.GetResponderBackup(),
			ResponderOncallGroupID: r.GetResponderOncallGroupId(), ResponderCIOwner: r.GetResponderCiOwner(), ResponderUserIDs: r.GetResponderUserIds(), Groups: r.GetGroups(),
		}
	}
	if a := in.GetAction(); a != nil {
		s.Action = biz.RuleAction{PlaybookID: a.GetPlaybookId(), PlaybookMode: a.GetPlaybookMode(), AutoResolve: a.GetAutoResolve(), AISummary: a.GetAiSummary(), Agent: a.GetAgent()}
	}
	return s
}

func toProtoRuleSpec(s *biz.RuleSpec) *v1.RuleSpec {
	return &v1.RuleSpec{
		Name: s.Name, Description: s.Description, Sort: s.Sort, Enabled: s.Enabled,
		Match:       &v1.RuleMatch{MinSeverity: s.Match.MinSeverity, GroupIds: s.Match.GroupIDs, RuleIds: s.Match.RuleIDs, Matchers: toProtoMatchers(s.Match.Matchers), ModelCodes: s.Match.ModelCodes},
		Correlation: &v1.RuleCorrelation{Keys: s.Correlation.Keys, WindowM: s.Correlation.WindowM, MinAlerts: s.Correlation.MinAlerts, MinCis: s.Correlation.MinCIs},
		Incident:    &v1.RuleIncident{Severity: s.Incident.Severity, TitleTemplate: s.Incident.TitleTemplate, BusinessId: s.Incident.BusinessID, BusinessLabel: s.Incident.BusinessLabel, IsDrill: s.Incident.IsDrill},
		Response: &v1.RuleResponseSpec{
			CommanderScheduleId: s.Response.CommanderScheduleID, CommanderUserId: s.Response.CommanderUserID, ResponderBackup: s.Response.ResponderBackup,
			ResponderOncallGroupId: s.Response.ResponderOncallGroupID, ResponderCiOwner: s.Response.ResponderCIOwner, ResponderUserIds: s.Response.ResponderUserIDs, Groups: s.Response.Groups,
		},
		Action: &v1.RuleAction{PlaybookId: s.Action.PlaybookID, PlaybookMode: s.Action.PlaybookMode, AutoResolve: s.Action.AutoResolve, AiSummary: s.Action.AISummary, Agent: s.Action.Agent},
	}
}

func toProtoRule(v *biz.RuleView) *v1.Rule {
	return &v1.Rule{
		Id: v.ID, Spec: toProtoRuleSpec(&v.Spec), Business: named(v.Spec.Incident.BusinessID, v.BusinessName), Playbook: named(v.Spec.Action.PlaybookID, v.PlaybookName),
		CommanderSchedule: named(v.Spec.Response.CommanderScheduleID, v.ScheduleName), CommanderUser: named(v.Spec.Response.CommanderUserID, v.CommanderName),
		IncidentCount: v.IncidentCount, Version: v.Version, CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
		UpdateUser: named(v.UpdateUser, v.UpdateUserName),
	}
}

// ListRules implements proto IncidentConfigService/ListRules.
func (s *ConfigService) ListRules(ctx context.Context, req *v1.ListRulesRequest) (*v1.ListRulesResponse, error) {
	items, total, err := s.rules.List(ctx, req.GetKeyword(), req.Enabled, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListRulesResponse{Items: make([]*v1.Rule, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoRule(it))
	}
	return out, nil
}

// GetRule implements proto IncidentConfigService/GetRule.
func (s *ConfigService) GetRule(ctx context.Context, req *v1.GetRuleRequest) (*v1.RuleResponse, error) {
	v, err := s.rules.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RuleResponse{Rule: toProtoRule(v)}, nil
}

// CreateRule implements proto IncidentConfigService/CreateRule.
func (s *ConfigService) CreateRule(ctx context.Context, req *v1.CreateRuleRequest) (*v1.RuleResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.rules.Create(ctx, uid, toRuleSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.RuleResponse{Rule: toProtoRule(v)}, nil
}

// UpdateRule implements proto IncidentConfigService/UpdateRule.
func (s *ConfigService) UpdateRule(ctx context.Context, req *v1.UpdateRuleRequest) (*v1.RuleResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.rules.Update(ctx, uid, req.GetId(), toRuleSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.RuleResponse{Rule: toProtoRule(v)}, nil
}

// DeleteRule implements proto IncidentConfigService/DeleteRule.
func (s *ConfigService) DeleteRule(ctx context.Context, req *v1.DeleteRuleRequest) (*v1.EmptyResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.rules.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.EmptyResponse{}, nil
}

// TestRule implements proto IncidentConfigService/TestRule.
func (s *ConfigService) TestRule(ctx context.Context, req *v1.TestRuleRequest) (*v1.TestRuleResponse, error) {
	res, err := s.rules.Test(ctx, toRuleSpec(req.GetSpec()), int(req.GetHours()))
	if err != nil {
		return nil, err
	}
	out := &v1.TestRuleResponse{Scanned: int32(res.Scanned), Matched: int32(res.Matched)} // #nosec G115 -- 扫描有上限
	for _, g := range res.Groups {
		pg := &v1.TestRuleGroup{CorrelationKey: g.Key, AlertCount: int32(len(g.Alerts)), CiCount: int32(g.CIs), WouldOpen: g.WouldOpen, Title: g.Title, Severity: g.Severity} // #nosec G115 -- 有上限
		for _, a := range g.Alerts {
			pg.Alerts = append(pg.Alerts, toProtoAlertItem(&biz.AlertItem{Link: &biz.AlertLink{AlertID: a.ID, RuleName: a.RuleName, Severity: a.Severity, CIID: a.CIID}, Alert: a}))
		}
		out.Groups = append(out.Groups, pg)
	}
	return out, nil
}

// ---- 业务字典 ----

func toBusiness(in *v1.BusinessSpec) biz.Business {
	return biz.Business{
		Code: in.GetCode(), Name: in.GetName(), Description: in.GetDescription(), OwnerID: in.GetOwnerId(), ScheduleID: in.GetScheduleId(),
		Groups: in.GetGroups(), CIID: in.GetCiId(), LabelValues: in.GetLabelValues(), Sort: in.GetSort(), Enabled: in.GetEnabled(),
	}
}

func toProtoBusiness(v *biz.BusinessView) *v1.Business {
	return &v1.Business{
		Id: v.ID, Code: v.Code, Name: v.Name, Description: v.Description, Owner: named(v.OwnerID, v.OwnerName), Schedule: named(v.ScheduleID, v.ScheduleName),
		Groups: v.Groups, Ci: named(v.CIID, v.CIName), LabelValues: v.LabelValues, Sort: v.Sort, Enabled: v.Enabled, Version: v.Version,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
	}
}

// ListBusinesses implements proto IncidentConfigService/ListBusinesses.
func (s *ConfigService) ListBusinesses(ctx context.Context, req *v1.ListBusinessesRequest) (*v1.ListBusinessesResponse, error) {
	items, total, err := s.businesses.List(ctx, req.GetKeyword(), req.Enabled, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListBusinessesResponse{Items: make([]*v1.Business, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoBusiness(it))
	}
	return out, nil
}

// CreateBusiness implements proto IncidentConfigService/CreateBusiness.
func (s *ConfigService) CreateBusiness(ctx context.Context, req *v1.CreateBusinessRequest) (*v1.BusinessResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.businesses.Create(ctx, uid, toBusiness(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.BusinessResponse{Business: toProtoBusiness(v)}, nil
}

// UpdateBusiness implements proto IncidentConfigService/UpdateBusiness.
func (s *ConfigService) UpdateBusiness(ctx context.Context, req *v1.UpdateBusinessRequest) (*v1.BusinessResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.businesses.Update(ctx, uid, req.GetId(), toBusiness(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.BusinessResponse{Business: toProtoBusiness(v)}, nil
}

// DeleteBusiness implements proto IncidentConfigService/DeleteBusiness.
func (s *ConfigService) DeleteBusiness(ctx context.Context, req *v1.DeleteBusinessRequest) (*v1.EmptyResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.businesses.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.EmptyResponse{}, nil
}

// ---- 预案 ----

func toPlaybookSpec(in *v1.PlaybookSpec) biz.PlaybookSpec {
	s := biz.PlaybookSpec{Name: in.GetName(), Description: in.GetDescription(), Doc: in.GetDoc(), AgentUserID: in.GetAgentUserId(), Enabled: in.GetEnabled()}
	for _, st := range in.GetSteps() {
		s.Steps = append(s.Steps, biz.PlaybookStep{Name: st.GetName(), Kind: st.GetKind(), Ref: st.GetRef(), Params: st.GetParams(), Approval: st.GetApproval(), Risk: st.GetRisk(), TargetMode: st.GetTargetMode()})
	}
	if m := in.GetMatch(); m != nil {
		s.Match = biz.PlaybookMatch{Matchers: toMatchers(m.GetMatchers()), ModelCodes: m.GetModelCodes(), BusinessIDs: m.GetBusinessIds()}
	}
	return s
}

func toProtoPlaybook(v *biz.PlaybookView) *v1.Playbook {
	spec := &v1.PlaybookSpec{
		Name: v.Spec.Name, Description: v.Spec.Description, Doc: v.Spec.Doc, AgentUserId: v.Spec.AgentUserID, Enabled: v.Spec.Enabled,
		Match: &v1.PlaybookMatch{Matchers: toProtoMatchers(v.Spec.Match.Matchers), ModelCodes: v.Spec.Match.ModelCodes, BusinessIds: v.Spec.Match.BusinessIDs},
	}
	out := &v1.Playbook{
		Id: v.ID, Spec: spec, AgentUser: named(v.Spec.AgentUserID, v.AgentUserName), ExecCount: v.ExecCount, Version: v.Version,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime), UpdateUser: named(v.UpdateUser, v.UpdateUserName),
	}
	for i, st := range v.Spec.Steps {
		spec.Steps = append(spec.Steps, &v1.PlaybookStep{Name: st.Name, Kind: st.Kind, Ref: st.Ref, Params: st.Params, Approval: st.Approval, Risk: st.Risk, TargetMode: st.TargetMode})
		name := ""
		if i < len(v.StepNames) {
			name = v.StepNames[i]
		}
		out.StepRefs = append(out.StepRefs, &v1.NamedRef{Id: st.Ref, Name: name, Missing: name == ""})
	}
	for _, id := range v.Spec.Match.BusinessIDs {
		out.BusinessRefs = append(out.BusinessRefs, &v1.NamedRef{Id: id, Name: v.BusinessNames[id], Missing: v.BusinessNames[id] == ""})
	}
	return out
}

// ListPlaybooks implements proto IncidentConfigService/ListPlaybooks.
func (s *ConfigService) ListPlaybooks(ctx context.Context, req *v1.ListPlaybooksRequest) (*v1.ListPlaybooksResponse, error) {
	items, total, err := s.playbooks.List(ctx, req.GetKeyword(), req.Enabled, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListPlaybooksResponse{Items: make([]*v1.Playbook, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoPlaybook(it))
	}
	return out, nil
}

// GetPlaybook implements proto IncidentConfigService/GetPlaybook.
func (s *ConfigService) GetPlaybook(ctx context.Context, req *v1.GetPlaybookRequest) (*v1.PlaybookResponse, error) {
	v, err := s.playbooks.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.PlaybookResponse{Playbook: toProtoPlaybook(v)}, nil
}

// CreatePlaybook implements proto IncidentConfigService/CreatePlaybook.
func (s *ConfigService) CreatePlaybook(ctx context.Context, req *v1.CreatePlaybookRequest) (*v1.PlaybookResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.playbooks.Create(ctx, uid, toPlaybookSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.PlaybookResponse{Playbook: toProtoPlaybook(v)}, nil
}

// UpdatePlaybook implements proto IncidentConfigService/UpdatePlaybook.
func (s *ConfigService) UpdatePlaybook(ctx context.Context, req *v1.UpdatePlaybookRequest) (*v1.PlaybookResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.playbooks.Update(ctx, uid, req.GetId(), toPlaybookSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.PlaybookResponse{Playbook: toProtoPlaybook(v)}, nil
}

// DeletePlaybook implements proto IncidentConfigService/DeletePlaybook.
func (s *ConfigService) DeletePlaybook(ctx context.Context, req *v1.DeletePlaybookRequest) (*v1.EmptyResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.playbooks.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.EmptyResponse{}, nil
}

// ListPlaybookTargets implements proto IncidentConfigService/ListPlaybookTargets.
func (s *ConfigService) ListPlaybookTargets(ctx context.Context, _ *v1.ListPlaybookTargetsRequest) (*v1.ListPlaybookTargetsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.playbooks.Targets(ctx, uid)
	if err != nil {
		return nil, err
	}
	out := &v1.ListPlaybookTargetsResponse{Items: make([]*v1.PlaybookTarget, 0, len(items))}
	for _, t := range items {
		out.Items = append(out.Items, &v1.PlaybookTarget{Kind: t.Kind, Ref: t.Ref, Name: t.Name, Description: t.Description, ParamNames: t.ParamNames})
	}
	return out, nil
}

// ---- 接入源 ----

func toSourceSpec(in *v1.SourceSpec) biz.SourceSpec {
	return biz.SourceSpec{Name: in.GetName(), Description: in.GetDescription(), DefaultSeverity: in.GetDefaultSeverity(), BusinessID: in.GetBusinessId(), Groups: in.GetGroups(), AllowedIPs: in.GetAllowedIps(), Enabled: in.GetEnabled()}
}

func toProtoSource(v *biz.SourceView) *v1.Source {
	return &v1.Source{
		Id: v.ID, Name: v.Name, Description: v.Description, TokenPrefix: v.TokenPrefix, DefaultSeverity: v.DefaultSeverity, Business: named(v.BusinessID, v.BusinessName),
		Groups: v.Groups, AllowedIps: v.AllowedIPs, Enabled: v.Enabled, LastUsedTime: protoTime(v.LastUsedTime), Version: v.Version,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
	}
}

// ListSources implements proto IncidentConfigService/ListSources.
func (s *ConfigService) ListSources(ctx context.Context, _ *v1.ListSourcesRequest) (*v1.ListSourcesResponse, error) {
	items, err := s.sources.List(ctx)
	if err != nil {
		return nil, err
	}
	out := &v1.ListSourcesResponse{Items: make([]*v1.Source, 0, len(items))}
	for _, it := range items {
		out.Items = append(out.Items, toProtoSource(it))
	}
	return out, nil
}

// CreateSource implements proto IncidentConfigService/CreateSource.
func (s *ConfigService) CreateSource(ctx context.Context, req *v1.CreateSourceRequest) (*v1.SourceTokenResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, token, err := s.sources.Create(ctx, uid, toSourceSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.SourceTokenResponse{Source: toProtoSource(v), Token: token}, nil
}

// UpdateSource implements proto IncidentConfigService/UpdateSource.
func (s *ConfigService) UpdateSource(ctx context.Context, req *v1.UpdateSourceRequest) (*v1.SourceResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.sources.Update(ctx, uid, req.GetId(), toSourceSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.SourceResponse{Source: toProtoSource(v)}, nil
}

// ResetSourceToken implements proto IncidentConfigService/ResetSourceToken.
func (s *ConfigService) ResetSourceToken(ctx context.Context, req *v1.ResetSourceTokenRequest) (*v1.SourceTokenResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, token, err := s.sources.ResetToken(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.SourceTokenResponse{Source: toProtoSource(v), Token: token}, nil
}

// DeleteSource implements proto IncidentConfigService/DeleteSource.
func (s *ConfigService) DeleteSource(ctx context.Context, req *v1.DeleteSourceRequest) (*v1.EmptyResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.sources.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.EmptyResponse{}, nil
}

// ---- Webhook ----

func toProtoWebhook(w *biz.Webhook) *v1.Webhook {
	return &v1.Webhook{
		Id: w.ID, Name: w.Name, Url: w.URL, HasSecret: len(w.Secret) > 0, Events: w.Events, Enabled: w.Enabled, Version: w.Version,
		CreateTime: timestamppb.New(w.CreateTime), UpdateTime: timestamppb.New(w.UpdateTime), Pending: w.Pending, Failed: w.Failed,
	}
}

// ListWebhooks implements proto IncidentConfigService/ListWebhooks.
func (s *ConfigService) ListWebhooks(ctx context.Context, _ *v1.ListWebhooksRequest) (*v1.ListWebhooksResponse, error) {
	items, err := s.webhooks.List(ctx)
	if err != nil {
		return nil, err
	}
	out := &v1.ListWebhooksResponse{Items: make([]*v1.Webhook, 0, len(items))}
	for _, w := range items {
		out.Items = append(out.Items, toProtoWebhook(w))
	}
	return out, nil
}

func toWebhookSpec(in *v1.WebhookSpec) biz.WebhookSpec {
	return biz.WebhookSpec{Name: in.GetName(), URL: in.GetUrl(), Secret: in.GetSecret(), Events: in.GetEvents(), Enabled: in.GetEnabled()}
}

// CreateWebhook implements proto IncidentConfigService/CreateWebhook.
func (s *ConfigService) CreateWebhook(ctx context.Context, req *v1.CreateWebhookRequest) (*v1.WebhookResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	w, err := s.webhooks.Create(ctx, uid, toWebhookSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.WebhookResponse{Webhook: toProtoWebhook(w)}, nil
}

// UpdateWebhook implements proto IncidentConfigService/UpdateWebhook.
func (s *ConfigService) UpdateWebhook(ctx context.Context, req *v1.UpdateWebhookRequest) (*v1.WebhookResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	w, err := s.webhooks.Update(ctx, uid, req.GetId(), toWebhookSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.WebhookResponse{Webhook: toProtoWebhook(w)}, nil
}

// DeleteWebhook implements proto IncidentConfigService/DeleteWebhook.
func (s *ConfigService) DeleteWebhook(ctx context.Context, req *v1.DeleteWebhookRequest) (*v1.EmptyResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.webhooks.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.EmptyResponse{}, nil
}

// ---- 设置 ----

func toProtoSettings(s *biz.Settings) *v1.Settings {
	out := &v1.Settings{
		Severities: map[int32]*v1.SeveritySetting{}, FallbackScheduleId: s.FallbackScheduleID, StormThreshold: s.StormThreshold, CandidateTtlM: s.CandidateTTLMinutes,
		ReopenWindowH: s.ReopenWindowHours, AutoCloseDays: s.AutoCloseDays, PersonalEvents: s.PersonalEvents, ProposalTtlM: s.ProposalTTLMinutes,
		VerifyWindowM: s.VerifyWindowMinutes, ChangeLookbackH: s.ChangeLookbackHours, AgentMaxFindings: s.AgentMaxFindings, Version: s.Version,
		AgentAutoCooldownM: s.AgentAutoCooldownMinutes, AgentAutoMaxRunning: s.AgentAutoMaxRunning, AgentAutoMaxRunningPerBusiness: s.AgentAutoMaxRunningPerBusiness,
	}
	for sev, v := range s.Severities {
		out.Severities[sev] = &v1.SeveritySetting{RespondM: v.RespondM, StaleM: v.StaleM, FirstUpdateM: v.FirstUpdateM, ReviewDays: v.ReviewDays, AllowAutoResolve: v.AllowAutoResolve, AllowAutoApprove: v.AllowAutoApprove}
	}
	for _, rc := range s.RootCauses {
		out.RootCauses = append(out.RootCauses, &v1.RootCause{Code: rc.Code, Name: rc.Name})
	}
	return out
}

func toSettings(in *v1.Settings) *biz.Settings {
	// personal_events 是空列表时也要保存：proto 的空 repeated 字段解码后是 nil，不转成非 nil 的空切片会被当成
	//「没保存过」而落回缺省值——管理员取消全部个人提醒后，界面显示已关闭，实际仍按缺省发送。
	personalEvents := in.GetPersonalEvents()
	if personalEvents == nil {
		personalEvents = []string{}
	}
	s := &biz.Settings{
		Severities: map[int32]biz.SeveritySetting{}, FallbackScheduleID: in.GetFallbackScheduleId(), StormThreshold: in.GetStormThreshold(), CandidateTTLMinutes: in.GetCandidateTtlM(),
		ReopenWindowHours: in.GetReopenWindowH(), AutoCloseDays: in.GetAutoCloseDays(), PersonalEvents: personalEvents, ProposalTTLMinutes: in.GetProposalTtlM(),
		VerifyWindowMinutes: in.GetVerifyWindowM(), ChangeLookbackHours: in.GetChangeLookbackH(), AgentMaxFindings: in.GetAgentMaxFindings(), Version: in.GetVersion(),
		AgentAutoCooldownMinutes: in.GetAgentAutoCooldownM(), AgentAutoMaxRunning: in.GetAgentAutoMaxRunning(), AgentAutoMaxRunningPerBusiness: in.GetAgentAutoMaxRunningPerBusiness(),
	}
	for sev, v := range in.GetSeverities() {
		s.Severities[sev] = biz.SeveritySetting{RespondM: v.GetRespondM(), StaleM: v.GetStaleM(), FirstUpdateM: v.GetFirstUpdateM(), ReviewDays: v.GetReviewDays(), AllowAutoResolve: v.GetAllowAutoResolve(), AllowAutoApprove: v.GetAllowAutoApprove()}
	}
	for _, rc := range in.GetRootCauses() {
		s.RootCauses = append(s.RootCauses, biz.RootCause{Code: rc.GetCode(), Name: rc.GetName()})
	}
	return s
}

func (s *ConfigService) settingsResponse(ctx context.Context, st *biz.Settings) (*v1.SettingsResponse, error) {
	out := &v1.SettingsResponse{Settings: toProtoSettings(st)}
	if st.FallbackScheduleID != "" {
		o, err := s.incidents.Options(ctx, []string{"schedules"})
		if err != nil {
			return nil, err
		}
		for _, sc := range o.Schedules {
			if sc.ID == st.FallbackScheduleID {
				out.FallbackSchedule = &v1.NamedRef{Id: sc.ID, Name: sc.Name}
			}
		}
		if out.FallbackSchedule == nil {
			out.FallbackSchedule = &v1.NamedRef{Id: st.FallbackScheduleID, Missing: true}
		}
	}
	return out, nil
}

// GetSettings implements proto IncidentConfigService/GetSettings.
func (s *ConfigService) GetSettings(ctx context.Context, _ *v1.GetSettingsRequest) (*v1.SettingsResponse, error) {
	st, err := s.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	return s.settingsResponse(ctx, st)
}

// UpdateSettings implements proto IncidentConfigService/UpdateSettings.
func (s *ConfigService) UpdateSettings(ctx context.Context, req *v1.UpdateSettingsRequest) (*v1.SettingsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	in := toSettings(req.GetSettings())
	st, err := s.settings.Update(ctx, uid, in, in.Version)
	if err != nil {
		return nil, err
	}
	return s.settingsResponse(ctx, st)
}
