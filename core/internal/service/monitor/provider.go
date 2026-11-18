// Package service 监控告警域 proto 接口实现：只做身份提取、参数编排与 proto ↔ 领域模型转换；
// 业务规则与数据范围判定全部收敛在 biz（ScopeUsecase）。
package service

import (
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is 监控告警域 service providers。
var ProviderSet = wire.NewSet(
	NewDatasourceService, NewRuleGroupService, NewMuteService, NewSubscriptionService,
	// 第二、三期：RuleService / AlertService 改由 Provide* 装配；六个新服务的 provider 集在各自的文件里。
	AlertP2ProviderSet, escalationProviders, ruleP2Providers, StatsProviders, InhibitProviderSet,
	wire.Struct(new(P2Services), "*"),
)

// P2Services 第二、三期新增的六个服务：合成一组交给 server.NewHTTPServer 注册（免得它的参数表再长六项）。
type P2Services struct {
	Escalation *EscalationService
	Inhibit    *InhibitService
	RecordRule *RecordRuleService
	AlertView  *AlertViewService
	Stats      *StatsService
	Engine     *EngineService
}

// optTime proto 时间戳 → *time.Time（未设置为 nil）。
func optTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

// protoTime *time.Time → proto 时间戳（nil 保持为空）。
func protoTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toProtoRefs(refs []biz.NamedRef) []*v1.NamedRef {
	out := make([]*v1.NamedRef, 0, len(refs))
	for _, r := range refs {
		out = append(out, &v1.NamedRef{Id: r.ID, Name: r.Name, Missing: r.Missing})
	}
	return out
}

func toMatchers(in []*v1.LabelMatcher) []mc.Matcher {
	out := make([]mc.Matcher, 0, len(in))
	for _, m := range in {
		out = append(out, mc.Matcher{Label: m.GetLabel(), Op: m.GetOp(), Value: m.GetValue(), Values: m.GetValues()})
	}
	return out
}

func toProtoMatchers(in []mc.Matcher) []*v1.LabelMatcher {
	out := make([]*v1.LabelMatcher, 0, len(in))
	for _, m := range in {
		out = append(out, &v1.LabelMatcher{Label: m.Label, Op: m.Op, Value: m.Value, Values: m.Values})
	}
	return out
}

func toEffective(in *v1.EffectiveTime) mc.Effective {
	out := mc.Effective{Timezone: in.GetTimezone(), Mode: in.GetMode(), Windows: make([]mc.TimeWindow, 0, len(in.GetWindows()))}
	for _, w := range in.GetWindows() {
		out.Windows = append(out.Windows, mc.TimeWindow{Days: w.GetDays(), Start: w.GetStart(), End: w.GetEnd()})
	}
	return out
}

func toProtoEffective(in mc.Effective) *v1.EffectiveTime {
	out := &v1.EffectiveTime{Timezone: in.Timezone, Mode: in.Mode, Windows: make([]*v1.TimeWindow, 0, len(in.Windows))}
	for _, w := range in.Windows {
		out.Windows = append(out.Windows, &v1.TimeWindow{Days: w.Days, Start: w.Start, End: w.End})
	}
	return out
}

// labelFilter 把 name=value 形式的过滤项拆成标签集（格式已由 proto 校验；同名取最后一项）。
func labelFilter(items []string) map[string]string {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]string, len(items))
	for _, it := range items {
		if name, value, ok := strings.Cut(it, "="); ok {
			out[name] = value
		}
	}
	return out
}

func toProtoAlert(a *biz.AlertView) *v1.Alert {
	return &v1.Alert{
		Id: a.ID, Fingerprint: a.Fingerprint, RuleId: a.RuleID, RuleName: a.RuleName, RuleExists: a.RuleExists,
		GroupId: a.GroupID, GroupName: a.GroupName, DatasourceId: a.DatasourceID, DatasourceName: a.DatasourceName,
		Severity: a.Severity, Status: a.Status, Labels: a.Labels, Annotations: a.Annotations, Expr: a.Expr,
		Value: a.Value, RecoveryValue: a.RecoveryValue,
		FirstFireTime: timestamppb.New(a.FirstFireTime), LastFireTime: timestamppb.New(a.LastFireTime), ResolveTime: protoTime(a.ResolveTime),
		DurationS: a.DurationS, Muted: a.Muted, MuteId: a.MuteID, MuteName: a.MuteName,
		NotifyCount: a.NotifyCount, LastNotifyTime: protoTime(a.LastNotifyTime),
		AckUserId: a.AckUser, AckUserName: a.AckUserName, AckTime: protoTime(a.AckTime), AckDurationS: a.AckDurationS,
		ClosedById: a.ClosedBy, ClosedByName: a.ClosedByName, CloseReason: a.CloseReason, Closing: a.Closing,
		Inhibited: a.Inhibited, InhibitKind: a.InhibitKind, InhibitId: a.InhibitID, InhibitName: a.InhibitName,
		InhibitAlertId: a.InhibitAlertID, InhibitAlertName: a.InhibitAlertName, CiId: a.CIID, CiName: a.CIName,
		EscalationLevel: a.EscalationLevel, TicketId: a.TicketID, TicketNumber: a.TicketNumber,
		WorkflowInstanceId: a.WorkflowInstanceID, AiSummary: a.AISummary, HandleState: a.HandleState(),
		CanManage: a.CanManage, CanAck: a.CanAck,
	}
}

func toProtoAlerts(alerts []*biz.AlertView) []*v1.Alert {
	out := make([]*v1.Alert, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, toProtoAlert(a))
	}
	return out
}
