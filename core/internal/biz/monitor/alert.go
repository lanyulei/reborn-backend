package biz

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 告警事件查询的限额。
const (
	// alertHistoryWindow 未给时间范围时历史告警的回看时长（活跃告警不受限）。
	alertHistoryWindow = 7 * 24 * time.Hour
	// alertMaxRange 时间范围的跨度上限。
	alertMaxRange = 90 * 24 * time.Hour
	// maxNotifyLogs 事件详情返回的通知记录条数上限（超过时保留最新的这些条）。
	maxNotifyLogs = 200
	// maxAggregates 聚合视图返回的行数上限。
	maxAggregates = 200
	// previewScanLimit 预览时扫描的事件数上限（标签匹配在内存里完成）；命中总数因此也以它为上限。
	previewScanLimit = 2000
	// previewItems 预览返回的事件条数上限。
	previewItems = 50
)

// severityLabels 级别中文名（全平台统一口径）。
var severityLabels = [...]string{"", "提示", "警告", "严重", "致命"}

// SeverityLabel 级别中文名。
func SeverityLabel(severity int32) string {
	if !mc.ValidSeverity(severity) {
		return ""
	}
	return severityLabels[severity]
}

// AlertUsecase 告警事件查询用例（只读）。只返回数据范围内的事件；详情另允许该事件的通知对象查看。
type AlertUsecase struct {
	perms  Permissions
	scopes *ScopeUsecase
	alerts AlertReader
	stats  AlertStats
	logs   NotifyLogReader
	rules  RuleLookup
	mutes  MuteReader
	dir    Directory
	now    func() time.Time
	// decorator 第二、三期的展示名（认领人、抑制规则、资源、工单编号等）；未接上时不补。
	decorator AlertViewDecorator
	// detail 事件详情的时间线 / 资源卡片 / 工单卡片；未接上时 GetDetail 不带这些内容。
	detail *AlertDetailUsecase
}

// WithP2 接上第二、三期的协作者：List / Get 返回的事件带上认领人、资源、工单编号等展示名，GetDetail
// 带上时间线、资源卡片、影响面与工单卡片。
func (uc *AlertUsecase) WithP2(decorator AlertViewDecorator, detail *AlertDetailUsecase) *AlertUsecase {
	uc.decorator, uc.detail = decorator, detail
	return uc
}

// ProvideAlertUsecase 装配用：NewAlertUsecase + WithP2。
func ProvideAlertUsecase(scopes *ScopeUsecase, alerts AlertReader, stats AlertStats, logs NotifyLogReader, rules RuleLookup, mutes MuteReader,
	dir Directory, perms Permissions, decorator AlertViewDecorator, detail *AlertDetailUsecase,
) *AlertUsecase {
	return NewAlertUsecase(scopes, alerts, stats, logs, rules, mutes, dir, perms).WithP2(decorator, detail)
}

// decorate 补齐第二、三期的展示名（未接上补齐器时原样返回）。
func (uc *AlertUsecase) decorate(ctx context.Context, views []*AlertView) error {
	if uc.decorator == nil || len(views) == 0 {
		return nil
	}
	return uc.decorator.Decorate(ctx, views)
}

// NewAlertUsecase new an AlertUsecase.
func NewAlertUsecase(scopes *ScopeUsecase, alerts AlertReader, stats AlertStats, logs NotifyLogReader, rules RuleLookup, mutes MuteReader,
	dir Directory, perms Permissions,
) *AlertUsecase {
	return &AlertUsecase{
		scopes: scopes, alerts: alerts, stats: stats, logs: logs, rules: rules, mutes: mutes, dir: dir, perms: perms, now: time.Now,
	}
}

// AlertListQuery 告警列表查询入参。
type AlertListQuery struct {
	GroupID string
	Mine    bool
	AlertFilter
}

// boundTimeRange 规整时间范围：都没给时活跃告警不受限、历史告警只回看 alertHistoryWindow；
// 只给一端时补齐另一端；跨度超过 alertMaxRange 返回 ErrTimeRangeInvalid。
func boundTimeRange(f *AlertFilter, now time.Time) error {
	if f.FireStart == nil && f.FireEnd == nil {
		since := now.Add(-alertHistoryWindow)
		f.HistorySince = &since
		return nil
	}
	if f.FireEnd == nil {
		f.FireEnd = &now
	}
	if f.FireStart == nil {
		start := f.FireEnd.Add(-alertHistoryWindow)
		f.FireStart = &start
	}
	if f.FireEnd.Before(*f.FireStart) {
		return detail(ErrTimeRangeInvalid, "fire_time_start must not be after fire_time_end")
	}
	if f.FireEnd.Sub(*f.FireStart) > alertMaxRange {
		return detail(ErrTimeRangeInvalid, "time range exceeds 90 days")
	}
	return nil
}

// List 分页查询告警事件（按最近触发时间倒序）。
func (uc *AlertUsecase) List(ctx context.Context, userID string, q AlertListQuery, page, pageSize int64) ([]*AlertView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	filter := q.AlertFilter
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	if err := boundTimeRange(&filter, uc.now()); err != nil {
		return nil, 0, err
	}
	filter.Groups = scope.Filter(q.GroupID)
	if q.Mine {
		// 我负责的：通知过我的，或由我认领的。以「我是接收方 / 认领人」为准，不要求对事件所在分组有范围
		// （同事件详情）。
		filter.RecipientID, filter.OrAckUser, filter.Groups = userID, true, scope.NotifiedFilter(q.GroupID)
	}
	if filter.Groups.Empty() {
		return []*AlertView{}, 0, nil
	}
	alerts, total, err := uc.alerts.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, scope, alerts)
	if err != nil {
		return nil, 0, err
	}
	if q.Mine {
		// 「通知过我的」可以含范围外的事件：这些只给收件人视图（与 Get 同一口径）。
		for _, v := range views {
			if !scope.CanRead(v.GroupID) {
				recipientView(v)
			}
		}
	}
	return views, total, uc.decorate(ctx, views)
}

// recipientView 把事件改成收件人视图：不带屏蔽规则、抑制规则（指向他未必有权查看的对象），不能做任何
// 操作。改的是副本，不动仓储返回的事件。
func recipientView(v *AlertView) {
	cp := *v.Alert
	cp.MuteID, cp.InhibitID = "", ""
	v.Alert, v.MuteName, v.InhibitName = &cp, "", ""
	v.Recipient, v.CanAck, v.CanManage = true, false, false
}

// views 批量补齐展示信息：分组名、规则是否仍存在、屏蔽规则名。
func (uc *AlertUsecase) views(ctx context.Context, scope Scope, alerts []*Alert) ([]*AlertView, error) {
	return alertViews(ctx, scope, uc.rules, uc.mutes, uc.now(), alerts)
}

// alertViews 供告警、屏蔽预览、订阅预览共用。
func alertViews(ctx context.Context, scope Scope, rules RuleLookup, mutes MuteReader, now time.Time, alerts []*Alert) ([]*AlertView, error) {
	var ruleIDs, muteIDs []string
	for _, a := range alerts {
		ruleIDs = append(ruleIDs, a.RuleID)
		muteIDs = append(muteIDs, a.MuteID)
	}
	ruleNames, muteNames := map[string]string{}, map[string]string{}
	var err error
	if ruleIDs = uniq(ruleIDs); len(ruleIDs) > 0 {
		if ruleNames, err = rules.Names(ctx, ruleIDs); err != nil {
			return nil, err
		}
	}
	if muteIDs = uniq(muteIDs); len(muteIDs) > 0 {
		if muteNames, err = mutes.Names(ctx, muteIDs); err != nil {
			return nil, err
		}
	}
	out := make([]*AlertView, 0, len(alerts))
	for _, a := range alerts {
		_, exists := ruleNames[a.RuleID]
		end := now
		if a.ResolveTime != nil {
			end = *a.ResolveTime
		}
		out = append(out, &AlertView{
			Alert: a, GroupName: scope.GroupName(a.GroupID), RuleExists: exists, MuteName: muteNames[a.MuteID],
			DurationS: max(int64(end.Sub(a.FirstFireTime).Seconds()), 0),
			CanAck:    scope.CanRead(a.GroupID), CanManage: scope.CanManage(a.GroupID),
		})
	}
	return out, nil
}

// Get 查询告警事件详情与通知时间线。
//
// 本接口免接口授权（通知里的链接要让收件人点得开，而收件人不一定有监控的任何权限），所以访问控制
// 全部在这里，两条路任一成立即可：
//   - 完整视图：持有「查询告警事件」接口权限，并且对事件所在分组有只读以上范围；
//   - 收件人视图：该用户是这条事件某次通知的接收方、且账号可用。只能看到这一条事件本身和发给自己的
//     那几次通知——不带其他接收方、不带屏蔽规则、不能跳转到规则。
//
// 都不成立时与事件不存在同样返回 ErrAlertNotFound（不泄露事件是否存在）。
func (uc *AlertUsecase) Get(ctx context.Context, userID, id string) (*AlertDetail, error) {
	_, d, err := uc.get(ctx, userID, id)
	return d, err
}

// GetDetail 同 Get，另带时间线、关联资源卡片与影响面、关联工单（按同一视图裁剪）。
func (uc *AlertUsecase) GetDetail(ctx context.Context, userID, id string) (*AlertDetail, *AlertExtras, error) {
	scope, d, err := uc.get(ctx, userID, id)
	if err != nil {
		return nil, nil, err
	}
	if uc.detail == nil {
		return d, &AlertExtras{}, nil
	}
	extras, err := uc.detail.extras(ctx, userID, scope, d)
	return d, extras, err
}

func (uc *AlertUsecase) get(ctx context.Context, userID, id string) (Scope, *AlertDetail, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	alert, err := uc.alerts.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	full := scope.CanRead(alert.GroupID)
	if full {
		if full, err = uc.perms.CanReadAlerts(ctx, userID); err != nil {
			return Scope{}, nil, err
		}
	}
	if !full {
		ok, err := uc.isActiveRecipient(ctx, id, userID)
		if err != nil {
			return Scope{}, nil, err
		}
		if !ok {
			return Scope{}, nil, ErrAlertNotFound
		}
	}
	views, err := uc.views(ctx, scope, []*Alert{alert})
	if err != nil {
		return Scope{}, nil, err
	}
	logs, err := uc.logs.ListByAlert(ctx, id, maxNotifyLogs)
	if err != nil {
		return Scope{}, nil, err
	}
	if !full {
		recipientView(views[0])
		logs = ownLogs(logs, userID)
	}
	if err := uc.fillRecipients(ctx, logs); err != nil {
		return Scope{}, nil, err
	}
	if err := uc.decorate(ctx, views); err != nil {
		return Scope{}, nil, err
	}
	return scope, &AlertDetail{Alert: views[0], Logs: logs, CanViewRule: full && views[0].RuleExists}, nil
}

// isActiveRecipient 用户是否这条事件的通知接收方且账号可用。
func (uc *AlertUsecase) isActiveRecipient(ctx context.Context, alertID, userID string) (bool, error) {
	notified, err := uc.logs.IsRecipient(ctx, alertID, userID)
	if err != nil || !notified {
		return false, err
	}
	active, err := uc.dir.ActiveUserIDs(ctx, []string{userID})
	return len(active) == 1, err
}

// ownLogs 收件人视图的通知时间线：只留发给该用户的记录，接收方只剩他自己；不带通知事件 ID 与订阅
// 规则 ID（指向他未必有权查看的其他对象），订阅名称保留——让他知道这条通知从何而来。
func ownLogs(logs []*NotifyLog, userID string) []*NotifyLog {
	out := make([]*NotifyLog, 0, len(logs))
	for _, l := range logs {
		if !slices.Contains(l.Recipients, userID) {
			continue
		}
		cp := *l
		cp.Recipients, cp.NotifyEventIDs, cp.SubscriptionID = []string{userID}, nil, ""
		out = append(out, &cp)
	}
	return out
}

// isGroupRecipient 接收方是否通知群标识（notify 的群收件人写法以 $ 开头）。
func isGroupRecipient(id string) bool { return strings.HasPrefix(id, "$") }

func (uc *AlertUsecase) fillRecipients(ctx context.Context, logs []*NotifyLog) error {
	var users []string
	for _, l := range logs {
		for _, r := range l.Recipients {
			if !isGroupRecipient(r) {
				users = append(users, r)
			}
		}
	}
	userNames := map[string]string{}
	if users = uniq(users); len(users) > 0 {
		var err error
		if userNames, err = uc.dir.UserNames(ctx, users); err != nil {
			return err
		}
	}
	for _, l := range logs {
		l.RecipientRefs = make([]RecipientRef, 0, len(l.Recipients))
		for _, r := range l.Recipients {
			if isGroupRecipient(r) {
				l.RecipientRefs = append(l.RecipientRefs, RecipientRef{Type: RecipientGroup, ID: r, Name: r})
				continue
			}
			l.RecipientRefs = append(l.RecipientRefs, RecipientRef{Type: RecipientUser, ID: r, Name: userNames[r]})
		}
	}
	return nil
}

// AggregateQuery 活跃告警聚合查询入参。By 取 rule / severity / group / label:<标签名>。
type AggregateQuery struct {
	By           string
	GroupID      string
	DatasourceID string
	Severities   []int32
	Muted        *bool
	Acked        *bool
	Inhibited    *bool
	// Labels 事件标签过滤（精确相等，各项之间是「并且」）。
	Labels map[string]string
}

// Aggregate 活跃告警的聚合视图（只统计数据范围内、告警中的事件）。
func (uc *AlertUsecase) Aggregate(ctx context.Context, userID string, q AggregateQuery) ([]*AlertAggregate, int64, error) {
	by := AggregateBy{Kind: q.By}
	if label, ok := strings.CutPrefix(q.By, AggregateByLabel+":"); ok {
		by = AggregateBy{Kind: AggregateByLabel, Label: label}
		if !mc.ValidLabelName(label) {
			return nil, 0, detail(ErrQueryInvalid, "invalid label name")
		}
	} else if !slices.Contains([]string{AggregateByRule, AggregateBySeverity, AggregateByGroup}, q.By) {
		return nil, 0, detail(ErrQueryInvalid, "by must be rule, severity, group or label:<name>")
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	filter := AlertFilter{
		Statuses: []int32{mc.AlertFiring}, Severities: q.Severities, Muted: q.Muted, Acked: q.Acked, Inhibited: q.Inhibited, Labels: q.Labels,
	}
	if q.DatasourceID != "" {
		filter.DatasourceIDs = []string{q.DatasourceID}
	}
	if filter.Groups = scope.Filter(q.GroupID); filter.Groups.Empty() {
		return []*AlertAggregate{}, 0, nil
	}
	items, total, err := uc.stats.Aggregate(ctx, by, filter, maxAggregates)
	if err != nil {
		return nil, 0, err
	}
	for _, it := range items {
		switch by.Kind {
		case AggregateByGroup:
			it.Name = scope.GroupName(it.Key)
		case AggregateBySeverity:
			if n, err := strconv.ParseInt(it.Key, 10, 32); err == nil {
				it.Name = SeverityLabel(int32(n))
			}
		case AggregateByLabel:
			it.Name = it.Key
		}
	}
	return items, total, nil
}

// isNotFound 报告错误是否「不存在」类（404）。
func isNotFound(err error) bool { return err != nil && kerrors.FromError(err).Code == 404 }
