package biz

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 事件详情里第二、三期的部分：时间线、关联资源卡片与影响面、关联工单。
const (
	// maxTimeline 事件详情返回的时间线条数上限。
	maxTimeline = 500
	// TimelineNotification 通知记录在时间线里的类型（只存在于接口，不落库）。
	TimelineNotification = "notification"
	// impactDepth / impactLimit 影响面向下的层数与节点数上限。
	impactDepth = 3
	impactLimit = 50
)

// TimelineItem 事件时间线的一条。Log 仅 Type = TimelineNotification 时有值；Operator 为 nil = 系统产生。
type TimelineItem struct {
	ID       string
	Type     string
	Time     time.Time
	Operator *NamedRef
	Detail   mc.AlertEventDetail
	// FromUser / ToUser 转交前后的认领人（transfer）；MuteName / InhibitName 屏蔽 / 抑制规则名称；
	// Maintenance 因关联资源处于维护态而屏蔽。
	FromUser    *NamedRef
	ToUser      *NamedRef
	MuteName    string
	InhibitName string
	Maintenance bool
	Log         *NotifyLog
}

// AlertCIView 事件关联资源的卡片。CanView 为 false 时只有 ID、Name、Exists 有值。
type AlertCIView struct {
	CICard
	// OwnerID 负责人是系统用户时的用户 ID（此时 Owner 为其显示名）。
	OwnerID     string
	Exists      bool
	CanView     bool
	FiringCount int32
}

// AlertImpactView 影响面里的一个下游资源及其活跃告警数。
type AlertImpactView struct {
	CIImpact
	FiringCount int32
}

// AlertTicketView 事件关联的工单。CanView 为 false 时 Title 为空、Status 为 0。
type AlertTicketView struct {
	ID      string
	Number  string
	Title   string
	Status  int32
	Auto    bool
	CanView bool
}

// AlertExtras 事件详情里第二、三期的内容。
type AlertExtras struct {
	Timeline        []*TimelineItem
	CI              *AlertCIView
	Impact          []*AlertImpactView
	ImpactTruncated bool
	Ticket          *AlertTicketView
}

// AlertDetailUsecase 组装事件详情里的时间线、资源卡片、影响面与工单卡片。访问控制已由 AlertUsecase.Get
// 完成：这里只按「完整视图 / 收件人视图」决定给多少，CMDB 与工单的内容一律按调用方自己的权限读取。
type AlertDetailUsecase struct {
	events   AlertEventRepo
	links    AlertTicketLinks
	stats    AlertCIStats
	cis      CIIndexReader
	cmdb     CMDBViewer
	tickets  TicketDesk
	dir      Directory
	mutes    MuteReader
	inhibits InhibitNames
}

// NewAlertDetailUsecase new an AlertDetailUsecase.
func NewAlertDetailUsecase(events AlertEventRepo, links AlertTicketLinks, stats AlertCIStats, cis CIIndexReader, cmdb CMDBViewer,
	tickets TicketDesk, dir Directory, mutes MuteReader, inhibits InhibitNames,
) *AlertDetailUsecase {
	return &AlertDetailUsecase{
		events: events, links: links, stats: stats, cis: cis, cmdb: cmdb, tickets: tickets, dir: dir, mutes: mutes, inhibits: inhibits,
	}
}

// recipientTimelineTypes 收件人视图能看到的时间线类型（通知记录另算）。
var recipientTimelineTypes = []string{mc.AlertEventFire, mc.AlertEventResolve, mc.AlertEventClose}

// extras 组装详情的附加内容。d 已按视图裁剪（收件人视图的 Logs 只含发给本人的）。
func (uc *AlertDetailUsecase) extras(ctx context.Context, userID string, scope Scope, d *AlertDetail) (*AlertExtras, error) {
	a, recipient := d.Alert, d.Alert.Recipient
	events, err := uc.events.ListByAlert(ctx, a.ID, maxTimeline)
	if err != nil {
		return nil, err
	}
	if recipient {
		events = slices.DeleteFunc(events, func(ev *AlertEvent) bool { return !slices.Contains(recipientTimelineTypes, ev.Type) })
	}
	out := &AlertExtras{Impact: []*AlertImpactView{}}
	if out.Timeline, err = uc.timeline(ctx, events, d.Logs, recipient); err != nil {
		return nil, err
	}
	if a.CIID != "" {
		if err := uc.fillCI(ctx, userID, scope, a, recipient, out); err != nil {
			return nil, err
		}
	}
	if a.TicketID != "" {
		if out.Ticket, err = uc.ticket(ctx, userID, a.Alert); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// timeline 把时间线条目与通知记录按时间合并（升序，至多 maxTimeline 条），名称各查一次。合并后超过上限时
// 保留的是最新的一段：详情页要看的恰恰是最近的几次通知与操作。
func (uc *AlertDetailUsecase) timeline(ctx context.Context, events []*AlertEvent, logs []*NotifyLog, recipient bool) ([]*TimelineItem, error) {
	items, err := uc.eventItems(ctx, events, recipient)
	if err != nil {
		return nil, err
	}
	for _, l := range logs {
		items = append(items, &TimelineItem{ID: l.ID, Type: TimelineNotification, Time: l.CreateTime, Log: l})
	}
	slices.SortStableFunc(items, func(x, y *TimelineItem) int { return cmp.Or(x.Time.Compare(y.Time), cmp.Compare(x.ID, y.ID)) })
	if len(items) > maxTimeline {
		items = items[len(items)-maxTimeline:]
	}
	return items, nil
}

// eventItems 时间线条目 → 展示项。收件人视图不带操作人与文字内容（备注、原因）。
func (uc *AlertDetailUsecase) eventItems(ctx context.Context, events []*AlertEvent, recipient bool) ([]*TimelineItem, error) {
	var users, muteIDs, inhibitIDs []string
	for _, ev := range events {
		users = append(users, ev.Operator, ev.Detail.FromUser, ev.Detail.ToUser)
		muteIDs = append(muteIDs, ev.Detail.MuteID)
		inhibitIDs = append(inhibitIDs, ev.Detail.InhibitID)
	}
	userNames, muteNames, inhibitNames := map[string]string{}, map[string]string{}, map[string]string{}
	var err error
	if users = uniq(users); len(users) > 0 && !recipient {
		if userNames, err = uc.dir.UserNames(ctx, users); err != nil {
			return nil, err
		}
	}
	if muteIDs = uniq(muteIDs); len(muteIDs) > 0 {
		if muteNames, err = uc.mutes.Names(ctx, muteIDs); err != nil {
			return nil, err
		}
	}
	if inhibitIDs = uniq(inhibitIDs); len(inhibitIDs) > 0 {
		if inhibitNames, err = uc.inhibits.InhibitNames(ctx, inhibitIDs); err != nil {
			return nil, err
		}
	}
	ref := func(id string) *NamedRef {
		if id == "" {
			return nil
		}
		name, ok := userNames[id]
		return &NamedRef{ID: id, Name: name, Missing: !ok}
	}
	items := make([]*TimelineItem, 0, len(events)+8)
	for _, ev := range events {
		it := &TimelineItem{ID: ev.ID, Type: ev.Type, Time: ev.CreateTime, Detail: ev.Detail}
		if recipient {
			it.Detail = mc.AlertEventDetail{Cause: ev.Detail.Cause, Severity: ev.Detail.Severity}
			items = append(items, it)
			continue
		}
		it.Operator, it.FromUser, it.ToUser = ref(ev.Operator), ref(ev.Detail.FromUser), ref(ev.Detail.ToUser)
		it.MuteName, it.InhibitName = muteNames[ev.Detail.MuteID], inhibitNames[ev.Detail.InhibitID]
		it.Maintenance = (ev.Type == mc.AlertEventMute || ev.Type == mc.AlertEventUnmute) && ev.Detail.MuteID == ""
		items = append(items, it)
	}
	return items, nil
}

// eventItem 单条时间线条目的展示项（备注接口的返回值）。
func (uc *AlertDetailUsecase) eventItem(ctx context.Context, ev *AlertEvent) (*TimelineItem, error) {
	items, err := uc.eventItems(ctx, []*AlertEvent{ev}, false)
	if err != nil {
		return nil, err
	}
	return items[0], nil
}

// fillCI 资源卡片与影响面。调用方无权在资源管理里查看该资源（或资源已不存在）时只给 ID 与名称
// （名称取自监控侧的资源索引）；收件人视图同样只给 ID 与名称，且没有影响面。
func (uc *AlertDetailUsecase) fillCI(ctx context.Context, userID string, scope Scope, a *AlertView, recipient bool, out *AlertExtras) error {
	briefs, err := uc.cis.Briefs(ctx, []string{a.CIID})
	if err != nil {
		return err
	}
	view := &AlertCIView{CICard: CICard{ID: a.CIID}}
	if b := briefs[a.CIID]; b != nil {
		view.Name, view.Exists = b.Name, b.Valid
	}
	out.CI = view
	if recipient {
		return nil
	}
	card, exists, err := uc.cmdb.Card(ctx, userID, a.CIID)
	if err != nil {
		return err
	}
	groups := scope.Filter("")
	if card != nil {
		view.CICard, view.Exists, view.CanView = *card, exists, true
		if idgen.IsUUIDFold(card.Owner) && card.OwnerName != "" {
			view.Owner, view.OwnerID = card.OwnerName, card.Owner
		}
	}
	nodes, truncated, err := uc.cmdb.Impact(ctx, userID, a.CIID, impactDepth, impactLimit)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(nodes)+1)
	ids = append(ids, a.CIID)
	for i := range nodes {
		ids = append(ids, nodes[i].ID)
	}
	counts := map[string]int32{}
	if !groups.Empty() {
		// 活跃告警数只统计调用方监控数据范围内的事件。
		if counts, err = uc.stats.FiringByCI(ctx, ids, groups); err != nil {
			return err
		}
	}
	view.FiringCount = counts[a.CIID]
	out.ImpactTruncated = truncated
	for _, n := range nodes[:min(len(nodes), impactLimit)] {
		if !n.CanView {
			n = CIImpact{ID: n.ID, Name: n.Name, Depth: n.Depth}
		}
		out.Impact = append(out.Impact, &AlertImpactView{CIImpact: n, FiringCount: counts[n.ID]})
	}
	return nil
}

// ticket 工单卡片：编号与是否自动建单取自事件时间线；标题与状态只有调用方能查看该工单时才给。
func (uc *AlertDetailUsecase) ticket(ctx context.Context, userID string, a *Alert) (*AlertTicketView, error) {
	links, err := uc.links.TicketLinks(ctx, []string{a.ID})
	if err != nil {
		return nil, err
	}
	view := &AlertTicketView{ID: a.TicketID, Number: links[a.ID].Number, Auto: links[a.ID].Auto}
	info, err := uc.tickets.View(ctx, a.TicketID, userID)
	if err != nil {
		return nil, err
	}
	if info != nil {
		view.Number, view.Title, view.Status, view.CanView = info.Number, info.Title, info.Status, true
	}
	return view, nil
}
