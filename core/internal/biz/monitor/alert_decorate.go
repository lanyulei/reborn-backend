package biz

import (
	"context"

	"github.com/google/wire"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// AlertP2ProviderSet 告警事件第二、三期（事件操作、时间线、展示名）的 biz providers。
var AlertP2ProviderSet = wire.NewSet(
	NewAlertDecorator, wire.Bind(new(AlertViewDecorator), new(*AlertDecorator)),
	NewAlertDetailUsecase, NewAlertActionUsecase, ProvideAlertUsecase,
)

// AlertRefReader 事件操作与展示用到的批量只读查询（运行表）。
type AlertRefReader interface {
	// ListByIDs 批量取事件；不存在的不在结果中。
	ListByIDs(ctx context.Context, ids []string) ([]*Alert, error)
	// RuleNames 这些事件各自的规则名称（事件 ID → 触发时的规则名）；不存在的不在结果中。
	RuleNames(ctx context.Context, alertIDs []string) (map[string]string, error)
	// PendingClose 这些事件里已下达手动关闭、引擎尚未执行的。
	PendingClose(ctx context.Context, alertIDs []string) (map[string]bool, error)
}

// AlertDecorator 批量补齐事件视图里第二、三期的展示名（implements AlertViewDecorator）：每类名称
// 一次查询，不逐行查。
type AlertDecorator struct {
	dir      Directory
	inhibits InhibitNames
	cis      CIIndexReader
	links    AlertTicketLinks
	refs     AlertRefReader
}

// NewAlertDecorator new an AlertDecorator.
func NewAlertDecorator(dir Directory, inhibits InhibitNames, cis CIIndexReader, links AlertTicketLinks, refs AlertRefReader) *AlertDecorator {
	return &AlertDecorator{dir: dir, inhibits: inhibits, cis: cis, links: links, refs: refs}
}

// Decorate implements AlertViewDecorator.
func (d *AlertDecorator) Decorate(ctx context.Context, views []*AlertView) error {
	var users, inhibitIDs, sources, ciIDs, ticketed, firing []string
	for _, v := range views {
		users = append(users, v.AckUser, v.ClosedBy)
		// 收件人视图不带抑制规则（recipientView 已清掉 ID）；源告警的规则名与资源名保留。
		inhibitIDs = append(inhibitIDs, v.InhibitID)
		sources = append(sources, v.InhibitAlertID)
		ciIDs = append(ciIDs, v.CIID)
		if v.TicketID != "" {
			ticketed = append(ticketed, v.ID)
		}
		if v.Status == mc.AlertFiring {
			firing = append(firing, v.ID)
		}
		if v.AckTime != nil {
			v.AckDurationS = max(int64(v.AckTime.Sub(v.FirstFireTime).Seconds()), 0)
		}
	}
	var (
		userNames, inhibitNames, sourceNames = map[string]string{}, map[string]string{}, map[string]string{}
		cis                                  = map[string]*CIEntry{}
		links                                = map[string]TicketLink{}
		closing                              = map[string]bool{}
		err                                  error
	)
	if users = uniq(users); len(users) > 0 {
		if userNames, err = d.dir.UserNames(ctx, users); err != nil {
			return err
		}
	}
	if inhibitIDs = uniq(inhibitIDs); len(inhibitIDs) > 0 {
		if inhibitNames, err = d.inhibits.InhibitNames(ctx, inhibitIDs); err != nil {
			return err
		}
	}
	if sources = uniq(sources); len(sources) > 0 {
		if sourceNames, err = d.refs.RuleNames(ctx, sources); err != nil {
			return err
		}
	}
	if ciIDs = uniq(ciIDs); len(ciIDs) > 0 {
		if cis, err = d.cis.Briefs(ctx, ciIDs); err != nil {
			return err
		}
	}
	if len(ticketed) > 0 {
		if links, err = d.links.TicketLinks(ctx, ticketed); err != nil {
			return err
		}
	}
	if len(firing) > 0 {
		if closing, err = d.refs.PendingClose(ctx, firing); err != nil {
			return err
		}
	}
	for _, v := range views {
		v.AckUserName, v.ClosedByName = userNames[v.AckUser], userNames[v.ClosedBy]
		v.InhibitName, v.InhibitAlertName = inhibitNames[v.InhibitID], sourceNames[v.InhibitAlertID]
		if ci := cis[v.CIID]; ci != nil {
			v.CIName = ci.Name
		}
		v.TicketNumber, v.Closing = links[v.ID].Number, closing[v.ID]
	}
	return nil
}
