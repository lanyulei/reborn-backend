package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// 通知事件：biz_id=incident，event_type = incident.<s1|s2|s3|s4>.<动作>（等级编入第二段，订阅 / 路由可按等级匹配）。
const (
	notifyBizID = "incident"
	// maxEventRecipients 单个通知事件的显式收件上限（notify 入参约束），超出拆成多个事件。
	maxEventRecipients = 20
	// maxTitleBytes 通知标题字节上限（notify varchar(255)）。
	maxTitleBytes = 255
	// payloadBudget 内容体字节预算（notify 上限 32 KiB）。
	payloadBudget = 30 * 1024
	// payloadTimeLayout 正文时间格式。
	payloadTimeLayout = "2006-01-02 15:04:05 -07:00"
)

// 动作段。
const (
	NotifyOpened          = "opened"
	NotifyEscalated       = "escalated"
	NotifyStale           = "stale"
	NotifySeverityChanged = "severity_changed"
	NotifyStatusChanged   = "status_changed"
	NotifyUpdate          = "update"
	NotifyAssigned        = "assigned"
	NotifyProposal        = "proposal"
	NotifyReviewDue       = "review_due"
	NotifyMerged          = "merged"
	NotifyStorm           = "storm"
	NotifyEngine          = "engine"
	// NotifyAgent 运维 Agent 写入了发现（每次运行合并为一条）。
	NotifyAgent = "agent"
)

// EventType 通知事件类型。
func EventType(severity int32, action string) string {
	return notifyBizID + "." + SeverityCode(severity) + "." + action
}

// notice 一条待写出的通知。
type notice struct {
	action string
	// dedup 去重键后缀（同一故障同一动作的不同次数靠它区分）。
	dedup string
	title string
	// extra 并入内容体的附加字段。
	extra map[string]any
	// users 个人收件人（用户 ID）；groups 为 nil 时取故障的通知群。
	users  []string
	groups []string
	// noGroups 不发给群（纯个人提醒）。
	noGroups bool
}

// basePayload 故障通知内容体的公共字段。
func (uc *IncidentUsecase) basePayload(inc *Incident, names map[string]string, businessName string, alertCount int) map[string]any {
	cis := make([]string, 0, len(inc.AffectedCIIDs))
	for _, id := range inc.AffectedCIIDs[:min(len(inc.AffectedCIIDs), 10)] {
		if n := names["ci:"+id]; n != "" {
			cis = append(cis, n)
		}
	}
	affected := strings.Join(cis, "、")
	if len(inc.AffectedCIIDs) > len(cis) {
		affected += fmt.Sprintf("（共 %d 个）", len(inc.AffectedCIIDs))
	}
	if affected == "" {
		affected = "—"
	}
	commander := names["user:"+inc.CommanderID]
	if commander == "" {
		commander = "未指定"
	}
	if businessName == "" {
		businessName = "—"
	}
	return map[string]any{
		"incident_id": inc.ID, "number": inc.Number, "title": inc.Title, "severity": inc.Severity, "severity_label": SeverityLabel(inc.Severity),
		"status": inc.Status, "status_label": StatusLabel(inc.Status), "business_name": businessName, "commander_name": commander,
		"occur_time": inc.OccurTime.Format(payloadTimeLayout), "affected_cis": affected, "alert_count": alertCount,
		"url": uc.d.Site.IncidentURL(inc.ID), "drill": inc.IsDrill, "review_due": formatOptTime(inc.ReviewDue),
	}
}

func formatOptTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(payloadTimeLayout)
}

// notifyTitle 通知标题（字节上限内）。
func notifyTitle(inc *Incident, prefix string) string {
	title := prefix + inc.Number + " " + inc.Title
	if inc.IsDrill {
		title = "[演练] " + title
	}
	return truncateBytes(title, maxTitleBytes)
}

// encodePayload 编码内容体并保证不超过预算：超出时截短 text。
func encodePayload(p map[string]any) ([]byte, error) {
	raw, err := json.Marshal(p)
	if err != nil || len(raw) <= payloadBudget {
		return raw, err
	}
	if text, ok := p["text"].(string); ok {
		over := len(raw) - payloadBudget
		p["text"] = truncateBytes(text, max(len(text)-over-16, 0))
	}
	return json.Marshal(p)
}

// emitTx 在事务内写通知事件。每 maxEventRecipients 个收件人一条；没有任何收件人时不带收件人写一条
// （交由通知路由的固定收件人与订阅者）。写入失败即返回（事务整体回滚：通知与业务不脱节）。
func (uc *IncidentUsecase) emitTx(ctx context.Context, tx Tx, inc *Incident, n notice, payload map[string]any) error {
	payload["action"] = n.action
	if n.extra != nil {
		for k, v := range n.extra {
			payload[k] = v
		}
	}
	raw, err := encodePayload(payload)
	if err != nil {
		return fmt.Errorf("encode incident notify payload: %w", err)
	}
	groups := n.groups
	if groups == nil && !n.noGroups {
		groups = inc.Groups
	}
	recipients := uniq(slices.Concat(groups, n.users))
	chunks := [][]string{nil}
	if len(recipients) > 0 {
		chunks = slices.Collect(slices.Chunk(recipients, maxEventRecipients))
	}
	title := n.title
	if title == "" {
		title = notifyTitle(inc, "【"+SeverityLabel(inc.Severity)+" 故障】")
	}
	for i, chunk := range chunks {
		ev := NotifyEvent{
			BizID: notifyBizID, EventType: EventType(inc.Severity, n.action), Severity: NotifySeverity(inc.Severity), Title: title,
			Payload: raw, GroupKey: "incident:" + inc.ID, Recipients: chunk,
			DedupKey: truncateBytes(fmt.Sprintf("incident:%s:%s:%s:%d", inc.ID, n.action, n.dedup, i), 128),
		}
		if _, err := uc.d.Notify.NotifyEvent(ctx, tx, ev); err != nil {
			return fmt.Errorf("write incident notify event: %w", err)
		}
	}
	return nil
}
