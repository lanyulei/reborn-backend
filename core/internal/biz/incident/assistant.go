package biz

import (
	"context"
	"fmt"
	"strings"
)

// ContextType 助手页内上下文的对象类型（前端故障详情页随对话携带）。
const ContextType = "incident"

// contextModule 助手系统提示里标注的模块名。
const contextModule = "故障管理"

// DescribeContext 助手页内上下文提供者（bizassistant.ContextProvider 的故障实现）：只支持 incident 类型，
// 读权限与详情页同口径（无权 → 404），文本为摘要级（关联告警 / 影响面 / 变更 / 公告各限量），全部按数据对待。
func (uc *IncidentUsecase) DescribeContext(ctx context.Context, userID, typ, id string) (module, text string, err error) {
	if typ != ContextType {
		return "", "", ErrInvalid
	}
	c, err := uc.GetContext(ctx, userID, id)
	if err != nil {
		return "", "", err
	}
	return contextModule, uc.describe(c), nil
}

// describe 把上下文包渲染为助手提示文本（逐项截短、整体封顶 6000 字节）。
func (uc *IncidentUsecase) describe(c *Context) string {
	inc := c.View.Incident
	var b strings.Builder
	fmt.Fprintf(&b, "当前页面是故障 %s「%s」。等级 %s，状态 %s", inc.Number, promptClip(inc.Title, 200), SeverityLabel(inc.Severity), StatusLabel(inc.Status))
	if c.View.BusinessName != "" {
		b.WriteString("，业务 " + promptClip(c.View.BusinessName, 60))
	}
	if c.View.CommanderName != "" {
		b.WriteString("，指挥官 " + promptClip(c.View.CommanderName, 60))
	}
	fmt.Fprintf(&b, "，发生于 %s", inc.OccurTime.Format(payloadTimeLayout))
	if inc.IsDrill {
		b.WriteString("（演练）")
	}
	b.WriteString("。\n")
	if inc.Summary != "" {
		b.WriteString("摘要：" + promptClip(inc.Summary, 600) + "\n")
	}
	if inc.AISummary != "" {
		b.WriteString("AI 摘要：" + promptClip(inc.AISummary, 800) + "\n")
	}
	if len(c.Alerts) > 0 {
		fmt.Fprintf(&b, "关联告警 %d 条（活跃 %d）：\n", len(c.Alerts), c.View.ActiveAlerts)
		for _, a := range c.Alerts[:min(len(c.Alerts), 10)] {
			if a.Alert != nil {
				fmt.Fprintf(&b, "- [S%d] %s（状态 %d）\n", a.Alert.Severity, promptClip(a.Alert.RuleName, 120), a.Alert.Status)
			}
		}
	}
	if len(c.Impact) > 0 {
		b.WriteString("受影响资源：\n")
		for _, n := range c.Impact[:min(len(c.Impact), 10)] {
			fmt.Fprintf(&b, "- %s（%s，层级 %d）\n", promptClip(n.Name, 100), n.ModelName, n.Depth)
		}
	}
	if len(c.Changes) > 0 {
		b.WriteString("近期变更：\n")
		for _, ch := range c.Changes[:min(len(c.Changes), 10)] {
			fmt.Fprintf(&b, "- [%s/%s] %s %s %s\n", ch.Domain, ch.Kind, formatOptTime(ch.RefTime), promptClip(ch.Summary, 160), relationLabel(ch.Relation))
		}
	}
	if len(c.Updates) > 0 {
		b.WriteString("最近公告：\n")
		for _, u := range c.Updates[:min(len(c.Updates), 3)] {
			fmt.Fprintf(&b, "- %s %s\n", u.CreateTime.Format(payloadTimeLayout), promptClip(u.Content, 300))
		}
	}
	if c.Pending > 0 {
		fmt.Fprintf(&b, "待审批的处置建议：%d 条。\n", c.Pending)
	}
	if len(c.Similar) > 0 {
		b.WriteString("相似历史故障：\n")
		for _, s := range c.Similar[:min(len(c.Similar), 5)] {
			fmt.Fprintf(&b, "- %s %s\n", s.Number, promptClip(s.Title, 120))
		}
	}
	return truncate(b.String(), 6000)
}
