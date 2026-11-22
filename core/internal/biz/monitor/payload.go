package biz

import (
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 通知内容体与标题的构造：通知派发（规则通知、订阅通知、升级通知）与模拟触发的测试通知共用。

const (
	// maxTitleRunes / maxTitleBytes 通知标题的长度：先按字符截到便于阅读的长度，再保证不超过 notify
	// 对标题的字节上限（varchar(255) 按字节校验；超限的事件写不进去，这一行会重试到被丢弃）。
	maxTitleRunes = 80
	maxTitleBytes = 255
	// alertPathPrefix 通知里事件详情链接的路径（前端路由）。配置了 monitor.site_url 时前面拼上
	// 站点地址，否则是站内相对路径。
	alertPathPrefix = "/monitor/alert/"
	// payloadTimeLayout 通知正文里的时间格式（带时区偏移，不依赖部署机器的本地时区）。
	payloadTimeLayout = "2006-01-02 15:04:05 -07:00"
	// fallbackTitle 规则名意外为空时的通知标题（通知事件的标题不能为空）。
	fallbackTitle = "监控告警"
)

// payloadBudget 通知内容体的字节预算，须不超过 notify 对事件载荷的上限（32KiB）。渲染后的附加信息
// 单项可达 4KiB、至多 10 项，原样带上会超限——超限的事件永远写不进去，这一行就会重试到被丢弃。
const payloadBudget = 30 * 1024

// alertTitle 通知标题 = 规则名 + 关键标签：先按字符截到 maxTitleRunes，再按字节截到 maxTitleBytes
// （不切开字符）。只按字符截不够——80 个 4 字节字符（表情、生僻字）是 320 字节，超过 notify 的上限。
func alertTitle(ruleName string, labels map[string]string) string {
	title := ruleName
	names := slices.Sorted(maps.Keys(labels))
	parts := make([]string, 0, 3)
	for _, n := range names[:min(len(names), 3)] {
		parts = append(parts, n+"="+labels[n])
	}
	if len(parts) > 0 {
		title += " [" + strings.Join(parts, ", ") + "]"
	}
	if title = strings.TrimSpace(truncateBytes(truncate(title, maxTitleRunes), maxTitleBytes)); title == "" {
		return fallbackTitle
	}
	return title
}

// encodePayload 把内容体编码为 JSON 并保证不超过 payloadBudget：超出时先逐级截短附加信息、再截短
// 标签，最后才整体舍弃——通知发得出去比内容完整重要。
func encodePayload(p mc.NotifyPayload) ([]byte, error) {
	raw, err := json.Marshal(p)
	if err != nil || len(raw) <= payloadBudget {
		return raw, err
	}
	p.Labels, p.Annotations = maps.Clone(p.Labels), maps.Clone(p.Annotations)
	for _, target := range []map[string]string{p.Annotations, p.Labels} {
		for _, limit := range []int{1024, 256, 64, 0} {
			for k, v := range target {
				if limit == 0 {
					delete(target, k)
				} else if r := []rune(v); len(r) > limit {
					target[k] = string(r[:limit]) + "…"
				}
			}
			if raw, err = json.Marshal(p); err != nil || len(raw) <= payloadBudget {
				return raw, err
			}
		}
	}
	p.Expr = truncate(p.Expr, 1024)
	return json.Marshal(p)
}

// payloadInput 构造通知内容体所需的输入。
type payloadInput struct {
	Alert *Alert
	// Timezone 规则生效时间配置的时区（通知里的时间按它显示；空或加载失败用 UTC）。
	Timezone  string
	GroupName string
	// SiteURL 前端站点地址（无结尾斜杠）；空 = 相对路径。
	SiteURL string
	// Resolved 恢复通知；Seq 第几次通知（升级通知传级内序号）。
	Resolved         bool
	Seq              int32
	SubscriptionName string
	Now              time.Time
	// CI 事件关联的资源（可为 nil）。
	CI *CIEntry
}

// buildPayload 通知内容体：迁移内置的模板按这些键取值；数值与时间预先格式化为文本。通知派发与模拟
// 触发的测试通知共用。升级通知的 escalation_* 与测试通知的 test 由调用方在返回值上补填。
func buildPayload(in payloadInput) mc.NotifyPayload {
	a := in.Alert
	loc := time.UTC
	if in.Timezone != "" {
		if l, err := mc.LoadLocation(in.Timezone); err == nil {
			loc = l
		}
	}
	p := mc.NotifyPayload{
		AlertID: a.ID, AlertURL: in.SiteURL + alertPathPrefix + a.ID, RuleID: a.RuleID, RuleName: a.RuleName,
		GroupName: in.GroupName, DatasourceName: a.DatasourceName, Severity: a.Severity,
		Status: mc.PayloadStatusFiring, Labels: a.Labels, Annotations: a.Annotations, Expr: a.Expr,
		FirstFireTime: a.FirstFireTime.In(loc).Format(payloadTimeLayout), NotifySeq: in.Seq, SubscriptionName: in.SubscriptionName,
	}
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	if p.Annotations == nil {
		p.Annotations = map[string]string{}
	}
	if a.Value != nil {
		p.Value = mc.FormatValue(*a.Value)
	}
	if in.CI != nil {
		p.CIID, p.CIName = in.CI.ID, in.CI.Name
	}
	end := in.Now
	if in.Resolved {
		p.Status = mc.PayloadStatusResolved
		if a.RecoveryValue != nil {
			p.RecoveryValue = mc.FormatValue(*a.RecoveryValue)
		}
		if a.ResolveTime != nil {
			end = *a.ResolveTime
		}
	}
	p.Duration = humanDuration(end.Sub(a.FirstFireTime))
	return p
}

// humanDuration 把时长写成中文：取最大的两个单位（如「2 天 3 小时」「5 分钟」「42 秒」）。
func humanDuration(d time.Duration) string {
	secs := max(int64(d.Seconds()), 0)
	units := [...]struct {
		size int64
		name string
	}{{86400, "天"}, {3600, "小时"}, {60, "分钟"}, {1, "秒"}}
	parts := make([]string, 0, 2)
	for _, u := range units {
		if n := secs / u.size; n > 0 && len(parts) < 2 {
			parts = append(parts, strconv.FormatInt(n, 10)+" "+u.name)
			secs -= n * u.size
		} else if len(parts) > 0 {
			break // 只取相邻的两个单位
		}
	}
	if len(parts) == 0 {
		return "0 秒"
	}
	return strings.Join(parts, " ")
}
