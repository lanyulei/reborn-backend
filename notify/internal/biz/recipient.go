package biz

import (
	"context"
	"regexp"
	"strings"
)

// Target 一个发送目标（渠道实例 × 收件标识）。
type Target struct {
	ChannelID string
	Recipient string
}

// SubscriberSource 按 (biz, event_type) 实时解析订阅者（订阅不进 10s 快照，
// 设计 §4.4）；实现为 notify_subscription ⋈ "system_user" 的 SQL 匹配。
type SubscriberSource interface {
	ListSubscribers(ctx context.Context, bizID, eventType string) ([]Subscriber, error)
}

// robotTarget 群机器人渠道的隐含收件标识（目标群由实例 secret 决定）。
const robotTarget = "$bot"

// uuidPattern 用户 ID 形态（显式收件里的用户 ID 经平台用户表解析为邮箱）。
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ExplicitRecipients 事件显式收件（notify_event.recipients）按渠道类别分拣后的结果
// （DDL 约定取值：邮箱 / 群标识 / 用户 ID）：
//   - 邮箱（含 @）→ Emails；
//   - 用户 ID（uuid）→ 经平台用户表解析为邮箱进 Emails（用户不存在/停用/无邮箱则
//     进 Unresolved）；
//   - 以 $ 开头的群标识（如 $group:ops / $bot）→ Groups（群机器人渠道）；
//   - 其余形态 → Unresolved（调用方记日志，不中断）。
type ExplicitRecipients struct {
	Emails     []string
	Groups     []string
	Unresolved []string
}

// UserIDs 提取显式收件里的用户 ID（供批内一次性查询）。
func UserIDs(recipients []string) []string {
	var ids []string
	for _, r := range recipients {
		if uuidPattern.MatchString(strings.TrimSpace(r)) {
			ids = append(ids, strings.ToLower(strings.TrimSpace(r)))
		}
	}
	return ids
}

// ClassifyRecipients 分拣显式收件；users 为 用户ID → 邮箱（已查得的平台用户）。
func ClassifyRecipients(recipients []string, users map[string]string) *ExplicitRecipients {
	if len(recipients) == 0 {
		return nil
	}
	out := &ExplicitRecipients{}
	for _, raw := range recipients {
		r := strings.TrimSpace(raw)
		switch {
		case r == "":
		case strings.HasPrefix(r, "$"):
			out.Groups = append(out.Groups, r)
		case strings.Contains(r, "@"):
			out.Emails = append(out.Emails, r)
		case uuidPattern.MatchString(r):
			if email := users[strings.ToLower(r)]; email != "" {
				out.Emails = append(out.Emails, email)
			} else {
				out.Unresolved = append(out.Unresolved, r)
			}
		default:
			out.Unresolved = append(out.Unresolved, r)
		}
	}
	return out
}

// ResolveTargets 按路由收件策略解析目标集合（设计 §4.4，去重 (channel, recipient)）：
//   - 渠道缺失（已删除）/ 停用 → 跳过（停用实例被路由跳过，§5.1.1）；
//   - email：显式邮箱非空时以其替代策略；否则 fixed + 订阅者邮箱；
//   - webhook：收件 = 实例 url（显式/fixed/订阅均不适用）；
//   - 群机器人：显式群标识非空时以其替代策略；否则 fixed → 订阅者该类型目标群
//     → 实例隐含 "$bot"。
//
// explicit 为 nil 表示事件无显式收件；subscribers 仅在策略 subscription=true 时需要。
func ResolveTargets(snap *Snapshot, route *Route, explicit *ExplicitRecipients, subscribers []Subscriber) []Target {
	seen := make(map[Target]struct{}, 8)
	out := make([]Target, 0, 8)
	add := func(channelID, recipient string) {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			return
		}
		t := Target{ChannelID: channelID, Recipient: recipient}
		if _, dup := seen[t]; dup {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for _, id := range route.ChannelIDs {
		ch := snap.Channel(id)
		if ch == nil || !ch.Enabled {
			continue
		}
		switch ch.Type {
		case ChannelTypeEmail:
			if explicit != nil && len(explicit.Emails) > 0 {
				for _, r := range explicit.Emails {
					add(id, r)
				}
				continue
			}
			for _, r := range route.Recipient.Fixed {
				add(id, r)
			}
			if route.Recipient.Subscription {
				for _, s := range subscribers {
					if v, ok := s.Prefs[ChannelTypeEmail].(bool); ok && v && s.Email != "" {
						add(id, s.Email)
					}
				}
			}
		case ChannelTypeWebhook:
			if ch.Webhook != nil {
				add(id, ch.Webhook.URL)
			}
		default: // 群机器人
			if explicit != nil && len(explicit.Groups) > 0 {
				for _, r := range explicit.Groups {
					add(id, r)
				}
				continue
			}
			if len(route.Recipient.Fixed) > 0 {
				for _, r := range route.Recipient.Fixed {
					add(id, r)
				}
				continue
			}
			added := false
			if route.Recipient.Subscription {
				for _, s := range subscribers {
					if g, ok := s.Prefs[ch.Type].(string); ok && g != "" {
						add(id, g)
						added = true
					} else if v, ok := s.Prefs[ch.Type].(bool); ok && v {
						add(id, robotTarget)
						added = true
					}
				}
			}
			if !added {
				add(id, robotTarget)
			}
		}
	}
	return out
}
