// Package biz 故障管理域（incident）的业务核心：故障生命周期、角色与时间线、告警归并（触发规则 / 候选故障 /
// 关联器）、影响面与变更关联、动作建议（提出 → 审批 → 执行 → 验证 → 熔断）、公告与分享链接、复盘与改进项、
// 开放接口与接入源、Webhook 订阅与出站箱、AI 摘要 / 草稿、Agent 租约与发现、统计。定义仓储接口与跨域窄接口
// （依赖倒置），零驱动、零传输层 import；跨域能力一律由组合根（internal/biz/incident_adapters.go）用其他域
// 已有的公开用例适配。
package biz

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/incident/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrForbidden         = errors.Forbidden(v1.ErrorReason_INCIDENT_FORBIDDEN.String(), "commander, responder or admin required")
	ErrVersionConflict   = errors.Conflict(v1.ErrorReason_INCIDENT_VERSION_CONFLICT.String(), "modified by someone else, please refresh and retry")
	ErrTimeRangeInvalid  = errors.BadRequest(v1.ErrorReason_INCIDENT_TIME_RANGE_INVALID.String(), "invalid time range")
	ErrSecretUnavailable = errors.ServiceUnavailable(v1.ErrorReason_INCIDENT_SECRET_UNAVAILABLE.String(), "credential master key is not configured")
	ErrRateLimited       = errors.New(429, v1.ErrorReason_INCIDENT_RATE_LIMITED.String(), "too many requests, please retry later")

	ErrNotFound         = errors.NotFound(v1.ErrorReason_INCIDENT_NOT_FOUND.String(), "incident not found")
	ErrInvalid          = errors.BadRequest(v1.ErrorReason_INCIDENT_INVALID.String(), "invalid incident")
	ErrStateInvalid     = errors.Conflict(v1.ErrorReason_INCIDENT_STATE_INVALID.String(), "operation not allowed in the current state")
	ErrAlertLinked      = errors.Conflict(v1.ErrorReason_INCIDENT_ALERT_LINKED.String(), "alert is already linked to another active incident")
	ErrAlertNotFound    = errors.NotFound(v1.ErrorReason_INCIDENT_ALERT_NOT_FOUND.String(), "alert not found or not visible")
	ErrReviewRequired   = errors.Conflict(v1.ErrorReason_INCIDENT_REVIEW_REQUIRED.String(), "postmortem must be completed before closing")
	ErrAgentBusy        = errors.Conflict(v1.ErrorReason_INCIDENT_AGENT_BUSY.String(), "another agent task is running on this incident")
	ErrCandidateMissing = errors.NotFound(v1.ErrorReason_INCIDENT_CANDIDATE_NOT_FOUND.String(), "candidate not found or already handled")
	ErrAIUnavailable    = errors.ServiceUnavailable(v1.ErrorReason_INCIDENT_AI_UNAVAILABLE.String(), "assistant model is not configured")

	ErrBusinessNotFound   = errors.NotFound(v1.ErrorReason_INCIDENT_BUSINESS_NOT_FOUND.String(), "business not found")
	ErrBusinessCodeExists = errors.Conflict(v1.ErrorReason_INCIDENT_BUSINESS_CODE_EXISTS.String(), "business code already exists")
	ErrBusinessInvalid    = errors.BadRequest(v1.ErrorReason_INCIDENT_BUSINESS_INVALID.String(), "invalid business")
	ErrBusinessInUse      = errors.Conflict(v1.ErrorReason_INCIDENT_BUSINESS_IN_USE.String(), "business is still referenced")

	ErrRuleNotFound   = errors.NotFound(v1.ErrorReason_INCIDENT_RULE_NOT_FOUND.String(), "trigger rule not found")
	ErrRuleNameExists = errors.Conflict(v1.ErrorReason_INCIDENT_RULE_NAME_EXISTS.String(), "trigger rule name already exists")
	ErrRuleInvalid    = errors.BadRequest(v1.ErrorReason_INCIDENT_RULE_INVALID.String(), "invalid trigger rule")

	ErrPlaybookNotFound   = errors.NotFound(v1.ErrorReason_INCIDENT_PLAYBOOK_NOT_FOUND.String(), "playbook not found")
	ErrPlaybookNameExists = errors.Conflict(v1.ErrorReason_INCIDENT_PLAYBOOK_NAME_EXISTS.String(), "playbook name already exists")
	ErrPlaybookInvalid    = errors.BadRequest(v1.ErrorReason_INCIDENT_PLAYBOOK_INVALID.String(), "invalid playbook")
	ErrPlaybookInUse      = errors.Conflict(v1.ErrorReason_INCIDENT_PLAYBOOK_IN_USE.String(), "playbook is still referenced by trigger rules")

	ErrProposalNotFound   = errors.NotFound(v1.ErrorReason_INCIDENT_PROPOSAL_NOT_FOUND.String(), "proposal not found")
	ErrProposalInvalid    = errors.BadRequest(v1.ErrorReason_INCIDENT_PROPOSAL_INVALID.String(), "invalid proposal")
	ErrProposalNotPending = errors.Conflict(v1.ErrorReason_INCIDENT_PROPOSAL_NOT_PENDING.String(), "proposal is not pending")
	ErrProposalDuplicate  = errors.Conflict(v1.ErrorReason_INCIDENT_PROPOSAL_DUPLICATE.String(), "an equivalent proposal is already pending or running")

	ErrUpdateInvalid = errors.BadRequest(v1.ErrorReason_INCIDENT_UPDATE_INVALID.String(), "invalid status update")
	ErrShareNotFound = errors.NotFound(v1.ErrorReason_INCIDENT_SHARE_NOT_FOUND.String(), "share link not found, expired or revoked")
	ErrShareInvalid  = errors.BadRequest(v1.ErrorReason_INCIDENT_SHARE_INVALID.String(), "invalid share link")

	ErrPostmortemInvalid  = errors.BadRequest(v1.ErrorReason_INCIDENT_POSTMORTEM_INVALID.String(), "invalid postmortem")
	ErrPostmortemLocked   = errors.Conflict(v1.ErrorReason_INCIDENT_POSTMORTEM_LOCKED.String(), "postmortem is completed; revise it first")
	ErrActionItemNotFound = errors.NotFound(v1.ErrorReason_INCIDENT_ACTION_ITEM_NOT_FOUND.String(), "action item not found")

	ErrSourceNotFound     = errors.NotFound(v1.ErrorReason_INCIDENT_SOURCE_NOT_FOUND.String(), "source not found")
	ErrSourceNameExists   = errors.Conflict(v1.ErrorReason_INCIDENT_SOURCE_NAME_EXISTS.String(), "source name already exists")
	ErrSourceInvalid      = errors.BadRequest(v1.ErrorReason_INCIDENT_SOURCE_INVALID.String(), "invalid source")
	ErrSourceUnauthorized = errors.Unauthorized(v1.ErrorReason_INCIDENT_SOURCE_UNAUTHORIZED.String(), "missing or invalid access token")
	ErrSourceForbidden    = errors.Forbidden(v1.ErrorReason_INCIDENT_SOURCE_FORBIDDEN.String(), "this source is not allowed to do that")

	ErrWebhookNotFound = errors.NotFound(v1.ErrorReason_INCIDENT_WEBHOOK_NOT_FOUND.String(), "webhook not found")
	ErrWebhookInvalid  = errors.BadRequest(v1.ErrorReason_INCIDENT_WEBHOOK_INVALID.String(), "invalid webhook")

	ErrSettingInvalid = errors.BadRequest(v1.ErrorReason_INCIDENT_SETTING_INVALID.String(), "invalid settings")
)

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，前端按 message 展示原因）。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}

func invalid(format string, args ...any) error { return detail(ErrInvalid, format, args...) }

// isNotFound 报告错误是否为任一「对象不存在」类哨兵。
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return kratosCodeIs(err, 404)
}

func kratosCodeIs(err error, code int32) bool {
	e := errors.FromError(err)
	return e != nil && e.Code == code
}

// truncate 按字符截断（不切开字符）；返回值不超过 n 个字符（超长时末位是省略号）。
// 此前超长时返回 n+1 个字符：按列宽 255 截断的标题落库报 22001，规则开故障因此在关联器里
// 无限重试，全平台告警都不归并。
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}

// truncateBytes 按字节截断且不切开字符。
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// cleanText 去掉 NUL 与首尾空白（jsonb / text 列不接受 NUL）。
func cleanText(s string) string { return strings.TrimSpace(strings.ReplaceAll(s, "\x00", "")) }

// uniq 去重并去空，保持顺序。
func uniq(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, it := range items {
		if it == "" {
			continue
		}
		if _, ok := seen[it]; ok {
			continue
		}
		seen[it] = struct{}{}
		out = append(out, it)
	}
	return out
}

// humanDuration 时长的中文可读形式（通知正文用）。
func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Minute)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	switch {
	case h >= 24:
		return fmt.Sprintf("%d 天 %d 小时", h/24, h%24)
	case h > 0:
		return fmt.Sprintf("%d 小时 %d 分钟", h, m)
	default:
		return fmt.Sprintf("%d 分钟", m)
	}
}

// i32 把计数收窄到 int32（计数都远小于上限，这里只是让转换的意图显式、免除逐处溢出告警）。
func i32(n int) int32 { return int32(min(n, math.MaxInt32)) } // #nosec G115 -- 已钳位
