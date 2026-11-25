// Package biz 监控告警域（monitor）管理面的业务核心：数据源、规则分组与角色数据范围、告警规则
// （含版本、批量、导入导出）、告警事件查询、屏蔽规则、订阅规则，以及把评估引擎写入的通知发件箱
// 派发为通知事件的后台用例。定义仓储接口（依赖倒置），零驱动、零传输层 import。
//
// 评估引擎（backend/monitor）是独立进程：两边共享的只有 monitor_* 表与 kit/monitorcontract。
// 数据范围统一由 ScopeUsecase 解析，各用例不得自行判断。
package biz

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/monitor/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrForbidden         = errors.Forbidden(v1.ErrorReason_MONITOR_FORBIDDEN.String(), "out of data scope or manage level required")
	ErrVersionConflict   = errors.Conflict(v1.ErrorReason_MONITOR_VERSION_CONFLICT.String(), "modified by someone else, please refresh and retry")
	ErrTimeRangeInvalid  = errors.BadRequest(v1.ErrorReason_MONITOR_TIME_RANGE_INVALID.String(), "invalid time range")
	ErrSecretUnavailable = errors.ServiceUnavailable(v1.ErrorReason_MONITOR_SECRET_UNAVAILABLE.String(), "credential master key is not configured")

	ErrDatasourceNotFound    = errors.NotFound(v1.ErrorReason_MONITOR_DATASOURCE_NOT_FOUND.String(), "datasource not found")
	ErrDatasourceNameExists  = errors.Conflict(v1.ErrorReason_MONITOR_DATASOURCE_NAME_EXISTS.String(), "datasource name already exists")
	ErrDatasourceInvalid     = errors.BadRequest(v1.ErrorReason_MONITOR_DATASOURCE_INVALID.String(), "invalid datasource config")
	ErrDatasourceInUse       = errors.Conflict(v1.ErrorReason_MONITOR_DATASOURCE_IN_USE.String(), "datasource is still referenced by rules")
	ErrDatasourceDisabled    = errors.Conflict(v1.ErrorReason_MONITOR_DATASOURCE_DISABLED.String(), "datasource is disabled")
	ErrDatasourceUnreachable = errors.New(502, v1.ErrorReason_MONITOR_DATASOURCE_UNREACHABLE.String(), "datasource is unreachable")
	ErrQueryInvalid          = errors.BadRequest(v1.ErrorReason_MONITOR_QUERY_INVALID.String(), "invalid query")
	ErrQueryBusy             = errors.New(429, v1.ErrorReason_MONITOR_QUERY_BUSY.String(), "too many concurrent queries, please retry later")
	// ErrDatasourceSecretRequired 不能沿用已保存的凭据 / 请求头值，须重新填写。
	ErrDatasourceSecretRequired = errors.BadRequest(v1.ErrorReason_MONITOR_DATASOURCE_SECRET_REQUIRED.String(), "saved credentials cannot be reused, please enter them again")
	// ErrDatasourceAddressBlocked 数据源地址指向不允许访问的地址（链路本地 / 云元数据，按配置还有回环）。
	ErrDatasourceAddressBlocked = errors.BadRequest(v1.ErrorReason_MONITOR_DATASOURCE_ADDRESS_BLOCKED.String(), "datasource address is not allowed")

	ErrGroupNotFound    = errors.NotFound(v1.ErrorReason_MONITOR_GROUP_NOT_FOUND.String(), "rule group not found")
	ErrGroupNameExists  = errors.Conflict(v1.ErrorReason_MONITOR_GROUP_NAME_EXISTS.String(), "rule group name already exists under the same parent")
	ErrGroupInUse       = errors.Conflict(v1.ErrorReason_MONITOR_GROUP_IN_USE.String(), "rule group still has child groups or rules")
	ErrGroupInvalid     = errors.BadRequest(v1.ErrorReason_MONITOR_GROUP_INVALID.String(), "invalid rule group")
	ErrRoleScopeInvalid = errors.BadRequest(v1.ErrorReason_MONITOR_ROLE_SCOPE_INVALID.String(), "invalid role scope")

	ErrRuleNotFound        = errors.NotFound(v1.ErrorReason_MONITOR_RULE_NOT_FOUND.String(), "alert rule not found")
	ErrRuleNameExists      = errors.Conflict(v1.ErrorReason_MONITOR_RULE_NAME_EXISTS.String(), "rule name already exists in this group")
	ErrRuleInvalid         = errors.BadRequest(v1.ErrorReason_MONITOR_RULE_INVALID.String(), "invalid rule config")
	ErrRuleVersionNotFound = errors.NotFound(v1.ErrorReason_MONITOR_RULE_VERSION_NOT_FOUND.String(), "rule version not found")
	ErrRuleBatchInvalid    = errors.BadRequest(v1.ErrorReason_MONITOR_RULE_BATCH_INVALID.String(), "invalid batch operation")
	ErrRuleImportInvalid   = errors.BadRequest(v1.ErrorReason_MONITOR_RULE_IMPORT_INVALID.String(), "import content is not a valid rule file")

	ErrAlertNotFound = errors.NotFound(v1.ErrorReason_MONITOR_ALERT_NOT_FOUND.String(), "alert not found")

	ErrMuteNotFound = errors.NotFound(v1.ErrorReason_MONITOR_MUTE_NOT_FOUND.String(), "mute rule not found")
	ErrMuteInvalid  = errors.BadRequest(v1.ErrorReason_MONITOR_MUTE_INVALID.String(), "invalid mute rule")

	ErrSubscriptionNotFound = errors.NotFound(v1.ErrorReason_MONITOR_SUBSCRIPTION_NOT_FOUND.String(), "subscription not found")
	ErrSubscriptionInvalid  = errors.BadRequest(v1.ErrorReason_MONITOR_SUBSCRIPTION_INVALID.String(), "invalid subscription")
)

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，
// 前端按 message 展示原因）。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}

// DatasourceInUse 数据源仍被规则指定引用：说明里带上引用它的规则名（data 层在删除事务内取到）。
func DatasourceInUse(ruleNames []string, total int) error {
	return detail(ErrDatasourceInUse, "datasource is still referenced by %d rule(s): %s", total, joinNames(ruleNames, total))
}

// DatasourceInvalid 数据源连接配置不合法（地址、认证、请求头）；说明来自 kit/promclient 的校验，
// 不含凭据。
func DatasourceInvalid(format string, args ...any) error {
	return detail(ErrDatasourceInvalid, format, args...)
}

// GroupReferenced 分组仍被屏蔽规则 / 订阅规则引用（data 层在删除事务内取到数量）：409，数量同时放进
// metadata，前端可以用自己的文案展示。
func GroupReferenced(mutes, subscriptions int) error {
	return errors.New(int(ErrGroupInUse.Code), ErrGroupInUse.Reason,
		fmt.Sprintf("rule group is still referenced by %d mute rule(s) and %d subscription(s)", mutes, subscriptions)).
		WithMetadata(map[string]string{"mute_count": strconv.Itoa(mutes), "subscription_count": strconv.Itoa(subscriptions)})
}

// DatasourceAddressBlocked 数据源地址指向不允许访问的地址；reason 是地址策略给出的说明（不含凭据）。
func DatasourceAddressBlocked(reason string) error {
	return detail(ErrDatasourceAddressBlocked, "%s", reason)
}

// GroupReferencedBy 分组仍被记录规则（以它为所属分组）、升级策略 / 抑制规则（以它为作用分组）引用（data 层
// 在删除事务内取到数量）：409，与 GroupReferenced 同一个错误码，数量同时放进 metadata。
func GroupReferencedBy(recordRules, escalations, inhibits int) error {
	return errors.New(int(ErrGroupInUse.Code), ErrGroupInUse.Reason,
		fmt.Sprintf("rule group is still referenced by %d record rule(s), %d escalation policies and %d inhibit rules", recordRules, escalations, inhibits)).
		WithMetadata(map[string]string{
			"record_rule_count": strconv.Itoa(recordRules), "escalation_count": strconv.Itoa(escalations), "inhibit_count": strconv.Itoa(inhibits),
		})
}

// ErrRuleDatasourceMissing 规则指定的数据源在保存时已不存在（data 层在写事务内判定）。
var ErrRuleDatasourceMissing = detail(ErrRuleInvalid, "a selected datasource no longer exists")

// GroupInvalid 分组参数不合法（层级 / 总数上限由 data 层在写事务内判定）。
func GroupInvalid(format string, args ...any) error { return detail(ErrGroupInvalid, format, args...) }

// QueryFailed 把数据源返回的错误说明包装成领域错误：badQuery 为查询本身的问题（400），
// 否则为数据源不可达（502）。msg 已由客户端去除凭据并截断。
func QueryFailed(badQuery bool, msg string) error {
	if badQuery {
		return detail(ErrQueryInvalid, "%s", msg)
	}
	return detail(ErrDatasourceUnreachable, "%s", msg)
}

func joinNames(names []string, total int) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	if total > len(names) {
		out += ", ..."
	}
	return out
}

// truncate 按字符截断（落库列为 varchar(n)）。
// dup: biz/workflow、biz/ticket、biz/oncall 各有一份同名同义的私有函数。各域的 biz 包不能互相 import，
// 合并需要新建共享包并改那三个域的文件，不在本域的改动范围内，留待统一处理。
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// truncateBytes 按字节截断到至多 n 字节，且不切开字符（对方按字节限长时用）。
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
