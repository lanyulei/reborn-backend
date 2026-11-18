package biz

import (
	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 第二、三期的哨兵错误（reason 来自 api/monitor/v1 错误码枚举）。
var (
	ErrRuleTemplateNotFound = errors.NotFound(v1.ErrorReason_MONITOR_RULE_TEMPLATE_NOT_FOUND.String(), "rule template pack not found")
	ErrSimulateInvalid      = errors.BadRequest(v1.ErrorReason_MONITOR_SIMULATE_INVALID.String(), "invalid simulate request")

	ErrAlertNotFiring     = errors.Conflict(v1.ErrorReason_MONITOR_ALERT_NOT_FIRING.String(), "alert is no longer firing")
	ErrAlertAlreadyAcked  = errors.Conflict(v1.ErrorReason_MONITOR_ALERT_ALREADY_ACKED.String(), "alert is already acknowledged by someone else")
	ErrAlertActionInvalid = errors.BadRequest(v1.ErrorReason_MONITOR_ALERT_ACTION_INVALID.String(), "invalid alert action")
	ErrAlertTicketExists  = errors.Conflict(v1.ErrorReason_MONITOR_ALERT_TICKET_EXISTS.String(), "alert already has a ticket")

	ErrAlertViewNotFound   = errors.NotFound(v1.ErrorReason_MONITOR_ALERT_VIEW_NOT_FOUND.String(), "alert view not found")
	ErrAlertViewNameExists = errors.Conflict(v1.ErrorReason_MONITOR_ALERT_VIEW_NAME_EXISTS.String(), "you already have a view with this name")
	ErrAlertViewInvalid    = errors.BadRequest(v1.ErrorReason_MONITOR_ALERT_VIEW_INVALID.String(), "invalid alert view")

	ErrEscalationNotFound   = errors.NotFound(v1.ErrorReason_MONITOR_ESCALATION_NOT_FOUND.String(), "escalation policy not found")
	ErrEscalationNameExists = errors.Conflict(v1.ErrorReason_MONITOR_ESCALATION_NAME_EXISTS.String(), "escalation policy name already exists")
	ErrEscalationInvalid    = errors.BadRequest(v1.ErrorReason_MONITOR_ESCALATION_INVALID.String(), "invalid escalation policy")
	ErrEscalationInUse      = errors.Conflict(v1.ErrorReason_MONITOR_ESCALATION_IN_USE.String(), "escalation policy is still referenced by rules")

	ErrInhibitNotFound = errors.NotFound(v1.ErrorReason_MONITOR_INHIBIT_NOT_FOUND.String(), "inhibit rule not found")
	ErrInhibitInvalid  = errors.BadRequest(v1.ErrorReason_MONITOR_INHIBIT_INVALID.String(), "invalid inhibit rule")

	ErrRecordRuleNotFound   = errors.NotFound(v1.ErrorReason_MONITOR_RECORD_RULE_NOT_FOUND.String(), "record rule not found")
	ErrRecordRuleNameExists = errors.Conflict(v1.ErrorReason_MONITOR_RECORD_RULE_NAME_EXISTS.String(), "a record rule with this metric name already exists")
	ErrRecordRuleInvalid    = errors.BadRequest(v1.ErrorReason_MONITOR_RECORD_RULE_INVALID.String(), "invalid record rule")
	ErrRecordRuleNoWriteURL = errors.BadRequest(v1.ErrorReason_MONITOR_RECORD_RULE_NO_WRITE_URL.String(), "the datasource has no remote write url")
)

// ErrRuleEscalationMissing 规则新引用的升级策略在保存时已被删除（data 层在写事务内判定，与策略的删除互斥）。
var ErrRuleEscalationMissing = detail(ErrRuleInvalid, "the selected escalation policy no longer exists")

// Detail 以哨兵错误的 code / reason 构造带具体说明的错误（data 层在写事务内判定出的业务错误用）。
func Detail(base *errors.Error, format string, args ...any) error {
	return detail(base, format, args...)
}

// WithMetadata 以哨兵错误的 code / reason / message 构造带 metadata 的错误（前端按 metadata 取数）。
func WithMetadata(base *errors.Error, md map[string]string) error {
	return errors.New(int(base.Code), base.Reason, base.Message).WithMetadata(md)
}
