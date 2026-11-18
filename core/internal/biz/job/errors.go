// Package biz 作业中心域（job）管理面的业务核心：凭据、项目（Git 仓库）、作业模板、执行节点的
// 维护，以及提交执行、查看执行记录与日志、取消执行。定义仓储接口（依赖倒置），零驱动、零传输层 import。
//
// 执行引擎与执行节点（backend/job）是独立进程：两边共享的只有 job_* 表与 kit/jobcontract。
// 安全边界：
//   - 凭据密文只写不读——本包只调用 SecretSealer.Seal，没有任何解密入口；解密只发生在执行引擎；
//   - 目标主机一律经 HostSource 按执行人的 CMDB 数据范围解析，各用例不得自行读取 CMDB；
//   - 提交执行时把执行内容（项目与分支、Playbook、参数取值、主机快照）固化进执行记录，
//     之后模板的任何改动都不影响已提交的执行。
package biz

import (
	"fmt"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/job/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrVersionConflict   = errors.Conflict(v1.ErrorReason_JOB_VERSION_CONFLICT.String(), "modified by someone else, please refresh and retry")
	ErrSecretUnavailable = errors.ServiceUnavailable(v1.ErrorReason_JOB_SECRET_UNAVAILABLE.String(), "credential master key is not configured")

	ErrCredentialNotFound   = errors.NotFound(v1.ErrorReason_JOB_CREDENTIAL_NOT_FOUND.String(), "credential not found")
	ErrCredentialNameExists = errors.Conflict(v1.ErrorReason_JOB_CREDENTIAL_NAME_EXISTS.String(), "credential name already exists")
	ErrCredentialInvalid    = errors.BadRequest(v1.ErrorReason_JOB_CREDENTIAL_INVALID.String(), "invalid credential")
	ErrCredentialInUse      = errors.Conflict(v1.ErrorReason_JOB_CREDENTIAL_IN_USE.String(), "credential is still used by job templates")

	ErrProjectNotFound   = errors.NotFound(v1.ErrorReason_JOB_PROJECT_NOT_FOUND.String(), "project not found")
	ErrProjectNameExists = errors.Conflict(v1.ErrorReason_JOB_PROJECT_NAME_EXISTS.String(), "project name already exists")
	ErrProjectInvalid    = errors.BadRequest(v1.ErrorReason_JOB_PROJECT_INVALID.String(), "invalid project")
	ErrProjectInUse      = errors.Conflict(v1.ErrorReason_JOB_PROJECT_IN_USE.String(), "project is still used by job templates")

	ErrTemplateNotFound   = errors.NotFound(v1.ErrorReason_JOB_TEMPLATE_NOT_FOUND.String(), "job template not found")
	ErrTemplateNameExists = errors.Conflict(v1.ErrorReason_JOB_TEMPLATE_NAME_EXISTS.String(), "job template name already exists")
	ErrTemplateInvalid    = errors.BadRequest(v1.ErrorReason_JOB_TEMPLATE_INVALID.String(), "invalid job template")
	ErrTemplateDisabled   = errors.Conflict(v1.ErrorReason_JOB_TEMPLATE_DISABLED.String(), "job template is disabled")

	ErrTargetForbidden   = errors.Forbidden(v1.ErrorReason_JOB_TARGET_FORBIDDEN.String(), "target hosts are outside your CMDB data scope")
	ErrTargetInvalid     = errors.BadRequest(v1.ErrorReason_JOB_TARGET_INVALID.String(), "invalid target hosts")
	ErrParamsInvalid     = errors.BadRequest(v1.ErrorReason_JOB_PARAMS_INVALID.String(), "invalid job params")
	ErrExecutionNotFound = errors.NotFound(v1.ErrorReason_JOB_EXECUTION_NOT_FOUND.String(), "execution not found")
	ErrExecutionConflict = errors.Conflict(v1.ErrorReason_JOB_EXECUTION_CONFLICT.String(), "this template forbids concurrent runs and already has an unfinished execution")
	ErrExecutionFinished = errors.Conflict(v1.ErrorReason_JOB_EXECUTION_FINISHED.String(), "execution has already finished")

	ErrRiskBlocked         = errors.Forbidden(v1.ErrorReason_JOB_RISK_BLOCKED.String(), "blocked by a high-risk command rule")
	ErrApprovalUnavailable = errors.Conflict(v1.ErrorReason_JOB_APPROVAL_UNAVAILABLE.String(), "this execution requires approval but no usable approval workflow is configured")
	ErrAdhocInvalid        = errors.BadRequest(v1.ErrorReason_JOB_ADHOC_INVALID.String(), "invalid quick execution")

	ErrScriptNotFound   = errors.NotFound(v1.ErrorReason_JOB_SCRIPT_NOT_FOUND.String(), "script not found")
	ErrScriptNameExists = errors.Conflict(v1.ErrorReason_JOB_SCRIPT_NAME_EXISTS.String(), "script name already exists")
	ErrScriptInvalid    = errors.BadRequest(v1.ErrorReason_JOB_SCRIPT_INVALID.String(), "invalid script")

	ErrRiskRuleNotFound   = errors.NotFound(v1.ErrorReason_JOB_RISK_RULE_NOT_FOUND.String(), "risk rule not found")
	ErrRiskRuleNameExists = errors.Conflict(v1.ErrorReason_JOB_RISK_RULE_NAME_EXISTS.String(), "risk rule name already exists")
	ErrRiskRuleInvalid    = errors.BadRequest(v1.ErrorReason_JOB_RISK_RULE_INVALID.String(), "invalid risk rule")
	ErrPolicyInvalid      = errors.BadRequest(v1.ErrorReason_JOB_POLICY_INVALID.String(), "invalid job policy")

	ErrScheduleNotFound   = errors.NotFound(v1.ErrorReason_JOB_SCHEDULE_NOT_FOUND.String(), "schedule not found")
	ErrScheduleNameExists = errors.Conflict(v1.ErrorReason_JOB_SCHEDULE_NAME_EXISTS.String(), "schedule name already exists")
	ErrScheduleInvalid    = errors.BadRequest(v1.ErrorReason_JOB_SCHEDULE_INVALID.String(), "invalid schedule")

	ErrTriggerNotFound     = errors.NotFound(v1.ErrorReason_JOB_TRIGGER_NOT_FOUND.String(), "trigger not found")
	ErrTriggerNameExists   = errors.Conflict(v1.ErrorReason_JOB_TRIGGER_NAME_EXISTS.String(), "trigger name already exists")
	ErrTriggerInvalid      = errors.BadRequest(v1.ErrorReason_JOB_TRIGGER_INVALID.String(), "invalid trigger")
	ErrTriggerUnauthorized = errors.Unauthorized(v1.ErrorReason_JOB_TRIGGER_UNAUTHORIZED.String(), "invalid trigger token or signature")
	ErrTriggerDisabled     = errors.Conflict(v1.ErrorReason_JOB_TRIGGER_DISABLED.String(), "trigger is disabled")

	ErrRunForbidden = errors.Forbidden(v1.ErrorReason_JOB_RUN_FORBIDDEN.String(), "the owner has no permission to run job templates")

	ErrRunnerNotFound   = errors.NotFound(v1.ErrorReason_JOB_RUNNER_NOT_FOUND.String(), "runner not found")
	ErrRunnerNameExists = errors.Conflict(v1.ErrorReason_JOB_RUNNER_NAME_EXISTS.String(), "runner name already exists")
	ErrRunnerInvalid    = errors.BadRequest(v1.ErrorReason_JOB_RUNNER_INVALID.String(), "invalid runner")
)

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，
// 前端按 message 展示原因）。说明里不得出现凭据内容。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}

// InUse 凭据 / 项目仍被作业模板引用：说明里带上引用它的模板名（data 层在删除事务内取到）。
func InUse(base *errors.Error, templateNames []string, total int) error {
	names := strings.Join(templateNames, ", ")
	if total > len(templateNames) {
		names += ", ..."
	}
	return detail(base, "still used by %d job template(s): %s", total, names)
}

// TargetInvalid 目标主机配置不合法（资源类型不存在、地址字段不存在或为敏感属性等；由 HostSource 的实现返回）。
func TargetInvalid(format string, args ...any) error {
	return detail(ErrTargetInvalid, format, args...)
}

// TemplateInvalid 模板内容不合法（引用的项目 / 凭据在保存时已不存在等；data 层在写事务内判定）。
func TemplateInvalid(format string, args ...any) error {
	return detail(ErrTemplateInvalid, format, args...)
}

// Message 取错误里面向用户的说明（Kratos 结构化错误取其 message，其余取 Error()）。
func Message(err error) string {
	if e := errors.FromError(err); e != nil && e.Message != "" {
		return e.Message
	}
	return err.Error()
}

// Rejected 报告错误是否为「请求被业务规则拒绝」（4xx）：重试也不会成功，应记为失败而不是稍后再试。
func Rejected(err error) bool {
	e := errors.FromError(err)
	return e != nil && e.Code >= 400 && e.Code < 500
}
