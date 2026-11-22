package biz

import (
	"fmt"

	v1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/agent/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrProfileNotFound   = errors.NotFound(v1.ErrorReason_AGENT_PROFILE_NOT_FOUND.String(), "agent profile not found")
	ErrProfileInvalid    = errors.BadRequest(v1.ErrorReason_AGENT_PROFILE_INVALID.String(), "invalid agent profile")
	ErrProfileNameExists = errors.Conflict(v1.ErrorReason_AGENT_PROFILE_NAME_EXISTS.String(), "agent profile name already exists")
	ErrVersionConflict   = errors.Conflict(v1.ErrorReason_AGENT_VERSION_CONFLICT.String(), "modified by someone else, please refresh and retry")
	ErrTaskNotFound      = errors.NotFound(v1.ErrorReason_AGENT_TASK_NOT_FOUND.String(), "agent task not found")
	ErrTaskStateInvalid  = errors.Conflict(v1.ErrorReason_AGENT_TASK_STATE_INVALID.String(), "operation not allowed in the current task state")
	ErrRunNotFound       = errors.NotFound(v1.ErrorReason_AGENT_RUN_NOT_FOUND.String(), "agent run not found")
	ErrForbidden         = errors.Forbidden(v1.ErrorReason_AGENT_FORBIDDEN.String(), "commander, responder or admin required")
	ErrDisabled          = errors.ServiceUnavailable(v1.ErrorReason_AGENT_DISABLED.String(), "agent is not enabled")
	ErrAccountInvalid    = errors.BadRequest(v1.ErrorReason_AGENT_ACCOUNT_INVALID.String(), "agent user must be an enabled machine account")
	ErrTaskExists        = errors.Conflict(v1.ErrorReason_AGENT_TASK_EXISTS.String(), "the incident already has a queued or running agent task")
	ErrProfileInUse      = errors.Conflict(v1.ErrorReason_AGENT_PROFILE_IN_USE.String(), "agent profile still has queued or running tasks")
	ErrTimeRangeInvalid  = errors.BadRequest(v1.ErrorReason_AGENT_TIME_RANGE_INVALID.String(), "invalid time range")
)

// detail 给哨兵错误附上具体原因（reason / code 不变）。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}
