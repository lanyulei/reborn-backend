// Package biz CMDB 域业务核心：资源模型（元数据）、资源实例与状态机、关系引擎、
// 变更历史；定义仓储接口（依赖倒置），零驱动、零传输层 import。
package biz

import (
	"fmt"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/cmdb/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrModelNotFound          = errors.NotFound(v1.ErrorReason_CMDB_MODEL_NOT_FOUND.String(), "model not found")
	ErrModelCodeExists        = errors.Conflict(v1.ErrorReason_CMDB_MODEL_CODE_EXISTS.String(), "model code already exists")
	ErrModelInUse             = errors.Conflict(v1.ErrorReason_CMDB_MODEL_IN_USE.String(), "model still has resources or relation types")
	ErrFieldNotFound          = errors.NotFound(v1.ErrorReason_CMDB_FIELD_NOT_FOUND.String(), "field not found")
	ErrFieldCodeExists        = errors.Conflict(v1.ErrorReason_CMDB_FIELD_CODE_EXISTS.String(), "field code already exists")
	ErrFieldInvalid           = errors.BadRequest(v1.ErrorReason_CMDB_FIELD_INVALID.String(), "invalid field definition")
	ErrRelationTypeNotFound   = errors.NotFound(v1.ErrorReason_CMDB_RELATION_TYPE_NOT_FOUND.String(), "relation type not found")
	ErrRelationTypeCodeExists = errors.Conflict(v1.ErrorReason_CMDB_RELATION_TYPE_CODE_EXISTS.String(), "relation type code already exists")
	ErrRelationTypeInUse      = errors.Conflict(v1.ErrorReason_CMDB_RELATION_TYPE_IN_USE.String(), "relation type still has relations")
	ErrCINotFound             = errors.NotFound(v1.ErrorReason_CMDB_CI_NOT_FOUND.String(), "resource not found")
	ErrCIInvalid              = errors.BadRequest(v1.ErrorReason_CMDB_CI_INVALID.String(), "invalid resource attributes")
	ErrCIDuplicate            = errors.Conflict(v1.ErrorReason_CMDB_CI_DUPLICATE.String(), "resource name or unique key already exists")
	ErrCIVersionConflict      = errors.Conflict(v1.ErrorReason_CMDB_CI_VERSION_CONFLICT.String(), "resource was modified by someone else")
	ErrCITransitionInvalid    = errors.BadRequest(v1.ErrorReason_CMDB_CI_TRANSITION_INVALID.String(), "status transition not allowed")
	ErrCIInUse                = errors.Conflict(v1.ErrorReason_CMDB_CI_IN_USE.String(), "resource still has relations")
	ErrRelationNotAllowed     = errors.BadRequest(v1.ErrorReason_CMDB_RELATION_NOT_ALLOWED.String(), "relation not allowed between these resource types")
	ErrRelationExists         = errors.Conflict(v1.ErrorReason_CMDB_RELATION_EXISTS.String(), "relation already exists")
	ErrRelationNotFound       = errors.NotFound(v1.ErrorReason_CMDB_RELATION_NOT_FOUND.String(), "relation not found")
	ErrRelationCardinality    = errors.Conflict(v1.ErrorReason_CMDB_RELATION_CARDINALITY.String(), "relation cardinality violated")
	ErrViewInvalid            = errors.BadRequest(v1.ErrorReason_CMDB_VIEW_INVALID.String(), "invalid detail view")
	ErrScopeForbidden         = errors.Forbidden(v1.ErrorReason_CMDB_SCOPE_FORBIDDEN.String(), "resource is out of your data scope")
	ErrSourceNotFound         = errors.NotFound(v1.ErrorReason_CMDB_SOURCE_NOT_FOUND.String(), "sync source not found")
	ErrSourceNameExists       = errors.Conflict(v1.ErrorReason_CMDB_SOURCE_NAME_EXISTS.String(), "sync source name already exists")
	ErrSourceInvalid          = errors.BadRequest(v1.ErrorReason_CMDB_SOURCE_INVALID.String(), "invalid sync source config")
	ErrSourceVersionConflict  = errors.Conflict(v1.ErrorReason_CMDB_SOURCE_VERSION_CONFLICT.String(), "sync source was modified by someone else")
	ErrSyncBusy               = errors.Conflict(v1.ErrorReason_CMDB_SYNC_BUSY.String(), "a sync task for this source is already pending or running")
	ErrSecretUnavailable      = errors.ServiceUnavailable(v1.ErrorReason_CMDB_SECRET_UNAVAILABLE.String(), "credential master key is not configured")
	ErrGroupNotFound          = errors.NotFound(v1.ErrorReason_CMDB_GROUP_NOT_FOUND.String(), "model group not found")
	ErrGroupCodeExists        = errors.Conflict(v1.ErrorReason_CMDB_GROUP_CODE_EXISTS.String(), "model group code already exists")
	ErrGroupInUse             = errors.Conflict(v1.ErrorReason_CMDB_GROUP_IN_USE.String(), "model group is builtin or still has models")
	// ErrRoleNotFound 复用 system 域错误码口径（角色不存在）。
	ErrRoleNotFound = errors.NotFound("ROLE_NOT_FOUND", "role not found")
)

// Actor 操作者上下文（审计与历史用）。
type Actor struct {
	UserID    string
	RequestID string
	// Source 来源：manual / sync:<id> / assistant；缺省 manual。
	Source string
}

func (a Actor) source() string {
	if a.Source == "" {
		return SourceManual
	}
	return a.Source
}

// SourceManual 人工来源字面量。
const SourceManual = cmdbcontract.SourceManual

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，
// 前端按 message 展示原因，如"field ip is required"）。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}
