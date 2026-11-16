// Package service 流程引擎域 proto 接口实现：参数编排、模型转换与身份提取。
package service

import (
	"context"
	"encoding/json"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is workflow 域 service providers.
// PermissionChecker 由 system 域 AuthzUsecase 实现，在组合根 cmd/server/wire.go 绑定。
var ProviderSet = wire.NewSet(NewDefinitionService, NewInstanceService, NewTaskService, NewConfigService, NewOpenService)

// PermissionChecker 判定用户是否持有某个接口操作的权限（超管恒为 true）。用于在
// 「参与者可见」之外放行管理视图：能否查看全部实例、能否终止 / 改派。
type PermissionChecker interface {
	Allowed(ctx context.Context, userID, operationCode string) (bool, error)
}

// 管理类操作的权限码（与授权中间件同一口径：md5(proto full method)）。
var (
	opListInstances     = bizsystem.OperationCode(v1.OperationWorkflowInstanceServiceListInstances)
	opTerminateInstance = bizsystem.OperationCode(v1.OperationWorkflowInstanceServiceTerminateInstance)
	opReassignTask      = bizsystem.OperationCode(v1.OperationWorkflowTaskServiceReassignTask)
)

// userID 取当前登录用户 ID。
func userID(ctx context.Context) (string, error) {
	return systemservice.UserIDFromContext(ctx)
}

// ts 零值时间转为 nil（proto 出参里「未发生」表达为字段缺失）。
func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// structOf 把 JSON 对象转为 Struct；不可表达时退化为空对象（不让整个响应失败）。
func structOf(raw []byte) *structpb.Struct {
	s := &structpb.Struct{}
	if len(raw) == 0 || s.UnmarshalJSON(raw) != nil {
		return &structpb.Struct{}
	}
	return s
}

// structOfMap 同 structOf，入参为 map。
func structOfMap(m map[string]any) *structpb.Struct {
	if len(m) == 0 {
		return &structpb.Struct{}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return &structpb.Struct{}
	}
	return structOf(raw)
}
