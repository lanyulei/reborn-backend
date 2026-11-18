// Package service 作业中心域 proto 接口实现：只做身份提取、参数编排与 proto ↔ 领域模型转换；
// 业务规则、目标主机的数据范围判定全部收敛在 biz。任何响应都不含凭据密文。
package service

import (
	"time"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is 作业中心域 service providers。
var ProviderSet = wire.NewSet(
	NewCredentialService, NewProjectService, NewTemplateService, NewExecutionService, NewRunnerService,
	NewScriptService, NewPolicyService, NewScheduleService, NewTriggerService,
)

// optTime proto 时间戳 → *time.Time（未设置为 nil）。
func optTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

// protoTime *time.Time → proto 时间戳（nil 保持为空）。
func protoTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
