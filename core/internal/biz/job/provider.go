package biz

import (
	"github.com/google/wire"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// ProviderSet is 作业中心域 biz providers。跨域窄接口（Directory / HostSource）在组合根 internal/biz 绑定。
var ProviderSet = wire.NewSet(
	NewCredentialUsecase, NewProjectUsecase, NewTargetUsecase, NewTemplateUsecase, NewExecutionUsecase, NewRunnerUsecase,
	NewExecutionDeps, NewApprovalSyncer, NewScriptUsecase, NewPolicyUsecase, NewScheduleUsecase,
	NewTriggerDeps, NewTriggerUsecase, NewNotifyUsecase, NewHousekeepUsecase,
)

// errorsIs 按 code + reason 比较 Kratos 结构化错误。
func errorsIs(err, target error) bool { return kerrors.Is(err, target) }
