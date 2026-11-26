package biz

import "github.com/google/wire"

// ProviderSet is 值班域 biz providers。
var ProviderSet = wire.NewSet(NewConfig, NewNotifier, NewGroupUsecase, NewScheduleUsecase, NewShareUsecase, NewWorkerUsecase, NewDutyUsecase)
