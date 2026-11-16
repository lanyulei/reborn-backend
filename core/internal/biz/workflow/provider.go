package biz

import "github.com/google/wire"

// ProviderSet is workflow 域 biz providers。Directory / Users 由 system 域实现、
// Listeners / Viewers 由各业务域登记，均在顶层 biz 聚合处装配。
var ProviderSet = wire.NewSet(NewDefinitionUsecase, NewRuntimeUsecase, NewJobUsecase, NewCalendarUsecase, NewDelegationUsecase, NewWebhookUsecase,
	NewClientUsecase, NewOpenUsecase)
