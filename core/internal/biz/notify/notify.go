// notify 域 biz providers：用例装配（仓储接口在 data 层绑定）。
package biz

import "github.com/google/wire"

// ProviderSet is notify 域 biz providers.
var ProviderSet = wire.NewSet(NewChannelUsecase, NewTemplateUsecase, NewRouteUsecase, NewSilenceUsecase, NewSubscriptionUsecase, NewProducerUsecase, NewEventUsecase, NewDispatchUsecase, NewDeliveryLogUsecase, NewDeadLetterUsecase, NewNotificationUsecase, NewPreviewUsecase, NewChannelHealthUsecase)
