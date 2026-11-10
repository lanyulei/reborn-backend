// notify 域 service providers：各 proto Server 的实现装配（跨域经顶层
// internal/service 聚合）。
package service

import "github.com/google/wire"

// ProviderSet is notify 域 service providers.
var ProviderSet = wire.NewSet(NewNotifyChannelService, NewNotifyTemplateService, NewNotifyRouteService, NewNotifySilenceService, NewNotifySubscriptionService, NewNotifyProducerService, NewNotifyEventService, NewNotifyDispatchService, NewNotifyDeliveryLogService, NewNotifyDeadLetterService)
