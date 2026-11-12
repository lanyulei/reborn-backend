// notify 域 data providers：凭据加密器与仓储实现（接口在消费侧 biz/notify 定义，
// 此处绑定；data 顶层不聚合各域 ProviderSet，由 cmd/server/wire.go 显式列出）。
package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// ProviderSet is notify 域 data providers.
var ProviderSet = wire.NewSet(
	NewSecretBox,
	NewChannelRepo,
	NewTemplateRepo,
	NewRouteRepo,
	NewSilenceRepo,
	NewSubscriptionRepo,
	NewProducerRepo,
	NewEventRepo,
	NewDispatchRepo,
	NewDeliveryLogRepo,
	NewDeadLetterRepo,
	NewChannelHealthRepo,
	wire.Bind(new(biz.ChannelRepo), new(*channelRepo)),
	wire.Bind(new(biz.ChannelHealthRepo), new(*channelHealthRepo)),
	wire.Bind(new(biz.TemplateRepo), new(*templateRepo)),
	wire.Bind(new(biz.RouteRepo), new(*routeRepo)),
	wire.Bind(new(biz.ChannelReferenceChecker), new(*routeRepo)),
	wire.Bind(new(biz.SilenceRepo), new(*silenceRepo)),
	wire.Bind(new(biz.SubscriptionRepo), new(*subscriptionRepo)),
	wire.Bind(new(biz.ProducerRepo), new(*producerRepo)),
	wire.Bind(new(biz.EventRepo), new(*eventRepo)),
	wire.Bind(new(biz.EventWriter), new(*eventRepo)),
	wire.Bind(new(biz.EventTxRepo), new(*eventRepo)),
	wire.Bind(new(biz.DispatchRepo), new(*dispatchRepo)),
	wire.Bind(new(biz.DeliveryLogRepo), new(*deliveryLogRepo)),
	wire.Bind(new(biz.DeadLetterRepo), new(*deadLetterRepo)),
)
