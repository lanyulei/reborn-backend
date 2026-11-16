package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// ProviderSet is workflow 域 data providers.
var ProviderSet = wire.NewSet(
	NewDefinitionRepo,
	NewInstanceRepo,
	NewJobRepo,
	NewConfigRepo,
	NewWebhookCaller,
	wire.Bind(new(biz.DefinitionReader), new(*definitionRepo)),
	wire.Bind(new(biz.DefinitionWriter), new(*definitionRepo)),
	wire.Bind(new(biz.DefinitionPublisher), new(*definitionRepo)),
	wire.Bind(new(biz.VersionReader), new(*definitionRepo)),
	wire.Bind(new(biz.InstanceStore), new(*instanceRepo)),
	wire.Bind(new(biz.InstanceReader), new(*instanceRepo)),
	wire.Bind(new(biz.ParticipantChecker), new(*instanceRepo)),
	wire.Bind(new(biz.InstanceLookup), new(*instanceRepo)),
	wire.Bind(new(biz.JobQueue), new(*jobRepo)),
	wire.Bind(new(biz.JobTx), new(*jobRepo)),
	wire.Bind(new(biz.JobStats), new(*jobRepo)),
	wire.Bind(new(biz.CalendarStore), new(*configRepo)),
	wire.Bind(new(biz.DelegationRepo), new(*configRepo)),
	wire.Bind(new(biz.Proxies), new(*configRepo)),
	wire.Bind(new(biz.WebhookRepo), new(*configRepo)),
	wire.Bind(new(biz.WebhookResolver), new(*configRepo)),
	wire.Bind(new(biz.WebhookChecker), new(*configRepo)),
	wire.Bind(new(biz.ServiceCaller), new(*WebhookCaller)),
	wire.Bind(new(biz.ClientRepo), new(*configRepo)),
	wire.Bind(new(biz.ClientAuthenticator), new(*configRepo)),
)
