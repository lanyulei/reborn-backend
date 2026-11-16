package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
)

// ProviderSet is ticket 域 data providers.
var ProviderSet = wire.NewSet(
	NewCatalogRepo,
	NewTicketRepo,
	NewCommentRepo,
	wire.Bind(new(biz.CatalogReader), new(*catalogRepo)),
	wire.Bind(new(biz.CategoryWriter), new(*catalogRepo)),
	wire.Bind(new(biz.TemplateWriter), new(*catalogRepo)),
	wire.Bind(new(biz.WorkflowReferences), new(*catalogRepo)),
	wire.Bind(new(biz.TicketStore), new(*ticketRepo)),
	wire.Bind(new(biz.TicketReader), new(*ticketRepo)),
	wire.Bind(new(biz.CommentRepo), new(*commentRepo)),
	wire.Bind(new(biz.TicketOps), new(*ticketRepo)),
	wire.Bind(new(biz.TicketInsight), new(*ticketRepo)),
)
