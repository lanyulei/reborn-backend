package biz

import "github.com/google/wire"

// ProviderSet is knowledge 域 biz providers。跨域窄接口（RoleSource / Directory / RoleDirectory /
// Files / Summarizer / NotificationWriter）的绑定在 internal/biz/biz.go 聚合处完成。
var ProviderSet = wire.NewSet(
	NewConfig,
	NewEmbedGuard,
	NewSpaceUsecase,
	NewSearchUsecase,
	NewDocumentUsecase,
	NewIngestUsecase,
	NewIndexUsecase,
	NewGovernanceUsecase,
	NewFileAccess,
	wire.Bind(new(Retriever), new(*SearchUsecase)),
)
