package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
)

// ProviderSet is cmdb 域 data providers。
var ProviderSet = wire.NewSet(
	NewModelRepo, NewFieldRepo, NewRelationTypeRepo, NewCIRepo, NewRelationRepo, NewHistoryRepo, NewViewRepo, NewScopeRepo, NewSyncSourceRepo, NewSyncTaskRepo, NewSyncRunRepo, NewSyncSecretBox, NewAttrIndexer,
	NewGroupRepo, NewFavoriteRepo, NewSchemaAuditRepo, NewOverviewRepo, NewUniqueRuleRepo,
	wire.Bind(new(biz.ModelRepo), new(*modelRepo)),
	wire.Bind(new(biz.FieldRepo), new(*fieldRepo)),
	wire.Bind(new(biz.RelationTypeRepo), new(*relationTypeRepo)),
	wire.Bind(new(biz.CIRepo), new(*ciRepo)),
	wire.Bind(new(biz.OwnerDirectory), new(*ciRepo)),
	wire.Bind(new(biz.RelationRepo), new(*relationRepo)),
	wire.Bind(new(biz.HistoryRepo), new(*historyRepo)),
	wire.Bind(new(biz.ViewRepo), new(*viewRepo)),
	wire.Bind(new(biz.ScopeRepo), new(*scopeRepo)),
	wire.Bind(new(biz.SyncSourceRepo), new(*syncSourceRepo)),
	wire.Bind(new(biz.SyncTaskRepo), new(*syncTaskRepo)),
	wire.Bind(new(biz.SyncRunRepo), new(*syncRunRepo)),
	wire.Bind(new(biz.CredentialSealer), new(*SyncSecretBox)),
	wire.Bind(new(biz.AttrIndexer), new(*attrIndexer)),
	wire.Bind(new(biz.GroupRepo), new(*groupRepo)),
	wire.Bind(new(biz.FavoriteRepo), new(*favoriteRepo)),
	wire.Bind(new(biz.SchemaAuditRepo), new(*schemaAuditRepo)),
	wire.Bind(new(biz.OverviewRepo), new(*overviewRepo)),
	wire.Bind(new(biz.UniqueRuleRepo), new(uniqueRuleRepo)),
)
