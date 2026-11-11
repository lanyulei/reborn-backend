package biz

import "github.com/google/wire"

// ProviderSet is assistant 域 biz providers。跨域窄接口（PermissionSource /
// ActorDescriber）的绑定在 internal/biz/biz.go 聚合处完成。
var ProviderSet = wire.NewSet(NewAgentConfig, NewAgentUsecase, NewConversationUsecase)
