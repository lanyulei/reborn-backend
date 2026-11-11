package service

import "github.com/google/wire"

// ProviderSet is assistant 域 service providers（含启动期生成的工具目录）。
var ProviderSet = wire.NewSet(NewToolCatalog, NewAssistantService, NewAssistantChatService)
