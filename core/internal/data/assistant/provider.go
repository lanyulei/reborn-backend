package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

// ProviderSet is assistant 域 data providers（仓储 + 模型客户端 + 回环执行器）。
var ProviderSet = wire.NewSet(
	NewConversationRepo, NewMessageRepo, NewToolCallRepo, NewChatModel, NewLoopbackInvoker,
	wire.Bind(new(biz.ConversationRepo), new(*conversationRepo)),
	wire.Bind(new(biz.MessageRepo), new(*messageRepo)),
	wire.Bind(new(biz.ToolCallRepo), new(*toolCallRepo)),
	wire.Bind(new(biz.ChatModel), new(*ChatModelClient)),
	wire.Bind(new(biz.ToolInvoker), new(*LoopbackInvoker)),
)
