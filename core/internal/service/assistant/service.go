// Package service 实现 api/assistant/v1 生成的服务接口：会话管理（CRUD）与
// 对话编排（Chat/Confirm 以事件回调驱动，SSE 编码在 server 层）。
package service

import (
	"context"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AssistantService 实现生成的 AssistantServiceServer（会话管理，仅本人）。
type AssistantService struct {
	uc *biz.ConversationUsecase
}

// NewAssistantService new an AssistantService.
func NewAssistantService(uc *biz.ConversationUsecase) *AssistantService {
	return &AssistantService{uc: uc}
}

// ListConversations implements proto AssistantService/ListConversations.
func (s *AssistantService) ListConversations(ctx context.Context, req *v1.ListConversationsRequest) (*v1.ListConversationsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.List(ctx, userID, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListConversationsResponse{Items: make([]*v1.AssistantConversation, 0, len(items)), Total: total}
	for _, c := range items {
		resp.Items = append(resp.Items, &v1.AssistantConversation{
			Id: c.ID, Title: c.Title,
			CreateTime: timestamppb.New(c.CreateTime), UpdateTime: timestamppb.New(c.UpdateTime),
		})
	}
	return resp, nil
}

// ListMessages implements proto AssistantService/ListMessages：把模型协议消息
// 聚合为展示轮次（user 一条；连续的 assistant/tool 消息合并为一条 assistant，
// 文本拼接、工具步骤按发生顺序挂载审计记录）。
func (s *AssistantService) ListMessages(ctx context.Context, req *v1.ListMessagesRequest) (*v1.ListMessagesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	msgs, calls, err := s.uc.History(ctx, req.GetId(), userID)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*biz.ToolCall, len(calls))
	for _, c := range calls {
		byID[c.CallID] = c
	}
	resp := &v1.ListMessagesResponse{Items: make([]*v1.AssistantMessage, 0, len(msgs))}
	var turn *v1.AssistantMessage
	var texts []string
	var reasons []string
	flush := func() {
		if turn == nil {
			return
		}
		turn.Content = strings.Join(texts, "\n\n")
		turn.Reasoning = strings.Join(reasons, "\n\n")
		resp.Items = append(resp.Items, turn)
		turn, texts, reasons = nil, nil, nil
	}
	for _, m := range msgs {
		switch m.Role {
		case biz.RoleUser:
			flush()
			resp.Items = append(resp.Items, &v1.AssistantMessage{
				Id: m.ID, Role: biz.RoleUser, Content: m.Content, CreateTime: timestamppb.New(m.CreateTime),
			})
		case biz.RoleAssistant:
			if turn == nil {
				turn = &v1.AssistantMessage{Role: biz.RoleAssistant, CreateTime: timestamppb.New(m.CreateTime)}
			}
			turn.Id = m.ID
			if m.Content != "" {
				texts = append(texts, m.Content)
			}
			if m.Reasoning != "" {
				reasons = append(reasons, m.Reasoning)
			}
			for _, ref := range m.ToolCalls {
				if tc, ok := byID[ref.ID]; ok {
					turn.ToolCalls = append(turn.ToolCalls, toProtoToolCall(tc))
				}
			}
		default:
			// tool 消息的内容已体现在工具调用审计记录里，不单独展示。
		}
	}
	flush()
	return resp, nil
}

// DeleteConversation implements proto AssistantService/DeleteConversation.
func (s *AssistantService) DeleteConversation(ctx context.Context, req *v1.DeleteConversationRequest) (*v1.DeleteConversationResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId(), userID); err != nil {
		return nil, err
	}
	return &v1.DeleteConversationResponse{}, nil
}

func toProtoToolCall(tc *biz.ToolCall) *v1.AssistantToolCall {
	return &v1.AssistantToolCall{
		Id: tc.ID, ToolName: tc.ToolName, Title: tc.Title, Status: tc.Status,
		Input: string(tc.Input), Output: string(tc.Output), Error: tc.Error,
		DurationMs: tc.DurationMS, Write: tc.Write, CreateTime: timestamppb.New(tc.CreateTime),
	}
}

// AssistantChatService 实现生成的 AssistantChatServiceServer 并提供流式编排
// 入口：生成接口的 Chat/Confirm 不会被路由到（HTTP 绑定由 server 层原生 SSE
// 路由承担），仅为满足接口返回 Unimplemented。
type AssistantChatService struct {
	uc *biz.AgentUsecase
}

// NewAssistantChatService new an AssistantChatService.
func NewAssistantChatService(uc *biz.AgentUsecase) *AssistantChatService {
	return &AssistantChatService{uc: uc}
}

// Chat implements proto AssistantChatService/Chat（占位：流式入口见 StreamChat）。
func (s *AssistantChatService) Chat(context.Context, *v1.ChatRequest) (*v1.ChatResponse, error) {
	return nil, errors.New(501, "NOT_IMPLEMENTED", "use the streaming endpoint")
}

// Confirm implements proto AssistantChatService/Confirm（占位：流式入口见 StreamConfirm）。
func (s *AssistantChatService) Confirm(context.Context, *v1.ConfirmRequest) (*v1.ConfirmResponse, error) {
	return nil, errors.New(501, "NOT_IMPLEMENTED", "use the streaming endpoint")
}

// StreamChat 以当前用户身份发起一轮对话；事件经 emit 回调交给传输层写出。
func (s *AssistantChatService) StreamChat(ctx context.Context, req *v1.ChatRequest, emit func(biz.Event) error) error {
	in, err := s.runInput(ctx)
	if err != nil {
		return err
	}
	in.ConversationID = req.GetConversationId()
	in.Content = req.GetContent()
	return s.uc.Run(ctx, in, emit)
}

// StreamConfirm 对待确认操作做裁决并继续本轮对话。
func (s *AssistantChatService) StreamConfirm(ctx context.Context, req *v1.ConfirmRequest, emit func(biz.Event) error) error {
	in, err := s.runInput(ctx)
	if err != nil {
		return err
	}
	in.ConversationID = req.GetConversationId()
	in.Confirm = &biz.Confirmation{ToolCallID: req.GetToolCallId(), Approved: req.GetApproved()}
	return s.uc.Run(ctx, in, emit)
}

// runInput 取操作者身份与其访问令牌（回环调用以同一身份执行，权限由既有中间件判定）。
func (s *AssistantChatService) runInput(ctx context.Context) (biz.RunInput, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return biz.RunInput{}, err
	}
	in := biz.RunInput{UserID: userID}
	if tr, ok := transport.FromServerContext(ctx); ok {
		in.Bearer = tr.RequestHeader().Get("Authorization")
	}
	return in, nil
}
