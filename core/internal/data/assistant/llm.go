package data

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/shared"
)

var _ biz.ChatModel = (*ChatModelClient)(nil)

// 模型客户端缺省参数。
const (
	defaultLLMConnectTimeout = 3 * time.Second
	// reasoningField OpenAI 兼容端点（GLM/DeepSeek 等）流式增量里的思考字段。
	reasoningField = "reasoning_content"
)

// ChatModelClient 是 biz.ChatModel 的 OpenAI 兼容协议实现（openai-go）。
// 出站超时：连接建立 ≤ llm_connect_timeout；整体受调用方 ctx（单轮 timeout）
// 约束——流式生成本身耗时以分钟计，AGENTS §7 的 3s 口径在此仅适用于连接阶段。
type ChatModelClient struct {
	client  openai.Client
	model   string
	enabled bool
	schemas sync.Map // tool name -> shared.FunctionParameters（目录不可变，解析一次）
}

// NewChatModel new a ChatModelClient；api_key 为空时构造停用客户端（Stream 报错）。
func NewChatModel(c *conf.Assistant) *ChatModelClient {
	connect := defaultLLMConnectTimeout
	if d := c.GetLlmConnectTimeout(); d != nil && d.AsDuration() > 0 {
		connect = d.AsDuration()
	}
	httpClient := &http.Client{Transport: &http.Transport{
		MaxIdleConns: 16, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
		DialContext:         (&net.Dialer{Timeout: connect}).DialContext,
		TLSHandshakeTimeout: connect,
	}}
	opts := []option.RequestOption{
		option.WithAPIKey(c.GetApiKey()),
		option.WithHTTPClient(httpClient),
		option.WithMaxRetries(1),
	}
	if base := strings.TrimSpace(c.GetBaseUrl()); base != "" {
		opts = append(opts, option.WithBaseURL(base))
	}
	return &ChatModelClient{client: openai.NewClient(opts...), model: c.GetModel(), enabled: c.GetApiKey() != ""}
}

// Stream implements biz.ChatModel：流式读取，content/reasoning 增量经 onDelta
// 推送；工具调用由 SDK 累加器聚合后在结束时一次返回。
func (m *ChatModelClient) Stream(ctx context.Context, req biz.ChatRequest, onDelta func(biz.Delta) error) (*biz.Completion, error) {
	if !m.enabled {
		return nil, biz.ErrDisabled
	}
	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(m.model),
		Messages: m.messages(req),
		Tools:    m.tools(req.Tools),
	}
	stream := m.client.Chat.Completions.NewStreaming(ctx, params)
	defer func() { _ = stream.Close() }()
	var acc openai.ChatCompletionAccumulator
	var reasoning strings.Builder
	for stream.Next() {
		chunk := stream.Current()
		acc.AddChunk(chunk)
		if len(chunk.Choices) == 0 {
			continue
		}
		d := chunk.Choices[0].Delta
		delta := biz.Delta{Text: d.Content}
		if f, ok := d.JSON.ExtraFields[reasoningField]; ok {
			var s string
			if err := json.Unmarshal([]byte(f.Raw()), &s); err == nil && s != "" {
				delta.Reasoning = s
				reasoning.WriteString(s)
			}
		}
		if delta.Text == "" && delta.Reasoning == "" {
			continue
		}
		if err := onDelta(delta); err != nil {
			return nil, err
		}
	}
	if err := stream.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %v", biz.ErrLLMUnavailable, err)
	}
	if len(acc.Choices) == 0 {
		return nil, fmt.Errorf("%w: empty completion", biz.ErrLLMUnavailable)
	}
	choice := acc.Choices[0]
	out := &biz.Completion{Content: choice.Message.Content, Reasoning: reasoning.String(), FinishReason: choice.FinishReason}
	for _, tc := range choice.Message.ToolCalls {
		if tc.Type != "" && tc.Type != "function" {
			continue
		}
		out.ToolCalls = append(out.ToolCalls, biz.ToolCallRef{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	return out, nil
}

// messages 把领域消息转换为模型协议消息。
func (m *ChatModelClient) messages(req biz.ChatRequest) []openai.ChatCompletionMessageParamUnion {
	out := make([]openai.ChatCompletionMessageParamUnion, 0, len(req.Messages)+1)
	if req.System != "" {
		out = append(out, openai.SystemMessage(req.System))
	}
	for _, msg := range req.Messages {
		switch msg.Role {
		case biz.RoleUser:
			out = append(out, openai.UserMessage(msg.Content))
		case biz.RoleAssistant:
			a := &openai.ChatCompletionAssistantMessageParam{}
			if msg.Content != "" || len(msg.ToolCalls) == 0 {
				a.Content.OfString = param.NewOpt(msg.Content)
			}
			for _, ref := range msg.ToolCalls {
				a.ToolCalls = append(a.ToolCalls, openai.ChatCompletionMessageToolCallUnionParam{
					OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
						ID:       ref.ID,
						Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{Name: ref.Name, Arguments: ref.Arguments},
					},
				})
			}
			out = append(out, openai.ChatCompletionMessageParamUnion{OfAssistant: a})
		case biz.RoleTool:
			out = append(out, openai.ToolMessage(msg.Content, msg.ToolCallID))
		}
	}
	return out
}

// tools 把工具目录转换为模型函数定义；JSON Schema 解析结果按工具名缓存。
func (m *ChatModelClient) tools(specs []biz.ToolSpec) []openai.ChatCompletionToolUnionParam {
	if len(specs) == 0 {
		return nil
	}
	out := make([]openai.ChatCompletionToolUnionParam, 0, len(specs))
	for _, spec := range specs {
		params := m.schema(spec)
		out = append(out, openai.ChatCompletionFunctionTool(shared.FunctionDefinitionParam{
			Name:        spec.Name,
			Description: param.NewOpt(spec.Description),
			Parameters:  params,
		}))
	}
	return out
}

func (m *ChatModelClient) schema(spec biz.ToolSpec) shared.FunctionParameters {
	if v, ok := m.schemas.Load(spec.Name); ok {
		return v.(shared.FunctionParameters)
	}
	var parsed map[string]any
	if err := json.Unmarshal(spec.InputSchema, &parsed); err != nil || parsed == nil {
		parsed = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	params := shared.FunctionParameters(parsed)
	m.schemas.Store(spec.Name, params)
	return params
}
