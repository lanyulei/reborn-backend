package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	// llmResponseHeaderTimeout 请求发出后等待响应头的上限：上游接受了连接却迟迟不应答时尽早失败，
	// 而不是占着并发名额与会话租约等到单轮超时。
	llmResponseHeaderTimeout = 2 * time.Minute
	// llmStreamIdleTimeout 流式读取中相邻两个分片之间的最长间隔，超过即视为上游卡死并中止。
	llmStreamIdleTimeout = 2 * time.Minute
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
	// noUsage 端点不接受 stream_options（请求用量统计）时置位，此后不再携带。
	noUsage atomic.Bool
	// idleTimeout 流式读取的空闲上限（缺省 llmStreamIdleTimeout）。
	idleTimeout time.Duration
}

// errStreamIdle 流式读取空闲超时（内部控制流，经 context cause 传递）。
var errStreamIdle = errors.New("assistant: model stream idle timeout")

// NewChatModel new a ChatModelClient；api_key 为空时构造停用客户端（Stream 报错）。
func NewChatModel(c *conf.Assistant) *ChatModelClient {
	connect := defaultLLMConnectTimeout
	if d := c.GetLlmConnectTimeout(); d != nil && d.AsDuration() > 0 {
		connect = d.AsDuration()
	}
	httpClient := &http.Client{Transport: &http.Transport{
		// 遵循 HTTPS_PROXY / NO_PROXY：模型接口多在公网，出网要走代理的环境靠它才连得上。
		Proxy:        http.ProxyFromEnvironment,
		MaxIdleConns: 16, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
		DialContext:           (&net.Dialer{Timeout: connect}).DialContext,
		TLSHandshakeTimeout:   connect,
		ResponseHeaderTimeout: llmResponseHeaderTimeout,
	}}
	opts := []option.RequestOption{
		option.WithAPIKey(c.GetApiKey()),
		option.WithHTTPClient(httpClient),
		option.WithMaxRetries(1),
	}
	if base := strings.TrimSpace(c.GetBaseUrl()); base != "" {
		opts = append(opts, option.WithBaseURL(base))
	}
	return &ChatModelClient{client: openai.NewClient(opts...), model: c.GetModel(), enabled: c.GetApiKey() != "", idleTimeout: llmStreamIdleTimeout}
}

// Stream implements biz.ChatModel：流式读取，content/reasoning 增量经 onDelta
// 推送；工具调用由 SDK 累加器聚合后在结束时一次返回。
func (m *ChatModelClient) Stream(ctx context.Context, req biz.ChatRequest, onDelta func(biz.Delta) error) (*biz.Completion, error) {
	if !m.enabled {
		return nil, biz.ErrDisabled
	}
	withUsage := !m.noUsage.Load()
	out, started, err := m.stream(ctx, req, onDelta, withUsage)
	var rejected *rejectedError
	if err != nil && withUsage && !started && errors.As(err, &rejected) {
		// 请求在产生任何输出前被拒绝：可能是端点不认识 stream_options，去掉它再试一次；
		// 成功则记住，此后不再携带（用量统计只是可观测性，不能因此影响对话）。
		retried, _, rerr := m.stream(ctx, req, onDelta, false)
		if rerr != nil {
			return nil, rerr
		}
		m.noUsage.Store(true)
		return retried, nil
	}
	return out, err
}

// stream 发起一次流式请求；started 报告是否已收到过分片（已有输出推给调用方，不可重试）。
func (m *ChatModelClient) stream(ctx context.Context, req biz.ChatRequest, onDelta func(biz.Delta) error, withUsage bool) (out *biz.Completion, started bool, err error) {
	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(m.model),
		Messages: m.messages(req),
		Tools:    m.tools(req.Tools),
	}
	if withUsage {
		params.StreamOptions = openai.ChatCompletionStreamOptionsParam{IncludeUsage: param.NewOpt(true)}
	}
	// 空闲看门狗：相邻分片间隔超过上限即取消本次请求（调用方 ctx 只约束整轮时长）。
	sctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	idle := time.AfterFunc(m.idleTimeout, func() { cancel(errStreamIdle) })
	defer idle.Stop()

	stream := m.client.Chat.Completions.NewStreaming(sctx, params)
	defer func() { _ = stream.Close() }()
	var acc openai.ChatCompletionAccumulator
	var reasoning strings.Builder
	for stream.Next() {
		started = true
		idle.Reset(m.idleTimeout)
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
			return nil, started, err
		}
	}
	if err := stream.Err(); err != nil {
		return nil, started, classifyStreamError(ctx, sctx, err)
	}
	if len(acc.Choices) == 0 {
		return nil, started, fmt.Errorf("%w: empty completion", biz.ErrLLMUnavailable)
	}
	choice := acc.Choices[0]
	out = &biz.Completion{
		Content: choice.Message.Content, Reasoning: reasoning.String(), FinishReason: choice.FinishReason,
		PromptTokens: acc.Usage.PromptTokens, CompletionTokens: acc.Usage.CompletionTokens,
	}
	for _, tc := range choice.Message.ToolCalls {
		if tc.Type != "" && tc.Type != "function" {
			continue
		}
		out.ToolCalls = append(out.ToolCalls, biz.ToolCallRef{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	return out, started, nil
}

// classifyStreamError 把流式请求的失败归类：调用方取消原样返回；请求本身被拒绝（4xx，鉴权 /
// 限流 / 超时除外）为 ErrLLMRejected；其余（网络、5xx、限流、空闲超时）为 ErrLLMUnavailable。
// 上游错误细节只随 error 链进日志，不下发客户端。
func classifyStreamError(ctx, sctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(context.Cause(sctx), errStreamIdle) {
		return fmt.Errorf("%w: %v", biz.ErrLLMUnavailable, errStreamIdle)
	}
	var apiErr *openai.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 {
		switch apiErr.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestTimeout, http.StatusTooManyRequests:
		default:
			return &rejectedError{cause: err}
		}
	}
	return fmt.Errorf("%w: %v", biz.ErrLLMUnavailable, err)
}

// rejectedError 模型接口拒绝了请求本身。对外等同 biz.ErrLLMRejected（errors.As 取到的
// kratos 错误即它）；单独成类型是因为 kratos 错误按 code+reason 判等，与 ErrLLMUnavailable
// 无法用 errors.Is 区分。
type rejectedError struct{ cause error }

func (e *rejectedError) Error() string { return biz.ErrLLMRejected.Error() + ": " + e.cause.Error() }
func (e *rejectedError) Unwrap() error { return biz.ErrLLMRejected }

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
