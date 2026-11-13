package testfake

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	dataassistant "github.com/lanyulei/reborn/backend/core/internal/data/assistant"
)

// 编译期断言：视图类型实现 assistant 域仓储接口。
var (
	_ biz.ConversationRepo = conversationView{}
	_ biz.MessageRepo      = messageView{}
	_ biz.ToolCallRepo     = toolCallView{}
	_ biz.ChatModel        = (*ChatModel)(nil)
)

// AssistantStore 是 assistant 域三个仓储接口的内存实现（独立于 Store：三个
// 接口的 Create/List 方法签名互斥，经 Conversations/Messages/ToolCalls 视图暴露）。
// 语义对齐 data 层：租约条件更新、状态条件迁移、按用户隔离、软删。
type AssistantStore struct {
	mu            sync.RWMutex
	conversations map[string]*conversationEntry
	messages      map[string][]*biz.Message
	toolCalls     map[string]*biz.ToolCall
}

type conversationEntry struct {
	conv       biz.Conversation
	runToken   string
	leaseUntil time.Time
	deleted    bool
}

// NewAssistantStore new an AssistantStore。
func NewAssistantStore() *AssistantStore {
	return &AssistantStore{
		conversations: make(map[string]*conversationEntry),
		messages:      make(map[string][]*biz.Message),
		toolCalls:     make(map[string]*biz.ToolCall),
	}
}

// Conversations 返回 biz.ConversationRepo 视图。
func (s *AssistantStore) Conversations() biz.ConversationRepo { return conversationView{s} }

// Messages 返回 biz.MessageRepo 视图。
func (s *AssistantStore) Messages() biz.MessageRepo { return messageView{s} }

// ToolCalls 返回 biz.ToolCallRepo 视图。
func (s *AssistantStore) ToolCalls() biz.ToolCallRepo { return toolCallView{s} }

// ToolCall 测试辅助：按 ID 读取审计记录副本；不存在返回 nil。
func (s *AssistantStore) ToolCall(id string) *biz.ToolCall {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tc, ok := s.toolCalls[id]
	if !ok {
		return nil
	}
	cp := *tc
	return &cp
}

// SeedToolCall 测试辅助：预置一条审计记录（如伪造 pending 以验证回环授权）。
func (s *AssistantStore) SeedToolCall(tc *biz.ToolCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *tc
	s.toolCalls[tc.ID] = &cp
}

type conversationView struct{ s *AssistantStore }

func (v conversationView) Create(_ context.Context, c *biz.Conversation) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	v.s.conversations[c.ID] = &conversationEntry{conv: *c}
	return nil
}

func (v conversationView) Get(_ context.Context, id, userID string) (*biz.Conversation, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	e, ok := v.s.conversations[id]
	if !ok || e.deleted || e.conv.UserID != userID {
		return nil, biz.ErrConversationNotFound
	}
	cp := e.conv
	return &cp, nil
}

func (v conversationView) List(_ context.Context, userID string, page, pageSize int64) ([]*biz.Conversation, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.Conversation, 0, len(v.s.conversations))
	for _, e := range v.s.conversations {
		if e.deleted || e.conv.UserID != userID {
			continue
		}
		cp := e.conv
		items = append(items, &cp)
	}
	v.s.mu.RUnlock()
	slices.SortFunc(items, func(a, b *biz.Conversation) int {
		if a.UpdateTime != b.UpdateTime {
			return b.UpdateTime.Compare(a.UpdateTime)
		}
		return strings.Compare(b.ID, a.ID)
	})
	total := int64(len(items))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	return items[start:min(start+pageSize, total)], total, nil
}

func (v conversationView) Touch(_ context.Context, id string, at time.Time) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if e, ok := v.s.conversations[id]; ok {
		e.conv.UpdateTime = at
	}
	return nil
}

func (v conversationView) SoftDelete(_ context.Context, id, userID string, _ time.Time) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.conversations[id]
	if !ok || e.deleted || e.conv.UserID != userID {
		return biz.ErrConversationNotFound
	}
	e.deleted = true
	return nil
}

func (v conversationView) AcquireLease(_ context.Context, id, userID, token string, until time.Time) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.conversations[id]
	if !ok || e.deleted || e.conv.UserID != userID {
		return false, nil
	}
	if e.runToken != "" && e.leaseUntil.After(time.Now()) {
		return false, nil
	}
	e.runToken, e.leaseUntil = token, until
	return true, nil
}

func (v conversationView) ReleaseLease(_ context.Context, id, token string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if e, ok := v.s.conversations[id]; ok && e.runToken == token {
		e.runToken, e.leaseUntil = "", time.Time{}
	}
	return nil
}

type messageView struct{ s *AssistantStore }

func (v messageView) Append(_ context.Context, m *biz.Message) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var maxSeq int32
	for _, existing := range v.s.messages[m.ConversationID] {
		maxSeq = max(maxSeq, existing.Seq)
	}
	m.Seq = maxSeq + 1
	cp := *m
	cp.ToolCalls = slices.Clone(m.ToolCalls)
	v.s.messages[m.ConversationID] = append(v.s.messages[m.ConversationID], &cp)
	return nil
}

func (v messageView) List(ctx context.Context, conversationID string) ([]*biz.Message, error) {
	return v.ListRecent(ctx, conversationID, 0)
}

// ListRecent 最近 limit 条（seq 正序）；limit <= 0 表示全量。
func (v messageView) ListRecent(_ context.Context, conversationID string, limit int) ([]*biz.Message, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	all := v.s.messages[conversationID]
	if limit > 0 && len(all) > limit {
		all = all[len(all)-limit:]
	}
	items := make([]*biz.Message, 0, len(all))
	for _, m := range all {
		cp := *m
		cp.ToolCalls = slices.Clone(m.ToolCalls)
		items = append(items, &cp)
	}
	return items, nil
}

type toolCallView struct{ s *AssistantStore }

func (v toolCallView) Create(_ context.Context, tc *biz.ToolCall) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cp := *tc
	v.s.toolCalls[tc.ID] = &cp
	return nil
}

func (v toolCallView) ListByConversation(_ context.Context, conversationID string) ([]*biz.ToolCall, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	items := make([]*biz.ToolCall, 0, 8)
	for _, tc := range v.s.toolCalls {
		if tc.ConversationID == conversationID {
			cp := *tc
			items = append(items, &cp)
		}
	}
	slices.SortFunc(items, func(a, b *biz.ToolCall) int {
		if a.CreateTime != b.CreateTime {
			return a.CreateTime.Compare(b.CreateTime)
		}
		return strings.Compare(a.ID, b.ID)
	})
	return items, nil
}

func (v toolCallView) Transition(_ context.Context, id, conversationID, from, to string, at time.Time) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	tc, ok := v.s.toolCalls[id]
	if !ok || tc.ConversationID != conversationID || tc.Status != from {
		return false, nil
	}
	tc.Status = to
	tc.ResolveTime = &at
	return true, nil
}

func (v toolCallView) Finish(_ context.Context, in *biz.ToolCall) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	tc, ok := v.s.toolCalls[in.ID]
	if !ok {
		return errors.New("testfake: tool call not found")
	}
	tc.Status, tc.Output, tc.Error, tc.DurationMS, tc.ResolveTime = in.Status, in.Output, in.Error, in.DurationMS, in.ResolveTime
	return nil
}

// ChatModel 是 biz.ChatModel 的脚本桩：按调用次序返回预置的 Completion，
// 脚本耗尽后回复固定文本；记录每次请求供断言（如提供给模型的工具集）。
type ChatModel struct {
	mu       sync.Mutex
	steps    []func(req biz.ChatRequest) (*biz.Completion, error)
	requests []biz.ChatRequest
}

// NewChatModel new a ChatModel。
func NewChatModel() *ChatModel { return &ChatModel{} }

// Script 追加一步脚本。
func (m *ChatModel) Script(step func(req biz.ChatRequest) (*biz.Completion, error)) *ChatModel {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps = append(m.steps, step)
	return m
}

// Reply 追加一步纯文本回复。
func (m *ChatModel) Reply(text string) *ChatModel {
	return m.Script(func(biz.ChatRequest) (*biz.Completion, error) {
		return &biz.Completion{Content: text, FinishReason: "stop"}, nil
	})
}

// Call 追加一步工具调用。
func (m *ChatModel) Call(id, tool string, args any) *ChatModel {
	raw, _ := json.Marshal(args)
	return m.Script(func(biz.ChatRequest) (*biz.Completion, error) {
		return &biz.Completion{
			ToolCalls:    []biz.ToolCallRef{{ID: id, Name: tool, Arguments: string(raw)}},
			FinishReason: "tool_calls",
		}, nil
	})
}

// Requests 返回已记录的请求副本。
func (m *ChatModel) Requests() []biz.ChatRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.requests)
}

// Stream implements biz.ChatModel。
func (m *ChatModel) Stream(ctx context.Context, req biz.ChatRequest, onDelta func(biz.Delta) error) (*biz.Completion, error) {
	m.mu.Lock()
	m.requests = append(m.requests, req)
	var step func(biz.ChatRequest) (*biz.Completion, error)
	if len(m.steps) > 0 {
		step = m.steps[0]
		m.steps = m.steps[1:]
	}
	m.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if step == nil {
		step = func(biz.ChatRequest) (*biz.Completion, error) {
			return &biz.Completion{Content: "好的。", FinishReason: "stop"}, nil
		}
	}
	c, err := step(req)
	if err != nil {
		return nil, err
	}
	if c.Reasoning != "" {
		if err := onDelta(biz.Delta{Reasoning: c.Reasoning}); err != nil {
			return nil, err
		}
	}
	if c.Content != "" {
		if err := onDelta(biz.Delta{Text: c.Content}); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Loopback 是回环执行器的测试脚手架：复用 data 层真实 HTTP 回环实现，目标
// 地址在 httptest 服务器启动后经 SetURL 注入，使工具调用真实经过
// auth/authz 中间件链（权限隔离的端到端验证）。
type Loopback struct {
	url atomic.Value
	*dataassistant.LoopbackInvoker
}

// NewLoopback new a Loopback。
func NewLoopback() *Loopback {
	l := &Loopback{}
	l.url.Store("http://127.0.0.1:0")
	l.LoopbackInvoker = dataassistant.NewLoopbackInvokerWithResolver(func() string {
		return l.url.Load().(string)
	}, 5*time.Second)
	return l
}

// SetURL 注入回环目标地址。
func (l *Loopback) SetURL(u string) { l.url.Store(u) }
