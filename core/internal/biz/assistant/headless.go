package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 无人值守运行（运维 Agent 等后台调用方）复用同一个 run：消息持久化、工具调用审计、脱敏截断、
// 上下文窗口裁剪、中断收尾与模型拒绝判定全部原样生效；差别只有四处——工具列表按白名单二次过滤、
// 写工具遇到「需要确认」时调用裁决回调而不是挂起等人、历史按加载边界截取、事件不走 SSE。
// 它不占人类轮次的并发信号量（任务一跑数分钟，占槽会让人类对话排队），并发由调用方自己限制。

// 无人值守一个阶段的结束原因。
const (
	// HeadlessCompleted 模型给出最终文本（不再调用工具）。
	HeadlessCompleted = "completed"
	// HeadlessMaxSteps 达到本阶段步数上限，以文本收尾。
	HeadlessMaxSteps = "max_steps"
	// HeadlessTimeout 达到时限。
	HeadlessTimeout = "timeout"
	// HeadlessLLMRejected 模型接口拒绝了请求（4xx），重试无意义。
	HeadlessLLMRejected = "llm_rejected"
	// HeadlessSessionExpired 访问令牌在运行中失效（401）。
	HeadlessSessionExpired = "session_expired"
	// HeadlessLLMUnavailable 模型不可用（5xx / 网络），可退避重试。
	HeadlessLLMUnavailable = "llm_unavailable"
	// capturedToolMessage 捕获模式回灌模型的说明。
	capturedToolMessage = "干跑：已记录拟调用参数，未实际执行"
	// approxBytesPerToken 模型接口不回报用量时的估算系数（字节 / token）。
	approxBytesPerToken = 4
)

// HeadlessDecision 对一次写工具调用的裁决：Approve 执行；Capture 不执行只记录（干跑）；
// 二者皆否 = 拒绝，Reason 作为失败原因回灌模型。
type HeadlessDecision struct {
	Approve bool
	Capture bool
	Reason  string
	// Rewrite 非空时以它替换模型给出的参数再执行 / 捕获（调用方注入 run_id、剔除不允许的字段等）；
	// 历史里该调用的 arguments 同步改写并落库，审计入参为改写后的脱敏副本。
	Rewrite json.RawMessage
}

// HeadlessWrite 成功执行的一次写调用（调用方据此回收产出：发现的事件 ID、建议 ID）。
type HeadlessWrite struct {
	Tool   string
	Input  json.RawMessage
	Output json.RawMessage
}

// HeadlessSpec 无人值守运行一个阶段的输入。
type HeadlessSpec struct {
	// ConversationID 会话（kind=agent，归 UserID 所有）。
	ConversationID string
	// UserID 机器账号；Bearer 其短期访问令牌。
	UserID string
	Bearer string
	// System 本阶段系统提示（替代人类对话的缺省提示）。
	System string
	// Tools 本阶段工具白名单（工具名 <Service>_<Method>）；为空 = 不下发工具（纯推理）。
	// 白名单之外的工具即使账号有权限也不下发；模型调用了未下发的工具按「未知工具」失败回灌。
	Tools []string
	// Input 本阶段的用户侧输入（上下文 / 上阶段结论）；为空则直接在现有历史上继续。
	Input string
	// HistoryAfter 只把该消息 ID 之后的历史发给模型（阶段折叠）；为空 = 不加载历史。
	// 会话里的消息不删除，审计与回放仍完整。
	HistoryAfter string
	// Decide 每次工具调用（读与写）执行前的裁决回调；入参为待建档的审计记录与原始（未脱敏）参数。
	// 读工具通常 Approve（可改写参数，如限定查询时间窗）；写工具按白名单与参数校验决定执行 / 捕获 / 拒绝。
	Decide func(ctx context.Context, tc *ToolCall, input json.RawMessage) HeadlessDecision
	// Observe 每次工具调用成功后的回调（读与写），入参为脱敏截断后的输出；调用方据此收集上下文里出现过的对象 ID。可为 nil。
	Observe func(tc *ToolCall, output json.RawMessage)
	// Transform 工具成功返回后、落库与回灌模型之前对输出的改写（日志按行截尾、给 Agent 先前产出打标等）；
	// 返回 nil 表示不改。改写后的内容同样会经脱敏与截断。可为 nil。
	Transform func(tc *ToolCall, output json.RawMessage) json.RawMessage
	// MaxSteps 本阶段最多模型调用次数；≤0 取配置缺省。
	MaxSteps int
	// Timeout 本阶段时限（也是会话租约 TTL）；≤0 取配置缺省。
	Timeout time.Duration
}

// HeadlessDenied 运行中被拒绝（403）的一次工具调用。
type HeadlessDenied struct {
	Tool    string
	Reason  string
	Message string
}

// HeadlessCaptured 干跑时捕获的一次写调用。
type HeadlessCaptured struct {
	Tool  string
	Input json.RawMessage
}

// HeadlessResult 一个阶段的结果。
type HeadlessResult struct {
	// Content 模型的最终文本（EndReason = completed 时有）。
	Content string
	// LastMessageID 本阶段最后一条消息 ID（下一阶段的 HistoryAfter）。
	LastMessageID string
	// Steps 模型调用次数。
	Steps int
	// PromptTokens / CompletionTokens 用量；接口不回报时按字节估算并置 Estimated。
	PromptTokens     int64
	CompletionTokens int64
	Estimated        bool
	// EndReason 结束原因（Headless* 常量）。
	EndReason string
	Denied    []HeadlessDenied
	Captured  []HeadlessCaptured
	Writes    []HeadlessWrite
}

// headlessState 挂在 run 上的无人值守状态。
type headlessState struct {
	spec   HeadlessSpec
	result *HeadlessResult
}

// RunHeadless 无人值守地跑一个阶段：抢会话租约 → 按边界加载历史 → 模型 ⇄ 工具 → 落库，返回阶段结果。
// 软结束（模型拒绝 / 令牌失效 / 超时 / 步数上限）以 EndReason 表达、error 为 nil；仓储等硬错误返回 error。
// 不检查单用户与单实例的并发名额：调用方（Agent 运行器）自己限流。
func (uc *AgentUsecase) RunHeadless(ctx context.Context, spec HeadlessSpec) (*HeadlessResult, error) {
	if !uc.cfg.Enabled {
		return nil, ErrDisabled
	}
	if spec.ConversationID == "" || spec.UserID == "" || spec.Decide == nil {
		return nil, fmt.Errorf("assistant: headless spec requires conversation, user and decide callback")
	}
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = uc.cfg.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conv, err := uc.convs.Get(ctx, spec.ConversationID, spec.UserID)
	if err != nil {
		return nil, err
	}
	token, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	ok, err := uc.convs.AcquireLease(ctx, conv.ID, spec.UserID, token, timeout+leaseGrace)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrRunBusy
	}
	defer func() {
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer ccancel()
		if rerr := uc.convs.ReleaseLease(cctx, conv.ID, token); rerr != nil {
			uc.logger.ErrorContext(cctx, "assistant: release headless lease", "error", rerr)
		}
		if terr := uc.convs.Touch(cctx, conv.ID, time.Now()); terr != nil {
			uc.logger.ErrorContext(cctx, "assistant: touch headless conversation", "error", terr)
		}
	}()

	maxSteps := spec.MaxSteps
	if maxSteps <= 0 {
		maxSteps = uc.cfg.MaxSteps
	}
	r := &run{
		uc:   uc,
		in:   RunInput{UserID: spec.UserID, Bearer: spec.Bearer, ConversationID: conv.ID, Content: spec.Input},
		conv: conv,
		emit: func(Event) error { return nil },
		// maxSteps 多留一步给「步数耗尽后的文本收尾」之外的最后一次模型调用。
		maxSteps: maxSteps,
		headless: &headlessState{spec: spec, result: &HeadlessResult{}},
	}
	if err := r.loadHeadless(ctx); err != nil {
		return nil, err
	}
	defer r.settleDangling(ctx)
	r.settleDangling(ctx)
	// 无人值守没有「等用户确认」：历史里遗留的待确认调用（不该出现）一律作废。
	for _, tc := range r.pending {
		if err := r.settle(ctx, tc, ToolStatusCancelled, nil, cancelledToolMessage); err != nil {
			return nil, err
		}
	}
	r.pending = nil
	if spec.Input != "" {
		if err := r.append(ctx, &Message{Role: RoleUser, Content: spec.Input}); err != nil {
			return nil, err
		}
	}
	err = r.loop(ctx)
	res := r.headless.result
	if n := len(r.messages); n > 0 {
		res.LastMessageID = r.messages[n-1].ID
	}
	switch {
	case err == nil:
		if res.EndReason == "" {
			res.EndReason = HeadlessCompleted
		}
	case errors.Is(err, ErrLLMRejected):
		res.EndReason = HeadlessLLMRejected
	case errors.Is(err, ErrSessionExpired):
		res.EndReason = HeadlessSessionExpired
	case errors.Is(err, ErrLLMUnavailable):
		res.EndReason = HeadlessLLMUnavailable
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		// 调用方取消（停机）：原样返回，由调用方决定续跑；不是超时。
		return res, context.Canceled
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil:
		res.EndReason = HeadlessTimeout
	default:
		return res, err
	}
	return res, nil
}

// wrapUpPrompt 步数耗尽后的收尾输入：不再提供工具，要求直接按 Schema 输出。
const wrapUpPrompt = "本阶段的工具调用次数已用完。请不要再调用工具，直接基于已掌握的信息按本阶段的输出 Schema 给出 JSON 结论；信息不足的字段如实留空或标注「无法判断」。"

// headlessWrapUp 步数耗尽后的收尾：追加一条说明，再做一次不带工具的模型调用，把结果作为本阶段结论。
// 这次调用仍计入用量；模型若仍试图调用工具（没有工具定义时极少发生），按文本收尾。
func (r *run) headlessWrapUp(ctx context.Context) error {
	if err := r.append(ctx, &Message{Role: RoleUser, Content: wrapUpPrompt}); err != nil {
		return r.fail(ctx, err)
	}
	window := contextWindowWithin(r.messages, historyBudget(r.system, nil))
	completion, err := r.uc.model.Stream(ctx, ChatRequest{System: r.system, Messages: window}, r.onDelta)
	if err != nil {
		return r.fail(ctx, err)
	}
	r.recordUsage(completion, window, nil)
	final := &Message{Role: RoleAssistant, Content: completion.Content, Reasoning: completion.Reasoning}
	if err := r.append(ctx, final); err != nil {
		return r.fail(ctx, err)
	}
	r.setHeadlessContent(completion.Content, HeadlessMaxSteps)
	return nil
}

// loadHeadless 加载历史（按 HistoryAfter 截取）、审计记录与按白名单过滤的工具；系统提示用调用方给的。
// 不启用按模块懒加载：白名单工具很少，一次性下发。
func (r *run) loadHeadless(ctx context.Context) error {
	msgs, err := r.uc.msgs.ListRecent(ctx, r.conv.ID, historyLoadLimit)
	if err != nil {
		return err
	}
	spec := r.headless.spec
	if spec.HistoryAfter == "" {
		// 不加载历史：但末尾悬空的工具调用仍要收尾，所以只保留最后一个 assistant 调用块（若未收尾）。
		msgs = trailingDangling(msgs)
	} else if i := slices.IndexFunc(msgs, func(m *Message) bool { return m.ID == spec.HistoryAfter }); i >= 0 {
		msgs = msgs[i+1:]
	}
	r.messages = msgs
	lastAssistant := ""
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == RoleAssistant {
			lastAssistant = msgs[i].ID
			break
		}
	}
	calls, err := r.uc.calls.ListActive(ctx, r.conv.ID, lastAssistant)
	if err != nil {
		return err
	}
	r.calls = make(map[string]*ToolCall, len(calls))
	for _, c := range calls {
		r.calls[c.ID] = c
		if c.Status == ToolStatusPending {
			r.pending = append(r.pending, c)
		}
	}
	_, _, isAdmin, err := r.uc.actors.DescribeActor(ctx, r.in.UserID)
	if err != nil {
		return err
	}
	allowed, err := r.uc.perms.EffectiveOperations(ctx, r.in.UserID)
	if err != nil {
		return err
	}
	granted := filterTools(r.uc.catalog, isAdmin, allowed)
	r.tools = make([]ToolSpec, 0, len(spec.Tools))
	for _, name := range spec.Tools {
		if i := slices.IndexFunc(granted, func(t ToolSpec) bool { return t.Name == name }); i >= 0 {
			r.tools = append(r.tools, granted[i])
		}
	}
	r.system = spec.System
	return nil
}

// trailingDangling 只保留历史末尾尚未收尾的 assistant 调用块（settleDangling 据此补写 tool 消息）；
// 末尾已完整或没有调用块时返回空。
func trailingDangling(msgs []*Message) []*Message {
	idx := -1
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role != RoleTool {
			idx = i
			break
		}
	}
	if idx < 0 || msgs[idx].Role != RoleAssistant || len(msgs[idx].ToolCalls) == 0 {
		return nil
	}
	answered := make(map[string]struct{}, len(msgs)-idx)
	for _, m := range msgs[idx+1:] {
		answered[m.ToolCallID] = struct{}{}
	}
	for _, ref := range msgs[idx].ToolCalls {
		if _, ok := answered[ref.ID]; !ok {
			return msgs[idx:]
		}
	}
	return nil
}

// dispatchHeadless 无人值守下的工具调用：建档前交裁决回调——执行 / 捕获（仅写工具）/ 拒绝；参数可被改写。
func (r *run) dispatchHeadless(ctx context.Context, tc *ToolCall, raw json.RawMessage) error {
	d := r.headless.spec.Decide(ctx, tc, raw)
	if !tc.Write && d.Capture {
		d.Capture, d.Approve = false, true
	}
	if len(d.Rewrite) > 0 && json.Valid(d.Rewrite) && (d.Approve || d.Capture) {
		r.rewriteArguments(ctx, tc.CallID, d.Rewrite)
		tc.Input = MaskJSON(d.Rewrite)
	}
	switch {
	case d.Capture:
		tc.Status = ToolStatusCaptured
	case d.Approve:
		tc.Status = ToolStatusApproved
	default:
		tc.Status = ToolStatusRejected
	}
	if err := r.uc.calls.Create(ctx, tc); err != nil {
		return err
	}
	r.calls[tc.ID] = tc
	switch {
	case d.Capture:
		r.headless.result.Captured = append(r.headless.result.Captured, HeadlessCaptured{Tool: tc.ToolName, Input: tc.Input})
		out, _ := json.Marshal(map[string]any{"captured": true, "message": capturedToolMessage})
		return r.settle(ctx, tc, ToolStatusCaptured, out, "")
	case d.Approve:
		return r.execute(ctx, tc)
	default:
		reason := d.Reason
		if reason == "" {
			reason = "该调用不在允许范围内，未执行"
		}
		return r.settle(ctx, tc, ToolStatusRejected, nil, reason)
	}
}

// transform 成功的工具输出交给调用方改写（人类对话原样返回）。
func (r *run) transform(tc *ToolCall, output json.RawMessage) json.RawMessage {
	if r.headless == nil || r.headless.spec.Transform == nil {
		return output
	}
	if out := r.headless.spec.Transform(tc, output); len(out) > 0 && json.Valid(out) {
		return out
	}
	return output
}

// observe 成功的工具调用交给调用方的观察回调（人类对话无操作）。
func (r *run) observe(tc *ToolCall, output json.RawMessage) {
	if r.headless == nil || r.headless.spec.Observe == nil {
		return
	}
	// 观察回调拿脱敏但未截断的输出：调用方要从里面收集对象 ID，截断后的预览会漏掉大半。
	r.headless.spec.Observe(tc, MaskJSON(output))
}

// rewriteArguments 把历史里该调用的参数换成调用方改写后的版本并落库（执行用 rawArguments 从历史取参数，所以必须先改历史）。
func (r *run) rewriteArguments(ctx context.Context, callID string, args json.RawMessage) {
	for i := len(r.messages) - 1; i >= 0; i-- {
		m := r.messages[i]
		if m.Role != RoleAssistant {
			continue
		}
		for j := range m.ToolCalls {
			if m.ToolCalls[j].ID != callID {
				continue
			}
			m.ToolCalls[j].Arguments = string(args)
			if err := r.uc.msgs.UpdateToolCalls(ctx, m); err != nil {
				r.uc.logger.ErrorContext(ctx, "assistant: rewrite tool call arguments", "message", m.ID, "error", err)
			}
			return
		}
	}
}

// recordWrite 记录一次成功的写调用（人类对话无操作）。
func (r *run) recordWrite(tc *ToolCall, output json.RawMessage) {
	if r.headless == nil || !tc.Write {
		return
	}
	r.headless.result.Writes = append(r.headless.result.Writes, HeadlessWrite{Tool: tc.ToolName, Input: tc.Input, Output: output})
}

// recordUsage 累计一步的用量：接口回报为 0 时按请求 / 回复字节估算（预算仍按估算值扣减）。
func (r *run) recordUsage(c *Completion, window []*Message, tools []ToolSpec) {
	if r.headless == nil {
		return
	}
	res := r.headless.result
	res.Steps++
	prompt, completion := c.PromptTokens, c.CompletionTokens
	if prompt == 0 {
		size := len(r.system)
		for _, m := range window {
			size += len(m.Content) + len(m.Reasoning)
			for _, ref := range m.ToolCalls {
				size += len(ref.Name) + len(ref.Arguments)
			}
		}
		for i := range tools {
			size += len(tools[i].Name) + len(tools[i].Description) + len(tools[i].InputSchema)
		}
		prompt = int64(size / approxBytesPerToken)
		res.Estimated = true
	}
	if completion == 0 {
		size := len(c.Content) + len(c.Reasoning)
		for _, ref := range c.ToolCalls {
			size += len(ref.Name) + len(ref.Arguments)
		}
		completion = int64(size / approxBytesPerToken)
		res.Estimated = true
	}
	res.PromptTokens += prompt
	res.CompletionTokens += completion
}

// setHeadlessContent 记录模型最终文本与结束原因（人类对话无操作）。
func (r *run) setHeadlessContent(content, reason string) {
	if r.headless == nil {
		return
	}
	r.headless.result.Content = content
	r.headless.result.EndReason = reason
}

// recordDenied 记录一次 403（权限 / 范围检查用；人类对话无操作）。
func (r *run) recordDenied(tool, reason, message string) {
	if r.headless == nil {
		return
	}
	r.headless.result.Denied = append(r.headless.result.Denied, HeadlessDenied{Tool: tool, Reason: reason, Message: message})
}
