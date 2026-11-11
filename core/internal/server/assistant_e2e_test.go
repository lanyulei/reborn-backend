package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// sseEvent 解析后的 SSE 事件。
type sseEvent struct {
	Type string
	Data map[string]any
}

// parseSSE 解析 text/event-stream 响应体（忽略注释行心跳）。
func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var events []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				ev.Type = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev.Data); err != nil {
					t.Fatalf("sse data not json: %q: %v", line, err)
				}
			}
		}
		if ev.Type != "" {
			events = append(events, ev)
		}
	}
	return events
}

func eventTypes(events []sseEvent) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type)
	}
	return out
}

func findEvent(events []sseEvent, typ string) *sseEvent {
	for i := range events {
		if events[i].Type == typ {
			return &events[i]
		}
	}
	return nil
}

func lastEvent(events []sseEvent, typ string) *sseEvent {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == typ {
			return &events[i]
		}
	}
	return nil
}

// chatSSE 发起对话并解析事件流；非 200 时返回原始响应体供断言。
func chatSSE(t *testing.T, ts *httptest.Server, bearer, body string) (int, []sseEvent, string) {
	t.Helper()
	code, raw := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/assistant/chat", bearer, body)
	if code != http.StatusOK {
		return code, nil, raw
	}
	return code, parseSSE(t, raw), raw
}

func confirmSSE(t *testing.T, ts *httptest.Server, bearer, body string) (int, []sseEvent, string) {
	t.Helper()
	code, raw := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/assistant/confirm", bearer, body)
	if code != http.StatusOK {
		return code, nil, raw
	}
	return code, parseSSE(t, raw), raw
}

// grantAssistantRole 创建角色并授予助手全部接口 + extraOps，绑定到用户。
func grantAssistantRole(t *testing.T, ts *httptest.Server, adminBearer, userID, roleCode string, extraOps ...string) {
	t.Helper()
	role := createRole(t, ts, adminBearer, "助手-"+roleCode, roleCode)
	ops := append([]string{
		"/assistant.v1.AssistantChatService/Chat",
		"/assistant.v1.AssistantChatService/Confirm",
		"/assistant.v1.AssistantService/ListConversations",
		"/assistant.v1.AssistantService/ListMessages",
		"/assistant.v1.AssistantService/DeleteConversation",
	}, extraOps...)
	codes := make([]string, 0, len(ops))
	for _, op := range ops {
		codes = append(codes, `"`+biz.OperationCode(op)+`"`)
	}
	code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/role/"+role.Id+"/permission", adminBearer,
		`{"menus":["assistant"],"apis":[`+strings.Join(codes, ",")+`]}`)
	if code != http.StatusOK {
		t.Fatalf("set role permissions: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+userID+"/role", adminBearer,
		`{"role_ids":["`+role.Id+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("bind role: status = %d body = %s", code, body)
	}
}

func toolNames(req bizassistant.ChatRequest) map[string]bool {
	out := make(map[string]bool, len(req.Tools))
	for _, tool := range req.Tools {
		out[tool.Name] = true
	}
	return out
}

// 超管读流程：模型调用 ListUsers（真实回环经中间件链）→ 结果回灌 → 文本收尾；
// 历史落库并可按轮次聚合查询。
func TestAssistant_ReadToolRoundTrip(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	svc.chatModel.Call("call_1", "UserService_ListUsers", map[string]any{"page": 1, "pageSize": 5}).Reply("当前共有 1 个用户。")

	code, events, raw := chatSSE(t, ts, bearer, `{"content":"列出用户"}`)
	if code != http.StatusOK {
		t.Fatalf("chat: status = %d body = %s", code, raw)
	}
	want := []string{"conversation", "tool_call", "tool_result", "text_delta", "done"}
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v\n%s", got, want, raw)
	}
	result := findEvent(events, "tool_result")
	if result.Data["status"] != bizassistant.ToolStatusSucceeded || !strings.Contains(raw, `"items"`) {
		t.Fatalf("tool_result = %v", result.Data)
	}
	if lastEvent(events, "done").Data["status"] != bizassistant.DoneCompleted {
		t.Fatalf("done = %v", lastEvent(events, "done").Data)
	}
	if reqs := svc.chatModel.Requests(); len(reqs) != 2 || !toolNames(reqs[0])["UserService_DeleteUser"] || !strings.Contains(reqs[0].System, "超级管理员") {
		t.Fatalf("admin must see all tools and be described in prompt: %d requests", len(reqs))
	}
	convID := findEvent(events, "conversation").Data["id"].(string)

	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/assistant/conversation?page=1&pageSize=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, `"title":"列出用户"`) {
		t.Fatalf("list conversations: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/assistant/conversation/"+convID+"/message", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list messages: status = %d body = %s", code, body)
	}
	var msgs struct {
		Items []struct {
			Role      string `json:"role"`
			Content   string `json:"content"`
			ToolCalls []struct {
				ToolName string `json:"toolName"`
				Status   string `json:"status"`
			} `json:"toolCalls"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &msgs); err != nil || len(msgs.Items) != 2 {
		t.Fatalf("messages body = %s (%v)", body, err)
	}
	if msgs.Items[0].Role != "user" || msgs.Items[1].Role != "assistant" || msgs.Items[1].Content != "当前共有 1 个用户。" ||
		len(msgs.Items[1].ToolCalls) != 1 || msgs.Items[1].ToolCalls[0].ToolName != "UserService_ListUsers" ||
		msgs.Items[1].ToolCalls[0].Status != bizassistant.ToolStatusSucceeded {
		t.Fatalf("aggregated messages wrong: %+v", msgs.Items)
	}
}

// 权限隔离：事前——未授权接口不出现在模型工具集，模型硬调按未知工具失败；
// 事中——伪造 pending 审计记录后确认，真实回环被 authz 中间件 403 拒绝。
func TestAssistant_PermissionIsolation(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	alice := createUser(t, ts, admin, "alice_ai")
	grantAssistantRole(t, ts, admin, alice.Id, "assistant_ro", "/system.v1.UserService/ListUsers")
	aliceBearer := testBearer(t, svc.signer, alice.Id)

	svc.chatModel.Call("call_x", "UserService_DeleteUser", map[string]any{"id": e2eActorID}).Reply("无法执行")
	code, events, raw := chatSSE(t, ts, aliceBearer, `{"content":"删除管理员"}`)
	if code != http.StatusOK {
		t.Fatalf("chat: status = %d body = %s", code, raw)
	}
	tools := toolNames(svc.chatModel.Requests()[0])
	if !tools["UserService_ListUsers"] || tools["UserService_DeleteUser"] || tools["RoleService_CreateRole"] {
		t.Fatalf("offered tools must follow grants: %v", tools)
	}
	result := findEvent(events, "tool_result")
	if result == nil || result.Data["status"] != bizassistant.ToolStatusFailed || !strings.Contains(result.Data["error"].(string), "未知工具") {
		t.Fatalf("unauthorized tool must fail as unknown: %v", result)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+e2eActorID, admin, ""); code != http.StatusOK {
		t.Fatalf("admin user must still exist, status = %d", code)
	}

	// 事中：绕过工具过滤，直接伪造一条待确认的 DeleteUser 审计记录并确认执行。
	ctx := context.Background()
	convID, _ := idgen.NewID()
	now := time.Now()
	if err := svc.assistantData.Conversations().Create(ctx, &bizassistant.Conversation{ID: convID, UserID: alice.Id, Title: "forged", CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatal(err)
	}
	userMsgID, _ := idgen.NewID()
	assistantMsgID, _ := idgen.NewID()
	args := `{"id":"` + e2eActorID + `"}`
	for _, m := range []*bizassistant.Message{
		{ID: userMsgID, ConversationID: convID, Role: bizassistant.RoleUser, Content: "删除管理员", CreateTime: now},
		{
			ID: assistantMsgID, ConversationID: convID, Role: bizassistant.RoleAssistant, CreateTime: now,
			ToolCalls: []bizassistant.ToolCallRef{{ID: "call_forged", Name: "UserService_DeleteUser", Arguments: args}},
		},
	} {
		if err := svc.assistantData.Messages().Append(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	callID, _ := idgen.NewID()
	svc.assistantData.SeedToolCall(&bizassistant.ToolCall{
		ID: callID, ConversationID: convID, MessageID: assistantMsgID, UserID: alice.Id, CallID: "call_forged",
		ToolName: "UserService_DeleteUser", Title: "删除用户", Operation: "/system.v1.UserService/DeleteUser",
		OperationCode: biz.OperationCode("/system.v1.UserService/DeleteUser"), Write: true,
		Input: json.RawMessage(args), Status: bizassistant.ToolStatusPending, CreateTime: now,
	})
	svc.chatModel.Reply("没有权限")
	code, events, raw = confirmSSE(t, ts, aliceBearer, `{"conversationId":"`+convID+`","toolCallId":"`+callID+`","approved":true}`)
	if code != http.StatusOK {
		t.Fatalf("confirm: status = %d body = %s", code, raw)
	}
	result = findEvent(events, "tool_result")
	if result == nil || result.Data["status"] != bizassistant.ToolStatusDenied || !strings.Contains(result.Data["error"].(string), "PERMISSION_DENIED") {
		t.Fatalf("forged write must be denied by loopback authz: %v\n%s", result, raw)
	}
	if audit := svc.assistantData.ToolCall(callID); audit == nil || audit.Status != bizassistant.ToolStatusDenied {
		t.Fatalf("audit = %+v, want denied", audit)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+e2eActorID, admin, ""); code != http.StatusOK {
		t.Fatalf("admin user must still exist after denied call, status = %d", code)
	}
}

// 写操作确认：挂起 → 拒绝不执行 → 批准真实执行；重复确认 409；新消息作废未决确认。
func TestAssistant_WriteConfirmation(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	roleArgs := map[string]any{"name": "AI运维", "code": "ops_ai"}
	hasRole := func() bool {
		_, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&pageSize=50", bearer, "")
		return strings.Contains(body, `"code":"ops_ai"`)
	}

	svc.chatModel.Call("call_c", "RoleService_CreateRole", roleArgs)
	code, events, raw := chatSSE(t, ts, bearer, `{"content":"创建角色"}`)
	if code != http.StatusOK {
		t.Fatalf("chat: status = %d body = %s", code, raw)
	}
	want := []string{"conversation", "tool_call", "tool_confirm_required", "done"}
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if lastEvent(events, "done").Data["status"] != bizassistant.DoneAwaitingConfirmation || hasRole() {
		t.Fatal("write must wait for confirmation and not execute")
	}
	convID := findEvent(events, "conversation").Data["id"].(string)
	callID := findEvent(events, "tool_confirm_required").Data["id"].(string)
	if findEvent(events, "tool_call").Data["status"] != bizassistant.ToolStatusPending {
		t.Fatal("tool_call must be pending")
	}

	// 拒绝：不执行，模型收到拒绝后收尾。
	svc.chatModel.Reply("已取消。")
	code, events, raw = confirmSSE(t, ts, bearer, `{"conversationId":"`+convID+`","toolCallId":"`+callID+`","approved":false}`)
	if code != http.StatusOK {
		t.Fatalf("reject: status = %d body = %s", code, raw)
	}
	if findEvent(events, "tool_result").Data["status"] != bizassistant.ToolStatusRejected || hasRole() ||
		lastEvent(events, "done").Data["status"] != bizassistant.DoneCompleted {
		t.Fatalf("reject flow wrong: %s", raw)
	}
	if audit := svc.assistantData.ToolCall(callID); audit.Status != bizassistant.ToolStatusRejected {
		t.Fatalf("audit = %+v", audit)
	}
	// 重复确认：已非 pending，标准 409。
	code, _, raw = confirmSSE(t, ts, bearer, `{"conversationId":"`+convID+`","toolCallId":"`+callID+`","approved":true}`)
	if code != http.StatusConflict || !strings.Contains(raw, "ASSISTANT_CONFIRMATION_NOT_PENDING") {
		t.Fatalf("duplicate confirm: status = %d body = %s", code, raw)
	}

	// 批准：真实创建。
	svc.chatModel.Call("call_d", "RoleService_CreateRole", roleArgs).Reply("已创建角色。")
	code, events, raw = chatSSE(t, ts, bearer, `{"conversationId":"`+convID+`","content":"再创建一次"}`)
	if code != http.StatusOK {
		t.Fatalf("chat 2: status = %d body = %s", code, raw)
	}
	callID = findEvent(events, "tool_confirm_required").Data["id"].(string)
	code, events, raw = confirmSSE(t, ts, bearer, `{"conversationId":"`+convID+`","toolCallId":"`+callID+`","approved":true}`)
	if code != http.StatusOK {
		t.Fatalf("approve: status = %d body = %s", code, raw)
	}
	if findEvent(events, "tool_result").Data["status"] != bizassistant.ToolStatusSucceeded || !hasRole() {
		t.Fatalf("approve flow wrong: %s", raw)
	}
	if audit := svc.assistantData.ToolCall(callID); audit.Status != bizassistant.ToolStatusSucceeded || !strings.Contains(string(audit.Output), "ops_ai") {
		t.Fatalf("audit = %+v", audit)
	}

	// 新消息作废未决确认：pending → cancelled，模型收到取消后继续。
	svc.chatModel.Call("call_e", "RoleService_DeleteRole", map[string]any{"id": "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"})
	_, events, _ = chatSSE(t, ts, bearer, `{"conversationId":"`+convID+`","content":"删除角色"}`)
	pendingID := findEvent(events, "tool_confirm_required").Data["id"].(string)
	svc.chatModel.Reply("好的，不删了。")
	_, events, raw = chatSSE(t, ts, bearer, `{"conversationId":"`+convID+`","content":"算了"}`)
	if findEvent(events, "tool_result").Data["status"] != bizassistant.ToolStatusCancelled {
		t.Fatalf("pending must be cancelled by new message: %s", raw)
	}
	if audit := svc.assistantData.ToolCall(pendingID); audit.Status != bizassistant.ToolStatusCancelled {
		t.Fatalf("audit = %+v", audit)
	}
}

// 入口授权：未授 assistant 接口 403；无令牌 401；会话按用户隔离；删除后不可见。
func TestAssistant_AccessAndIsolation(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	bob := createUser(t, ts, admin, "bob_ai")
	bobBearer := testBearer(t, svc.signer, bob.Id)

	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/assistant/chat", bobBearer, `{"content":"hi"}`); code != http.StatusForbidden || !strings.Contains(body, "PERMISSION_DENIED") {
		t.Fatalf("ungranted user: status = %d body = %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/assistant/conversation?page=1&pageSize=10", bobBearer, ""); code != http.StatusForbidden {
		t.Fatalf("ungranted list: status = %d", code)
	}
	if code, _ := doRequest(t, http.MethodPost, ts.URL+"/api/v1/assistant/chat", `{"content":"hi"}`); code != http.StatusUnauthorized {
		t.Fatalf("anonymous: status = %d", code)
	}
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/assistant/chat", admin, `{"content":""}`); code != http.StatusBadRequest {
		t.Fatalf("validation: status = %d body = %s", code, body)
	}

	svc.chatModel.Reply("你好")
	_, events, _ := chatSSE(t, ts, admin, `{"content":"你好"}`)
	convID := findEvent(events, "conversation").Data["id"].(string)

	grantAssistantRole(t, ts, admin, bob.Id, "assistant_bob")
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/assistant/conversation/"+convID+"/message", bobBearer, ""); code != http.StatusNotFound {
		t.Fatalf("other user's conversation must be 404, got %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/assistant/conversation/"+convID, bobBearer, ""); code != http.StatusNotFound {
		t.Fatalf("other user's delete must be 404, got %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/assistant/chat", bobBearer, `{"conversationId":"`+convID+`","content":"插话"}`); code != http.StatusNotFound {
		t.Fatalf("chat into other user's conversation must be 404, got %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/assistant/conversation/"+convID, admin, ""); code != http.StatusOK {
		t.Fatalf("owner delete: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/assistant/conversation/"+convID+"/message", admin, ""); code != http.StatusNotFound {
		t.Fatalf("deleted conversation must be 404, got %d", code)
	}
}

// 脱敏：确认卡与审计里的密码为 ***，而回环执行拿到原始密码（新用户可登录）。
func TestAssistant_SecretsMasked(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	svc.chatModel.Call("call_p", "UserService_CreateUser", map[string]any{
		"username": "carol_ai", "password": "P@ssw0rd!x", "nickname": "carol", "gender": "female",
	})
	_, events, raw := chatSSE(t, ts, bearer, `{"content":"创建用户 carol"}`)
	confirm := findEvent(events, "tool_confirm_required")
	if confirm == nil || strings.Contains(raw, "P@ssw0rd!x") || !strings.Contains(raw, `\"password\":\"***\"`) && !strings.Contains(raw, `"password":"***"`) {
		t.Fatalf("password must be masked in events: %s", raw)
	}
	callID := confirm.Data["id"].(string)
	if audit := svc.assistantData.ToolCall(callID); strings.Contains(string(audit.Input), "P@ssw0rd!x") || !strings.Contains(string(audit.Input), "***") {
		t.Fatalf("audit input must be masked: %s", audit.Input)
	}
	convID := findEvent(events, "conversation").Data["id"].(string)
	svc.chatModel.Reply("已创建用户。")
	code, events, raw := confirmSSE(t, ts, bearer, `{"conversationId":"`+convID+`","toolCallId":"`+callID+`","approved":true}`)
	if code != http.StatusOK || findEvent(events, "tool_result").Data["status"] != bizassistant.ToolStatusSucceeded {
		t.Fatalf("approve: status = %d body = %s", code, raw)
	}
	if code, body := doRequest(t, http.MethodPost, ts.URL+"/api/v1/login", `{"username":"carol_ai","password":"P@ssw0rd!x"}`); code != http.StatusOK {
		t.Fatalf("created user must be able to log in with the raw password: status = %d body = %s", code, body)
	}
}
