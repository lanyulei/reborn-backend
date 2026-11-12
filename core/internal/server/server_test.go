package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	assistantservice "github.com/lanyulei/reborn/backend/core/internal/service/assistant"
	cmdbservice "github.com/lanyulei/reborn/backend/core/internal/service/cmdb"
	notifyservice "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/durationpb"
)

// testServices 是 e2e 测试装配产物：十一个 service、授权用例与测试签发器
// （authSvc 供路由注册，signer 供测试直接签发访问令牌）。
type testServices struct {
	user          *service.UserService
	role          *service.RoleService
	auth          *service.AuthService
	syscfg        *service.SystemConfigService
	loginLog      *service.LoginLogService
	notify        *notifyservice.NotifyChannelService
	template      *notifyservice.NotifyTemplateService
	route         *notifyservice.NotifyRouteService
	silence       *notifyservice.NotifySilenceService
	subscription  *notifyservice.NotifySubscriptionService
	producer      *notifyservice.NotifyProducerService
	event         *notifyservice.NotifyEventService
	dispatch      *notifyservice.NotifyDispatchService
	deliveryLog   *notifyservice.NotifyDeliveryLogService
	deadLetter    *notifyservice.NotifyDeadLetterService
	assistant     *assistantservice.AssistantService
	assistantChat *assistantservice.AssistantChatService
	chatModel     *testfake.ChatModel
	loopback      *testfake.Loopback
	assistantData *testfake.AssistantStore
	cmdbModel     *cmdbservice.CmdbModelService
	cmdbCI        *cmdbservice.CmdbCIService
	cmdbRelation  *cmdbservice.CmdbRelationService
	cmdbScope     *cmdbservice.CmdbScopeService
	cmdbSync      *cmdbservice.CmdbSyncService
	cmdbInsight   *cmdbservice.CmdbInsightService
	syncData      *testfake.SyncStore
	cmdbData      *testfake.CmdbStore
	store         *testfake.Store
	authz         *biz.AuthzUsecase
	signer        *biz.TokenSigner
}

// newTestServices 组装真实依赖链（biz 用例 + 内存仓储）下的 service 与签发器。
// 授权中间件按 claims 里的 userID 实时查用户，既有 e2e 的固定操作者必须以
// is_admin 用户身份预置（否则全部既有用例被 authz 403 拦截）。
func newTestServices(t *testing.T) testServices {
	t.Helper()
	store := testfake.NewStore()
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	signer, err := testfake.NewSigner()
	if err != nil {
		t.Fatalf("testfake.NewSigner() error = %v", err)
	}
	authorizer, err := testfake.NewAuthorizer(store, time.Minute)
	if err != nil {
		t.Fatalf("testfake.NewAuthorizer() error = %v", err)
	}
	actor := &biz.User{
		ID:       e2eActorID,
		Username: "e2e-actor",
		Nickname: "e2e-actor",
		Status:   biz.StatusEnabled,
		IsAdmin:  true,
	}
	if err := store.CreateUser(context.Background(), actor); err != nil {
		t.Fatalf("seed e2e actor: %v", err)
	}
	authUC, err := biz.NewAuthUsecase(store, store, store, store, hasher, signer,
		store, testLogger())
	if err != nil {
		t.Fatalf("biz.NewAuthUsecase() error = %v", err)
	}
	userUC := biz.NewUserUsecase(store, store, store, store, hasher, store)
	authzUC := biz.NewAuthzUsecase(store, store, store, authorizer)
	channelUC := biznotify.NewChannelUsecase(store, store, store).WithTestEventCooldown(0)
	// 智能助手：脚本模型桩 + 真实 HTTP 回环（地址在 httptest 启动后注入），
	// 工具调用真实经过 auth/authz 中间件链。
	chatModel := testfake.NewChatModel()
	loopback := testfake.NewLoopback()
	assistantData := testfake.NewAssistantStore()
	catalog, err := assistantservice.NewToolCatalog()
	if err != nil {
		t.Fatalf("assistantservice.NewToolCatalog() error = %v", err)
	}
	agentUC := bizassistant.NewAgentUsecase(assistantData.Conversations(), assistantData.Messages(), assistantData.ToolCalls(),
		chatModel, loopback, authzUC, userUC, catalog,
		bizassistant.AgentConfig{Enabled: true, Timeout: 30 * time.Second, MaxSteps: 6, MaxConcurrentRuns: 4},
		testLogger())
	conversationUC := bizassistant.NewConversationUsecase(assistantData.Conversations(), assistantData.Messages(), assistantData.ToolCalls())
	// CMDB：内存仓储，模型/资源/关系/历史语义对齐 data 层。
	cmdbData := testfake.NewCmdbStore()
	roleUC := biz.NewRoleUsecase(store, store, authorizer)
	cmdbModelUC := bizcmdb.NewModelUsecase(cmdbData.Models(), cmdbData.Fields(), cmdbData.RelationTypes(), cmdbData.Relations(), cmdbData.Views(), &testfake.FakeIndexer{}, cmdbData.Groups(), cmdbData.SchemaAudits())
	cmdbRelationUC := bizcmdb.NewRelationUsecase(cmdbData.RelationTypes(), cmdbData.CIs(), cmdbData.Relations(), cmdbData.Fields())
	cmdbCIUC := bizcmdb.NewCIUsecase(cmdbData.Models(), cmdbData.Fields(), cmdbData.RelationTypes(), cmdbData.CIs(), cmdbData.Relations(), cmdbData.Histories(), cmdbRelationUC)
	cmdbScopeUC := bizcmdb.NewScopeUsecase(cmdbData.Scopes(), authzUC, roleUC, cmdbData.Models())
	syncData := testfake.NewSyncStore()
	cmdbSyncUC := bizcmdb.NewSyncUsecase(syncData.Sources(), syncData.Tasks(), syncData.Runs(), &testfake.FakeSealer{Enabled: true})
	cmdbInsightUC := bizcmdb.NewInsightUsecase(cmdbData.Groups(), cmdbData.Favorites(), cmdbData.Overviews(), cmdbData.SchemaAudits(), cmdbData.Models())
	return testServices{
		cmdbModel:     cmdbservice.NewCmdbModelService(cmdbModelUC),
		cmdbCI:        cmdbservice.NewCmdbCIService(cmdbCIUC, cmdbScopeUC),
		cmdbRelation:  cmdbservice.NewCmdbRelationService(cmdbRelationUC, cmdbCIUC, cmdbScopeUC),
		cmdbScope:     cmdbservice.NewCmdbScopeService(cmdbScopeUC),
		cmdbSync:      cmdbservice.NewCmdbSyncService(cmdbSyncUC),
		cmdbInsight:   cmdbservice.NewCmdbInsightService(cmdbInsightUC, cmdbScopeUC),
		syncData:      syncData,
		cmdbData:      cmdbData,
		assistant:     assistantservice.NewAssistantService(conversationUC),
		assistantChat: assistantservice.NewAssistantChatService(agentUC),
		chatModel:     chatModel,
		loopback:      loopback,
		assistantData: assistantData,
		user:          service.NewUserService(userUC),
		role:          service.NewRoleService(biz.NewRoleUsecase(store, store, authorizer), biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager()), userUC),
		auth:          service.NewAuthService(authUC, authzUC, biz.NewAvatarUsecase(store, store, testfake.NewAvatarStore())),
		syscfg:        service.NewSystemConfigService(biz.NewSystemConfigUsecase(store)),
		loginLog:      service.NewLoginLogService(biz.NewLoginLogUsecase(store)),
		notify:        notifyservice.NewNotifyChannelService(channelUC, biznotify.NewChannelHealthUsecase(store), authzUC),
		template:      notifyservice.NewNotifyTemplateService(biznotify.NewTemplateUsecase(store), authzUC),
		route:         notifyservice.NewNotifyRouteService(biznotify.NewRouteUsecase(store, store), biznotify.NewPreviewUsecase(store, store, store, store), authzUC),
		silence:       notifyservice.NewNotifySilenceService(biznotify.NewSilenceUsecase(store), authzUC),
		// 订阅代管校验复用 system 用户用例（is_admin 直查 store）。
		subscription: notifyservice.NewNotifySubscriptionService(
			biznotify.NewSubscriptionUsecase(store), userUC, authzUC),
		producer: notifyservice.NewNotifyProducerService(biznotify.NewProducerUsecase(store), authzUC),
		event:    notifyservice.NewNotifyEventService(biznotify.NewEventUsecase(store, store), biznotify.NewDispatchUsecase(store), biznotify.NewDeliveryLogUsecase(store), authzUC),
		dispatch: notifyservice.NewNotifyDispatchService(biznotify.NewDispatchUsecase(store), channelUC, authzUC),
		deliveryLog: notifyservice.NewNotifyDeliveryLogService(
			biznotify.NewDeliveryLogUsecase(store), channelUC, authzUC),
		deadLetter: notifyservice.NewNotifyDeadLetterService(
			biznotify.NewDeadLetterUsecase(store), channelUC, authzUC),
		store:  store, // 事件/任务/明细/死信等运行数据用例经 Seed* 预置
		authz:  authzUC,
		signer: signer,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// 验证 HTTP Server 在完整配置下可构造，且路由已注册。
func TestNewHTTPServerWithOptions(t *testing.T) {
	t.Parallel()
	svc := newTestServices(t)
	srv := NewHTTPServer(
		&conf.Server{Http: &conf.Server_HTTP{
			Network: "tcp",
			Addr:    "127.0.0.1:0",
			Timeout: durationpb.New(time.Second),
		}},
		testLogger(),
		svc.user,
		svc.role,
		svc.auth,
		svc.syscfg,
		svc.loginLog,
		svc.notify,
		svc.template,
		svc.route,
		svc.silence,
		svc.subscription,
		svc.producer,
		svc.event,
		svc.dispatch,
		svc.deliveryLog,
		svc.deadLetter,
		svc.assistant,
		svc.assistantChat,
		svc.cmdbModel,
		svc.cmdbCI,
		svc.cmdbRelation,
		svc.cmdbScope,
		svc.cmdbSync,
		svc.cmdbInsight,
		svc.signer,
		svc.authz,
	)
	if srv == nil {
		t.Fatal("NewHTTPServer() = nil")
	}
	if ep, err := srv.Endpoint(); err != nil || ep == nil {
		t.Fatalf("http server endpoint = %v, err = %v", ep, err)
	}
}
