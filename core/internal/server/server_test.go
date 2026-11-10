package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	notifyservice "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/durationpb"
)

// testServices 是 e2e 测试装配产物：十一个 service、授权用例与测试签发器
// （authSvc 供路由注册，signer 供测试直接签发访问令牌）。
type testServices struct {
	user         *service.UserService
	role         *service.RoleService
	auth         *service.AuthService
	syscfg       *service.SystemConfigService
	loginLog     *service.LoginLogService
	notify       *notifyservice.NotifyChannelService
	template     *notifyservice.NotifyTemplateService
	route        *notifyservice.NotifyRouteService
	silence      *notifyservice.NotifySilenceService
	subscription *notifyservice.NotifySubscriptionService
	producer     *notifyservice.NotifyProducerService
	event        *notifyservice.NotifyEventService
	dispatch     *notifyservice.NotifyDispatchService
	deliveryLog  *notifyservice.NotifyDeliveryLogService
	deadLetter   *notifyservice.NotifyDeadLetterService
	store        *testfake.Store
	authz        *biz.AuthzUsecase
	signer       *biz.TokenSigner
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
	channelUC := biznotify.NewChannelUsecase(store, store, store)
	return testServices{
		user:     service.NewUserService(userUC),
		role:     service.NewRoleService(biz.NewRoleUsecase(store, store, authorizer), biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager())),
		auth:     service.NewAuthService(authUC, authzUC),
		syscfg:   service.NewSystemConfigService(biz.NewSystemConfigUsecase(store)),
		loginLog: service.NewLoginLogService(biz.NewLoginLogUsecase(store)),
		notify:   notifyservice.NewNotifyChannelService(channelUC, authzUC),
		template: notifyservice.NewNotifyTemplateService(biznotify.NewTemplateUsecase(store), authzUC),
		route:    notifyservice.NewNotifyRouteService(biznotify.NewRouteUsecase(store, store), authzUC),
		silence:  notifyservice.NewNotifySilenceService(biznotify.NewSilenceUsecase(store), authzUC),
		// 订阅代管校验复用 system 用户用例（is_admin 直查 store）。
		subscription: notifyservice.NewNotifySubscriptionService(
			biznotify.NewSubscriptionUsecase(store), userUC),
		producer: notifyservice.NewNotifyProducerService(biznotify.NewProducerUsecase(store), authzUC),
		event:    notifyservice.NewNotifyEventService(biznotify.NewEventUsecase(store), authzUC),
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

// 验证 gRPC Server 在完整配置（network/addr/timeout）下可构造、可启停。
func TestNewGRPCServer(t *testing.T) {
	t.Parallel()
	svc := newTestServices(t)
	srv := NewGRPCServer(
		&conf.Server{Grpc: &conf.Server_GRPC{
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
		svc.signer,
		svc.authz,
	)
	if srv == nil {
		t.Fatal("NewGRPCServer() = nil")
	}
	if ep, err := srv.Endpoint(); err != nil || ep == nil {
		t.Fatalf("grpc server endpoint = %v, err = %v", ep, err)
	}
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
