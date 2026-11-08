package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/conf"
	"github.com/lanyulei/monitor/backend/core/internal/service"
	"github.com/lanyulei/monitor/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/durationpb"
)

// testServices 是 e2e 测试装配产物：三个 service、授权用例与测试签发器
// （authSvc 供路由注册，signer 供测试直接签发访问令牌）。
type testServices struct {
	user   *service.UserService
	role   *service.RoleService
	auth   *service.AuthService
	syscfg *service.SystemConfigService
	authz  *biz.AuthzUsecase
	signer *biz.TokenSigner
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
	authUC, err := biz.NewAuthUsecase(store, store, store, store, hasher, signer)
	if err != nil {
		t.Fatalf("biz.NewAuthUsecase() error = %v", err)
	}
	return testServices{
		user:   service.NewUserService(biz.NewUserUsecase(store, store, store, store, hasher)),
		role:   service.NewRoleService(biz.NewRoleUsecase(store, store, authorizer), biz.NewPermissionUsecase(store, store, authorizer)),
		auth:   service.NewAuthService(authUC, biz.NewAuthzUsecase(store, store, store, authorizer)),
		syscfg: service.NewSystemConfigService(biz.NewSystemConfigUsecase(store)),
		authz:  biz.NewAuthzUsecase(store, store, store, authorizer),
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
