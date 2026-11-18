package biz

import (
	"context"
	"io"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// PlaneUsecase 把请求连同调用者身份转给数据面，并管理终端票据。
//
// 身份只在这里构造：用户 ID 来自登录态，启用角色与是否超管每次实时读取（账号停用、角色解绑即刻
// 生效），调用方无法自带角色。数据范围不在这里判定——由集群按该身份判定。
type PlaneUsecase struct {
	plane    Plane
	roles    RoleSource
	accounts Accounts
	dir      Directory
	clusters ClusterReader
	tickets  TicketRepo
	now      func() time.Time
	// access 转发前的集群可见性检查；由 NewAccessResolver 在装配期注入（nil = 不检查，仅单元测试）。
	access *AccessResolver
	// debug 调试容器权限判定；由 ChangeUsecase 在装配期注入（nil = 不检查，仅单元测试）。
	debug BypassChecker
	// gate 写操作的审批关口；由 ChangeUsecase 在装配期注入（nil = 不拦截，仅单元测试）。
	gate writeGate
}

// NewPlaneUsecase new a PlaneUsecase.
func NewPlaneUsecase(plane Plane, roles RoleSource, accounts Accounts, dir Directory, clusters ClusterReader, tickets TicketRepo) *PlaneUsecase {
	return &PlaneUsecase{plane: plane, roles: roles, accounts: accounts, dir: dir, clusters: clusters, tickets: tickets, now: time.Now}
}

// identity 解析调用者在数据面的身份。clusterID 非空时先确认调用者在该集群上有数据范围：没有任何授权的
// 用户不转发——否则他会以 system:authenticated 的身份到达集群，集群里凡是绑给这个组的权限都对他生效。
// 范围外与不存在同样返回 404（不泄露集群是否存在）。
func (uc *PlaneUsecase) identity(ctx context.Context, caller Caller, clusterID string) (kc.Identity, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
	if err != nil {
		return kc.Identity{}, err
	}
	if uc.access != nil && clusterID != "" && !isAdmin {
		access, err := uc.access.forRoles(ctx, false, roleIDs)
		if err != nil {
			return kc.Identity{}, err
		}
		if !access.AllowsCluster(clusterID) {
			return kc.Identity{}, ErrClusterNotFound
		}
	}
	id := kc.Identity{UserID: caller.UserID, RoleIDs: roleIDs, IsAdmin: isAdmin, ClientIP: caller.ClientIP, IssuedAt: uc.now().Unix()}
	if id.RoleIDs == nil {
		id.RoleIDs = []string{}
	}
	if names, err := uc.dir.UserNames(ctx, []string{caller.UserID}); err == nil {
		id.Username = names[caller.UserID]
	}
	if rid, err := idgen.NewID(); err == nil {
		id.RequestID = rid
	}
	return id, nil
}

// Call 一元调用：返回数据面的 JSON 响应体。
func (uc *PlaneUsecase) Call(ctx context.Context, caller Caller, clusterID, op string, req any) ([]byte, error) {
	id, err := uc.identity(ctx, caller, clusterID)
	if err != nil {
		return nil, err
	}
	if uc.gate != nil && clusterID != "" && kc.IsWriteOp(op) {
		if err := uc.gate.Intercept(ctx, caller, &id, clusterID, op, req); err != nil {
			return nil, err
		}
	}
	return uc.plane.Call(ctx, id, clusterID, op, req)
}

// Stream 流式调用：返回数据面的 SSE 响应体，调用方负责 Close。
func (uc *PlaneUsecase) Stream(ctx context.Context, caller Caller, clusterID, stream string, req any) (io.ReadCloser, error) {
	id, err := uc.identity(ctx, caller, clusterID)
	if err != nil {
		return nil, err
	}
	if uc.gate != nil && clusterID != "" && kc.IsWriteStream(stream) {
		if err := uc.gate.Intercept(ctx, caller, &id, clusterID, stream, req); err != nil {
			return nil, err
		}
	}
	return uc.plane.Stream(ctx, id, clusterID, stream, req)
}

// execCluster 终端的前置检查：集群存在、已启用、未禁止终端。
func (uc *PlaneUsecase) execCluster(ctx context.Context, clusterID string) error {
	c, err := uc.clusters.Get(ctx, clusterID)
	if err != nil {
		return err
	}
	switch {
	case !c.Enabled:
		return ErrClusterNotFound
	case c.ExecDisabled:
		return ErrExecDisabled
	}
	return nil
}

// OpenExecTicket 签发一次性的终端票据（ExecTicketTTL 内有效），返回票据明文。
// 这里只确认集群允许终端；能否进入具体容器在建连时由集群按用户身份判定。
func (uc *PlaneUsecase) OpenExecTicket(ctx context.Context, caller Caller, clusterID string, params kc.ExecParams) (string, error) {
	if err := uc.execCluster(ctx, clusterID); err != nil {
		return "", err
	}
	if _, err := uc.identity(ctx, caller, clusterID); err != nil {
		return "", err
	}
	if params.Kind == 0 {
		params.Kind = kc.ExecKindShell
	}
	if params.Kind == kc.ExecKindDebug && uc.debug != nil {
		ok, err := uc.debug.CanDebug(ctx, caller.UserID)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", ErrDebugForbidden
		}
	}
	ticket, hash, err := kc.NewTicket()
	if err != nil {
		return "", err
	}
	if err := uc.tickets.Create(ctx, hash, ExecTicket{UserID: caller.UserID, ClusterID: clusterID, Params: params}, kc.ExecTicketTTL); err != nil {
		return "", err
	}
	return ticket, nil
}

// ConnectExec 核销票据并建立到数据面的终端连接。票据只能用一次；身份按票据的签发用户在此刻重新
// 解析：账号已停用或已删除、集群已停用或已禁止终端，一律拒绝。
func (uc *PlaneUsecase) ConnectExec(ctx context.Context, ticket, clientIP string) (ExecConn, error) {
	if len(ticket) < 32 || len(ticket) > 128 {
		return nil, ErrTicketInvalid
	}
	t, err := uc.tickets.Redeem(ctx, kc.HashSecret(ticket))
	if err != nil {
		return nil, err
	}
	active, err := uc.accounts.Active(ctx, t.UserID)
	if err != nil {
		if kerrors.FromError(err).Code == 500 {
			return nil, err
		}
		return nil, ErrTicketInvalid
	}
	if !active {
		return nil, ErrTicketInvalid
	}
	if err := uc.execCluster(ctx, t.ClusterID); err != nil {
		return nil, err
	}
	id, err := uc.identity(ctx, Caller{UserID: t.UserID, ClientIP: clientIP}, t.ClusterID)
	if err != nil {
		return nil, err
	}
	return uc.plane.DialExec(ctx, id, t.ClusterID, t.Params)
}
