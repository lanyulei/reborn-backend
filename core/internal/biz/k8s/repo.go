package biz

import (
	"context"
	"io"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ClusterReader 集群读取。恒不含凭据密文。
type ClusterReader interface {
	List(ctx context.Context, filter ClusterFilter, page, pageSize int64) ([]*Cluster, int64, error)
	// Get 不存在返回 ErrClusterNotFound。
	Get(ctx context.Context, id string) (*Cluster, error)
	// Options 已启用的集群及调用者在其上的最高档位：all 为 true（超管）返回全部且档位为集群管理员，
	// 否则只返回 roleIDs 有授权的集群。按环境、名称排序。
	Options(ctx context.Context, all bool, roleIDs []string) ([]*ClusterOption, error)
}

// ClusterWriter 集群写入。每次写入同事务 bump k8s_cfg_version 并追加一条审计。
type ClusterWriter interface {
	// Create 同事务插入运行状态行；名称重复返回 ErrClusterNameExists。
	Create(ctx context.Context, c *Cluster, credential []byte, audit *AuditEntry) error
	// Update 乐观锁更新；credential 为 nil 沿用已保存的。版本不一致返回 ErrVersionConflict。
	// 名称与接入方式不在更新列内。
	Update(ctx context.Context, c *Cluster, credential []byte, expectVersion int32, audit *AuditEntry) error
	// Delete 软删并清除凭据，同时删除该集群的数据范围与 Agent 令牌；审计记录保留。
	Delete(ctx context.Context, id string, audit *AuditEntry) error
}

// AgentTokenRepo Agent 令牌。写入同事务 bump k8s_cfg_version 并追加审计。
type AgentTokenRepo interface {
	// ReplaceRegister 作废该集群已有的注册令牌并写入新的；集群不存在返回 ErrClusterNotFound。
	ReplaceRegister(ctx context.Context, clusterID, tokenID string, hash []byte, expire time.Time, audit *AuditEntry) error
	// RevokeAll 删除该集群的全部令牌；集群不存在返回 ErrClusterNotFound。
	RevokeAll(ctx context.Context, clusterID string, audit *AuditEntry) error
}

// ScopeRepo 角色数据范围。
type ScopeRepo interface {
	// Get 某角色的数据范围（只含未删除的集群），按集群名称、命名空间排序。
	Get(ctx context.Context, roleID string) ([]*RoleScope, error)
	// Replace 同事务整组替换、bump k8s_cfg_version 并追加审计；引用了不存在的集群返回 ErrScopeInvalid 的说明。
	Replace(ctx context.Context, roleID string, items []*RoleScope, audit *AuditEntry) error
	// Grant 把一条授权并入角色已有的数据范围（同一集群同一命名空间取较高档位）：读、合并、整组写入在同一
	// 事务内并持有该角色的锁，与管理端的 Replace、其它实例的并发授予互斥，不会丢掉并发写入的授权。
	// 审计与配置版本的处理同 Replace；引用了不存在的集群同样返回 ErrScopeInvalid 的说明。
	Grant(ctx context.Context, roleID string, item *RoleScope, audit *AuditEntry) error
}

// AuditReader 审计与终端会话读取。
type AuditReader interface {
	List(ctx context.Context, filter AuditFilter, page, pageSize int64) ([]*Audit, int64, error)
	// Get 含对象内容；不存在返回 ErrAuditNotFound。
	Get(ctx context.Context, id string) (*Audit, error)
	ListExecSessions(ctx context.Context, filter ExecSessionFilter, page, pageSize int64) ([]*ExecSession, int64, error)
}

// ExecTicket 终端票据核销后的内容。
type ExecTicket struct {
	UserID    string
	ClusterID string
	Params    kc.ExecParams
}

// TicketRepo 终端票据（k8s_exec_ticket）。
type TicketRepo interface {
	// Create 写入票据摘要，ttl 后过期（数据库时钟）；顺带清理已过期的票据。
	Create(ctx context.Context, hash []byte, t ExecTicket, ttl time.Duration) error
	// Redeem 单语句核销（DELETE … RETURNING）：不存在、已用过或已过期返回 ErrTicketInvalid。
	Redeem(ctx context.Context, hash []byte) (*ExecTicket, error)
}

// OutboxRepo 通知发件箱。core 只改派发状态相关的列。
type OutboxRepo interface {
	// ClaimTx 事务内领取一行待派发的（FOR UPDATE SKIP LOCKED），跳过 exclude；没有返回 nil。
	ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*OutboxItem, error)
	// FinishTx 置为已派发或已丢弃。
	FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error
	// Fail 事务外记一次失败；达到上限即置为已丢弃并返回 true。
	Fail(ctx context.Context, id, reason string, maxAttempts int32) (dropped bool, err error)
}

// ExecConn 到数据面的一条终端连接（WebSocket 二进制帧，格式见 kit/k8scontract 的 Frame*）。
type ExecConn interface {
	// Read 读一帧；连接结束返回 error。
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, frame []byte) error
	// Close 关闭连接（幂等）。
	Close() error
}

// Plane 数据面访问（internal/pkg/k8splane 实现）：按集群归属选实例，身份头签名，错误体还原为业务错误。
// clusterID 为空表示与集群无关的操作（任选一个存活实例）。
type Plane interface {
	// Call 一元调用：req 编码为 JSON，返回数据面的 JSON 响应体（原样透传给浏览器）。
	Call(ctx context.Context, id kc.Identity, clusterID, op string, req any) ([]byte, error)
	// Stream 流式调用：返回数据面的 SSE 响应体，调用方负责 Close。建立前的失败是普通业务错误。
	Stream(ctx context.Context, id kc.Identity, clusterID, stream string, req any) (io.ReadCloser, error)
	// DialExec 建立终端连接。
	DialExec(ctx context.Context, id kc.Identity, clusterID string, params kc.ExecParams) (ExecConn, error)
}
