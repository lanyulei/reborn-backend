package biz

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误。
var (
	ErrClientNotFound     = errors.NotFound(v1.ErrorReason_WORKFLOW_CLIENT_NOT_FOUND.String(), "workflow client not found")
	ErrClientNameExists   = errors.Conflict(v1.ErrorReason_WORKFLOW_CLIENT_NAME_EXISTS.String(), "workflow client name already exists")
	ErrClientInvalid      = errors.BadRequest(v1.ErrorReason_WORKFLOW_CLIENT_INVALID.String(), "invalid workflow client")
	ErrClientUnauthorized = errors.Unauthorized(v1.ErrorReason_WORKFLOW_CLIENT_UNAUTHORIZED.String(), "missing or invalid access token")
	ErrClientForbidden    = errors.Forbidden(v1.ErrorReason_WORKFLOW_CLIENT_FORBIDDEN.String(), "this client is not allowed to do that")
)

const (
	// maxClients 接入凭据数量上限（列表不分页）。
	maxClients = 100
	// tokenPrefix 令牌前缀：便于在日志 / 配置里一眼认出这是流程接入令牌（也便于密钥扫描工具识别）。
	tokenPrefix = "wfk_"
	// tokenBytes 令牌随机部分的字节数（256 位）。
	tokenBytes = 32
	// shownPrefix 列表里展示的令牌开头长度。
	shownPrefix = 10
	// touchInterval 「最近调用时间」的更新间隔下限（避免每次调用都写库）。
	touchInterval = time.Minute
)

// Client 外部接入凭据。
type Client struct {
	ID             string
	Name           string
	TokenPrefix    string
	ActingUserID   string
	DefinitionKeys []string
	Status         int32
	Description    string
	LastUsedTime   time.Time
	UpdateTime     time.Time
}

// 接入凭据状态。
const (
	ClientDisabled int32 = 0
	ClientEnabled  int32 = 1
)

// ClientRepo 接入凭据仓储接口。
type ClientRepo interface {
	// CreateClient 创建；重名返回 ErrClientNameExists，数量达到 limit 返回 ErrClientInvalid。
	CreateClient(ctx context.Context, c *Client, tokenHash []byte, limit int) error
	// UpdateClient 整组更新（不改令牌）；不存在返回 ErrClientNotFound。
	UpdateClient(ctx context.Context, c *Client) error
	// SetClientToken 换令牌；不存在返回 ErrClientNotFound。
	SetClientToken(ctx context.Context, id string, tokenHash []byte, prefix string) error
	// DeleteClient 软删（同时作废令牌）；不存在返回 ErrClientNotFound。
	DeleteClient(ctx context.Context, id string) error
	// ListClients 全部未删除的凭据，名称升序。
	ListClients(ctx context.Context) ([]*Client, error)
}

// ClientAuthenticator 按令牌哈希取启用的凭据并（至多每 touch 一次）更新最近调用时间；
// 没有匹配的返回 ErrClientUnauthorized。
type ClientAuthenticator interface {
	FindClientByToken(ctx context.Context, tokenHash []byte, touch time.Duration) (*Client, error)
}

// newToken 生成一枚令牌，返回明文、哈希与展示用的开头。
func newToken() (token string, hash []byte, prefix string, err error) {
	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, "", err
	}
	token = tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	return token, hashToken(token), token[:shownPrefix], nil
}

func hashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// ClientUsecase 接入凭据管理用例。
type ClientUsecase struct {
	repo  ClientRepo
	defs  DefinitionReader
	dir   Directory
	users Users
}

// NewClientUsecase new a ClientUsecase.
func NewClientUsecase(repo ClientRepo, defs DefinitionReader, dir Directory, users Users) *ClientUsecase {
	return &ClientUsecase{repo: repo, defs: defs, dir: dir, users: users}
}

// check 归一化并校验凭据：名称非空、代表用户可用、允许的流程都存在。
func (uc *ClientUsecase) check(ctx context.Context, c *Client) error {
	c.Name, c.Description, c.ActingUserID = clean(strings.TrimSpace(c.Name)), clean(strings.TrimSpace(c.Description)), strings.ToLower(c.ActingUserID)
	if c.Name == "" || len(c.DefinitionKeys) == 0 {
		return invalid(ErrClientInvalid, "a client needs a name and at least one allowed workflow")
	}
	// 停用状态下不校验代表用户：代表用户离职后，凭据仍要能被编辑成停用。
	if c.Status == ClientEnabled {
		active, err := uc.dir.ActiveUserIDs(ctx, []string{c.ActingUserID})
		if err != nil {
			return err
		}
		if len(active) != 1 {
			return invalid(ErrClientInvalid, "the acting user must be an active user")
		}
	}
	slices.Sort(c.DefinitionKeys)
	c.DefinitionKeys = slices.Compact(c.DefinitionKeys)
	for _, key := range c.DefinitionKeys {
		if _, err := uc.defs.GetDefinitionByKey(ctx, key); errors.Is(err, ErrDefinitionNotFound) {
			return invalid(ErrClientInvalid, "workflow "+key+" does not exist")
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Create 创建凭据并签发令牌；令牌明文只在此返回一次。
func (uc *ClientUsecase) Create(ctx context.Context, c *Client) (*Client, string, error) {
	if err := uc.check(ctx, c); err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	token, hash, prefix, err := newToken()
	if err != nil {
		return nil, "", err
	}
	c.ID, c.TokenPrefix, c.UpdateTime = id, prefix, time.Now().UTC()
	if err := uc.repo.CreateClient(ctx, c, hash, maxClients); err != nil {
		return nil, "", err
	}
	return c, token, nil
}

// Update 整组更新凭据（不改令牌）。
func (uc *ClientUsecase) Update(ctx context.Context, c *Client) error {
	if err := uc.check(ctx, c); err != nil {
		return err
	}
	return uc.repo.UpdateClient(ctx, c)
}

// Rotate 重置令牌：旧令牌立即失效，返回新令牌明文（只此一次）。
func (uc *ClientUsecase) Rotate(ctx context.Context, id string) (string, error) {
	token, hash, prefix, err := newToken()
	if err != nil {
		return "", err
	}
	if err := uc.repo.SetClientToken(ctx, id, hash, prefix); err != nil {
		return "", err
	}
	return token, nil
}

// Delete 删除凭据。
func (uc *ClientUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteClient(ctx, id)
}

// List 全部凭据与代表用户的显示名。
func (uc *ClientUsecase) List(ctx context.Context) ([]*Client, map[string]string, error) {
	items, err := uc.repo.ListClients(ctx)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(items))
	for _, c := range items {
		ids = append(ids, c.ActingUserID)
	}
	names, err := uc.users.UserNames(ctx, uniqueStrings(ids))
	return items, names, err
}

// OpenUsecase 开放接口用例：凭令牌认证后，以凭据的代表用户身份操作它自己发起的实例。
type OpenUsecase struct {
	rt      *RuntimeUsecase
	clients ClientAuthenticator
}

// NewOpenUsecase new an OpenUsecase.
func NewOpenUsecase(rt *RuntimeUsecase, clients ClientAuthenticator) *OpenUsecase {
	return &OpenUsecase{rt: rt, clients: clients}
}

// authenticate 校验令牌并确认代表用户仍然可用。令牌缺失 / 格式不对 / 不存在 / 凭据停用一律
// 返回同一个错误（不区分原因，不给探测留线索）。
func (uc *OpenUsecase) authenticate(ctx context.Context, token string) (*Client, error) {
	if !strings.HasPrefix(token, tokenPrefix) || len(token) > 128 {
		return nil, ErrClientUnauthorized
	}
	c, err := uc.clients.FindClientByToken(ctx, hashToken(token), touchInterval)
	if err != nil {
		return nil, err
	}
	active, err := uc.rt.dir.ActiveUserIDs(ctx, []string{c.ActingUserID})
	if err != nil {
		return nil, err
	}
	if len(active) != 1 {
		return nil, invalid(ErrClientForbidden, "the acting user of this client is no longer active")
	}
	return c, nil
}

// own 取凭据自己发起的实例；不是它发起的按「不存在」处理（不泄露别人的实例是否存在）。
func (uc *OpenUsecase) own(ctx context.Context, c *Client, instanceID string) (*Instance, error) {
	in, err := uc.rt.lookup.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if in.ClientID != c.ID {
		return nil, ErrInstanceNotFound
	}
	return in, nil
}

// Start 发起流程（须在凭据允许的范围内）。
func (uc *OpenUsecase) Start(ctx context.Context, token string, p StartParams) (*Instance, error) {
	c, err := uc.authenticate(ctx, token)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(c.DefinitionKeys, p.DefinitionKey) {
		return nil, invalid(ErrClientForbidden, "this client is not allowed to start that workflow")
	}
	p.Initiator, p.InitiatorDept, p.BusinessType, p.BusinessID, p.clientID = c.ActingUserID, "", "", "", c.ID
	// 同一幂等键的并发请求由 Start 自己重试一次，读到首次发起的实例。
	in, err := uc.rt.Start(ctx, p)
	if err != nil {
		return nil, err
	}
	if in.ClientID != c.ID {
		// 幂等键撞上了同一代表用户经别的途径发起的实例：不把那个实例交给本凭据。
		return nil, ErrBusinessDuplicate
	}
	// 幂等重放拿到的实例不带流程变量：统一回读一次，响应完整。
	return uc.own(ctx, c, in.ID)
}

// Get 查询自己发起的实例。
func (uc *OpenUsecase) Get(ctx context.Context, token, instanceID string) (*Instance, error) {
	c, err := uc.authenticate(ctx, token)
	if err != nil {
		return nil, err
	}
	return uc.own(ctx, c, instanceID)
}

// Signal 向自己发起的实例投递信号。
func (uc *OpenUsecase) Signal(ctx context.Context, token, instanceID, name string, vars map[string]any) (*Instance, error) {
	c, err := uc.authenticate(ctx, token)
	if err != nil {
		return nil, err
	}
	if _, err := uc.own(ctx, c, instanceID); err != nil {
		return nil, err
	}
	if _, err := uc.rt.Signal(ctx, instanceID, name, vars); err != nil {
		return nil, err
	}
	return uc.own(ctx, c, instanceID)
}

// Cancel 撤回自己发起的实例。以实例的发起人身份撤回（凭据的代表用户此后被换成别人，也不
// 影响它撤回自己早先发起的实例）。
func (uc *OpenUsecase) Cancel(ctx context.Context, token, instanceID, comment string) (*Instance, error) {
	c, err := uc.authenticate(ctx, token)
	if err != nil {
		return nil, err
	}
	in, err := uc.own(ctx, c, instanceID)
	if err != nil {
		return nil, err
	}
	if err := uc.rt.Cancel(ctx, instanceID, in.Initiator, comment); err != nil {
		return nil, err
	}
	return uc.own(ctx, c, instanceID)
}
