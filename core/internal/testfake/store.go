// Package testfake 提供 biz 仓储接口的内存实现，仅供各层单测组装完整链路（不依赖数据库）。
// 语义对齐 data 层 PostgreSQL 实现：唯一键冲突返回相同哨兵错误、绑定全量覆盖、删除级联清绑定。
package testfake

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 编译期断言：Store 同时实现 biz 定制的全部仓储接口。
var (
	_ biz.UserReader             = (*Store)(nil)
	_ biz.UserWriter             = (*Store)(nil)
	_ biz.RoleReader             = (*Store)(nil)
	_ biz.RoleWriter             = (*Store)(nil)
	_ biz.UserRoleRepo           = (*Store)(nil)
	_ biz.RefreshTokenReader     = (*Store)(nil)
	_ biz.RefreshTokenWriter     = (*Store)(nil)
	_ biz.PermissionPolicySource = (*Store)(nil)
	_ biz.PermissionAdminRepo    = (*Store)(nil)
	_ biz.PermissionGrantReader  = (*Store)(nil)
	_ biz.SystemConfigRepo       = (*Store)(nil)
	_ biz.LoginLogWriter         = (*Store)(nil)
	_ biz.LoginLogReader         = (*Store)(nil)
	_ biznotify.ChannelRepo      = (*Store)(nil)
	_ biznotify.TemplateRepo     = (*Store)(nil)
	_ biznotify.RouteRepo        = (*Store)(nil)
	_ biznotify.SilenceRepo      = (*Store)(nil)
	_ biznotify.SubscriptionRepo = (*Store)(nil)
	_ biznotify.ProducerRepo     = (*Store)(nil)
	_ biznotify.EventRepo        = (*Store)(nil)
	_ biznotify.EventWriter      = (*Store)(nil)
	_ biznotify.EventTxRepo      = (*Store)(nil)
	_ biznotify.DispatchRepo     = (*Store)(nil)
	_ biznotify.DeliveryLogRepo  = (*Store)(nil)
	_ biznotify.DeadLetterRepo   = (*Store)(nil)
)

// Store 是全部仓储接口的内存实现；并发安全（读路径快照后迭代，临界区无调用方逻辑）。
type Store struct {
	mu            sync.RWMutex
	users         map[string]*biz.User
	roles         map[string]*biz.Role
	bindings      map[string][]string            // userID -> roleIDs（无序）
	refreshTokens map[string]*biz.RefreshToken   // tokenHash -> token
	permissions   map[string]map[string]struct{} // roleID -> operation 集合（物化策略）
	grants        map[string]biz.GrantSet        // roleID -> 直接授权（Store 持有所有权）
	loginLogs     []*biz.LoginLog                // 追加即序（append-only，无删除）
	channels      map[string]*channelEntry       // channelID -> 渠道条目（含软删标记）
	templates     map[string]*templateEntry      // templateID -> 模板条目（含软删标记）
	routes        map[string]*routeEntry         // routeID -> 路由条目（含软删标记）
	silences      map[string]*silenceEntry       // silenceID -> 静默条目（含软删标记）
	subscriptions map[string]*subscriptionEntry  // subscriptionID -> 订阅条目（含软删标记）
	producers     map[string]*producerEntry      // producerID -> 生产者条目（含软删标记）
	events        []*biznotify.Event             // 事件运行数据（append-only，经 SeedEvent 预置）
	dispatches    []*biznotify.Dispatch          // 发送任务运行数据（append-only，经 SeedDispatch 预置）
	deliveryLogs  []*biznotify.DeliveryLog       // 投递明细运行数据（append-only，经 SeedDeliveryLog 预置）
	deadLetters   []*biznotify.DeadLetter        // 死信运行数据（sender 写入，经 SeedDeadLetter 预置）
	dedup         map[string]dedupEntry          // 直插去重门闩（biz|dedup_key → 首次事件）
	systemConfig  *biz.SystemConfig              // 单例配置行（nil = 未配置）
	logoFile      []byte                         // logo 文件内容（nil = 未上传）
	logoMime      string                         // logo 文件 MIME
}

// NewStore new a Store.
func NewStore() *Store {
	return &Store{
		users:         make(map[string]*biz.User),
		roles:         make(map[string]*biz.Role),
		bindings:      make(map[string][]string),
		refreshTokens: make(map[string]*biz.RefreshToken),
		permissions:   make(map[string]map[string]struct{}),
		grants:        make(map[string]biz.GrantSet),
		channels:      make(map[string]*channelEntry),
		templates:     make(map[string]*templateEntry),
		routes:        make(map[string]*routeEntry),
		silences:      make(map[string]*silenceEntry),
		subscriptions: make(map[string]*subscriptionEntry),
		producers:     make(map[string]*producerEntry),
	}
}

// NewHasher 构造测试用 Argon2id 哈希器（OWASP 下限参数，加速测试）。
func NewHasher() (*biz.PasswordHasher, error) {
	return biz.NewPasswordHasher(&conf.Security{
		Password: &conf.Security_Password{
			MemoryKb:    19456,
			Iterations:  2,
			Parallelism: 1,
		},
	})
}

// testSigningKey 是测试专用 HS256 密钥（36 字节，满足构造下限）；
// 仅存在于测试脚手架，不是任何环境的真实凭据。
const testSigningKey = "testonly-signing-key-0123456789abcdef"

// NewSigner 构造测试用访问令牌签发器（固定参数，仅供单测）。
func NewSigner() (*biz.TokenSigner, error) {
	return biz.NewTokenSigner(&conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     testSigningKey,
			AccessTtl:  durationpb.New(15 * time.Minute),
			RefreshTtl: durationpb.New(24 * time.Hour),
			Issuer:     "test",
		},
	})
}

func cloneUser(u *biz.User) *biz.User {
	cp := *u
	cp.Password = ""
	return &cp
}

// GetUser implements biz.UserReader.
func (s *Store) GetUser(_ context.Context, id string) (*biz.User, error) {
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	if !ok {
		return nil, biz.ErrUserNotFound
	}
	return cloneUser(u), nil
}

// GetUserByUsername implements biz.UserReader：认证链路专用，出参包含 password
// 哈希 —— 语义对齐 data 层（全部读路径中唯一带出密码的例外）。
func (s *Store) GetUserByUsername(_ context.Context, username string) (*biz.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.Username == username {
			cp := *u
			return &cp, nil
		}
	}
	return nil, biz.ErrUserNotFound
}

// ListUsers implements biz.UserReader.
func (s *Store) ListUsers(_ context.Context, filter biz.UserFilter, page, pageSize int64) ([]*biz.User, int64, error) {
	s.mu.RLock()
	users := make([]*biz.User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	s.mu.RUnlock()

	keyword := strings.ToLower(filter.Keyword)
	matched := make([]*biz.User, 0, len(users))
	for _, u := range users {
		if filter.Status != nil && u.Status != *filter.Status {
			continue
		}
		if keyword != "" &&
			!strings.Contains(strings.ToLower(u.Username), keyword) &&
			!strings.Contains(strings.ToLower(u.Nickname), keyword) &&
			!strings.Contains(strings.ToLower(u.Email), keyword) {
			continue
		}
		matched = append(matched, cloneUser(u))
	}
	slices.SortFunc(matched, func(a, b *biz.User) int {
		if c := b.CreateTime.Compare(a.CreateTime); c != 0 {
			return c
		}
		return strings.Compare(b.ID, a.ID)
	})

	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// CreateUser implements biz.UserWriter.
func (s *Store) CreateUser(_ context.Context, user *biz.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range s.users {
		if u.Username == user.Username {
			return biz.ErrUserExists
		}
		if user.Email != "" && u.Email == user.Email {
			return biz.ErrUserEmailExists
		}
		if user.Phone != "" && u.Phone == user.Phone {
			return biz.ErrUserPhoneExists
		}
	}
	cp := *user
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.users[cp.ID] = &cp
	return nil
}

// UpdateUser implements biz.UserWriter.
func (s *Store) UpdateUser(_ context.Context, id string, upd biz.UserUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[id]
	if !ok {
		return biz.ErrUserNotFound
	}
	if upd.Nickname != nil {
		u.Nickname = *upd.Nickname
	}
	if upd.Email != nil {
		u.Email = *upd.Email
	}
	if upd.Phone != nil {
		u.Phone = *upd.Phone
	}
	if upd.Gender != nil {
		u.Gender = *upd.Gender
	}
	if upd.IsAdmin != nil {
		u.IsAdmin = *upd.IsAdmin
	}
	if upd.Status != nil {
		u.Status = *upd.Status
	}
	if upd.PasswordHash != nil {
		u.Password = *upd.PasswordHash
	}
	u.UpdateTime = time.Now()
	return nil
}

// DeleteUser implements biz.UserWriter：删除用户并级联清空绑定。
func (s *Store) DeleteUser(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[id]; !ok {
		return biz.ErrUserNotFound
	}
	delete(s.users, id)
	delete(s.bindings, id)
	return nil
}

// GetRole implements biz.RoleReader.
func (s *Store) GetRole(_ context.Context, id string) (*biz.Role, error) {
	s.mu.RLock()
	r, ok := s.roles[id]
	s.mu.RUnlock()
	if !ok {
		return nil, biz.ErrRoleNotFound
	}
	cp := *r
	return &cp, nil
}

// ListRoles implements biz.RoleReader.
func (s *Store) ListRoles(_ context.Context, filter biz.RoleFilter, page, pageSize int64) ([]*biz.Role, int64, error) {
	s.mu.RLock()
	roles := make([]*biz.Role, 0, len(s.roles))
	for _, r := range s.roles {
		roles = append(roles, r)
	}
	s.mu.RUnlock()

	keyword := strings.ToLower(filter.Keyword)
	matched := make([]*biz.Role, 0, len(roles))
	for _, r := range roles {
		if filter.Status != nil && r.Status != *filter.Status {
			continue
		}
		if keyword != "" &&
			!strings.Contains(strings.ToLower(r.Name), keyword) &&
			!strings.Contains(strings.ToLower(r.Code), keyword) {
			continue
		}
		cp := *r
		matched = append(matched, &cp)
	}
	slices.SortFunc(matched, func(a, b *biz.Role) int {
		if c := b.CreateTime.Compare(a.CreateTime); c != 0 {
			return c
		}
		return strings.Compare(b.ID, a.ID)
	})

	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// ListRolesByIDs implements biz.RoleReader.
func (s *Store) ListRolesByIDs(_ context.Context, ids []string) ([]*biz.Role, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]struct{}, len(ids))
	roles := make([]*biz.Role, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if r, ok := s.roles[id]; ok {
			cp := *r
			roles = append(roles, &cp)
		}
	}
	return roles, nil
}

// CreateRole implements biz.RoleWriter.
func (s *Store) CreateRole(_ context.Context, role *biz.Role) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.roles {
		if r.Code == role.Code {
			return biz.ErrRoleCodeExists
		}
	}
	cp := *role
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.roles[cp.ID] = &cp
	return nil
}

// UpdateRole implements biz.RoleWriter.
func (s *Store) UpdateRole(_ context.Context, id string, upd biz.RoleUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.roles[id]
	if !ok {
		return biz.ErrRoleNotFound
	}
	if upd.Name != nil {
		r.Name = *upd.Name
	}
	if upd.Status != nil {
		r.Status = *upd.Status
	}
	if upd.BizScope != nil {
		r.BizScope = slices.Clone(*upd.BizScope)
	}
	r.UpdateTime = time.Now()
	return nil
}

// DeleteRole implements biz.RoleWriter：删除角色并级联清空全部用户绑定、
// 直接授权与物化接口授权。
func (s *Store) DeleteRole(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.roles[id]; !ok {
		return biz.ErrRoleNotFound
	}
	delete(s.roles, id)
	delete(s.permissions, id)
	delete(s.grants, id)
	for userID, ids := range s.bindings {
		s.bindings[userID] = slices.DeleteFunc(ids, func(rid string) bool { return rid == id })
	}
	return nil
}

// BindUserRoles implements biz.UserRoleRepo：全量覆盖绑定。
func (s *Store) BindUserRoles(_ context.Context, userID string, roleIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[userID] = slices.Clone(roleIDs)
	return nil
}

// ListUserRoles implements biz.UserRoleRepo：按 code 升序返回。
func (s *Store) ListUserRoles(_ context.Context, userID string) ([]*biz.Role, error) {
	s.mu.RLock()
	ids := slices.Clone(s.bindings[userID])
	roles := make([]*biz.Role, 0, len(ids))
	for _, id := range ids {
		if r, ok := s.roles[id]; ok {
			cp := *r
			roles = append(roles, &cp)
		}
	}
	s.mu.RUnlock()
	slices.SortFunc(roles, func(a, b *biz.Role) int { return strings.Compare(a.Code, b.Code) })
	return roles, nil
}

// FindByHash implements biz.RefreshTokenReader；不存在返回 ErrTokenInvalid。
func (s *Store) FindByHash(_ context.Context, tokenHash string) (*biz.RefreshToken, error) {
	s.mu.RLock()
	t, ok := s.refreshTokens[tokenHash]
	s.mu.RUnlock()
	if !ok {
		return nil, biz.ErrTokenInvalid
	}
	cp := *t
	return &cp, nil
}

// Save implements biz.RefreshTokenWriter：插入新令牌并惰性清理该用户已过期行。
func (s *Store) Save(_ context.Context, token *biz.RefreshToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for hash, t := range s.refreshTokens {
		if t.UserID == token.UserID && t.ExpireTime.Before(now) {
			delete(s.refreshTokens, hash)
		}
	}
	cp := *token
	s.refreshTokens[cp.TokenHash] = &cp
	return nil
}

// Rotate implements biz.RefreshTokenWriter：原子轮换；旧行不存在返回 ErrTokenInvalid。
func (s *Store) Rotate(_ context.Context, oldHash string, next *biz.RefreshToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.refreshTokens[oldHash]; !ok {
		return biz.ErrTokenInvalid
	}
	delete(s.refreshTokens, oldHash)
	cp := *next
	s.refreshTokens[cp.TokenHash] = &cp
	return nil
}

// DeleteByUser implements biz.RefreshTokenWriter：删除该用户全部刷新令牌；幂等。
func (s *Store) DeleteByUser(_ context.Context, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, t := range s.refreshTokens {
		if t.UserID == userID {
			delete(s.refreshTokens, hash)
		}
	}
	return nil
}

// ListPolicies implements biz.PermissionPolicySource：全量快照后解析角色 code，
// 剔除已删除角色；按 (code, operation) 字典序对齐 data 层输出。
func (s *Store) ListPolicies(_ context.Context) ([]biz.PermissionPolicy, error) {
	s.mu.RLock()
	policies := make([]biz.PermissionPolicy, 0, len(s.permissions))
	for roleID, ops := range s.permissions {
		role, ok := s.roles[roleID]
		if !ok {
			continue
		}
		for op := range ops {
			policies = append(policies, biz.PermissionPolicy{RoleCode: role.Code, Operation: op})
		}
	}
	s.mu.RUnlock()
	slices.SortFunc(policies, func(a, b biz.PermissionPolicy) int {
		if c := strings.Compare(a.RoleCode, b.RoleCode); c != 0 {
			return c
		}
		return strings.Compare(a.Operation, b.Operation)
	})
	return policies, nil
}

// cloneGrants 返回 GrantSet 三类切片的深拷贝（Store 内部状态不外泄）。
func cloneGrants(g biz.GrantSet) biz.GrantSet {
	return biz.GrantSet{
		Menus:    slices.Clone(g.Menus),
		Elements: slices.Clone(g.Elements),
		Apis:     slices.Clone(g.Apis),
	}
}

// ListGrantsByRole implements biz.PermissionGrantReader：三类直接授权副本。
func (s *Store) ListGrantsByRole(_ context.Context, roleID string) (biz.GrantSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneGrants(s.grants[roleID]), nil
}

// ListGrantsByRoles implements biz.PermissionGrantReader：多角色直接授权
// 并集（三类各自去重、字典序），对齐 data 层 DISTINCT + ORDER BY 口径。
func (s *Store) ListGrantsByRoles(_ context.Context, roleIDs []string) (biz.GrantSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var union biz.GrantSet
	for _, id := range roleIDs {
		g := s.grants[id]
		union.Menus = append(union.Menus, g.Menus...)
		union.Elements = append(union.Elements, g.Elements...)
		union.Apis = append(union.Apis, g.Apis...)
	}
	return biz.GrantSet{
		Menus:    sortedUnique(union.Menus),
		Elements: sortedUnique(union.Elements),
		Apis:     sortedUnique(union.Apis),
	}, nil
}

// sortedUnique 返回去重后的字典序副本（nil 输入返回 nil，对齐 data 层无行
// 时的 nil 切片）。
func sortedUnique(codes []string) []string {
	if len(codes) == 0 {
		return nil
	}
	out := slices.Clone(codes)
	slices.Sort(out)
	return slices.Compact(out)
}

// ReplaceForRole implements biz.PermissionAdminRepo：同一临界区内全量覆盖
// 直接授权与物化策略（模拟 data 层同事务两表原子一致；事务句柄由用例层
// 传入，内存桩无事务语义，直接生效）。
func (s *Store) ReplaceForRole(_ context.Context, _ txrun.Tx, roleID string, grants biz.GrantSet, operations []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants[roleID] = cloneGrants(grants)
	next := make(map[string]struct{}, len(operations))
	for _, op := range operations {
		next[op] = struct{}{}
	}
	s.permissions[roleID] = next
	return nil
}

// NewAuthorizer 构造测试用授权器：策略来自 store，ttl 可控（测试惰性刷新
// 时可传极小值）；日志丢弃，断言关注行为不关注日志。
func NewAuthorizer(store *Store, ttl time.Duration) (*biz.Authorizer, error) {
	return biz.NewAuthorizer(store, &conf.Security{
		Authz: &conf.Security_Authz{PolicyTtl: durationpb.New(ttl)},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// GetConfig implements biz.SystemConfigRepo：未配置（nil）返回零值配置。
func (s *Store) GetConfig(_ context.Context) (*biz.SystemConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.systemConfig == nil {
		return &biz.SystemConfig{}, nil
	}
	cp := *s.systemConfig
	return &cp, nil
}

// SaveConfig implements biz.SystemConfigRepo：单行覆盖保存。
func (s *Store) SaveConfig(_ context.Context, cfg *biz.SystemConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *cfg
	s.systemConfig = &cp
	return nil
}

// SaveLogoFile implements biz.SystemConfigRepo：内存覆盖保存 logo 文件。
func (s *Store) SaveLogoFile(_ context.Context, data []byte, ext string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logoFile = append([]byte(nil), data...)
	s.logoMime = map[string]string{
		".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml",
		".webp": "image/webp", ".ico": "image/x-icon",
	}[ext]
	return biz.LogoVersion(data), nil
}

// ReadLogoFile implements biz.SystemConfigRepo：未上传（nil）返回 ErrLogoNotFound。
func (s *Store) ReadLogoFile(_ context.Context) ([]byte, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.logoFile == nil {
		return nil, "", biz.ErrLogoNotFound
	}
	return append([]byte(nil), s.logoFile...), s.logoMime, nil
}

// Create implements biz.LoginLogWriter：clone 后追加（append-only，无删除/更新）。
func (s *Store) Create(_ context.Context, log *biz.LoginLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *log
	if cp.CreateTime.IsZero() {
		cp.CreateTime = time.Now().UTC()
	}
	s.loginLogs = append(s.loginLogs, &cp)
	return nil
}

// ListLoginLogs implements biz.LoginLogReader：快照后过滤 + 排序 + 切片分页，
// 语义对齐 data 层（ILIKE ≈ 忽略大小写包含匹配，时间范围含端点）。
func (s *Store) ListLoginLogs(_ context.Context, filter biz.LoginLogFilter, page, pageSize int64) ([]*biz.LoginLog, int64, error) {
	s.mu.RLock()
	logs := make([]*biz.LoginLog, 0, len(s.loginLogs))
	logs = append(logs, s.loginLogs...)
	s.mu.RUnlock()

	username := strings.ToLower(filter.Username)
	matched := make([]*biz.LoginLog, 0, len(logs))
	for _, l := range logs {
		if filter.Result != nil && l.Result != *filter.Result {
			continue
		}
		if username != "" && !strings.Contains(strings.ToLower(l.Username), username) {
			continue
		}
		if filter.StartTime != nil && l.CreateTime.Before(*filter.StartTime) {
			continue
		}
		if filter.EndTime != nil && l.CreateTime.After(*filter.EndTime) {
			continue
		}
		cp := *l
		matched = append(matched, &cp)
	}
	slices.SortFunc(matched, func(a, b *biz.LoginLog) int {
		if c := b.CreateTime.Compare(a.CreateTime); c != 0 {
			return c
		}
		return strings.Compare(b.ID, a.ID)
	})

	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}
