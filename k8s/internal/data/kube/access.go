package kube

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// accessTTL 权限询问结果的缓存时长。
const accessTTL = 30 * time.Second

type accessEntry struct {
	allowed bool
	expire  time.Time
}

// accessCache 权限询问（SubjectAccessReview）结果缓存。键含用户的分组集合：角色变化即换键。
type accessCache struct {
	mu sync.Mutex
	m  map[string]accessEntry
	// gen 每次 clear 加一：清空之前发出的询问不得把结果写回。
	gen uint64
}

func newAccessCache() *accessCache { return &accessCache{m: map[string]accessEntry{}} }

func (a *accessCache) clear() {
	a.mu.Lock()
	a.m = map[string]accessEntry{}
	a.gen++ // 清空之前发出、之后才返回的询问结果不得写回（那是收回授权之前的答案）
	a.mu.Unlock()
}

func accessKey(id biz.Identity, namespace string, gvr k8scontract.GVR, verb string) string {
	groups := slices.Clone(id.RoleIDs)
	slices.Sort(groups)
	admin := "0"
	if id.IsAdmin {
		admin = "1"
	}
	// 键里带用户 ID：集群内可能存在直接授予某个用户的绑定（非平台管理），结果不能跨用户复用。
	return strings.Join([]string{id.UserID, admin, strings.Join(groups, ","), namespace, gvr.Group, gvr.Resource, verb}, "|")
}

// allowed 以平台身份向集群询问「该用户能否在此命名空间对此资源执行 verb」。判定权在集群：
// 平台不解释数据范围，只转述集群的回答。
func (c *conn) allowed(ctx context.Context, id biz.Identity, namespace string, gvr k8scontract.GVR, verb string) (bool, error) {
	if err := validIdentity(id); err != nil {
		return false, err
	}
	key := accessKey(id, namespace, gvr, verb)
	now := time.Now()
	c.access.mu.Lock()
	if e, ok := c.access.m[key]; ok && now.Before(e.expire) {
		c.access.mu.Unlock()
		return e.allowed, nil
	}
	gen := c.access.gen
	c.access.mu.Unlock()

	sar := &authv1.SubjectAccessReview{Spec: authv1.SubjectAccessReviewSpec{
		User:   k8scontract.ImpersonateUser(id.UserID),
		Groups: k8scontract.ImpersonateGroups(id.RoleIDs, id.IsAdmin),
		ResourceAttributes: &authv1.ResourceAttributes{
			Namespace: namespace, Verb: verb, Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		},
	}}
	out, err := c.platKube.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return false, mapError(err)
	}
	ok := out.Status.Allowed && !out.Status.Denied
	c.access.mu.Lock()
	if len(c.access.m) > 50000 { // 上限保护：直接清空，30 秒内重新填充
		c.access.m = map[string]accessEntry{}
	}
	if c.access.gen == gen {
		c.access.m[key] = accessEntry{allowed: ok, expire: now.Add(accessTTL)}
	}
	c.access.mu.Unlock()
	return ok, nil
}

// scopedNamespaces 用户的数据范围里显式列出的命名空间（不含「全部命名空间」的授权）。
func (c *conn) scopedNamespaces(ctx context.Context, id biz.Identity) ([]string, error) {
	scopes, err := c.m.scopes.Scopes(ctx, c.cluster.ID)
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var out []string
	for _, s := range scopes {
		if s.Namespace == k8scontract.AllNamespaces || !slices.Contains(id.RoleIDs, s.RoleID) {
			continue
		}
		if _, ok := seen[s.Namespace]; !ok {
			seen[s.Namespace] = struct{}{}
			out = append(out, s.Namespace)
		}
	}
	slices.Sort(out)
	return out, nil
}

// readableNamespaces 判定一次列表 / 订阅可以读哪些命名空间。
//   - all = true：整个请求范围都可读（namespaces 为 nil 表示不按命名空间过滤）；
//   - all = false：只可读返回的这些命名空间（请求全部命名空间，但用户只有部分权限）。
//
// 一个都不可读返回 403。
func (c *conn) readableNamespaces(ctx context.Context, id biz.Identity, gvr k8scontract.GVR, namespaced bool, namespace string) (namespaces []string, all bool, err error) {
	if !namespaced {
		namespace = ""
	}
	ok, err := c.allowed(ctx, id, namespace, gvr, "list")
	if err != nil {
		return nil, false, err
	}
	if ok {
		if namespace != "" {
			return []string{namespace}, true, nil
		}
		return nil, true, nil
	}
	if !namespaced || namespace != "" {
		return nil, false, biz.ErrForbidden("you are not allowed to list %s here", gvr.Resource)
	}
	scoped, err := c.scopedNamespaces(ctx, id)
	if err != nil {
		return nil, false, err
	}
	for _, ns := range scoped {
		ok, err := c.allowed(ctx, id, ns, gvr, "list")
		if err != nil {
			return nil, false, err
		}
		if ok {
			namespaces = append(namespaces, ns)
		}
	}
	if len(namespaces) == 0 {
		return nil, false, biz.ErrForbidden("you are not allowed to list %s in any namespace", gvr.Resource)
	}
	return namespaces, false, nil
}
