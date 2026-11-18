package kube

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var managedLabels = map[string]string{k8scontract.ManagedByLabel: k8scontract.ManagedByValue}

// clusterViewRules reborn:cluster-view 的规则：集群级资源只读（内置的 view 不含它们）。
var clusterViewRules = []rbacv1.PolicyRule{
	{APIGroups: []string{""}, Resources: []string{"nodes", "namespaces", "persistentvolumes"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"storageclasses"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{"apiextensions.k8s.io"}, Resources: []string{"customresourcedefinitions"}, Verbs: []string{"get", "list", "watch"}},
}

// debugRules reborn:debug 的规则：给 Pod 添加临时容器（内置的 edit / admin 不含这个子资源）。
var debugRules = []rbacv1.PolicyRule{
	{APIGroups: []string{""}, Resources: []string{"pods/ephemeralcontainers"}, Verbs: []string{"get", "update", "patch"}},
}

// nsAdminRules 由内置 admin 的规则派生 reborn:ns-admin 的规则：去掉 escalate / bind / impersonate，
// RBAC 组（含通配组）只保留读。命名空间管理员因此不能自建绑定——否则他可以把权限绑给自己的平台用户名，
// 平台收回授权后依然有效。
func nsAdminRules(admin []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	out := make([]rbacv1.PolicyRule, 0, len(admin))
	for _, r := range admin {
		rbacGroup := slices.Contains(r.APIGroups, rbacv1.GroupName) || slices.Contains(r.APIGroups, rbacv1.APIGroupAll)
		var verbs []string
		for _, v := range r.Verbs {
			switch v {
			case "escalate", "bind", "impersonate":
				continue
			case "get", "list", "watch":
				verbs = append(verbs, v)
			case rbacv1.VerbAll:
				if rbacGroup {
					verbs = append(verbs, "get", "list", "watch")
				} else {
					// 通配动词含 escalate / bind / impersonate：展开为常规动词。
					verbs = append(verbs, "get", "list", "watch", "create", "update", "patch", "delete", "deletecollection")
				}
			default:
				if !rbacGroup {
					verbs = append(verbs, v)
				}
			}
		}
		if len(verbs) == 0 {
			continue
		}
		nr := *r.DeepCopy()
		nr.Verbs = verbs
		out = append(out, nr)
	}
	return out
}

// agentBindingName 集群内 Agent 的服务账号绑定（安装清单创建，引用 cluster-admin）。它也用 reborn: 前缀并带
// 平台标签，但不属于授权对账：对账一旦把它当作「多余的绑定」删掉，Agent 就失去权限、集群随之失联。
const agentBindingName = "reborn:agent"

// platformOwned 报告绑定是否归授权对账管理：名称以 reborn: 开头（平台的保留命名），Agent 自己的绑定除外。
// 不以标签为准：标签可以被有权编辑该对象的人摘掉（摘掉后就永远删不掉），也可以被别人贴到无关对象上。
func platformOwned(name string) bool {
	return strings.HasPrefix(name, k8scontract.BindingPrefix) && name != agentBindingName
}

// platformSubject 报告主体是否是平台在集群内使用的用户 / 分组（reborn:<用户>、reborn:role:<角色>、reborn:admin）。
func platformSubject(s rbacv1.Subject) bool {
	return (s.Kind == rbacv1.UserKind || s.Kind == rbacv1.GroupKind) && strings.HasPrefix(s.Name, k8scontract.BindingPrefix)
}

func anyPlatformSubject(subjects []rbacv1.Subject) bool {
	return slices.ContainsFunc(subjects, platformSubject)
}

// EnsureClusterView implements biz.RBAC：确保平台管理的 ClusterRole（reborn:cluster-view、reborn:debug、
// reborn:ns-admin）存在且规则正确。
func (m *Manager) EnsureClusterView(ctx context.Context, clusterID string) error {
	c, err := m.conn(clusterID)
	if err != nil {
		return err
	}
	if err := c.ensureClusterRole(ctx, k8scontract.ClusterViewRole, clusterViewRules); err != nil {
		return err
	}
	if err := c.ensureClusterRole(ctx, k8scontract.DebugRole, debugRules); err != nil {
		return err
	}
	admin, err := c.platKube.RbacV1().ClusterRoles().Get(ctx, "admin", metav1.GetOptions{})
	if err != nil {
		return mapError(err)
	}
	return c.ensureClusterRole(ctx, biz.NamespaceAdminRole, nsAdminRules(admin.Rules))
}

func (c *conn) ensureClusterRole(ctx context.Context, name string, rules []rbacv1.PolicyRule) error {
	roles := c.platKube.RbacV1().ClusterRoles()
	cur, err := roles.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = roles.Create(ctx, &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: managedLabels},
			Rules:      rules,
		}, metav1.CreateOptions{})
		return mapError(err)
	}
	if err != nil {
		return mapError(err)
	}
	// reborn: 前缀是平台的保留命名：同名对象一律接管（补回标签、改正规则、去掉聚合规则），
	// 否则一个被摘掉标签或被人预先创建的同名对象会让授权对账永远失败。
	managed := cur.Labels[k8scontract.ManagedByLabel] == k8scontract.ManagedByValue
	if managed && cur.AggregationRule == nil && reflect.DeepEqual(cur.Rules, rules) {
		return nil
	}
	if cur.Labels == nil {
		cur.Labels = map[string]string{}
	}
	cur.Labels[k8scontract.ManagedByLabel] = k8scontract.ManagedByValue
	cur.AggregationRule = nil
	cur.Rules = rules
	_, err = roles.Update(ctx, cur, metav1.UpdateOptions{})
	return mapError(err)
}

func bindingOf(namespace, name string, subjects []rbacv1.Subject, ref rbacv1.RoleRef) biz.Binding {
	b := biz.Binding{Namespace: namespace, Name: name, Role: ref.Name}
	// 形态不符（多主体、非分组主体、引用的不是 ClusterRole）的绑定留空 Group：对账会把它替换成正确形态。
	if len(subjects) == 1 && subjects[0].Kind == rbacv1.GroupKind && ref.Kind == "ClusterRole" {
		b.Group = subjects[0].Name
	}
	return b
}

// rbacListChunk 列出绑定时的分页大小（全量列出，不带标签选择器）。
const rbacListChunk = 500

// ListBindings implements biz.RBAC：列出全部绑定，按「带平台标签或名称以 reborn: 开头」认定归平台管理；
// 其余绑定里主体引用了平台用户 / 分组的，作为 foreign 返回（命名空间/名称，集群级为 /名称）。
func (m *Manager) ListBindings(ctx context.Context, clusterID string) ([]biz.Binding, []string, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return nil, nil, err
	}
	var (
		out     []biz.Binding
		foreign []string
	)
	opts := metav1.ListOptions{Limit: rbacListChunk}
	for {
		crbs, err := c.platKube.RbacV1().ClusterRoleBindings().List(ctx, opts)
		if err != nil {
			return nil, nil, mapError(err)
		}
		for i := range crbs.Items {
			b := &crbs.Items[i]
			switch {
			case platformOwned(b.Name):
				out = append(out, bindingOf("", b.Name, b.Subjects, b.RoleRef))
			case anyPlatformSubject(b.Subjects):
				foreign = append(foreign, "/"+b.Name)
			}
		}
		if opts.Continue = crbs.Continue; opts.Continue == "" {
			break
		}
	}
	opts = metav1.ListOptions{Limit: rbacListChunk}
	for {
		rbs, err := c.platKube.RbacV1().RoleBindings(metav1.NamespaceAll).List(ctx, opts)
		if err != nil {
			return nil, nil, mapError(err)
		}
		for i := range rbs.Items {
			b := &rbs.Items[i]
			switch {
			case platformOwned(b.Name):
				out = append(out, bindingOf(b.Namespace, b.Name, b.Subjects, b.RoleRef))
			case anyPlatformSubject(b.Subjects):
				foreign = append(foreign, b.Namespace+"/"+b.Name)
			}
		}
		if opts.Continue = rbs.Continue; opts.Continue == "" {
			break
		}
	}
	return out, foreign, nil
}

// namespaceGone 报告创建失败是否因为命名空间不存在或正在删除：下一轮对账会跳过它，不算失败。
func namespaceGone(err error) bool {
	return apierrors.IsNotFound(err) || apierrors.HasStatusCause(err, "NamespaceTerminating")
}

// ApplyBindings implements biz.RBAC。roleRef 不可修改，所以「替换」= 删除后重建。先删后建（收回优先于
// 授予）；单个对象失败不影响其余，错误合并返回——否则一个建不出来的绑定会让该集群此后的授权都不生效。
func (m *Manager) ApplyBindings(ctx context.Context, clusterID string, upsert, remove []biz.Binding) error {
	c, err := m.conn(clusterID)
	if err != nil {
		return err
	}
	var errs []error
	for _, b := range remove {
		if err := c.deleteBinding(ctx, b); err != nil {
			errs = append(errs, err)
		}
	}
	for _, b := range upsert {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		if err := c.deleteBinding(ctx, b); err != nil {
			errs = append(errs, err)
			continue
		}
		meta := metav1.ObjectMeta{Name: b.Name, Labels: managedLabels}
		subjects := []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: b.Group}}
		ref := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: b.Role}
		if b.Namespace == "" {
			_, err = c.platKube.RbacV1().ClusterRoleBindings().Create(ctx,
				&rbacv1.ClusterRoleBinding{ObjectMeta: meta, Subjects: subjects, RoleRef: ref}, metav1.CreateOptions{})
		} else {
			_, err = c.platKube.RbacV1().RoleBindings(b.Namespace).Create(ctx,
				&rbacv1.RoleBinding{ObjectMeta: meta, Subjects: subjects, RoleRef: ref}, metav1.CreateOptions{})
		}
		if err != nil && (b.Namespace == "" || !namespaceGone(err)) {
			errs = append(errs, mapError(err))
		}
	}
	return errors.Join(errs...)
}

func (c *conn) deleteBinding(ctx context.Context, b biz.Binding) error {
	// 名称以 reborn: 开头的绑定归平台管理（见 platformOwned）；其余名称平台从不创建，也就从不删除。
	if !platformOwned(b.Name) {
		return biz.NewError(409, k8scontract.ReasonConflict, "binding %s/%s is not managed by the platform", b.Namespace, b.Name)
	}
	var err error
	if b.Namespace == "" {
		err = c.platKube.RbacV1().ClusterRoleBindings().Delete(ctx, b.Name, metav1.DeleteOptions{})
	} else {
		err = c.platKube.RbacV1().RoleBindings(b.Namespace).Delete(ctx, b.Name, metav1.DeleteOptions{})
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	return mapError(err)
}
