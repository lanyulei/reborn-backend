package kube

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// 原地调整运行中 Pod 的容器资源（KEP-1287）：Pod 的 spec 不允许直接改 resources，只能经 resize 子资源。
// 集群只是「接受」调整，实际生效由 kubelet 异步完成（Pod 的 PodResizePending / PodResizeInProgress 状况）。

// resizeSubresource Pod 的原地调整子资源（Kubernetes 1.33 起默认提供）。
const resizeSubresource = "resize"

// resizeChange 一个容器的一项资源调整。
type resizeChange struct {
	container string
	listField string // containers / initContainers
	kind      string // requests / limits
	resource  string // cpu / memory
	before    string // 空 = 原先未设置
	after     string
}

// path 该项在 Pod 里的字段路径（审批期间被改动即冲突）。
func (c resizeChange) path() []string {
	return []string{"spec", c.listField, "name=" + c.container, "resources", c.kind, c.resource}
}

// resizeItems 可调整的资源项，按展示顺序。
var resizeItems = [...]struct{ kind, resource, label string }{
	{"requests", "cpu", "CPU 请求"},
	{"limits", "cpu", "CPU 上限"},
	{"requests", "memory", "内存请求"},
	{"limits", "memory", "内存上限"},
}

// resizeContainer 在 Pod 里找到可调整的容器：普通容器，或 restartPolicy 为 Always 的初始化容器（边车）。
func resizeContainer(pod map[string]any, name string) (map[string]any, string, error) {
	for _, c := range nestedList(pod, "spec", "containers") {
		if m := asMap(c); str(m, "name") == name {
			return m, "containers", nil
		}
	}
	for _, c := range nestedList(pod, "spec", "initContainers") {
		m := asMap(c)
		if str(m, "name") != name {
			continue
		}
		if str(m, "restartPolicy") != "Always" {
			return nil, "", biz.ErrInvalid("container %q is an init container; only regular and sidecar (restartPolicy: Always) containers can be resized", name)
		}
		return m, "initContainers", nil
	}
	return nil, "", biz.ErrInvalid("container %q not found in %s", name, str(pod, "metadata", "name"))
}

// resizeQuantity 解析请求里的一个数量：nil = 不改（返回 nil）；空串、非法或负数拒绝。
func resizeQuantity(container, kind, res string, v *string) (*resource.Quantity, error) {
	if v == nil {
		return nil, nil
	}
	if *v == "" {
		return nil, biz.ErrInvalid("container %q: %s.%s must not be empty (omit it to keep the current value)", container, kind, res)
	}
	q, err := resource.ParseQuantity(*v)
	if err != nil || q.Sign() < 0 {
		return nil, biz.ErrInvalid("container %q: %s.%s %q is not a valid quantity", container, kind, res, *v)
	}
	return &q, nil
}

// planResize 校验请求并与 Pod 现状比对，得出实际要改的资源项（顺序确定：按请求里的容器顺序，再按 resizeItems）。
// 每种资源在合并现状之后须满足 请求 ≤ 上限；其余规则（QoS 等级不可变等）由集群校验。
func planResize(pod map[string]any, req k8scontract.ResizeRequest) ([]resizeChange, error) {
	if len(req.Containers) == 0 {
		return nil, biz.ErrInvalid("containers are required")
	}
	var out []resizeChange
	seen := make(map[string]struct{}, len(req.Containers))
	for _, c := range req.Containers {
		if c.Name == "" {
			return nil, biz.ErrInvalid("container name is required")
		}
		if _, dup := seen[c.Name]; dup {
			return nil, biz.ErrInvalid("container %q is listed more than once", c.Name)
		}
		seen[c.Name] = struct{}{}
		cur, listField, err := resizeContainer(pod, c.Name)
		if err != nil {
			return nil, err
		}
		want := map[string]*string{}
		if c.Requests != nil {
			want["requests.cpu"], want["requests.memory"] = c.Requests.CPU, c.Requests.Memory
		}
		if c.Limits != nil {
			want["limits.cpu"], want["limits.memory"] = c.Limits.CPU, c.Limits.Memory
		}
		effective := map[string]*resource.Quantity{}
		for _, it := range resizeItems {
			key := it.kind + "." + it.resource
			next, err := resizeQuantity(c.Name, it.kind, it.resource, want[key])
			if err != nil {
				return nil, err
			}
			before := str(cur, "resources", it.kind, it.resource)
			var was *resource.Quantity
			if q, perr := resource.ParseQuantity(before); before != "" && perr == nil {
				was = &q
			}
			effective[key] = was
			if next == nil {
				continue
			}
			effective[key] = next
			if was == nil || was.Cmp(*next) != 0 {
				out = append(out, resizeChange{
					container: c.Name, listField: listField, kind: it.kind, resource: it.resource, before: before, after: next.String(),
				})
			}
		}
		for _, res := range []string{"cpu", "memory"} {
			r, l := effective["requests."+res], effective["limits."+res]
			if r != nil && l != nil && r.Cmp(*l) > 0 {
				return nil, biz.ErrInvalid("container %q: %s request %s exceeds its limit %s", c.Name, res, r.String(), l.String())
			}
		}
	}
	if len(out) == 0 {
		return nil, biz.ErrInvalid("the request does not change any container resources")
	}
	return out, nil
}

// resizePatch 策略合并补丁：容器列表按名称合并，resources 里只带要改的项，其余字段与其余容器都不动。
func resizePatch(changes []resizeChange) map[string]any {
	spec := map[string]any{}
	items := map[string]map[string]any{} // listField/容器名 → 补丁里的容器项
	for _, c := range changes {
		key := c.listField + "/" + c.container
		item, ok := items[key]
		if !ok {
			item = map[string]any{"name": c.container, "resources": map[string]any{}}
			items[key] = item
			list, _ := spec[c.listField].([]any)
			spec[c.listField] = append(list, item)
		}
		res := asMap(item["resources"])
		kind, ok := res[c.kind].(map[string]any)
		if !ok {
			kind = map[string]any{}
			res[c.kind] = kind
		}
		kind[c.resource] = c.after
	}
	return map[string]any{"spec": spec}
}

// resizeSummary 一句话说明，如「调整资源 app: CPU 请求 100m→200m / 内存上限 256Mi→512Mi」；多个容器以分号分隔。
func resizeSummary(changes []resizeChange) string {
	var b strings.Builder
	b.WriteString("调整资源 ")
	last := ""
	for _, c := range changes {
		switch {
		case last == "":
			b.WriteString(c.container + ": ")
		case last != c.container:
			b.WriteString("；" + c.container + ": ")
		default:
			b.WriteString(" / ")
		}
		last = c.container
		before := c.before
		if before == "" {
			before = "未设置"
		}
		for _, it := range resizeItems {
			if it.kind == c.kind && it.resource == c.resource {
				b.WriteString(it.label + " " + before + "→" + c.after)
			}
		}
	}
	return b.String()
}

// resizeParams 审计里的动作参数：每项调整的容器、字段与前后值。
func resizeParams(changes []resizeChange) []map[string]any {
	out := make([]map[string]any, 0, len(changes))
	for _, c := range changes {
		out = append(out, map[string]any{"container": c.container, "field": c.kind + "." + c.resource, "from": c.before, "to": c.after})
	}
	return out
}

// resizeError 集群不提供 resize 子资源（旧版本）时给出明确说明；其余错误原样返回。
// 调用前刚读到过这个 Pod，所以 404 只有两种：Pod 恰好被删（集群返回带对象名的 NotFound），
// 或路径本身不存在——后者的响应不带对象名，或根本不是集群的 Status（客户端据此标记为 UnexpectedServerResponse）。
func resizeError(err error) error {
	var st apierrors.APIStatus
	if !errors.As(err, &st) || st.Status().Code != http.StatusNotFound {
		return err
	}
	if d := st.Status().Details; d != nil && d.Name != "" && !apierrors.IsUnexpectedServerError(err) {
		return err
	}
	return biz.NewError(http.StatusUnprocessableEntity, k8scontract.ReasonRejected,
		"this cluster does not serve the pod resize subresource: in-place resize needs Kubernetes 1.33+")
}

// resize 以用户身份经 resize 子资源调整 Pod 的容器资源（能否调整由集群判定：pods/resize 的 patch 权限）。
func (u *userClients) resize(req k8scontract.ResizeRequest) ([]*biz.AuditEvent, error) {
	ref := podRef(req.Ref.Namespace, req.Ref.Name)
	audit := &biz.AuditEvent{Ref: ref, Kind: "Pod", Action: k8scontract.ActionResize, Patch: req.Containers}
	audits := []*biz.AuditEvent{audit}
	fail := func(err error) ([]*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return audits, err
	}
	if ref.Namespace == "" || ref.Name == "" {
		return fail(biz.ErrInvalid("namespace and name are required"))
	}
	rc, _, err := u.resource(ref)
	if err != nil {
		return fail(err)
	}
	cur, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return fail(err)
	}
	changes, err := planResize(cur.Object, req)
	if err != nil {
		return fail(err)
	}
	audit.Patch, audit.Before = resizeParams(changes), cur.Object
	raw, err := json.Marshal(resizePatch(changes))
	if err != nil {
		return fail(err)
	}
	after, err := rc.Patch(u.ctx, ref.Name, types.StrategicMergePatchType, raw, metav1.PatchOptions{}, resizeSubresource)
	if err != nil {
		return fail(resizeError(err))
	}
	audit.After = after.Object
	return audits, nil
}
