package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/yaml"
)

func decode[T any](body []byte) (T, error) {
	var v T
	if len(body) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return v, biz.ErrInvalid("malformed request body: %s", err.Error())
	}
	return v, nil
}

// Execute implements biz.Executor.
func (m *Manager) Execute(ctx context.Context, cl *biz.Cluster, id biz.Identity, op string, body []byte) (any, []*biz.AuditEvent, error) {
	c, err := m.conn(cl.ID)
	if err != nil {
		return nil, nil, err
	}
	u, err := c.asUser(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	resp, audits, err := u.dispatch(op, body)
	return resp, audits, mapError(err)
}

func (u *userClients) dispatch(op string, body []byte) (any, []*biz.AuditEvent, error) {
	if isAppsOp(op) {
		return u.dispatchApps(op, body)
	}
	switch op {
	case k8scontract.OpAPIResources:
		req, err := decode[k8scontract.APIResourcesRequest](body)
		if err != nil {
			return nil, nil, err
		}
		d, err := u.c.apiResources(u.ctx, req.Refresh)
		if err != nil {
			return nil, nil, err
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		return k8scontract.APIResourcesResponse{Items: d.items}, nil, nil
	case k8scontract.OpNamespaces:
		r, err := u.namespaces()
		return r, nil, err
	case k8scontract.OpList:
		req, err := decode[k8scontract.ListRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.list(req)
		return r, nil, err
	case k8scontract.OpGet:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.get(req.Ref)
		return r, nil, err
	case k8scontract.OpEvents:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.events(req.Ref)
		return r, nil, err
	case k8scontract.OpRevisions:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.revisions(req.Ref)
		return r, nil, err
	case k8scontract.OpDrainPreview:
		req, err := decode[k8scontract.DrainRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.drainPreview(req)
		return r, nil, err
	}
	if op == k8scontract.OpChangePlan {
		req, err := decode[k8scontract.ChangePlanRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.changePlan(req)
		return r, nil, err
	}
	if resp, handled, err := u.dispatchInsight(op, body); handled {
		return resp, nil, err
	}
	return u.dispatchWrite(op, body)
}

// namespaces 命名空间切换器的可选项：在全部命名空间都有读权限（以 Pod 的 list 权限为准）的用户返回全部；
// 否则只返回其数据范围内且集群确认可读的命名空间。不以「能否列出命名空间对象」为准：任何有授权的
// 角色都绑定了 reborn:cluster-view，按它判断会把无权访问的命名空间名称也暴露出来。
func (u *userClients) namespaces() (k8scontract.NamespacesResponse, error) {
	out := k8scontract.NamespacesResponse{Items: []string{}}
	all, err := u.c.allowed(u.ctx, u.id, "", k8scontract.GVRPod, "list")
	if err != nil {
		return out, err
	}
	if all {
		e, err := u.c.cache.get(u.ctx, k8scontract.GVRNamespace)
		if err != nil {
			return out, err
		}
		for _, en := range e.store.snapshot(nil) {
			out.Items = append(out.Items, en.Row.Name)
		}
		sort.Strings(out.Items)
		out.All = true
		return out, nil
	}
	scoped, err := u.c.scopedNamespaces(u.ctx, u.id)
	if err != nil {
		return out, err
	}
	for _, ns := range scoped {
		ok, err := u.c.allowed(u.ctx, u.id, ns, k8scontract.GVRPod, "list")
		if err != nil {
			return out, err
		}
		if ok {
			out.Items = append(out.Items, ns)
		}
	}
	return out, nil
}

func (u *userClients) list(req k8scontract.ListRequest) (k8scontract.ListResponse, error) {
	gvr := req.GVR()
	res, err := u.c.resolve(u.ctx, gvr)
	if err != nil {
		return k8scontract.ListResponse{}, err
	}
	if !res.Namespaced {
		req.Namespace = ""
	}
	// 命名空间会拼进请求路径（未缓存的资源）：不合法的取值（含 / 或 ..）能把列表请求改写到别的 API 路径上。
	if req.Namespace != "" && !nsPattern.MatchString(req.Namespace) {
		return k8scontract.ListResponse{}, biz.ErrInvalid("invalid namespace")
	}
	if k8scontract.Cached(gvr) {
		return u.listCached(req, res)
	}
	return u.listTable(req, res)
}

// cachedFilters 一种缓存资源支持的筛选：有状态列的才能按状态筛选（没有状态列的资源，行的状态恒为空，
// 选任何状态都只会得到空列表）；Pod 另可按所在节点筛选。
func cachedFilters(gvr k8scontract.GVR) []string {
	filters := make([]string, 0, 3)
	if gvr == k8scontract.GVRPod {
		filters = append(filters, k8scontract.FilterNode)
	}
	if slices.ContainsFunc(views[gvr].columns, func(c k8scontract.Column) bool { return c.Type == k8scontract.ColState }) {
		filters = append(filters, k8scontract.FilterState)
	}
	return append(filters, k8scontract.FilterOwner)
}

func (u *userClients) listCached(req k8scontract.ListRequest, res k8scontract.APIResource) (k8scontract.ListResponse, error) {
	gvr := req.GVR()
	out := k8scontract.ListResponse{Rows: []k8scontract.Row{}, Columns: views[gvr].columns}
	out.Capabilities = k8scontract.Capabilities{Cached: true, Keyword: true, Sort: true, Total: true, Watch: true, Filters: cachedFilters(gvr)}
	// 先问集群，再碰缓存：无权限的请求不会触发缓存建立，也读不到缓存里的任何内容。
	namespaces, all, err := u.c.readableNamespaces(u.ctx, u.id, gvr, res.Namespaced, req.Namespace)
	if err != nil {
		return out, err
	}
	q := biz.ListQuery{
		Keyword: req.Keyword, State: req.Filters[k8scontract.FilterState], Node: req.Filters[k8scontract.FilterNode],
		Owner: req.Filters[k8scontract.FilterOwner], SortBy: req.SortBy, SortDesc: req.SortDesc, Page: req.Page, PageSize: req.PageSize,
	}
	if strings.TrimSpace(req.LabelSelector) != "" {
		sel, err := labels.Parse(req.LabelSelector)
		if err != nil {
			return out, biz.ErrInvalid("invalid label selector: %s", err.Error())
		}
		q.Match = func(l map[string]string) bool { return sel.Matches(labels.Set(l)) }
	}
	e, err := u.c.cache.get(u.ctx, gvr)
	if err != nil {
		return out, err
	}
	page, total := biz.SelectEntries(e.store.snapshot(namespaces), q)
	for _, en := range page {
		out.Rows = append(out.Rows, en.Row)
	}
	out.Total = int64(total)
	if !all {
		out.PartialNamespaces = namespaces
	}
	return out, nil
}

// tableAccept 请求表格格式（与 kubectl get 同源的列）。
const tableAccept = "application/json;as=Table;v=v1;g=meta.k8s.io,application/json"

// listTable 未缓存的资源与 CRD：以用户身份直接查集群，分页用集群的 limit / continue，只支持标签筛选。
func (u *userClients) listTable(req k8scontract.ListRequest, res k8scontract.APIResource) (k8scontract.ListResponse, error) {
	gvr := req.GVR()
	out := k8scontract.ListResponse{Rows: []k8scontract.Row{}, Columns: []k8scontract.Column{}, Total: -1}
	size := req.PageSize
	if size <= 0 {
		size = biz.DefaultPageSize
	}
	if size > biz.MaxPageSize {
		size = biz.MaxPageSize
	}
	path := "/api/" + gvr.Version
	if gvr.Group != "" {
		path = "/apis/" + gvr.Group + "/" + gvr.Version
	}
	if res.Namespaced && req.Namespace != "" {
		path += "/namespaces/" + req.Namespace
	}
	r := u.kube.Discovery().RESTClient().Get().AbsPath(path+"/"+gvr.Resource).
		SetHeader("Accept", tableAccept).
		Param("limit", fmt.Sprint(size)).Param("includeObject", "Metadata")
	if req.LabelSelector != "" {
		r = r.Param("labelSelector", req.LabelSelector)
	}
	if req.Continue != "" {
		r = r.Param("continue", req.Continue)
	}
	raw, err := r.DoRaw(u.ctx)
	if err != nil {
		return out, err
	}
	var table struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		ColumnDefinitions []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Priority int    `json:"priority"`
		} `json:"columnDefinitions"`
		Rows []struct {
			Cells  []any          `json:"cells"`
			Object map[string]any `json:"object"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		return out, biz.NewError(502, k8scontract.ReasonUpstream, "cluster returned an unexpected list format")
	}
	keys := make([]string, len(table.ColumnDefinitions))
	for i, cd := range table.ColumnDefinitions {
		lower := strings.ToLower(cd.Name)
		if lower == "name" || lower == "age" { // 名称与创建时间是行的固定字段
			continue
		}
		keys[i] = cd.Name
		typ := k8scontract.ColText
		if cd.Type == "integer" || cd.Type == "number" {
			typ = k8scontract.ColNumber
		}
		out.Columns = append(out.Columns, k8scontract.Column{Key: cd.Name, Title: cd.Name, Type: typ, Priority: cd.Priority})
	}
	for _, tr := range table.Rows {
		row := k8scontract.Row{
			Namespace: str(tr.Object, "metadata", "namespace"), Name: str(tr.Object, "metadata", "name"),
			UID: str(tr.Object, "metadata", "uid"), Cells: map[string]any{},
		}
		row.CreateTime, _ = timeAt(tr.Object, "metadata", "creationTimestamp")
		for i, cell := range tr.Cells {
			if i < len(keys) && keys[i] != "" {
				row.Cells[keys[i]] = cell
			}
		}
		out.Rows = append(out.Rows, row)
	}
	out.Continue = table.Metadata.Continue
	return out, nil
}

func checkRef(ref k8scontract.Ref, needName bool) error {
	if ref.Version == "" || ref.Resource == "" {
		return biz.ErrInvalid("ref.version and ref.resource are required")
	}
	if needName && ref.Name == "" {
		return biz.ErrInvalid("ref.name is required")
	}
	return nil
}

// resource 以用户身份访问某个资源类型的客户端（已按是否区分命名空间处理）。
func (u *userClients) resource(ref k8scontract.Ref) (resourceClient, k8scontract.APIResource, error) {
	res, err := u.c.resolve(u.ctx, ref.GVR())
	if err != nil {
		return nil, res, err
	}
	nr := u.dyn.Resource(toSchema(ref.GVR()))
	if res.Namespaced {
		if ref.Namespace == "" {
			return nil, res, biz.ErrInvalid("namespace is required for %s", ref.Resource)
		}
		return nr.Namespace(ref.Namespace), res, nil
	}
	return nr, res, nil
}

// stripNoise 去掉对人无用的字段（就地修改）。
func stripNoise(obj map[string]any) {
	if meta := nestedMap(obj, "metadata"); meta != nil {
		delete(meta, "managedFields")
	}
}

func toYAML(obj map[string]any) string {
	if obj == nil {
		return ""
	}
	raw, err := yaml.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (u *userClients) get(ref k8scontract.Ref) (k8scontract.ObjectResponse, error) {
	var out k8scontract.ObjectResponse
	if err := checkRef(ref, true); err != nil {
		return out, err
	}
	rc, _, err := u.resource(ref)
	if err != nil {
		return out, err
	}
	obj, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return out, err
	}
	stripNoise(obj.Object)
	view := obj.Object
	if biz.IsSecret(view) {
		view = biz.SanitizeObject(view) // 掩码 data，并去掉可能内嵌明文的 last-applied 注解
		view["status"] = obj.Object["status"]
		if view["status"] == nil {
			delete(view, "status")
		}
	}
	out.Kind, out.UID, out.ResourceVersion = obj.GetKind(), string(obj.GetUID()), obj.GetResourceVersion()
	out.Object, out.YAML = view, toYAML(view)
	return out, nil
}

// eventsPageSize / maxObjectEvents 对象事件的每页条数，以及一次最多读取的条数。
// 集群按 etcd 键序返回（同一对象的事件，旧的在前），只读一页拿到的是最旧的一批：事件多的对象最新的反而
// 读不到。按 continue 翻页读满 maxObjectEvents 再按时间排序截断。
const (
	eventsPageSize  = 500
	maxObjectEvents = 2000
)

// events 对象的关联事件：按 involvedObject 字段筛选，最近的在前，至多 200 条。
func (u *userClients) events(ref k8scontract.Ref) (k8scontract.EventsResponse, error) {
	out := k8scontract.EventsResponse{Items: []k8scontract.Event{}}
	if err := checkRef(ref, true); err != nil {
		return out, err
	}
	res, err := u.c.resolve(u.ctx, ref.GVR())
	if err != nil {
		return out, err
	}
	sel := "involvedObject.name=" + ref.Name + ",involvedObject.kind=" + res.Kind
	ns := ref.Namespace
	if res.Namespaced {
		sel += ",involvedObject.namespace=" + ref.Namespace
	} else if ref.GVR() == k8scontract.GVRNamespace {
		ns = ref.Name
		sel = ""
	} else {
		ns = metav1.NamespaceAll
	}
	list := &corev1.EventList{}
	opts := metav1.ListOptions{FieldSelector: sel, Limit: eventsPageSize}
	for range maxObjectEvents / eventsPageSize { // 页数有上界：至多读满 maxObjectEvents 条
		page, err := u.kube.CoreV1().Events(ns).List(u.ctx, opts)
		if err != nil {
			return out, err
		}
		list.Items = append(list.Items, page.Items...)
		if opts.Continue = page.Continue; opts.Continue == "" {
			break
		}
	}
	for i := range list.Items {
		ev := &list.Items[i]
		item := k8scontract.Event{
			Type: ev.Type, Reason: ev.Reason, Message: ev.Message, Count: ev.Count,
			FirstTime: ev.FirstTimestamp.Time, LastTime: ev.LastTimestamp.Time, Source: ev.Source.Component,
			Object: ev.InvolvedObject.Kind + "/" + ev.InvolvedObject.Name,
		}
		if item.LastTime.IsZero() {
			item.LastTime = ev.EventTime.Time
		}
		if item.LastTime.IsZero() {
			item.LastTime = ev.CreationTimestamp.Time
		}
		if item.FirstTime.IsZero() {
			item.FirstTime = item.LastTime
		}
		if ev.Series != nil && ev.Series.Count > item.Count {
			item.Count = ev.Series.Count
		}
		if item.Source == "" {
			item.Source = ev.ReportingController
		}
		out.Items = append(out.Items, item)
	}
	sort.SliceStable(out.Items, func(i, j int) bool { return out.Items[i].LastTime.After(out.Items[j].LastTime) })
	if len(out.Items) > 200 {
		out.Items = out.Items[:200]
	}
	return out, nil
}

// Namespaces implements biz.NamespaceLister（平台身份，来自缓存；授权同步用）。
func (m *Manager) Namespaces(ctx context.Context, clusterID string) ([]string, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return nil, err
	}
	e, err := c.cache.get(ctx, k8scontract.GVRNamespace)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, en := range e.store.snapshot(nil) {
		out = append(out, en.Row.Name)
	}
	sort.Strings(out)
	return out, nil
}

// watchCoalesce 变更通知的最小间隔。
const watchCoalesce = time.Second

// watch 订阅某类资源的变更：缓存有变化时推送一条不含内容的通知，合并为至多每秒一条。
func (u *userClients) watch(req k8scontract.WatchRequest, emit biz.Emit) error {
	gvr := k8scontract.GVR{Group: req.Group, Version: req.Version, Resource: req.Resource}
	if !k8scontract.Cached(gvr) {
		return biz.ErrUnsupported("resource %s does not support change subscription", gvr.Resource)
	}
	res, err := u.c.resolve(u.ctx, gvr)
	if err != nil {
		return err
	}
	namespaces, all, err := u.c.readableNamespaces(u.ctx, u.id, gvr, res.Namespaced, req.Namespace)
	if err != nil {
		return err
	}
	e, err := u.c.cache.get(u.ctx, gvr)
	if err != nil {
		return err
	}
	e.subs.Add(1)
	defer e.subs.Add(-1)
	// 整个范围可读：按请求的命名空间（或全部）订阅；只有部分命名空间可读：只订阅这些命名空间。
	ns := ""
	var only []string
	if all && len(namespaces) == 1 {
		ns = namespaces[0]
	} else if !all {
		only = namespaces
	}
	sub := e.store.subscribe(ns, only)
	defer e.store.unsubscribe(sub)
	// 订阅就绪后立刻发一个心跳：响应头随首个事件写出，客户端据此得知订阅已生效。
	if err := emit(k8scontract.EventPing, "", k8scontract.Empty{}); err != nil {
		return nil
	}
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-u.ctx.Done():
			return nil
		case <-u.c.ctx.Done():
			return emit(k8scontract.EventEnd, "", k8scontract.StreamEnd{Reason: k8scontract.EndError, Message: "cluster connection closed"})
		case <-ping.C:
			e.last.Store(time.Now().UnixNano())
			if err := emit(k8scontract.EventPing, "", k8scontract.Empty{}); err != nil {
				return nil
			}
		case <-sub.ch:
			select { // 合并窗口
			case <-time.After(watchCoalesce):
			case <-u.ctx.Done():
				return nil
			}
			select {
			case <-sub.ch:
			default:
			}
			n := sub.pending.Swap(0)
			if err := emit(k8scontract.EventChange, "", k8scontract.WatchEvent{Count: int(n)}); err != nil {
				return nil
			}
		}
	}
}

// resourceClient 某个资源类型（已定位命名空间）的客户端。
type resourceClient = dynamic.ResourceInterface
