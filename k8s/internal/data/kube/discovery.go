package kube

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// discoveryTTL 资源目录的缓存时长。
const discoveryTTL = 5 * time.Minute

type discoveryCache struct {
	mu     sync.Mutex
	items  []k8scontract.APIResource
	byGVR  map[k8scontract.GVR]k8scontract.APIResource
	byKind map[string]k8scontract.APIResource // 键：apiVersion + "/" + kind
	expire time.Time
	// loaded 最近一次实际读取目录的时间：强制刷新的最小间隔以它为准。
	loaded time.Time
}

// discoveryMinRefresh 强制刷新的最小间隔。任何用户都能触发刷新（refresh=true，或请求一个不存在的资源类型），
// 而刷新是用平台凭据做全量发现：不限速就是一个放大器。间隔内的刷新请求直接用现有目录。
const discoveryMinRefresh = 5 * time.Second

// discoveryTimeout 一次资源发现的整体超时（发现接口不接受 context）。
const discoveryTimeout = 30 * time.Second

// apiResources 集群的资源类型目录（平台身份读取：目录本身不含业务数据，对所有接入用户一致）。
func (c *conn) apiResources(ctx context.Context, refresh bool) (*discoveryCache, error) {
	d := c.disco
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if d.items != nil && (!refresh && now.Before(d.expire) || refresh && now.Sub(d.loaded) < discoveryMinRefresh) {
		return d, nil
	}
	_, lists, err := c.platDisco.ServerGroupsAndResources()
	if err != nil && (len(lists) == 0 || !discovery.IsGroupDiscoveryFailedError(err)) {
		if d.items != nil { // 刷新失败：沿用旧目录
			return d, nil
		}
		return nil, mapError(err)
	}
	crdGroups := map[string]struct{}{}
	// 只取元数据：CRD 的名称恒为 <复数名>.<组>，不必为了读 spec.group 把每个 CRD 的完整 schema 拉回来。
	cctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	crds, cerr := c.platMeta.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).
		List(cctx, metav1.ListOptions{})
	cancel()
	if cerr == nil {
		for i := range crds.Items {
			if _, g, ok := strings.Cut(crds.Items[i].Name, "."); ok {
				crdGroups[g] = struct{}{}
			}
		}
	}
	// 每个 group/resource 只保留一个版本：缓存清单里的版本优先，否则取目录里先出现的（首选版本在前）。
	type key struct{ group, resource string }
	picked := map[key]k8scontract.APIResource{}
	var order []key
	for _, l := range lists {
		gv, perr := schema.ParseGroupVersion(l.GroupVersion)
		if perr != nil {
			continue
		}
		for _, r := range l.APIResources {
			if strings.Contains(r.Name, "/") { // 子资源
				continue
			}
			item := k8scontract.APIResource{
				Group: gv.Group, Version: gv.Version, Resource: r.Name, Kind: r.Kind, Namespaced: r.Namespaced,
				Verbs: slices.Clone([]string(r.Verbs)), ShortNames: slices.Clone(r.ShortNames),
			}
			item.Cached = k8scontract.Cached(k8scontract.GVR{Group: item.Group, Version: item.Version, Resource: item.Resource})
			_, item.CRD = crdGroups[gv.Group]
			k := key{gv.Group, r.Name}
			if cur, ok := picked[k]; ok {
				if !cur.Cached && item.Cached {
					picked[k] = item
				}
				continue
			}
			picked[k] = item
			order = append(order, k)
		}
	}
	items := make([]k8scontract.APIResource, 0, len(order))
	byGVR := map[k8scontract.GVR]k8scontract.APIResource{}
	byKind := map[string]k8scontract.APIResource{}
	// byGVR / byKind 收录全部版本：用户可以用非首选版本的 YAML。
	for _, l := range lists {
		gv, perr := schema.ParseGroupVersion(l.GroupVersion)
		if perr != nil {
			continue
		}
		for _, r := range l.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			item := k8scontract.APIResource{Group: gv.Group, Version: gv.Version, Resource: r.Name, Kind: r.Kind, Namespaced: r.Namespaced, Verbs: []string(r.Verbs)}
			byGVR[k8scontract.GVR{Group: gv.Group, Version: gv.Version, Resource: r.Name}] = item
			if _, ok := byKind[l.GroupVersion+"/"+r.Kind]; !ok {
				byKind[l.GroupVersion+"/"+r.Kind] = item
			}
		}
	}
	for _, k := range order {
		items = append(items, picked[k])
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Group != items[j].Group {
			return items[i].Group < items[j].Group
		}
		return items[i].Resource < items[j].Resource
	})
	d.items, d.byGVR, d.byKind, d.expire, d.loaded = items, byGVR, byKind, time.Now().Add(discoveryTTL), time.Now()
	return d, nil
}

// resolve 查出某个资源类型的目录项（是否区分命名空间、Kind）；目录里没有时刷新一次再查（新装的 CRD）。
func (c *conn) resolve(ctx context.Context, gvr k8scontract.GVR) (k8scontract.APIResource, error) {
	if gvr.Version == "" || gvr.Resource == "" {
		return k8scontract.APIResource{}, biz.ErrInvalid("version and resource are required")
	}
	for _, refresh := range []bool{false, true} {
		d, err := c.apiResources(ctx, refresh)
		if err != nil {
			return k8scontract.APIResource{}, err
		}
		d.mu.Lock()
		r, ok := d.byGVR[gvr]
		d.mu.Unlock()
		if ok {
			return r, nil
		}
	}
	return k8scontract.APIResource{}, biz.ErrNotFound("resource type %s/%s %s does not exist in this cluster", gvr.Group, gvr.Version, gvr.Resource)
}

// resolveKind 由 apiVersion + kind 查出资源类型（多文档 YAML）。
func (c *conn) resolveKind(ctx context.Context, apiVersion, kind string) (k8scontract.APIResource, error) {
	for _, refresh := range []bool{false, true} {
		d, err := c.apiResources(ctx, refresh)
		if err != nil {
			return k8scontract.APIResource{}, err
		}
		d.mu.Lock()
		r, ok := d.byKind[apiVersion+"/"+kind]
		d.mu.Unlock()
		if ok {
			return r, nil
		}
	}
	return k8scontract.APIResource{}, biz.ErrNotFound("kind %s (%s) does not exist in this cluster", kind, apiVersion)
}

func toSchema(g k8scontract.GVR) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: g.Group, Version: g.Version, Resource: g.Resource}
}
