package kube

import (
	"context"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// cached 缓存里的一个对象：只保留定位用的元数据与列表行（biz.Entry），不保留对象原文。
// 嵌入 ObjectMeta 只为满足 reflector 临时存储的取键函数（meta.Accessor）。
type cached struct {
	metav1.ObjectMeta
	entry *biz.Entry
}

// entryStore reflector 的落点：把对象转成 biz.Entry 后按 命名空间 → 名称 存放，并通知订阅者。
// 实现 cache.ReflectorStore 与 cache.TransformingStore（流式初始列表的临时存储里也只放转换后的对象）。
type entryStore struct {
	gvr     k8scontract.GVR
	convert func(u *unstructured.Unstructured) *biz.Entry

	mu     sync.RWMutex
	items  map[string]map[string]*biz.Entry // 不变式：内层 map 非空；Entry 写入后只读
	count  int
	synced bool
	ready  chan struct{} // 首次 Replace 后关闭
	subs   map[*subscription]struct{}
}

type subscription struct {
	namespace string
	only      map[string]struct{}
	ch        chan struct{} // 容量 1：只表示「有变化」
	pending   atomic.Int64
}

func newEntryStore(gvr k8scontract.GVR) *entryStore {
	return &entryStore{
		gvr: gvr, convert: converterFor(gvr), items: map[string]map[string]*biz.Entry{},
		ready: make(chan struct{}), subs: map[*subscription]struct{}{},
	}
}

func (s *entryStore) toCached(obj any) *cached {
	switch t := obj.(type) {
	case *cached:
		return t
	case *unstructured.Unstructured:
		e := s.convert(t)
		return &cached{ObjectMeta: metav1.ObjectMeta{Namespace: t.GetNamespace(), Name: t.GetName(), UID: t.GetUID(), ResourceVersion: t.GetResourceVersion()}, entry: e}
	case *metav1.PartialObjectMetadata:
		// 只取元数据的缓存（ConfigMap / Secret）：对象内容从未离开集群。
		obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(t)
		if err != nil {
			return nil
		}
		u := &unstructured.Unstructured{Object: obj}
		e := s.convert(u)
		return &cached{ObjectMeta: metav1.ObjectMeta{Namespace: t.Namespace, Name: t.Name, UID: t.UID, ResourceVersion: t.ResourceVersion}, entry: e}
	case cache.DeletedFinalStateUnknown:
		return s.toCached(t.Obj)
	}
	return nil
}

// Transformer implements cache.TransformingStore.
func (s *entryStore) Transformer() cache.TransformFunc {
	return func(obj any) (any, error) {
		if c := s.toCached(obj); c != nil {
			return c, nil
		}
		return obj, nil
	}
}

func (s *entryStore) put(c *cached) {
	ns := s.items[c.Namespace]
	if ns == nil {
		ns = map[string]*biz.Entry{}
		s.items[c.Namespace] = ns
	}
	if _, ok := ns[c.Name]; !ok {
		s.count++
	}
	ns[c.Name] = c.entry
}

// Add implements cache.ReflectorStore.
func (s *entryStore) Add(obj any) error {
	c := s.toCached(obj)
	if c == nil {
		return nil
	}
	s.mu.Lock()
	s.put(c)
	s.mu.Unlock()
	s.notify(c.Namespace)
	return nil
}

// Update implements cache.ReflectorStore.
func (s *entryStore) Update(obj any) error { return s.Add(obj) }

// Delete implements cache.ReflectorStore.
func (s *entryStore) Delete(obj any) error {
	c := s.toCached(obj)
	if c == nil {
		return nil
	}
	s.mu.Lock()
	if ns := s.items[c.Namespace]; ns != nil {
		if _, ok := ns[c.Name]; ok {
			delete(ns, c.Name)
			s.count--
			if len(ns) == 0 {
				delete(s.items, c.Namespace)
			}
		}
	}
	s.mu.Unlock()
	s.notify(c.Namespace)
	return nil
}

// Replace implements cache.ReflectorStore.
func (s *entryStore) Replace(list []any, _ string) error {
	items := map[string]map[string]*biz.Entry{}
	count := 0
	for _, obj := range list {
		c := s.toCached(obj)
		if c == nil {
			continue
		}
		ns := items[c.Namespace]
		if ns == nil {
			ns = map[string]*biz.Entry{}
			items[c.Namespace] = ns
		}
		ns[c.Name] = c.entry
		count++
	}
	s.mu.Lock()
	s.items, s.count = items, count
	first := !s.synced
	s.synced = true
	s.mu.Unlock()
	if first {
		close(s.ready)
	}
	s.notify("")
	return nil
}

// Resync implements cache.ReflectorStore.
func (s *entryStore) Resync() error { return nil }

// notify 通知订阅者；namespace 为空 = 全量替换，通知所有人。
func (s *entryStore) notify(namespace string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for sub := range s.subs {
		if namespace != "" && sub.namespace != "" && sub.namespace != namespace {
			continue
		}
		if namespace != "" && sub.only != nil {
			// 只有部分命名空间可读的订阅者：其余命名空间的变更不通知（次数本身也是信息）。
			if _, ok := sub.only[namespace]; !ok {
				continue
			}
		}
		sub.pending.Add(1)
		select {
		case sub.ch <- struct{}{}:
		default:
		}
	}
}

// subscribe 订阅变更通知。namespace 非空 = 只订阅该命名空间；only 非 nil = 只订阅这些命名空间。
func (s *entryStore) subscribe(namespace string, only []string) *subscription {
	sub := &subscription{namespace: namespace, ch: make(chan struct{}, 1)}
	if only != nil {
		sub.only = make(map[string]struct{}, len(only))
		for _, ns := range only {
			sub.only[ns] = struct{}{}
		}
	}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	s.mu.Unlock()
	return sub
}

func (s *entryStore) unsubscribe(sub *subscription) {
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
}

// snapshot 某些命名空间（nil = 全部）下的条目快照；返回的切片归调用方所有，Entry 只读。
func (s *entryStore) snapshot(namespaces []string) []*biz.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if namespaces == nil {
		out := make([]*biz.Entry, 0, s.count)
		for _, ns := range s.items {
			for _, e := range ns {
				out = append(out, e)
			}
		}
		return out
	}
	var out []*biz.Entry
	for _, name := range namespaces {
		for _, e := range s.items[name] {
			out = append(out, e)
		}
	}
	return out
}

func (s *entryStore) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// resourceCache 一个集群的按需缓存：某类资源首次被访问时启动 reflector，一段时间无人访问后停止并释放。
type resourceCache struct {
	c *conn

	mu      sync.Mutex
	entries map[k8scontract.GVR]*cacheEntry
}

type cacheEntry struct {
	store  *entryStore
	cancel context.CancelFunc
	last   atomic.Int64 // 最近一次访问（UnixNano）
	subs   atomic.Int64 // 活跃订阅数：有订阅时不因空闲释放
}

func newResourceCache(c *conn) *resourceCache {
	rc := &resourceCache{c: c, entries: map[k8scontract.GVR]*cacheEntry{}}
	go rc.reap() // 退出路径：c.ctx 取消（连接断开）
	return rc
}

// heapBytes 上一次垃圾回收之后仍存活的堆字节数（不触发 STW）。用它而不是「堆上对象的字节数」：
// 后者含尚未清扫的垃圾，一次大的列表请求就能让它短暂越线，把新缓存误拒掉。运行时不提供该指标时
// 退回到后者（偏保守）。
func heapBytes() uint64 {
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	for _, s := range sample {
		if s.Value.Kind() == metrics.KindUint64 && s.Value.Uint64() > 0 {
			return s.Value.Uint64()
		}
	}
	return 0
}

// get 取得某类资源已同步的缓存：未启动则启动，并等待首次同步（至多 SyncTimeout）。
func (rc *resourceCache) get(ctx context.Context, gvr k8scontract.GVR) (*cacheEntry, error) {
	if !k8scontract.Cached(gvr) {
		return nil, biz.ErrUnsupported("resource %s is not cached", gvr.Resource)
	}
	rc.mu.Lock()
	e := rc.entries[gvr]
	if e == nil {
		if limit := rc.c.m.opts.MemoryLimit; limit > 0 && heapBytes() > limit {
			rc.mu.Unlock()
			return nil, biz.ErrCacheSyncing("memory limit reached, cannot build the %s cache now; retry later", gvr.Resource)
		}
		e = rc.start(gvr)
		rc.entries[gvr] = e
	}
	// 在锁内刷新访问时间：否则空闲回收可能恰好在「取到条目」与「刷新时间」之间把它停掉，
	// 调用方就会拿着一个已停止的缓存空等到超时。
	e.last.Store(time.Now().UnixNano())
	rc.mu.Unlock()

	timer := time.NewTimer(rc.c.m.opts.SyncTimeout)
	defer timer.Stop()
	select {
	case <-e.store.ready:
		return e, nil
	case <-timer.C:
		return nil, biz.ErrCacheSyncing("the %s cache is being built, retry shortly", gvr.Resource)
	case <-ctx.Done():
		return nil, biz.ErrCacheSyncing("the %s cache is being built, retry shortly", gvr.Resource)
	case <-rc.c.ctx.Done():
		return nil, biz.ErrClusterOffline("cluster connection closed")
	}
}

// peek 已同步的缓存（不启动、不等待）；没有返回 nil。
func (rc *resourceCache) peek(gvr k8scontract.GVR) *entryStore {
	rc.mu.Lock()
	e := rc.entries[gvr]
	rc.mu.Unlock()
	if e == nil {
		return nil
	}
	select {
	case <-e.store.ready:
		return e.store
	default:
		return nil
	}
}

func (rc *resourceCache) start(gvr k8scontract.GVR) *cacheEntry {
	store := newEntryStore(gvr)
	sgvr := schema.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource}
	var (
		lw       cache.ListerWatcher
		expected runtime.Object = &unstructured.Unstructured{}
	)
	if k8scontract.MetadataOnly(gvr) {
		// ConfigMap / Secret：用元数据客户端（PartialObjectMetadata）。平台凭据从不请求它们的内容——
		// 否则任何人一打开 Secret 列表，全集群的 Secret 明文就会流经数据面的内存。
		res := rc.c.platMeta.Resource(sgvr)
		lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) { return res.List(ctx, o) },
			WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
				return res.Watch(ctx, o)
			},
		}, rc.c.platMeta)
		expected = &metav1.PartialObjectMetadata{}
	} else {
		res := rc.c.platDyn.Resource(sgvr)
		lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) { return res.List(ctx, o) },
			WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
				return res.Watch(ctx, o)
			},
		}, rc.c.platDyn)
	}
	ref := cache.NewReflectorWithOptions(lw, expected, store, cache.ReflectorOptions{
		Name: "reborn/" + rc.c.cluster.Name + "/" + gvr.Resource,
	})
	ctx, cancel := context.WithCancel(rc.c.ctx)
	e := &cacheEntry{store: store, cancel: cancel}
	go ref.RunWithContext(ctx) // 退出路径：cancel（空闲释放）或连接断开
	return e
}

// reap 周期释放空闲的缓存。
func (rc *resourceCache) reap() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-rc.c.ctx.Done():
			return
		case now := <-ticker.C:
			idle := rc.c.m.opts.CacheIdle
			rc.mu.Lock()
			for gvr, e := range rc.entries {
				if e.subs.Load() == 0 && now.Sub(time.Unix(0, e.last.Load())) > idle {
					e.cancel()
					delete(rc.entries, gvr)
				}
			}
			rc.mu.Unlock()
		}
	}
}

func (rc *resourceCache) stopAll() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for gvr, e := range rc.entries {
		e.cancel()
		delete(rc.entries, gvr)
	}
}

// CacheStats 各资源类型当前缓存的对象数（全部集群合计），供指标导出；标签基数 = 缓存清单的大小。
func (m *Manager) CacheStats() map[string]int {
	out := map[string]int{}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.conns {
		if c.cache == nil {
			continue
		}
		c.cache.mu.Lock()
		for gvr, e := range c.cache.entries {
			out[gvr.Resource] += e.store.size()
		}
		c.cache.mu.Unlock()
	}
	return out
}
