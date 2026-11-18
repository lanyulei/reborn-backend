package biz

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
)

// SyncProviderSet 后台同步与事件动作的用例（monitor-ci-sync / monitor-calendar / monitor-action 三个循环）。
// 它们依赖的跨域窄接口由组合根（internal/biz 的 monitor_p2.go）提供。
var SyncProviderSet = wire.NewSet(NewCISyncUsecase, NewCalendarSyncUsecase, NewActionDeps, NewActionUsecase)

// CMDB 资源索引同步（后台循环 monitor-ci-sync）：把 CMDB 资源经窄接口物化到 monitor_ci_index，评估引擎
// 只读这张表（不读 cmdb_* 表）。两种轮次：
//   - 增量（每轮）：按 CMDB 的更新时间自新到旧走到上次的水位（索引里最大的 source_update_time 减去一段
//     重叠），在册资源 upsert、回收站里的资源写成失效的行；
//   - 对账（进程启动后的第一轮，之后每 ciReconcileInterval 一次，或增量走不到水位时）：全量走一遍，索引里
//     没走到的行置失效，并重算上游资源。拓扑关系的变化只在对账时生效。
//
// 多实例：一轮同步在一个事务里完成，事务先取事务级 advisory lock（monitorcontract.LockCISync）——同一时刻
// 只有一个实例在走；没抢到的实例本轮什么都不做。写入全部幂等（内容没变的行不写），所以即使两个实例先后各
// 走一遍，结果也相同。monitor_cfg_version 的 monitor_ci_index 行在本轮确有变化时随同一事务 +1 一次；进程
// 中途崩溃则整轮回滚，下一轮重来。「上次对账时间」只在进程内存里（每个实例各自每小时对账一次，不影响正确性）。
const (
	// ciPageSize 一页读取的资源数（CMDB 列表接口的页大小上限）。
	ciPageSize = 100
	// ciIncrementalPages 增量一轮至多读取的页数；读完仍没走到水位即改走对账。
	ciIncrementalPages = 20
	// ciReconcilePages 对账一轮至多读取的页数（20 万个资源）；超出时不做「未走到即失效」，并记日志。
	ciReconcilePages = 2000
	// ciWriteBatch 一次写入的行数。
	ciWriteBatch = 200
	// ciOverlap 增量水位向前回看的时长：容忍 CMDB 写入方与数据库之间的时钟偏差与在途事务。
	ciOverlap = 2 * time.Minute
	// ciReconcileInterval 对账周期。
	ciReconcileInterval = time.Hour
	// maxUpstreamDepth / maxUpstreams 上游资源的层数与每个资源的上游个数上限。
	maxUpstreamDepth = 3
	maxUpstreams     = 64
	// maxCIAttrValues / maxCIAttrValueLen 一个可匹配属性的取值个数与单个取值的长度（字节）上限。
	maxCIAttrValues   = 32
	maxCIAttrValueLen = 256
	// maxCIMatchAttrs 部署配置里可匹配属性的个数上限。
	maxCIMatchAttrs = 20
	// ciMaskedValue CMDB 对敏感属性返回的掩码：不是真实取值，不进索引。
	ciMaskedValue = "******"
)

// SourceCI CMDB 里的一个资源（同步来源的视图）。
type SourceCI struct {
	ID         string
	ModelCode  string
	Name       string
	Env        string
	Status     string
	Owner      string
	Tags       []string
	Attrs      map[string]any
	UpdateTime time.Time
}

// CISource CMDB 资源的同步来源（组合根以 cmdb 域的公开用例适配）。
type CISource interface {
	// Page 按更新时间自新到旧读一页，至多 limit 个；deleted 为真读回收站。cursor 为空从最新的开始；
	// 返回的 next 为空表示没有更多。
	Page(ctx context.Context, deleted bool, cursor string, limit int) (items []SourceCI, next string, err error)
	// Upstreams 各资源的直接上游资源（资源 ID → 上游资源 ID），按部署配置的拓扑关系；complete 为 false
	// 表示关系太多、只读到了一部分。
	Upstreams(ctx context.Context) (direct map[string][]string, complete bool, err error)
}

// CIIndexWriter 一轮同步内对索引的写入（都在这一轮的事务里）。内容没有变化的行不写，返回实际写入的行数。
type CIIndexWriter interface {
	// HighWater 索引里最大的 source_update_time；索引为空返回 nil。
	HighWater(ctx context.Context) (*time.Time, error)
	// Upsert 写入资源（不动 upstream_ids）；Valid 为 false 的行照常写入并保留。
	Upsert(ctx context.Context, entries []*CIEntry) (int, error)
}

// CIIndexReconciler 对账轮次的写入。
type CIIndexReconciler interface {
	// InvalidateExcept 把不在 keep 里的有效行置为失效。
	InvalidateExcept(ctx context.Context, keep []string) (int, error)
	// SetUpstreams 整体替换上游资源：不在 upstreams 里的行清空。
	SetUpstreams(ctx context.Context, upstreams map[string][]string) (int, error)
}

// CIIndexStore 索引的同步入口。
type CIIndexStore interface {
	// Sync 取得同步锁后在一个事务里执行 fn，fn 返回的变更行数大于 0 时同事务把配置版本 +1；别的实例
	// 正持有同步锁时不执行 fn，返回 ran = false。
	Sync(ctx context.Context, fn func(ctx context.Context, w CIIndexWriter, r CIIndexReconciler) (changed int, err error)) (ran bool, err error)
}

// CISyncUsecase CMDB 资源索引同步用例。
type CISyncUsecase struct {
	src   CISource
	store CIIndexStore
	attrs []string
	log   *slog.Logger
	now   func() time.Time

	// mu 保护 lastReconcile（本实例上次对账完成的时间；零值 = 还没对过账）。
	mu            sync.Mutex
	lastReconcile time.Time
}

var ciAttrRE = regexp.MustCompile(mc.CIAttrPattern)

// CIMatchAttrs 规整部署配置里的可匹配属性：缺省只有 ip；属性名须符合 monitorcontract.CIAttrPattern，
// 去重，至多 maxCIMatchAttrs 个。
func CIMatchAttrs(c *conf.Monitor) ([]string, error) {
	raw := c.GetCiMatchAttrs()
	if len(raw) == 0 {
		return []string{mc.DefaultCIMatchAttr}, nil
	}
	out := make([]string, 0, len(raw))
	for _, a := range raw {
		a = strings.TrimSpace(a)
		if !ciAttrRE.MatchString(a) {
			return nil, fmt.Errorf("monitor.ci_match_attrs: %q is not a valid attribute name", a)
		}
		if !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	if len(out) > maxCIMatchAttrs {
		return nil, fmt.Errorf("monitor.ci_match_attrs: at most %d attributes", maxCIMatchAttrs)
	}
	return out, nil
}

// NewCISyncUsecase new a CISyncUsecase.
func NewCISyncUsecase(src CISource, store CIIndexStore, logger *slog.Logger, c *conf.Monitor) (*CISyncUsecase, error) {
	attrs, err := CIMatchAttrs(c)
	if err != nil {
		return nil, err
	}
	return &CISyncUsecase{src: src, store: store, attrs: attrs, log: logger, now: time.Now}, nil
}

// Run 执行一轮同步，返回写入的行数。恒返回 0 条「待续」（循环按固定周期运行，不连续空转）。
func (uc *CISyncUsecase) Run(ctx context.Context) (int, error) {
	uc.mu.Lock()
	reconcile := uc.lastReconcile.IsZero() || uc.now().Sub(uc.lastReconcile) >= ciReconcileInterval
	uc.mu.Unlock()
	var changed int
	var reconciled bool
	ran, err := uc.store.Sync(ctx, func(ctx context.Context, w CIIndexWriter, r CIIndexReconciler) (int, error) {
		changed, reconciled = 0, false
		if !reconcile {
			n, reached, err := uc.incremental(ctx, w)
			if err != nil {
				return 0, err
			}
			changed = n
			if reached {
				return changed, nil
			}
			uc.log.WarnContext(ctx, "monitor ci sync: too many changes for an incremental run, reconciling", "pages", ciIncrementalPages)
		}
		n, err := uc.reconcile(ctx, w, r)
		if err != nil {
			return 0, err
		}
		changed, reconciled = changed+n, true
		return changed, nil
	})
	if err != nil || !ran {
		return 0, err
	}
	if reconciled {
		uc.mu.Lock()
		uc.lastReconcile = uc.now()
		uc.mu.Unlock()
	}
	if changed > 0 {
		uc.log.InfoContext(ctx, "monitor ci index synced", "changed", changed, "reconcile", reconciled)
	}
	return 0, nil
}

// ciBatcher 攒够一批再写。
type ciBatcher[T any] struct {
	items []T
	flush func(items []T) (int, error)
	total int
}

func (b *ciBatcher[T]) add(it T) error {
	if b.items = append(b.items, it); len(b.items) < ciWriteBatch {
		return nil
	}
	return b.done()
}

func (b *ciBatcher[T]) done() error {
	if len(b.items) == 0 {
		return nil
	}
	n, err := b.flush(b.items)
	b.total += n
	b.items = b.items[:0]
	return err
}

// walk 自新到旧读资源，逐个交给 visit；读到更新时间早于 floor 的资源即停（floor 为 nil 不设下限）。
// reached 表示走到了下限或走完了全部（没有被页数上限截断）。
func (uc *CISyncUsecase) walk(ctx context.Context, deleted bool, floor *time.Time, maxPages int, visit func(ci *SourceCI) error) (reached bool, err error) {
	cursor := ""
	for page := 0; page < maxPages; page++ {
		items, next, err := uc.src.Page(ctx, deleted, cursor, ciPageSize)
		if err != nil {
			return false, fmt.Errorf("read cmdb resources: %w", err)
		}
		for i := range items {
			if floor != nil && items[i].UpdateTime.Before(*floor) {
				return true, nil
			}
			if err := visit(&items[i]); err != nil {
				return false, err
			}
		}
		if next == "" {
			return true, nil
		}
		cursor = next
	}
	return false, nil
}

// incremental 增量一轮：水位之后变化过的在册资源 upsert、回收站里的置失效。索引为空或走不到水位时
// reached 为 false（调用方改走对账）。
func (uc *CISyncUsecase) incremental(ctx context.Context, w CIIndexWriter) (changed int, reached bool, err error) {
	hw, err := w.HighWater(ctx)
	if err != nil || hw == nil {
		return 0, false, err
	}
	floor := hw.Add(-ciOverlap)
	ups := &ciBatcher[*CIEntry]{flush: func(items []*CIEntry) (int, error) { return w.Upsert(ctx, items) }}
	// 先处理回收站再处理在册的：同一轮里被恢复的资源以在册为准。回收站里的资源同样整行写入（置失效），
	// 这样水位也随删除推进，不会每一轮都重读同一批已删除的资源。
	for _, deleted := range []bool{true, false} {
		reached, err = uc.walk(ctx, deleted, &floor, ciIncrementalPages, func(ci *SourceCI) error {
			e := uc.entry(ci)
			e.Valid = e.Valid && !deleted
			return ups.add(e)
		})
		if err != nil || !reached {
			return ups.total, false, err
		}
		if err := ups.done(); err != nil {
			return 0, false, err
		}
	}
	return ups.total, true, nil
}

// reconcile 对账一轮：全量 upsert，没走到的行置失效，重算上游资源。
func (uc *CISyncUsecase) reconcile(ctx context.Context, w CIIndexWriter, r CIIndexReconciler) (int, error) {
	seen := make([]string, 0, 1024)
	ups := &ciBatcher[*CIEntry]{flush: func(items []*CIEntry) (int, error) { return w.Upsert(ctx, items) }}
	complete, err := uc.walk(ctx, false, nil, ciReconcilePages, func(ci *SourceCI) error {
		seen = append(seen, ci.ID)
		return ups.add(uc.entry(ci))
	})
	if err != nil {
		return 0, err
	}
	if err := ups.done(); err != nil {
		return 0, err
	}
	changed := ups.total
	if complete {
		n, err := r.InvalidateExcept(ctx, seen)
		if err != nil {
			return 0, err
		}
		changed += n
	} else {
		uc.log.WarnContext(ctx, "monitor ci sync: too many cmdb resources, removed resources are not invalidated this round", "limit", ciReconcilePages*ciPageSize)
	}
	direct, full, err := uc.src.Upstreams(ctx)
	if err != nil {
		return 0, fmt.Errorf("read cmdb topology: %w", err)
	}
	if !full {
		// 只读到一部分关系时不改上游：整体替换会把没读到的清空。
		uc.log.WarnContext(ctx, "monitor ci sync: too many topology relations, upstream resources are left unchanged this round")
		return changed, nil
	}
	n, err := r.SetUpstreams(ctx, closeUpstreams(direct))
	if err != nil {
		return 0, err
	}
	return changed + n, nil
}

// entry 把 CMDB 资源转换为索引行：退役的资源置失效；属性只留登记的可匹配属性。
func (uc *CISyncUsecase) entry(ci *SourceCI) *CIEntry {
	at := ci.UpdateTime
	tags := slices.Clone(ci.Tags)
	slices.Sort(tags)
	return &CIEntry{
		ID: ci.ID, ModelCode: truncateBytes(ci.ModelCode, 64), Name: truncate(ci.Name, 128), Env: truncate(ci.Env, 32),
		Status: truncate(ci.Status, 16), Owner: truncate(ci.Owner, 64), Tags: slices.Compact(tags),
		Attrs: matchAttrs(ci.Attrs, uc.attrs), Valid: ci.Status != ciStatusRetired, SourceUpdateTime: &at,
	}
}

// ciStatusRetired CMDB 资源的「已退役」状态（cmdb_ci.status）。
const ciStatusRetired = "retired"

// matchAttrs 取出登记的可匹配属性，取值一律是字符串数组：标量包成单元素数组，数组展开，其余类型
// （对象、嵌套数组、null）、空串与掩码跳过；去重并限制个数与长度。
func matchAttrs(attrs map[string]any, names []string) map[string][]string {
	out := make(map[string][]string, len(names))
	for _, name := range names {
		var vals []string
		add := func(v any) {
			s, ok := scalarText(v)
			if !ok || s == "" || s == ciMaskedValue || len(s) > maxCIAttrValueLen || len(vals) >= maxCIAttrValues || slices.Contains(vals, s) {
				return
			}
			vals = append(vals, s)
		}
		switch v := attrs[name].(type) {
		case []any:
			for _, item := range v {
				add(item)
			}
		case []string:
			for _, item := range v {
				add(item)
			}
		default:
			add(v)
		}
		if len(vals) > 0 {
			out[name] = vals
		}
	}
	return out
}

// scalarText 标量取值的文本形式；不是标量返回 false。
func scalarText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x), true
	case bool:
		return strconv.FormatBool(x), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case int:
		return strconv.Itoa(x), true
	case int64:
		return strconv.FormatInt(x, 10), true
	}
	return "", false
}

// closeUpstreams 由直接上游求传递闭包：向上至多 maxUpstreamDepth 层，去重、不含自身（关系成环时也会
// 终止），每个资源至多 maxUpstreams 个，按 ID 排序（写入时据此判断有没有变化）。
func closeUpstreams(direct map[string][]string) map[string][]string {
	out := make(map[string][]string, len(direct))
	for id := range direct {
		seen := map[string]struct{}{id: {}}
		frontier := []string{id}
		var ups []string
		for depth := 0; depth < maxUpstreamDepth && len(frontier) > 0 && len(ups) < maxUpstreams; depth++ {
			var next []string
			for _, cur := range frontier {
				for _, up := range direct[cur] {
					if _, dup := seen[up]; dup || len(ups) >= maxUpstreams {
						continue
					}
					seen[up] = struct{}{}
					ups = append(ups, up)
					next = append(next, up)
				}
			}
			frontier = next
		}
		if len(ups) > 0 {
			slices.Sort(ups)
			out[id] = ups
		}
	}
	return out
}

// TopologyRelation 一种用于拓扑抑制与影响面的 CMDB 关系类型。UpstreamSrc 为真表示关系的 src 一侧是上游
// （上游影响下游），否则 dst 一侧是上游。
type TopologyRelation struct {
	TypeCode    string
	UpstreamSrc bool
}

// maxTopologyRelations 部署配置里拓扑关系类型的个数上限。
const maxTopologyRelations = 10

var topologyTypeRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// TopologyRelations 解析部署配置 monitor.topology_relations（每项 "<关系类型编码>:<src|dst>"）；缺省为
// 内置的 host_runs_vm:src（物理机是其虚拟机的上游）与 uplink:dst（上联的网络设备是接入它的设备的上游）。
func TopologyRelations(c *conf.Monitor) ([]TopologyRelation, error) {
	raw := c.GetTopologyRelations()
	if len(raw) == 0 {
		raw = []string{"host_runs_vm:src", "uplink:dst"}
	}
	if len(raw) > maxTopologyRelations {
		return nil, fmt.Errorf("monitor.topology_relations: at most %d relation types", maxTopologyRelations)
	}
	out := make([]TopologyRelation, 0, len(raw))
	for _, item := range raw {
		code, side, ok := strings.Cut(strings.TrimSpace(item), ":")
		if !ok || !topologyTypeRE.MatchString(code) || (side != "src" && side != "dst") {
			return nil, fmt.Errorf("monitor.topology_relations: %q must look like <relation type>:<src|dst>", item)
		}
		if slices.ContainsFunc(out, func(r TopologyRelation) bool { return r.TypeCode == code }) {
			return nil, fmt.Errorf("monitor.topology_relations: relation type %q is listed twice", code)
		}
		out = append(out, TopologyRelation{TypeCode: code, UpstreamSrc: side == "src"})
	}
	return out, nil
}
