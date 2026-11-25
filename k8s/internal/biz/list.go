package biz

import (
	"cmp"
	"slices"
	"sort"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Entry 缓存里的一个对象：只保留列表行与筛选所需的字段（不保留对象原文——详情与 YAML 直接读集群）。
// 写入缓存后只读。
type Entry struct {
	Row    k8scontract.Row
	Labels map[string]string
	// Node Pod 所在节点。
	Node string
	// Owners 所有者对象的 UID。
	Owners []string
	// CPU / Memory 总览摘要用：Pod 为各容器 requests 之和（已结束的 Pod 为 0），节点为可分配量。
	// 单位：毫核 / 字节。
	CPU    int64
	Memory int64
	// InternalIPs 节点的全部内网地址（主机定位用；其他资源为空）。列表的「内网 IP」列只展示第一个。
	InternalIPs []string
}

// ListQuery 列表的筛选、排序与分页条件。
type ListQuery struct {
	Keyword string
	State   string
	Node    string
	Owner   string
	// Match 标签选择器（nil = 不筛选）。
	Match    func(labels map[string]string) bool
	SortBy   string
	SortDesc bool
	Page     int
	PageSize int
}

// 分页缺省值与上限。
const (
	DefaultPageSize = 20
	MaxPageSize     = 200
)

// SelectEntries 对缓存条目筛选、排序、分页，返回当前页与筛选后的总数。不修改入参切片。
func SelectEntries(entries []*Entry, q ListQuery) (page []*Entry, total int) {
	keyword := strings.ToLower(strings.TrimSpace(q.Keyword))
	matched := make([]*Entry, 0, len(entries))
	for _, e := range entries {
		if keyword != "" && !strings.Contains(strings.ToLower(e.Row.Name), keyword) {
			continue
		}
		if q.State != "" && e.Row.State != q.State {
			continue
		}
		if q.Node != "" && e.Node != q.Node {
			continue
		}
		if q.Owner != "" && !slices.Contains(e.Owners, q.Owner) {
			continue
		}
		if q.Match != nil && !q.Match(e.Labels) {
			continue
		}
		matched = append(matched, e)
	}
	total = len(matched)
	sortEntries(matched, q.SortBy, q.SortDesc)

	size := q.PageSize
	if size <= 0 {
		size = DefaultPageSize
	}
	if size > MaxPageSize {
		size = MaxPageSize
	}
	pageNo := max(q.Page, 1)
	start := (pageNo - 1) * size
	if start >= total || start < 0 {
		return nil, total
	}
	return matched[start:min(start+size, total)], total
}

func sortEntries(entries []*Entry, by string, desc bool) {
	byName := func(a, b *Entry) int {
		if c := cmp.Compare(a.Row.Namespace, b.Row.Namespace); c != 0 {
			return c
		}
		return cmp.Compare(a.Row.Name, b.Row.Name)
	}
	var primary func(a, b *Entry) int
	switch by {
	case "", "name":
		primary = func(a, b *Entry) int { return cmp.Compare(a.Row.Name, b.Row.Name) }
	case "namespace":
		primary = func(a, b *Entry) int { return cmp.Compare(a.Row.Namespace, b.Row.Namespace) }
	case "createTime", "age":
		primary = func(a, b *Entry) int { return a.Row.CreateTime.Compare(b.Row.CreateTime) }
	case "state":
		primary = func(a, b *Entry) int { return cmp.Compare(a.Row.State, b.Row.State) }
	default:
		sortByCell(entries, by, desc, byName)
		return
	}
	sort.SliceStable(entries, func(i, j int) bool {
		c := primary(entries[i], entries[j])
		if desc {
			c = -c
		}
		if c != 0 {
			return c < 0
		}
		return byName(entries[i], entries[j]) < 0
	})
}

// cellKey 一个条目在某列上的排序键：每个条目只取一次单元格（一次 map 查找 + 一次类型判断），
// 比较时不再碰 map。5 万个 Pod 按自定义列排序：按节点 165ms → 50ms，按状态 103ms → 48ms，
// 按重启次数（大量并列，耗时在名称决胜上）169ms → 100ms（BenchmarkSelectEntries）。
type cellKey struct {
	e     *Entry
	num   float64
	isNum bool
	text  string
}

// sortByCell 按自定义列排序；两边都是数字按数值，否则按文本（nil 最小）。
func sortByCell(entries []*Entry, by string, desc bool, byName func(a, b *Entry) int) {
	keys := make([]cellKey, len(entries))
	for i, e := range entries {
		v := e.Row.Cells[by]
		k := cellKey{e: e}
		if k.num, k.isNum = cellNumber(v); !k.isNum {
			k.text = cellText(v)
		}
		keys[i] = k
	}
	slices.SortStableFunc(keys, func(a, b cellKey) int {
		var c int
		if a.isNum && b.isNum {
			c = cmp.Compare(a.num, b.num)
		} else {
			c = cmp.Compare(a.text, b.text)
		}
		if desc {
			c = -c
		}
		if c != 0 {
			return c
		}
		return byName(a.e, b.e)
	})
	for i := range keys {
		entries[i] = keys[i].e
	}
}

func cellNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

func cellText(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case k8scontract.StateCell:
		return t.Text
	case []string:
		return strings.Join(t, ",")
	case bool:
		if t {
			return "1"
		}
		return "0"
	}
	return ""
}
