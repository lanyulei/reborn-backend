package biz

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 告警统计的限额与口径。
const (
	// statsMaxRange 带时间范围的统计接口的跨度上限。
	statsMaxRange = 90 * 24 * time.Hour
	// statsDefaultTimezone 未指定时区时按哪个时区的自然日统计。
	statsDefaultTimezone = "Asia/Shanghai"
	// statsGroupSeries 按分组拆分时单独成行的分组数，其余并入「其他」。
	statsGroupSeries = 10
	// statsTopDefault / statsTopMax 排行返回的行数。
	statsTopDefault = 10
	statsTopMax     = 50
	// StatsOtherKey / StatsTotalKey 「其他」「合计」行的键。
	StatsOtherKey = "other"
	StatsTotalKey = "total"

	statsOtherName = "其他"
	statsTotalName = "合计"
)

// 统计的拆分维度（StatsReader 的 dim）。
const (
	StatsBySeverity = "severity"
	StatsByGroup    = "group"
)

// 排行对象。
const (
	TopByRule  = "rule"
	TopByCI    = "ci"
	TopByLabel = "label"
)

// StatsRange 统计的取数范围：分组过滤（由 Scope 生成）+ 首次触发时间 [Start, End)。
type StatsRange struct {
	Groups GroupFilter
	Start  time.Time
	End    time.Time
}

// AlertOverview 告警概览。
type AlertOverview struct {
	Firing           int32
	FiringBySeverity [4]int32
	Unacked          int32
	Muted            int32
	Inhibited        int32
	TodayFired       int32
	TodayResolved    int32
	TodayClosed      int32
}

// StatsCell 趋势的一格：某天（yyyy-mm-dd）、某个键（级别数字或分组 ID）的事件数。
type StatsCell struct {
	Day   string
	Key   string
	Count int32
}

// EfficiencyRow 处理效率的一行（数据层按级别或分组 ID 聚合；耗时为秒数之和，平均值由用例计算）。
type EfficiencyRow struct {
	Key         string
	Alerts      int32
	Acked       int32
	Resolved    int32
	AckSumS     float64
	ResolveSumS float64
}

// TopBy 排行对象；Kind 为 TopByLabel 时 Label 为标签名。
type TopBy struct {
	Kind  string
	Label string
}

// TopRow 排行的一行。GroupID 仅按规则排行时有值（事件触发时规则所在的分组）。
type TopRow struct {
	Key         string
	Name        string
	GroupID     string
	Count       int32
	Firing      int32
	MaxSeverity int32
}

// AlertNoise 降噪效果。口径（受现有列所限）：
//   - Muted：事件此刻带屏蔽标记，或时间线里有过 mute 条目（被屏蔽过后来解除的也算）；
//   - Inhibited：事件此刻带抑制标记，或时间线里有过 inhibit 条目；
//   - Notified：通知计数大于 0，或有任意一条通知记录；Silent = Alerts − Notified；
//   - 各类通知次数取自 monitor_notify_log：一行 = 一次通知中因规则 / 某条订阅 / 升级策略某一级发出的那一份
//     （发给多人算一次），只统计范围内事件的记录。
type AlertNoise struct {
	Alerts                    int32
	Muted                     int32
	Inhibited                 int32
	Notified                  int32
	RuleNotifications         int32
	SubscriptionNotifications int32
	EscalationNotifications   int32
}

// StatsReader 告警统计读仓储（概览、趋势、效率）。调用方保证 Groups 非空过滤、时间范围有界。
type StatsReader interface {
	// Overview 当前活跃告警的分布，以及 [dayStart, dayEnd) 内新增 / 恢复 / 关闭的事件数。
	Overview(ctx context.Context, groups GroupFilter, dayStart, dayEnd time.Time) (*AlertOverview, error)
	// Trend 按天（timezone 的自然日）与 dim（StatsBySeverity / StatsByGroup）统计事件数。
	Trend(ctx context.Context, r StatsRange, dim, timezone string) ([]StatsCell, error)
	// Efficiency 按 dim 统计认领 / 恢复的数量与耗时之和。
	Efficiency(ctx context.Context, r StatsRange, dim string) ([]EfficiencyRow, error)
}

// StatsRanker 告警统计读仓储（排行、降噪效果）。
type StatsRanker interface {
	// Top 按事件数倒序取至多 limit 行；total 为参与排行的事件总数。
	Top(ctx context.Context, r StatsRange, by TopBy, limit int) (rows []TopRow, total int64, err error)
	Noise(ctx context.Context, r StatsRange) (*AlertNoise, error)
}

// StatsUsecase 告警统计（只读）：只统计调用方数据范围内（对规则分组只读以上）的事件。
type StatsUsecase struct {
	scopes *ScopeUsecase
	reader StatsReader
	ranker StatsRanker
	rules  RuleLookup
	cis    CIIndexReader
	now    func() time.Time
}

// NewStatsUsecase new a StatsUsecase.
func NewStatsUsecase(scopes *ScopeUsecase, reader StatsReader, ranker StatsRanker, rules RuleLookup, cis CIIndexReader) *StatsUsecase {
	return &StatsUsecase{scopes: scopes, reader: reader, ranker: ranker, rules: rules, cis: cis, now: time.Now}
}

// statsLocation 解析统计用的时区：留空取缺省；无法识别返回 ErrQueryInvalid。返回规范的时区名
// （交给数据库按同一时区分桶）。
func statsLocation(name string) (*time.Location, string, error) {
	if name = strings.TrimSpace(name); name == "" {
		name = statsDefaultTimezone
	}
	// "Local" 取决于部署机器，数据库也不认识它。
	if name == "Local" {
		return nil, "", detail(ErrQueryInvalid, "unknown timezone %q", name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, "", detail(ErrQueryInvalid, "unknown timezone %q", truncate(name, 64))
	}
	return loc, name, nil
}

// statsRange 校验 [start, end) 并结合数据范围生成取数范围。
func statsRange(scope Scope, groupID string, start, end time.Time) (StatsRange, error) {
	if !start.Before(end) {
		return StatsRange{}, detail(ErrTimeRangeInvalid, "start_time must be before end_time")
	}
	if end.Sub(start) > statsMaxRange {
		return StatsRange{}, detail(ErrTimeRangeInvalid, "time range exceeds 90 days")
	}
	return StatsRange{Groups: scope.Filter(groupID), Start: start, End: end}, nil
}

// Overview 概览：当前各级别的活跃告警数、未认领数，以及今日（timezone 的自然日）新增 / 恢复 / 关闭数。
func (uc *StatsUsecase) Overview(ctx context.Context, userID, groupID, timezone string) (*AlertOverview, error) {
	loc, _, err := statsLocation(timezone)
	if err != nil {
		return nil, err
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	groups := scope.Filter(groupID)
	if groups.Empty() {
		return &AlertOverview{}, nil
	}
	now := uc.now().In(loc)
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	return uc.reader.Overview(ctx, groups, dayStart, dayStart.AddDate(0, 0, 1))
}

// TrendSeries 趋势里的一条折线；Counts 的下标与 AlertTrend.Days 对应。
type TrendSeries struct {
	Key    string
	Name   string
	Counts []int32
	Total  int32
}

// AlertTrend 按天的告警量。
type AlertTrend struct {
	Days   []string
	Series []*TrendSeries
	Totals []int32
}

// StatsQuery 带时间范围的统计接口的公共入参。By 的取值随接口而定。
type StatsQuery struct {
	GroupID string
	Start   time.Time
	End     time.Time
	By      string
}

// trendDays [start, end) 在 loc 里覆盖的自然日（升序、连续）。跨度已由调用方限制在 90 天内。
func trendDays(start, end time.Time, loc *time.Location) []string {
	first := start.In(loc)
	last := end.Add(-time.Nanosecond).In(loc)
	day := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, loc)
	stop := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, loc)
	days := make([]string, 0, 92)
	for ; !day.After(stop); day = day.AddDate(0, 0, 1) {
		days = append(days, day.Format(time.DateOnly))
	}
	return days
}

// statsBucket 按分组拆分时一个事件归到哪一行：给了 base（group_id）时取 base 的直接下级（事件直接在
// base 里则是 base 自己）；否则取事件所在分组链上最靠上的可读分组（内置根分组只是容器，跳过它，除非
// 事件就在根分组里）。分组已被删除（不在树里）的事件归入「其他」。
func statsBucket(scope Scope, base, groupID string) string {
	chain := scope.Ancestors(groupID)
	if len(chain) == 0 {
		return StatsOtherKey
	}
	if base != "" {
		i := slices.Index(chain, base)
		switch {
		case i < 0:
			return StatsOtherKey
		case i == len(chain)-1:
			return base
		}
		return chain[i+1]
	}
	for _, id := range chain {
		if id != mc.RootGroupID && scope.CanRead(id) {
			return id
		}
	}
	return groupID
}

// groupSeriesName 分组行的显示名：给了 base 时是下级分组自己的名称，否则是完整名称。
func groupSeriesName(scope Scope, base, id string) string {
	if id == StatsOtherKey {
		return statsOtherName
	}
	if g, ok := scope.Group(id); ok && base != "" && id != base {
		return g.Name
	}
	return scope.GroupName(id)
}

// keepTop 把 totals（键 → 数量）里数量最多的 statsGroupSeries 个键留下，返回「键 → 归并后的键」的函数
// 与留下的键（按数量倒序；有被归并的则末尾带 StatsOtherKey）。
func keepTop(totals map[string]int64) (func(string) string, []string) {
	keys := make([]string, 0, len(totals))
	for k := range totals {
		if k != StatsOtherKey {
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Or(cmp.Compare(totals[b], totals[a]), cmp.Compare(a, b))
	})
	_, other := totals[StatsOtherKey]
	if len(keys) > statsGroupSeries {
		keys, other = keys[:statsGroupSeries], true
	}
	kept := slices.Clone(keys)
	if other {
		keys = append(keys, StatsOtherKey)
	}
	return func(k string) string {
		if slices.Contains(kept, k) {
			return k
		}
		return StatsOtherKey
	}, keys
}

// severityKeys 级别行的键（1-4）。
func severityKeys() []string {
	return []string{"1", "2", "3", "4"}
}

func severityKeyName(key string) string {
	n, _ := strconv.ParseInt(key, 10, 32)
	return SeverityLabel(int32(n)) // #nosec G115 -- ParseInt 已限 32 位
}

// Trend 趋势：按天的告警量，按级别（缺省）或规则分组拆分。
func (uc *StatsUsecase) Trend(ctx context.Context, userID string, q StatsQuery, timezone string) (*AlertTrend, error) {
	dim := q.By
	if dim == "" {
		dim = StatsBySeverity
	}
	if dim != StatsBySeverity && dim != StatsByGroup {
		return nil, detail(ErrQueryInvalid, "by must be severity or group")
	}
	loc, tz, err := statsLocation(timezone)
	if err != nil {
		return nil, err
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	r, err := statsRange(scope, q.GroupID, q.Start, q.End)
	if err != nil {
		return nil, err
	}
	out := &AlertTrend{Days: trendDays(q.Start, q.End, loc), Series: []*TrendSeries{}}
	out.Totals = make([]int32, len(out.Days))
	var cells []StatsCell
	if !r.Groups.Empty() {
		if cells, err = uc.reader.Trend(ctx, r, dim, tz); err != nil {
			return nil, err
		}
	}
	dayIndex := make(map[string]int, len(out.Days))
	for i, d := range out.Days {
		dayIndex[d] = i
	}
	keys, name, merge := severityKeys(), severityKeyName, func(k string) string { return k }
	if dim == StatsByGroup {
		totals := map[string]int64{}
		for i := range cells {
			cells[i].Key = statsBucket(scope, q.GroupID, cells[i].Key)
			totals[cells[i].Key] += int64(cells[i].Count)
		}
		merge, keys = keepTop(totals)
		name = func(k string) string { return groupSeriesName(scope, q.GroupID, k) }
	}
	series := make(map[string]*TrendSeries, len(keys))
	for _, k := range keys {
		s := &TrendSeries{Key: k, Name: name(k), Counts: make([]int32, len(out.Days))}
		series[k] = s
		out.Series = append(out.Series, s)
	}
	for _, c := range cells {
		i, ok := dayIndex[c.Day]
		s := series[merge(c.Key)]
		if !ok || s == nil {
			continue // 数据库与应用的时区数据不一致时可能多出边界上的一天：不计入，不越界
		}
		s.Counts[i] += c.Count
		s.Total += c.Count
		out.Totals[i] += c.Count
	}
	return out, nil
}

// EfficiencyItem 处理效率的一行。MTTA / MTTR 为平均秒数，没有可算的事件时为 0。
type EfficiencyItem struct {
	Key      string
	Name     string
	Alerts   int32
	Acked    int32
	Resolved int32
	MTTAS    int64
	MTTRS    int64
}

type efficiencyAcc struct {
	EfficiencyRow
}

func (a *efficiencyAcc) add(r EfficiencyRow) {
	a.Alerts += r.Alerts
	a.Acked += r.Acked
	a.Resolved += r.Resolved
	a.AckSumS += r.AckSumS
	a.ResolveSumS += r.ResolveSumS
}

func (a *efficiencyAcc) item(key, name string) *EfficiencyItem {
	it := &EfficiencyItem{Key: key, Name: name, Alerts: a.Alerts, Acked: a.Acked, Resolved: a.Resolved}
	if a.Acked > 0 {
		it.MTTAS = int64(max(a.AckSumS, 0) / float64(a.Acked))
	}
	if a.Resolved > 0 {
		it.MTTRS = int64(max(a.ResolveSumS, 0) / float64(a.Resolved))
	}
	return it
}

// Efficiency 处理效率：平均认领耗时（首次触发 → 当前这次认领，只算此刻处于已认领的事件——取消认领后
// 认领时间被清空）与平均恢复耗时（只算已恢复的），按规则分组（缺省）或级别拆分，另给汇总行。
func (uc *StatsUsecase) Efficiency(ctx context.Context, userID string, q StatsQuery) ([]*EfficiencyItem, *EfficiencyItem, error) {
	dim := q.By
	if dim == "" {
		dim = StatsByGroup
	}
	if dim != StatsBySeverity && dim != StatsByGroup {
		return nil, nil, detail(ErrQueryInvalid, "by must be group or severity")
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	r, err := statsRange(scope, q.GroupID, q.Start, q.End)
	if err != nil {
		return nil, nil, err
	}
	var rows []EfficiencyRow
	if !r.Groups.Empty() {
		if rows, err = uc.reader.Efficiency(ctx, r, dim); err != nil {
			return nil, nil, err
		}
	}
	keys, name, merge := severityKeys(), severityKeyName, func(k string) string { return k }
	if dim == StatsByGroup {
		totals := map[string]int64{}
		for i := range rows {
			rows[i].Key = statsBucket(scope, q.GroupID, rows[i].Key)
			totals[rows[i].Key] += int64(rows[i].Alerts)
		}
		merge, keys = keepTop(totals)
		name = func(k string) string { return groupSeriesName(scope, q.GroupID, k) }
	}
	accs := make(map[string]*efficiencyAcc, len(keys))
	for _, k := range keys {
		accs[k] = &efficiencyAcc{}
	}
	var total efficiencyAcc
	for _, row := range rows {
		if acc := accs[merge(row.Key)]; acc != nil {
			acc.add(row)
			total.add(row)
		}
	}
	items := make([]*EfficiencyItem, 0, len(keys))
	for _, k := range keys {
		items = append(items, accs[k].item(k, name(k)))
	}
	return items, total.item(StatsTotalKey, statsTotalName), nil
}

// TopItem 排行里的一行。
type TopItem struct {
	TopRow
	GroupName string
	Exists    bool
}

// TopQuery 排行查询入参。By 取 rule / ci / label:<标签名>；Limit 为 0 取缺省。
type TopQuery struct {
	StatsQuery
	Limit int
}

// Top 排行：时间范围内告警最多的规则、资源或某个标签的取值。
func (uc *StatsUsecase) Top(ctx context.Context, userID string, q TopQuery) ([]*TopItem, int64, error) {
	by := TopBy{Kind: q.By}
	if label, ok := strings.CutPrefix(q.By, TopByLabel+":"); ok {
		by = TopBy{Kind: TopByLabel, Label: label}
		if !mc.ValidLabelName(label) {
			return nil, 0, detail(ErrQueryInvalid, "invalid label name")
		}
	} else if q.By != TopByRule && q.By != TopByCI {
		return nil, 0, detail(ErrQueryInvalid, "by must be rule, ci or label:<name>")
	}
	limit := q.Limit
	if limit <= 0 {
		limit = statsTopDefault
	}
	limit = min(limit, statsTopMax)
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	r, err := statsRange(scope, q.GroupID, q.Start, q.End)
	if err != nil {
		return nil, 0, err
	}
	if r.Groups.Empty() {
		return []*TopItem{}, 0, nil
	}
	rows, total, err := uc.ranker.Top(ctx, r, by, limit)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Key)
	}
	items := make([]*TopItem, 0, len(rows))
	switch by.Kind {
	case TopByRule:
		names := map[string]string{}
		if len(ids) > 0 {
			if names, err = uc.rules.Names(ctx, ids); err != nil {
				return nil, 0, err
			}
		}
		for _, row := range rows {
			it := &TopItem{TopRow: row, GroupName: scope.GroupName(row.GroupID)}
			// 规则仍在时用当前名称；已删除的保留事件上记下的名称。
			if cur, ok := names[row.Key]; ok {
				it.Name, it.Exists = cur, true
			}
			items = append(items, it)
		}
	case TopByCI:
		briefs := map[string]*CIEntry{}
		if len(ids) > 0 {
			if briefs, err = uc.cis.Briefs(ctx, ids); err != nil {
				return nil, 0, err
			}
		}
		for _, row := range rows {
			it := &TopItem{TopRow: row}
			if ci := briefs[row.Key]; ci != nil {
				it.Name, it.Exists = ci.Name, ci.Valid
			}
			it.GroupID = ""
			items = append(items, it)
		}
	default:
		for _, row := range rows {
			row.Name, row.GroupID = row.Key, ""
			items = append(items, &TopItem{TopRow: row, Exists: true})
		}
	}
	return items, total, nil
}

// Noise 降噪效果（口径见 AlertNoise）。
func (uc *StatsUsecase) Noise(ctx context.Context, userID string, q StatsQuery) (*AlertNoise, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	r, err := statsRange(scope, q.GroupID, q.Start, q.End)
	if err != nil {
		return nil, err
	}
	if r.Groups.Empty() {
		return &AlertNoise{}, nil
	}
	return uc.ranker.Noise(ctx, r)
}
