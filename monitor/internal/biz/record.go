package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 记录规则的固定参数。
const (
	// recordWriteSeries / recordWriteBytes 一次 Remote Write 请求至多携带的序列数与（估算的）标签字节数：
	// 大结果分多次写，单个请求不会顶到 promclient.MaxWriteBytes。
	recordWriteSeries = 500
	recordWriteBytes  = 4 << 20
	// maxRecordLabels 写回的序列的标签数上限（不含 __name__）：上游标签上限 + 规则附加标签上限。
	maxRecordLabels = maxSeriesLabels + monitorcontract.MaxRuleLabels
)

// errDuplicateSeries 结果里有两条序列在去掉指标名、套上规则标签之后标签完全相同。
var errDuplicateSeries = errors.New("result has series with identical labels after applying the rule labels")

// RecordUsecase 记录规则执行器：领取到期任务 → instant 查询 → 结果改名为规则的指标名、套上规则标签 →
// 经数据源的 Remote Write 地址写回 → 写回任务。任务的领取 / 租约 / 围栏 / 抖动与评估任务是同一套
// （TaskRepo、claimAndRun、nextInterval）。
//
// 写回发生在围栏检查之前（网络 I/O 不可能放进事务）：租约过期被别的实例重领时，同一轮可能被写两次。
// 两次写的是各自查询得到的采样点，对时序库是多一个点，不是错误的数据——所以这里是至少一次。
//
// 结果不完整（被 max_series 截断，或上游声明是部分结果）时整轮不写：只写一部分序列，其余序列在新指标里
// 就成了「消失」，引用它的告警规则会据此判恢复；而整轮缺一个点，对消费方只是一次普通的数据缺口。
type RecordUsecase struct {
	repo  RecordRepo
	cache *ConfigCache
	stats *InstanceStats
	obs   Observer
	log   *slog.Logger
	opts  EvaluatorOptions
	// failing 失败日志的限频状态（按记录规则）。
	failing failureLog
}

// NewRecordUsecase new a RecordUsecase.
func NewRecordUsecase(repo RecordRepo, cache *ConfigCache, stats *InstanceStats, obs Observer, log *slog.Logger) *RecordUsecase {
	return &RecordUsecase{
		repo: repo, cache: cache, stats: stats, obs: obs, log: log,
		opts: EvaluatorOptions{Batch: 10, Lease: time.Minute, QueryTimeoutMax: promclient.MaxTimeout},
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *RecordUsecase) WithOptions(o EvaluatorOptions) *RecordUsecase {
	if o.Batch > 0 {
		uc.opts.Batch = o.Batch
	}
	if o.Lease > 0 {
		uc.opts.Lease = o.Lease
	}
	if o.QueryTimeoutMax > 0 {
		uc.opts.QueryTimeoutMax = min(o.QueryTimeoutMax, promclient.MaxTimeout)
	}
	return uc
}

// RunOnce 领取一批到期的记录规则任务并发执行，返回领取数。停机语义同评估器。
func (uc *RecordUsecase) RunOnce(ctx context.Context, token string) (int, error) {
	if err := refreshCaches(ctx, uc.cache, nil); err != nil || !uc.cache.Loaded() {
		return 0, err
	}
	return claimAndRun(ctx, uc.repo, token, uc.opts, uc.log, uc.runOne)
}

func (uc *RecordUsecase) runOne(ctx context.Context, task *Task) {
	start := time.Now()
	rctx, cancel := context.WithTimeout(ctx, roundQueryBudgets*uc.opts.QueryTimeoutMax+settleTimeout)
	result, series, err := uc.run(rctx, task, start)
	cancel()
	switch {
	case errors.Is(err, ErrLeaseLost):
		result = ResultLeaseLost
		uc.log.InfoContext(ctx, "record round discarded: lease lost", "record_rule_id", task.RuleID)
	case err != nil:
		result = ResultFailed
		uc.log.ErrorContext(ctx, "record round failed", "record_rule_id", task.RuleID, "error", err)
		releaseTasks(ctx, uc.repo, task.Token, []*Task{task}, retryDelay, uc.log)
	}
	uc.stats.observe(result, task.Lag)
	uc.obs.ObserveRecord(ctx, result, time.Since(start), series)
}

// run 一次记录规则执行，返回结果标签与写回的序列数。
func (uc *RecordUsecase) run(ctx context.Context, task *Task, start time.Time) (string, int, error) {
	rule, now, err := uc.repo.LoadRecordRule(ctx, task.RuleID)
	if err != nil && !errors.Is(err, ErrRuleNotFound) {
		return "", 0, fmt.Errorf("load record rule: %w", err)
	}
	var h *DatasourceHandle
	if err == nil {
		h = uc.cache.Datasource(rule.DatasourceID)
	}
	if err != nil || !rule.Active || h == nil || !h.Info.Enabled {
		// 规则或数据源已不可执行：等规划器删除任务。
		if err := uc.repo.Release(ctx, task.Token, []*Task{task}, retryDelay); err != nil {
			return "", 0, fmt.Errorf("release unrunnable record task: %w", err)
		}
		return ResultSkipped, 0, nil
	}
	finish := func(status int32, result, msg string, series int) (string, int, error) {
		res := &RecordResult{
			Status: status, Error: truncate(msg, maxErrorBytes), Duration: time.Since(start),
			Series: int32(series), Interval: nextInterval(rule.Interval), // #nosec G115 -- 序列数受 max_series 约束
		}
		if err := uc.repo.Finish(ctx, task, res); err != nil {
			return "", 0, err
		}
		failed := status == monitorcontract.RecordQueryError || status == monitorcontract.RecordWriteError
		if uc.failing.observe(rule.ID, failed, time.Now()) {
			attrs := []any{"record_rule_id", rule.ID, "metric", rule.Name, "datasource_id", h.Info.ID, "datasource_name", h.Info.Name}
			if failed {
				uc.log.WarnContext(ctx, "record rule is failing", append(attrs, "error", res.Error)...)
			} else {
				uc.log.InfoContext(ctx, "record rule recovered", attrs...)
			}
		}
		return result, series, nil
	}
	if !monitorcontract.ValidMetricName(rule.Name) {
		return finish(monitorcontract.RecordQueryError, ResultError, "record rule name is not a valid metric name", 0)
	}
	if msg := h.Unavailable(); msg != "" {
		return finish(monitorcontract.RecordQueryError, ResultError, msg, 0)
	}
	// 与评估查询共用该数据源的并发槽；排不上就归还稍后重试，不记失败。
	res, err := h.Query(ctx, time.Now().Add(h.Timeout()), rule.Expr)
	var perr *promclient.Error
	switch {
	case errors.As(err, &perr):
		return finish(monitorcontract.RecordQueryError, ResultError, perr.Error(), 0)
	case err != nil:
		if err := uc.repo.Release(ctx, task.Token, []*Task{task}, retryDelay); err != nil {
			return "", 0, fmt.Errorf("release postponed record task: %w", err)
		}
		return ResultBusy, 0, nil
	case res.Type != promclient.ResultVector && res.Type != promclient.ResultScalar:
		return finish(monitorcontract.RecordQueryError, ResultError,
			"query must return an instant vector or a scalar, got a "+resultTypeName(res.Type), 0)
	case res.Truncated || res.Partial:
		why := "the datasource returned a partial result"
		if res.Truncated {
			why = "the result was truncated to " + strconv.Itoa(len(res.Series)) + " series"
		}
		return finish(monitorcontract.RecordQueryError, ResultIncomplete, incompletePrefix+why+"; nothing was written", 0)
	}
	series, rejected, err := recordSeries(rule, res.Series, now)
	if err != nil {
		return finish(monitorcontract.RecordQueryError, ResultError, err.Error(), 0)
	}
	note := ""
	if rejected > 0 {
		note = strconv.Itoa(rejected) + " series were not written: their labels do not form a valid metric"
	}
	switch {
	case len(series) == 0 && rejected > 0:
		// 有结果但一条都写不出去：算写回失败，不能和「查询无结果」混为一谈。
		return finish(monitorcontract.RecordWriteError, ResultWriteError, note, 0)
	case len(series) == 0:
		return finish(monitorcontract.RecordNoData, ResultNoData, "", 0)
	}
	// 全部分片共用一个查询超时：一轮 = 等槽 + 查询 + 写回，各至多一个查询超时（与租约的算法一致）。
	wctx, cancel := context.WithTimeout(ctx, h.Timeout())
	defer cancel()
	written := 0
	for _, chunk := range chunkSeries(series) {
		if err := h.Write(wctx, chunk); err != nil {
			msg := err.Error() + "; wrote " + strconv.Itoa(written) + " of " + strconv.Itoa(len(series)) + " series"
			return finish(monitorcontract.RecordWriteError, ResultWriteError, joinNotes(msg, note), written)
		}
		written += len(chunk)
	}
	return finish(monitorcontract.RecordOK, ResultOK, note, written)
}

// recordSeries 把查询结果变成要写回的序列：去掉原指标名、套上规则标签（取值为空 = 删除该标签，
// 与 Prometheus 记录规则一致）、指标名改为规则名，每条只带最新的一个采样点（没有时间戳的用 now）。
// 标签不合法的序列（标签名不合法或以 __ 开头、取值不是合法 UTF-8 / 含 NUL / 超过 1024 字节、标签过多）
// 被拒绝并计数——写出去也会被时序库拒绝或变成另一条序列；处理后标签完全相同的两条序列使整轮失败
// （errDuplicateSeries）。series 的 Labels 被就地改写并由返回值接管。纯函数。
func recordSeries(rule *RecordRule, series []promclient.Series, now time.Time) (out []promclient.Series, rejected int, err error) {
	out = make([]promclient.Series, 0, len(series))
	seen := make(map[string]struct{}, len(series))
	for i := range series {
		points := series[i].Points
		if len(points) == 0 {
			continue
		}
		labels := series[i].Labels
		if labels == nil {
			labels = make(map[string]string, len(rule.Labels)+1)
		}
		delete(labels, "__name__")
		for name, value := range rule.Labels {
			if value == "" {
				delete(labels, name)
			} else {
				labels[name] = value
			}
		}
		if !validRecordLabels(labels) {
			rejected++
			continue
		}
		key := monitorcontract.Fingerprint(rule.ID, "", labels, 0)
		if _, dup := seen[key]; dup {
			return nil, 0, errDuplicateSeries
		}
		seen[key] = struct{}{}
		labels["__name__"] = rule.Name
		point := points[len(points)-1]
		if point.Time.IsZero() {
			point.Time = now
		}
		out = append(out, promclient.Series{Labels: labels, Points: []promclient.Point{point}})
	}
	return out, rejected, nil
}

func validRecordLabels(labels map[string]string) bool {
	if len(labels) > maxRecordLabels {
		return false
	}
	for name, value := range labels {
		if !monitorcontract.ValidLabelName(name) || strings.HasPrefix(name, "__") {
			return false
		}
		if len(value) > maxLabelValueBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return false
		}
	}
	return true
}

// chunkSeries 把序列切成若干次写入：每次至多 recordWriteSeries 条、标签合计约 recordWriteBytes 字节。
func chunkSeries(series []promclient.Series) [][]promclient.Series {
	var chunks [][]promclient.Series
	start, size := 0, 0
	for i := range series {
		n := 32
		for name, value := range series[i].Labels {
			n += len(name) + len(value) + 8
		}
		if i > start && (i-start >= recordWriteSeries || size+n > recordWriteBytes) {
			chunks = append(chunks, series[start:i])
			start, size = i, 0
		}
		size += n
	}
	return append(chunks, series[start:])
}
