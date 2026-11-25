package biz

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

// 记录规则分片写回中途失败（monitor 模块评审 P3-2）：此前要等一个完整的记录周期才重做，期间新指标里只有
// 部分序列，引用它的告警规则会把缺失的序列当成「消失」。
func TestRecordUsecase_PartialWriteFailureIsRetriedAfterAShortDelay(t *testing.T) {
	// 第 2 个写回请求以 500 拒绝（hits 置 0 后重新计数）；其余正常接收。
	var hits atomic.Int32
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(flaky.Close)
	h := newRecordHarness(t, 5000, func(d *Datasource) { d.RemoteWriteURL = flaky.URL })
	samples := make([]promtest.Sample, 1200) // 3 个分片：500 + 500 + 200
	for i := range samples {
		samples[i] = promtest.S(float64(i), "instance", fmt.Sprintf("host-%04d", i))
	}
	h.prom.Set(recordExpr, samples...)

	// runWith 同 recordHarness.run，但能指定任务行上一轮的状态（领取时带回）。
	runWith := func(lastStatus int32) (*RecordResult, string) {
		t.Helper()
		h.repo.mu.Lock()
		before := len(h.repo.finished)
		h.repo.due = []*Task{{RuleID: testRuleID, DatasourceID: testDSID, LastStatus: lastStatus}}
		h.repo.mu.Unlock()
		if n, err := h.uc.RunOnce(context.Background(), "tok"); err != nil || n != 1 {
			t.Fatalf("RunOnce() = %d, %v", n, err)
		}
		h.repo.mu.Lock()
		defer h.repo.mu.Unlock()
		result := h.obs.records[len(h.obs.records)-1]
		if len(h.repo.finished) == before {
			return nil, result
		}
		return &h.repo.finished[len(h.repo.finished)-1], result
	}
	wantInterval := func(what string, interval time.Duration, short bool) {
		t.Helper()
		switch {
		case short && interval != retryDelay:
			t.Fatalf("%s: next run in %v, want the short retry delay %v", what, interval, retryDelay)
		case !short && (interval < time.Minute || interval > time.Minute+evalJitter):
			t.Fatalf("%s: next run in %v, want a regular record interval", what, interval)
		}
	}

	// 第 1 片写成、第 2 片失败（上一轮正常）：写回失败，已写出 500 条；隔 retryDelay 就重做。
	hits.Store(0)
	res, result := runWith(monitorcontract.RecordOK)
	if res == nil || res.Status != monitorcontract.RecordWriteError || result != ResultWriteError || res.Series != recordWriteSeries ||
		!strings.Contains(res.Error, "wrote 500 of 1200 series") {
		t.Fatalf("partial failure: result = %+v, %s", res, result)
	}
	wantInterval("partial failure", res.Interval, true)

	// 重做：全部写出，回到正常的记录周期。
	hits.Store(1000)
	if res, result = runWith(monitorcontract.RecordWriteError); res == nil || res.Status != monitorcontract.RecordOK || result != ResultOK ||
		res.Series != 1200 || h.stats.fails.Load() != 1 {
		t.Fatalf("redo: result = %+v, %s", res, result)
	}
	wantInterval("after a successful redo", res.Interval, false)

	// 连续两轮都是写回失败：第二轮回到记录周期，不给已经吃紧的写入端加压。
	hits.Store(0)
	if res, _ = runWith(monitorcontract.RecordOK); res == nil || res.Status != monitorcontract.RecordWriteError {
		t.Fatalf("second partial failure: result = %+v", res)
	}
	wantInterval("first failure of an episode", res.Interval, true)
	hits.Store(0)
	if res, _ = runWith(monitorcontract.RecordWriteError); res == nil || res.Status != monitorcontract.RecordWriteError || res.Series != recordWriteSeries {
		t.Fatalf("consecutive partial failure: result = %+v", res)
	}
	wantInterval("consecutive failure", res.Interval, false)

	// 第 1 片就失败（什么都没写出）：不是「部分写入」，照旧等一个记录周期。
	hits.Store(1)
	if res, _ = runWith(monitorcontract.RecordOK); res == nil || res.Status != monitorcontract.RecordWriteError || res.Series != 0 {
		t.Fatalf("failure before anything was written: result = %+v", res)
	}
	wantInterval("nothing written", res.Interval, false)
}
