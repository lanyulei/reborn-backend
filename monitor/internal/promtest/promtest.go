// Package promtest 提供测试用的假 Prometheus 查询端（httptest）：按表达式返回预置的 instant 结果，
// 供评估器的单元测试与数据层的集成测试共用。只被 _test 文件引用，不进入服务二进制的依赖图。
package promtest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Sample 一条 instant 查询结果序列。
type Sample struct {
	Labels map[string]string
	Value  float64
}

// S 构造一条序列：kv 为标签的键值对。
func S(value float64, kv ...string) Sample {
	labels := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	return Sample{Labels: labels, Value: value}
}

type reply struct {
	samples []Sample
	status  int
	errType string
	message string
	// scalar 非 nil 时返回标量结果。
	scalar *float64
}

// Server 假 Prometheus。未预置的表达式返回空向量；表达式 `1` 恒返回标量 1（探活）。并发安全。
type Server struct {
	// URL 查询地址（不含 /api/v1/query）。
	URL string

	mu      sync.Mutex
	replies map[string]reply
	hits    map[string]int
	headers []http.Header
	delay   time.Duration
	down    bool
	partial bool
	// inFlight / maxInFlight 当前与历史最高的并发请求数。
	inFlight, maxInFlight int
}

// New 启动一个假 Prometheus，测试结束时自动关闭。
func New(t testing.TB) *Server {
	t.Helper()
	s := &Server{replies: map[string]reply{}, hits: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(srv.Close)
	s.URL = srv.URL
	return s
}

// Set 预置表达式的结果（覆盖此前的结果或错误）。
func (s *Server) Set(expr string, samples ...Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[expr] = reply{samples: samples}
}

// SetError 让表达式返回 Prometheus 错误结构。
func (s *Server) SetError(expr string, status int, errType, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[expr] = reply{status: status, errType: errType, message: message}
}

// SetScalar 让表达式返回标量结果（如 time()）。
func (s *Server) SetScalar(expr string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[expr] = reply{scalar: &value}
}

// SetPartial 为真时所有成功响应带 "isPartial": true（VictoriaMetrics 集群版的部分结果）。
func (s *Server) SetPartial(partial bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partial = partial
}

// MaxInFlight 历史最高的并发请求数。
func (s *Server) MaxInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInFlight
}

// SetDelay 每个请求应答前等待的时长。
func (s *Server) SetDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay = d
}

// SetDown 为真时所有请求返回 503（非 Prometheus 错误结构）。
func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

// Hits 表达式被查询的次数。
func (s *Server) Hits(expr string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[expr]
}

// TotalHits 全部请求数。
func (s *Server) TotalHits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.hits {
		n += c
	}
	return n
}

// LastHeader 最近一次请求的请求头；尚无请求返回 nil。
func (s *Server) LastHeader() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.headers) == 0 {
		return nil
	}
	return s.headers[len(s.headers)-1]
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expr := r.Form.Get("query")
	s.mu.Lock()
	s.hits[expr]++
	s.headers = append(s.headers[max(0, len(s.headers)-15):], r.Header.Clone())
	rep, delay, down, partial := s.replies[expr], s.delay, s.down, s.partial
	s.inFlight++
	s.maxInFlight = max(s.maxInFlight, s.inFlight)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if down {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream unavailable"))
		return
	}
	now := float64(time.Now().Unix())
	if rep.status != 0 {
		w.WriteHeader(rep.status)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "errorType": rep.errType, "error": rep.message})
		return
	}
	if expr == "1" && rep.samples == nil && rep.scalar == nil {
		one := 1.0
		rep.scalar = &one
	}
	if rep.scalar != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success", "isPartial": partial,
			"data": map[string]any{"resultType": "scalar", "result": []any{now, strconv.FormatFloat(*rep.scalar, 'g', -1, 64)}},
		})
		return
	}
	result := make([]map[string]any, 0, len(rep.samples))
	for _, sm := range rep.samples {
		labels := sm.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		result = append(result, map[string]any{
			"metric": labels, "value": []any{now, strconv.FormatFloat(sm.Value, 'g', -1, 64)},
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "success", "isPartial": partial, "data": map[string]any{"resultType": "vector", "result": result},
	})
}
