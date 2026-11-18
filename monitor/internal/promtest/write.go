package promtest

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// WrittenSeries Remote Write 接收端解码出的一条序列（只取每条序列的第一个采样点）。
type WrittenSeries struct {
	Labels      map[string]string
	Value       float64
	TimestampMS int64
}

// WriteReceiver 假的 Prometheus Remote Write 接收端：校验请求头，解 Snappy 块压缩与 protobuf
// WriteRequest，把收到的序列按请求记下来。并发安全。
type WriteReceiver struct {
	// URL 完整的写入地址。
	URL string

	mu       sync.Mutex
	requests [][]WrittenSeries
	headers  []http.Header
	status   int
	problems []string
}

// NewWriteReceiver 启动一个接收端，测试结束时自动关闭。
func NewWriteReceiver(t testing.TB) *WriteReceiver {
	t.Helper()
	r := &WriteReceiver{}
	srv := httptest.NewServer(http.HandlerFunc(r.handle))
	t.Cleanup(srv.Close)
	r.URL = srv.URL + "/api/v1/write"
	return r
}

// SetStatus 非 0 时所有请求以该状态码拒绝（不记录内容）。
func (r *WriteReceiver) SetStatus(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
}

// Requests 收到的写入请求（每个请求一组序列）。
func (r *WriteReceiver) Requests() [][]WrittenSeries {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]WrittenSeries(nil), r.requests...)
}

// Series 全部请求里的序列，按到达顺序拼接。
func (r *WriteReceiver) Series() []WrittenSeries {
	var out []WrittenSeries
	for _, req := range r.Requests() {
		out = append(out, req...)
	}
	return out
}

// LastHeader 最近一次请求的请求头；尚无请求返回 nil。
func (r *WriteReceiver) LastHeader() http.Header {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.headers) == 0 {
		return nil
	}
	return r.headers[len(r.headers)-1]
}

// Problems 协议层面的问题（请求头不对、解压或解码失败）；空 = 全部请求都合规。
func (r *WriteReceiver) Problems() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.problems...)
}

func (r *WriteReceiver) handle(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, 64<<20))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headers = append(r.headers[max(0, len(r.headers)-15):], req.Header.Clone())
	if r.status != 0 {
		w.WriteHeader(r.status)
		return
	}
	fail := func(msg string) {
		r.problems = append(r.problems, msg)
		http.Error(w, msg, http.StatusBadRequest)
	}
	switch {
	case err != nil:
		fail("read body: " + err.Error())
		return
	case req.Method != http.MethodPost:
		fail("method " + req.Method)
		return
	case req.Header.Get("Content-Encoding") != "snappy" || req.Header.Get("Content-Type") != "application/x-protobuf" ||
		req.Header.Get("X-Prometheus-Remote-Write-Version") != "0.1.0":
		fail("unexpected remote write headers")
		return
	}
	raw, err := snappyDecode(body)
	if err != nil {
		fail(err.Error())
		return
	}
	series, err := decodeWriteRequest(raw)
	if err != nil {
		fail(err.Error())
		return
	}
	r.requests = append(r.requests, series)
	w.WriteHeader(http.StatusNoContent)
}

var errSnappy = errors.New("snappy: corrupt input")

// snappyDecode Snappy 块格式的解码器，按格式说明独立实现，用来验证引擎发出的请求体确实可解。
// dup: kit/promclient 的测试里有一份同样用途的解码器，但它在 _test 文件里、无法引用；本包只被测试使用。
func snappyDecode(src []byte) ([]byte, error) {
	want, n := binary.Uvarint(src)
	if n <= 0 || want > 256<<20 {
		return nil, errSnappy
	}
	src = src[n:]
	out := make([]byte, 0, want)
	for len(src) > 0 {
		tag := src[0]
		var length, offset, used int
		switch tag & 3 {
		case 0:
			length, used = int(tag>>2)+1, 1
			if extra := int(tag>>2) - 59; extra > 0 {
				if len(src) < 1+extra {
					return nil, errSnappy
				}
				length = 0
				for i := range extra {
					length |= int(src[1+i]) << (8 * i)
				}
				length++
				used = 1 + extra
			}
			if used+length > len(src) {
				return nil, errSnappy
			}
			out = append(out, src[used:used+length]...)
			src = src[used+length:]
			continue
		case 1:
			if len(src) < 2 {
				return nil, errSnappy
			}
			length, offset, used = int(tag>>2&7)+4, int(tag>>5)<<8|int(src[1]), 2
		case 2:
			if len(src) < 3 {
				return nil, errSnappy
			}
			length, offset, used = int(tag>>2)+1, int(binary.LittleEndian.Uint16(src[1:])), 3
		default:
			if len(src) < 5 {
				return nil, errSnappy
			}
			length, offset, used = int(tag>>2)+1, int(binary.LittleEndian.Uint32(src[1:])), 5
		}
		if offset == 0 || offset > len(out) {
			return nil, errSnappy
		}
		for range length { // 逐字节：拷贝可以与自身重叠
			out = append(out, out[len(out)-offset])
		}
		src = src[used:]
	}
	if uint64(len(out)) != want {
		return nil, errSnappy
	}
	return out, nil
}

var errProto = errors.New("remote write: malformed WriteRequest")

// fields 遍历一段 protobuf 消息的字段：bytes 字段交给 onBytes，其余类型交给 onScalar（原始数值）。
func fields(b []byte, onBytes func(num protowire.Number, v []byte) error, onScalar func(num protowire.Number, v uint64)) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return errProto
		}
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return errProto
			}
			if err := onBytes(num, v); err != nil {
				return err
			}
			b = b[m:]
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return errProto
			}
			onScalar(num, v)
			b = b[m:]
		case protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(b)
			if m < 0 {
				return errProto
			}
			onScalar(num, v)
			b = b[m:]
		default:
			return errProto
		}
	}
	return nil
}

// decodeWriteRequest 解 prometheus.WriteRequest（timeseries = 1；TimeSeries: labels = 1、samples = 2；
// Label: name = 1、value = 2；Sample: value = 1、timestamp = 2）。
func decodeWriteRequest(raw []byte) ([]WrittenSeries, error) {
	var out []WrittenSeries
	none := func(protowire.Number, uint64) {}
	err := fields(raw, func(num protowire.Number, ts []byte) error {
		if num != 1 {
			return nil
		}
		s := WrittenSeries{Labels: map[string]string{}}
		samples := 0
		err := fields(ts, func(num protowire.Number, sub []byte) error {
			switch num {
			case 1:
				var name, value string
				if err := fields(sub, func(n protowire.Number, v []byte) error {
					switch n {
					case 1:
						name = string(v)
					case 2:
						value = string(v)
					}
					return nil
				}, none); err != nil {
					return err
				}
				s.Labels[name] = value
			case 2:
				samples++
				if samples > 1 {
					return nil
				}
				return fields(sub, func(protowire.Number, []byte) error { return errProto }, func(n protowire.Number, v uint64) {
					switch n {
					case 1:
						s.Value = math.Float64frombits(v)
					case 2:
						s.TimestampMS = int64(v) // #nosec G115 -- protobuf int64 的 varint 就是补码
					}
				})
			}
			return nil
		}, none)
		if err != nil {
			return err
		}
		if samples == 0 {
			return errProto
		}
		out = append(out, s)
		return nil
	}, none)
	return out, err
}
