package promclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// Remote Write 的限额。
const (
	// MaxWriteBytes 一次写入的请求体上限（压缩前的 protobuf 字节数），超出返回 ErrTypeBadData，不发请求。
	MaxWriteBytes = 16 << 20
	// maxWriteErrorBody 非 2xx 时为连接复用而读掉的响应体上限（内容不进错误信息）。
	maxWriteErrorBody = 64 << 10
	// remoteWriteVersion Prometheus Remote Write 1.0 协议的版本头取值。
	remoteWriteVersion = "0.1.0"
	// metricNameLabel 指标名所在的标签。
	metricNameLabel = "__name__"
)

// prometheus.WriteRequest / TimeSeries / Label / Sample 的字段号（prompb/remote.proto、types.proto）。
const (
	fieldWriteTimeseries   = 1
	fieldTimeSeriesLabels  = 1
	fieldTimeSeriesSamples = 2
	fieldLabelName         = 1
	fieldLabelValue        = 2
	fieldSampleValue       = 1
	fieldSampleTimestamp   = 2
)

// parseWriteURL 校验 Remote Write 地址：与查询地址同一口径（仅 http / https、不带凭据与 fragment），
// 但它是完整的写入地址（不追加路径），允许带 query（如 VictoriaMetrics 的 extra_label）。
func parseWriteURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", errors.New("promclient: invalid write url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("promclient: write url scheme must be http or https")
	}
	if u.User != nil {
		return "", errors.New("promclient: write url must not contain credentials")
	}
	if u.Hostname() == "" || u.Opaque != "" {
		return "", errors.New("promclient: write url host is required")
	}
	if u.Fragment != "" {
		return "", errors.New("promclient: write url must not contain a fragment")
	}
	return u.String(), nil
}

// ValidateWriteURL 校验 Remote Write 地址的格式（保存数据源时调用，口径与 New 相同）：仅 http / https，
// 不带凭据与 fragment，须有主机。不发起网络请求。
func ValidateWriteURL(raw string) error {
	_, err := parseWriteURL(raw)
	return err
}

// Write 经 Prometheus Remote Write 1.0 协议（protobuf WriteRequest + Snappy 块压缩）把采样点写入
// Config.WriteURL。超时、不跟随跳转、DialGuard、认证与自定义请求头、错误信息里的凭据清除都与查询一致。
//
// 每条序列的标签须含指标名（__name__）；标签名须非空，取值为空的标签被省略（Prometheus 语义：空 = 不存在）；
// 标签按名称排序后写出；采样时间按毫秒截断；没有采样点的序列被跳过。全部序列都没有采样点时不发请求、返回 nil。
// series 只读。
//
// 失败返回 *Error：未配置写入地址、序列不合法或请求体超过 MaxWriteBytes 为 ErrTypeBadData（未发请求）；
// 上游非 2xx 为 ErrTypeHTTP 并带 Status（400 表示数据被拒绝，重试无益；429 / 5xx 可重试），响应体不进
// 错误信息；3xx 为 ErrTypeRedirect。
func (c *Client) Write(ctx context.Context, series []Series) error {
	if c.writeURL == "" {
		return &Error{ErrorType: ErrTypeBadData, Message: "remote write url is not configured"}
	}
	payload, err := encodeWriteRequest(series)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	body := snappyEncode(make([]byte, 0, len(payload)/4+64), payload)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.writeURL, bytes.NewReader(body))
	if err != nil {
		return &Error{ErrorType: ErrTypeNetwork, Message: "build request failed"}
	}
	for k, v := range c.header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", remoteWriteVersion)

	resp, err := c.hc.Do(req)
	if err != nil {
		return c.transportError(ctx, err, 0)
	}
	defer func() { _ = resp.Body.Close() }()
	// 读掉（有上限的）响应体以便连接复用；内容一律不进错误信息（理由同 statusError）。
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxWriteErrorBody))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return &Error{ErrorType: ErrTypeRedirect, Status: resp.StatusCode, Message: "upstream answered with a redirect; redirects are not followed"}
	}
	return &Error{ErrorType: ErrTypeHTTP, Status: resp.StatusCode, Message: statusReason(resp.StatusCode)}
}

// encodeWriteRequest 把序列编码为 prometheus.WriteRequest；没有任何采样点时返回 nil, nil。
func encodeWriteRequest(series []Series) ([]byte, error) {
	var out, ts, sub []byte
	var names []string
	for i := range series {
		s := &series[i]
		if len(s.Points) == 0 {
			continue
		}
		if s.Labels[metricNameLabel] == "" {
			return nil, &Error{ErrorType: ErrTypeBadData, Message: "series without a metric name (__name__)"}
		}
		names = names[:0]
		for name, value := range s.Labels {
			if name == "" || !utf8.ValidString(name) || !utf8.ValidString(value) {
				return nil, &Error{ErrorType: ErrTypeBadData, Message: "series with an empty or non UTF-8 label"}
			}
			if value != "" {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		ts = ts[:0]
		for _, name := range names {
			sub = protowire.AppendTag(sub[:0], fieldLabelName, protowire.BytesType)
			sub = protowire.AppendString(sub, name)
			sub = protowire.AppendTag(sub, fieldLabelValue, protowire.BytesType)
			sub = protowire.AppendString(sub, s.Labels[name])
			ts = protowire.AppendTag(ts, fieldTimeSeriesLabels, protowire.BytesType)
			ts = protowire.AppendBytes(ts, sub)
		}
		for _, p := range s.Points {
			sub = protowire.AppendTag(sub[:0], fieldSampleValue, protowire.Fixed64Type)
			sub = protowire.AppendFixed64(sub, math.Float64bits(p.Value))
			sub = protowire.AppendTag(sub, fieldSampleTimestamp, protowire.VarintType)
			sub = protowire.AppendVarint(sub, uint64(p.Time.UnixMilli())) // #nosec G115 -- int64 按补码编码为 varint（protobuf int64 的线格式）
			ts = protowire.AppendTag(ts, fieldTimeSeriesSamples, protowire.BytesType)
			ts = protowire.AppendBytes(ts, sub)
		}
		out = protowire.AppendTag(out, fieldWriteTimeseries, protowire.BytesType)
		out = protowire.AppendBytes(out, ts)
		if len(out) > MaxWriteBytes {
			return nil, &Error{ErrorType: ErrTypeBadData, Message: "write request exceeds the size limit; write fewer series at a time"}
		}
	}
	return out, nil
}
