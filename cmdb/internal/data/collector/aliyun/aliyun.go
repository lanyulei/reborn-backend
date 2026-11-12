// Package aliyun 阿里云 ECS 采集器：DescribeInstances（2014-05-26）RPC 风格 API，
// HMAC-SHA1 签名（V1），分页全量拉取。只用标准库：签名算法稳定、可审计，避免引入
// 庞大的 SDK 依赖树。输出固定映射到内置类型 cloud_host。
package aliyun

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // 阿里云 V1 签名算法规定 HMAC-SHA1，非用于密码学安全场景
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

const (
	defaultEndpoint = "https://ecs.aliyuncs.com"
	apiVersion      = "2014-05-26"
	pageSize        = 100
	maxPages        = 10000
	maxRetries      = 3
	maxBody         = 8 << 20
)

// Collector implements biz.Collector（provider=aliyun）。
type Collector struct {
	client *http.Client
	now    func() time.Time
}

// New new a Collector（连接复用、禁跳转、请求级超时）。
func New(timeout time.Duration) *Collector {
	return &Collector{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns: 16, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
				DialContext: (&net.Dialer{Timeout: timeout}).DialContext, TLSHandshakeTimeout: timeout,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now,
	}
}

// Provider implements biz.Collector.
func (c *Collector) Provider() string { return cmdbcontract.ProviderAliyun }

// describeResponse DescribeInstances 响应（仅用到的字段）。
type describeResponse struct {
	RequestID  string `json:"RequestId"`
	TotalCount int    `json:"TotalCount"`
	PageNumber int    `json:"PageNumber"`
	Instances  struct {
		Instance []instance `json:"Instance"`
	} `json:"Instances"`
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

type ipList struct {
	IPAddress []string `json:"IpAddress"`
}

type instance struct {
	InstanceID    string `json:"InstanceId"`
	InstanceName  string `json:"InstanceName"`
	RegionID      string `json:"RegionId"`
	ZoneID        string `json:"ZoneId"`
	InstanceType  string `json:"InstanceType"`
	CPU           int    `json:"Cpu"`
	Memory        int    `json:"Memory"`
	OSName        string `json:"OSName"`
	Status        string `json:"Status"`
	VpcAttributes struct {
		PrivateIPAddress ipList `json:"PrivateIpAddress"`
	} `json:"VpcAttributes"`
	InnerIPAddress  ipList `json:"InnerIpAddress"`
	PublicIPAddress ipList `json:"PublicIpAddress"`
	EipAddress      struct {
		IPAddress string `json:"IpAddress"`
	} `json:"EipAddress"`
}

// Discover implements biz.Collector：按页拉取直至 TotalCount 覆盖；单页 5xx/网络错误重试 ≤3 次。
func (c *Collector) Discover(ctx context.Context, src *biz.Source, credential string, sink biz.Sink) error {
	cfg, err := cmdbcontract.DecodeConfig(cmdbcontract.ProviderAliyun, src.Config)
	if err != nil {
		return err
	}
	ac, ok := cfg.(*cmdbcontract.AliyunConfig)
	if !ok {
		return errors.New("aliyun: unexpected config type")
	}
	endpoint := ac.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	seen := 0
	for page := 1; page <= maxPages; page++ {
		resp, err := c.describe(ctx, endpoint, ac, credential, page)
		if err != nil {
			return err
		}
		for i := range resp.Instances.Instance {
			if err := sink.Put(ctx, toResource(&resp.Instances.Instance[i], ac.RegionID)); err != nil {
				return err
			}
		}
		seen += len(resp.Instances.Instance)
		if len(resp.Instances.Instance) == 0 || seen >= resp.TotalCount {
			return nil
		}
	}
	return fmt.Errorf("aliyun: too many pages (>%d)", maxPages)
}

func toResource(in *instance, region string) *biz.Resource {
	attrs := map[string]any{
		"provider":      cmdbcontract.ProviderAliyun,
		"instance_id":   in.InstanceID,
		"region":        firstNonEmpty(in.RegionID, region),
		"zone":          in.ZoneID,
		"instance_type": in.InstanceType,
		"cpu_cores":     in.CPU,
		"memory_gb":     in.Memory / 1024,
		"os":            in.OSName,
		"power_state":   in.Status,
	}
	if ip := firstIP(in.VpcAttributes.PrivateIPAddress.IPAddress, in.InnerIPAddress.IPAddress); ip != "" {
		attrs["private_ip"] = ip
	}
	if ip := firstIP(in.PublicIPAddress.IPAddress, []string{in.EipAddress.IPAddress}); ip != "" {
		attrs["public_ip"] = ip
	}
	return &biz.Resource{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: in.InstanceID, Name: firstNonEmpty(in.InstanceName, in.InstanceID), Attrs: attrs}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstIP(lists ...[]string) string {
	for _, l := range lists {
		for _, ip := range l {
			if ip != "" {
				return ip
			}
		}
	}
	return ""
}

func (c *Collector) describe(ctx context.Context, endpoint string, cfg *cmdbcontract.AliyunConfig, secret string, page int) (*describeResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		resp, retry, err := c.describeOnce(ctx, endpoint, cfg, secret, page)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retry || ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

// describeOnce 单次请求；retry 标记是否可重试（5xx / 网络错误 / 限流）。
func (c *Collector) describeOnce(ctx context.Context, endpoint string, cfg *cmdbcontract.AliyunConfig, secret string, page int) (*describeResponse, bool, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, false, err
	}
	params := map[string]string{
		"Action": "DescribeInstances", "Version": apiVersion, "Format": "JSON", "RegionId": cfg.RegionID,
		"PageSize": strconv.Itoa(pageSize), "PageNumber": strconv.Itoa(page),
		"AccessKeyId": cfg.AccessKeyID, "SignatureMethod": "HMAC-SHA1", "SignatureVersion": "1.0",
		"SignatureNonce": hex.EncodeToString(nonce), "Timestamp": c.now().UTC().Format("2006-01-02T15:04:05Z"),
	}
	params["Signature"] = Sign(http.MethodGet, params, secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/?"+Encode(params), nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("aliyun: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, true, fmt.Errorf("aliyun: read response: %w", err)
	}
	var out describeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, resp.StatusCode >= 500, fmt.Errorf("aliyun: decode response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests || out.Code == "Throttling"
		return nil, retry, fmt.Errorf("aliyun: %s (%s) status %d", out.Code, out.Message, resp.StatusCode)
	}
	return &out, false, nil
}

// percentEncode 阿里云规范编码：RFC 3986（空格 %20、* %2A、~ 不编码）。
func percentEncode(s string) string {
	e := url.QueryEscape(s)
	e = strings.ReplaceAll(e, "+", "%20")
	e = strings.ReplaceAll(e, "*", "%2A")
	return strings.ReplaceAll(e, "%7E", "~")
}

// Encode 按键排序编码为查询串。
func Encode(params map[string]string) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(percentEncode(k))
		b.WriteByte('=')
		b.WriteString(percentEncode(params[k]))
	}
	return b.String()
}

// Sign 计算 V1 签名：base64(HMAC-SHA1(secret+"&", method&%2F&percentEncode(canonicalized)))。
// params 不应包含 Signature 键。
func Sign(method string, params map[string]string, secret string) string {
	canonical := make(map[string]string, len(params))
	for k, v := range params {
		if k != "Signature" {
			canonical[k] = v
		}
	}
	stringToSign := method + "&" + percentEncode("/") + "&" + percentEncode(Encode(canonical))
	mac := hmac.New(sha1.New, []byte(secret+"&"))
	mac.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
