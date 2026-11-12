// Package tencent 腾讯云 CVM 采集器：DescribeInstances（2017-03-12）JSON API，TC3-HMAC-SHA256 签名，
// Offset/Limit 分页全量拉取。只用标准库。输出固定映射到内置类型 cloud_host。
package tencent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

const (
	defaultEndpoint = "https://cvm.tencentcloudapi.com"
	service         = "cvm"
	apiVersion      = "2017-03-12"
	action          = "DescribeInstances"
	pageSize        = 100
	maxPages        = 10000
	maxRetries      = 3
	maxBody         = 8 << 20
)

// Collector implements biz.Collector（provider=tencent）。
type Collector struct {
	client *http.Client
	now    func() time.Time
}

// New new a Collector.
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
func (c *Collector) Provider() string { return cmdbcontract.ProviderTencent }

type instance struct {
	InstanceID         string   `json:"InstanceId"`
	InstanceName       string   `json:"InstanceName"`
	InstanceType       string   `json:"InstanceType"`
	CPU                int      `json:"CPU"`
	Memory             int      `json:"Memory"`
	OsName             string   `json:"OsName"`
	InstanceState      string   `json:"InstanceState"`
	PrivateIPAddresses []string `json:"PrivateIpAddresses"`
	PublicIPAddresses  []string `json:"PublicIpAddresses"`
	Placement          struct {
		Zone string `json:"Zone"`
	} `json:"Placement"`
}

type describeResponse struct {
	Response struct {
		TotalCount  int        `json:"TotalCount"`
		InstanceSet []instance `json:"InstanceSet"`
		RequestID   string     `json:"RequestId"`
		Error       *struct {
			Code    string `json:"Code"`
			Message string `json:"Message"`
		} `json:"Error"`
	} `json:"Response"`
}

// Discover implements biz.Collector.
func (c *Collector) Discover(ctx context.Context, src *biz.Source, credential string, sink biz.Sink) error {
	cfg, err := cmdbcontract.DecodeConfig(cmdbcontract.ProviderTencent, src.Config)
	if err != nil {
		return err
	}
	tc, ok := cfg.(*cmdbcontract.TencentConfig)
	if !ok {
		return errors.New("tencent: unexpected config type")
	}
	endpoint := tc.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	seen := 0
	for page := 0; page < maxPages; page++ {
		resp, err := c.describe(ctx, endpoint, tc, credential, page*pageSize)
		if err != nil {
			return err
		}
		for i := range resp.Response.InstanceSet {
			if err := sink.Put(ctx, toResource(&resp.Response.InstanceSet[i], tc.Region)); err != nil {
				return err
			}
		}
		seen += len(resp.Response.InstanceSet)
		if len(resp.Response.InstanceSet) == 0 || seen >= resp.Response.TotalCount {
			return nil
		}
	}
	return fmt.Errorf("tencent: too many pages (>%d)", maxPages)
}

func toResource(in *instance, region string) *biz.Resource {
	attrs := map[string]any{
		"provider": cmdbcontract.ProviderTencent, "instance_id": in.InstanceID, "region": region, "zone": in.Placement.Zone,
		"instance_type": in.InstanceType, "cpu_cores": in.CPU, "memory_gb": in.Memory, "os": in.OsName, "power_state": in.InstanceState,
	}
	if len(in.PrivateIPAddresses) > 0 && in.PrivateIPAddresses[0] != "" {
		attrs["private_ip"] = in.PrivateIPAddresses[0]
	}
	if len(in.PublicIPAddresses) > 0 && in.PublicIPAddresses[0] != "" {
		attrs["public_ip"] = in.PublicIPAddresses[0]
	}
	name := in.InstanceName
	if name == "" {
		name = in.InstanceID
	}
	return &biz.Resource{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: in.InstanceID, Name: name, Attrs: attrs}
}

func (c *Collector) describe(ctx context.Context, endpoint string, cfg *cmdbcontract.TencentConfig, secret string, offset int) (*describeResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		resp, retry, err := c.describeOnce(ctx, endpoint, cfg, secret, offset)
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

func (c *Collector) describeOnce(ctx context.Context, endpoint string, cfg *cmdbcontract.TencentConfig, secret string, offset int) (*describeResponse, bool, error) {
	payload, err := json.Marshal(map[string]int{"Offset": offset, "Limit": pageSize})
	if err != nil {
		return nil, false, err
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, false, err
	}
	now := c.now().UTC()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", bytes.NewReader(payload))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-TC-Action", action)
	req.Header.Set("X-TC-Version", apiVersion)
	req.Header.Set("X-TC-Region", cfg.Region)
	req.Header.Set("X-TC-Timestamp", strconv.FormatInt(now.Unix(), 10))
	req.Header.Set("Authorization", Authorization(cfg.SecretID, secret, u.Host, payload, now))
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("tencent: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, true, fmt.Errorf("tencent: read response: %w", err)
	}
	var out describeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, resp.StatusCode >= 500, fmt.Errorf("tencent: decode response (status %d): %w", resp.StatusCode, err)
	}
	if out.Response.Error != nil {
		code := out.Response.Error.Code
		retry := resp.StatusCode >= 500 || code == "RequestLimitExceeded" || code == "InternalError"
		return nil, retry, fmt.Errorf("tencent: %s (%s)", code, out.Response.Error.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests, fmt.Errorf("tencent: status %d", resp.StatusCode)
	}
	return &out, false, nil
}

func hmacSHA256(key []byte, msg string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msg))
	return mac.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Authorization 计算 TC3-HMAC-SHA256 的 Authorization 头（POST /，签名头 content-type;host;x-tc-action）。
func Authorization(secretID, secretKey, host string, payload []byte, now time.Time) string {
	date := now.UTC().Format("2006-01-02")
	canonicalRequest := "POST\n/\n\n" +
		"content-type:application/json; charset=utf-8\nhost:" + host + "\nx-tc-action:describeinstances\n\n" +
		"content-type;host;x-tc-action\n" + sha256Hex(payload)
	scope := date + "/" + service + "/tc3_request"
	stringToSign := "TC3-HMAC-SHA256\n" + strconv.FormatInt(now.Unix(), 10) + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))
	secretDate := hmacSHA256([]byte("TC3"+secretKey), date)
	secretService := hmacSHA256(secretDate, service)
	secretSigning := hmacSHA256(secretService, "tc3_request")
	signature := hex.EncodeToString(hmacSHA256(secretSigning, stringToSign))
	return "TC3-HMAC-SHA256 Credential=" + secretID + "/" + scope + ", SignedHeaders=content-type;host;x-tc-action, Signature=" + signature
}
