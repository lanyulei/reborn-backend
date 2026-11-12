// Package aws AWS EC2 采集器：DescribeInstances（2016-11-15）Query API，SigV4 签名，NextToken 分页。
// 只用标准库（encoding/xml 解析响应）。输出固定映射到内置类型 cloud_host。
package aws

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

const (
	service    = "ec2"
	apiVersion = "2016-11-15"
	pageSize   = "1000"
	maxPages   = 10000
	maxRetries = 3
	maxBody    = 16 << 20
)

// Collector implements biz.Collector（provider=aws）。
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
func (c *Collector) Provider() string { return cmdbcontract.ProviderAWS }

// XML 响应结构（只声明用到的字段）。
type (
	tagItem struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	instanceItem struct {
		InstanceID      string `xml:"instanceId"`
		InstanceType    string `xml:"instanceType"`
		PlatformDetails string `xml:"platformDetails"`
		PrivateIP       string `xml:"privateIpAddress"`
		PublicIP        string `xml:"ipAddress"`
		State           struct {
			Name string `xml:"name"`
		} `xml:"instanceState"`
		Placement struct {
			Zone string `xml:"availabilityZone"`
		} `xml:"placement"`
		CPUOptions struct {
			CoreCount      int `xml:"coreCount"`
			ThreadsPerCore int `xml:"threadsPerCore"`
		} `xml:"cpuOptions"`
		Tags []tagItem `xml:"tagSet>item"`
	}
	describeResponse struct {
		XMLName      xml.Name      `xml:"DescribeInstancesResponse"`
		Reservations []reservation `xml:"reservationSet>item"`
		NextToken    string        `xml:"nextToken"`
	}
	reservation struct {
		Instances []instanceItem `xml:"instancesSet>item"`
	}
	errorResponse struct {
		XMLName xml.Name `xml:"Response"`
		Errors  []struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Errors>Error"`
	}
)

// Discover implements biz.Collector.
func (c *Collector) Discover(ctx context.Context, src *biz.Source, credential string, sink biz.Sink) error {
	cfg, err := cmdbcontract.DecodeConfig(cmdbcontract.ProviderAWS, src.Config)
	if err != nil {
		return err
	}
	ac, ok := cfg.(*cmdbcontract.AWSConfig)
	if !ok {
		return errors.New("aws: unexpected config type")
	}
	endpoint := ac.Endpoint
	if endpoint == "" {
		endpoint = "https://ec2." + ac.Region + ".amazonaws.com"
	}
	token := ""
	for page := 0; page < maxPages; page++ {
		resp, err := c.describe(ctx, endpoint, ac, credential, token)
		if err != nil {
			return err
		}
		for i := range resp.Reservations {
			for j := range resp.Reservations[i].Instances {
				if err := sink.Put(ctx, toResource(&resp.Reservations[i].Instances[j], ac.Region)); err != nil {
					return err
				}
			}
		}
		if resp.NextToken == "" {
			return nil
		}
		token = resp.NextToken
	}
	return fmt.Errorf("aws: too many pages (>%d)", maxPages)
}

func toResource(in *instanceItem, region string) *biz.Resource {
	name := in.InstanceID
	for _, tag := range in.Tags {
		if tag.Key == "Name" && tag.Value != "" {
			name = tag.Value
		}
	}
	attrs := map[string]any{
		"provider": cmdbcontract.ProviderAWS, "instance_id": in.InstanceID, "region": region, "zone": in.Placement.Zone,
		"instance_type": in.InstanceType, "os": in.PlatformDetails, "power_state": in.State.Name,
	}
	if cores := in.CPUOptions.CoreCount * max(in.CPUOptions.ThreadsPerCore, 1); cores > 0 {
		attrs["cpu_cores"] = cores
	}
	if in.PrivateIP != "" {
		attrs["private_ip"] = in.PrivateIP
	}
	if in.PublicIP != "" {
		attrs["public_ip"] = in.PublicIP
	}
	return &biz.Resource{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: in.InstanceID, Name: name, Attrs: attrs}
}

func (c *Collector) describe(ctx context.Context, endpoint string, cfg *cmdbcontract.AWSConfig, secret, token string) (*describeResponse, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		resp, retry, err := c.describeOnce(ctx, endpoint, cfg, secret, token)
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

func (c *Collector) describeOnce(ctx context.Context, endpoint string, cfg *cmdbcontract.AWSConfig, secret, token string) (*describeResponse, bool, error) {
	form := url.Values{"Action": {"DescribeInstances"}, "Version": {apiVersion}, "MaxResults": {pageSize}}
	if token != "" {
		form.Set("NextToken", token)
	}
	body := form.Encode()
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, false, err
	}
	now := c.now().UTC()
	headers := map[string]string{
		"content-type": "application/x-www-form-urlencoded; charset=utf-8",
		"host":         u.Host,
		"x-amz-date":   now.Format("20060102T150405Z"),
	}
	auth := Authorization(http.MethodPost, "/", "", headers, sha256Hex([]byte(body)), now, cfg.Region, service, cfg.AccessKeyID, secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/", strings.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", headers["content-type"])
	req.Header.Set("X-Amz-Date", headers["x-amz-date"])
	req.Header.Set("Authorization", auth)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("aws: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, true, fmt.Errorf("aws: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var er errorResponse
		code, msg := "", ""
		if xml.Unmarshal(raw, &er) == nil && len(er.Errors) > 0 {
			code, msg = er.Errors[0].Code, er.Errors[0].Message
		}
		retry := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests || code == "RequestLimitExceeded"
		return nil, retry, fmt.Errorf("aws: %s (%s) status %d", code, msg, resp.StatusCode)
	}
	var out describeResponse
	if err := xml.Unmarshal(raw, &out); err != nil {
		return nil, false, fmt.Errorf("aws: decode response: %w", err)
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

// Authorization 计算 SigV4 Authorization 头。headers 为小写头名 → 值（全部参与签名），
// payloadHash 为请求体 SHA-256 hex；时间取 now（UTC）。
func Authorization(method, path, query string, headers map[string]string, payloadHash string, now time.Time, region, svc, accessKey, secret string) string {
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, strings.ToLower(k))
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + strings.TrimSpace(headers[k]) + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{method, path, query, canonicalHeaders.String(), signedHeaders, payloadHash}, "\n")
	date := now.UTC().Format("20060102")
	scope := date + "/" + region + "/" + svc + "/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", now.UTC().Format("20060102T150405Z"), scope, sha256Hex([]byte(canonicalRequest))}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, svc)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	return "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature
}
