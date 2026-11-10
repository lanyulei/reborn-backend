package channel

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// 群机器人官方入口（可经 WithEndpoint 覆盖，测试指向 httptest）。
const (
	DingtalkEndpoint = "https://oapi.dingtalk.com/robot/send"
	WecomEndpoint    = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send"
	FeishuEndpoint   = "https://open.feishu.cn/open-apis/bot/v2/hook/"
)

// robotBodyLimit 群机器人正文上限（钉钉 markdown 20KB；按 rune 截断留余量）。
const robotBodyLimit = 4000

// RobotSender 群机器人适配器（dingtalk / wecom / feishu）：实例 secret 即机器人
// access_token/key，目标群由实例决定（收件标识 $group:xxx 仅作标注）；config.at_mobiles
// 作 @ 名单（钉钉 / 企微支持）。平台返回 errcode/code≠0 为确定性失败（不可重试）；
// 频控类错误码与 HTTP 429/5xx/超时可重试。
type RobotSender struct {
	client    *http.Client
	endpoints map[string]string
}

// NewRobotSender new a RobotSender.
func NewRobotSender() *RobotSender {
	return &RobotSender{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
				DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		endpoints: map[string]string{
			biz.ChannelTypeDingtalk: DingtalkEndpoint,
			biz.ChannelTypeWecom:    WecomEndpoint,
			biz.ChannelTypeFeishu:   FeishuEndpoint,
		},
	}
}

// WithEndpoint 覆盖某类型机器人的入口（测试用）。
func (s *RobotSender) WithEndpoint(channelType, endpoint string) *RobotSender {
	s.endpoints[channelType] = endpoint
	return s
}

// Supports implements biz.ChannelSender.
func (s *RobotSender) Supports(channelType string) bool {
	_, ok := s.endpoints[channelType]
	return ok
}

// Send implements biz.ChannelSender.
func (s *RobotSender) Send(ctx context.Context, ch *biz.Channel, msg biz.RenderedMessage) (biz.SendResult, error) {
	if ch.Secret == "" {
		return biz.SendResult{Retryable: false}, errors.New("robot secret (access token) missing")
	}
	var mobiles []string
	if ch.Robot != nil {
		mobiles = ch.Robot.AtMobiles
	}
	body := truncateRunes(msg.Body, robotBodyLimit)
	var (
		target  string
		payload any
	)
	switch ch.Type {
	case biz.ChannelTypeDingtalk:
		target = s.endpoints[ch.Type] + "?access_token=" + url.QueryEscape(ch.Secret)
		if ch.SignSecret != "" {
			ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
			target += "&timestamp=" + ts + "&sign=" + url.QueryEscape(DingtalkSign(ch.SignSecret, ts))
		}
		payload = map[string]any{
			"msgtype":  "markdown",
			"markdown": map[string]string{"title": msg.Subject, "text": "### " + msg.Subject + "\n\n" + body + atSuffix(mobiles)},
			"at":       map[string]any{"atMobiles": mobiles, "isAtAll": false},
		}
	case biz.ChannelTypeWecom:
		target = s.endpoints[ch.Type] + "?key=" + url.QueryEscape(ch.Secret)
		if len(mobiles) > 0 {
			payload = map[string]any{"msgtype": "text", "text": map[string]any{
				"content": msg.Subject + "\n" + body, "mentioned_mobile_list": mobiles,
			}}
		} else {
			payload = map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": "**" + msg.Subject + "**\n" + body}}
		}
	case biz.ChannelTypeFeishu:
		target = s.endpoints[ch.Type] + url.PathEscape(ch.Secret)
		feishu := map[string]any{"msg_type": "text", "content": map[string]string{"text": msg.Subject + "\n" + body}}
		if ch.SignSecret != "" {
			ts := strconv.FormatInt(time.Now().Unix(), 10)
			feishu["timestamp"], feishu["sign"] = ts, FeishuSign(ch.SignSecret, ts)
		}
		payload = feishu
	default:
		return biz.SendResult{Retryable: false}, biz.ErrChannelTypeUnsupported
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("encode robot body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("build robot request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderSendKey, msg.SendKey)
	resp, err := s.client.Do(req)
	if err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("robot request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return biz.SendResult{Retryable: true}, fmt.Errorf("robot responded %d", resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return biz.SendResult{Retryable: false}, fmt.Errorf("robot responded %d: %s", resp.StatusCode, truncateRunes(string(respBody), 200))
	}
	code, message := robotResult(respBody)
	if code == 0 {
		return biz.SendResult{ProviderMsgID: msg.SendKey}, nil
	}
	return biz.SendResult{Retryable: robotRetryable(ch.Type, code)}, fmt.Errorf("robot error %d: %s", code, message)
}

// robotResult 解析三家机器人的统一结果：钉钉/企微 errcode+errmsg；飞书 code+msg
// （旧版 StatusCode+StatusMessage）。
func robotResult(body []byte) (int64, string) {
	var r struct {
		ErrCode       *int64 `json:"errcode"`
		ErrMsg        string `json:"errmsg"`
		Code          *int64 `json:"code"`
		Msg           string `json:"msg"`
		StatusCode    *int64 `json:"StatusCode"`
		StatusMessage string `json:"StatusMessage"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return -1, "unparseable response"
	}
	switch {
	case r.ErrCode != nil:
		return *r.ErrCode, r.ErrMsg
	case r.Code != nil:
		return *r.Code, r.Msg
	case r.StatusCode != nil:
		return *r.StatusCode, r.StatusMessage
	}
	return -1, "missing result code"
}

// robotRetryable 频控类错误码可重试：钉钉 130101（发送过快）、企微 45009（接口调用
// 超过限制）、飞书 11232（too many request）；其余（token 无效、关键词/IP 不符、
// 内容非法）为确定性失败。
func robotRetryable(channelType string, code int64) bool {
	switch channelType {
	case biz.ChannelTypeDingtalk:
		return code == 130101
	case biz.ChannelTypeWecom:
		return code == 45009
	case biz.ChannelTypeFeishu:
		return code == 11232
	}
	return false
}

func atSuffix(mobiles []string) string {
	if len(mobiles) == 0 {
		return ""
	}
	var b strings.Builder
	for _, m := range mobiles {
		b.WriteString(" @" + m)
	}
	return "\n" + b.String()
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max]) + "…"
}

// DingtalkSign 钉钉加签：base64(HMAC-SHA256(key = secret, msg = ts(ms) + "\n" + secret))。
func DingtalkSign(secret, tsMillis string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsMillis + "\n" + secret))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// FeishuSign 飞书签名校验：base64(HMAC-SHA256(key = ts(s) + "\n" + secret, msg = ""))。
func FeishuSign(secret, tsSeconds string) string {
	mac := hmac.New(sha256.New, []byte(tsSeconds+"\n"+secret))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
