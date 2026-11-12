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

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// 群机器人官方入口（可经 WithEndpoint 覆盖，测试指向 httptest）。
const (
	DingtalkEndpoint = "https://oapi.dingtalk.com/robot/send"
	WecomEndpoint    = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send"
	FeishuEndpoint   = "https://open.feishu.cn/open-apis/bot/v2/hook/"
)

// 平台正文上限（字节；官方上限留余量，UTF-8 安全截断）：钉钉 markdown 20000、
// 企微 text 2048 / markdown 4096（企微按**字节**计，4000 个汉字 ≈ 12KB 直接 errcode 死信）、
// 飞书 text 30000。标题与装饰（### / ** / @ 名单）从预算里扣除。
const (
	dingtalkMarkdownBytes = 18000
	wecomTextBytes        = 2000
	wecomMarkdownBytes    = 4000
	wecomCardBytes        = 500 // template_card sub_title_text 上限 512B
	feishuTextBytes       = 28000
	robotBodyMinBytes     = 200
	defaultCardButton     = "查看详情"
)

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
	at := atSuffix(mobiles)
	// budget 按平台字节上限扣除标题与装饰后的正文预算。
	budget := func(limit int, decor string) string {
		return truncateBytes(msg.Body, max(limit-len(decor), robotBodyMinBytes))
	}
	// 卡片：模板 format=card 且（钉钉/企微）渠道配了 card_url；否则按各平台既有格式
	// （text/markdown 对机器人均为现有口径：钉钉 markdown、企微 markdown/有 @ 则 text、飞书 text）。
	card := msg.Format == notifycontract.TemplateFormatCard
	cardURL, cardButton := "", defaultCardButton
	if ch.Robot != nil {
		cardURL = ch.Robot.CardURL
		if ch.Robot.CardButton != "" {
			cardButton = ch.Robot.CardButton
		}
	}
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
		head := "### " + escapeMarkdown(msg.Subject) + "\n\n"
		if card && cardURL != "" {
			// actionCard 不支持 @ 名单；整体跳转按钮指向 card_url。
			payload = map[string]any{
				"msgtype": "actionCard",
				"actionCard": map[string]string{
					"title": msg.Subject, "text": head + budget(dingtalkMarkdownBytes, head),
					"btnOrientation": "0", "singleTitle": cardButton, "singleURL": cardURL,
				},
			}
		} else {
			payload = map[string]any{
				"msgtype":  "markdown",
				"markdown": map[string]string{"title": msg.Subject, "text": head + budget(dingtalkMarkdownBytes, head+at) + at},
				"at":       map[string]any{"atMobiles": mobiles, "isAtAll": false},
			}
		}
	case biz.ChannelTypeWecom:
		target = s.endpoints[ch.Type] + "?key=" + url.QueryEscape(ch.Secret)
		switch {
		case card && cardURL != "":
			payload = map[string]any{"msgtype": "template_card", "template_card": map[string]any{
				"card_type":      "text_notice",
				"main_title":     map[string]string{"title": truncateBytes(msg.Subject, 128)},
				"sub_title_text": budget(wecomCardBytes, ""),
				"card_action":    map[string]any{"type": 1, "url": cardURL},
			}}
		case len(mobiles) > 0:
			head := msg.Subject + "\n"
			payload = map[string]any{"msgtype": "text", "text": map[string]any{
				"content": head + budget(wecomTextBytes, head), "mentioned_mobile_list": mobiles,
			}}
		default:
			head := "**" + escapeMarkdown(msg.Subject) + "**\n"
			payload = map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": head + budget(wecomMarkdownBytes, head)}}
		}
	case biz.ChannelTypeFeishu:
		target = s.endpoints[ch.Type] + url.PathEscape(ch.Secret)
		var feishu map[string]any
		if card {
			feishu = feishuCard(msg, budget(feishuTextBytes, ""), cardURL, cardButton)
		} else {
			head := msg.Subject + "\n"
			feishu = map[string]any{"msg_type": "text", "content": map[string]string{"text": head + budget(feishuTextBytes, head)}}
		}
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
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return biz.SendResult{Retryable: true, RetryAfter: retryAfter(resp.Header)}, fmt.Errorf("robot responded %d", resp.StatusCode)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return biz.SendResult{Retryable: false}, fmt.Errorf("robot responded %d: %s", resp.StatusCode, truncateRunes(string(respBody), 200))
	}
	code, message := robotResult(respBody)
	if code == 0 {
		return biz.SendResult{ProviderMsgID: msg.SendKey}, nil
	}
	return biz.SendResult{Retryable: robotRetryable(ch.Type, code)}, fmt.Errorf("robot error %d: %s", code, message)
}

// feishuCard 飞书 interactive 卡片：标题色随严重级（1 blue / 2 orange / 3 red / 4 carmine），
// 正文 lark_md，有 card_url 时追加按钮。
func feishuCard(msg biz.RenderedMessage, body, cardURL, cardButton string) map[string]any {
	colors := map[int32]string{1: "blue", 2: "orange", 3: "red", 4: "carmine"}
	color, ok := colors[msg.Severity]
	if !ok {
		color = "blue"
	}
	elements := []any{map[string]any{"tag": "div", "text": map[string]string{"tag": "lark_md", "content": body}}}
	if cardURL != "" {
		elements = append(elements, map[string]any{"tag": "action", "actions": []any{map[string]any{
			"tag": "button", "type": "primary", "text": map[string]string{"tag": "plain_text", "content": cardButton}, "url": cardURL,
		}}})
	}
	return map[string]any{"msg_type": "interactive", "card": map[string]any{
		"config":   map[string]bool{"wide_screen_mode": true},
		"header":   map[string]any{"template": color, "title": map[string]string{"tag": "plain_text", "content": msg.Subject}},
		"elements": elements,
	}}
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

// truncateBytes 按字节上限截断（在 rune 边界切，追加 …；上限含省略号）。
func truncateBytes(s string, max int) string {
	const ellipsis = "…"
	if len(s) <= max {
		return s
	}
	cut := max - len(ellipsis)
	if cut <= 0 {
		return ellipsis
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// markdownEscaper 标题进 markdown 字段前转义控制字符：标题来自事件 title（生产者
// 可控），`](http://…)` / `#` 等会改写渲染结构。正文是模板作者输出，不转义。
var markdownEscaper = strings.NewReplacer(
	"\\", "\\\\", "*", "\\*", "_", "\\_", "#", "\\#", "[", "\\[", "]", "\\]", "(", "\\(", ")", "\\)", ">", "\\>", "`", "\\`", "~", "\\~",
)

func escapeMarkdown(s string) string { return markdownEscaper.Replace(s) }

// retryAfter 解析 Retry-After（秒数或 HTTP 日期）；缺失/非法返回 0。
func retryAfter(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
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
