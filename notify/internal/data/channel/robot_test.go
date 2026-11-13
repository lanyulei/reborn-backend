package channel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

type robotCapture struct {
	mu   sync.Mutex
	path string
	body map[string]any
}

func newRobotServer(t *testing.T, reply string, status int) (*httptest.Server, *robotCapture) {
	t.Helper()
	cap := &robotCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		cap.mu.Lock()
		cap.path = r.URL.RequestURI()
		cap.body = nil // json.Unmarshal 会合并进已有 map，须先清空
		_ = json.Unmarshal(raw, &cap.body)
		cap.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

func TestRobotSender_Dingtalk(t *testing.T) {
	t.Parallel()
	srv, cap := newRobotServer(t, `{"errcode":0,"errmsg":"ok"}`, 200)
	s := NewRobotSender().WithEndpoint(biz.ChannelTypeDingtalk, srv.URL+"/robot/send")
	ch := &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "tok&en", Robot: &biz.RobotConfig{AtMobiles: []string{"13800000000"}}}
	res, err := s.Send(context.Background(), ch, biz.RenderedMessage{Subject: "告警", Body: "主机失联", SendKey: "k1", Recipient: "$group:ops"})
	if err != nil || res.ProviderMsgID != "k1" {
		t.Fatalf("Send() = %+v, %v", res, err)
	}
	if cap.path != "/robot/send?access_token=tok%26en" {
		t.Fatalf("path = %s", cap.path)
	}
	md := cap.body["markdown"].(map[string]any)
	if cap.body["msgtype"] != "markdown" || md["title"] != "告警" || !strings.Contains(md["text"].(string), "主机失联") || !strings.Contains(md["text"].(string), "@13800000000") {
		t.Fatalf("body = %v", cap.body)
	}
	if at := cap.body["at"].(map[string]any); at["atMobiles"].([]any)[0] != "13800000000" {
		t.Fatalf("at = %v", at)
	}
}

func TestRobotSender_WecomAndFeishu(t *testing.T) {
	t.Parallel()
	wecom, wc := newRobotServer(t, `{"errcode":0,"errmsg":"ok"}`, 200)
	feishu, fc := newRobotServer(t, `{"code":0,"msg":"success"}`, 200)
	s := NewRobotSender().WithEndpoint(biz.ChannelTypeWecom, wecom.URL+"/cgi-bin/webhook/send").
		WithEndpoint(biz.ChannelTypeFeishu, feishu.URL+"/open-apis/bot/v2/hook/")
	msg := biz.RenderedMessage{Subject: "S", Body: "B", SendKey: "k"}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWecom, Secret: "wkey", Robot: &biz.RobotConfig{}}, msg); err != nil {
		t.Fatalf("wecom: %v", err)
	}
	if wc.path != "/cgi-bin/webhook/send?key=wkey" || wc.body["msgtype"] != "markdown" {
		t.Fatalf("wecom request = %s %v", wc.path, wc.body)
	}
	// 企微有 @ 名单时切 text 消息携带 mentioned_mobile_list。
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWecom, Secret: "wkey", Robot: &biz.RobotConfig{AtMobiles: []string{"1"}}}, msg); err != nil {
		t.Fatal(err)
	}
	if wc.body["msgtype"] != "text" || wc.body["text"].(map[string]any)["mentioned_mobile_list"].([]any)[0] != "1" {
		t.Fatalf("wecom @ request = %v", wc.body)
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "f/key"}, msg); err != nil {
		t.Fatalf("feishu: %v", err)
	}
	if fc.path != "/open-apis/bot/v2/hook/f%2Fkey" || fc.body["msg_type"] != "text" || !strings.Contains(fc.body["content"].(map[string]any)["text"].(string), "S\nB") {
		t.Fatalf("feishu request = %s %v", fc.path, fc.body)
	}
}

func TestRobotSender_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		typ       string
		reply     string
		status    int
		retryable bool
	}{
		{"dingtalk invalid token", biz.ChannelTypeDingtalk, `{"errcode":300001,"errmsg":"token is not exist"}`, 200, false},
		{"dingtalk rate limited", biz.ChannelTypeDingtalk, `{"errcode":130101,"errmsg":"send too fast"}`, 200, true},
		{"wecom freq limit", biz.ChannelTypeWecom, `{"errcode":45009,"errmsg":"api freq out of limit"}`, 200, true},
		{"wecom bad key", biz.ChannelTypeWecom, `{"errcode":93000,"errmsg":"invalid webhook url"}`, 200, false},
		{"feishu too many", biz.ChannelTypeFeishu, `{"code":11232,"msg":"too many request"}`, 200, true},
		{"feishu legacy status", biz.ChannelTypeFeishu, `{"StatusCode":19001,"StatusMessage":"param invalid"}`, 200, false},
		{"http 500", biz.ChannelTypeDingtalk, `oops`, 500, true},
		{"http 429", biz.ChannelTypeFeishu, ``, 429, true},
		{"http 403", biz.ChannelTypeWecom, `forbidden`, 403, false},
		{"unparseable", biz.ChannelTypeDingtalk, `<html>`, 200, false},
	}
	for _, c := range cases {
		srv, _ := newRobotServer(t, c.reply, c.status)
		s := NewRobotSender().WithEndpoint(c.typ, srv.URL+"/x")
		res, err := s.Send(context.Background(), &biz.Channel{Type: c.typ, Secret: "s"}, biz.RenderedMessage{Subject: "S", Body: "B"})
		if err == nil || res.Retryable != c.retryable {
			t.Errorf("%s: retryable=%v err=%v, want retryable=%v", c.name, res.Retryable, err, c.retryable)
		}
	}
	// 无 secret：不可重试；超长正文截断。
	s := NewRobotSender()
	if res, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk}, biz.RenderedMessage{}); err == nil || res.Retryable {
		t.Fatalf("missing secret: %+v %v", res, err)
	}
	if got := truncateBytes(strings.Repeat("字", 5000), 2000); len(got) > 2000 || !strings.HasSuffix(got, "…") || !utf8.ValidString(got) {
		t.Fatalf("truncateBytes = %d bytes valid=%v", len(got), utf8.ValidString(got))
	}
	if !NewMux().Supports(biz.ChannelTypeFeishu) {
		t.Fatal("mux must support robot types")
	}
}

// TestRobotSender_Signatures 钉钉加签（URL timestamp+sign）与飞书签名校验（body
// timestamp+sign）：接收端按官方算法复核；无 sign_secret 时不带签名。
func TestRobotSender_Signatures(t *testing.T) {
	t.Parallel()
	ding, dc := newRobotServer(t, `{"errcode":0,"errmsg":"ok"}`, 200)
	feishu, fc := newRobotServer(t, `{"code":0,"msg":"success"}`, 200)
	s := NewRobotSender().WithEndpoint(biz.ChannelTypeDingtalk, ding.URL+"/robot/send").
		WithEndpoint(biz.ChannelTypeFeishu, feishu.URL+"/hook/")
	msg := biz.RenderedMessage{Subject: "S", Body: "B", SendKey: "k"}

	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "tok", SignSecret: "SECd", Robot: &biz.RobotConfig{}}, msg); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(dc.path)
	ts, sign := u.Query().Get("timestamp"), u.Query().Get("sign")
	if ts == "" || sign == "" || sign != DingtalkSign("SECd", ts) || u.Query().Get("access_token") != "tok" {
		t.Fatalf("dingtalk signed url = %s", dc.path)
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "fkey", SignSecret: "SECf"}, msg); err != nil {
		t.Fatal(err)
	}
	fts, _ := fc.body["timestamp"].(string)
	fsign, _ := fc.body["sign"].(string)
	if fts == "" || fsign != FeishuSign("SECf", fts) {
		t.Fatalf("feishu signed body = %v", fc.body)
	}
	// 无签名密钥：不带签名字段。
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "fkey"}, msg); err != nil {
		t.Fatal(err)
	}
	if _, ok := fc.body["sign"]; ok {
		t.Fatal("unsigned feishu request must not carry sign")
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "tok", Robot: &biz.RobotConfig{}}, msg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(dc.path, "sign=") {
		t.Fatalf("unsigned dingtalk request must not carry sign: %s", dc.path)
	}
}

// TestRobotSender_ByteBudgets 正文按平台字节上限截断（企微按字节：4000 个汉字须被
// 截到 ≤2000B text / ≤4000B markdown），标题中的 markdown 控制字符转义，Retry-After 透传。
func TestRobotSender_ByteBudgets(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("告", 10000) // 30000 bytes，超过全部平台上限
	cases := []struct {
		typ     string
		mobiles []string
		limit   int
		field   func(m map[string]any) string
	}{
		{biz.ChannelTypeWecom, []string{"138"}, wecomTextBytes, func(m map[string]any) string { return m["text"].(map[string]any)["content"].(string) }},
		{biz.ChannelTypeWecom, nil, wecomMarkdownBytes, func(m map[string]any) string { return m["markdown"].(map[string]any)["content"].(string) }},
		{biz.ChannelTypeDingtalk, nil, dingtalkMarkdownBytes, func(m map[string]any) string { return m["markdown"].(map[string]any)["text"].(string) }},
		{biz.ChannelTypeFeishu, nil, feishuTextBytes, func(m map[string]any) string { return m["content"].(map[string]any)["text"].(string) }},
	}
	for _, c := range cases {
		srv, captured := newRobotServer(t, `{"errcode":0,"code":0}`, 200)
		s := NewRobotSender().WithEndpoint(c.typ, srv.URL+"/x")
		ch := &biz.Channel{Type: c.typ, Secret: "s", Robot: &biz.RobotConfig{AtMobiles: c.mobiles}}
		if _, err := s.Send(context.Background(), ch, biz.RenderedMessage{Subject: "S [x](http://e)", Body: long}); err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		captured.mu.Lock()
		m := captured.body
		captured.mu.Unlock()
		text := c.field(m)
		if len(text) > c.limit || !utf8.ValidString(text) || !strings.Contains(text, "…") {
			t.Fatalf("%s: field %d bytes (limit %d) valid=%v", c.typ, len(text), c.limit, utf8.ValidString(text))
		}
		if c.typ != biz.ChannelTypeFeishu && c.mobiles == nil && !strings.Contains(text, `\[x\]`) {
			t.Fatalf("%s: markdown title must be escaped: %q", c.typ, text[:60])
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	s := NewRobotSender().WithEndpoint(biz.ChannelTypeDingtalk, srv.URL+"/x")
	res, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "s"}, biz.RenderedMessage{Subject: "S", Body: "B"})
	if err == nil || !res.Retryable || res.RetryAfter != 7*time.Second {
		t.Fatalf("retry-after: %+v %v", res, err)
	}
}

// TestRobotSender_Cards format=card：钉钉 actionCard / 企微 template_card / 飞书 interactive；
// 钉钉/企微无 card_url 降级 markdown；飞书无 card_url 卡片无按钮；非 card 格式载荷不变。
func TestRobotSender_Cards(t *testing.T) {
	t.Parallel()
	msg := biz.RenderedMessage{Subject: "告警", Body: "主机失联", Severity: 3, Format: "card", SendKey: "k"}
	robot := &biz.RobotConfig{CardURL: "https://ops.example.com/a/1", CardButton: "处理"}

	srv, dc := newRobotServer(t, `{"errcode":0}`, 200)
	s := NewRobotSender().WithEndpoint(biz.ChannelTypeDingtalk, srv.URL+"/x")
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "t", Robot: robot}, msg); err != nil {
		t.Fatal(err)
	}
	ac := dc.body["actionCard"].(map[string]any)
	if dc.body["msgtype"] != "actionCard" || ac["singleURL"] != robot.CardURL || ac["singleTitle"] != "处理" || !strings.Contains(ac["text"].(string), "主机失联") {
		t.Fatalf("dingtalk card = %v", dc.body)
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeDingtalk, Secret: "t"}, msg); err != nil || dc.body["msgtype"] != "markdown" {
		t.Fatalf("dingtalk card without url must degrade to markdown: %v %v", dc.body["msgtype"], err)
	}

	srv, wc := newRobotServer(t, `{"errcode":0}`, 200)
	s = NewRobotSender().WithEndpoint(biz.ChannelTypeWecom, srv.URL+"/x")
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWecom, Secret: "t", Robot: robot}, msg); err != nil {
		t.Fatal(err)
	}
	tc := wc.body["template_card"].(map[string]any)
	if wc.body["msgtype"] != "template_card" || tc["card_type"] != "text_notice" || tc["sub_title_text"] != "主机失联" ||
		tc["card_action"].(map[string]any)["url"] != robot.CardURL || tc["main_title"].(map[string]any)["title"] != "告警" {
		t.Fatalf("wecom card = %v", wc.body)
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWecom, Secret: "t", Robot: &biz.RobotConfig{}}, msg); err != nil || wc.body["msgtype"] != "markdown" {
		t.Fatalf("wecom card without url must degrade: %v %v", wc.body["msgtype"], err)
	}

	srv, fc := newRobotServer(t, `{"code":0}`, 200)
	s = NewRobotSender().WithEndpoint(biz.ChannelTypeFeishu, srv.URL+"/x")
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "t", Robot: robot, SignSecret: "ss"}, msg); err != nil {
		t.Fatal(err)
	}
	card := fc.body["card"].(map[string]any)
	if fc.body["msg_type"] != "interactive" || card["header"].(map[string]any)["template"] != "red" || len(card["elements"].([]any)) != 2 || fc.body["sign"] == nil {
		t.Fatalf("feishu card = %v", fc.body)
	}
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "t"}, msg); err != nil || len(fc.body["card"].(map[string]any)["elements"].([]any)) != 1 {
		t.Fatalf("feishu card without url must have no button: %v %v", fc.body, err)
	}
	// markdown/text 格式：飞书仍为 text。
	plain := msg
	plain.Format = "markdown"
	if _, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeFeishu, Secret: "t", Robot: robot}, plain); err != nil || fc.body["msg_type"] != "text" {
		t.Fatalf("non-card format must keep text: %v %v", fc.body["msg_type"], err)
	}
}

// TestRobotSender_ErrorsCarryNoToken 出站失败（连接拒绝）的错误文本不得带 access_token /
// key / 飞书 hook 路径：错误会落库（last_error / delivery_log / 死信）并进日志。
func TestRobotSender_ErrorsCarryNoToken(t *testing.T) {
	t.Parallel()
	const secret = "s3cr3t-token-value"
	for _, typ := range []string{biz.ChannelTypeDingtalk, biz.ChannelTypeWecom, biz.ChannelTypeFeishu} {
		s := NewRobotSender().WithEndpoint(typ, "http://127.0.0.1:1/hook/")
		res, err := s.Send(context.Background(), &biz.Channel{Type: typ, Secret: secret, SignSecret: "sign"}, biz.RenderedMessage{Subject: "S", Body: "B"})
		if err == nil || !res.Retryable {
			t.Fatalf("%s: connection refused must be retryable, got %+v %v", typ, res, err)
		}
		if msg := err.Error(); strings.Contains(msg, secret) || strings.Contains(msg, "access_token") || strings.Contains(msg, "key=") || strings.Contains(msg, "sign=") {
			t.Fatalf("%s: error leaks request url: %s", typ, msg)
		}
	}
}
