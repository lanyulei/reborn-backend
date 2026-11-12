package channel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	htmlpkg "html"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// EmailSender SMTP 适配器：465 隐式 TLS；其余端口明文连接后按 starttls 升级；
// username 非空时按服务器通告的机制认证（PLAIN > LOGIN > CRAM-MD5；Exchange/
// 国产 SMTP 常只开 LOGIN）。错误按 SMTP 应答码分级：4xx（灰名单/临时拒绝/454 临时
// 认证失败）可重试，5xx（无此用户/认证失败）死信；连接/超时类可重试。
type EmailSender struct {
	dialer *net.Dialer
	// tlsConfig 测试注入（自签名）；nil 用 ServerName=host 的默认配置。
	tlsConfig *tls.Config
}

// NewEmailSender new an EmailSender.
func NewEmailSender() *EmailSender {
	return &EmailSender{dialer: &net.Dialer{Timeout: 3 * time.Second}}
}

// WithTLSConfig 注入 TLS 配置（测试用）。
func (s *EmailSender) WithTLSConfig(c *tls.Config) *EmailSender {
	s.tlsConfig = c
	return s
}

// Supports implements biz.ChannelSender.
func (s *EmailSender) Supports(channelType string) bool { return channelType == biz.ChannelTypeEmail }

// Send implements biz.ChannelSender.
func (s *EmailSender) Send(ctx context.Context, ch *biz.Channel, msg biz.RenderedMessage) (biz.SendResult, error) {
	cfg := ch.Email
	if cfg == nil || cfg.Host == "" || cfg.FromAddr == "" {
		return biz.SendResult{Retryable: false}, errors.New("email config missing host/from_addr")
	}
	to, err := mail.ParseAddress(msg.Recipient)
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("invalid recipient %q: %w", msg.Recipient, err)
	}
	port := cfg.Port
	if port == 0 {
		port = 25
	}
	addr := net.JoinHostPort(cfg.Host, strconv.FormatUint(uint64(port), 10))
	tlsCfg := s.tlsConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	}
	var conn net.Conn
	if port == 465 {
		conn, err = (&tls.Dialer{NetDialer: s.dialer, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = s.dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp dial %s: %w", addr, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		_ = conn.Close()
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp handshake: %w", err)
	}
	defer func() { _ = client.Close() }()
	if cfg.StartTLS && port != 465 {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return biz.SendResult{Retryable: false}, errors.New("smtp server does not support STARTTLS")
		}
		if err := client.StartTLS(tlsCfg); err != nil {
			return biz.SendResult{Retryable: true}, fmt.Errorf("smtp starttls: %w", err)
		}
	}
	if cfg.Username != "" {
		if err := client.Auth(chooseAuth(client, cfg.Username, ch.Secret, cfg.Host)); err != nil {
			return biz.SendResult{Retryable: smtpRetryable(err, false)}, fmt.Errorf("smtp auth: %w", err)
		}
	}
	if err := client.Mail(cfg.FromAddr); err != nil {
		return biz.SendResult{Retryable: smtpRetryable(err, true)}, fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	if err := client.Rcpt(to.Address); err != nil {
		return biz.SendResult{Retryable: smtpRetryable(err, false)}, fmt.Errorf("smtp RCPT TO: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return biz.SendResult{Retryable: smtpRetryable(err, true)}, fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write(buildMessage(cfg.FromAddr, to.Address, msg)); err != nil {
		_ = w.Close()
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		return biz.SendResult{Retryable: smtpRetryable(err, true)}, fmt.Errorf("smtp finish: %w", err)
	}
	_ = client.Quit()
	return biz.SendResult{ProviderMsgID: msg.SendKey}, nil
}

// smtpRetryable 按 SMTP 应答码分级：4xx 临时失败可重试，5xx 永久失败不可重试；
// 非应答类错误（I/O）取 fallback。
func smtpRetryable(err error, fallback bool) bool {
	var te *textproto.Error
	if errors.As(err, &te) {
		return te.Code >= 400 && te.Code < 500
	}
	return fallback
}

// chooseAuth 按服务器 EHLO 通告的 AUTH 机制选择：PLAIN > LOGIN > CRAM-MD5；未通告时
// 仍尝试 PLAIN（由服务器拒绝给出明确应答）。
func chooseAuth(client *smtp.Client, username, password, host string) smtp.Auth {
	ok, mechs := client.Extension("AUTH")
	m := strings.ToUpper(mechs)
	switch {
	case !ok || strings.Contains(m, "PLAIN"):
		return smtp.PlainAuth("", username, password, host)
	case strings.Contains(m, "LOGIN"):
		return &loginAuth{username: username, password: password}
	case strings.Contains(m, "CRAM-MD5"):
		return smtp.CRAMMD5Auth(username, password)
	}
	return smtp.PlainAuth("", username, password, host)
}

// loginAuth AUTH LOGIN（RFC 草案机制，Exchange / Outlook365 / 多数国产 SMTP 的唯一选项）。
type loginAuth struct{ username, password string }

func (a *loginAuth) Start(*smtp.ServerInfo) (string, []byte, error) { return "LOGIN", nil, nil }

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimSpace(string(fromServer))) {
	case "username:":
		return []byte(a.username), nil
	case "password:":
		return []byte(a.password), nil
	}
	return nil, fmt.Errorf("unexpected AUTH LOGIN challenge %q", fromServer)
}

// buildMessage 组装 RFC 5322 文本邮件（UTF-8，主题 RFC 2047 编码，幂等键进自定义头；
// Message-ID 域取发件地址域，DMARC/反垃圾评分不再看到陌生域）。
func buildMessage(from, to string, msg biz.RenderedMessage) []byte {
	domain := "reborn-notify"
	if at := strings.LastIndex(from, "@"); at >= 0 && at < len(from)-1 {
		domain = from[at+1:]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", msg.Subject))
	fmt.Fprintf(&b, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&b, "Message-ID: <%s@%s>\r\n", msg.SendKey, domain)
	fmt.Fprintf(&b, "%s: %s\r\n", HeaderSendKey, msg.SendKey)
	fmt.Fprintf(&b, "%s: %s\r\n", HeaderEventID, msg.EventID)
	if msg.Format == notifycontract.TemplateFormatHTML {
		writeHTMLBody(&b, msg.Body, msg.SendKey)
		return []byte(b.String())
	}
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(strings.ReplaceAll(msg.Body, "\n", "\r\n"))
	b.WriteString("\r\n")
	return []byte(b.String())
}

// writeHTMLBody multipart/alternative：text/plain（去标签的降级文本）在前、text/html 在后，
// 两部分 quoted-printable（对 7-bit 中继与长行安全）。边界含 send_key 保证唯一。
func writeHTMLBody(b *strings.Builder, html, sendKey string) {
	boundary := "=_reborn_" + strings.NewReplacer("-", "", ":", "").Replace(sendKey)
	fmt.Fprintf(b, "MIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", boundary)
	part := func(contentType, body string) {
		fmt.Fprintf(b, "--%s\r\nContent-Type: %s; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n", boundary, contentType)
		w := quotedprintable.NewWriter(b)
		_, _ = w.Write([]byte(body))
		_ = w.Close()
		b.WriteString("\r\n")
	}
	part("text/plain", htmlToText(html))
	part("text/html", html)
	fmt.Fprintf(b, "--%s--\r\n", boundary)
}

// htmlToText HTML 降级为纯文本：块级标签换行、其余标签剥离、实体解码、折叠空行。
var (
	htmlBlockTags = regexp.MustCompile(`(?i)</?(p|div|br|tr|li|h[1-6]|table|ul|ol)[^>]*>`)
	htmlAnyTag    = regexp.MustCompile(`(?s)<[^>]*>`)
	htmlStyle     = regexp.MustCompile(`(?is)<(script|style)[^>]*>.*?</(script|style)>`)
	multiNewline  = regexp.MustCompile(`\n{3,}`)
)

func htmlToText(s string) string {
	s = htmlStyle.ReplaceAllString(s, "")
	s = htmlBlockTags.ReplaceAllString(s, "\n")
	s = htmlAnyTag.ReplaceAllString(s, "")
	s = htmlpkg.UnescapeString(s)
	return strings.TrimSpace(multiNewline.ReplaceAllString(s, "\n\n"))
}
