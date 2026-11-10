package channel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// EmailSender SMTP 适配器：465 隐式 TLS；其余端口明文连接后按 starttls 升级；
// username 非空时 PLAIN 认证（secret = 密码）。收件地址非法/认证失败不可重试，
// 连接/超时类错误可重试。
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
		conn, err = tls.DialWithDialer(s.dialer, "tcp", addr, tlsCfg)
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
		if err := client.Auth(smtp.PlainAuth("", cfg.Username, ch.Secret, cfg.Host)); err != nil {
			return biz.SendResult{Retryable: false}, fmt.Errorf("smtp auth: %w", err)
		}
	}
	if err := client.Mail(cfg.FromAddr); err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	if err := client.Rcpt(to.Address); err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("smtp RCPT TO: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write(buildMessage(cfg.FromAddr, to.Address, msg)); err != nil {
		_ = w.Close()
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("smtp finish: %w", err)
	}
	_ = client.Quit()
	return biz.SendResult{ProviderMsgID: msg.SendKey}, nil
}

// buildMessage 组装 RFC 5322 文本邮件（UTF-8，主题 RFC 2047 编码，幂等键进自定义头）。
func buildMessage(from, to string, msg biz.RenderedMessage) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", msg.Subject))
	fmt.Fprintf(&b, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&b, "Message-ID: <%s@reborn-notify>\r\n", msg.SendKey)
	fmt.Fprintf(&b, "%s: %s\r\n", HeaderSendKey, msg.SendKey)
	fmt.Fprintf(&b, "%s: %s\r\n", HeaderEventID, msg.EventID)
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(strings.ReplaceAll(msg.Body, "\n", "\r\n"))
	b.WriteString("\r\n")
	return []byte(b.String())
}
