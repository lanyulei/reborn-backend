package channel

import (
	"bufio"
	"context"
	"encoding/base64"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// fakeSMTP 进程内最小 SMTP 服务：记录认证与邮件正文；RCPT reject@ 返回 550。
type fakeSMTP struct {
	authMech string // EHLO 通告的 AUTH 机制（空 = PLAIN）
	greylist bool   // RCPT 返回 450（临时拒绝）
	ln       net.Listener
	mu       sync.Mutex
	auth     string
	mail     []string
	rcpt     []string
	data     []string
}

func newFakeSMTP(t *testing.T) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSMTP{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeSMTP) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	w := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	w("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		cmd := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(cmd, "EHLO"):
			w("250-fake")
			if f.authMech != "" {
				w("250 AUTH " + f.authMech)
			} else {
				w("250 AUTH PLAIN")
			}
		case strings.HasPrefix(cmd, "AUTH PLAIN"):
			f.mu.Lock()
			f.auth = strings.TrimSpace(line[len("AUTH PLAIN"):])
			f.mu.Unlock()
			w("235 ok")
		case cmd == "AUTH LOGIN":
			w("334 VXNlcm5hbWU6") // "Username:"
			u, _ := r.ReadString('\n')
			w("334 UGFzc3dvcmQ6") // "Password:"
			pw, _ := r.ReadString('\n')
			f.mu.Lock()
			f.auth = "LOGIN " + strings.TrimSpace(u) + " " + strings.TrimSpace(pw)
			f.mu.Unlock()
			w("235 ok")
		case strings.HasPrefix(cmd, "MAIL FROM"):
			f.mu.Lock()
			f.mail = append(f.mail, line)
			f.mu.Unlock()
			w("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO"):
			if strings.Contains(line, "reject@") {
				w("550 no such user")
				continue
			}
			if f.greylist {
				w("450 4.7.1 greylisted, try again later")
				continue
			}
			f.mu.Lock()
			f.rcpt = append(f.rcpt, line)
			f.mu.Unlock()
			w("250 ok")
		case cmd == "DATA":
			w("354 go")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			f.mu.Lock()
			f.data = append(f.data, b.String())
			f.mu.Unlock()
			w("250 queued")
		case cmd == "QUIT":
			w("221 bye")
			return
		default:
			w("250 ok")
		}
	}
}

func (f *fakeSMTP) hostPort() (string, uint32) {
	host, port, _ := net.SplitHostPort(f.ln.Addr().String())
	p, _ := strconv.Atoi(port)
	return host, uint32(p) //nolint:gosec // 测试监听端口 <65536
}

func TestEmailSender_Send(t *testing.T) {
	t.Parallel()
	srv := newFakeSMTP(t)
	host, port := srv.hostPort()
	ch := &biz.Channel{
		Type: biz.ChannelTypeEmail, Secret: "pw",
		Email: &biz.EmailConfig{Host: host, Port: port, Username: "user", FromAddr: "noreply@example.com"},
	}
	msg := biz.RenderedMessage{EventID: "e1", Subject: "告警 ☃", Body: "line1\nline2", Recipient: "Ops <ops@example.com>", SendKey: "key-1"}
	res, err := NewEmailSender().Send(context.Background(), ch, msg)
	if err != nil || res.ProviderMsgID != "key-1" {
		t.Fatalf("Send() = %+v, %v", res, err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.mail) != 1 || !strings.Contains(srv.mail[0], "noreply@example.com") || len(srv.rcpt) != 1 || !strings.Contains(srv.rcpt[0], "ops@example.com") {
		t.Fatalf("envelope = %v / %v", srv.mail, srv.rcpt)
	}
	authRaw, _ := base64.StdEncoding.DecodeString(srv.auth)
	if string(authRaw) != "\x00user\x00pw" {
		t.Fatalf("auth = %q", authRaw)
	}
	if len(srv.data) != 1 || !strings.Contains(srv.data[0], "X-Notify-Send-Key: key-1") || !strings.Contains(srv.data[0], "=?utf-8?q?") ||
		!strings.Contains(srv.data[0], "line1\r\nline2") {
		t.Fatalf("message = %q", srv.data)
	}
}

func TestEmailSender_Failures(t *testing.T) {
	t.Parallel()
	srv := newFakeSMTP(t)
	host, port := srv.hostPort()
	s := NewEmailSender()
	ch := &biz.Channel{Type: biz.ChannelTypeEmail, Email: &biz.EmailConfig{Host: host, Port: port, FromAddr: "noreply@example.com"}}
	// 收件人被拒：不可重试。
	res, err := s.Send(context.Background(), ch, biz.RenderedMessage{Recipient: "reject@example.com", SendKey: "k"})
	if err == nil || res.Retryable {
		t.Fatalf("rejected rcpt: %+v %v", res, err)
	}
	// 非法地址：不可重试。
	res, err = s.Send(context.Background(), ch, biz.RenderedMessage{Recipient: "not-an-address", SendKey: "k"})
	if err == nil || res.Retryable {
		t.Fatalf("invalid address: %+v %v", res, err)
	}
	// 连接失败：可重试。
	dead := &biz.Channel{Type: biz.ChannelTypeEmail, Email: &biz.EmailConfig{Host: "127.0.0.1", Port: 1, FromAddr: "a@b.c"}}
	res, err = s.Send(context.Background(), dead, biz.RenderedMessage{Recipient: "x@example.com", SendKey: "k"})
	if err == nil || !res.Retryable {
		t.Fatalf("dial failure: %+v %v", res, err)
	}
	// 要求 STARTTLS 但服务端不支持：不可重试（配置错误）。
	tlsCh := &biz.Channel{Type: biz.ChannelTypeEmail, Email: &biz.EmailConfig{Host: host, Port: port, FromAddr: "a@b.c", StartTLS: true}}
	res, err = s.Send(context.Background(), tlsCh, biz.RenderedMessage{Recipient: "x@example.com", SendKey: "k"})
	if err == nil || res.Retryable {
		t.Fatalf("starttls unsupported: %+v %v", res, err)
	}
}

// TestEmailSender_LoginAuthAndGreylist 服务器只通告 AUTH LOGIN 时走 LOGIN 机制；RCPT 450
// （灰名单）按 4xx 可重试，而非像 550 一样死信。
func TestEmailSender_LoginAuthAndGreylist(t *testing.T) {
	t.Parallel()
	f := newFakeSMTP(t)
	f.authMech = "LOGIN"
	host, port := f.hostPort()
	ch := &biz.Channel{Type: biz.ChannelTypeEmail, Secret: "pw", Email: &biz.EmailConfig{Host: host, Port: port, Username: "u", FromAddr: "noreply@corp.example"}}
	msg := biz.RenderedMessage{Subject: "S", Body: "B", Recipient: "ops@example.com", SendKey: "k", EventID: "e"}
	if res, err := NewEmailSender().Send(context.Background(), ch, msg); err != nil || res.ProviderMsgID != "k" {
		t.Fatalf("Send(LOGIN) = %+v, %v", res, err)
	}
	f.mu.Lock()
	auth, data := f.auth, strings.Join(f.data, "")
	f.mu.Unlock()
	if !strings.HasPrefix(auth, "LOGIN dQ== cHc=") {
		t.Fatalf("auth = %q, want base64 LOGIN exchange", auth)
	}
	if !strings.Contains(data, "Message-ID: <k@corp.example>") {
		t.Fatalf("Message-ID domain must follow from_addr: %q", data)
	}
	f.greylist = true
	res, err := NewEmailSender().Send(context.Background(), ch, msg)
	if err == nil || !res.Retryable {
		t.Fatalf("450 greylist must be retryable: %+v %v", res, err)
	}
	f.greylist = false
	res, err = NewEmailSender().Send(context.Background(), ch, biz.RenderedMessage{Subject: "S", Body: "B", Recipient: "reject@example.com"})
	if err == nil || res.Retryable {
		t.Fatalf("550 must stay non-retryable: %+v %v", res, err)
	}
}

// TestEmailSender_HTML format=html → multipart/alternative（text/plain 降级 + text/html，
// quoted-printable）；text 路径的报文结构不变。
func TestEmailSender_HTML(t *testing.T) {
	t.Parallel()
	srv := newFakeSMTP(t)
	host, port := srv.hostPort()
	ch := &biz.Channel{Type: biz.ChannelTypeEmail, Secret: "pw", Email: &biz.EmailConfig{Host: host, Port: port, Username: "u", FromAddr: "noreply@example.com"}}
	msg := biz.RenderedMessage{EventID: "e1", Subject: "S", Body: "<h1>告警</h1><p>主机 &amp; 失联</p><style>x{}</style>", Format: "html", Recipient: "ops@example.com", SendKey: "k-html"}
	if _, err := NewEmailSender().Send(context.Background(), ch, msg); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	data := srv.data[0]
	srv.mu.Unlock()
	for _, want := range []string{"Content-Type: multipart/alternative; boundary=", "Content-Type: text/plain; charset=utf-8", "Content-Type: text/html; charset=utf-8", "Content-Transfer-Encoding: quoted-printable", "<h1>"} {
		if !strings.Contains(data, want) {
			t.Fatalf("html message missing %q:\n%s", want, data)
		}
	}
	if strings.Index(data, "text/plain") > strings.Index(data, "text/html") {
		t.Fatal("text/plain alternative must come first")
	}
	if got := htmlToText(msg.Body); got != "告警\n\n主机 & 失联" {
		t.Fatalf("htmlToText = %q", got)
	}
	// text 路径不变。
	plain := biz.RenderedMessage{EventID: "e2", Subject: "S", Body: "line", Recipient: "ops@example.com", SendKey: "k-text"}
	if _, err := NewEmailSender().Send(context.Background(), ch, plain); err != nil {
		t.Fatal(err)
	}
	srv.mu.Lock()
	data = srv.data[1]
	srv.mu.Unlock()
	if !strings.Contains(data, "Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit") || strings.Contains(data, "multipart") {
		t.Fatalf("text message changed: %s", data)
	}
}
