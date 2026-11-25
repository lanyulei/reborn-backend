package channel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/textproto"
	"os"
	"syscall"
	"testing"
)

// 网络类错误与处在会话的哪个阶段无关，一律可重试；此前 AUTH / RCPT 阶段把它们判成永久失败，
// 一次超时或连接重置就直接死信。
func TestSMTPRetryable_TransientIOAlwaysRetryable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		err      error
		fallback bool
		want     bool
	}{
		{"4xx on RCPT", &textproto.Error{Code: 452, Msg: "insufficient storage"}, false, true},
		{"5xx on RCPT", &textproto.Error{Code: 550, Msg: "no such user"}, false, false},
		{"read timeout on AUTH", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, false, true},
		{"connection reset on RCPT", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, false, true},
		{"EOF on AUTH", io.EOF, false, true},
		{"canceled on RCPT", context.Canceled, false, true},
		{"wrapped timeout", &net.OpError{Op: "write", Net: "tcp", Err: context.DeadlineExceeded}, false, true},
		{"auth config error", errors.New("unencrypted connection"), false, false},
		{"unknown without fallback", errors.New("boom"), false, false},
		{"unknown with fallback", errors.New("boom"), true, true},
	}
	for _, c := range cases {
		if got := smtpRetryable(c.err, c.fallback); got != c.want {
			t.Errorf("%s: smtpRetryable(fallback=%v) = %v, want %v", c.name, c.fallback, got, c.want)
		}
	}
}
