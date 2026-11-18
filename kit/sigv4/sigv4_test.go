package sigv4

import (
	"strings"
	"testing"
	"time"
)

// AWS 官方测试向量（get-vanilla）。
func TestAuthorization_KnownVector(t *testing.T) {
	t.Parallel()
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	got := Authorization("GET", "/", "", map[string]string{"Host": "example.amazonaws.com", "X-Amz-Date": "20150830T123600Z"},
		HashHex(nil), now, "us-east-1", "service", "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
	const want = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
		"SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got != want {
		t.Fatalf("Authorization() =\n%s\nwant\n%s", got, want)
	}
}

// 头名大小写、值两端空白不影响签名；任一入参变化签名随之变化。
func TestAuthorization_Canonicalization(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sig := func(headers map[string]string, body, secret string) string {
		return Authorization("PUT", "/b/k", "", headers, HashHex([]byte(body)), now, "r", "s3", "ak", secret)
	}
	base := sig(map[string]string{"host": "h", "x-amz-date": "d"}, "x", "sk")
	if sig(map[string]string{"HOST": " h ", "X-Amz-Date": "d"}, "x", "sk") != base {
		t.Error("header name case / surrounding spaces must not change the signature")
	}
	for name, other := range map[string]string{
		"body":   sig(map[string]string{"host": "h", "x-amz-date": "d"}, "y", "sk"),
		"secret": sig(map[string]string{"host": "h", "x-amz-date": "d"}, "x", "sk2"),
		"header": sig(map[string]string{"host": "h2", "x-amz-date": "d"}, "x", "sk"),
	} {
		if other == base {
			t.Errorf("changing the %s must change the signature", name)
		}
	}
	if !strings.Contains(base, "Credential=ak/20260102/r/s3/aws4_request") {
		t.Errorf("scope = %s", base)
	}
}
