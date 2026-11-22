package secretscan

import "testing"

func TestScan(t *testing.T) {
	t.Parallel()
	hits := map[string]string{
		"-----BEGIN RSA PRIVATE KEY-----\nMIIE...":      "private_key",
		"export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE": "aws_access_key",
		"accessKeyId: LTAI5tFakeKeyExample12":           "aliyun_access_key",
		"Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c": "jwt",
		"password = Sup3rS3cret!":       "password_assignment",
		"api_key: \"abcdef1234567890\"": "password_assignment",
	}
	for text, kind := range hits {
		got := Scan(text)
		if len(got) == 0 || got[0].Kind != kind {
			t.Errorf("Scan(%q) = %+v, want kind %s", text, got, kind)
		}
	}
	clean := []string{
		"password=<your-password>",
		"password: ${DB_PASSWORD}",
		"token: xxxxxxxxxxxx",
		"用 password 参数指定口令，例如 password=changeme123",
		"短口令 password=abc",
		"普通正文：重启 nginx 后检查 502 是否消失。",
	}
	for _, text := range clean {
		if got := Scan(text); len(got) != 0 {
			t.Errorf("Scan(%q) = %+v, want no findings", text, got)
		}
	}
	multi := Scan("line1\npassword=RealPassw0rd\nline3\n-----BEGIN PRIVATE KEY-----")
	if len(multi) != 2 || multi[0].Line != 2 || multi[1].Line != 4 {
		t.Fatalf("multi-line findings = %+v", multi)
	}
}
