package secretscan

import "testing"

// 带前缀的键名（db_password、client_secret、access_token …）是真实配置里最常见的写法，此前全部漏报。
func TestScan_PrefixedKeyNames(t *testing.T) {
	t.Parallel()
	const secret = "Abcdefgh12345"
	for _, line := range []string{
		"db_password=" + secret,
		"DB_PASSWORD=" + secret,
		"client_secret: " + secret,
		`access_token="abcdefghijklmnopqrstuvwxyz0123"`,
		"auth_token=" + secret,
		"private_token=" + secret,
		"mysql.password: " + secret,
	} {
		got := Scan("export FOO=bar\n" + line + "\n")
		if len(got) != 1 || got[0].Kind != "password_assignment" || got[0].Line != 2 {
			t.Errorf("Scan(%q) = %+v, want one password_assignment on line 2", line, got)
		}
	}
	for _, line := range []string{
		"password=<your-password>",
		"password=xxxxxxxx",
		"imagePullSecrets: [{name: regcred}]",
		"secretName: db-secret-credential",
		"tokenUrl: https://example.com/token",
		"password_file: /etc/shadow",
		"the password is rotated every quarter",
	} {
		if got := Scan(line); len(got) != 0 {
			t.Errorf("Scan(%q) = %+v, want no finding", line, got)
		}
	}
}
