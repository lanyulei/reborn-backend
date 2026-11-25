package httpx

import (
	"log/slog"
	"strings"
	"testing"
)

// 脱敏正则此前要求关键字前是单词边界：old_password / become_secret / client_secret 这类带前缀的字段名
// 整段漏打码，修改密码的旧密码与新密码、作业提权密码、采集凭据都曾明文进入访问日志。
func TestMaskSensitiveAttr_PrefixedAndCredentialFields(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"change password", `user_id:"018f" old_password:"Old-123!" new_password:"New-456!"`,
			`user_id:"018f" old_password:"***" new_password:"***"`,
		}, {
			"job credential", `spec:{name:"prod-ssh"  type:2  secret:"ssh-password-1"  become_method:"sudo"  become_secret:"Sudo-Pass-9"}`,
			`spec:{name:"prod-ssh"  type:2  secret:"***"  become_method:"sudo"  become_secret:"***"}`,
		}, {
			"cmdb source credential", `name:"aliyun-prod"  config:{}  credential:"LTAI5tAccessKeySecretXYZ"`,
			`name:"aliyun-prod"  config:{}  credential:"***"`,
		}, {
			"k8s cluster credential", `credential:{token:"tok-1" client_cert:"CERT" client_key:"-----BEGIN PRIVATE KEY-----\nMIGH\n"}`,
			`credential:{token:"***" client_cert:"CERT" client_key:"***"}`,
		}, {
			"credential-like header names beyond the old blacklist", `webhook:{url:"https://hooks.example.com/x" headers:{key:"X-Token" value:"REAL-SECRET-TOKEN"} headers:{key:"X-Auth-Token" value:"t2"} headers:{key:"Private-Token" value:"glpat-x"} headers:{key:"X-Trace" value:"t"} method:"POST"}`,
			`webhook:{url:"https://hooks.example.com/x" headers:{key:"X-Token" value:"***"} headers:{key:"X-Auth-Token" value:"***"} headers:{key:"Private-Token" value:"***"} headers:{key:"X-Trace" value:"t"} method:"POST"}`,
		}, {
			"map entry with a sensitive key", `attrs:{key:"db_password" value:"hunter2hunter"} attrs:{key:"ip" value:"10.0.0.1"}`,
			`attrs:{key:"db_password" value:"***"} attrs:{key:"ip" value:"10.0.0.1"}`,
		}, {
			"uppercase names", `Password:"Abc12345!" Authorization:"Bearer t0ken123"`,
			`Password:"***" Authorization:"***"`,
		}, {
			"escaped quotes inside the value", `password:"ab\"cd\\\"ef" nickname:"n"`,
			`password:"***" nickname:"n"`,
		}, {
			"non-sensitive fields are kept", `keyword:"password" has_secret:false user_id:"018f" name:"password-reset-job" become_method:"sudo" token_count:3`,
			`keyword:"password" has_secret:false user_id:"018f" name:"password-reset-job" become_method:"sudo" token_count:3`,
		},
	}
	for _, c := range cases {
		if got := MaskSensitiveAttr(nil, slog.String("args", c.in)).Value.String(); got != c.want {
			t.Errorf("%s:\n got  %s\n want %s", c.name, got, c.want)
		}
	}
}

// 掩码仍受 4096 字节截断约束：上传类请求不会产生超长日志。
func TestMaskSensitiveAttr_StillTruncates(t *testing.T) {
	in := `data:"` + strings.Repeat("A", 10<<10) + `"`
	got := MaskSensitiveAttr(nil, slog.String("args", in)).Value.String()
	if len(got) > 4200 || !strings.Contains(got, "truncated") {
		t.Fatalf("masked args length = %d, want a truncated line", len(got))
	}
}
