package biz

import (
	"errors"
	"strings"

	"golang.org/x/crypto/ssh"
)

// normalizePrivateKey 规整并校验 SSH 私钥：换行统一为 LF、结尾补换行（ssh-add 要求），能被解析，
// 且不带口令（执行节点以非交互方式加载私钥，无法输入口令）。返回规整后的私钥与其公钥指纹。
// 错误说明不含私钥内容。
func normalizePrivateKey(raw string) (key, fingerprint string, err error) {
	key = strings.TrimSpace(strings.ReplaceAll(raw, "\r\n", "\n"))
	if key == "" {
		return "", "", errors.New("private key must not be blank")
	}
	key += "\n"
	signer, err := ssh.ParsePrivateKey([]byte(key))
	if err != nil {
		var missing *ssh.PassphraseMissingError
		if errors.As(err, &missing) {
			return "", "", errors.New("passphrase-protected private keys are not supported: remove the passphrase first (ssh-keygen -p)")
		}
		return "", "", errors.New("not a valid private key (expect OpenSSH or PEM format)")
	}
	return key, ssh.FingerprintSHA256(signer.PublicKey()), nil
}

// checkPassword 密码 / 令牌类密文：不得为空、不得含换行（会被写进单行的变量文件与请求头）。
func checkPassword(s, what string) error {
	if s == "" {
		return errors.New(what + " must not be blank")
	}
	if strings.ContainsAny(s, "\r\n\x00") {
		return errors.New(what + " must not contain line breaks")
	}
	return nil
}

// uniq 去掉空串与重复项，保持顺序。
func uniq(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
