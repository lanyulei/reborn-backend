package runner

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// maskText 凭据在日志与错误信息里的替身。
	maskText = "********"
	// minSecretLen 参与掩码的最短凭据长度：更短的值（如 "1"、"ab"）掩码会把正常输出打成筛子，
	// 而它们本身也谈不上保密强度。
	minSecretLen = 4
)

// pemArmor PEM 的首尾标记行（-----BEGIN … PRIVATE KEY----- / -----END …-----）：不是秘密，掩码它们只会
// 误伤正常输出。只认这种形状——别的以 ----- 开头的行（比如恰好这样开头的密码）照常掩码。
var pemArmor = regexp.MustCompile(`^-----(BEGIN|END) [A-Z0-9 ]+-----$`)

// masker 把输出里出现的凭据替换为 maskText。流式使用：Write 返回可以安全外发的部分，
// 把「可能是某个凭据前半截」的尾巴与不完整的 UTF-8 序列留到下一次；Flush 吐出剩余全部。
// 非并发安全（每条输出流一个实例）。
type masker struct {
	secrets []string // 去重，按长度降序
	pending []byte   // 尚未外发的原始输出（未掩码）
}

// newMasker 由凭据明文构造。多行凭据（私钥）按行拆开，每一行都是独立的掩码目标；
// 另加入常见的编码变体（JSON 转义、URL 编码）——工具回显凭据时未必是原样。
func newMasker(secrets ...string) *masker {
	seen := map[string]struct{}{}
	add := func(s string) {
		if len(s) < minSecretLen {
			return
		}
		seen[s] = struct{}{}
	}
	for _, secret := range secrets {
		for _, line := range strings.Split(secret, "\n") {
			line = strings.TrimSpace(line)
			if pemArmor.MatchString(line) {
				continue
			}
			add(line)
			if enc, err := json.Marshal(line); err == nil {
				add(string(enc[1 : len(enc)-1]))
			}
			add(url.QueryEscape(line))
			add(url.PathEscape(line))
		}
	}
	m := &masker{secrets: make([]string, 0, len(seen))}
	for s := range seen {
		m.secrets = append(m.secrets, s)
	}
	sort.Slice(m.secrets, func(i, j int) bool {
		if len(m.secrets[i]) != len(m.secrets[j]) {
			return len(m.secrets[i]) > len(m.secrets[j])
		}
		return m.secrets[i] < m.secrets[j]
	})
	return m
}

// span 文本里一段要掩码的区间 [start, end)。
type span struct{ start, end int }

// spans 找出 s 里全部凭据的全部出现位置，并把相互重叠的合并成一段（按起点排序）。逐个凭据独立查找再合并，
// 而不是「从左到右、匹配一个跳过一个」：后者在两个凭据重叠出现时（abcd 与 bcdefgh 出现在 abcdefgh 里）
// 会漏掉后一个的尾巴。首尾相接但不重叠的出现各自成段。
func (m *masker) spans(s string) []span {
	var found []span
	for _, secret := range m.secrets {
		for from := 0; from < len(s); {
			i := strings.Index(s[from:], secret)
			if i < 0 {
				break
			}
			found = append(found, span{from + i, from + i + len(secret)})
			from += i + 1 // 允许同一凭据与自己重叠的出现
		}
	}
	if len(found) < 2 {
		return found
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].start != found[j].start {
			return found[i].start < found[j].start
		}
		return found[i].end > found[j].end
	})
	merged := found[:1]
	for _, sp := range found[1:] {
		last := &merged[len(merged)-1]
		switch {
		case sp.start < last.end:
			last.end = max(last.end, sp.end)
		default:
			merged = append(merged, sp)
		}
	}
	return merged
}

// redact 按区间把 s 掩码。
func redact(s string, spans []span) string {
	if len(spans) == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	pos := 0
	for _, sp := range spans {
		b.WriteString(s[pos:sp.start])
		b.WriteString(maskText)
		pos = sp.end
	}
	b.WriteString(s[pos:])
	return b.String()
}

// taskSecrets 一个任务里全部需要掩码的值：登录私钥 / 密码、提权密码、Git 令牌或私钥，
// 以及 Git 令牌的 Basic 认证编码（git 报错时可能回显请求头）。
func taskSecrets(t *jobcontract.Task) []string {
	out := []string{t.Credential.Secret, t.Credential.BecomeSecret, t.Git.Secret}
	if t.Git.Type == jobcontract.GitAuthToken {
		out = append(out, basicAuth(gitUser(t.Git), t.Git.Secret))
	}
	return out
}

// gitUser 令牌认证用的用户名：未填时用占位用户名 git（多数平台只看令牌）。请求头与掩码目标都取它，
// 两者必须是同一个值。
func gitUser(auth jobcontract.GitAuth) string {
	if auth.Username == "" {
		return "git"
	}
	return auth.Username
}

// basicAuth HTTP Basic 认证的凭据编码。
func basicAuth(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}

// Mask 一次性掩码一段完整文本（错误信息等）。
func (m *masker) Mask(s string) string {
	if len(m.secrets) == 0 {
		return s
	}
	return redact(s, m.spans(s))
}

// Write 送入一段输出，返回可以外发的已掩码文本（合法 UTF-8）。留存的是原始输出：掩码之后再留存，
// 跨两次写入、又与已掩码部分重叠的凭据就认不出来了。
func (m *masker) Write(p []byte) string {
	m.pending = append(m.pending, p...)
	// 末尾不完整的 UTF-8 序列留到下一次（读管道可能把一个字符切成两半）。
	keep := incompleteSuffix(m.pending)
	text := string(m.pending[:len(m.pending)-keep])
	// 末尾若是某个凭据的前半截，留下它等后半截；切点落在某段待掩码区间之内时退到该区间的起点，
	// 不把一段区间拆开（它可能还要与后面的内容合并）。
	cut := len(text) - m.partialSuffix(text)
	var emit []span
	for _, sp := range m.spans(text) {
		if sp.end <= cut {
			emit = append(emit, sp)
			continue
		}
		if sp.start < cut {
			cut = sp.start
		}
		break
	}
	out := redact(text[:cut], emit)
	m.pending = append(m.pending[:0], m.pending[cut:]...)
	return strings.ToValidUTF8(out, "�")
}

// Flush 吐出留存的尾巴（输出结束时调用）。
func (m *masker) Flush() string {
	out := m.Mask(string(m.pending))
	m.pending = m.pending[:0]
	return strings.ToValidUTF8(out, "�")
}

// partialSuffix 返回 text 末尾「是某个凭据的真前缀」的最长长度。
func (m *masker) partialSuffix(text string) int {
	longest := 0
	for _, s := range m.secrets {
		for n := min(len(s)-1, len(text)); n > longest; n-- {
			if strings.HasSuffix(text, s[:n]) {
				longest = n
				break
			}
		}
	}
	// 不把一个字符切成两半。
	for longest > 0 && longest < len(text) && !utf8.RuneStart(text[len(text)-longest]) {
		longest++
	}
	return longest
}

// incompleteSuffix 返回 b 末尾不完整的 UTF-8 序列的字节数（0-3）。
func incompleteSuffix(b []byte) int {
	for i := 1; i <= min(utf8.UTFMax-1, len(b)); i++ {
		c := b[len(b)-i]
		if utf8.RuneStart(c) {
			if c >= utf8.RuneSelf && !utf8.FullRune(b[len(b)-i:]) {
				return i
			}
			return 0
		}
	}
	return 0
}
