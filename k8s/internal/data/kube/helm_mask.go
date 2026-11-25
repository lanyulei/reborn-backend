package kube

import (
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"sigs.k8s.io/yaml"
)

// splitMarker 报告一行（已去掉行尾换行）是否以文档标记 marker（`---` 或 `...`）开始，并返回标记之后的部分。
// 标记之后必须紧同行尾、空格或制表符；紧跟其他字符的是普通内容（如 `---x`），不是标记。
func splitMarker(t, marker string) (rest string, ok bool) {
	if !strings.HasPrefix(t, marker) {
		return "", false
	}
	if rest = t[len(marker):]; rest != "" && rest[0] != ' ' && rest[0] != '\t' {
		return "", false
	}
	return rest, true
}

// maskManifestSecrets 把 Helm 渲染清单里 Secret 文档的 data / stringData / binaryData 的值换成掩码
// （List 文档掩码 items 里的 Secret 条目），其余文档原样保留。
// Release 详情与预检把清单给人看；Secret 对象在别处读取时是掩码的、看明文要单独的权限并记审计，
// 不能因为它出现在清单里就绕过去。解析不了的文档原样返回（它不可能是一个能被集群接受的 Secret）。
func maskManifestSecrets(manifest string) string {
	if !strings.Contains(manifest, "Secret") {
		return manifest
	}
	lines := strings.SplitAfter(manifest, "\n")
	var (
		out strings.Builder
		doc []string
	)
	flush := func() {
		if len(doc) == 0 {
			return
		}
		out.WriteString(maskSecretDoc(strings.Join(doc, "")))
		doc = doc[:0]
	}
	for _, l := range lines {
		t := strings.TrimRight(l, "\r\n")
		if rest, ok := splitMarker(t, "---"); ok {
			flush()
			if strings.TrimSpace(rest) == "" { // 纯分隔符：其后至多空白
				out.WriteString(l)
				continue
			}
			// 分隔符之后还有内容（`--- {kind: Secret, ...}`、`--- # 注释` 都是合法 YAML）：这一行是下一个
			// 文档的第一行，同样要交给掩码处理，不能当作纯分隔符原样写出。
		} else if _, ok := splitMarker(t, "..."); ok {
			// 文档结束标记：其后可以不再写 `---` 就开始下一个文档，这里同样要把文档切开。
			flush()
			out.WriteString(l)
			continue
		}
		doc = append(doc, l)
	}
	flush()
	return out.String()
}

func maskSecretDoc(doc string) string {
	if !strings.Contains(doc, "Secret") {
		return doc
	}
	var obj map[string]any
	if err := yaml.Unmarshal([]byte(doc), &obj); err != nil || obj == nil {
		return doc
	}
	kind, _ := obj["kind"].(string)
	if kind != "Secret" && kind != "SecretList" && kind != "List" {
		return doc
	}
	// List 的内容在 items[] 里：SecretList 的每一项都是 Secret；通用的 List（Helm 安装时把这类模板拆成单个对象
	// 创建，清单里留的仍是 List 文档）只掩码其中是 Secret 的条目。顶层也扫一遍（List 上不该有 data，真有同样
	// 不能外泄）。
	targets := []map[string]any{obj}
	if kind == "SecretList" || kind == "List" {
		items, _ := obj["items"].([]any)
		for _, it := range items {
			if m, ok := it.(map[string]any); ok && (kind == "SecretList" || m["kind"] == "Secret") {
				targets = append(targets, m)
			}
		}
	}
	changed := false
	for _, target := range targets {
		// binaryData 与 data / stringData 一样是 Secret 的内容，同样掩码。
		for _, field := range []string{"data", "stringData", "binaryData"} {
			if m, ok := target[field].(map[string]any); ok {
				for k := range m {
					m[k] = k8scontract.MaskedValue
					changed = true
				}
			}
		}
	}
	if !changed {
		return doc
	}
	raw, err := yaml.Marshal(obj)
	if err != nil {
		return "# secret content omitted\n"
	}
	// 保留文档开头的分隔符行与注释行（# Source: …），方便对照是哪个模板。
	var head strings.Builder
	for i, l := range strings.SplitAfter(doc, "\n") {
		text := strings.TrimRight(l, "\r\n")
		comment := strings.HasPrefix(strings.TrimSpace(text), "#")
		rest, _ := splitMarker(text, "---")
		marker := i == 0 && rest != "" // 文档以「分隔符 + 内容 / 注释」同一行开始
		if !comment && !marker {
			break // 对象从这一行开始
		}
		if marker && !strings.HasPrefix(strings.TrimSpace(rest), "#") {
			head.WriteString("---\n") // 带了内容：只保留分隔符，内容由下面重新生成的 YAML 取代
		} else {
			head.WriteString(l) // 注释行；只带注释的分隔符行
		}
	}
	return head.String() + string(raw)
}
