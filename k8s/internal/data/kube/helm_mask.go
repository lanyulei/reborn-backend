package kube

import (
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"sigs.k8s.io/yaml"
)

// maskManifestSecrets 把 Helm 渲染清单里 Secret 文档的 data / stringData 的值换成掩码，其余文档原样保留。
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
		if t := strings.TrimRight(l, "\r\n"); t == "---" || strings.HasPrefix(t, "--- ") {
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
	if kind, _ := obj["kind"].(string); kind != "Secret" && kind != "SecretList" {
		return doc
	}
	changed := false
	for _, field := range []string{"data", "stringData"} {
		if m, ok := obj[field].(map[string]any); ok {
			for k := range m {
				m[k] = k8scontract.MaskedValue
				changed = true
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
	// 保留文档开头的注释行（# Source: …），方便对照是哪个模板。
	var head strings.Builder
	for _, l := range strings.SplitAfter(doc, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			break
		}
		head.WriteString(l)
	}
	return head.String() + string(raw)
}
