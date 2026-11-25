package kube

import (
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// documents 把清单重新拆成对象，核对掩码后文档结构没变。
func documents(t *testing.T, manifest string) []map[string]any {
	t.Helper()
	var out []map[string]any
	dec := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	for {
		var obj map[string]any
		err := dec.Decode(&obj)
		if err != nil {
			break
		}
		if len(obj) > 0 {
			out = append(out, obj)
		}
	}
	return out
}

// Secret 的 binaryData 与 data / stringData 一样是内容，必须掩码。
func TestMaskManifestSecretsBinaryData(t *testing.T) {
	t.Parallel()
	const (
		blob = "LS0tLS1CRUdJTi0tLS0tY2VydC0tLS0t"
		pass = "c3VwZXItc2VjcmV0"
	)
	manifest := "---\n# Source: demo/templates/tls.yaml\napiVersion: v1\nkind: Secret\nmetadata:\n  name: tls\ndata:\n  password: " + pass +
		"\nbinaryData:\n  ca.crt: " + blob + "\n"
	out := maskManifestSecrets(manifest)
	for _, leak := range []string{blob, pass} {
		if strings.Contains(out, leak) {
			t.Fatalf("secret value %q leaked:\n%s", leak, out)
		}
	}
	if strings.Count(out, k8scontract.MaskedValue) != 2 || !strings.Contains(out, "# Source: demo/templates/tls.yaml") ||
		!strings.Contains(out, "ca.crt:") || !strings.Contains(out, "password:") {
		t.Fatalf("masked manifest:\n%s", out)
	}
	// 只有 binaryData 的 Secret 同样要掩码（原先 changed=false，整份原样返回）。
	only := "---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: only\nbinaryData:\n  key: " + blob + "\n"
	if out := maskManifestSecrets(only); strings.Contains(out, blob) || !strings.Contains(out, k8scontract.MaskedValue) {
		t.Fatalf("binaryData-only secret:\n%s", out)
	}
}

// List 的内容在 items[] 里：SecretList 的每一项、通用 List 里是 Secret 的条目都要掩码，其余字段保留。
func TestMaskManifestSecretsList(t *testing.T) {
	t.Parallel()
	const (
		passA = "cGFzc3dvcmQtYQ=="
		token = "plain-token"
		blob  = "YmxvYi1ieXRlcw=="
	)
	manifest := "---\napiVersion: v1\nkind: SecretList\nmetadata:\n  resourceVersion: \"7\"\nitems:\n" +
		"- apiVersion: v1\n  kind: Secret\n  metadata: {name: a}\n  data: {password: " + passA + "}\n  stringData: {token: " + token + "}\n" +
		"- apiVersion: v1\n  kind: Secret\n  metadata: {name: b}\n  binaryData: {blob: " + blob + "}\n"
	out := maskManifestSecrets(manifest)
	for _, leak := range []string{passA, token, blob} {
		if strings.Contains(out, leak) {
			t.Fatalf("secret value %q leaked:\n%s", leak, out)
		}
	}
	if strings.Count(out, k8scontract.MaskedValue) != 3 {
		t.Fatalf("every value in items must be masked:\n%s", out)
	}
	docs := documents(t, out)
	if len(docs) != 1 || docs[0]["kind"] != "SecretList" {
		t.Fatalf("structure changed: %v", docs)
	}
	items, _ := docs[0]["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %v", items)
	}
	for i, name := range []string{"a", "b"} {
		item, _ := items[i].(map[string]any)
		meta, _ := item["metadata"].(map[string]any)
		if item["kind"] != "Secret" || meta["name"] != name {
			t.Fatalf("item %d = %v", i, item)
		}
	}
	// 没有 Secret 内容的 List 原样返回。
	empty := "---\napiVersion: v1\nkind: SecretList\nitems: []\n"
	if maskManifestSecrets(empty) != empty {
		t.Fatalf("an empty list must be returned verbatim:\n%s", maskManifestSecrets(empty))
	}

	// 通用的 kind: List（Helm 会把这类模板拆成单个对象创建，清单里留的仍是 List 文档）：只掩码其中是 Secret 的
	// 条目，其余条目与结构原样。
	generic := "---\napiVersion: v1\nkind: List\nitems:\n" +
		"- apiVersion: v1\n  kind: Secret\n  metadata: {name: s}\n  data: {password: " + passA + "}\n" +
		"- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: c}\n  data: {k: v}\n"
	out = maskManifestSecrets(generic)
	if strings.Contains(out, passA) || strings.Count(out, k8scontract.MaskedValue) != 1 || !strings.Contains(out, "k: v") {
		t.Fatalf("generic list:\n%s", out)
	}
	docs = documents(t, out)
	if len(docs) != 1 || docs[0]["kind"] != "List" {
		t.Fatalf("structure changed: %v", docs)
	}
	items, _ = docs[0]["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %v", items)
	}
	for i, want := range []struct{ kind, name string }{{"Secret", "s"}, {"ConfigMap", "c"}} {
		item, _ := items[i].(map[string]any)
		meta, _ := item["metadata"].(map[string]any)
		if item["kind"] != want.kind || meta["name"] != want.name {
			t.Fatalf("item %d = %v", i, item)
		}
	}
	// List 里没有 Secret：整份原样。
	noSecrets := "---\napiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  data: {note: Secret}\n"
	if maskManifestSecrets(noSecrets) != noSecrets {
		t.Fatalf("a list without secrets must be returned verbatim:\n%s", maskManifestSecrets(noSecrets))
	}
}

// 文档标记的几种合法写法，原先都让 Secret 绕过掩码：内容与分隔符写在同一行（`--- {…}`，标记后可以是空格
// 或制表符；原先这样的行被当作纯分隔符原样写出）、文档结束标记 `...` 之后不再写 `---` 直接开始下一个文档
// （原先两个文档落在同一处，只检查了第一个）。内容同样要过掩码；非 Secret 文档逐字节原样。
func TestMaskManifestSecretsMarkerForms(t *testing.T) {
	t.Parallel()
	const pass = "cGFzc3dvcmQ="
	// 非 Secret 的文档里带有 Secret 字样：确保走的是逐文档处理，而不是开头的快速返回。
	cm := "apiVersion: v1\nkind: ConfigMap\ndata: {note: Secret}\n"
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: s}\ndata: {password: " + pass + "}\n"

	// 掩码后：不泄漏；文档数、顺序、类型不变。
	masked := func(name, in string, kinds ...string) string {
		t.Helper()
		out := maskManifestSecrets(in)
		if strings.Contains(out, pass) {
			t.Fatalf("%s: the secret leaked:\n%s", name, out)
		}
		docs := documents(t, out)
		if len(docs) != len(kinds) {
			t.Fatalf("%s: %d documents, want %d:\n%s", name, len(docs), len(kinds), out)
		}
		for i, kind := range kinds {
			if docs[i]["kind"] != kind {
				t.Fatalf("%s: document %d is %v, want %s:\n%s", name, i, docs[i]["kind"], kind, out)
			}
		}
		return out
	}
	// 原样：非 Secret 文档在任何写法下逐字节不变。
	verbatim := func(name, in string) {
		t.Helper()
		if out := maskManifestSecrets(in); out != in {
			t.Fatalf("%s: non-secret documents must be returned verbatim:\nin:\n%s\nout:\n%s", name, in, out)
		}
	}

	// 内容与分隔符同一行。
	out := masked("inline", cm+"--- {kind: Secret, apiVersion: v1, metadata: {name: s}, data: {password: "+pass+"}}\n---\n"+secret,
		"ConfigMap", "Secret", "Secret")
	if !strings.HasPrefix(out, cm+"---\n") || strings.Count(out, "\n---\n") != 2 {
		t.Fatalf("separators must be kept, one before each following document:\n%s", out)
	}
	masked("inline after a tab", cm+"---\t{kind: Secret, apiVersion: v1, data: {password: "+pass+"}}\n", "ConfigMap", "Secret")
	masked("inline CRLF", "apiVersion: v1\nkind: ConfigMap\r\n--- {kind: Secret, apiVersion: v1, data: {password: "+pass+"}}\r\n", "ConfigMap", "Secret")
	masked("inline at EOF without newline", cm+"--- {kind: Secret, data: {password: "+pass+"}}", "ConfigMap", "Secret")

	// 分隔符后只有空白（空格 / 制表符）：仍是纯分隔符，原样保留。
	for _, sep := range []string{"--- ", "---   ", "---\t"} {
		if out := masked("bare "+strings.TrimPrefix(sep, "---"), cm+sep+"\n"+secret, "ConfigMap", "Secret"); !strings.HasPrefix(out, cm+sep+"\n") {
			t.Fatalf("separator %q must be kept verbatim:\n%s", sep, out)
		}
	}

	// 注释与分隔符同一行：注释留在文档里（掩码后仍在开头）。
	if out := masked("comment on the separator line", cm+"--- # Source: demo/templates/secret.yaml\n"+secret, "ConfigMap", "Secret"); !strings.Contains(out, "# Source: demo/templates/secret.yaml") {
		t.Fatalf("the comment on the separator line must be kept:\n%s", out)
	}

	// 文档结束标记之后直接开始下一个文档。apimachinery 的分文档器只认 `---`，这里以结束标记这一行为界
	// 分别核对前后两段。
	isMaskedSecret := func(name, text string) {
		t.Helper()
		docs := documents(t, text)
		if len(docs) != 1 || docs[0]["kind"] != "Secret" {
			t.Fatalf("%s: expected one secret document, got %v:\n%s", name, docs, text)
		}
		if data, _ := docs[0]["data"].(map[string]any); data["password"] != k8scontract.MaskedValue {
			t.Fatalf("%s: the password is not masked: %v", name, data)
		}
	}
	afterEndMarker := func(in, marker string) (before, after string) {
		t.Helper()
		out := maskManifestSecrets(in)
		i := strings.Index(out, marker+"\n")
		if strings.Contains(out, pass) || i < 0 {
			t.Fatalf("end marker %q: leaked or the marker was dropped:\n%s", marker, out)
		}
		return out[:i+len(marker)+1], out[i+len(marker)+1:]
	}
	before, after := afterEndMarker(cm+"...\n"+secret, "...")
	if before != cm+"...\n" {
		t.Fatalf("text before the end marker must be verbatim:\n%s", before)
	}
	isMaskedSecret("bare document after the end marker", after)
	before, after = afterEndMarker(cm+"... # end\n"+secret, "... # end")
	if before != cm+"... # end\n" {
		t.Fatalf("text before the end marker must be verbatim:\n%s", before)
	}
	isMaskedSecret("bare document after the end marker with a comment", after)
	before, after = afterEndMarker(secret+"...\n"+cm, "...")
	isMaskedSecret("secret before the end marker", before[:len(before)-len("...\n")])
	if after != cm {
		t.Fatalf("the document after the end marker must be verbatim:\n%s", after)
	}

	// 非 Secret 文档：各种写法逐字节原样。缩进的 `---` 不是文档标记，`---x` 是普通内容。
	plain := "--- # c\napiVersion: v1\nkind: Pod\n--- \n---\t\nkind: Pod\n...\nkind: Pod\n... # end\n"
	verbatim("bare separators", cm+plain)
	verbatim("inline non-secret", cm+"--- # Source: x\nkind: Pod\n"+cm+"--- {kind: Pod}\n---\t{kind: Pod}\n")
	verbatim("indented marker is not a marker", cm+"  --- {kind: Pod}\n")
	verbatim("dashes without a space are content", cm+"---x\n")
}
