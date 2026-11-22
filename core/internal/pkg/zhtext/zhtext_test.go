package zhtext

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		in   string
		want []string
	}{
		"cjk bigram":   {"数据库连接池", []string{"数据", "据库", "库连", "连接", "接池"}},
		"single cjk":   {"库", []string{"库"}},
		"latin words":  {"Nginx 502 Bad-Gateway", []string{"nginx", "502", "bad", "gateway"}},
		"mixed":        {"重启nginx服务", []string{"重启", "nginx", "服务"}},
		"dedup":        {"重启 重启", []string{"重启"}},
		"punct only":   {"。。！！", nil},
		"empty":        {"", nil},
		"kana/hangul":  {"東京タワー", []string{"東京", "京タ", "タワ", "ワー"}},
		"underscore":   {"max_conns", []string{"max", "conns"}},
		"version-like": {"v1.2.3", []string{"v1", "2", "3"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := Tokenize(tc.in)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Tokenize(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestTokenizeLimit(t *testing.T) {
	t.Parallel()
	got := tokenize("一二三四五六七八九十", 3)
	if len(got) != 3 {
		t.Fatalf("limit: got %d tokens %v", len(got), got)
	}
}

func TestQueryAndDocument(t *testing.T) {
	t.Parallel()
	if q := Query("磁盘 满了"); q != "磁盘 | 满了" {
		t.Fatalf("Query = %q", q)
	}
	if q := Query("。"); q != "" {
		t.Fatalf("Query(punct) = %q, want empty", q)
	}
	if d := Document("CPU 高"); d != "cpu 高" {
		t.Fatalf("Document = %q", d)
	}
	// 查询侧与索引侧同一分词：查询词元必须是文档词元的子集。
	doc := Document("Nginx 返回 502 时先查上游连接池是否耗尽")
	for _, tok := range Tokenize("上游连接池 502") {
		if !containsWord(doc, tok) {
			t.Fatalf("query token %q not in document tokens %q", tok, doc)
		}
	}
}

func containsWord(doc, tok string) bool {
	for _, w := range splitWords(doc) {
		if w == tok {
			return true
		}
	}
	return false
}

func splitWords(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ' ' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
