package docparse

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"unicode/utf16"
	"unicode/utf8"
)

// utf16WithBOM 把文本编成带 BOM 的 UTF-16。
func utf16WithBOM(s string, order binary.ByteOrder) []byte {
	units := utf16.Encode([]rune(s))
	out := make([]byte, 0, 2+2*len(units))
	if order == binary.BigEndian {
		out = append(out, bomUTF16BE...)
	} else {
		out = append(out, bomUTF16LE...)
	}
	var unit [2]byte
	for _, u := range units {
		order.PutUint16(unit[:], u)
		out = append(out, unit[:]...)
	}
	return out
}

// 文本类文件是带 BOM 的 UTF-16（记事本「Unicode」另存的 md / txt / html）：转成 UTF-8 后正常解析。
func TestParseText_UTF16WithBOM(t *testing.T) {
	t.Parallel()
	const md = "# 告警处理\r\n\r\n第一行：重启服务 emoji 🙂\r\n"
	for _, tc := range []struct {
		name  string
		order binary.ByteOrder
	}{{"le", binary.LittleEndian}, {"be", binary.BigEndian}} {
		data := utf16WithBOM(md, tc.order)
		if utf8.Valid(data) {
			t.Fatalf("%s: fixture must not be valid UTF-8", tc.name)
		}
		got, err := Parse("markdown", data)
		if err != nil {
			t.Fatalf("%s: Parse(markdown) error = %v", tc.name, err)
		}
		if want := "# 告警处理\n\n第一行：重启服务 emoji 🙂\n"; got != want {
			t.Fatalf("%s: Parse(markdown) = %q, want %q", tc.name, got, want)
		}
		// 文件开头再带一个 U+FEFF（两个文件拼接）：与 UTF-8 BOM 一样被去掉。
		got, err = Parse("text", utf16WithBOM("\uFEFF正文", tc.order))
		if err != nil || got != "正文\n" {
			t.Fatalf("%s: Parse(text, doubled BOM) = %q, %v", tc.name, got, err)
		}
	}

	html := utf16WithBOM("<html><body><h1>标题</h1><p>段落 <b>加粗</b></p></body></html>", binary.LittleEndian)
	got, err := Parse("html", html)
	if err != nil {
		t.Fatalf("Parse(html) error = %v", err)
	}
	if got != "# 标题\n\n段落 加粗\n" {
		t.Fatalf("Parse(html) = %q", got)
	}
}

// 不是 UTF-8 也没有 UTF-16 BOM 的文件（如记事本「ANSI」另存的 GBK）：返回 ErrEncoding，而不是把非法字节
// 当正文返回、留到写库才失败。
func TestParseText_NotUTF8ReturnsErrEncoding(t *testing.T) {
	t.Parallel()
	gbk := []byte{0xb8, 0xe6, 0xbe, 0xaf, 0xb4, 0xa6, 0xc0, 0xed, 0xb1, 0xca, 0xbc, 0xc7} // 「告警处理笔记」的 GBK 编码
	if utf8.Valid(gbk) {
		t.Fatal("fixture must not be valid UTF-8")
	}
	for _, kind := range []string{"markdown", "text", "html"} {
		got, err := Parse(kind, gbk)
		if !errors.Is(err, ErrEncoding) {
			t.Fatalf("Parse(%s, gbk) = %q, %v; want ErrEncoding", kind, got, err)
		}
		if got != "" {
			t.Fatalf("Parse(%s, gbk) must not return content", kind)
		}
	}
	// 没有 BOM 的 UTF-16（非 ASCII）同样无法识别。
	raw := utf16.Encode([]rune("告警"))
	var noBOM []byte
	for _, u := range raw {
		noBOM = binary.LittleEndian.AppendUint16(noBOM, u)
	}
	if _, err := Parse("text", noBOM); !errors.Is(err, ErrEncoding) {
		t.Fatalf("Parse(text, UTF-16 without BOM) error = %v, want ErrEncoding", err)
	}
	// UTF-32LE 的 BOM 以 UTF-16LE 的 BOM 开头：不能当成 UTF-16 解出满篇错字。
	utf32le := []byte{0xFF, 0xFE, 0x00, 0x00, 0x3C, 0x00, 0x00, 0x00}
	if _, err := Parse("text", utf32le); !errors.Is(err, ErrEncoding) {
		t.Fatalf("Parse(text, UTF-32LE BOM) error = %v, want ErrEncoding", err)
	}
	utf32be := []byte{0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 0x3C}
	if _, err := Parse("text", utf32be); !errors.Is(err, ErrEncoding) {
		t.Fatalf("Parse(text, UTF-32BE BOM) error = %v, want ErrEncoding", err)
	}
}

// 合法 UTF-8 的处理不变：内容原样（含 UTF-8 BOM 的去除、换行统一），不带 BOM 的纯 ASCII UTF-16 仍按原样
// 去掉 NUL 后得到文字。
func TestParseText_ValidUTF8Unchanged(t *testing.T) {
	t.Parallel()
	body := []byte("# 标题\r\n正文有：中文、emoji 🙂\r\n\x00")
	for _, kind := range []string{"markdown", "text"} {
		got, err := Parse(kind, body)
		if err != nil || got != "# 标题\n正文有：中文、emoji 🙂\n" {
			t.Fatalf("Parse(%s) = %q, %v", kind, got, err)
		}
	}
	asciiUTF16 := []byte("H\x00e\x00l\x00l\x00o\x00")
	if !utf8.Valid(asciiUTF16) {
		t.Fatal("fixture must be valid UTF-8")
	}
	if got, err := Parse("text", asciiUTF16); err != nil || got != "Hello\n" {
		t.Fatalf("Parse(text, ASCII UTF-16LE) = %q, %v", got, err)
	}
	// 只有一个 BOM：按空文件处理，与此前一致。
	if got, err := Parse("text", []byte("\xEF\xBB\xBF")); err != nil || got != "\n" {
		t.Fatalf("Parse(text, UTF-8 BOM only) = %q, %v", got, err)
	}
	// 二进制内容（pdf / docx 是容器格式）不经编码检查。
	if _, err := Parse("pdf", []byte("%PDF-1.7 garbage")); err == nil || errors.Is(err, ErrEncoding) {
		t.Fatalf("Parse(pdf) error = %v, want a parse error", err)
	}
	if !strings.Contains(ErrEncoding.Error(), "UTF-8") {
		t.Fatalf("ErrEncoding must tell the user what to do: %q", ErrEncoding)
	}
}
