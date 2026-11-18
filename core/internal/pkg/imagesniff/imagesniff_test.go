package imagesniff

import "testing"

func TestDetect(t *testing.T) {
	t.Parallel()
	pad := string(make([]byte, 24))
	cases := map[string]struct{ data, want string }{
		"png":             {"\x89PNG\r\n\x1a\n" + pad, PNG},
		"jpeg":            {"\xff\xd8\xff\xe0" + pad, JPEG},
		"gif":             {"GIF89a" + pad, GIF},
		"webp":            {"RIFF\x24\x00\x00\x00WEBPVP8 " + pad, WebP},
		"bmp":             {"BM" + pad, BMP},
		"ico":             {"\x00\x00\x01\x00" + pad, ICO},
		"svg":             {`<svg xmlns="http://www.w3.org/2000/svg"/>`, SVG},
		"svg with prolog": {"\xef\xbb\xbf<?xml version=\"1.0\"?>\n<!-- c -->\n<SVG></SVG>", SVG},
		"truncated png":   {"\x89PNG-fake", ""},
		"html":            {"<html><body><svg/></body></html>", ""},
		"script":          {"<script>alert(1)</script>", ""},
		"plain text":      {"hello", ""},
		"xml without svg": {`<?xml version="1.0"?><note/>`, ""},
		"pdf":             {"%PDF-1.7\n", ""},
		"exe":             {"MZ\x90\x00\x03\x00\x00\x00\xff\xfe\x01", ""},
		"empty":           {"", ""},
	}
	for name, c := range cases {
		if got := Detect([]byte(c.data)); got != c.want {
			t.Errorf("%s: Detect() = %q, want %q", name, got, c.want)
		}
	}
}
