package kube

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// 下载目录时，靠多级 .. 逃出目标目录的符号链接不下发（原先按归档根计算，这种链接被放过）；
// 指向目标目录之内的照常下发。
func TestRelayTarDropsLinksEscapingBase(t *testing.T) {
	t.Parallel()
	src := tarOf(t,
		tar.Header{Typeflag: tar.TypeDir, Name: "log/"},
		tar.Header{Typeflag: tar.TypeReg, Name: "log/a.txt"},
		tar.Header{Typeflag: tar.TypeDir, Name: "log/sub/"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/sub/escape", Linkname: "../../x"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/sub/escape2", Linkname: "a/../../../x"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/up", Linkname: "../x"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/sub/up", Linkname: "../a.txt"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/in", Linkname: "sub/../a.txt"},
	)
	var out bytes.Buffer
	begin := func(string, bool, int64) io.Writer { return &out }
	if _, err := relayTar(bytes.NewReader(src), "log", 1<<20, begin); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&out)
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	if got := strings.Join(names, " "); got != "log/ log/a.txt log/sub/ log/sub/up log/in" {
		t.Fatalf("relayed entries = %v (links escaping the base must be dropped)", names)
	}
}
