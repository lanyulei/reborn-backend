// Package imagesniff 按内容判定图片类型：不信任客户端声明的类型与扩展名。
// 中性包：纯计算、零 IO，biz 层可直接 import（系统 logo、用户头像共用）。
package imagesniff

import (
	"bytes"
	"mime"
	"net/http"
)

// 可判定的图片类型。
const (
	PNG  = "image/png"
	JPEG = "image/jpeg"
	GIF  = "image/gif"
	WebP = "image/webp"
	BMP  = "image/bmp"
	ICO  = "image/x-icon"
	SVG  = "image/svg+xml"
)

// svgProbe 判定 SVG 时查看的前缀长度（根元素之前可以有 XML 声明、注释与 DOCTYPE）。
const svgProbe = 4096

// Detect 返回 data 的图片类型（上面的常量之一）；不是可判定的图片返回空串。
// 位图按文件头判定；SVG 是文本，按「文本内容且前部出现 <svg 根元素」判定。
func Detect(data []byte) string {
	detected, _, err := mime.ParseMediaType(http.DetectContentType(data))
	if err != nil {
		return ""
	}
	switch detected {
	case PNG, JPEG, GIF, WebP, BMP, ICO:
		return detected
	case "text/xml", "text/plain":
		if bytes.Contains(bytes.ToLower(data[:min(len(data), svgProbe)]), []byte("<svg")) {
			return SVG
		}
	}
	return ""
}
