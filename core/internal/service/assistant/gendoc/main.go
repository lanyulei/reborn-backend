// Command gendoc 从 proto 描述集（含源码注释）提取每个消息字段的注释，生成
// 助手工具入参说明 fielddoc.json（键为字段全名 pkg.Message.field）。
//
// 生成的 Go 代码不保留 proto 注释，而模型需要靠它们理解字段含义（枚举取值、
// 时间约束等），所以随 make gen 生成一份文本并嵌入二进制：
//
//	buf build --exclude-imports -o - | go run ./internal/service/assistant/gendoc > internal/service/assistant/fielddoc.json
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// descriptor.proto 里的字段号：FileDescriptorProto.message_type / DescriptorProto.field / nested_type。
const (
	tagFileMessage   = 4
	tagMessageField  = 2
	tagMessageNested = 3
)

func main() {
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail(err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		fail(fmt.Errorf("decode descriptor set: %w", err))
	}
	docs := map[string]string{}
	for _, file := range set.GetFile() {
		comments := map[string]string{}
		for _, loc := range file.GetSourceCodeInfo().GetLocation() {
			if text := clean(loc.GetLeadingComments()); text != "" {
				comments[fmt.Sprint(loc.GetPath())] = text
			}
		}
		for i, msg := range file.GetMessageType() {
			walk(msg, file.GetPackage(), []int32{tagFileMessage, int32(i)}, comments, docs)
		}
	}
	// encoding/json 按键排序输出：结果稳定，便于评审差异。
	out, err := json.MarshalIndent(docs, "", "  ")
	if err != nil {
		fail(err)
	}
	if _, err := os.Stdout.Write(append(out, '\n')); err != nil {
		fail(err)
	}
}

func walk(msg *descriptorpb.DescriptorProto, scope string, path []int32, comments, docs map[string]string) {
	name := scope + "." + msg.GetName()
	for i, field := range msg.GetField() {
		if text := comments[fmt.Sprint(extend(path, tagMessageField, i))]; text != "" {
			docs[name+"."+field.GetName()] = text
		}
	}
	for i, nested := range msg.GetNestedType() {
		walk(nested, name, extend(path, tagMessageNested, i), comments, docs)
	}
}

func extend(path []int32, tag int32, index int) []int32 {
	out := make([]int32, 0, len(path)+2)
	out = append(out, path...)
	return append(out, tag, int32(index)) //nolint:gosec // 字段数量远小于 int32 上限
}

// clean 把多行注释压成一行。
func clean(comment string) string {
	return strings.Join(strings.Fields(comment), " ")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gendoc:", err)
	os.Exit(1)
}
