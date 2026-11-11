package service

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxSchemaDepth 嵌套消息展开深度上限（防递归消息无限展开）。
const maxSchemaDepth = 6

// messageSchema 把 proto 消息描述符转换为 JSON Schema（object）。字段名用
// protojson 名（camelCase），与回环接口的 protojson 解码一致；64 位整数声明为
// integer（protojson 解码同时接受数字与字符串）。startup-only：运行期不反射。
func messageSchema(md protoreflect.MessageDescriptor, required []string) map[string]any {
	return buildObject(md, required, map[protoreflect.FullName]struct{}{}, 0)
}

func buildObject(md protoreflect.MessageDescriptor, required []string, visiting map[protoreflect.FullName]struct{}, depth int) map[string]any {
	props := make(map[string]any, md.Fields().Len())
	visiting[md.FullName()] = struct{}{}
	defer delete(visiting, md.FullName())
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		props[fd.JSONName()] = fieldSchema(fd, visiting, depth)
	}
	obj := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		obj["required"] = required
	}
	return obj
}

func fieldSchema(fd protoreflect.FieldDescriptor, visiting map[protoreflect.FullName]struct{}, depth int) map[string]any {
	if fd.IsMap() {
		return map[string]any{
			"type":                 "object",
			"additionalProperties": scalarOrMessageSchema(fd.MapValue(), visiting, depth),
		}
	}
	item := scalarOrMessageSchema(fd, visiting, depth)
	if fd.IsList() {
		return map[string]any{"type": "array", "items": item}
	}
	return item
}

func scalarOrMessageSchema(fd protoreflect.FieldDescriptor, visiting map[protoreflect.FullName]struct{}, depth int) map[string]any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return map[string]any{"type": "integer"}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{"type": "number"}
	case protoreflect.StringKind:
		return map[string]any{"type": "string"}
	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "contentEncoding": "base64"}
	case protoreflect.EnumKind:
		values := fd.Enum().Values()
		names := make([]string, 0, values.Len())
		for i := range values.Len() {
			names = append(names, string(values.Get(i).Name()))
		}
		return map[string]any{"type": "string", "enum": names}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return wellKnownOrObject(fd.Message(), visiting, depth)
	default:
		return map[string]any{}
	}
}

func wellKnownOrObject(md protoreflect.MessageDescriptor, visiting map[protoreflect.FullName]struct{}, depth int) map[string]any {
	switch md.FullName() {
	case "google.protobuf.Timestamp":
		return map[string]any{"type": "string", "format": "date-time", "description": "RFC3339 时间，如 2025-01-01T00:00:00Z"}
	case "google.protobuf.Duration":
		return map[string]any{"type": "string", "description": "时长，秒格式，如 300s"}
	case "google.protobuf.StringValue":
		return map[string]any{"type": "string"}
	case "google.protobuf.BoolValue":
		return map[string]any{"type": "boolean"}
	case "google.protobuf.Int32Value", "google.protobuf.Int64Value", "google.protobuf.UInt32Value", "google.protobuf.UInt64Value":
		return map[string]any{"type": "integer"}
	case "google.protobuf.FloatValue", "google.protobuf.DoubleValue":
		return map[string]any{"type": "number"}
	case "google.protobuf.Struct":
		return map[string]any{"type": "object"}
	case "google.protobuf.Value", "google.protobuf.Any":
		return map[string]any{}
	case "google.protobuf.ListValue":
		return map[string]any{"type": "array"}
	}
	if _, cyc := visiting[md.FullName()]; cyc || depth >= maxSchemaDepth {
		return map[string]any{"type": "object"}
	}
	return buildObject(md, nil, visiting, depth+1)
}
