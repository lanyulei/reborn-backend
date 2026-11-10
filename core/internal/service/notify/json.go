package service

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// jsonToStruct 把运行表 jsonb 原始字节转为 proto Struct 供控制台渲染。
// 运行表只保证列值是合法 JSON，不保证是对象：生产者发来的 payload 可以
// 是数组/标量/null（ingress 契约 §6.2 仅约束 ≤32KB）。Struct 只能承载对象，
// 非对象值包成 {"value": <原值>}；连 JSON 都解析不了的字节（理论上不会
// 出现）包成 {"raw": "<原文>"}。任何形态都不失败——否则一条异常行会让
// 包含它的整页列表 500，排障视图恰在最需要时不可用。
func jsonToStruct(raw []byte) *structpb.Struct {
	if len(raw) == 0 {
		return nil
	}
	var out structpb.Struct
	if err := protojson.Unmarshal(raw, &out); err == nil {
		return &out
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err == nil {
		if value, err := structpb.NewValue(generic); err == nil {
			return &structpb.Struct{Fields: map[string]*structpb.Value{"value": value}}
		}
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{"raw": structpb.NewStringValue(string(raw))}}
}
