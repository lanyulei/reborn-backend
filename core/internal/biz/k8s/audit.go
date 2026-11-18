package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/paging"

	"gopkg.in/yaml.v3"
)

// 审计与终端会话的查询窗口：运行表只增不删改且按月分区，无界查询会扫全部分区。
const (
	defaultAuditWindow = 7 * 24 * time.Hour
	maxAuditWindow     = 90 * 24 * time.Hour
)

// AuditUsecase 操作审计查询用例（只读）。
//
// 审计含对象内容、终端会话含操作对象，按数据范围过滤：非超管只看到自己有授权的集群与命名空间里的记录；
// 集群级对象与平台配置变更要求「全部命名空间」的授权；不属于任何集群的记录只有超管可见。
type AuditUsecase struct {
	reader AuditReader
	access *AccessResolver
	now    func() time.Time
}

// NewAuditUsecase new an AuditUsecase.
func NewAuditUsecase(reader AuditReader, access *AccessResolver) *AuditUsecase {
	return &AuditUsecase{reader: reader, access: access, now: time.Now}
}

// window 规整查询窗口：结束缺省为当前时间，起始缺省为结束前 7 天；起止颠倒或跨度超过 90 天拒绝。
func window(start, end *time.Time, now time.Time) (time.Time, time.Time, error) {
	e := now
	if end != nil {
		e = *end
	}
	s := e.Add(-defaultAuditWindow)
	if start != nil {
		s = *start
	}
	switch {
	case s.After(e):
		return s, e, detail(ErrTimeRangeInvalid, "start time must not be later than end time")
	case e.Sub(s) > maxAuditWindow:
		return s, e, detail(ErrTimeRangeInvalid, "time range must not exceed 90 days")
	}
	return s, e, nil
}

// List 分页查询审计记录。
func (uc *AuditUsecase) List(ctx context.Context, caller Caller, filter AuditFilter, start, end *time.Time, page, pageSize int64) ([]*Audit, int64, error) {
	var err error
	if filter.Access, err = uc.access.Resolve(ctx, caller.UserID); err != nil {
		return nil, 0, err
	}
	if filter.Start, filter.End, err = window(start, end, uc.now()); err != nil {
		return nil, 0, err
	}
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.reader.List(ctx, filter, page, pageSize)
}

// Get 审计详情（含对象内容）。
func (uc *AuditUsecase) Get(ctx context.Context, caller Caller, id string) (*Audit, error) {
	access, err := uc.access.Resolve(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	a, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !access.AllowsObject(a.ClusterID, a.Namespace) {
		return nil, ErrAuditNotFound
	}
	return a, nil
}

// ListExecSessions 分页查询终端会话记录。
func (uc *AuditUsecase) ListExecSessions(ctx context.Context, caller Caller, filter ExecSessionFilter, start, end *time.Time, page, pageSize int64) ([]*ExecSession, int64, error) {
	var err error
	if filter.Access, err = uc.access.Resolve(ctx, caller.UserID); err != nil {
		return nil, 0, err
	}
	if filter.Start, filter.End, err = window(start, end, uc.now()); err != nil {
		return nil, 0, err
	}
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.reader.ListExecSessions(ctx, filter, page, pageSize)
}

// ObjectYAML 把审计里的对象（JSON）渲染为 YAML，保持键的原有顺序；空或无法解析时返回空串。
func ObjectYAML(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ""
	}
	var node yaml.Node
	// JSON 是 YAML 的子集：直接按 YAML 解析成节点树，再去掉 flow 风格重新编码。
	if err := yaml.Unmarshal(raw, &node); err != nil {
		return ""
	}
	blockStyle(&node)
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&node); err != nil {
		return ""
	}
	_ = enc.Close()
	return buf.String()
}

// blockStyle 递归清除 flow 风格与引号风格，让输出成为常规的块状 YAML。
func blockStyle(n *yaml.Node) {
	n.Style = 0
	for _, c := range n.Content {
		blockStyle(c)
	}
}
