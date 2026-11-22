package biz

import (
	"context"

	bizknowledge "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// knowledgeRoles 把 system 域的角色列表适配为知识库域的角色目录（ACL 校验与展示）。
type knowledgeRoles struct {
	roles *bizsystem.RoleUsecase
}

// NewKnowledgeRoles 构造知识库域的角色目录适配器。
func NewKnowledgeRoles(roles *bizsystem.RoleUsecase) bizknowledge.RoleDirectory {
	return knowledgeRoles{roles: roles}
}

// ListRoles implements bizknowledge.RoleDirectory（角色为小集合，按页上限循环取全）。
func (k knowledgeRoles) ListRoles(ctx context.Context) ([]bizknowledge.RoleRef, error) {
	var out []bizknowledge.RoleRef
	for page := int64(1); ; page++ {
		roles, total, err := k.roles.List(ctx, bizsystem.RoleFilter{}, page, paging.MaxPageSize)
		if err != nil {
			return nil, err
		}
		for _, r := range roles {
			out = append(out, bizknowledge.RoleRef{ID: r.ID, Name: r.Name, Enabled: r.Status == bizsystem.RoleStatusEnabled})
		}
		if int64(len(out)) >= total || len(roles) == 0 {
			return out, nil
		}
	}
}

// NewKnowledgeSummarizer 知识库的 AI 摘要复用监控域的文本摘要适配器（同一模型客户端、同一并发名额）：
// 两个接口的方法集相同，直接转型。
func NewKnowledgeSummarizer(s bizmonitor.Summarizer) bizknowledge.Summarizer { return s }
