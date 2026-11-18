package biz

import (
	"context"
	"slices"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// maxOptionEnvs 候选环境取值的个数上限。
const maxOptionEnvs = 200

// CIEnvLister 资源索引里出现过的环境取值（monitor_ci_index，本域自己的表）。
type CIEnvLister interface {
	// ListEnvs 有效资源上不同的非空环境取值，按字典序，至多 limit 个。
	ListEnvs(ctx context.Context, limit int) ([]string, error)
}

// RuleOptions 规则表单里第二、三期配置项的候选项（最小披露）。
type RuleOptions struct {
	TicketTemplates []TicketTemplate
	Workflows       []WorkflowDef
	CIModels        []CIModel
	CIEnvs          []string
	CIMatchAttrs    []string
	AIAvailable     bool
}

// RuleOptionUsecase 规则配置候选项。工单模板、流程、CMDB 模型分属各自的域，那边的列表接口挂在各自
// 页面的权限下；监控域只需要「可选项」这一最小视图，所以自带一个只读接口，随规则的新建 / 编辑权限开放
// （与 TargetUsecase 同一理由）。
type RuleOptionUsecase struct {
	tickets    TicketCatalog
	workflows  Workflows
	cmdb       CMDBCatalog
	envs       CIEnvLister
	summarizer Summarizer
	attrs      []string
}

// NewRuleOptionUsecase new a RuleOptionUsecase.
func NewRuleOptionUsecase(tickets TicketCatalog, workflows Workflows, cmdb CMDBCatalog, envs CIEnvLister, summarizer Summarizer,
	c *conf.Monitor,
) *RuleOptionUsecase {
	return &RuleOptionUsecase{
		tickets: tickets, workflows: workflows, cmdb: cmdb, envs: envs, summarizer: summarizer, attrs: ruleMatchAttrs(c.GetCiMatchAttrs()),
	}
}

// Options 取全部候选项。
func (uc *RuleOptionUsecase) Options(ctx context.Context) (*RuleOptions, error) {
	out := &RuleOptions{CIMatchAttrs: slices.Clone(uc.attrs), AIAvailable: uc.summarizer.Available()}
	var err error
	if out.TicketTemplates, err = uc.tickets.Templates(ctx); err != nil {
		return nil, err
	}
	if out.Workflows, err = uc.workflows.Definitions(ctx); err != nil {
		return nil, err
	}
	if out.CIModels, err = uc.cmdb.Models(ctx); err != nil {
		return nil, err
	}
	if out.CIEnvs, err = uc.envs.ListEnvs(ctx, maxOptionEnvs); err != nil {
		return nil, err
	}
	return out, nil
}
