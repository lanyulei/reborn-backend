package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 预案（功能设计 §5.2）：处置手册 + 可执行步骤（作业模板 / 流程定义 + 参数映射 + 审批策略）+ 适用条件 + 代理执行人。

const maxPlaybookSteps = 20

// PlaybookUsecase 预案。
type PlaybookUsecase struct {
	d *Deps
}

// NewPlaybookUsecase new a PlaybookUsecase.
func NewPlaybookUsecase(d *Deps) *PlaybookUsecase { return &PlaybookUsecase{d: d} }

// PlaybookView 预案及其展示信息。
type PlaybookView struct {
	*Playbook
	StepNames      []string
	AgentUserName  string
	BusinessNames  map[string]string
	UpdateUserName string
	ExecCount      int32
}

// PlaybookTarget 步骤可引用的对象。
type PlaybookTarget struct {
	Kind        string
	Ref         string
	Name        string
	Description string
	ParamNames  []string
}

func (uc *PlaybookUsecase) validate(ctx context.Context, s *PlaybookSpec) ([]string, error) {
	s.Name, s.Description, s.Doc = cleanText(s.Name), cleanText(s.Description), strings.ReplaceAll(s.Doc, "\x00", "")
	if s.Name == "" || len([]rune(s.Name)) > 128 {
		return nil, detail(ErrPlaybookInvalid, "name must be 1-128 characters")
	}
	if len(s.Doc) > 65536 {
		return nil, detail(ErrPlaybookInvalid, "doc must be within 64 KiB")
	}
	if len(s.Steps) > maxPlaybookSteps {
		return nil, detail(ErrPlaybookInvalid, "at most %d steps", maxPlaybookSteps)
	}
	if _, err := uc.d.Matchers.Compile(s.Match.Matchers); err != nil {
		return nil, detail(ErrPlaybookInvalid, "match.matchers: %v", err)
	}
	s.Match.ModelCodes, s.Match.BusinessIDs = uniq(s.Match.ModelCodes), uniq(s.Match.BusinessIDs)
	if len(s.Steps) > 0 && s.AgentUserID == "" {
		return nil, detail(ErrPlaybookInvalid, "agent user is required when the playbook has executable steps")
	}
	if s.AgentUserID != "" {
		active, err := uc.d.Dir.ActiveUserIDs(ctx, []string{s.AgentUserID})
		if err != nil {
			return nil, err
		}
		if len(active) != 1 {
			return nil, detail(ErrPlaybookInvalid, "agent user must be an active user")
		}
	}
	names := make([]string, 0, len(s.Steps))
	for i := range s.Steps {
		st := &s.Steps[i]
		st.Name = cleanText(st.Name)
		if st.Name == "" {
			return nil, detail(ErrPlaybookInvalid, "step %d: name is required", i+1)
		}
		if st.Approval < ApprovalDirect || st.Approval > ApprovalExisting {
			return nil, detail(ErrPlaybookInvalid, "step %d: approval must be 1-3", i+1)
		}
		if st.Risk < RiskLow || st.Risk > RiskHigh {
			return nil, detail(ErrPlaybookInvalid, "step %d: risk must be 1-3", i+1)
		}
		if st.TargetMode == 0 {
			st.TargetMode = TargetAffected
		}
		if len(st.Params) > 50 {
			return nil, detail(ErrPlaybookInvalid, "step %d: too many params", i+1)
		}
		name, err := uc.targetName(ctx, s.AgentUserID, st.Kind, st.Ref)
		if err != nil {
			return nil, err
		}
		if name == "" {
			return nil, detail(ErrPlaybookInvalid, "step %d: %s %q is not available to the agent user", i+1, st.Kind, st.Ref)
		}
		names = append(names, name)
	}
	return names, nil
}

// targetName 作业模板 / 流程定义当前是否可用（以代理执行人身份看），可用返回名称。
func (uc *PlaybookUsecase) targetName(ctx context.Context, agentUser, kind, ref string) (string, error) {
	switch kind {
	case "job":
		t, err := uc.d.Jobs.Template(ctx, agentUser, ref)
		if err != nil {
			if isNotFound(err) || kratosCodeIs(err, 403) {
				return "", nil
			}
			return "", err
		}
		if t == nil || !t.Enabled {
			return "", nil
		}
		return t.Name, nil
	case "workflow":
		name, ok, err := uc.d.Flows.Startable(ctx, ref)
		if err != nil || !ok {
			return "", err
		}
		return name, nil
	}
	return "", detail(ErrPlaybookInvalid, "step kind must be job or workflow")
}

func (uc *PlaybookUsecase) views(ctx context.Context, items []*Playbook) ([]*PlaybookView, error) {
	var users, businesses []string
	for _, p := range items {
		users = append(users, p.Spec.AgentUserID, p.UpdateUser)
		businesses = append(businesses, p.Spec.Match.BusinessIDs...)
	}
	userNames, err := uc.d.Dir.UserNames(ctx, uniq(users))
	if err != nil {
		return nil, err
	}
	businessNames := map[string]string{}
	for _, id := range uniq(businesses) {
		if b, err := uc.d.Businesses.Get(ctx, id); err == nil {
			businessNames[id] = b.Name
		}
	}
	since := time.Now().Add(-30 * 24 * time.Hour)
	out := make([]*PlaybookView, 0, len(items))
	for _, p := range items {
		n, err := uc.d.Proposals.CountPlaybookExecs(ctx, p.ID, since)
		if err != nil {
			return nil, err
		}
		v := &PlaybookView{Playbook: p, AgentUserName: userNames[p.Spec.AgentUserID], UpdateUserName: userNames[p.UpdateUser], BusinessNames: businessNames, ExecCount: i32(n)}
		for _, st := range p.Spec.Steps {
			name, err := uc.targetName(ctx, p.Spec.AgentUserID, st.Kind, st.Ref)
			if err != nil {
				return nil, err
			}
			v.StepNames = append(v.StepNames, name)
		}
		out = append(out, v)
	}
	return out, nil
}

// List 分页。
func (uc *PlaybookUsecase) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*PlaybookView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Playbooks.List(ctx, cleanText(keyword), enabled, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, items)
	return views, total, err
}

// Get 详情。
func (uc *PlaybookUsecase) Get(ctx context.Context, id string) (*PlaybookView, error) {
	p, err := uc.d.Playbooks.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, []*Playbook{p})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// Create 新建。
func (uc *PlaybookUsecase) Create(ctx context.Context, userID string, spec PlaybookSpec) (*PlaybookView, error) {
	if _, err := uc.validate(ctx, &spec); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	if err := uc.d.Playbooks.Create(ctx, &Playbook{ID: id, Spec: spec}, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑。
func (uc *PlaybookUsecase) Update(ctx context.Context, userID, id string, spec PlaybookSpec, version int32) (*PlaybookView, error) {
	if _, err := uc.validate(ctx, &spec); err != nil {
		return nil, err
	}
	if err := uc.d.Playbooks.Update(ctx, &Playbook{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Delete 删除；仍被规则引用时拒绝。
func (uc *PlaybookUsecase) Delete(ctx context.Context, userID, id string) error {
	p, err := uc.d.Playbooks.Get(ctx, id)
	if err != nil {
		return err
	}
	n, err := uc.d.Rules.CountPlaybookRefs(ctx, id)
	if err != nil {
		return err
	}
	if n > 0 {
		return detail(ErrPlaybookInUse, "playbook is referenced by %d rule(s)", n)
	}
	return uc.d.Playbooks.Delete(ctx, id, p.Version, userID)
}

// Targets 步骤可引用的对象（以调用者身份可见的作业模板 + 可发起的流程定义）。
func (uc *PlaybookUsecase) Targets(ctx context.Context, userID string) ([]*PlaybookTarget, error) {
	var out []*PlaybookTarget
	templates, err := uc.d.Jobs.Templates(ctx, userID)
	if err != nil {
		return nil, err
	}
	for _, t := range templates {
		if t.Enabled {
			out = append(out, &PlaybookTarget{Kind: "job", Ref: t.ID, Name: t.Name, Description: t.Description, ParamNames: t.ParamNames})
		}
	}
	defs, err := uc.d.Flows.Definitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, d := range defs {
		out = append(out, &PlaybookTarget{Kind: "workflow", Ref: d.ID, Name: d.Name})
	}
	return out, nil
}

// recommended 适用于某故障的启用预案（按适用条件：业务 / 资源模型 / 任一关联告警的标签）。
func (uc *IncidentUsecase) recommended(ctx context.Context, inc *Incident, alerts []*AlertRef, models map[string]string) ([]*Playbook, error) {
	all, err := uc.d.Playbooks.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	var out []*Playbook
	for _, p := range all {
		m := p.Spec.Match
		if len(m.BusinessIDs) > 0 && !slices.Contains(m.BusinessIDs, inc.BusinessID) {
			continue
		}
		if len(m.ModelCodes) > 0 {
			hit := false
			for _, ci := range inc.AffectedCIIDs {
				if slices.Contains(m.ModelCodes, models[ci]) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		if len(m.Matchers) > 0 {
			set, err := uc.d.Matchers.Compile(m.Matchers)
			if err != nil {
				continue
			}
			hit := false
			for _, a := range alerts {
				if set.Match(a.Labels) {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		out = append(out, p)
		if len(out) >= 10 {
			break
		}
	}
	return out, nil
}
