package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
)

// 编译期断言：视图类型实现 cmdb 域洞察相关仓储接口。
var (
	_ biz.GroupRepo       = cmdbGroupView{}
	_ biz.FavoriteRepo    = cmdbFavoriteView{}
	_ biz.SchemaAuditRepo = cmdbSchemaAuditView{}
	_ biz.OverviewRepo    = cmdbOverviewView{}
)

// builtinGroups 与迁移 0040 预置的内置分组一致。
func builtinGroups() map[string]*biz.ModelGroup {
	now := time.Now()
	out := make(map[string]*biz.ModelGroup, 6)
	for i, g := range []struct{ code, name string }{
		{"compute", "计算"}, {"network", "网络"}, {"storage", "存储"},
		{"cloud", "云资源"}, {"component", "配件"},
	} {
		out[g.code] = &biz.ModelGroup{Code: g.code, Name: g.name, Sort: int32((i + 1) * 10), Builtin: true, CreateTime: now, UpdateTime: now}
	}
	out["other"] = &biz.ModelGroup{Code: "other", Name: "其他", Sort: 900, Builtin: true, CreateTime: now, UpdateTime: now}
	return out
}

// ---- GroupRepo ----

type cmdbGroupView struct{ s *CmdbStore }

// Groups 返回 biz.GroupRepo 视图。
func (s *CmdbStore) Groups() biz.GroupRepo { return cmdbGroupView{s} }

func (s *CmdbStore) groupModelCount(code string) int64 {
	var n int64
	for _, e := range s.models {
		if !e.deleted && e.model.Category == code {
			n++
		}
	}
	return n
}

func (v cmdbGroupView) List(_ context.Context) ([]*biz.ModelGroup, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	items := make([]*biz.ModelGroup, 0, len(v.s.groups))
	for code, g := range v.s.groups {
		cp := *g
		cp.ModelCount = v.s.groupModelCount(code)
		items = append(items, &cp)
	}
	slices.SortFunc(items, func(a, b *biz.ModelGroup) int {
		if a.Sort != b.Sort {
			return int(a.Sort - b.Sort)
		}
		return strings.Compare(a.Code, b.Code)
	})
	return items, nil
}

func (v cmdbGroupView) Get(_ context.Context, code string) (*biz.ModelGroup, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	g, ok := v.s.groups[code]
	if !ok {
		return nil, biz.ErrGroupNotFound
	}
	cp := *g
	cp.ModelCount = v.s.groupModelCount(code)
	return &cp, nil
}

func (v cmdbGroupView) Create(_ context.Context, g *biz.ModelGroup) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.groups[g.Code]; ok {
		return biz.ErrGroupCodeExists
	}
	cp := *g
	v.s.groups[g.Code] = &cp
	return nil
}

func (v cmdbGroupView) Update(_ context.Context, code string, upd biz.ModelGroupUpdate) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	g, ok := v.s.groups[code]
	if !ok {
		return biz.ErrGroupNotFound
	}
	if upd.Name != nil {
		g.Name = *upd.Name
	}
	if upd.Sort != nil {
		g.Sort = *upd.Sort
	}
	g.UpdateTime = time.Now()
	return nil
}

func (v cmdbGroupView) Delete(_ context.Context, code string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	g, ok := v.s.groups[code]
	if !ok || g.Builtin || v.s.groupModelCount(code) > 0 {
		return biz.ErrGroupInUse
	}
	delete(v.s.groups, code)
	return nil
}

// ---- FavoriteRepo ----

type cmdbFavoriteView struct{ s *CmdbStore }

// Favorites 返回 biz.FavoriteRepo 视图。
func (s *CmdbStore) Favorites() biz.FavoriteRepo { return cmdbFavoriteView{s} }

func (v cmdbFavoriteView) List(_ context.Context, userID string) ([]string, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	out := make([]string, 0, len(v.s.favorites[userID]))
	for _, code := range v.s.favorites[userID] {
		if e, ok := v.s.models[code]; ok && !e.deleted {
			out = append(out, code)
		}
	}
	return out, nil
}

func (v cmdbFavoriteView) Set(_ context.Context, userID, modelCode string, favorite bool) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cur := v.s.favorites[userID]
	idx := slices.Index(cur, modelCode)
	switch {
	case favorite && idx < 0:
		v.s.favorites[userID] = append(cur, modelCode)
	case !favorite && idx >= 0:
		v.s.favorites[userID] = slices.Delete(cur, idx, idx+1)
	}
	return nil
}

// ---- SchemaAuditRepo ----

type cmdbSchemaAuditView struct{ s *CmdbStore }

// SchemaAudits 返回 biz.SchemaAuditRepo 视图。
func (s *CmdbStore) SchemaAudits() biz.SchemaAuditRepo { return cmdbSchemaAuditView{s} }

func (v cmdbSchemaAuditView) Insert(_ context.Context, a *biz.SchemaAudit) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cp := *a
	v.s.schemaAudits = append(v.s.schemaAudits, &cp)
	return nil
}

func (v cmdbSchemaAuditView) List(_ context.Context, filter biz.SchemaAuditFilter, page, pageSize int64) ([]*biz.SchemaAudit, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.SchemaAudit, 0, len(v.s.schemaAudits))
	for _, a := range v.s.schemaAudits {
		if filter.TargetType != "" && a.TargetType != filter.TargetType {
			continue
		}
		if filter.ModelCode != "" && a.ModelCode != filter.ModelCode {
			continue
		}
		if filter.Action != "" && a.Action != filter.Action {
			continue
		}
		if filter.Operator != "" && a.Operator != filter.Operator {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(a.TargetCode, filter.Keyword) && !strings.Contains(a.TargetName, filter.Keyword) {
			continue
		}
		if filter.From != nil && a.CreateTime.Before(*filter.From) {
			continue
		}
		if filter.To != nil && a.CreateTime.After(*filter.To) {
			continue
		}
		cp := *a
		items = append(items, &cp)
	}
	v.s.mu.RUnlock()
	slices.Reverse(items)
	return pageSlice(items, page, pageSize)
}

// ---- OverviewRepo ----

type cmdbOverviewView struct{ s *CmdbStore }

// Overviews 返回 biz.OverviewRepo 视图。
func (s *CmdbStore) Overviews() biz.OverviewRepo { return cmdbOverviewView{s} }

func inScope(set []string, value string) bool { return set == nil || slices.Contains(set, value) }

// issueMatch 资源是否命中某类数据质量问题（口径同 data 层）。
func (s *CmdbStore) issueMatch(ci *biz.CI, issue string, outdatedBefore time.Time) bool {
	switch issue {
	case biz.IssueNoOwner:
		return ci.Owner == ""
	case biz.IssueStale:
		return ci.Stale
	case biz.IssueOutdated:
		return ci.UpdateTime.Before(outdatedBefore)
	case biz.IssueOrphan:
		for _, r := range s.relations {
			if r.SrcID == ci.ID || r.DstID == ci.ID {
				return false
			}
		}
		return true
	case biz.IssueMissingRequired:
		for _, f := range s.fields {
			if f.ModelCode == ci.ModelCode && f.Required && !f.Deprecated {
				if v, ok := ci.Attrs[f.Code]; !ok || v == nil || v == "" {
					return true
				}
			}
		}
	}
	return false
}

func (v cmdbOverviewView) Quality(_ context.Context, filter biz.OverviewFilter, outdatedBefore time.Time) ([]*biz.QualityRow, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	byModel := map[string]*biz.QualityRow{}
	for _, e := range v.s.cis {
		ci := e.ci
		if e.deleted || ci.Status == biz.CIStatusRetired || !inScope(filter.ModelCodes, ci.ModelCode) || !inScope(filter.Envs, ci.Env) {
			continue
		}
		q := byModel[ci.ModelCode]
		if q == nil {
			q = &biz.QualityRow{ModelCode: ci.ModelCode, ModelName: ci.ModelName}
			byModel[ci.ModelCode] = q
		}
		q.Total++
		for issue, n := range map[string]*int64{biz.IssueMissingRequired: &q.MissingRequired, biz.IssueNoOwner: &q.NoOwner, biz.IssueOrphan: &q.Orphan, biz.IssueStale: &q.Stale, biz.IssueOutdated: &q.Outdated} {
			if v.s.issueMatch(&ci, issue, outdatedBefore) {
				*n++
			}
		}
	}
	out := make([]*biz.QualityRow, 0, len(byModel))
	for _, q := range byModel {
		out = append(out, q)
	}
	return out, nil
}

func (v cmdbOverviewView) Stats(_ context.Context, filter biz.OverviewFilter) (*biz.Overview, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	out := &biz.Overview{}
	for code, e := range v.s.models {
		if !e.deleted && inScope(filter.ModelCodes, code) {
			out.ModelCount++
		}
	}
	visible := make(map[string]bool, len(v.s.cis))
	byModel, byStatus, byEnv, bySource := map[string]int64{}, map[string]int64{}, map[string]int64{}, map[string]int64{}
	for id, e := range v.s.cis {
		ci := e.ci
		if !inScope(filter.ModelCodes, ci.ModelCode) || !inScope(filter.Envs, ci.Env) {
			continue
		}
		visible[id] = true // 历史趋势含已删除资源
		if e.deleted {
			continue
		}
		out.CICount++
		byModel[ci.ModelCode]++
		byStatus[ci.Status]++
		byEnv[ci.Env]++
		source := ci.Source
		if strings.HasPrefix(source, "sync:") {
			source = "sync"
		}
		bySource[source]++
	}
	for _, rel := range v.s.relations {
		if src, ok := v.s.cis[rel.SrcID]; ok && !src.deleted && visible[rel.SrcID] {
			out.RelationCount++
		}
	}
	toCounts := func(m map[string]int64, name func(string) string) []biz.OverviewCount {
		items := make([]biz.OverviewCount, 0, len(m))
		for k, n := range m {
			c := biz.OverviewCount{Key: k, Count: n}
			if name != nil {
				c.Name = name(k)
			}
			items = append(items, c)
		}
		slices.SortFunc(items, func(a, b biz.OverviewCount) int {
			if a.Count != b.Count {
				return int(b.Count - a.Count)
			}
			return strings.Compare(a.Key, b.Key)
		})
		return items
	}
	out.ByModel = toCounts(byModel, func(code string) string { return v.s.models[code].model.Name })
	out.ByStatus, out.ByEnv, out.BySource = toCounts(byStatus, nil), toCounts(byEnv, nil), toCounts(bySource, nil)
	trend := map[[2]string]int64{}
	for _, h := range v.s.histories {
		if h == nil || !visible[h.CIID] || h.CreateTime.Before(filter.Since) {
			continue
		}
		trend[[2]string{h.CreateTime.Format("2006-01-02"), h.Action}]++
		out.ChangeCount++
	}
	for key, n := range trend {
		out.Trend = append(out.Trend, biz.OverviewTrendPoint{Date: key[0], Action: key[1], Count: n})
	}
	slices.SortFunc(out.Trend, func(a, b biz.OverviewTrendPoint) int {
		if a.Date != b.Date {
			return strings.Compare(a.Date, b.Date)
		}
		return strings.Compare(a.Action, b.Action)
	})
	return out, nil
}
