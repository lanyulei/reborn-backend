package biz

import (
	"context"
	"fmt"

	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// jobHosts 把 cmdb 域的数据范围、资源类型与资源查询适配为作业中心的目标主机来源。
// 只读、单向（job → cmdb）：cmdb 域不感知作业中心，也没有为它新增任何接口。
type jobHosts struct {
	scopes *bizcmdb.ScopeUsecase
	models *bizcmdb.ModelUsecase
	cis    *bizcmdb.CIUsecase
}

// NewJobHostSource 构造作业中心的目标主机来源。
func NewJobHostSource(scopes *bizcmdb.ScopeUsecase, models *bizcmdb.ModelUsecase, cis *bizcmdb.CIUsecase) bizjob.HostSource {
	return jobHosts{scopes: scopes, models: models, cis: cis}
}

// Check implements bizjob.HostSource.
func (j jobHosts) Check(ctx context.Context, userID string, target jobcontract.Target) error {
	_, err := j.check(ctx, userID, target)
	return err
}

// check 校验目标配置并返回用户的数据范围。判定顺序：先数据范围、再类型与字段——范围外的类型
// 一律 403，不透露它是否存在。
func (j jobHosts) check(ctx context.Context, userID string, target jobcontract.Target) (bizcmdb.Scope, error) {
	scope, err := j.scopes.Resolve(ctx, userID)
	if err != nil {
		return bizcmdb.Scope{}, err
	}
	if !scope.AllowsModel(target.ModelCode) {
		return bizcmdb.Scope{}, bizjob.ErrTargetForbidden
	}
	if target.Mode == jobcontract.TargetModeFilter && target.Env != "" && !scope.AllowsEnv(target.Env) {
		return bizcmdb.Scope{}, bizjob.ErrTargetForbidden
	}
	_, fields, _, err := j.models.GetModel(ctx, target.ModelCode)
	if kerrors.Is(err, bizcmdb.ErrModelNotFound) {
		return bizcmdb.Scope{}, bizjob.TargetInvalid("resource model %q does not exist", target.ModelCode)
	}
	if err != nil {
		return bizcmdb.Scope{}, err
	}
	if target.AddressField == "" {
		return scope, nil
	}
	for _, f := range fields {
		if f.Code != target.AddressField {
			continue
		}
		switch {
		case f.Deprecated:
			return bizcmdb.Scope{}, bizjob.TargetInvalid("address field %q is deprecated", f.Code)
		case f.Sensitive:
			// 敏感属性读出来是掩码，不可能是可用的地址；也不应把敏感值当主机名写进日志。
			return bizcmdb.Scope{}, bizjob.TargetInvalid("address field %q is sensitive and cannot be used as the host address", f.Code)
		case f.Type != bizcmdb.FieldTypeString && f.Type != bizcmdb.FieldTypeIP:
			return bizcmdb.Scope{}, bizjob.TargetInvalid("address field %q must be a string or ip field", f.Code)
		}
		return scope, nil
	}
	return bizcmdb.Scope{}, bizjob.TargetInvalid("address field %q does not exist on model %q", target.AddressField, target.ModelCode)
}

// Resolve implements bizjob.HostSource.
func (j jobHosts) Resolve(ctx context.Context, userID string, target jobcontract.Target, limit int) (bizjob.ResourceSet, error) {
	scope, err := j.check(ctx, userID, target)
	if err != nil {
		return bizjob.ResourceSet{}, err
	}
	if target.Mode == jobcontract.TargetModeCIs {
		return j.resolveIDs(ctx, scope, target, limit)
	}
	return j.resolveFilter(ctx, scope, target, limit)
}

// resolveIDs 一次取齐指定的资源（CMDB 批量读取，不再逐个 GetByID）；已不存在、不属于该类型、已退役或不在
// 数据范围内的计入 Skipped。顺序保持用户选择的顺序，个数受 limit 约束（上限 jobcontract.MaxHosts）。
func (j jobHosts) resolveIDs(ctx context.Context, scope bizcmdb.Scope, target jobcontract.Target, limit int) (bizjob.ResourceSet, error) {
	set := bizjob.ResourceSet{Items: make([]bizjob.Resource, 0, min(len(target.CIIDs), limit))}
	found, err := j.cis.GetByIDs(ctx, target.CIIDs)
	if err != nil {
		return bizjob.ResourceSet{}, err
	}
	byID := make(map[string]*bizcmdb.CI, len(found))
	for _, ci := range found {
		byID[ci.ID] = ci
	}
	for _, id := range target.CIIDs {
		if len(set.Items) >= limit {
			set.Truncated = true
			break
		}
		ci, ok := byID[id]
		if !ok || ci.ModelCode != target.ModelCode || ci.Status == bizcmdb.CIStatusRetired || !scope.Allows(ci.ModelCode, ci.Env) {
			set.Skipped++
			continue
		}
		set.Items = append(set.Items, jobResource(ci, target.AddressField))
	}
	return set, nil
}

// resolveFilter 按条件遍历资源（键集分页，不统计总数），叠加数据范围的环境集合；已退役的资源跳过。
// 取满 limit 后若还有下一条即标记 Truncated。
func (j jobHosts) resolveFilter(ctx context.Context, scope bizcmdb.Scope, target jobcontract.Target, limit int) (bizjob.ResourceSet, error) {
	filter := bizcmdb.CIFilter{
		ModelCode: target.ModelCode, Env: target.Env, Tag: target.Tag, Keyword: target.Keyword, NoCount: true,
	}
	set := bizjob.ResourceSet{Items: make([]bizjob.Resource, 0, 64)}
	// 与 CMDB 资源列表同一口径（service/cmdb scopeGuard.narrowFilter）：环境受限且未指定环境时按可见环境集合过滤。
	if !scope.Unrestricted && !scope.EnvUnrestricted && filter.Env == "" {
		for env := range scope.Envs {
			filter.Envs = append(filter.Envs, env)
		}
		if len(filter.Envs) == 0 {
			return set, nil
		}
	}
	for {
		items, _, err := j.cis.List(ctx, filter, 1, paging.MaxPageSize)
		if err != nil {
			return bizjob.ResourceSet{}, err
		}
		for _, ci := range items {
			if ci.Status == bizcmdb.CIStatusRetired {
				continue
			}
			if len(set.Items) >= limit {
				set.Truncated = true
				return set, nil
			}
			set.Items = append(set.Items, jobResource(ci, target.AddressField))
		}
		if int64(len(items)) < paging.MaxPageSize {
			return set, nil
		}
		filter.Cursor = bizcmdb.EncodeCursor(items[len(items)-1])
	}
}

// jobResource 取出资源的连接地址：未指定地址字段时用资源名称；属性值非字符串时取其文本形式
// （是否合法由 job 域统一校验）。
func jobResource(ci *bizcmdb.CI, addressField string) bizjob.Resource {
	r := bizjob.Resource{ID: ci.ID, Name: ci.Name, Env: ci.Env, Address: ci.Name}
	if addressField == "" {
		return r
	}
	switch v := ci.Attrs[addressField].(type) {
	case nil:
		r.Address = ""
	case string:
		r.Address = v
	default:
		r.Address = fmt.Sprint(v)
	}
	return r
}

// ModelNames implements bizjob.HostSource：只返回用户数据范围内的类型名称。
func (j jobHosts) ModelNames(ctx context.Context, userID string, codes []string) (map[string]string, error) {
	out := make(map[string]string, len(codes))
	if len(codes) == 0 {
		return out, nil
	}
	scope, err := j.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	for _, code := range codes {
		if !scope.AllowsModel(code) {
			continue
		}
		m, _, _, err := j.models.GetModel(ctx, code)
		if kerrors.Is(err, bizcmdb.ErrModelNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[code] = m.Name
	}
	return out, nil
}
