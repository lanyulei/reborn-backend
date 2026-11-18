package biz

import (
	"context"
	"errors"
	"slices"
)

// names 展示名解析：把一批实体引用到的用户 / 部门 / 值班表 / 数据源 / 规则 ID 收齐后各查一次
// （列表页不做逐行查询）。各域的名称来源都是窄接口，查不到的 ID 在结果里标记 Missing。
type names struct {
	dir         Directory
	schedules   Schedules
	datasources DatasourceLookup
	rules       RuleLookup
}

// refSet 待解析的 ID 集合与解析结果。
type refSet struct {
	users, depts, schedules, datasources, rules    []string
	userNames, deptNames, scheduleNames, ruleNames map[string]string
	datasourceBriefs                               map[string]DatasourceBrief
}

func uniq(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// resolve 批量查询 set 里收集到的 ID。
func (n names) resolve(ctx context.Context, set *refSet) error {
	var err error
	if set.users = uniq(set.users); len(set.users) > 0 {
		if set.userNames, err = n.dir.UserNames(ctx, set.users); err != nil {
			return err
		}
	}
	if set.depts = uniq(set.depts); len(set.depts) > 0 {
		if set.deptNames, err = n.dir.DeptNames(ctx, set.depts); err != nil {
			return err
		}
	}
	if set.schedules = uniq(set.schedules); len(set.schedules) > 0 {
		if set.scheduleNames, err = n.schedules.ScheduleNames(ctx, set.schedules); err != nil {
			return err
		}
	}
	if set.datasources = uniq(set.datasources); len(set.datasources) > 0 {
		if set.datasourceBriefs, err = n.datasources.Briefs(ctx, set.datasources); err != nil {
			return err
		}
	}
	if set.rules = uniq(set.rules); len(set.rules) > 0 {
		if set.ruleNames, err = n.rules.Names(ctx, set.rules); err != nil {
			return err
		}
	}
	return nil
}

// errInactiveUser 给出的用户里有不存在或非可用（停用、离职）的。
var errInactiveUser = errors.New("monitor: user does not exist or is not active")

// requireActiveUsers 通知对象 / 接收方里的用户须是可用用户。kept 是保存前就已经在里面的用户，不再
// 要求可用：人离职后不该让引用他的规则、订阅从此无法保存（派发时本来就只通知可用用户）。
// 不能用 Directory.UserNames 判断——它为了展示历史记录，停用的用户照样返回名字。
func requireActiveUsers(ctx context.Context, dir Directory, ids, kept []string) error {
	added := slices.DeleteFunc(slices.Clone(ids), func(id string) bool { return slices.Contains(kept, id) })
	if len(added) == 0 {
		return nil
	}
	active, err := dir.ActiveUserIDs(ctx, added)
	if err != nil {
		return err
	}
	if len(active) != len(uniq(added)) {
		return errInactiveUser
	}
	return nil
}

// refs 按 ids 的顺序生成 NamedRef 列表；查不到的标记 Missing。
func refs(ids []string, lookup map[string]string) []NamedRef {
	out := make([]NamedRef, 0, len(ids))
	for _, id := range ids {
		name, ok := lookup[id]
		out = append(out, NamedRef{ID: id, Name: name, Missing: !ok})
	}
	return out
}

func (s *refSet) datasourceRefs(ids []string) []NamedRef {
	out := make([]NamedRef, 0, len(ids))
	for _, id := range ids {
		b, ok := s.datasourceBriefs[id]
		out = append(out, NamedRef{ID: id, Name: b.Name, Missing: !ok})
	}
	return out
}
