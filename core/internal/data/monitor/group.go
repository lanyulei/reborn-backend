package data

import (
	"context"
	"errors"
	"fmt"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.GroupReader   = (*groupRepo)(nil)
	_ biz.GroupWriter   = (*groupRepo)(nil)
	_ biz.RoleScopeRepo = (*roleScopeRepo)(nil)
)

type groupRepo struct {
	data *basedata.Data
}

// NewGroupRepo new a groupRepo.
func NewGroupRepo(data *basedata.Data) *groupRepo { return &groupRepo{data: data} }

// ListAll implements biz.GroupReader。按物化路径长度排序保证上级在前；分组总数有上限
// （biz.MaxGroups），LIMIT 只是兜底。只读分组表本身：每次范围解析、每轮通知派发都要读这棵树，
// 计数另由 RuleCounts 提供（只有分组列表页需要）。
func (r *groupRepo) ListAll(ctx context.Context) ([]*biz.RuleGroup, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT g.id::text, COALESCE(g.parent_id::text, ''), g.path, g.name, g.description, g.sort, g.create_time, g.update_time
		 FROM monitor_rule_group g WHERE g.delete_time IS NULL
		 ORDER BY length(g.path), g.sort, g.name, g.id LIMIT $1`, biz.MaxGroups*2)
	if err != nil {
		return nil, fmt.Errorf("list monitor rule groups: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RuleGroup, 0, 32)
	for rows.Next() {
		var g biz.RuleGroup
		if err := rows.Scan(&g.ID, &g.ParentID, &g.Path, &g.Name, &g.Description, &g.Sort, &g.CreateTime, &g.UpdateTime); err != nil {
			return nil, fmt.Errorf("scan monitor rule group: %w", err)
		}
		items = append(items, &g)
	}
	return items, rows.Err()
}

// RuleCounts implements biz.GroupReader：一次分组聚合（走 uk_monitor_rule_name 的 group_id 前缀）。
func (r *groupRepo) RuleCounts(ctx context.Context) (map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT group_id::text, count(*) FROM monitor_rule WHERE delete_time IS NULL GROUP BY group_id")
	if err != nil {
		return nil, fmt.Errorf("count monitor rules by group: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int32, 32)
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan monitor rule count: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

// Create implements biz.GroupWriter。层级与总数上限是「先查后写」，以配置版本行的行锁串行化。
func (r *groupRepo) Create(ctx context.Context, g *biz.RuleGroup, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgRuleGroup); err != nil {
			return err
		}
		var total int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM monitor_rule_group WHERE delete_time IS NULL").Scan(&total); err != nil {
			return fmt.Errorf("count monitor rule groups: %w", err)
		}
		if total >= biz.MaxGroups {
			return biz.GroupInvalid("at most %d rule groups", biz.MaxGroups)
		}
		parentPath := "/"
		if g.ParentID != "" {
			err := tx.QueryRow(ctx, "SELECT path FROM monitor_rule_group WHERE id = $1 AND delete_time IS NULL", g.ParentID).Scan(&parentPath)
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrGroupNotFound
			}
			if err != nil {
				return fmt.Errorf("get parent monitor rule group: %w", err)
			}
			if strings.Count(parentPath, "/")-1 >= biz.MaxGroupDepth {
				return biz.GroupInvalid("rule groups can be nested at most %d levels", biz.MaxGroupDepth)
			}
		}
		g.Path = parentPath + g.ID + "/"
		_, err := tx.Exec(ctx,
			`INSERT INTO monitor_rule_group (id, parent_id, path, name, description, sort, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			g.ID, dbx.NilIfEmpty(g.ParentID), g.Path, g.Name, g.Description, g.Sort, dbx.NilIfEmpty(actor))
		if err != nil {
			if isUniqueViolation(err, "uk_monitor_rule_group_name") {
				return biz.ErrGroupNameExists
			}
			return fmt.Errorf("insert monitor rule group: %w", err)
		}
		return bumpCfg(ctx, tx, mc.CfgRuleGroup)
	})
}

// Update implements biz.GroupWriter。与 Create / Delete 一样先锁配置版本行再碰分组行：加锁顺序不一致
// 时，并发的 Update（分组行 → 版本行）与 Delete（版本行 → 分组行）会互相等待成死锁。
func (r *groupRepo) Update(ctx context.Context, g *biz.RuleGroup, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgRuleGroup); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_rule_group SET name = $2, description = $3, sort = $4, update_user = $5, update_time = now()
			 WHERE id = $1 AND delete_time IS NULL`, g.ID, g.Name, g.Description, g.Sort, dbx.NilIfEmpty(actor))
		if err != nil {
			if isUniqueViolation(err, "uk_monitor_rule_group_name") {
				return biz.ErrGroupNameExists
			}
			return fmt.Errorf("update monitor rule group: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrGroupNotFound
		}
		return bumpCfg(ctx, tx, mc.CfgRuleGroup)
	})
}

// Delete implements biz.GroupWriter。先行锁分组行再查下级、规则与引用它的屏蔽规则 / 订阅 / 记录规则 /
// 升级策略 / 抑制规则：新建规则、记录规则、下级分组、屏蔽规则、订阅、升级策略、抑制规则时都对所属分组行
// 加共享锁（lockGroup / lockGroups），与这里互斥，不会出现「刚判定为空就被塞进一条规则」。
func (r *groupRepo) Delete(ctx context.Context, id, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgRuleGroup); err != nil {
			return err
		}
		var locked string
		err := tx.QueryRow(ctx, "SELECT id::text FROM monitor_rule_group WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrGroupNotFound
		}
		if err != nil {
			return fmt.Errorf("lock monitor rule group: %w", err)
		}
		var inUse bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM monitor_rule_group WHERE parent_id = $1 AND delete_time IS NULL)
			     OR EXISTS (SELECT 1 FROM monitor_rule WHERE group_id = $1 AND delete_time IS NULL)`, id).Scan(&inUse); err != nil {
			return fmt.Errorf("check monitor rule group usage: %w", err)
		}
		if inUse {
			return biz.ErrGroupInUse
		}
		// 屏蔽规则以它为作用分组、订阅规则订阅了它：删掉分组后这些配置既不再生效也无从修正
		// （分组选择器里已没有它），所以先让用户处理掉。
		var mutes, subscriptions int
		if err := tx.QueryRow(ctx,
			`SELECT (SELECT count(*) FROM monitor_mute WHERE group_id = $1 AND delete_time IS NULL),
			        (SELECT count(*) FROM monitor_subscription WHERE group_ids @> ARRAY[$1::uuid] AND delete_time IS NULL)`, id).
			Scan(&mutes, &subscriptions); err != nil {
			return fmt.Errorf("count references to monitor rule group: %w", err)
		}
		if mutes > 0 || subscriptions > 0 {
			return biz.GroupReferenced(mutes, subscriptions)
		}
		// 记录规则以它为所属分组：删掉分组后规则仍被引擎照常执行，非超级管理员却再也看不到、管不了它。
		// 升级策略 / 抑制规则以它为作用分组：分组没了它们既不生效也无从修正。同样先让用户处理掉。
		var recordRules, escalations, inhibits int
		if err := tx.QueryRow(ctx,
			`SELECT (SELECT count(*) FROM monitor_record_rule WHERE group_id = $1 AND delete_time IS NULL),
			        (SELECT count(*) FROM monitor_escalation WHERE group_id = $1 AND delete_time IS NULL),
			        (SELECT count(*) FROM monitor_inhibit WHERE group_id = $1 AND delete_time IS NULL)`, id).
			Scan(&recordRules, &escalations, &inhibits); err != nil {
			return fmt.Errorf("count references to monitor rule group: %w", err)
		}
		if recordRules > 0 || escalations > 0 || inhibits > 0 {
			return biz.GroupReferencedBy(recordRules, escalations, inhibits)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM monitor_role_scope WHERE group_id = $1", id); err != nil {
			return fmt.Errorf("delete monitor role scopes of group: %w", err)
		}
		if _, err := tx.Exec(ctx,
			"UPDATE monitor_rule_group SET delete_time = now(), delete_user = $2 WHERE id = $1", id, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("delete monitor rule group: %w", err)
		}
		return bumpCfg(ctx, tx, mc.CfgRuleGroup)
	})
}

// lockGroup 在写事务内确认分组存在并加共享锁（与分组删除互斥）；不存在返回 ErrGroupNotFound。
func lockGroup(ctx context.Context, tx pgx.Tx, id string) error {
	var locked string
	err := tx.QueryRow(ctx, "SELECT id::text FROM monitor_rule_group WHERE id = $1 AND delete_time IS NULL FOR SHARE", id).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.ErrGroupNotFound
	}
	if err != nil {
		return fmt.Errorf("lock monitor rule group: %w", err)
	}
	return nil
}

// lockGroups 在写事务内确认这些分组都存在并加共享锁（按 ID 排序，锁序确定）；有不存在的返回
// ErrGroupNotFound。
func lockGroups(ctx context.Context, tx pgx.Tx, ids []string) error {
	rows, err := tx.Query(ctx,
		"SELECT id::text FROM monitor_rule_group WHERE id = ANY($1::uuid[]) AND delete_time IS NULL ORDER BY id FOR SHARE", ids)
	if err != nil {
		return fmt.Errorf("lock monitor rule groups: %w", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lock monitor rule groups: %w", err)
	}
	if n != len(ids) {
		return biz.ErrGroupNotFound
	}
	return nil
}

type roleScopeRepo struct {
	data *basedata.Data
}

// NewRoleScopeRepo new a roleScopeRepo.
func NewRoleScopeRepo(data *basedata.Data) *roleScopeRepo { return &roleScopeRepo{data: data} }

func (r *roleScopeRepo) query(ctx context.Context, where string, ids []string) ([]*biz.RoleScope, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT role_id::text, group_id::text, level, COALESCE(update_user::text, ''), update_time FROM monitor_role_scope WHERE "+where, ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor role scopes: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RoleScope, 0, 8)
	for rows.Next() {
		var s biz.RoleScope
		if err := rows.Scan(&s.RoleID, &s.GroupID, &s.Level, &s.UpdateUser, &s.UpdateTime); err != nil {
			return nil, fmt.Errorf("scan monitor role scope: %w", err)
		}
		items = append(items, &s)
	}
	return items, rows.Err()
}

// ListByRoles implements biz.RoleScopeRepo.
func (r *roleScopeRepo) ListByRoles(ctx context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	return r.query(ctx, "role_id = ANY($1::uuid[])", roleIDs)
}

// ListByGroups implements biz.RoleScopeRepo.
func (r *roleScopeRepo) ListByGroups(ctx context.Context, groupIDs []string) ([]*biz.RoleScope, error) {
	return r.query(ctx, "group_id = ANY($1::uuid[])", groupIDs)
}

// Replace implements biz.RoleScopeRepo：整组替换（单事务，分组行共享锁防止替换期间分组被删）。
func (r *roleScopeRepo) Replace(ctx context.Context, groupID string, items []*biz.RoleScope, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockGroup(ctx, tx, groupID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM monitor_role_scope WHERE group_id = $1", groupID); err != nil {
			return fmt.Errorf("clear monitor role scopes: %w", err)
		}
		if len(items) == 0 {
			return nil
		}
		roleIDs := make([]string, len(items))
		levels := make([]int32, len(items))
		for i, it := range items {
			roleIDs[i], levels[i] = it.RoleID, it.Level
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO monitor_role_scope (role_id, group_id, level, update_user)
			 SELECT x.role_id, $1, x.level, $4 FROM unnest($2::uuid[], $3::int[]) AS x(role_id, level)`,
			groupID, roleIDs, levels, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("insert monitor role scopes: %w", err)
		}
		return nil
	})
}
