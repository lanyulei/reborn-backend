-- 0053 CMDB 保存的视图（服务端存储，随账号漫游）/ 同步冲突真实总数。

-- 保存的视图：按用户 + 模型的命名筛选条件；filters 为前端筛选表单的值，后端只存取不解释。
-- 不设外键（同 cmdb_model_favorite）：用户 / 模型删除后成为无主数据，不影响正确性。
CREATE TABLE cmdb_ci_view (
    user_id     uuid         NOT NULL,
    model_code  varchar(64)  NOT NULL,
    name        varchar(32)  NOT NULL,
    filters     jsonb        NOT NULL DEFAULT '{}',
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, model_code, name)
);
COMMENT ON TABLE cmdb_ci_view IS 'CMDB 资源列表保存的视图：按用户 + 模型的命名筛选条件';
GRANT SELECT, INSERT, UPDATE, DELETE ON cmdb_ci_view TO cmdb_core_role;

-- 同步冲突总数：某来源某类型最近一次对账的真实冲突数（cmdb_sync_conflict 有留存上限，这里不截断）。
-- 控制面据此提示"清单不完整"。由数据面写入，控制面只读；删除采集源时一并清理。
CREATE TABLE cmdb_sync_conflict_stat (
    source      varchar(80) NOT NULL,
    model_code  varchar(64) NOT NULL,
    total       integer     NOT NULL,
    update_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, model_code)
);
COMMENT ON TABLE cmdb_sync_conflict_stat IS 'CMDB 同步冲突总数：最近一次对账的真实冲突数（不受清单留存上限截断）';
ALTER TABLE cmdb_sync_conflict_stat OWNER TO cmdb_sync_role;
GRANT SELECT, DELETE ON cmdb_sync_conflict_stat TO cmdb_core_role;
-- 存量：按现有清单初始化（已达上限的在下次对账前视为不完整）。
INSERT INTO cmdb_sync_conflict_stat (source, model_code, total)
SELECT source, model_code, CASE WHEN count(*) >= 2000 THEN 2001 ELSE count(*) END FROM cmdb_sync_conflict GROUP BY source, model_code;

-- 接口权限：视图接口授予已持有"收藏模型"接口的角色（同为资源列表页的个人偏好）。
WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbInsightService/ListFavorites', '/cmdb.v1.CmdbInsightService/ListCIViews'),
    ('/cmdb.v1.CmdbInsightService/SetFavorite',   '/cmdb.v1.CmdbInsightService/SaveCIView'),
    ('/cmdb.v1.CmdbInsightService/SetFavorite',   '/cmdb.v1.CmdbInsightService/DeleteCIView'))
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), p.role_id, n.new_op
FROM system_permission p JOIN pairs n ON n.old_op = p.operation
ON CONFLICT (role_id, operation) DO NOTHING;

WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbInsightService/ListFavorites', '/cmdb.v1.CmdbInsightService/ListCIViews'),
    ('/cmdb.v1.CmdbInsightService/SetFavorite',   '/cmdb.v1.CmdbInsightService/SaveCIView'),
    ('/cmdb.v1.CmdbInsightService/SetFavorite',   '/cmdb.v1.CmdbInsightService/DeleteCIView'))
INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), g.role_id, 'api', md5(n.new_op)
FROM system_grant g JOIN pairs n ON g.resource_type = 'api' AND g.resource_code = md5(n.old_op)
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;
