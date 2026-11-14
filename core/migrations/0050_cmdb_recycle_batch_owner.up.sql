-- 0050 CMDB 回收站 / 批量操作 / 负责人。
-- 回收站列表：已删除资源按类型 + 更新时间倒序分页（部分索引，只含已删除行，体量小）。
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_deleted ON cmdb_ci (model_code, update_time DESC, id DESC) WHERE delete_time IS NOT NULL;
-- 按负责人筛选（"我负责的"）：只索引有负责人的未删除资源。
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_owner ON cmdb_ci (owner, update_time DESC, id DESC) WHERE delete_time IS NULL AND owner <> '';
COMMENT ON COLUMN cmdb_ci.owner IS '负责人：系统用户 ID（uuid 文本）或历史自由文本';

-- 同步冲突：对账时因名称 / 唯一规则冲突而未能写入的采集行，按（来源、类型、外部标识）留存当前
-- 仍未解决的那些——每次对账整体替换该来源该类型的记录，冲突消失即移除。运行记录里只有
-- 少量样例，这里是完整的"待处理"清单。由数据面写入，控制面只读。
CREATE TABLE cmdb_sync_conflict (
    source      varchar(80)  NOT NULL,
    model_code  varchar(64)  NOT NULL,
    external_id varchar(128) NOT NULL,
    name        varchar(128) NOT NULL,
    reason      varchar(512) NOT NULL,
    first_seen  timestamptz  NOT NULL DEFAULT now(),
    last_seen   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (source, model_code, external_id)
);
COMMENT ON TABLE cmdb_sync_conflict IS 'CMDB 同步冲突：当前仍未能写入的采集行及原因（每次对账整体刷新）';
COMMENT ON COLUMN cmdb_sync_conflict.source IS '来源：sync:<采集源 ID>（同 cmdb_ci.source）';
COMMENT ON COLUMN cmdb_sync_conflict.first_seen IS '该冲突首次出现的时间（持续存在期间不变）';
ALTER TABLE cmdb_sync_conflict OWNER TO cmdb_sync_role;
GRANT SELECT ON cmdb_sync_conflict TO cmdb_core_role;

-- 接口权限：新接口授予已持有对应单条接口的角色（接口权限在保存授权时逐条物化，不会自动获得）。
WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbCIService/UpdateCI',     '/cmdb.v1.CmdbCIService/BatchUpdateCIs'),
    ('/cmdb.v1.CmdbCIService/TransitionCI', '/cmdb.v1.CmdbCIService/BatchTransitionCIs'),
    ('/cmdb.v1.CmdbCIService/DeleteCI',     '/cmdb.v1.CmdbCIService/BatchDeleteCIs'),
    ('/cmdb.v1.CmdbCIService/DeleteCI',     '/cmdb.v1.CmdbCIService/RestoreCI'),
    ('/cmdb.v1.CmdbCIService/ListCIs',      '/cmdb.v1.CmdbCIService/ListOwnerCandidates'),
    ('/cmdb.v1.CmdbInsightService/GetOverview', '/cmdb.v1.CmdbInsightService/GetQuality'),
    ('/cmdb.v1.CmdbSyncService/ListRuns',   '/cmdb.v1.CmdbSyncService/ListConflicts'))
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), p.role_id, n.new_op
FROM system_permission p JOIN pairs n ON n.old_op = p.operation
ON CONFLICT (role_id, operation) DO NOTHING;

WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbCIService/UpdateCI',     '/cmdb.v1.CmdbCIService/BatchUpdateCIs'),
    ('/cmdb.v1.CmdbCIService/TransitionCI', '/cmdb.v1.CmdbCIService/BatchTransitionCIs'),
    ('/cmdb.v1.CmdbCIService/DeleteCI',     '/cmdb.v1.CmdbCIService/BatchDeleteCIs'),
    ('/cmdb.v1.CmdbCIService/DeleteCI',     '/cmdb.v1.CmdbCIService/RestoreCI'),
    ('/cmdb.v1.CmdbCIService/ListCIs',      '/cmdb.v1.CmdbCIService/ListOwnerCandidates'),
    ('/cmdb.v1.CmdbInsightService/GetOverview', '/cmdb.v1.CmdbInsightService/GetQuality'),
    ('/cmdb.v1.CmdbSyncService/ListRuns',   '/cmdb.v1.CmdbSyncService/ListConflicts'))
INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), g.role_id, 'api', md5(n.new_op)
FROM system_grant g JOIN pairs n ON g.resource_type = 'api' AND g.resource_code = md5(n.old_op)
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;
