-- 0051 CMDB 认领同步冲突 / 时间点回溯：补充授权（0050 已发布，不在原文件上修改）。
-- 控制面：认领成功后移除冲突行；删除采集源时清理其冲突清单。
GRANT DELETE ON cmdb_sync_conflict TO cmdb_core_role;

-- 接口权限：新接口授予已持有对应接口的角色（接口权限在保存授权时逐条物化，不会自动获得）。
WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbSyncService/UpdateSource', '/cmdb.v1.CmdbSyncService/ClaimConflict'),
    ('/cmdb.v1.CmdbCIService/ListCIHistory',  '/cmdb.v1.CmdbCIService/GetCIAt'))
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), p.role_id, n.new_op
FROM system_permission p JOIN pairs n ON n.old_op = p.operation
ON CONFLICT (role_id, operation) DO NOTHING;

WITH pairs(old_op, new_op) AS (VALUES
    ('/cmdb.v1.CmdbSyncService/UpdateSource', '/cmdb.v1.CmdbSyncService/ClaimConflict'),
    ('/cmdb.v1.CmdbCIService/ListCIHistory',  '/cmdb.v1.CmdbCIService/GetCIAt'))
INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), g.role_id, 'api', md5(n.new_op)
FROM system_grant g JOIN pairs n ON g.resource_type = 'api' AND g.resource_code = md5(n.old_op)
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;
