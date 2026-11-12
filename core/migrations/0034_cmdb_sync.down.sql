-- 0034 回滚：删除同步控制面表与资源表的消失时间列。
REVOKE ALL ON cmdb_model, cmdb_model_field, cmdb_relation_type, cmdb_ci, cmdb_relation, cmdb_ci_history,
    cmdb_sync_source, cmdb_sync_task, cmdb_sync_run, cmdb_sync_staging FROM cmdb_sync_role;
DROP TABLE IF EXISTS cmdb_sync_staging;
DROP TABLE IF EXISTS cmdb_sync_run;
DROP TABLE IF EXISTS cmdb_sync_task;
DROP TABLE IF EXISTS cmdb_sync_source;
DROP INDEX IF EXISTS idx_cmdb_ci_source;
ALTER TABLE cmdb_ci DROP COLUMN IF EXISTS missing_since;
DROP ROLE IF EXISTS cmdb_sync_role;
