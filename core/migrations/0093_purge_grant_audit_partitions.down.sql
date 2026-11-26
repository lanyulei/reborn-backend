-- 0093 回滚：收回数据面角色对同步任务 / 运行记录的 DELETE。
-- 审计分区的收权不在这里回滚：它与 0092 是同一件事的补全，回滚 0092 时一并恢复。

REVOKE DELETE ON cmdb_sync_task, cmdb_sync_run FROM cmdb_sync_role;
