-- 0086 down：只收回本迁移新增的对象权限。权限数据的修复（4a / 4b）不回滚——
-- 回滚只会让补授再次失效、让「运维 Agent」角色再次一编辑就丢权限。

REVOKE SELECT, INSERT, DELETE ON notify_ingress_replay FROM notify_pipe_role;
REVOKE CREATE ON SCHEMA public FROM notify_pipe_role;
REVOKE SELECT, INSERT, UPDATE, DELETE ON cmdb_model_group, cmdb_model_favorite, cmdb_schema_audit FROM cmdb_core_role;
