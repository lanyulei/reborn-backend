-- 资源列表：按类型过滤 + (update_time, id) 倒序 / 键集分页。
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_model_update ON cmdb_ci (model_code, update_time DESC, id DESC) WHERE delete_time IS NULL;
-- 全局变更审计：按时间倒序分页。
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_history_time ON cmdb_ci_history (create_time DESC, id DESC);
