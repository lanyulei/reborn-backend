-- 0036 回滚。
REVOKE UPDATE (webhook_last_time, last_status, last_run_id, last_run_time, next_run_time) ON cmdb_sync_source FROM cmdb_sync_role;
REVOKE ALL ON cmdb_model, cmdb_model_field, cmdb_relation_type, cmdb_ci, cmdb_relation, cmdb_model_view, cmdb_role_scope,
    cmdb_sync_source, cmdb_sync_task, cmdb_sync_run, cmdb_ci_history FROM cmdb_core_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'cmdb_ci_history'::regclass LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO CURRENT_USER', r.part);
    END LOOP;
END
$$;
ALTER TABLE cmdb_ci_history OWNER TO CURRENT_USER;
REVOKE CREATE ON SCHEMA public FROM cmdb_sync_role;
DROP ROLE IF EXISTS cmdb_core_role;
ALTER TABLE cmdb_sync_source DROP COLUMN IF EXISTS webhook_last_time;
ALTER TABLE cmdb_sync_source DROP COLUMN IF EXISTS webhook_token_hash;
ALTER TABLE cmdb_model_field DROP COLUMN IF EXISTS sensitive;
ALTER TABLE cmdb_model_field DROP COLUMN IF EXISTS indexed;
