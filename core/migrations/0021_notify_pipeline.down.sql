-- 回滚 0021：撤销授权与属主变更、删除管道运行表与索引（角色保留，跨库共享）。
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits
             WHERE inhparent IN ('notify_event'::regclass, 'notify_dispatch'::regclass, 'notify_delivery_log'::regclass)
    LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO CURRENT_USER', r.part);
    END LOOP;
END $$;
ALTER TABLE notify_event OWNER TO CURRENT_USER;
ALTER TABLE notify_dispatch OWNER TO CURRENT_USER;
ALTER TABLE notify_delivery_log OWNER TO CURRENT_USER;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM notify_pipe_role, notify_core_role;
ALTER TABLE notify_dispatch DROP CONSTRAINT IF EXISTS ck_notify_dispatch_due;
DROP INDEX IF EXISTS idx_notify_dispatch_sending;
DROP TABLE IF EXISTS notify_rate_bucket;
DROP TABLE IF EXISTS notify_agg_group;
DROP TABLE IF EXISTS notify_dedup;
