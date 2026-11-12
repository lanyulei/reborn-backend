-- 0036 CMDB 第三阶段优化：字段索引/敏感标记、采集源回调令牌、历史分区维护权限。

-- 字段：indexed 为该属性建表达式索引（core 维护）；sensitive 读取时脱敏。
ALTER TABLE cmdb_model_field ADD COLUMN indexed boolean NOT NULL DEFAULT false;
ALTER TABLE cmdb_model_field ADD COLUMN sensitive boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN cmdb_model_field.indexed IS '为 attrs->>code 建表达式索引（CREATE INDEX ... ((attrs ->> code)) WHERE model_code = ...）';
COMMENT ON COLUMN cmdb_model_field.sensitive IS '敏感属性：读取接口、历史与导出一律脱敏';

-- 采集源：回调触发令牌（只存 SHA-256，明文只在轮换时返回一次）与最近回调时间（限流）。
ALTER TABLE cmdb_sync_source ADD COLUMN webhook_token_hash bytea NULL;
ALTER TABLE cmdb_sync_source ADD COLUMN webhook_last_time timestamptz NULL;
COMMENT ON COLUMN cmdb_sync_source.webhook_token_hash IS '回调令牌 SHA-256；NULL = 未启用回调';
COMMENT ON COLUMN cmdb_sync_source.webhook_last_time IS '最近一次回调触发时间（数据面限流：同源 60s 内最多一次）';

-- 历史分区维护由数据面执行（预建未来分区 / 按保留期删除）：ATTACH / DROP 须为父表属主。
-- core 经 cmdb_core_role 保留读写（部署时 core 登录账号 GRANT 为成员；dev 超级用户不受影响）。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cmdb_core_role') THEN
        CREATE ROLE cmdb_core_role NOLOGIN;
    END IF;
END
$$;
GRANT CREATE ON SCHEMA public TO cmdb_sync_role;
ALTER TABLE cmdb_ci_history OWNER TO cmdb_sync_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'cmdb_ci_history'::regclass LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO cmdb_sync_role', r.part);
    END LOOP;
END
$$;
GRANT SELECT, INSERT ON cmdb_ci_history TO cmdb_core_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON cmdb_model, cmdb_model_field, cmdb_relation_type, cmdb_ci, cmdb_relation,
    cmdb_model_view, cmdb_role_scope, cmdb_sync_source, cmdb_sync_task TO cmdb_core_role;
GRANT SELECT ON cmdb_sync_run TO cmdb_core_role;
GRANT UPDATE (webhook_last_time, last_status, last_run_id, last_run_time, next_run_time) ON cmdb_sync_source TO cmdb_sync_role;
