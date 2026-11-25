-- 0092 回滚：把 UPDATE / DELETE / TRUNCATE 重新授予数据面角色（父表与现有分区），并对以后新建的分区授回同样的权限。

GRANT UPDATE, DELETE, TRUNCATE ON k8s_audit TO k8s_plane_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'k8s_audit'::regclass LOOP
        EXECUTE format('GRANT UPDATE, DELETE, TRUNCATE ON %s TO k8s_plane_role', r.part);
    END LOOP;
END
$$;
ALTER DEFAULT PRIVILEGES FOR ROLE k8s_plane_role IN SCHEMA public GRANT UPDATE, DELETE, TRUNCATE ON TABLES TO k8s_plane_role;
