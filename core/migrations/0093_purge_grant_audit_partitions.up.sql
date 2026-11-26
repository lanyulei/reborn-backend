-- 0093 保留期清理所需的授权 + 审计分区收权的补全（只改权限，不改表结构）。
--
-- 1) cmdb_sync_task / cmdb_sync_run：把 DELETE 授给数据面角色。同步任务与运行记录的保留期清理由数据面的
--    维护循环执行，而 0034 只给了 SELECT / INSERT / UPDATE——按角色部署时清理语句会被拒绝（42501），
--    清理不生效。按超级用户连接的开发环境看不出这个问题。
--
-- 2) k8s_audit：补 0092 的两处（0092 的语句对已挂接的分区是生效的，这里不重复它们的效果）。
--    a. 0092 的注释说「以后由数据面角色新建的分区通过默认权限规则自动适用」，这不成立：带 IN SCHEMA 的
--       REVOKE 建不出条目，而且默认权限只按「建表时的角色」匹配——分区通常是服务的登录账号建出来、再把属主
--       对齐到父表属主的。现在改由数据面在每建出一个分区时显式收回权限
--       （k8s/internal/data/maintenance.go 的 ensurePartition）。
--    b. 0092 只处理了当时已挂接的分区：按分区命名规则建出、还没来得及挂接的独立表（分区维护中途失败留下的
--       半成品，之后会被接续并挂载）没有收回；以及 0092 与带上述代码的数据面版本不是同时上线时，两者之间
--       新建出来的分区。这里一并收回。
--    口径与 0092 相同：UPDATE / DELETE / TRUNCATE 收回，DEFAULT 分区保留 DELETE（新月份分区建好时要把
--    落在 DEFAULT 里的行搬过去）。
--
-- 幂等：GRANT / REVOKE 重复执行无副作用。

GRANT DELETE ON cmdb_sync_task, cmdb_sync_run TO cmdb_sync_role;

DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT c.oid, c.relname FROM pg_class c
        WHERE c.relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = 'k8s_audit'::regclass)
          AND c.relkind = 'r'
          AND (c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = 'k8s_audit'::regclass)
               OR c.relname ~ '^k8s_audit_[0-9]{4}_[0-9]{2}$')
    LOOP
        EXECUTE format('REVOKE UPDATE, TRUNCATE ON %s FROM k8s_plane_role', r.oid::regclass);
        IF r.relname <> 'k8s_audit_default' THEN
            EXECUTE format('REVOKE DELETE ON %s FROM k8s_plane_role', r.oid::regclass);
        END IF;
    END LOOP;
END
$$;
