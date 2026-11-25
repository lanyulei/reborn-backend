-- 0092 k8s_audit：数据面角色只追加（只改权限，不改表结构）。
--
-- 背景：迁移 0072 为了让数据面自己做审计表的分区维护，把 k8s_audit 及其分区的属主设成了 k8s_plane_role。
-- 属主拥有全部权限，盖过了同一处写的「只授 SELECT, INSERT」——执行用户集群操作的那个服务因此可以改、删、
-- 清空审计表，审计的「只增不改」只靠代码约束。
--
-- 做法：收回该角色自己的 UPDATE / DELETE / TRUNCATE。属主身份保留（分区维护需要它：建分区、挂载、卸载、
-- 删除过期分区，这些是属主权利，与 DML 权限无关），INSERT / SELECT 不变。例外是 DEFAULT 分区保留 DELETE：
-- 新月份的分区建好时，要把此前落进 DEFAULT 的行搬过去（见 k8s/internal/data/maintenance.go 的 ensurePartition）。
-- 以后由数据面角色新建的分区通过默认权限规则自动适用。
--
-- 边界：这只是防误操作（程序缺陷、误连上来的手工操作）。持有该角色凭据的人仍可以自己把权限授回去——
-- 属主永远保留授权权。要防蓄意篡改得把分区维护移出数据面并更换属主，属于更大的改动。
--
-- 幂等：对不存在的权限 REVOKE 是空操作。

REVOKE UPDATE, DELETE, TRUNCATE ON k8s_audit FROM k8s_plane_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits WHERE inhparent = 'k8s_audit'::regclass LOOP
        EXECUTE format('REVOKE UPDATE, TRUNCATE ON %s FROM k8s_plane_role', r.part);
        IF r.part <> 'k8s_audit_default'::regclass THEN
            EXECUTE format('REVOKE DELETE ON %s FROM k8s_plane_role', r.part);
        END IF;
    END LOOP;
END
$$;
ALTER DEFAULT PRIVILEGES FOR ROLE k8s_plane_role IN SCHEMA public REVOKE UPDATE, DELETE, TRUNCATE ON TABLES FROM k8s_plane_role;
