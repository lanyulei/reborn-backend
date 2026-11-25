-- 0072 down：删除容器平台的全部表与内置的通知配置（表级授权随 DROP TABLE 一并清除）。
-- 角色 k8s_plane_role / k8s_core_role 保留：角色是集群级对象，可能被其它库引用（同 0069 / 0070）。

DELETE FROM notify_template WHERE id IN ('018f3e1a-0cd2-7000-8000-000000000001', '018f3e1a-0cd2-7000-8000-000000000002');
DELETE FROM notify_route WHERE id = '018f3e1a-0cd1-7000-8000-000000000001';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

-- schema 级授权不随表删除而消失，须显式收回（同 0069 的回滚）。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'k8s_plane_role') THEN
        REVOKE CREATE ON SCHEMA public FROM k8s_plane_role;
    END IF;
END
$$;

DROP TABLE IF EXISTS k8s_notify_outbox;
DROP TABLE IF EXISTS k8s_exec_ticket;
DROP TABLE IF EXISTS k8s_exec_record;
DROP TABLE IF EXISTS k8s_exec_session;
DROP TABLE IF EXISTS k8s_audit;
DROP TABLE IF EXISTS k8s_cluster_runtime;
DROP TABLE IF EXISTS k8s_instance;
DROP TABLE IF EXISTS k8s_cfg_version;
DROP TABLE IF EXISTS k8s_role_scope;
DROP TABLE IF EXISTS k8s_agent_token;
DROP TABLE IF EXISTS k8s_cluster;
