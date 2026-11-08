-- 0007_permission_restore.up.sql 恢复权限管理域：重建 0004/0005 建立、
-- 0006 删除的两张表（结构一致，不含 0005 的存量迁移 INSERT——历史授权数据
-- 已不可恢复，如需授权须重新配置）。
-- system_permission：角色→接口操作（Casbin p 策略行，与 apis 直接授权
-- 一一对应）；system_grant：角色三类直接授权（menu/element/api）。

CREATE TABLE system_permission (
    id            uuid         PRIMARY KEY,
    role_id       uuid         NOT NULL REFERENCES system_role (id),
    operation     varchar(256) NOT NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    create_user   uuid         NULL
);

COMMENT ON TABLE system_permission IS '角色接口操作授权表（Casbin p 策略）：与角色的 apis 直接授权一一对应，保存时全量替换，物理删除';
COMMENT ON COLUMN system_permission.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_permission.role_id IS '角色 ID，引用 system_role.id；删角色时级联物理删除';
COMMENT ON COLUMN system_permission.operation IS '接口操作 code = md5(proto full method) 32 位小写 hex';
COMMENT ON COLUMN system_permission.create_time IS '授权时间';
COMMENT ON COLUMN system_permission.create_user IS '授权操作者，当前置 NULL 待认证中间件注入';

CREATE UNIQUE INDEX uk_system_permission_role_operation ON system_permission (role_id, operation);

CREATE TABLE system_grant (
    id            uuid         PRIMARY KEY,
    role_id       uuid         NOT NULL REFERENCES system_role (id),
    resource_type varchar(16)  NOT NULL CHECK (resource_type IN ('menu', 'element', 'api')),
    resource_code varchar(256) NOT NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    create_user   uuid         NULL
);

COMMENT ON TABLE system_grant IS '角色资源授权表：菜单/页面元素/接口三类直接授权，物理删除；有效接口权限物化于 system_permission';
COMMENT ON COLUMN system_grant.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_grant.role_id IS '角色 ID，引用 system_role.id；删角色时级联物理删除';
COMMENT ON COLUMN system_grant.resource_type IS '资源类型：menu=菜单（页面） element=页面标签元素 api=接口操作';
COMMENT ON COLUMN system_grant.resource_code IS '资源码：菜单/元素为目录码（如 system.user.create），接口为 32 位 md5 hex 操作码';
COMMENT ON COLUMN system_grant.create_time IS '授权时间';
COMMENT ON COLUMN system_grant.create_user IS '授权操作者，当前置 NULL 待认证中间件注入';

CREATE UNIQUE INDEX uk_system_grant_role_resource ON system_grant (role_id, resource_type, resource_code);
