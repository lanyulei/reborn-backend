-- 0001_system_init.up.sql 系统域初始表结构（库：reborn）。
-- 由旧 0001/0002/0007 整合（user_ 前缀建表 + 改名 system_ + 角色权限列）。
-- 已执行过旧迁移全量链路的库无需重复执行：表/索引/约束语义等价，且本基线
-- 更干净（NOT NULL 约束名无 user_* 旧前缀残留，permissions 列位于 status 后
-- 而非表尾）。
-- 约定：
--   * id 一律 uuid（应用侧生成 UUIDv7，跨服务全局唯一）。
--   * 审计字段固定顺序：create_time, update_time, delete_time, create_user, update_user, delete_user。
--   * 业务表软删除（delete_time 置位）；关联表只保留 create_time / create_user，物理删除。
--   * 唯一性只约束未删除行（部分唯一索引），软删后业务键可复用。
-- 注意：SYSTEM_USER 是 PostgreSQL 15+ 保留字（SQL 标准函数），该表名在所有 SQL 中必须加双引号。

-- 用户表
CREATE TABLE "system_user" (
    id          uuid         PRIMARY KEY,
    username    varchar(64)  NOT NULL,
    password    varchar(255) NOT NULL,
    nickname    varchar(64)  NOT NULL DEFAULT '',
    email       varchar(255) NULL,
    phone       varchar(20)  NULL,
    gender      varchar(16)  NOT NULL DEFAULT 'unknown',
    is_admin    boolean      NOT NULL DEFAULT false,
    status      smallint     NOT NULL DEFAULT 1,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL
);

COMMENT ON TABLE "system_user" IS '用户表';
COMMENT ON COLUMN "system_user".id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN "system_user".username IS '登录名，未删除行内唯一';
COMMENT ON COLUMN "system_user".password IS 'Argon2id PHC 格式哈希串';
COMMENT ON COLUMN "system_user".nickname IS '昵称';
COMMENT ON COLUMN "system_user".email IS '邮箱，可空，未删除行内唯一';
COMMENT ON COLUMN "system_user".phone IS '手机号，可空，未删除行内唯一';
COMMENT ON COLUMN "system_user".gender IS '性别：male / female / unknown';
COMMENT ON COLUMN "system_user".is_admin IS '是否超级管理员';
COMMENT ON COLUMN "system_user".status IS '状态：1=可用 2=停用 3=暂停 4=离职';

CREATE UNIQUE INDEX uk_system_user_username ON "system_user" (username) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uk_system_user_email ON "system_user" (email) WHERE delete_time IS NULL AND email IS NOT NULL;
CREATE UNIQUE INDEX uk_system_user_phone ON "system_user" (phone) WHERE delete_time IS NULL AND phone IS NOT NULL;

-- 角色表
CREATE TABLE system_role (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    code        varchar(64)  NOT NULL,
    status      smallint     NOT NULL DEFAULT 1,
    permissions jsonb        NOT NULL DEFAULT '[]'::jsonb,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL
);

COMMENT ON TABLE system_role IS '角色表';
COMMENT ON COLUMN system_role.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_role.name IS '角色展示名';
COMMENT ON COLUMN system_role.code IS '角色业务标识，创建后不可改，未删除行内唯一';
COMMENT ON COLUMN system_role.status IS '状态：1=启用 0=禁用';
COMMENT ON COLUMN system_role.permissions IS '菜单权限码数组（如 ["system.user"]），合法目录由应用层校验，全量覆盖式更新';

CREATE UNIQUE INDEX uk_system_role_code ON system_role (code) WHERE delete_time IS NULL;

-- 用户角色关系表：一个用户可绑定多个角色；仅 create_time / create_user，物理删除。
CREATE TABLE system_user_role (
    id          uuid        PRIMARY KEY,
    user_id     uuid        NOT NULL REFERENCES "system_user" (id),
    role_id     uuid        NOT NULL REFERENCES system_role (id),
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL
);

COMMENT ON TABLE system_user_role IS '用户角色关系表（物理删除）';
COMMENT ON COLUMN system_user_role.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_user_role.user_id IS '用户 ID，引用 "system_user".id';
COMMENT ON COLUMN system_user_role.role_id IS '角色 ID，引用 system_role.id';

CREATE UNIQUE INDEX uk_system_user_role ON system_user_role (user_id, role_id);
CREATE INDEX idx_system_user_role_role_id ON system_user_role (role_id);

-- 内置超级管理员：admin / Admin123!（Argon2id PHC 串由应用同款 PasswordHasher
-- 生成并经 Verify 校验，参数与 configs/config.yaml 一致 m=65536,t=3,p=4）。
-- 固定 UUIDv7 保证跨环境主键稳定；ON CONFLICT 使该语句在已建表的库上
-- 手动补种时可安全重放。生产环境部署后应立即改密或停用该账号。
INSERT INTO "system_user" (id, username, password, nickname, is_admin, status) VALUES (
    '01a1b987-129c-7186-9a25-92bd159a765f',
    'admin',
    '$argon2id$v=19$m=65536,t=3,p=4$iHvYng7lmFB3Nqp8RCmLbA$T2E3QDXxXpbGjszgeqQ1VQnuSt9hM6OjrurLxssf4NU',
    '超级管理员',
    true,
    1
) ON CONFLICT (username) WHERE delete_time IS NULL DO NOTHING;
