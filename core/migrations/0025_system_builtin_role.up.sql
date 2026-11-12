-- 0025 内置默认角色：随迁移预置、不可删除、写操作仅超管、新建用户自动绑定且不可解绑。
-- builtin 标记由迁移写入（应用层不开放创建内置角色）；删除由 biz 直接拒绝，
-- 编辑/授权由 service 层按操作者 is_admin 守卫。

ALTER TABLE system_role ADD COLUMN builtin boolean NOT NULL DEFAULT false;
COMMENT ON COLUMN system_role.builtin IS '是否内置角色（迁移预置）：不可删除，编辑/授权仅超管；新建用户自动绑定且不可解绑';

-- 默认角色 default：初始空授权，由超管按需配置。固定 UUIDv7 保证跨环境主键稳定；
-- ON CONFLICT 与 0001 内置 admin 同式，可安全重放。
INSERT INTO system_role (id, name, code, status, builtin) VALUES (
    '01a1b987-129c-7186-9a25-92bd159a7660',
    '默认角色',
    'default',
    1,
    true
) ON CONFLICT (code) WHERE delete_time IS NULL DO NOTHING;

-- 存量用户补绑默认角色（幂等：uk_system_user_role (user_id, role_id)）。
-- 绑定行主键在 SQL 中以 gen_random_uuid() 生成（迁移无法生成 UUIDv7；仅作行标识，无排序语义）。
INSERT INTO system_user_role (id, user_id, role_id)
SELECT gen_random_uuid(), u.id, r.id
FROM "system_user" u
CROSS JOIN system_role r
WHERE r.code = 'default' AND r.delete_time IS NULL AND u.delete_time IS NULL
ON CONFLICT (user_id, role_id) DO NOTHING;
