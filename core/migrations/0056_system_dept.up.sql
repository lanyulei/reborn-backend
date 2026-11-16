-- 0056 组织架构（system 域）：部门树 / 部门负责人 / 用户所属部门。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；部门软删除，两张关系表物理删除。
-- 部门树以物化路径存储：path = 祖先链（含自身）的 id，以 '/' 分隔并首尾带 '/'
-- （根部门 '/<id>/'，子部门 '<父 path><id>/'）；子树 = path 前缀匹配，上溯 = 拆分 path。
-- 结构性写（新建 / 移动 / 删除）在应用层经同一把事务级 advisory lock 串行，路径不会交错。

CREATE TABLE system_dept (
    id          uuid         PRIMARY KEY,
    parent_id   uuid         NULL REFERENCES system_dept (id),
    path        varchar(512) NOT NULL,
    name        varchar(64)  NOT NULL,
    code        varchar(64)  NULL,
    sort        integer      NOT NULL DEFAULT 0,
    status      smallint     NOT NULL DEFAULT 1,
    description varchar(255) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_system_dept_status CHECK (status IN (0, 1)),
    CONSTRAINT ck_system_dept_parent CHECK (parent_id IS NULL OR parent_id <> id)
);
COMMENT ON TABLE system_dept IS '部门（组织架构树），软删';
COMMENT ON COLUMN system_dept.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN system_dept.parent_id IS '上级部门；NULL = 根部门';
COMMENT ON COLUMN system_dept.path IS '物化路径：祖先链（含自身）id，形如 /<根>/<...>/<自身>/；层级上限 10';
COMMENT ON COLUMN system_dept.name IS '部门名称，同一上级下未删除行内唯一';
COMMENT ON COLUMN system_dept.code IS '部门编码，可空，未删除行内唯一';
COMMENT ON COLUMN system_dept.sort IS '同级排序值，升序';
COMMENT ON COLUMN system_dept.status IS '状态：1=启用 0=停用';
COMMENT ON COLUMN system_dept.description IS '说明';
-- 根部门的 parent_id 为 NULL：以全零 UUID 归一后参与唯一性，根部门之间同样不可重名。
CREATE UNIQUE INDEX uk_system_dept_parent_name ON system_dept
    (COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid), name) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uk_system_dept_code ON system_dept (code) WHERE delete_time IS NULL AND code IS NOT NULL;
-- 子树前缀匹配（path LIKE '<prefix>%'）：varchar_pattern_ops 使前缀匹配在任意排序规则下走索引。
CREATE INDEX idx_system_dept_path ON system_dept (path varchar_pattern_ops) WHERE delete_time IS NULL;
CREATE INDEX idx_system_dept_parent ON system_dept (parent_id) WHERE delete_time IS NULL;

CREATE TABLE system_dept_leader (
    id          uuid        PRIMARY KEY,
    dept_id     uuid        NOT NULL REFERENCES system_dept (id),
    user_id     uuid        NOT NULL REFERENCES "system_user" (id),
    sort        integer     NOT NULL DEFAULT 0,
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL
);
COMMENT ON TABLE system_dept_leader IS '部门负责人（一个部门可多人，物理删除）；负责人不要求是本部门成员';
COMMENT ON COLUMN system_dept_leader.dept_id IS '部门 ID';
COMMENT ON COLUMN system_dept_leader.user_id IS '负责人用户 ID';
COMMENT ON COLUMN system_dept_leader.sort IS '负责人顺序，升序（依次审批等按此顺序）';
CREATE UNIQUE INDEX uk_system_dept_leader ON system_dept_leader (dept_id, user_id);
CREATE INDEX idx_system_dept_leader_user ON system_dept_leader (user_id);

CREATE TABLE system_user_dept (
    id          uuid        PRIMARY KEY,
    user_id     uuid        NOT NULL REFERENCES "system_user" (id),
    dept_id     uuid        NOT NULL REFERENCES system_dept (id),
    is_primary  boolean     NOT NULL DEFAULT false,
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL
);
COMMENT ON TABLE system_user_dept IS '用户所属部门（一个用户可属多个部门，其中至多一个主部门，物理删除）';
COMMENT ON COLUMN system_user_dept.user_id IS '用户 ID';
COMMENT ON COLUMN system_user_dept.dept_id IS '部门 ID';
COMMENT ON COLUMN system_user_dept.is_primary IS '是否主部门；每个用户至多一行为 true';
CREATE UNIQUE INDEX uk_system_user_dept ON system_user_dept (user_id, dept_id);
CREATE UNIQUE INDEX uk_system_user_dept_primary ON system_user_dept (user_id) WHERE is_primary;
CREATE INDEX idx_system_user_dept_dept ON system_user_dept (dept_id);
