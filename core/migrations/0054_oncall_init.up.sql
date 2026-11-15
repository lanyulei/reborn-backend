-- 0054 值班域（oncall）：分组 / 成员 / 值班表 / 轮转名单 / 班次 / 公共地址 / 变更记录。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；业务表软删除，关联表与班次物理删除；
-- 时间一律 timestamptz（轮转规则按值班表自己的时区在应用层计算）。
-- 用户引用（user_id 等）不设外键：用户归 system 域，停用/离职后仍需保留历史班次。

-- 班次排他约束需要对 uuid / smallint 建 GiST 等值算子。
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE oncall_group (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    description varchar(255) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL
);
COMMENT ON TABLE oncall_group IS '值班分组：一个值班团队，包含成员与若干值班表，软删';
COMMENT ON COLUMN oncall_group.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN oncall_group.name IS '分组名称，未删除行内唯一';
COMMENT ON COLUMN oncall_group.description IS '说明';
CREATE UNIQUE INDEX uk_oncall_group_name ON oncall_group (name) WHERE delete_time IS NULL;

CREATE TABLE oncall_group_member (
    id          uuid        PRIMARY KEY,
    group_id    uuid        NOT NULL REFERENCES oncall_group (id),
    user_id     uuid        NOT NULL,
    role        smallint    NOT NULL DEFAULT 2,
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL,
    CONSTRAINT ck_oncall_group_member_role CHECK (role IN (1, 2))
);
COMMENT ON TABLE oncall_group_member IS '值班分组成员（物理删除）';
COMMENT ON COLUMN oncall_group_member.user_id IS '用户 ID，引用 "system_user".id（不设外键）';
COMMENT ON COLUMN oncall_group_member.role IS '身份：1=管理员 2=成员';
CREATE UNIQUE INDEX uk_oncall_group_member ON oncall_group_member (group_id, user_id);
CREATE INDEX idx_oncall_group_member_user ON oncall_group_member (user_id);

CREATE TABLE oncall_schedule (
    id                uuid         PRIMARY KEY,
    group_id          uuid         NOT NULL REFERENCES oncall_group (id),
    name              varchar(64)  NOT NULL,
    description       varchar(255) NOT NULL DEFAULT '',
    timezone          varchar(64)  NOT NULL,
    rotation_unit     smallint     NOT NULL,
    rotation_interval smallint     NOT NULL,
    anchor_time       timestamptz  NOT NULL,
    anchor_offset     smallint     NOT NULL DEFAULT 0,
    backup_enabled    boolean      NOT NULL DEFAULT true,
    horizon_days      smallint     NOT NULL DEFAULT 60,
    remind_minutes    integer[]    NOT NULL DEFAULT '{}',
    digest_time       time         NULL,
    last_digest_date  date         NULL,
    status            smallint     NOT NULL DEFAULT 1,
    generated_until   timestamptz  NULL,
    create_time       timestamptz  NOT NULL DEFAULT now(),
    update_time       timestamptz  NOT NULL DEFAULT now(),
    delete_time       timestamptz  NULL,
    create_user       uuid         NULL,
    update_user       uuid         NULL,
    delete_user       uuid         NULL,
    CONSTRAINT ck_oncall_schedule_unit CHECK (rotation_unit IN (1, 2)),
    CONSTRAINT ck_oncall_schedule_interval CHECK (rotation_interval BETWEEN 1 AND 30),
    CONSTRAINT ck_oncall_schedule_status CHECK (status IN (1, 2))
);
COMMENT ON TABLE oncall_schedule IS '值班表：一套全天连续值班的轮转规则，软删';
COMMENT ON COLUMN oncall_schedule.name IS '名称，同一分组未删除行内唯一';
COMMENT ON COLUMN oncall_schedule.timezone IS 'IANA 时区名；轮转边界按该时区本地时间逐日累加（夏令时不漂移）';
COMMENT ON COLUMN oncall_schedule.rotation_unit IS '轮转周期单位：1=天 2=周';
COMMENT ON COLUMN oncall_schedule.rotation_interval IS '轮转周期间隔（按天 1-30，按周 1-4，应用层校验）';
COMMENT ON COLUMN oncall_schedule.anchor_time IS '轮转起点（首次交接时间）；排班只覆盖该时刻之后';
COMMENT ON COLUMN oncall_schedule.anchor_offset IS '轮转起点对应轮转名单中的位置（从 0 起）';
COMMENT ON COLUMN oncall_schedule.backup_enabled IS '是否设备值班（主值班在名单中的下一位）';
COMMENT ON COLUMN oncall_schedule.horizon_days IS '提前排班天数';
COMMENT ON COLUMN oncall_schedule.remind_minutes IS '上班提醒提前量（分钟），至多两个；0=交接时刻通知';
COMMENT ON COLUMN oncall_schedule.digest_time IS '每日播报时刻（值班表时区的本地时刻）；NULL=不播报';
COMMENT ON COLUMN oncall_schedule.last_digest_date IS '最近一次播报的本地日期（防重）';
COMMENT ON COLUMN oncall_schedule.status IS '状态：1=启用 2=停用';
COMMENT ON COLUMN oncall_schedule.generated_until IS '班次已生成到的时间（后台任务据此补排）';
CREATE UNIQUE INDEX uk_oncall_schedule_name ON oncall_schedule (group_id, name) WHERE delete_time IS NULL;
CREATE INDEX idx_oncall_schedule_extend ON oncall_schedule (generated_until) WHERE delete_time IS NULL AND status = 1;

CREATE TABLE oncall_schedule_member (
    id          uuid        PRIMARY KEY,
    schedule_id uuid        NOT NULL REFERENCES oncall_schedule (id),
    user_id     uuid        NOT NULL,
    sort        smallint    NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    create_user uuid        NULL
);
COMMENT ON TABLE oncall_schedule_member IS '值班表轮转名单（物理删除，整体替换）';
COMMENT ON COLUMN oncall_schedule_member.sort IS '轮转顺序，从 0 起';
CREATE UNIQUE INDEX uk_oncall_schedule_member ON oncall_schedule_member (schedule_id, user_id);
CREATE INDEX idx_oncall_schedule_member_user ON oncall_schedule_member (user_id);

CREATE TABLE oncall_shift (
    id             uuid         PRIMARY KEY,
    schedule_id    uuid         NOT NULL REFERENCES oncall_schedule (id),
    group_id       uuid         NOT NULL,
    role           smallint     NOT NULL,
    user_id        uuid         NOT NULL,
    start_time     timestamptz  NOT NULL,
    end_time       timestamptz  NOT NULL,
    source         smallint     NOT NULL DEFAULT 1,
    origin_user_id uuid         NULL,
    remark         varchar(255) NOT NULL DEFAULT '',
    reminded_mask  smallint     NOT NULL DEFAULT 0,
    create_time    timestamptz  NOT NULL DEFAULT now(),
    update_time    timestamptz  NOT NULL DEFAULT now(),
    create_user    uuid         NULL,
    update_user    uuid         NULL,
    CONSTRAINT ck_oncall_shift_role CHECK (role IN (1, 2)),
    CONSTRAINT ck_oncall_shift_source CHECK (source IN (1, 2)),
    CONSTRAINT ck_oncall_shift_range CHECK (end_time > start_time),
    -- 同一值班表同一角色的班次时间不重叠：并发排班/替班由数据库兜底，不只靠应用判断。
    CONSTRAINT ex_oncall_shift_overlap EXCLUDE USING gist (
        schedule_id WITH =, role WITH =, tstzrange(start_time, end_time) WITH &&)
);
COMMENT ON TABLE oncall_shift IS '班次：某人在某段时间担任某个角色（物理删除，只会删除尚未开始的自动班次）';
COMMENT ON COLUMN oncall_shift.group_id IS '所属分组（冗余列，供日历按分组查询）';
COMMENT ON COLUMN oncall_shift.role IS '角色：1=主值班 2=备值班';
COMMENT ON COLUMN oncall_shift.start_time IS '开始时间（含）';
COMMENT ON COLUMN oncall_shift.end_time IS '结束时间（不含）';
COMMENT ON COLUMN oncall_shift.source IS '来源：1=自动排班 2=替班（重排不覆盖）';
COMMENT ON COLUMN oncall_shift.origin_user_id IS '替班时的原值班人；填补空档的替班为 NULL';
COMMENT ON COLUMN oncall_shift.remark IS '替班原因';
COMMENT ON COLUMN oncall_shift.reminded_mask IS '已发送的上班提醒，按 remind_minutes 下标置位';
CREATE INDEX idx_oncall_shift_start ON oncall_shift (start_time);
CREATE INDEX idx_oncall_shift_end ON oncall_shift (end_time);

CREATE TABLE oncall_share_link (
    id               uuid         PRIMARY KEY,
    scope_type       smallint     NOT NULL,
    scope_id         uuid         NOT NULL,
    name             varchar(64)  NOT NULL,
    token_hash       bytea        NOT NULL,
    show_phone       boolean      NOT NULL DEFAULT false,
    expire_time      timestamptz  NULL,
    last_access_time timestamptz  NULL,
    create_time      timestamptz  NOT NULL DEFAULT now(),
    update_time      timestamptz  NOT NULL DEFAULT now(),
    delete_time      timestamptz  NULL,
    create_user      uuid         NULL,
    update_user      uuid         NULL,
    delete_user      uuid         NULL,
    CONSTRAINT ck_oncall_share_link_scope CHECK (scope_type IN (1, 2, 3))
);
COMMENT ON TABLE oncall_share_link IS '公共值班日历地址：免登录只读日历与 iCalendar 订阅，软删即作废';
COMMENT ON COLUMN oncall_share_link.scope_type IS '范围类型：1=分组 2=值班表 3=个人';
COMMENT ON COLUMN oncall_share_link.scope_id IS '范围对象：分组 / 值班表 / 用户 ID';
COMMENT ON COLUMN oncall_share_link.token_hash IS '访问令牌的 SHA-256：匿名访问按它等值查找';
COMMENT ON COLUMN oncall_share_link.show_phone IS '是否向匿名访问者显示值班人手机号';
COMMENT ON COLUMN oncall_share_link.expire_time IS '过期时间；NULL=长期有效';
COMMENT ON COLUMN oncall_share_link.last_access_time IS '最近访问时间（至多每小时更新一次）';
CREATE UNIQUE INDEX uk_oncall_share_link_token ON oncall_share_link (token_hash);
CREATE INDEX idx_oncall_share_link_scope ON oncall_share_link (scope_type, scope_id) WHERE delete_time IS NULL;

CREATE TABLE oncall_change_log (
    id          uuid         PRIMARY KEY,
    group_id    uuid         NOT NULL,
    schedule_id uuid         NULL,
    action      varchar(32)  NOT NULL,
    summary     varchar(255) NOT NULL,
    detail      jsonb        NOT NULL DEFAULT '{}',
    create_time timestamptz  NOT NULL DEFAULT now(),
    create_user uuid         NULL
);
COMMENT ON TABLE oncall_change_log IS '值班变更记录（append-only 审计）：成员、值班表、替班、公共地址的变更';
COMMENT ON COLUMN oncall_change_log.action IS '操作码，如 schedule.update / shift.override';
COMMENT ON COLUMN oncall_change_log.summary IS '一句话描述';
COMMENT ON COLUMN oncall_change_log.detail IS '变更前后内容';
COMMENT ON COLUMN oncall_change_log.create_user IS '操作人';
CREATE INDEX idx_oncall_change_log_group ON oncall_change_log (group_id, create_time DESC);
