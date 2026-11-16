-- 0059 工单域（ticket）：服务目录分类 / 工单模板 / 工单 / 评论 / 编号序列。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；分类与模板软删除，工单与评论不删除。
-- 工单的流转完全交给流程引擎（workflow 域）：工单只保存业务数据与由流程同步来的状态，
-- 两域之间只以 ID 关联（instance_id / business_id），不设外键、不跨域 JOIN。
-- 用户 / 部门引用同样不设外键（归 system 域）。

CREATE TABLE ticket_category (
    id          uuid         PRIMARY KEY,
    name        varchar(64)  NOT NULL,
    description varchar(255) NOT NULL DEFAULT '',
    sort        integer      NOT NULL DEFAULT 0,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL
);
COMMENT ON TABLE ticket_category IS '工单分类（服务目录的一级），软删';
COMMENT ON COLUMN ticket_category.name IS '分类名称，未删除行内唯一';
COMMENT ON COLUMN ticket_category.sort IS '排序值，升序';
CREATE UNIQUE INDEX uk_ticket_category_name ON ticket_category (name) WHERE delete_time IS NULL;

CREATE TABLE ticket_template (
    id            uuid         PRIMARY KEY,
    category_id   uuid         NOT NULL REFERENCES ticket_category (id),
    name          varchar(64)  NOT NULL,
    description   varchar(255) NOT NULL DEFAULT '',
    workflow_key  varchar(64)  NOT NULL,
    number_prefix varchar(8)   NOT NULL,
    form          jsonb        NOT NULL DEFAULT '[]'::jsonb,
    status        smallint     NOT NULL DEFAULT 1,
    sort          integer      NOT NULL DEFAULT 0,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    delete_time   timestamptz  NULL,
    create_user   uuid         NULL,
    update_user   uuid         NULL,
    delete_user   uuid         NULL,
    CONSTRAINT ck_ticket_template_status CHECK (status IN (0, 1))
);
COMMENT ON TABLE ticket_template IS '工单模板：表单 + 绑定的流程 + 编号前缀，软删';
COMMENT ON COLUMN ticket_template.name IS '模板名称，未删除行内唯一';
COMMENT ON COLUMN ticket_template.workflow_key IS '绑定的流程标识（workflow_definition.key）；提单时按它发起流程';
COMMENT ON COLUMN ticket_template.number_prefix IS '工单编号前缀（大写字母），编号形如 <前缀>-<yyyymmdd>-<4 位序号>';
COMMENT ON COLUMN ticket_template.form IS '表单字段定义（数组）：code / name / type / required / enumValues / rule / description';
COMMENT ON COLUMN ticket_template.status IS '状态：1=启用 0=停用（停用后不能提单，已有工单不受影响）';
CREATE UNIQUE INDEX uk_ticket_template_name ON ticket_template (name) WHERE delete_time IS NULL;
CREATE INDEX idx_ticket_template_category ON ticket_template (category_id) WHERE delete_time IS NULL;

CREATE TABLE ticket_order (
    id            uuid         PRIMARY KEY,
    number        varchar(32)  NOT NULL,
    template_id   uuid         NOT NULL REFERENCES ticket_template (id),
    template_name varchar(64)  NOT NULL,
    category_name varchar(64)  NOT NULL,
    title         varchar(255) NOT NULL,
    priority      smallint     NOT NULL DEFAULT 2,
    form          jsonb        NOT NULL,
    data          jsonb        NOT NULL DEFAULT '{}'::jsonb,
    status        smallint     NOT NULL DEFAULT 1,
    creator       uuid         NOT NULL,
    creator_dept  uuid         NULL,
    instance_id   uuid         NULL,
    request_id    varchar(64)  NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    end_time      timestamptz  NULL,
    CONSTRAINT ck_ticket_order_priority CHECK (priority IN (1, 2, 3, 4)),
    CONSTRAINT ck_ticket_order_status CHECK (status IN (1, 2, 3, 4, 5))
);
COMMENT ON TABLE ticket_order IS '工单';
COMMENT ON COLUMN ticket_order.number IS '工单编号，全局唯一';
COMMENT ON COLUMN ticket_order.template_name IS '提单时的模板名称（冗余，列表免关联）';
COMMENT ON COLUMN ticket_order.category_name IS '提单时的分类名称（冗余）';
COMMENT ON COLUMN ticket_order.priority IS '优先级：1=低 2=普通 3=高 4=紧急';
COMMENT ON COLUMN ticket_order.form IS '提单时的表单字段定义快照（模板之后怎么改都不影响已有工单的展示与校验）';
COMMENT ON COLUMN ticket_order.data IS '表单数据';
COMMENT ON COLUMN ticket_order.status IS '状态（由流程同步）：1=处理中 2=已完成 3=已拒绝 4=已撤回 5=已终止';
COMMENT ON COLUMN ticket_order.creator IS '提单人用户 ID';
COMMENT ON COLUMN ticket_order.creator_dept IS '提单部门 ID';
COMMENT ON COLUMN ticket_order.instance_id IS '流程实例 ID（workflow_instance.id，不设外键）';
COMMENT ON COLUMN ticket_order.request_id IS '提单请求的幂等键（客户端生成）；同一提单人重复提交返回首次创建的工单';
CREATE UNIQUE INDEX uk_ticket_order_number ON ticket_order (number);
CREATE UNIQUE INDEX uk_ticket_order_request ON ticket_order (creator, request_id) WHERE request_id IS NOT NULL;
CREATE INDEX idx_ticket_order_creator ON ticket_order (creator, create_time DESC);
CREATE INDEX idx_ticket_order_status ON ticket_order (status, create_time DESC);
CREATE INDEX idx_ticket_order_template ON ticket_order (template_id, create_time DESC);
-- 管理视图的缺省排序，以及标题 / 编号的包含匹配（pg_trgm 已在 0047 启用）。
CREATE INDEX idx_ticket_order_create_time ON ticket_order (create_time DESC, id DESC);
CREATE INDEX idx_ticket_order_title_trgm ON ticket_order USING gin (title gin_trgm_ops);
CREATE INDEX idx_ticket_order_number_trgm ON ticket_order USING gin (number gin_trgm_ops);

CREATE TABLE ticket_comment (
    id          uuid          PRIMARY KEY,
    ticket_id   uuid          NOT NULL REFERENCES ticket_order (id),
    user_id     uuid          NOT NULL,
    content     varchar(2000) NOT NULL,
    create_time timestamptz   NOT NULL DEFAULT now()
);
COMMENT ON TABLE ticket_comment IS '工单评论（只追加）';
CREATE INDEX idx_ticket_comment_ticket ON ticket_comment (ticket_id, id);
-- 单人评论限流：统计某人最近一分钟的评论数。
CREATE INDEX idx_ticket_comment_user ON ticket_comment (user_id, create_time);

CREATE TABLE ticket_sequence (
    prefix varchar(8) NOT NULL,
    day    date       NOT NULL,
    seq    integer    NOT NULL,
    PRIMARY KEY (prefix, day)
);
COMMENT ON TABLE ticket_sequence IS '工单编号序列：每个前缀每天一行（日期取数据库会话时区的当天，各实例一致），提单事务内自增（行锁串行，事务回滚不留空号）';
