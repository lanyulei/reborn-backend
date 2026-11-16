-- 0058 流程引擎域（workflow）：流程定义 / 版本 / 实例 / 执行位置 / 任务 / 处理人 / 历史。
-- 约定同 0001：id 为应用侧生成的 UUIDv7；定义软删除，运行数据不删除。
-- 引擎与业务无关：实例只记 business_type + business_id + 变量（jsonb）。
-- 用户 / 部门引用不设外键：归 system 域，用户离职、部门调整后仍需保留流转记录。
-- 并发口径：对一个实例的任何写命令都先 SELECT ... FOR UPDATE 锁住实例行，
-- 实例下的执行位置 / 任务 / 处理人只在持有该行锁时改写。

CREATE TABLE workflow_definition (
    id                   uuid         PRIMARY KEY,
    key                  varchar(64)  NOT NULL,
    name                 varchar(64)  NOT NULL,
    category             varchar(64)  NOT NULL DEFAULT '',
    description          varchar(255) NOT NULL DEFAULT '',
    status               smallint     NOT NULL DEFAULT 1,
    draft                jsonb        NOT NULL,
    revision             integer      NOT NULL DEFAULT 1,
    published_version    integer      NOT NULL DEFAULT 0,
    published_version_id uuid         NULL,
    create_time          timestamptz  NOT NULL DEFAULT now(),
    update_time          timestamptz  NOT NULL DEFAULT now(),
    delete_time          timestamptz  NULL,
    create_user          uuid         NULL,
    update_user          uuid         NULL,
    delete_user          uuid         NULL,
    CONSTRAINT ck_workflow_definition_status CHECK (status IN (0, 1))
);
COMMENT ON TABLE workflow_definition IS '流程定义：草稿可反复保存，发布生成不可变版本；软删';
COMMENT ON COLUMN workflow_definition.key IS '流程标识（业务域按它发起流程），未删除行内唯一，创建后不可改';
COMMENT ON COLUMN workflow_definition.name IS '流程名称';
COMMENT ON COLUMN workflow_definition.category IS '分类（自由文本，仅用于筛选）';
COMMENT ON COLUMN workflow_definition.status IS '状态：1=启用 0=停用（停用后不能发起新实例，在途实例不受影响）';
COMMENT ON COLUMN workflow_definition.draft IS '草稿（流程图 JSON，可以是未完成状态）';
COMMENT ON COLUMN workflow_definition.revision IS '草稿修订号（乐观锁）：每次保存草稿 +1';
COMMENT ON COLUMN workflow_definition.published_version IS '当前发布的版本号；0 = 从未发布';
COMMENT ON COLUMN workflow_definition.published_version_id IS '当前发布版本的 ID';
CREATE UNIQUE INDEX uk_workflow_definition_key ON workflow_definition (key) WHERE delete_time IS NULL;

CREATE TABLE workflow_definition_version (
    id            uuid        PRIMARY KEY,
    definition_id uuid        NOT NULL REFERENCES workflow_definition (id),
    version       integer     NOT NULL,
    graph         jsonb       NOT NULL,
    create_time   timestamptz NOT NULL DEFAULT now(),
    create_user   uuid        NULL
);
COMMENT ON TABLE workflow_definition_version IS '流程定义的已发布版本（不可变）：实例绑定发起时的版本，改定义不影响在途实例';
COMMENT ON COLUMN workflow_definition_version.version IS '版本号，同一定义内从 1 递增';
COMMENT ON COLUMN workflow_definition_version.graph IS '发布时通过校验的流程图 JSON';
COMMENT ON COLUMN workflow_definition_version.create_user IS '发布人';
CREATE UNIQUE INDEX uk_workflow_definition_version ON workflow_definition_version (definition_id, version);

CREATE TABLE workflow_instance (
    id              uuid         PRIMARY KEY,
    definition_id   uuid         NOT NULL REFERENCES workflow_definition (id),
    version_id      uuid         NOT NULL REFERENCES workflow_definition_version (id),
    definition_key  varchar(64)  NOT NULL,
    definition_name varchar(64)  NOT NULL,
    version         integer      NOT NULL,
    business_type   varchar(64)  NOT NULL DEFAULT '',
    business_id     varchar(64)  NOT NULL DEFAULT '',
    title           varchar(255) NOT NULL,
    initiator       uuid         NOT NULL,
    initiator_dept  uuid         NULL,
    status          smallint     NOT NULL DEFAULT 1,
    variables       jsonb        NOT NULL DEFAULT '{}'::jsonb,
    request_id      varchar(64)  NULL,
    start_time      timestamptz  NOT NULL DEFAULT now(),
    end_time        timestamptz  NULL,
    update_time     timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_workflow_instance_status CHECK (status IN (1, 2, 3, 4, 5))
);
COMMENT ON TABLE workflow_instance IS '流程实例';
COMMENT ON COLUMN workflow_instance.definition_key IS '发起时的流程标识（冗余，列表免关联）';
COMMENT ON COLUMN workflow_instance.definition_name IS '发起时的流程名称（冗余）';
COMMENT ON COLUMN workflow_instance.business_type IS '业务类型（如 ticket）；空 = 独立发起，无业务对象';
COMMENT ON COLUMN workflow_instance.business_id IS '业务对象 ID；同一业务对象同一时刻至多一个进行中的流程';
COMMENT ON COLUMN workflow_instance.request_id IS '发起请求的幂等键（客户端生成）；同一发起人重复提交返回首次发起的实例';
COMMENT ON COLUMN workflow_instance.initiator IS '发起人用户 ID';
COMMENT ON COLUMN workflow_instance.initiator_dept IS '发起部门 ID（发起时固化，之后调部门不影响在途流程）';
COMMENT ON COLUMN workflow_instance.status IS '状态：1=进行中 2=已通过 3=已拒绝 4=已撤回 5=已终止';
COMMENT ON COLUMN workflow_instance.variables IS '流程变量（表单数据），条件表达式以 form 引用';
-- 同一业务对象至多一个进行中的流程；结束（通过 / 拒绝 / 撤回 / 终止）后可再次发起。
CREATE UNIQUE INDEX uk_workflow_instance_business ON workflow_instance (business_type, business_id)
    WHERE business_type <> '' AND status = 1;
CREATE UNIQUE INDEX uk_workflow_instance_request ON workflow_instance (initiator, request_id) WHERE request_id IS NOT NULL;
CREATE INDEX idx_workflow_instance_initiator ON workflow_instance (initiator, start_time DESC);
CREATE INDEX idx_workflow_instance_status ON workflow_instance (status, start_time DESC);
CREATE INDEX idx_workflow_instance_definition ON workflow_instance (definition_id, start_time DESC);

CREATE TABLE workflow_token (
    id          uuid        PRIMARY KEY,
    instance_id uuid        NOT NULL REFERENCES workflow_instance (id),
    node_id     varchar(64) NOT NULL,
    status      smallint    NOT NULL,
    CONSTRAINT ck_workflow_token_status CHECK (status IN (1, 2, 3))
);
COMMENT ON TABLE workflow_token IS '执行位置：并行分支各持一个';
COMMENT ON COLUMN workflow_token.node_id IS '所在节点 ID';
COMMENT ON COLUMN workflow_token.status IS '状态：1=活动 2=在并行汇聚处等待 3=已消耗';
CREATE INDEX idx_workflow_token_instance ON workflow_token (instance_id);

CREATE TABLE workflow_task (
    id          uuid         PRIMARY KEY,
    instance_id uuid         NOT NULL REFERENCES workflow_instance (id),
    token_id    uuid         NOT NULL,
    node_id     varchar(64)  NOT NULL,
    node_name   varchar(128) NOT NULL DEFAULT '',
    kind        varchar(16)  NOT NULL,
    mode        varchar(16)  NOT NULL DEFAULT '',
    status      smallint     NOT NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    end_time    timestamptz  NULL,
    CONSTRAINT ck_workflow_task_kind CHECK (kind IN ('approve', 'cc')),
    CONSTRAINT ck_workflow_task_status CHECK (status IN (1, 2, 3, 4, 5, 6))
);
COMMENT ON TABLE workflow_task IS '任务：一个节点的一次到达产生一个任务（人工任务或抄送）';
COMMENT ON COLUMN workflow_task.kind IS '类型：approve=人工任务 cc=抄送';
COMMENT ON COLUMN workflow_task.mode IS '多人模式：any=或签 all=会签 sequential=依次审批';
COMMENT ON COLUMN workflow_task.status IS '状态：1=待处理 2=已同意 3=已拒绝 4=已取消 5=已退回 6=已抄送';
CREATE INDEX idx_workflow_task_instance ON workflow_task (instance_id, create_time);

CREATE TABLE workflow_task_assignee (
    id          uuid          PRIMARY KEY,
    task_id     uuid          NOT NULL REFERENCES workflow_task (id),
    instance_id uuid          NOT NULL REFERENCES workflow_instance (id),
    user_id     uuid          NOT NULL,
    seq         integer       NOT NULL,
    status      smallint      NOT NULL,
    comment     varchar(1000) NOT NULL DEFAULT '',
    create_time timestamptz   NOT NULL DEFAULT now(),
    action_time timestamptz   NULL,
    CONSTRAINT ck_workflow_task_assignee_status CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8))
);
COMMENT ON TABLE workflow_task_assignee IS '任务处理人（或抄送接收人）：待办 / 已办 / 抄送列表的查询入口';
COMMENT ON COLUMN workflow_task_assignee.seq IS '顺序（依次审批按此顺序）';
COMMENT ON COLUMN workflow_task_assignee.status IS '状态：1=待处理 2=排队中 3=已同意 4=已拒绝 5=已取消 6=已转办 7=已退回 8=已抄送';
COMMENT ON COLUMN workflow_task_assignee.comment IS '处理意见';
-- 待办 / 已办 / 抄送：按用户 + 状态取，时间倒序。
CREATE INDEX idx_workflow_task_assignee_user ON workflow_task_assignee (user_id, status, create_time DESC);
-- 已办：按处理时间倒序。
CREATE INDEX idx_workflow_task_assignee_done ON workflow_task_assignee (user_id, action_time DESC) WHERE action_time IS NOT NULL;
CREATE INDEX idx_workflow_task_assignee_task ON workflow_task_assignee (task_id);
CREATE INDEX idx_workflow_task_assignee_instance ON workflow_task_assignee (instance_id, user_id);

CREATE TABLE workflow_history (
    id          uuid          PRIMARY KEY,
    instance_id uuid          NOT NULL REFERENCES workflow_instance (id),
    node_id     varchar(64)   NOT NULL DEFAULT '',
    node_name   varchar(128)  NOT NULL DEFAULT '',
    task_id     uuid          NULL,
    action      varchar(32)   NOT NULL,
    operator    uuid          NULL,
    comment     varchar(1000) NOT NULL DEFAULT '',
    detail      jsonb         NOT NULL DEFAULT '{}'::jsonb,
    create_time timestamptz   NOT NULL DEFAULT now()
);
COMMENT ON TABLE workflow_history IS '流转历史（只追加，不更新不删除）';
COMMENT ON COLUMN workflow_history.action IS '动作：start / approve / reject / return / transfer / reassign / skip / cc / cancel / terminate / complete';
COMMENT ON COLUMN workflow_history.operator IS '操作人；系统动作（自动跳过、抄送、结束）为空';
COMMENT ON COLUMN workflow_history.detail IS '动作明细（如退回目标、转办对象）';
CREATE INDEX idx_workflow_history_instance ON workflow_history (instance_id, id);
