-- 0060 流程引擎增强：异步作业队列（定时 / 超时 / 自动任务 / 子流程衔接）、包容网关的分支计数、
-- 加签 / 委派 / 代理的席位关系、子流程父子关系，以及代理设置、工作日历、Webhook 端点三张配置表。
-- 新增列均带常量缺省值（PostgreSQL 11+ 只改元数据，不重写表）。

ALTER TABLE workflow_instance
    ADD COLUMN parent_instance_id uuid        NULL,
    ADD COLUMN parent_node_id     varchar(64) NOT NULL DEFAULT '',
    ADD COLUMN parent_token_id    uuid        NULL,
    ADD COLUMN depth              smallint    NOT NULL DEFAULT 0;
COMMENT ON COLUMN workflow_instance.parent_instance_id IS '父流程实例 ID（由子流程节点发起时有值；不设外键，父子之间只经作业队列衔接）';
COMMENT ON COLUMN workflow_instance.parent_node_id IS '父流程里发起本实例的子流程节点 ID';
COMMENT ON COLUMN workflow_instance.depth IS '子流程嵌套深度：0 = 顶层';
CREATE INDEX idx_workflow_instance_parent ON workflow_instance (parent_instance_id) WHERE parent_instance_id IS NOT NULL;

ALTER TABLE workflow_token ADD COLUMN scope text NOT NULL DEFAULT '';
COMMENT ON COLUMN workflow_token.scope IS '包容网关的激活栈：每进入一次包容分叉压入「激活 ID:被激活的分支数」，以 | 分隔；汇聚据栈顶等齐后弹出';

ALTER TABLE workflow_task ADD COLUMN due_time timestamptz NULL;
COMMENT ON COLUMN workflow_task.due_time IS '处理时限（节点配置了超时才有值）';

ALTER TABLE workflow_task_assignee
    ADD COLUMN origin    varchar(16) NOT NULL DEFAULT '',
    ADD COLUMN owner_id  uuid        NULL,
    ADD COLUMN on_behalf uuid        NULL;
COMMENT ON COLUMN workflow_task_assignee.origin IS '席位来源：空 = 节点规则解析；before / after = 前 / 后加签；delegate = 委派';
COMMENT ON COLUMN workflow_task_assignee.owner_id IS '加签 / 委派出本席位的那个席位 ID（本席位处理完后由它接续）';
COMMENT ON COLUMN workflow_task_assignee.on_behalf IS '被代理人用户 ID：本席位是代理人替他处理（代理设置生效时写入）';
ALTER TABLE workflow_task_assignee DROP CONSTRAINT ck_workflow_task_assignee_status;
ALTER TABLE workflow_task_assignee ADD CONSTRAINT ck_workflow_task_assignee_status CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8, 9));
COMMENT ON COLUMN workflow_task_assignee.status IS '1=待处理 2=排队中 3=已同意 4=已拒绝 5=已取消 6=已转办 7=已退回 8=已抄送 9=等待加签 / 委派的人处理';
-- 被代理人也算实例的参与者（可查看）。
CREATE INDEX idx_workflow_task_assignee_behalf ON workflow_task_assignee (instance_id, on_behalf) WHERE on_behalf IS NOT NULL;

CREATE TABLE workflow_job (
    id          uuid          PRIMARY KEY,
    instance_id uuid          NOT NULL REFERENCES workflow_instance (id),
    kind        varchar(16)   NOT NULL,
    node_id     varchar(64)   NOT NULL DEFAULT '',
    token_id    uuid          NULL,
    task_id     uuid          NULL,
    payload     jsonb         NOT NULL DEFAULT '{}'::jsonb,
    status      smallint      NOT NULL DEFAULT 1,
    due_time    timestamptz   NOT NULL,
    attempts    integer       NOT NULL DEFAULT 0,
    last_error  varchar(500)  NOT NULL DEFAULT '',
    lock_until  timestamptz   NULL,
    create_time timestamptz   NOT NULL DEFAULT now(),
    update_time timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT ck_workflow_job_kind CHECK (kind IN ('timer', 'timeout', 'remind', 'service', 'resume', 'stop')),
    CONSTRAINT ck_workflow_job_status CHECK (status IN (1, 2, 3, 4))
);
COMMENT ON TABLE workflow_job IS '流程异步作业队列：到期由后台执行器领取（SKIP LOCKED + 租约，多实例安全），每个作业只锁自己所属的实例';
COMMENT ON COLUMN workflow_job.kind IS 'timer=定时节点到期 timeout=任务超时 remind=超时前提醒 service=自动任务 resume=子流程结束后唤醒父流程 stop=父流程结束后终止子流程';
COMMENT ON COLUMN workflow_job.status IS '1=待执行 2=已完成 3=已取消 4=死信（重试耗尽，待人工重试或跳过）';
COMMENT ON COLUMN workflow_job.lock_until IS '租约到期时间：领取后在此之前其他执行器不会重复领取；执行器崩溃后租约过期自动可被再次领取';
CREATE INDEX idx_workflow_job_due ON workflow_job (due_time) WHERE status = 1;
CREATE INDEX idx_workflow_job_instance ON workflow_job (instance_id, status);
CREATE INDEX idx_workflow_job_task ON workflow_job (task_id) WHERE status = 1 AND task_id IS NOT NULL;
-- 死信数量（指标）。
CREATE INDEX idx_workflow_job_dead ON workflow_job (update_time) WHERE status = 4;
-- 清理早已了结的作业。
CREATE INDEX idx_workflow_job_settled ON workflow_job (update_time) WHERE status IN (2, 3);

CREATE TABLE workflow_delegation (
    id          uuid         PRIMARY KEY,
    user_id     uuid         NOT NULL,
    delegate_id uuid         NOT NULL,
    start_time  timestamptz  NOT NULL,
    end_time    timestamptz  NOT NULL,
    reason      varchar(255) NOT NULL DEFAULT '',
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL,
    CONSTRAINT ck_workflow_delegation_range CHECK (end_time > start_time),
    CONSTRAINT ck_workflow_delegation_self CHECK (delegate_id <> user_id)
);
COMMENT ON TABLE workflow_delegation IS '代理设置：用户在一段时间内把新到达的待办交给代理人处理（只转一层，不连环代理），软删';
CREATE INDEX idx_workflow_delegation_user ON workflow_delegation (user_id, end_time) WHERE delete_time IS NULL;

CREATE TABLE workflow_calendar (
    id          smallint    PRIMARY KEY,
    config      jsonb       NOT NULL,
    update_time timestamptz NOT NULL DEFAULT now(),
    update_user uuid        NULL,
    CONSTRAINT ck_workflow_calendar_single CHECK (id = 1)
);
COMMENT ON TABLE workflow_calendar IS '工作日历（全局唯一一行）：时区、每周工作日、每日工作时段、节假日与调休上班日；按工作时间计时的时限据此推算';
INSERT INTO workflow_calendar (id, config) VALUES (1,
    '{"timezone":"Asia/Shanghai","weekdays":[1,2,3,4,5],"segments":[{"start":"09:00","end":"12:00"},{"start":"13:30","end":"18:00"}],"holidays":[],"workdays":[]}'::jsonb);

CREATE TABLE workflow_webhook (
    id          uuid          PRIMARY KEY,
    name        varchar(64)   NOT NULL,
    url         varchar(1024) NOT NULL,
    secret      bytea         NULL,
    status      smallint      NOT NULL DEFAULT 1,
    description varchar(255)  NOT NULL DEFAULT '',
    create_time timestamptz   NOT NULL DEFAULT now(),
    update_time timestamptz   NOT NULL DEFAULT now(),
    delete_time timestamptz   NULL,
    create_user uuid          NULL,
    update_user uuid          NULL,
    delete_user uuid          NULL,
    CONSTRAINT ck_workflow_webhook_status CHECK (status IN (0, 1))
);
COMMENT ON TABLE workflow_webhook IS '自动任务可调用的 Webhook 端点（地址与签名密钥只在这里维护，流程图里只引用 ID——流程图对参与者可见，不能放密钥），软删';
COMMENT ON COLUMN workflow_webhook.secret IS '签名密钥密文（AES-256-GCM）；空 = 不签名';
CREATE UNIQUE INDEX uk_workflow_webhook_name ON workflow_webhook (name) WHERE delete_time IS NULL;
