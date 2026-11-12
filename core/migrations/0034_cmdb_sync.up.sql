-- 0034 CMDB 同步控制面：采集源 / 任务队列 / 运行记录 / 暂存区；资源表补充消失时间。
-- 控制面（core）只写采集源与手动任务；数据面（backend/cmdb）领取任务、写暂存与对账结果。

ALTER TABLE cmdb_ci ADD COLUMN missing_since timestamptz NULL;
COMMENT ON COLUMN cmdb_ci.missing_since IS '同步对账首次未发现该资源的时间；宽限期后退役；再次发现清空';
CREATE INDEX idx_cmdb_ci_source ON cmdb_ci (source, model_code) WHERE delete_time IS NULL AND source <> 'manual';

CREATE TABLE cmdb_sync_source (
    id                   uuid         PRIMARY KEY,
    name                 varchar(64)  NOT NULL,
    provider             varchar(16)  NOT NULL,
    config               jsonb        NOT NULL DEFAULT '{}',
    credential           bytea        NULL,
    env                  varchar(32)  NOT NULL DEFAULT '',
    interval_seconds     integer      NOT NULL DEFAULT 3600,
    grace_period_seconds integer      NOT NULL DEFAULT 86400,
    enabled              boolean      NOT NULL DEFAULT true,
    next_run_time        timestamptz  NULL,
    last_run_id          uuid         NULL,
    last_run_time        timestamptz  NULL,
    last_status          varchar(16)  NOT NULL DEFAULT 'never',
    version              bigint       NOT NULL DEFAULT 1,
    create_user          uuid         NULL,
    update_user          uuid         NULL,
    create_time          timestamptz  NOT NULL DEFAULT now(),
    update_time          timestamptz  NOT NULL DEFAULT now(),
    delete_time          timestamptz  NULL
);
COMMENT ON TABLE cmdb_sync_source IS 'CMDB 采集源：云账号 / 虚拟化平台接入配置（控制面写，数据面读）';
COMMENT ON COLUMN cmdb_sync_source.provider IS 'aliyun / vmware（kit/cmdbcontract）';
COMMENT ON COLUMN cmdb_sync_source.config IS '提供方配置 jsonb（不含凭据），结构见 kit/cmdbcontract';
COMMENT ON COLUMN cmdb_sync_source.credential IS '凭据密文 AES-256-GCM（nonce||ct），只写不读；NULL = 未配置';
COMMENT ON COLUMN cmdb_sync_source.env IS '写入资源的缺省环境标识';
COMMENT ON COLUMN cmdb_sync_source.interval_seconds IS '周期（秒）；next_run_time 由数据面调度器推进';
COMMENT ON COLUMN cmdb_sync_source.grace_period_seconds IS '资源消失后到退役的宽限期（秒），0 = 立即退役';
COMMENT ON COLUMN cmdb_sync_source.last_status IS 'never / running / succeeded / failed';
CREATE UNIQUE INDEX uq_cmdb_sync_source_name ON cmdb_sync_source (name) WHERE delete_time IS NULL;
CREATE INDEX idx_cmdb_sync_source_due ON cmdb_sync_source (next_run_time) WHERE delete_time IS NULL AND enabled;

CREATE TABLE cmdb_sync_task (
    id          uuid        PRIMARY KEY,
    source_id   uuid        NOT NULL REFERENCES cmdb_sync_source (id),
    trigger     varchar(16) NOT NULL,
    status      varchar(16) NOT NULL DEFAULT 'pending',
    attempts    integer     NOT NULL DEFAULT 0,
    claimed_by  varchar(128) NOT NULL DEFAULT '',
    lease_until timestamptz NULL,
    run_id      uuid        NULL,
    error       varchar(1024) NOT NULL DEFAULT '',
    create_user uuid        NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    start_time  timestamptz NULL,
    finish_time timestamptz NULL
);
COMMENT ON TABLE cmdb_sync_task IS 'CMDB 同步任务队列：手动触发（控制面）/ 定时（数据面调度器）；数据面以租约领取';
COMMENT ON COLUMN cmdb_sync_task.status IS 'pending / running / succeeded / failed';
COMMENT ON COLUMN cmdb_sync_task.claimed_by IS '领取实例的属主令牌（实例 ID + 随机串）；续约与完成均以此围栏';
COMMENT ON COLUMN cmdb_sync_task.lease_until IS '租约到期时间；过期的 running 任务可被其它实例重领（attempts+1）';
-- 同一采集源同时最多一个未完成任务（手动触发与调度器都依赖此约束实现幂等）。
CREATE UNIQUE INDEX uq_cmdb_sync_task_active ON cmdb_sync_task (source_id) WHERE status IN ('pending', 'running');
CREATE INDEX idx_cmdb_sync_task_queue ON cmdb_sync_task (create_time) WHERE status IN ('pending', 'running');

CREATE TABLE cmdb_sync_run (
    id           uuid         PRIMARY KEY,
    source_id    uuid         NOT NULL REFERENCES cmdb_sync_source (id),
    task_id      uuid         NOT NULL,
    status       varchar(16)  NOT NULL DEFAULT 'running',
    discovered   integer      NOT NULL DEFAULT 0,
    created      integer      NOT NULL DEFAULT 0,
    updated      integer      NOT NULL DEFAULT 0,
    unchanged    integer      NOT NULL DEFAULT 0,
    disappeared  integer      NOT NULL DEFAULT 0,
    retired      integer      NOT NULL DEFAULT 0,
    failed       integer      NOT NULL DEFAULT 0,
    error        varchar(1024) NOT NULL DEFAULT '',
    errors       jsonb        NOT NULL DEFAULT '[]',
    start_time   timestamptz  NOT NULL DEFAULT now(),
    finish_time  timestamptz  NULL
);
COMMENT ON TABLE cmdb_sync_run IS 'CMDB 同步运行记录：每次任务执行一行，含三态对账统计与错误样本';
COMMENT ON COLUMN cmdb_sync_run.errors IS '[{model_code, external_id, name, reason}] 最多 50 条样本';
CREATE INDEX idx_cmdb_sync_run_source ON cmdb_sync_run (source_id, start_time DESC);

CREATE TABLE cmdb_sync_staging (
    run_id      uuid         NOT NULL,
    model_code  varchar(64)  NOT NULL,
    external_id varchar(128) NOT NULL,
    name        varchar(128) NOT NULL,
    attrs       jsonb        NOT NULL DEFAULT '{}',
    unique_key  varchar(512) NOT NULL DEFAULT '',
    relations   jsonb        NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, model_code, external_id)
);
COMMENT ON TABLE cmdb_sync_staging IS 'CMDB 同步暂存区：采集结果按运行隔离，对账后清理';

-- DB 角色：数据面登录账号 GRANT 为 cmdb_sync_role 成员（控制面沿用 core 账号）。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cmdb_sync_role') THEN
        CREATE ROLE cmdb_sync_role NOLOGIN;
    END IF;
END
$$;
GRANT SELECT ON cmdb_model, cmdb_model_field, cmdb_relation_type TO cmdb_sync_role;
GRANT SELECT, INSERT, UPDATE ON cmdb_ci, cmdb_relation, cmdb_sync_source, cmdb_sync_task, cmdb_sync_run TO cmdb_sync_role;
GRANT DELETE ON cmdb_relation TO cmdb_sync_role;
GRANT INSERT ON cmdb_ci_history TO cmdb_sync_role;
GRANT SELECT, INSERT, DELETE ON cmdb_sync_staging TO cmdb_sync_role;
