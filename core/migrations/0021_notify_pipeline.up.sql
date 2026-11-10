-- 0021 notify 域：管道服务（backend/notify）P1 运行表补全 + DB 角色矩阵（设计 §4.2/§4.5/§4.6/§5.3）。
-- 本迁移由 core 仓库统一编号维护（单库单一 schema 事实源），对象归管道服务使用。

-- 去重门闩（§4.2 / §7.1 第二道防线）：ingress 单语句 upsert RETURNING event_id——
-- 命中返回首次落库事件；过期行被新事件原地替换；event_id 列用于返回上次结果。
CREATE TABLE notify_dedup (
    biz_id      varchar(64)  NOT NULL,
    dedup_key   varchar(128) NOT NULL,
    event_id    uuid         NOT NULL,           -- 首次落库事件（幂等返回）
    expire_time timestamptz  NOT NULL,           -- 门闩有效期（缺省 24h，maintenance 清理）
    PRIMARY KEY (biz_id, dedup_key)
);
COMMENT ON TABLE notify_dedup IS '事件去重门闩：(biz_id, dedup_key) 有效期内幂等；管道 ingress 写，maintenance 清理过期行';
CREATE INDEX idx_notify_dedup_expire ON notify_dedup (expire_time);

-- 聚合组（§4.5）：主键 = biz|group_key；router 批事务内按键排序 upsert（count 累加、
-- samples 封顶 20、窗口滑动但封顶 first_event_time + max_window）；sweeper 条件关闭
-- （status 1→2）并按 count > 阈值置 suppressed_until（抑制期内同键仅计数不重开）。
CREATE TABLE notify_agg_group (
    group_key        varchar(191) PRIMARY KEY,   -- biz|group_key
    biz_id           varchar(64)  NOT NULL,
    event_type       varchar(128) NOT NULL,
    route_id         uuid         NOT NULL,      -- 命中的聚合路由
    first_event_id   uuid         NOT NULL,      -- 本窗口首事件（汇总 dispatch 引用）
    first_event_time timestamptz  NOT NULL,
    count            int          NOT NULL DEFAULT 0,
    samples          jsonb        NOT NULL DEFAULT '[]'::jsonb, -- 前 20 条 {event_id,title,severity,create_time}
    window_until     timestamptz  NOT NULL,
    suppressed_until timestamptz,                -- 非空且未来 = 抑制期
    status           smallint     NOT NULL DEFAULT 1, -- 1 open / 2 closed
    create_time      timestamptz  NOT NULL DEFAULT now(),
    update_time      timestamptz  NOT NULL DEFAULT now()
);
COMMENT ON TABLE notify_agg_group IS '聚合微批组：router 累加 / sweeper 关窗出汇总；已关闭组由 maintenance 按保留期清理';
CREATE INDEX idx_notify_agg_open ON notify_agg_group (window_until) WHERE status = 1;

-- 令牌桶（§4.6）：按渠道实例共享配额，条件 UPDATE 原子消费；随配置快照幂等同步。
CREATE TABLE notify_rate_bucket (
    channel_id  uuid             PRIMARY KEY,
    tokens      double precision NOT NULL,
    capacity    double precision NOT NULL,
    rate        double precision NOT NULL,       -- 每秒补充
    last_refill timestamptz      NOT NULL DEFAULT now()
);
COMMENT ON TABLE notify_rate_bucket IS '渠道令牌桶：管道多实例共享配额（PG 条件 UPDATE），qps=0 渠道无行';

-- sender 租约回收索引：status=2 且租约（next_retry_time）过期的行。
CREATE INDEX idx_notify_dispatch_sending ON notify_dispatch (next_retry_time) WHERE status = 2;
-- 待发/重试/发送中三态恒有到期时间，杜绝 NULL 不可见行（评审口径）。
ALTER TABLE notify_dispatch ADD CONSTRAINT ck_notify_dispatch_due CHECK (status NOT IN (1, 2, 4) OR next_retry_time IS NOT NULL);

-- DB 角色矩阵（§5.3，NOLOGIN 组角色；部署时各服务登录账号 GRANT 为成员）。
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'notify_core_role') THEN
        CREATE ROLE notify_core_role NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'notify_pipe_role') THEN
        CREATE ROLE notify_pipe_role NOLOGIN;
    END IF;
END $$;
-- core：配置表全权、运行表只读、事件仅 INSERT、死信仅改重放标记。
GRANT SELECT, INSERT, UPDATE, DELETE ON notify_channel, notify_template, notify_route, notify_silence,
    notify_subscription, notify_producer, notify_cfg_version TO notify_core_role;
GRANT SELECT ON notify_event, notify_dispatch, notify_delivery_log, notify_dlq TO notify_core_role;
GRANT INSERT ON notify_event TO notify_core_role;
GRANT UPDATE (replay_state, update_time) ON notify_dlq TO notify_core_role;
-- 管道：配置表只读（含 system_user：订阅者邮箱解析）、运行表读写、小表可清理。
GRANT SELECT ON notify_channel, notify_template, notify_route, notify_silence, notify_subscription,
    notify_producer, notify_cfg_version, "system_user" TO notify_pipe_role;
GRANT SELECT, INSERT, UPDATE ON notify_event, notify_dispatch, notify_delivery_log, notify_dlq TO notify_pipe_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON notify_dedup, notify_agg_group, notify_rate_bucket TO notify_pipe_role;
GRANT DELETE ON notify_dlq TO notify_pipe_role;
-- 分区维护（ATTACH/DROP）需父表属主：三张分区表及其现有分区归管道角色。
ALTER TABLE notify_event OWNER TO notify_pipe_role;
ALTER TABLE notify_dispatch OWNER TO notify_pipe_role;
ALTER TABLE notify_delivery_log OWNER TO notify_pipe_role;
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT inhrelid::regclass AS part FROM pg_inherits
             WHERE inhparent IN ('notify_event'::regclass, 'notify_dispatch'::regclass, 'notify_delivery_log'::regclass)
    LOOP
        EXECUTE format('ALTER TABLE %s OWNER TO notify_pipe_role', r.part);
    END LOOP;
END $$;
