-- 0023 notify 域：多租户治理与管道增强（设计 §4.2 配额 / §4.6 熔断 / §9 机器人签名 / 按 biz 管理隔离 / core 直插去重）。
-- 全部新列取缺省值时语义与此前一致：biz_scope 空=不限、配额 0=不限、无 sign_secret=不签名。

-- 角色可管理的 biz 范围（空数组 = 不限；超管恒不限）。
ALTER TABLE system_role ADD COLUMN biz_scope text[] NOT NULL DEFAULT '{}';
COMMENT ON COLUMN system_role.biz_scope IS '可管理的通知 biz 列表（空=不限）；用户可管范围 = 启用角色 scope 并集，任一角色为空即不限';

-- 生产者（token）配额：每秒 / 每日（UTC）事件数上限，0 = 不限。
ALTER TABLE notify_producer
    ADD COLUMN qps_limit   int NOT NULL DEFAULT 0,
    ADD COLUMN daily_limit int NOT NULL DEFAULT 0;
COMMENT ON COLUMN notify_producer.qps_limit IS '每秒接入事件上限（0=不限）；管道 ingress 按生产者计数，超限 429';
COMMENT ON COLUMN notify_producer.daily_limit IS '每日（UTC）接入事件上限（0=不限）';

-- 机器人签名密钥（钉钉加签 / 飞书签名校验），AES-256-GCM 密文，只写不读；仅 dingtalk/feishu 使用。
ALTER TABLE notify_channel ADD COLUMN sign_secret bytea;
COMMENT ON COLUMN notify_channel.sign_secret IS '机器人签名密钥密文（nonce||ct，只写不读）；NULL=不签名；仅 dingtalk/feishu';

-- 配额计数器：每生产者两行（1=秒窗 2=日窗），条件 UPDATE 原子扣减，多实例共享。
CREATE TABLE notify_quota_counter (
    producer_id  uuid        NOT NULL,
    window_kind  smallint    NOT NULL,           -- 1 second / 2 day(UTC)
    window_start timestamptz NOT NULL,
    count        int         NOT NULL DEFAULT 0,
    PRIMARY KEY (producer_id, window_kind)
);
COMMENT ON TABLE notify_quota_counter IS '生产者配额计数（秒窗/日窗）：ingress 事务内条件 upsert，0 行=超限';

-- 渠道健康（共享熔断）：1 分钟滑窗计数、打开截止、探测占位；多实例同一视图。
CREATE TABLE notify_channel_health (
    channel_id   uuid        PRIMARY KEY,
    window_start timestamptz NOT NULL,
    samples      int         NOT NULL DEFAULT 0,
    failures     int         NOT NULL DEFAULT 0,
    open_until   timestamptz,                    -- 非空且未来 = 熔断打开
    probing      boolean     NOT NULL DEFAULT false, -- 半开探测已被某副本领取
    update_time  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE notify_channel_health IS '渠道共享熔断状态：sender 每次发送结果 upsert，≥10 样本失败率>50% 打开 60s，仅一副本探测';

GRANT SELECT, INSERT, UPDATE, DELETE ON notify_quota_counter, notify_channel_health TO notify_pipe_role;
-- core 直插接入去重门闩（与管道 ingress 同一单语句 upsert）。
GRANT SELECT, INSERT, UPDATE ON notify_dedup TO notify_core_role;
