-- 0010_notify_channel.up.sql notify 域：通知渠道表 + 配置版本表。
-- 渠道一行 = 一个可独立发送/限流/启停的端点实例（邮件服务器/webhook 目标/群机器人），
-- type 只决定管道侧适配器；多套邮件、多个 webhook 即多行记录。
-- secret 只写不读：AES-256-GCM 密文落库（主密钥经 KRATOS_NOTIFY_MASTER_KEY 注入），
-- 更新不传即保留旧密文；任何查询路径不返回该列。
-- notify_cfg_version：core 每次写配置同事务 bump 版本号，管道服务 10s 轮询比对
-- 刷新缓存（控制面/数据面跨平面契约，见 backend/docs/notify-design.md §5.1/§5.3）。

CREATE TABLE notify_channel (
    id          uuid         PRIMARY KEY,
    biz_id      varchar(64)  NOT NULL DEFAULT 'default',
    type        varchar(32)  NOT NULL,
    name        varchar(128) NOT NULL,
    config      jsonb        NOT NULL,
    secret      bytea        NULL,
    rate_limit  jsonb        NOT NULL DEFAULT '{"qps":0,"burst":0}'::jsonb,
    enabled     boolean      NOT NULL DEFAULT true,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    delete_time timestamptz  NULL,
    create_user uuid         NULL,
    update_user uuid         NULL,
    delete_user uuid         NULL
);

COMMENT ON TABLE notify_channel IS '通知渠道表：一行=一个渠道实例（邮件服务器/webhook 目标/群机器人），软删';
COMMENT ON COLUMN notify_channel.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN notify_channel.biz_id IS '业务租户标识，默认 default；路由/配额按 biz 隔离';
COMMENT ON COLUMN notify_channel.type IS '渠道类型：email/webhook/dingtalk/wecom/feishu，决定管道侧适配器，创建后不可改';
COMMENT ON COLUMN notify_channel.name IS '渠道展示名，同 biz 未删除行内唯一';
COMMENT ON COLUMN notify_channel.config IS '非敏感配置（jsonb，按 type 定型：email={host,port,username,from_addr,starttls}；webhook={url,method,headers,timeout_s}；机器人={at_mobiles}）';
COMMENT ON COLUMN notify_channel.secret IS '敏感凭据密文（AES-256-GCM，nonce 前缀拼接）；只写不读，更新不传=保留旧密文';
COMMENT ON COLUMN notify_channel.rate_limit IS '令牌桶限流 {"qps":n,"burst":n}，0=不限；管道侧条件 UPDATE 原子消费，按实例独立';
COMMENT ON COLUMN notify_channel.enabled IS '启用状态；停用 ≤10s 生效（管道配置版本轮询），路由匹配跳过，在途任务照常完成';
COMMENT ON COLUMN notify_channel.create_time IS '创建时间';
COMMENT ON COLUMN notify_channel.update_time IS '更新时间';
COMMENT ON COLUMN notify_channel.delete_time IS '软删时间；唯一约束仅约束未删除行，删除后名称可复用';
COMMENT ON COLUMN notify_channel.create_user IS '创建人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_channel.update_user IS '更新人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_channel.delete_user IS '删除人（操作者 ID 注入待补，当前 NULL）';

CREATE UNIQUE INDEX uk_notify_channel_name ON notify_channel (biz_id, name) WHERE delete_time IS NULL;
CREATE INDEX idx_notify_channel_biz_type ON notify_channel (biz_id, type) WHERE delete_time IS NULL;

CREATE TABLE notify_cfg_version (
    table_name  varchar(64)  PRIMARY KEY,
    version     bigint       NOT NULL,
    update_time timestamptz  NOT NULL DEFAULT now()
);

COMMENT ON TABLE notify_cfg_version IS '通知配置版本表：core 每次写配置同事务 bump，管道 10s 轮询比对刷新缓存';
COMMENT ON COLUMN notify_cfg_version.table_name IS '配置表名（如 notify_channel）';
COMMENT ON COLUMN notify_cfg_version.version IS '配置版本号，单调递增；每次写操作同事务 +1';
COMMENT ON COLUMN notify_cfg_version.update_time IS '最近一次 bump 时间';

INSERT INTO notify_cfg_version (table_name, version) VALUES ('notify_channel', 1);
