-- 0014 notify 域：用户订阅（订阅中心）
-- 语义：一行 = 某用户在某 biz 对某事件模式的渠道偏好；自然键
-- (user_id, biz_id, event_pattern) 在未删除行内唯一（部分唯一索引），
-- 软删后同键可重建。channel_prefs 落设计 §5.1 紧凑形态：
--   {"email":true,"dingtalk":"$group:ops"}
-- 值仅 bool（订阅开关，地址取用户资料）或 string（机器人目标群）。
-- 注意：订阅不属于管道 10s 轮询的配置快照（§4.1：渠道/路由/静默/模板），
-- 管道按事件实时解析订阅者，故不登记 notify_cfg_version、写路径不 bump。
CREATE TABLE notify_subscription (
    id            uuid        PRIMARY KEY,
    user_id       uuid        NOT NULL,                    -- 订阅人（core system_user，无外键约束与 system 域解耦）
    biz_id        varchar(64) NOT NULL DEFAULT 'default',
    event_pattern varchar(128) NOT NULL DEFAULT '*',       -- 精确 / 前缀通配 / '*'，与路由同式
    channel_prefs jsonb       NOT NULL,                    -- 渠道偏好（紧凑形态，key 为可订阅渠道类型）
    enabled       boolean     NOT NULL DEFAULT true,       -- 停用后订阅者解析跳过
    create_time   timestamptz NOT NULL DEFAULT now(),
    update_time   timestamptz NOT NULL DEFAULT now(),
    delete_time   timestamptz,                             -- 软删；唯一约束仅约束未删除行，删除后同键可重建
    create_user   uuid,
    update_user   uuid,
    delete_user   uuid
);

COMMENT ON TABLE notify_subscription IS '用户通知订阅（订阅中心）：自然键 (user_id,biz_id,event_pattern) 未删除行内唯一，Upsert 语义';
COMMENT ON COLUMN notify_subscription.id IS '订阅唯一标识（UUIDv7）';
COMMENT ON COLUMN notify_subscription.user_id IS '订阅人用户 ID（引用 system_user，逻辑关联）';
COMMENT ON COLUMN notify_subscription.biz_id IS '业务租户标识；订阅匹配按 biz 隔离';
COMMENT ON COLUMN notify_subscription.event_pattern IS '事件匹配模式：精确 / 前缀通配（monitor.alert.*）/ 全匹配（*），与路由同式';
COMMENT ON COLUMN notify_subscription.channel_prefs IS '渠道偏好紧凑形态：{"email":true,"dingtalk":"$group:ops"}；可订阅类型 email/dingtalk/wecom/feishu';
COMMENT ON COLUMN notify_subscription.enabled IS '启用状态；停用后订阅者解析跳过';
COMMENT ON COLUMN notify_subscription.create_time IS '创建时间';
COMMENT ON COLUMN notify_subscription.update_time IS '更新时间';
COMMENT ON COLUMN notify_subscription.delete_time IS '软删时间；NULL=未删除';
COMMENT ON COLUMN notify_subscription.create_user IS '创建人（操作者），暂未落值';
COMMENT ON COLUMN notify_subscription.update_user IS '最后更新人（操作者），暂未落值';
COMMENT ON COLUMN notify_subscription.delete_user IS '删除人（操作者），暂未落值';

CREATE UNIQUE INDEX uk_notify_subscription ON notify_subscription (user_id, biz_id, event_pattern) WHERE delete_time IS NULL;
