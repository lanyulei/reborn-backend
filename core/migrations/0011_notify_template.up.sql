-- 0011_notify_template.up.sql notify 域：通知模板表。
-- 模板按 biz × event_type × channel_type 三维覆盖，'*' 通配；管道渲染时解析链
-- (biz,event,channel) > (biz,event,*) > (biz,*,channel) > (biz,*,*) > 内置默认模板
-- （见 backend/docs/notify-design.md §5.1/§6.3）。subject/body 为 Go template，
-- core 写入前做语法解析 + 渲染变量白名单校验（零函数注册，副作用无从发生）；
-- 更新即 version+1（dispatch 记录 template_id，历史通知可复现）。
-- 每次写操作同事务 bump notify_cfg_version('notify_template')：管道 10s 轮询刷新缓存。

CREATE TABLE notify_template (
    id           uuid         PRIMARY KEY,
    biz_id       varchar(64)  NOT NULL DEFAULT 'default',
    event_type   varchar(128) NOT NULL DEFAULT '*',
    channel_type varchar(32)  NOT NULL DEFAULT '*',
    subject      text         NOT NULL,
    body         text         NOT NULL,
    version      integer      NOT NULL DEFAULT 1,
    enabled      boolean      NOT NULL DEFAULT true,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    update_time  timestamptz  NOT NULL DEFAULT now(),
    delete_time  timestamptz  NULL,
    create_user  uuid         NULL,
    update_user  uuid         NULL,
    delete_user  uuid         NULL
);

COMMENT ON TABLE notify_template IS '通知模板表：biz × event_type × channel_type 三维覆盖（* 通配），软删';
COMMENT ON COLUMN notify_template.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN notify_template.biz_id IS '业务租户标识，默认 default；渲染解析链按 biz 隔离';
COMMENT ON COLUMN notify_template.event_type IS '事件类型精确匹配或 * 通配（如 monitor.alert.firing）';
COMMENT ON COLUMN notify_template.channel_type IS '渠道类型（email/webhook/dingtalk/wecom/feishu）或 * 通配';
COMMENT ON COLUMN notify_template.subject IS '标题模板（邮件主题/机器人标题），Go template';
COMMENT ON COLUMN notify_template.body IS '正文模板，Go template；管道渲染超时 100ms、输出上限 64KB';
COMMENT ON COLUMN notify_template.version IS '模板版本号，创建为 1，每次更新 +1；dispatch 记录 (template_id, version) 可复现历史通知';
COMMENT ON COLUMN notify_template.enabled IS '启用状态；停用后解析链跳过（≤10s 生效，配置版本轮询）';
COMMENT ON COLUMN notify_template.create_time IS '创建时间';
COMMENT ON COLUMN notify_template.update_time IS '更新时间';
COMMENT ON COLUMN notify_template.delete_time IS '软删时间；唯一约束仅约束未删除行，删除后该三维组合可复用';
COMMENT ON COLUMN notify_template.create_user IS '创建人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_template.update_user IS '更新人（操作者 ID 注入待补，当前 NULL）';
COMMENT ON COLUMN notify_template.delete_user IS '删除人（操作者 ID 注入待补，当前 NULL）';

-- 三维组合在未删除行内唯一：同一 (biz,event,channel) 只有一个生效模板
CREATE UNIQUE INDEX uk_notify_template_spec
    ON notify_template (biz_id, event_type, channel_type) WHERE delete_time IS NULL;
CREATE INDEX idx_notify_template_biz_event
    ON notify_template (biz_id, event_type) WHERE delete_time IS NULL;

INSERT INTO notify_cfg_version (table_name, version) VALUES ('notify_template', 1);
