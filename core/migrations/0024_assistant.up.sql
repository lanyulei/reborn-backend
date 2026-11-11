-- 0024_assistant.up.sql 智能助手域：会话 / 消息 / 工具调用（审计）三表。
-- 会话按 user_id 隔离（仅本人可见）；run_token/run_lease_until 是会话级
-- 租约（同一会话同时只允许一个回复轮次，多实例下靠条件 UPDATE 互斥，
-- 租约到期自动可重入，实例崩溃不会永久锁死）。
-- 消息 content 以 jsonb 存储模型对话结构（role/content/reasoning/tool_calls/
-- tool_call_id），回放时零转换；(conversation_id, seq) 唯一保证顺序与幂等。
-- 工具调用表即审计日志：记录每次接口调用的入参/出参（已脱敏）、状态与耗时；
-- 写操作先落 pending 等用户确认，确认/拒绝经条件 UPDATE（status='pending'）
-- 保证只被处理一次。三表均不设外键到 system_user：审计行独立于用户生命周期。

CREATE TABLE assistant_conversation (
    id              uuid         PRIMARY KEY,
    user_id         uuid         NOT NULL,
    title           varchar(128) NOT NULL,
    run_token       uuid         NULL,
    run_lease_until timestamptz  NULL,
    create_time     timestamptz  NOT NULL DEFAULT now(),
    update_time     timestamptz  NOT NULL DEFAULT now(),
    delete_time     timestamptz  NULL
);

COMMENT ON TABLE assistant_conversation IS '智能助手会话：按用户隔离，软删';
COMMENT ON COLUMN assistant_conversation.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN assistant_conversation.user_id IS '所属用户 ID；仅本人可访问';
COMMENT ON COLUMN assistant_conversation.title IS '会话标题（首条用户消息截断）';
COMMENT ON COLUMN assistant_conversation.run_token IS '当前回复轮次的租约 token；NULL 表示空闲';
COMMENT ON COLUMN assistant_conversation.run_lease_until IS '租约到期时间；到期后其它轮次可抢占（实例崩溃兜底）';
COMMENT ON COLUMN assistant_conversation.update_time IS '最近活动时间（列表排序依据）';
COMMENT ON COLUMN assistant_conversation.delete_time IS '软删时间；NULL 表示未删除';

CREATE INDEX idx_assistant_conversation_user
    ON assistant_conversation (user_id, update_time DESC)
    WHERE delete_time IS NULL;

CREATE TABLE assistant_message (
    id              uuid        PRIMARY KEY,
    conversation_id uuid        NOT NULL,
    seq             integer     NOT NULL,
    role            varchar(16) NOT NULL,
    content         jsonb       NOT NULL,
    create_time     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_assistant_message_seq UNIQUE (conversation_id, seq)
);

COMMENT ON TABLE assistant_message IS '智能助手消息：模型对话结构原样存储（user/assistant/tool）';
COMMENT ON COLUMN assistant_message.seq IS '会话内序号，从 1 递增；唯一约束保证顺序与幂等';
COMMENT ON COLUMN assistant_message.role IS '消息角色：user / assistant / tool';
COMMENT ON COLUMN assistant_message.content IS '消息体 JSON：{content, reasoning, tool_calls:[{id,name,arguments}], tool_call_id}';

CREATE TABLE assistant_tool_call (
    id              uuid         PRIMARY KEY,
    conversation_id uuid         NOT NULL,
    message_id      uuid         NOT NULL,
    user_id         uuid         NOT NULL,
    call_id         varchar(128) NOT NULL,
    tool_name       varchar(128) NOT NULL,
    title           varchar(64)  NOT NULL DEFAULT '',
    operation       varchar(255) NOT NULL,
    operation_code  char(32)     NOT NULL,
    write           boolean      NOT NULL,
    input           jsonb        NOT NULL,
    status          varchar(16)  NOT NULL,
    output          jsonb        NULL,
    error           text         NULL,
    duration_ms     integer      NOT NULL DEFAULT 0,
    create_time     timestamptz  NOT NULL DEFAULT now(),
    resolve_time    timestamptz  NULL
);

COMMENT ON TABLE assistant_tool_call IS '智能助手工具调用审计：每次接口调用的入参/出参（脱敏）、状态与耗时';
COMMENT ON COLUMN assistant_tool_call.message_id IS '发起该调用的 assistant 消息 ID';
COMMENT ON COLUMN assistant_tool_call.user_id IS '以谁的身份调用（= 会话所属用户）';
COMMENT ON COLUMN assistant_tool_call.call_id IS '模型侧工具调用 ID（对话协议 tool_calls[].id），与消息 tool_call_id 对应';
COMMENT ON COLUMN assistant_tool_call.tool_name IS '工具名 <Service>_<Method>';
COMMENT ON COLUMN assistant_tool_call.title IS '接口中文名（title 注解快照）';
COMMENT ON COLUMN assistant_tool_call.operation IS '接口 proto full method';
COMMENT ON COLUMN assistant_tool_call.operation_code IS '接口授权 code（md5(operation)），与 system_permission 同口径';
COMMENT ON COLUMN assistant_tool_call.write IS '是否写操作（写操作需用户确认后执行）';
COMMENT ON COLUMN assistant_tool_call.input IS '入参 JSON（password/secret/token 等已脱敏）';
COMMENT ON COLUMN assistant_tool_call.status IS 'pending / approved / rejected / denied / succeeded / failed / cancelled';
COMMENT ON COLUMN assistant_tool_call.output IS '出参 JSON（脱敏；失败/拒绝为 NULL）';
COMMENT ON COLUMN assistant_tool_call.error IS '失败原因（接口错误 reason+message / 网络错误）';
COMMENT ON COLUMN assistant_tool_call.resolve_time IS '状态终结时间（确认/拒绝/执行完成）';

CREATE INDEX idx_assistant_tool_call_conversation
    ON assistant_tool_call (conversation_id, create_time);
CREATE INDEX idx_assistant_tool_call_user
    ON assistant_tool_call (user_id, create_time DESC);
