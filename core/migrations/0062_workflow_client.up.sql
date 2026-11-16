-- 0062 流程引擎外部接入：其他系统凭接入凭据（API 令牌）发起、查询、撤回流程并投递信号。

CREATE TABLE workflow_client (
    id              uuid         PRIMARY KEY,
    name            varchar(64)  NOT NULL,
    token_hash      bytea        NOT NULL,
    token_prefix    varchar(12)  NOT NULL,
    acting_user_id  uuid         NOT NULL,
    definition_keys jsonb        NOT NULL DEFAULT '[]'::jsonb,
    status          smallint     NOT NULL DEFAULT 1,
    description     varchar(255) NOT NULL DEFAULT '',
    last_used_time  timestamptz  NULL,
    create_time     timestamptz  NOT NULL DEFAULT now(),
    update_time     timestamptz  NOT NULL DEFAULT now(),
    delete_time     timestamptz  NULL,
    create_user     uuid         NULL,
    update_user     uuid         NULL,
    delete_user     uuid         NULL,
    CONSTRAINT ck_workflow_client_status CHECK (status IN (0, 1))
);
COMMENT ON TABLE workflow_client IS '流程引擎的外部接入凭据：每个外部系统一条，凭令牌调用开放接口，软删';
COMMENT ON COLUMN workflow_client.token_hash IS '令牌的 SHA-256（库内不存明文；明文只在创建 / 重置时返回一次）';
COMMENT ON COLUMN workflow_client.token_prefix IS '令牌开头几位（列表里辨认用，不足以还原令牌）';
COMMENT ON COLUMN workflow_client.acting_user_id IS '代表用户：该系统发起的流程以他为发起人（须为可用用户）';
COMMENT ON COLUMN workflow_client.definition_keys IS '允许发起的流程标识（数组）；不在其中的流程一律拒绝';
COMMENT ON COLUMN workflow_client.last_used_time IS '最近一次调用时间（至多每分钟更新一次）';
CREATE UNIQUE INDEX uk_workflow_client_name ON workflow_client (name) WHERE delete_time IS NULL;
CREATE UNIQUE INDEX uk_workflow_client_token ON workflow_client (token_hash);

ALTER TABLE workflow_instance ADD COLUMN client_id uuid NULL;
COMMENT ON COLUMN workflow_instance.client_id IS '经开放接口发起时的接入凭据 ID：该系统只能查询 / 操作自己发起的实例';
