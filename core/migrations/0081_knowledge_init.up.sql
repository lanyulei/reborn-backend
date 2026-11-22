-- 0081_knowledge_init.up.sql 知识库域：空间 / ACL / 文档 / 版本 / 分块 / 关联 / 导入任务 / 索引任务 / 检索日志。
--
-- 检索：分块表 tsv 为应用侧分词（中文二元组 + 英文数字词）后写入的 'simple' 配置 tsvector，GIN 索引；
-- 向量列与 HNSW 索引是条件 DDL：pgvector 扩展可用（pg_available_extensions 有 vector）才创建。
-- 扩展不可用时知识库照常工作，检索退化为纯全文；之后安装了 pgvector，在库上手工执行本文件末尾
-- 「向量列」一段（幂等）即可启用向量检索，无需重放迁移。
-- 维度固定 1024（bge-m3 / text-embedding-v3 等的缺省维度）；halfvec 比 vector 省一半存储。
--
-- 权限：空间 visibility=1 公开（登录即可读）/ 2 受限（按 knowledge_space_acl）；负责人隐含管理权限。
-- 文档正文只存 Markdown；content 为已发布正文，draft_content 为待发布草稿（NULL = 无未发布修改）。
-- 所有表不设外键到 system_user：行独立于用户生命周期。

CREATE TABLE knowledge_space (
    id                uuid         PRIMARY KEY,
    code              varchar(64)  NOT NULL,
    name              varchar(128) NOT NULL,
    type              varchar(32)  NOT NULL DEFAULT 'general',
    description       text         NOT NULL DEFAULT '',
    visibility        smallint     NOT NULL DEFAULT 1,
    owner_id          uuid         NOT NULL,
    embedding_enabled boolean      NOT NULL DEFAULT true,
    review_required   boolean      NOT NULL DEFAULT false,
    review_days       integer      NOT NULL DEFAULT 0,
    create_user       uuid         NOT NULL,
    create_time       timestamptz  NOT NULL DEFAULT now(),
    update_time       timestamptz  NOT NULL DEFAULT now(),
    delete_time       timestamptz  NULL,
    CONSTRAINT ck_knowledge_space_visibility CHECK (visibility IN (1, 2)),
    CONSTRAINT ck_knowledge_space_review_days CHECK (review_days >= 0)
);
COMMENT ON TABLE knowledge_space IS '知识空间：权限与治理单位，软删';
COMMENT ON COLUMN knowledge_space.code IS '空间编码（URL 友好），未删除行内唯一';
COMMENT ON COLUMN knowledge_space.type IS 'general / manual / standard / incident / faq';
COMMENT ON COLUMN knowledge_space.visibility IS '1 公开（所有登录用户可读）/ 2 受限（按 ACL）';
COMMENT ON COLUMN knowledge_space.embedding_enabled IS 'false 则内容不发往 embedding 服务，只做全文检索';
COMMENT ON COLUMN knowledge_space.review_required IS '发布是否需负责人 / 管理者审核';
COMMENT ON COLUMN knowledge_space.review_days IS '复审周期（天），0 = 不复审';
CREATE UNIQUE INDEX uq_knowledge_space_code ON knowledge_space (code) WHERE delete_time IS NULL;

CREATE TABLE knowledge_space_acl (
    space_id     uuid        NOT NULL,
    subject_type smallint    NOT NULL,
    subject_id   uuid        NOT NULL,
    level        smallint    NOT NULL,
    update_user  uuid        NULL,
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (space_id, subject_type, subject_id),
    CONSTRAINT ck_knowledge_space_acl_subject CHECK (subject_type IN (1, 2, 3)),
    CONSTRAINT ck_knowledge_space_acl_level CHECK (level IN (1, 2, 3))
);
COMMENT ON TABLE knowledge_space_acl IS '知识空间访问控制：主体（角色 / 用户 / 机器账号）→ 级别';
COMMENT ON COLUMN knowledge_space_acl.subject_type IS '1 角色 / 2 用户 / 3 机器账号（预留）';
COMMENT ON COLUMN knowledge_space_acl.level IS '1 读 / 2 写 / 3 管理';
CREATE INDEX idx_knowledge_space_acl_subject ON knowledge_space_acl (subject_type, subject_id);

CREATE TABLE knowledge_document (
    id            uuid         PRIMARY KEY,
    space_id      uuid         NOT NULL,
    slug          varchar(128) NOT NULL,
    title         varchar(255) NOT NULL,
    status        smallint     NOT NULL DEFAULT 1,
    review_state  smallint     NOT NULL DEFAULT 0,
    review_note   varchar(255) NOT NULL DEFAULT '',
    content       text         NOT NULL DEFAULT '',
    draft_content text         NULL,
    draft_title   varchar(255) NULL,
    summary       text         NOT NULL DEFAULT '',
    ai_summary    text         NOT NULL DEFAULT '',
    tags          text[]       NOT NULL DEFAULT '{}',
    source        varchar(16)  NOT NULL DEFAULT 'manual',
    source_ref    varchar(128) NOT NULL DEFAULT '',
    owner_id      uuid         NOT NULL,
    review_at     timestamptz  NULL,
    review_notify_time timestamptz NULL,
    version       integer      NOT NULL DEFAULT 0,
    index_status  smallint     NOT NULL DEFAULT 0,
    create_user   uuid         NOT NULL,
    update_user   uuid         NOT NULL,
    create_time   timestamptz  NOT NULL DEFAULT now(),
    update_time   timestamptz  NOT NULL DEFAULT now(),
    publish_time  timestamptz  NULL,
    delete_time   timestamptz  NULL,
    CONSTRAINT ck_knowledge_document_status CHECK (status IN (1, 2, 3)),
    CONSTRAINT ck_knowledge_document_review_state CHECK (review_state IN (0, 1)),
    CONSTRAINT ck_knowledge_document_index_status CHECK (index_status IN (0, 1, 2, 3))
);
COMMENT ON TABLE knowledge_document IS '知识文档：Markdown 正文，发布即版本，软删';
COMMENT ON COLUMN knowledge_document.status IS '1 草稿 / 2 已发布 / 3 已归档';
COMMENT ON COLUMN knowledge_document.review_state IS '0 无 / 1 待审核（空间要求审核时发布先进入待审核）';
COMMENT ON COLUMN knowledge_document.content IS '已发布正文（草稿状态下为草稿正文）';
COMMENT ON COLUMN knowledge_document.draft_content IS '已发布文档的待发布草稿正文；NULL = 无未发布修改';
COMMENT ON COLUMN knowledge_document.draft_title IS '待发布草稿标题（与 draft_content 同生命周期）';
COMMENT ON COLUMN knowledge_document.source IS 'manual / import / assistant / postmortem';
COMMENT ON COLUMN knowledge_document.review_notify_time IS '最近一次复审提醒时间（巡检去重：到期前提醒一次、到期后再提醒一次）';
COMMENT ON COLUMN knowledge_document.version IS '当前已发布版本号；0 = 从未发布';
COMMENT ON COLUMN knowledge_document.index_status IS '0 未索引 / 1 索引中 / 2 已索引 / 3 失败';
CREATE UNIQUE INDEX uq_knowledge_document_slug ON knowledge_document (space_id, slug) WHERE delete_time IS NULL;
CREATE INDEX idx_knowledge_document_space ON knowledge_document (space_id, status, update_time DESC) WHERE delete_time IS NULL;
CREATE INDEX idx_knowledge_document_owner ON knowledge_document (owner_id, update_time DESC) WHERE delete_time IS NULL;
CREATE INDEX idx_knowledge_document_review ON knowledge_document (review_at) WHERE delete_time IS NULL AND status = 2 AND review_at IS NOT NULL;
CREATE INDEX idx_knowledge_document_title_trgm ON knowledge_document USING gin (title gin_trgm_ops) WHERE delete_time IS NULL;
CREATE INDEX idx_knowledge_document_tags ON knowledge_document USING gin (tags) WHERE delete_time IS NULL;
CREATE INDEX idx_knowledge_document_deleted ON knowledge_document (delete_time) WHERE delete_time IS NOT NULL;

CREATE TABLE knowledge_version (
    id           uuid         PRIMARY KEY,
    document_id  uuid         NOT NULL,
    version      integer      NOT NULL,
    title        varchar(255) NOT NULL,
    content      text         NOT NULL,
    note         varchar(255) NOT NULL DEFAULT '',
    publish_user uuid         NOT NULL,
    create_time  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_knowledge_version UNIQUE (document_id, version)
);
COMMENT ON TABLE knowledge_version IS '知识文档发布版本：正文快照，不可改';

CREATE TABLE knowledge_chunk (
    id              uuid         PRIMARY KEY,
    document_id     uuid         NOT NULL,
    space_id        uuid         NOT NULL,
    version         integer      NOT NULL,
    seq             integer      NOT NULL,
    heading_path    varchar(512) NOT NULL DEFAULT '',
    content         text         NOT NULL,
    tsv             tsvector     NOT NULL,
    content_sha256  char(64)     NOT NULL,
    embedding_model varchar(64)  NULL,
    token_count     integer      NOT NULL DEFAULT 0,
    create_time     timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_knowledge_chunk_seq UNIQUE (document_id, seq)
);
COMMENT ON TABLE knowledge_chunk IS '知识分块：只索引已发布版本；tsv 由应用侧分词写入（simple 配置）';
COMMENT ON COLUMN knowledge_chunk.heading_path IS '标题路径，如 "部署 > 回滚"';
COMMENT ON COLUMN knowledge_chunk.content_sha256 IS '分块正文摘要：重发布时相同内容复用已有向量';
COMMENT ON COLUMN knowledge_chunk.embedding_model IS '向量对应的模型；NULL = 尚未向量化';
CREATE INDEX idx_knowledge_chunk_tsv ON knowledge_chunk USING gin (tsv);
CREATE INDEX idx_knowledge_chunk_space ON knowledge_chunk (space_id);
CREATE INDEX idx_knowledge_chunk_sha ON knowledge_chunk (content_sha256) WHERE embedding_model IS NOT NULL;

CREATE TABLE knowledge_link (
    id          uuid         PRIMARY KEY,
    document_id uuid         NOT NULL,
    target_type varchar(32)  NOT NULL,
    target_id   varchar(128) NOT NULL,
    target_name varchar(255) NOT NULL DEFAULT '',
    create_user uuid         NOT NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT uq_knowledge_link UNIQUE (document_id, target_type, target_id)
);
COMMENT ON TABLE knowledge_link IS '知识文档 ↔ 运维对象关联（CI / 模型 / 告警规则 / 预案 / 工单 / 作业 / 流程）';
CREATE INDEX idx_knowledge_link_target ON knowledge_link (target_type, target_id);

CREATE TABLE knowledge_ingest_job (
    id          uuid         PRIMARY KEY,
    space_id    uuid         NOT NULL,
    document_id uuid         NULL,
    file_id     uuid         NOT NULL,
    filename    varchar(255) NOT NULL,
    title       varchar(255) NOT NULL DEFAULT '',
    tags        text[]       NOT NULL DEFAULT '{}',
    publish     boolean      NOT NULL DEFAULT false,
    status      smallint     NOT NULL DEFAULT 1,
    error       text         NOT NULL DEFAULT '',
    chunk_count integer      NOT NULL DEFAULT 0,
    attempts    integer      NOT NULL DEFAULT 0,
    lease_until timestamptz  NULL,
    create_user uuid         NOT NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    update_time timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_knowledge_ingest_job_status CHECK (status IN (1, 2, 3, 4))
);
COMMENT ON TABLE knowledge_ingest_job IS '文件导入任务：SKIP LOCKED 领取，租约到期可被重新领取';
COMMENT ON COLUMN knowledge_ingest_job.status IS '1 排队 / 2 处理中 / 3 成功 / 4 失败';
CREATE INDEX idx_knowledge_ingest_job_pending ON knowledge_ingest_job (create_time) WHERE status IN (1, 2);
CREATE INDEX idx_knowledge_ingest_job_space ON knowledge_ingest_job (space_id, create_time DESC);

CREATE TABLE knowledge_index_job (
    document_id   uuid        PRIMARY KEY,
    version       integer     NOT NULL,
    status        smallint    NOT NULL DEFAULT 1,
    attempts      integer     NOT NULL DEFAULT 0,
    error         text        NOT NULL DEFAULT '',
    lease_until   timestamptz NULL,
    next_run_time timestamptz NOT NULL DEFAULT now(),
    create_time   timestamptz NOT NULL DEFAULT now(),
    update_time   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_knowledge_index_job_status CHECK (status IN (1, 2, 3, 4))
);
COMMENT ON TABLE knowledge_index_job IS '索引任务：每篇文档至多一条（以最新版本为准），SKIP LOCKED 领取，指数退避重试';
COMMENT ON COLUMN knowledge_index_job.status IS '1 排队 / 2 处理中 / 3 成功 / 4 失败（重试耗尽）';
CREATE INDEX idx_knowledge_index_job_pending ON knowledge_index_job (next_run_time) WHERE status IN (1, 2);

CREATE TABLE knowledge_search_log (
    id          uuid        PRIMARY KEY,
    user_id     uuid        NOT NULL,
    source      varchar(16) NOT NULL,
    query       text        NOT NULL,
    top_k       integer     NOT NULL,
    hit_count   integer     NOT NULL,
    degraded    boolean     NOT NULL DEFAULT false,
    duration_ms integer     NOT NULL DEFAULT 0,
    chunk_ids   uuid[]      NOT NULL DEFAULT '{}',
    feedback    smallint    NOT NULL DEFAULT 0,
    create_time timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_knowledge_search_log_feedback CHECK (feedback IN (0, 1, 2))
);
COMMENT ON TABLE knowledge_search_log IS '检索日志：评估与知识缺口分析；按保留期由后台清理';
COMMENT ON COLUMN knowledge_search_log.source IS 'web / assistant / related';
COMMENT ON COLUMN knowledge_search_log.feedback IS '0 无 / 1 有用 / 2 没用';
CREATE INDEX idx_knowledge_search_log_time ON knowledge_search_log (create_time DESC);
CREATE INDEX idx_knowledge_search_log_user ON knowledge_search_log (user_id, create_time DESC);

-- 向量列（条件 DDL，幂等；pgvector 可用才执行）。之后安装 pgvector 时在库上单独执行这一段即可。
-- 门槛：扩展可用且版本 ≥ 0.7.0（halfvec 类型自 0.7.0 起），并且已装上或当前角色有权装（CREATE EXTENSION
-- 需要库上的 CREATE 权限 / 超级用户）；不满足只提示不报错，检索退化为纯全文，其余迁移照常。
DO $$
DECLARE
    ver text;
BEGIN
    SELECT default_version INTO ver FROM pg_available_extensions WHERE name = 'vector';
    IF ver IS NULL THEN
        RAISE NOTICE 'knowledge: pgvector not available, vector search disabled';
        RETURN;
    END IF;
    IF string_to_array(split_part(ver, '-', 1), '.')::int[] < ARRAY[0, 7, 0] THEN
        RAISE NOTICE 'knowledge: pgvector % too old (need >= 0.7.0 for halfvec), vector search disabled', ver;
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')
       AND NOT has_database_privilege(current_database(), 'CREATE')
       AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN
        RAISE NOTICE 'knowledge: no privilege to CREATE EXTENSION vector, vector search disabled';
        RETURN;
    END IF;
    CREATE EXTENSION IF NOT EXISTS vector;
    ALTER TABLE knowledge_chunk ADD COLUMN IF NOT EXISTS embedding halfvec(1024) NULL;
    CREATE INDEX IF NOT EXISTS idx_knowledge_chunk_embedding ON knowledge_chunk
        USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 64);
END
$$;
