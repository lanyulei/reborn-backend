-- 0057 文件域（file）：通用附件元数据。文件内容存放在对象存储驱动里（第一版为本地磁盘），
-- 本表只记元数据与归属。约定同 0001：id 为应用侧生成的 UUIDv7。
-- 生命周期：上传即落一行「临时」（status=1）；业务提交时在业务事务里绑定到业务对象
-- （status=2，写入 biz_type / biz_id）；超过保留期仍未绑定的临时文件、以及已软删的文件，
-- 由后台任务连同对象一并物理清理。
-- 用户引用（create_user）不设外键：用户归 system 域，用户删除后附件仍随业务对象保留。

CREATE TABLE file_object (
    id          uuid         PRIMARY KEY,
    driver      varchar(32)  NOT NULL,
    object_key  varchar(255) NOT NULL,
    filename    varchar(255) NOT NULL,
    size        bigint       NOT NULL,
    mime        varchar(128) NOT NULL,
    sha256      char(64)     NOT NULL,
    status      smallint     NOT NULL DEFAULT 1,
    biz_type    varchar(64)  NULL,
    biz_id      varchar(64)  NULL,
    create_user uuid         NOT NULL,
    create_time timestamptz  NOT NULL DEFAULT now(),
    bind_time   timestamptz  NULL,
    delete_time timestamptz  NULL,
    purge_time  timestamptz  NULL,
    CONSTRAINT ck_file_object_status CHECK (status IN (1, 2)),
    CONSTRAINT ck_file_object_size CHECK (size >= 0),
    CONSTRAINT ck_file_object_bound CHECK ((status = 2) = (biz_type IS NOT NULL AND biz_id IS NOT NULL))
);
COMMENT ON TABLE file_object IS '文件对象元数据（通用附件）：内容在对象存储驱动，软删后由后台任务物理清理';
COMMENT ON COLUMN file_object.id IS '主键，UUIDv7，应用侧生成';
COMMENT ON COLUMN file_object.driver IS '存储驱动名（如 local）：切换驱动后旧文件仍按原驱动读取';
COMMENT ON COLUMN file_object.object_key IS '驱动内的对象键（服务端生成，不含用户输入）';
COMMENT ON COLUMN file_object.filename IS '原始文件名（仅展示与下载命名，不参与存储路径）';
COMMENT ON COLUMN file_object.size IS '字节数';
COMMENT ON COLUMN file_object.mime IS '内容类型（服务端按文件头嗅探后确定）';
COMMENT ON COLUMN file_object.sha256 IS '内容 SHA-256（hex）';
COMMENT ON COLUMN file_object.status IS '状态：1=临时（已上传未绑定） 2=已绑定';
COMMENT ON COLUMN file_object.biz_type IS '所属业务类型（如 ticket / ticket_comment）；临时文件为空';
COMMENT ON COLUMN file_object.biz_id IS '所属业务对象 ID；临时文件为空';
COMMENT ON COLUMN file_object.create_user IS '上传者用户 ID（不设外键）';
COMMENT ON COLUMN file_object.bind_time IS '绑定时间';
COMMENT ON COLUMN file_object.delete_time IS '软删时间；非空的行等待后台物理清理';
COMMENT ON COLUMN file_object.purge_time IS '最近一次清理失败的时间（对象删除失败）；一段时间内不再重试，避免坏行堵住清理队列';
-- 业务对象 → 附件列表。
CREATE INDEX idx_file_object_biz ON file_object (biz_type, biz_id) WHERE status = 2 AND delete_time IS NULL;
-- 后台清理：过期临时文件 / 已软删文件。
CREATE INDEX idx_file_object_temp ON file_object (create_time) WHERE status = 1 AND delete_time IS NULL;
CREATE INDEX idx_file_object_deleted ON file_object (delete_time) WHERE delete_time IS NOT NULL;
-- 上传配额：统计某用户尚未绑定的临时文件数。
CREATE INDEX idx_file_object_temp_user ON file_object (create_user) WHERE status = 1 AND delete_time IS NULL;
