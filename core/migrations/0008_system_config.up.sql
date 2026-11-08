-- 0008_system_config.up.sql 系统配置表：全局展示配置（标题/子标题/logo/
-- 页脚文案）的单行存储。无业务键、无软删（全局唯一配置行，覆盖即更新，
-- 非业务实体无恢复语义）；审计仅留 update_time/update_user（最后修改痕迹）。
-- 应用层保证单行：首次更新时 INSERT，此后 UPDATE；GET 无行返回空配置。

CREATE TABLE system_config (
    id          uuid        PRIMARY KEY,
    title       text        NOT NULL DEFAULT '',
    sub_title   text        NOT NULL DEFAULT '',
    logo        text        NOT NULL DEFAULT '',
    footer      text        NOT NULL DEFAULT '',
    update_time timestamptz NOT NULL DEFAULT now(),
    update_user uuid
);

COMMENT ON TABLE system_config IS '系统配置表：全局展示配置单行存储，覆盖更新，不做软删';
COMMENT ON COLUMN system_config.id IS '主键，UUIDv7，应用侧生成；表内仅一行';
COMMENT ON COLUMN system_config.title IS '系统标题（顶栏/标签页/登录页），空串表示未配置';
COMMENT ON COLUMN system_config.sub_title IS '系统子标题，空串表示未配置';
COMMENT ON COLUMN system_config.logo IS '系统 logo 图片 URL，空串表示未配置';
COMMENT ON COLUMN system_config.footer IS '页脚文案，空串表示未配置';
COMMENT ON COLUMN system_config.update_time IS '最后更新时间';
COMMENT ON COLUMN system_config.update_user IS '最后修改人用户 ID，引用 "system_user".id';
