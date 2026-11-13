-- 0045_system_refresh_token_family.down.sql 回滚 0045：先清除墓碑行（旧版本
-- 语义「行存在即有效」，墓碑留存会让已轮换的令牌复活），再删除列与索引。

DELETE FROM system_refresh_token WHERE rotated_at IS NOT NULL;

DROP INDEX IF EXISTS idx_system_refresh_token_family_id;

ALTER TABLE system_refresh_token DROP COLUMN IF EXISTS rotated_at;
ALTER TABLE system_refresh_token DROP COLUMN IF EXISTS family_id;

COMMENT ON TABLE system_refresh_token IS '刷新令牌表：SHA-256 hex 落库，物理删除，不做软删';
