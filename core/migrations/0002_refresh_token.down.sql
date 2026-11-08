-- 0002_refresh_token.down.sql 回滚 0002：删除刷新令牌表（索引随表删除）。

DROP TABLE IF EXISTS system_refresh_token;
