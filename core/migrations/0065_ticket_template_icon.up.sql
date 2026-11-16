-- 0065 工单模板：图标。
ALTER TABLE ticket_template ADD COLUMN icon varchar(64) NOT NULL DEFAULT '';
COMMENT ON COLUMN ticket_template.icon IS '图标标识（前端图标集的键）；空 = 缺省图标';
