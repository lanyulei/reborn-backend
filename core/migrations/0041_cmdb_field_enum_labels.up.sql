-- 枚举字段展示值（实际值 → 展示值）；仅显示层使用，可空。
ALTER TABLE cmdb_model_field ADD COLUMN IF NOT EXISTS enum_labels jsonb;
