-- 0068 down
ALTER TABLE ticket_order DROP COLUMN IF EXISTS form_columns;
ALTER TABLE ticket_template DROP COLUMN IF EXISTS form_columns;
