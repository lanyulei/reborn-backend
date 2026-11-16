-- 0068 工单表单布局：单列（一行一个字段）/ 双列（一行两个字段）。
ALTER TABLE ticket_template ADD COLUMN form_columns smallint NOT NULL DEFAULT 1;
COMMENT ON COLUMN ticket_template.form_columns IS '表单布局：1=单列（一行一个字段） 2=双列（一行两个字段，多行文本仍独占一行）';
ALTER TABLE ticket_template ADD CONSTRAINT ck_ticket_template_form_columns CHECK (form_columns IN (1, 2));

ALTER TABLE ticket_order ADD COLUMN form_columns smallint NOT NULL DEFAULT 1;
COMMENT ON COLUMN ticket_order.form_columns IS '提单时模板的表单布局快照：1=单列 2=双列';
ALTER TABLE ticket_order ADD CONSTRAINT ck_ticket_order_form_columns CHECK (form_columns IN (1, 2));
