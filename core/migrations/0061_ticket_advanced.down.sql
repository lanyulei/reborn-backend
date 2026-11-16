-- 0061 down
DROP TABLE IF EXISTS ticket_resource;
DROP INDEX IF EXISTS idx_ticket_order_sla_resolve;
DROP INDEX IF EXISTS idx_ticket_order_sla_response;
ALTER TABLE ticket_order
    DROP CONSTRAINT ck_ticket_order_rating,
    DROP COLUMN rating_time, DROP COLUMN rating_comment, DROP COLUMN rating, DROP COLUMN sla_notified,
    DROP COLUMN sla_escalate, DROP COLUMN response_time, DROP COLUMN resolve_due, DROP COLUMN response_due;
ALTER TABLE ticket_template
    DROP CONSTRAINT ck_ticket_template_ci_link,
    DROP COLUMN ci_link, DROP COLUMN sla_escalate, DROP COLUMN sla;
