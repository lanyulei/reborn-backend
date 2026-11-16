-- 0061 工单增强：SLA（响应 / 解决时限与超时通知）、满意度评价、关联 CMDB 资源。
-- 新增列均带常量缺省值（只改元数据，不重写表）。

ALTER TABLE ticket_template
    ADD COLUMN sla          jsonb    NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN sla_escalate jsonb    NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN ci_link      smallint NOT NULL DEFAULT 0,
    ADD CONSTRAINT ck_ticket_template_ci_link CHECK (ci_link IN (0, 1, 2));
COMMENT ON COLUMN ticket_template.sla IS 'SLA 规则（数组）：priority / response / resolve；时限为 {value, unit, workTime}，按工作时间计时走流程引擎的工作日历';
COMMENT ON COLUMN ticket_template.sla_escalate IS 'SLA 超时的升级通知对象（用户 ID 数组），与当前处理人一并通知';
COMMENT ON COLUMN ticket_template.ci_link IS '提单时关联 CMDB 资源：0=不关联 1=可选 2=必填';

ALTER TABLE ticket_order
    ADD COLUMN response_due   timestamptz  NULL,
    ADD COLUMN resolve_due    timestamptz  NULL,
    ADD COLUMN response_time  timestamptz  NULL,
    ADD COLUMN sla_escalate   jsonb        NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN sla_notified   smallint     NOT NULL DEFAULT 0,
    ADD COLUMN rating         smallint     NULL,
    ADD COLUMN rating_comment varchar(500) NOT NULL DEFAULT '',
    ADD COLUMN rating_time    timestamptz  NULL,
    ADD CONSTRAINT ck_ticket_order_rating CHECK (rating IS NULL OR rating BETWEEN 1 AND 5);
COMMENT ON COLUMN ticket_order.response_due IS '响应时限（提单时按模板的 SLA 规则算出；未配置为空）';
COMMENT ON COLUMN ticket_order.resolve_due IS '解决时限';
COMMENT ON COLUMN ticket_order.response_time IS '首次响应时间：处理人第一次有实质动作的时间（由流程引擎同事务回写）';
COMMENT ON COLUMN ticket_order.sla_escalate IS '提单时的升级通知对象快照（用户 ID 数组）';
COMMENT ON COLUMN ticket_order.sla_notified IS '已发出的 SLA 超时通知（位标志）：1=响应超时 2=解决超时';
COMMENT ON COLUMN ticket_order.rating IS '满意度评分 1-5（工单完成后由提单人评价一次）';
-- SLA 巡检：只看仍在处理中、且该项通知尚未发出的工单。
CREATE INDEX idx_ticket_order_sla_response ON ticket_order (response_due)
    WHERE status = 1 AND response_time IS NULL AND response_due IS NOT NULL AND (sla_notified & 1) = 0;
CREATE INDEX idx_ticket_order_sla_resolve ON ticket_order (resolve_due)
    WHERE status = 1 AND resolve_due IS NOT NULL AND (sla_notified & 2) = 0;

CREATE TABLE ticket_resource (
    ticket_id  uuid         NOT NULL REFERENCES ticket_order (id),
    ci_id      uuid         NOT NULL,
    ci_name    varchar(255) NOT NULL,
    model_name varchar(64)  NOT NULL DEFAULT '',
    PRIMARY KEY (ticket_id, ci_id)
);
COMMENT ON TABLE ticket_resource IS '工单关联的 CMDB 资源（只以 ID 关联，不设外键、不跨域 JOIN；名称为关联时的快照）';
CREATE INDEX idx_ticket_resource_ci ON ticket_resource (ci_id);
