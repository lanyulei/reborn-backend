-- 0083 故障管理域（incident）索引补充：关联器按「触发规则 + 归并键」直接定位活跃 / 刚恢复的故障，
-- 以及风暴判定按规则统计近期开出的故障数——两者此前都是全表分页后在内存里过滤。

CREATE INDEX idx_incident_rule_key ON incident_record (rule_id, correlation_key, status) WHERE rule_id IS NOT NULL;
CREATE INDEX idx_incident_rule_open ON incident_record (rule_id, open_time DESC) WHERE rule_id IS NOT NULL;
