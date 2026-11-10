-- 0020：三张按月分区运行表补 DEFAULT 分区兜底。
-- 0016-0018 只预建了 2026-11 ~ 2027-01 三个月分区；分区维护 job 属管道 P1，
-- 在其上线前（或 job 故障时）一旦写入落到无分区区间，PG 直接拒绝 INSERT
-- （no partition of relation found for row）——对 notify_event 意味着
-- NotificationWriter 同事务直插失败，业务写（如角色授权）随之整体回滚，
-- 通知系统的运维空窗会演变成业务面硬故障。DEFAULT 分区接住越界行，
-- 维护 job 建出对应月份分区后可把 DEFAULT 中的行迁回（§5.4）。
CREATE TABLE notify_event_default PARTITION OF notify_event DEFAULT;
CREATE TABLE notify_dispatch_default PARTITION OF notify_dispatch DEFAULT;
CREATE TABLE notify_delivery_log_default PARTITION OF notify_delivery_log DEFAULT;
