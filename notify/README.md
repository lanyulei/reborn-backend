# notify — 通知管道服务（数据面）

Kratos v3 单二进制，按 `SERVER_ROLES` 启用组件，与 core 共用 PostgreSQL（配置表只读、
运行表读写），P1 骨干为 **PG 队列**（`FOR UPDATE SKIP LOCKED` + 条件更新状态机），
NATS JetStream 作为后续可插拔实现（接口已抽象为 `biz.EventSource` / `biz.DispatchRepo`）。
行为契约见 [AGENTS.md](AGENTS.md)，设计见 `backend/docs/notify-design.md`。
与 core 共用的基础代码在同仓 `backend/kit`（`replace ../kit`）：传输装配 `httpx`、数据访问 `dbx`、
凭据 `secretbox`（管道只 Open）、`idgen`，以及**数据契约** `notifycontract`——jsonb 结构体、状态常量、
事件类型/模式正则与匹配语义、`notify_event` INSERT 与 dedup 门闩 SQL 两端同源，改契约只改 kit。

## 接入方必读：语义边界

- **事件命名**：`biz_id`（≤64，建议 `<系统名>`，core 自身事件用 `core`，未指定为 `default`）与 `event_type`
  （小写点分 `<域>.<对象>.<动作>`，如 `ticket.apply.created`、`monitor.alert.firing`）都是自由字符串、
  **无登记表**：拼写不一致即路由不命中 → 事件 `status=4 failed`（控制台 `/api/v1/notify/event?status=4` 可查）。
  各接入方须在自己的文档固定命名，并在 core 为该 biz 配好路由后再上线。
- **收件（notify_event.recipients，显式）**：元素可为 **邮箱** / **平台用户 ID**（经 `system_user` 解析为邮箱；
  停用/无邮箱则忽略并记日志）/ **`$` 开头的群标识**（如 `$group:ops`，发往路由里的群机器人渠道）。
  显式邮箱非空时**替代**路由对 email 渠道的 fixed/订阅策略；显式群标识非空时替代机器人渠道策略；
  webhook 渠道恒以实例 URL 为收件，不受显式收件影响。
- **路由匹配只取一条**：同 biz 内启用路由中 pattern 命中 ∧ severity ≥ 下限 ∧ tag_match 全等者，按
  priority（小者优先）→ 模式精确度（精确 > 前缀 > `*`）→ id 决胜，**仅最高者生效**。一条事件要发多个
  渠道，把渠道都放进该路由的 `channel_ids`。
- **tag_match**：键先按 payload 顶层字面键查，未命中再按点分路径下钻（`labels.env` → `payload.labels.env`）；
  值按 JSON 文本比较（`3` 与 `"3"` 等价、`true` 与 `"true"` 等价）。
- **静默粒度**：`event_pattern + severity_max + 时间窗`，不支持按主机/标签静默；要静默某台机器，
  请在生产者侧用 dedup/不投递处理。
- **订阅中心仅对平台用户生效**（`notify_subscription ⋈ system_user`）；外部项目的用户不在平台用户表时，
  用路由 fixed 收件或事件显式收件。
- **模板**：顶层变量 `Title/Payload/Severity/Count/Samples/BizID/EventTime` 拼错 → 该任务不可重试进死信；
  `Payload.xxx` / `Samples` 元素字段缺失 → 渲染为可见的 `<no value>`（不失败）。`Samples` 元素键：
  `event_id/title/severity/create_time`。
- **BatchSend 全有或全无**：≤500 条同事务，任一条校验/scope 失败整批 400/403，生产者需按条重试。
- **数据面只暴露 HTTP**（HMAC 签名绑定请求体）；无 gRPC ingress。
- **删除渠道**前须先从路由解绑（否则 409 `NOTIFY_CHANNEL_IN_USE`）。

## 组件与角色

| 角色 | 组件 | 职责 |
|---|---|---|
| `api` | ingress（HTTP 8020） | `POST /ingress/v1/events`、`/ingress/v1/events:batch`；HMAC 鉴权；dedup 门闩；先落库后应答 |
| `router` | router + sweeper | 声明 pending 事件 → 静默 → 选路 → 收件解析（fixed + 订阅 ⋈ system_user）→ 生成 dispatch 或聚合计数；sweeper（选主）关闭聚合窗口出汇总/摘要 |
| `sender` | sender + 租约回收 | 声明到期任务 → 渲染快照 → 熔断/令牌桶 → email / webhook / dingtalk / wecom / feishu 适配器 → 投递日志 → 成功/退避/死信 |
| `dlqworker` | dlq 重放 | 控制台标记的死信 → 新 dispatch |
| `maintenance` | 分区维护 + 清理（选主） | 预建未来 2 月分区（先迁出 DEFAULT 再 ATTACH）、按保留期 DROP、dedup/dlq/聚合组清理 |
| — | HTTP 恒启动 | `/healthz`、`/metrics`（OTel → Prometheus） |

`SERVER_ROLES` 为空 = 全部角色；`relay` 保留给 NATS 骨干。

## 快速开始

```bash
# 1. 数据库：在 core 仓库应用全部迁移（含 0021 管道表 + 角色矩阵）
cd ../core && for f in migrations/*.up.sql; do psql -h 127.0.0.1 -U postgres -d reborn -f "$f"; done
# 2. 主密钥与 core 相同（渠道 secret 解密）
export KRATOS_NOTIFY_MASTER_KEY=...
# 3. 运行
cd ../notify && make init && make gen && SERVER_ROLES=api,router,sender,dlqworker,maintenance go run ./cmd/notify -conf ./configs
# 4. 自检
make check
TEST_DATABASE_SOURCE="host=127.0.0.1 user=postgres password=<pw> dbname=reborn_test sslmode=disable" \
  go test ./internal/data/... -race -count=1 -tags=integration -p 1
```

## ingress 鉴权（生产者接入）

```
Authorization: HMAC <producer_id>:<sig>:<ts>
key       = SHA-256(token 明文)                      # token 由 core 生产者管理签发（npt_...）
canonical = ts + "\n" + METHOD + "\n" + PATH + "\n" + hex(sha256(body))
sig       = hex(HMAC-SHA256(key, canonical))         # ts 为 Unix 秒，±5min
```
Go 生产者可直接用 `pkg/ingressauth.Header(producerID, token, method, path, body, time.Now())`。

## Webhook 出站契约

JSON 体 `{event_id, biz_id, event_type, severity, title, subject, body, payload, send_key, aggregate?, occurred_at}`；
头 `X-Notify-Send-Key`（幂等键，接收端去重）、`X-Notify-Event-Id`、`X-Notify-Timestamp`、
`X-Notify-Signature: sha256=hex(HMAC-SHA256(secret, ts + "." + body))`（渠道配置了 secret 时）。
仅 HTTPS；2xx 成功；408/429/5xx/超时重试；其余 4xx 不可重试直接死信。

## 状态机速查

- 事件 `publish_state` 0→1（交付 router），`status` 1 received → 2 routed / 3 suppressed / 4 failed（无路由/无目标/重试超限）
- 任务 `status` 1 pending → 2 sending（租约）→ 3 sent / 4 failed（退避）/ 5 dead（+ notify_dlq）
- 死信 `replay_state` 1 none → 2 pending（core 标记）→ 3 replayed（新 dispatch）
- 聚合组 `status` 1 open → 2 closed；`suppressed_until` 抑制期内同键仅计数

## 多租户与可靠性能力（迁移 0023）

- **生产者配额**：core 生产者管理的 `qps_limit` / `daily_limit`（0 = 不限，现有生产者缺省）。ingress 在落库
  事务内按生产者扣减秒窗 / 日窗（UTC）计数（`notify_quota_counter` 条件 upsert，多实例共享），超限返回
  **429 `INGRESS_QUOTA_EXCEEDED`** 且整批不落库；单批条数本身超过上限也拒绝。core 内部直插不受配额约束。
- **按 biz 的控制面隔离**：角色 `biz_scope`（core 角色管理，空 = 不限 = 现有角色缺省）；用户可管范围 =
  启用角色 scope 并集，任一角色不限即不限，超管恒不限。受限用户：创建须 biz 在范围内、改/删/查/测试
  按存储行的 biz 判定（403 `NOTIFY_BIZ_FORBIDDEN`）、渠道/模板/路由/静默/生产者/事件列表**必须带 `biz_id`**、
  任务/明细/死信列表与死信重放**必须带 `channel_id`**（400 `NOTIFY_BIZ_REQUIRED`）。`/api/v1/me` 返回
  `biz_scope` / `biz_unrestricted` 供前端限定 biz 选择。订阅中心沿用「本人或 admin」语义。
- **跨实例共享熔断**：发送结果写 `notify_channel_health`（单语句 upsert：60s 滑窗、≥10 样本失败率 >50%
  打开 60s），所有 sender 副本每 2s 刷新同一视图；到期后集群内**仅一副本**领取探测名额（探测方 30s 无结果
  可被接管）。健康表不可达时退化为进程内判定，不阻塞发送。
- **core 直插去重**：`NotificationWriter` 带 `dedup_key` 的事件与 ingress 走同一 `notify_dedup` 门闩
  （同事务：业务回滚即释放），有效期内重复返回首次事件（`Deduplicated=true`，不落第二行）；TTL 由 core
  `notify.dedup_ttl` 配置（缺省 24h）。
- **机器人签名**：渠道 `sign_secret`（只写不读，仅 dingtalk/feishu 可设；传空串清除）。钉钉加签：URL 追加
  `timestamp`(ms) + `sign`；飞书签名校验：body 追加 `timestamp`(s) + `sign`。未设则不签名（企微忽略）。

## 已知口径

- 群机器人实例 secret = access_token/key，目标群由实例决定；`config.at_mobiles` 作 @ 名单（钉钉/企微）。
- 控制面按 biz 隔离只约束 notify 域接口；system 域（用户/角色）权限模型不变。

## 部署：数据库角色

迁移 0021 创建 NOLOGIN 组角色 `notify_core_role`（core：配置表全权、运行表只读、事件仅 INSERT、
死信仅改重放标记）与 `notify_pipe_role`（管道：配置表只读、运行表读写、三张分区表属主）。
生产环境各服务使用独立登录账号并加入对应组角色，使权限边界由数据库强制：

```sql
CREATE ROLE core_app LOGIN PASSWORD '...';   GRANT notify_core_role TO core_app;
CREATE ROLE notify_app LOGIN PASSWORD '...'; GRANT notify_pipe_role TO notify_app;
ALTER ROLE notify_app SET ROLE notify_pipe_role;  -- 分区 DDL 以属主身份执行
```
本地开发沿用 `postgres` 超级用户（配置文件 DSN），权限边界仅由代码自律保证。
