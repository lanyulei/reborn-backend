# notify — 通知管道服务（数据面）

Kratos v3 单二进制，按 `SERVER_ROLES` 启用组件，与 core 共用 PostgreSQL（配置表只读、
运行表读写），P1 骨干为 **PG 队列**（`FOR UPDATE SKIP LOCKED` + 条件更新状态机），
NATS JetStream 为可切换骨干（`pipeline.backbone: nats`，见「NATS 骨干」）；PG 在两种骨干下都是事实源与状态机。
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
| `api` | ingress（HTTP 8020） | `POST /ingress/v1/events`、`/ingress/v1/events:batch`；HMAC 鉴权；dedup 门闩；先落库后应答（单事务两轮 pgx.Batch：门闩一批、插入一批；请求日志失败恒记、成功 1/100 采样） |
| `router` | router + sweeper | 声明 pending 事件 → 静默 → 选路 → 收件解析（fixed + 订阅 ⋈ system_user）→ 生成 dispatch 或聚合计数；sweeper（选主）关闭聚合窗口出汇总/摘要 |
| `sender` | sender + 租约回收 | 声明到期任务 → 渲染快照 → 熔断/令牌桶 → email / webhook / dingtalk / wecom / feishu 适配器 → 投递日志 → 成功/退避/死信；发送超时 = 渠道 `timeout_s`（上限 `sender.max_send_timeout` 10s）或缺省 `send_timeout` 3s |
| `dlqworker` | dlq 重放 | 控制台标记的死信 → 新 dispatch（保留聚合上下文、按当前模板重新渲染） |
| `maintenance` | 分区维护 + 清理（选主） | 预建未来 2 月分区（先迁出 DEFAULT 再 ATTACH）、按保留期 DROP、dedup/dlq/聚合组清理 |
| — | HTTP 恒启动 | `/healthz`（快照已加载 ∧ 刷新未停滞超 2 分钟 ∧ DB 可达）、`/metrics`（OTel → Prometheus） |

指标：`notify_loop_runs_total{loop,result}` / `notify_loop_items_total{loop}` / `notify_dispatch_total{channel_type,result}` /
`notify_send_latency_ms{channel_type}`（直方图）/ `notify_ingress_requests_total{result}` / `notify_ingress_events_total{result}` /
`notify_events_pending` / `notify_dispatch_due`（有界计数，10001 = 饱和）/ `notify_queue_oldest_age_seconds{queue}` /
`notify_breaker_open{channel_id}` / `notify_config_age_seconds` / `notify_config_refresh_failures_total`。
`SERVER_ROLES` 拼错直接装配失败（不再静默起一个只有 HTTP 的进程）。

`SERVER_ROLES` 为空 = 全部角色（`backbone: nats` 下含 `relay`）；pg 骨干下 `relay` 为 no-op。

## NATS 骨干（backbone: nats）

- **定位**：JetStream 只做"队列游标"，流里只放 `{id, create_time}`；router/sender 消费到游标后仍用与 pg 路径完全相同的
  PG 条件 UPDATE 做门闩（事件 `status=received`）与围栏（任务 `status IN (1,4)` 且到期 → sending，`attempts` 令牌），
  `RouterUsecase` / `SenderUsecase` 业务代码不变。消息丢失、MaxDeliver 耗尽、消费者长期离线都由 PG 行兜底。
- **relay**（唯一发布者，1~N 副本经 SKIP LOCKED 互斥）：`notify_event.publish_state=0` 且到期 → 发布 `notify.event.<biz>`
  → `publish_state=1`；`notify_dispatch.publish_state=0 AND status IN (1,4)` 且到期 → 发布 `notify.dispatch.<channel_id>`
  → `publish_state=1`（迁移 0038 新增列，pg 骨干恒 0）。发布失败：事件 `publish_attempts+1` 退避，任务原地重试。
  **超期回拨**：已发布但 `2×ack_wait` 后仍未处理的行回拨 `publish_state=0` 重新发布。
- **消费语义**：行已终态 → ack（重投幂等）；行被他人持有/未到期 → `NakWithDelay` 到期时刻；router `DeferEvent` →
  按退避 Nak；sender 可重试失败 → PG 写 `next_retry_time` 后按 `RetryAfter` Nak；死信 → ack；落账失败 → 短延后 Nak，
  租约回收照常。`Nats-Msg-Id = <id>:<代际>`（事件 publish_attempts / 任务 attempts），去重窗口 `min(2m, ack_wait)`。
- **流/消费者**：`<prefix>_EVENTS`（`notify.event.>`）与 `<prefix>_DISPATCHES`（`notify.dispatch.>`），WorkQueue、文件存储、
  `replicas` 配置（生产 3）、保留 7 天；durable pull 消费者 `router` / `sender`（AckWait=`ack_wait`，MaxDeliver=`max_deliver`）。
  启动时 CreateOrUpdate 幂等。`/healthz` 追加 NATS 连接与流探测。
- **切换**：改 `backbone: nats` + `nats.url`（token 走 `KRATOS_NATS_TOKEN`）滚动重启即可；切回 pg 时未消费消息无害
  （pg 路径不看 publish_state，直接按 PG 行流转）。集成测试用内嵌 nats-server（`-tags=integration`）。

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

- **密钥落库形态**：core 配置了主密钥时 `notify_producer.token_hash` 为封存形态（`notifycontract.WrapSealedProducerKey`），
  管道用同一主密钥解封；未配置时为裸哈希，管道两种都接受。存量裸哈希由 core 启动时一次性补封存（幂等）。
  **发布顺序**：先升级 notify（能解封），再升级 core（开始写封存形态）；两边主密钥必须一致。
- **重放防护**：时间窗（±5min）内同一签名的请求只受理一次——实例内存表 + 共享表 `notify_ingress_replay`
  （迁移 0048，多副本同口径，过期行由维护循环清理）。带 `dedup_key` 的请求由去重门闩兜底，重发按幂等受理；
  不带的直接 401，指标 `notify_ingress_requests_total{result="replayed"}` 单列。
- **配额口径**：秒窗（`qps_limit`）按请求事件数计；日窗（`daily_limit`）只计实际落库的事件，被去重的重发不扣日配额。

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

- **停机**：各循环只关闭准入，在途迭代在不受取消的 ctx 上跑完（sender 一批最坏 ≈ batch/concurrency × max_send_timeout）；
  `kratos.StopTimeout` 30s 用尽才硬中止；发送后的落账（Finish/Release/熔断记录）恒不随取消失败，
  避免行停在 sending 直到租约到期被重发。
- **发送前瞬时错误**（取事件/渲染超时/写快照/限流器故障）放回 pending 5s 后重试且不计尝试，不烧租约。
- **配额**在 ingress 事务末尾消费，行锁只跨提交；超限整体回滚。
- **分区边界**恒为 UTC 月初（字面量带 `+00`），与会话 TimeZone 无关。
- **sender 往返**：一批任务的源事件一次取回（distinct event_id + create_time 剪枝）；首次渲染快照随 Finish 同事务
  写入（delivery_log + 终态 + dlq 一个 pgx.Batch）；渲染后被放回（熔断/限流）则下次重渲染（模板按 id+version 缓存）；
  熔断打开的渠道暂停/放回到 open_until 为止，不再每 5s 声明再放回。
- **router** 一批事件的终态单语句提交（unnest）。
- **积压指标** `notify_events_pending` / `notify_dispatch_due` 有界计数，读数 10001 表示饱和（≥ 1 万）。
- 群机器人实例 secret = access_token/key，目标群由实例决定；`config.at_mobiles` 作 @ 名单（钉钉/企微）。
- **渠道适配器口径（第四批）**：群机器人正文按平台**字节**上限截断（钉钉 markdown 18000B、企微 text 2000B / markdown 4000B、
  飞书 text 28000B），标题进 markdown 字段前转义控制字符；限流缺省按类型（钉钉/企微 20/min 容量 1、飞书 100/min 容量 5，
  `rate_limit.per_minute` 可显式配置，`qps` 优先）；SMTP 按应答码分级（4xx 可重试、5xx 死信），AUTH 按服务器通告选
  PLAIN > LOGIN > CRAM-MD5，Message-ID 域取发件地址域；webhook 出站 SSRF 防护——core 拒绝本机/私网字面量，管道在**解析后**
  对真实地址再校验（任一地址私网即拒绝，免疫 DNS 重绑定），429/503/408 的 `Retry-After` 透传给退避（上限 2h），
  幂等键同时放 `Idempotency-Key`；接收端校验请 import `backend/notify/pkg/webhooksig`（`VerifyWithin` 含时间窗）。
- **模板函数白名单**（core 校验与管道渲染同一集合 `kit/notifycontract.TemplateFuncs`）：`fmtTime t [layout]`、
  `severity n`（info/warning/error/critical）、`json v`、`trunc s n`、`upper`、`lower`；缺省正文群机器人紧凑版、
  邮件/webhook 多行版，载荷以 JSON 打印；非对象载荷注入为 `{"_raw": …}`。
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
