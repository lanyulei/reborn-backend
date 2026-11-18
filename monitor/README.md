# monitor — 监控告警评估引擎（数据面）

Kratos v3 单二进制，按 `SERVER_ROLES` 启用组件，与 core 共用 PostgreSQL：配置表只读（数据源的健康状态三列除外），
运行表读写。引擎**只读写 `monitor_*` 表**，不 import core / notify / cmdb，不解析收件人、不写 `notify_event`——
需要通知时只往 `monitor_notify_outbox` 写一行，由 core 的 `monitor-notify` 循环派发。
自动建单 / 触发流程 / AI 摘要只往 `monitor_alert_action` 写一行，平台自监控事件只往 `monitor_engine_event` 写一行，
同样由 core 执行。
设计见 `backend/docs/monitor-tech-design.md` §2–§4、§7、`monitor-tech-design-p2p3.md` 与 `monitor-foundation-notes.md`；
工程规范见 [`../notify/AGENTS.md`](../notify/AGENTS.md)。与 core 共享的只有表结构（`core/migrations/0069_monitor_init`、
`0076_monitor_p2`）和 `backend/kit` 的三个包：数据契约 `monitorcontract`（jsonb 结构、状态字面量、指纹、匹配器、
生效时间、工作日历）、事件流水线 `monitorpipeline`（标签改写、资源关联与富化、条件丢弃、资源范围、抑制、升级、
模板——与 core 的模拟触发同一份实现）与查询 / 写入客户端 `promclient`。

**发布顺序：迁移（0076）→ core 的全部实例 → 引擎。** 第二期引擎会写 `kind = 4` 的发件箱行，第一期的 core 不认识它
（见 `monitor-foundation-notes.md`「发布顺序」）；引擎启动时要读 0076 的表，库没迁移到 0076 时首次加载缓存即失败、
不评估任何任务。

## 组件与角色

| 角色 | 循环 | 职责 |
|---|---|---|
| `planner` | `materialize`（选主，5s） | 为「启用规则 × 其生效的启用数据源」补评估任务（首次评估时刻带 0–min(评估周期, 30s) 的随机抖动）；规则在任务上次评估之后被修改过 → 把任务提前到立即评估（每次修改只提前一次）；删除不再成立的任务，同事务删除其判定状态、把活跃告警置为已关闭（不发恢复通知，时间线记一条 `close`、原因 `rule`）。每轮至多清理 500 个任务，清满即连续执行。记录规则的任务（`monitor_record_task`）同样补齐 / 提前 / 清理 |
| `planner` | `probe`（选主，30s） | 对启用数据源并发（8）执行 `query=1`，写回 `health_status / health_error / health_time / health_since`（`health_since` = 当前状态自何时起保持不变）。从异常转为正常的数据源在同一事务里写恢复事件（见「平台自监控」）。探活走每个数据源的专用槽位，不排在评估查询后面 |
| `planner` | `maintenance`（选主，1h，启动即跑一轮） | `monitor_eval_record` 日分区：预建当天至未来 N 天（独立建表 → 迁出 DEFAULT 中重叠行 → ATTACH）、按保留期 DROP；分批删除过期的非活跃告警（连同其通知记录、发件箱行、时间线、事件动作、事件指令、升级进度）、已派发 / 已丢弃的发件箱行与自监控事件、已处理的事件指令、DEFAULT 分区里的过期行 |
| `planner` | `watch`（选主，30s） | 平台自监控的检测：数据源持续不可用、规则持续评估失败 → 写 `monitor_engine_event`；清理过期的心跳行 |
| `evaluator` | `evaluator-<i>`（并发可配，缺省 4） | 领取到期任务（SKIP LOCKED + 租约）→ 查询数据源 → 事件流水线 + 状态机 → 单事务落库（判定状态、告警事件、发件箱、时间线、事件动作、判定记录、任务写回） |
| `evaluator` | `recorder-<i>`（并发可配，缺省 1） | 记录规则：领取到期任务（与评估任务同一套租约）→ instant 查询 → 经数据源的 Remote Write 地址写回 → 写回任务 |
| `evaluator` | `command`（不选主，2s） | 消费 core 下达的事件指令（`monitor_alert_cmd`：手动关闭） |
| 全部 | `heartbeat`（10s） | 每个实例 upsert `monitor_engine_instance` 的一行；正常停机时删除 |
| — | HTTP 恒启动（8040） | `/healthz`（数据库可达）、`/metrics`（OTel → Prometheus）。没有业务接口 |

`SERVER_ROLES`（逗号分隔，优先于 `engine.roles`）为空 = 全部角色。

### 一轮评估（evaluator）

1. 领取：`UPDATE … WHERE (rule_id, datasource_id) IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`，
   租约 = `max(engine.evaluator.lease, 3 × min(数据源 timeout_ms, query_timeout_max) + 10s)`，全部用数据库时钟
   （三段各至多一个查询超时：等数据源并发槽、判定查询、恢复取值查询）。
   **一批任务并发评估**（并发度 = 批大小；对单个数据源另受并发槽约束），慢数据源上的任务不拖后同批其余任务
2. 读规则行（每轮都读）与数据库 `now()`；数据源、屏蔽规则、抑制规则、升级策略、资源索引、工作日历来自按
   `monitor_cfg_version` 失效的进程内缓存，抑制结论来自按周期刷新的活跃告警快照（见「抑制」）
3. 不在生效时间 → 只记一条判定记录（status 5），不改状态。生效时间为「仅工作时间 / 仅非工作时间」
   （`effective.mode` = `work_time` / `off_time`）时按工作日历（`monitor_calendar`）判定；**日历尚未物化或配置非法时
   这两种规则恒生效**（宁可多评估，不因缺日历漏告警）
4. 逐条查询执行 instant query（多条并发）。每条查询先等该数据源的并发槽（单实例 8 个），**查询超时从拿到槽
   之后才起算**；等槽另有预算（一个查询超时），超出则这一轮顺延（见下文「并发槽与顺延」），不算查询错误。
   **任一查询出错：整轮记为查询错误，不改任何状态、绝不判恢复**。
   结果必须是瞬时向量：标量（如 `time()`，恒有值会无条件触发）、区间向量、字符串一律记为查询错误
   （`query must return an instant vector, got a scalar`）
5. 事件流水线的前半段（纯计算，`mergeSamples`）：去掉 `__name__`、把上游标签收进上限（见下文「上游标签上限」）、
   合并规则附加标签（附加标签优先）→ 标签改写 → 算指纹 → 关联资源并写入 `ci_` 富化标签 → 条件丢弃 → 资源范围
   过滤 → 级别抑制（同序列只留最高级别）。见「事件流水线」
6. 对将要判恢复的事件执行恢复取值查询（每轮至多 20 条，失败不阻塞恢复）
7. 开事务：围栏锁任务行 → `FOR UPDATE` 读判定状态（规则引用了升级策略时再用一条语句读各级的通知进度）→
   纯函数 `biz.Advance` 走完流水线的后半段（规则间抑制 / 拓扑抑制 → 屏蔽与维护态 → 状态转移 → 通知、升级、
   时间线、事件动作）并算出全部变更 → 批量落库 → 写回任务。
   下一次评估时刻 = `now() + 评估周期 + 抖动`（抖动 ≤ min(1s, 周期 / 10)）

**网络 I/O 与模板渲染全部发生在事务之外**（附加信息、恢复取值、条件丢弃表达式都是）；事务内只有纯计算与批量写
（`unnest` 单语句 + 一个 `pgx.Batch`），一轮的语句数固定（十余条，第二期至多再加四条：升级进度、时间线、事件动作、
恢复事件），不随序列数增长。本轮要新建事件且规则配置了附加信息时：第一次提交在事务内只算出
「哪些样本要建事件」便回滚（什么都没写），附加信息在事务外渲染后再提交一次（租约围栏保证其间状态不被别人改动）。

### 并发槽与顺延

单实例对同一个数据源至多 8 条评估查询在途。排不上槽的查询最多等一个查询超时；等到了，查询本身仍有完整的
查询超时（健康但慢的数据源在槽位排满时不会被记成「上游超时」）。等不到，这一轮**顺延**：任务归还、5 秒后
重试，指标记 `busy`，不写判定记录、不记查询错误、不动 `last_eval_time` / `last_status`。

顺延的那一轮什么都没观察到，口径与查询错误、不在生效时间的轮次相同：下一次评估成的那一轮，待触发的持续时长
与留观时长都**重新起算**。标记记在任务行上（`lease_owner` 为空而 `lease_until` 是一个已过期的时刻），由任何
实例的下一次领取带回，评估完成（含记为查询错误 / 不在生效时间的一轮）才清除；领取后没评估就归还（停机移交、
规则 / 数据源待清理）不清除。**槽位持续排满时，`for_s > 0` 的规则可能一直攒不满持续时长**——与数据源持续
间歇故障时相同；`datasource query slots are saturated` 日志与 `busy` 指标即是信号，应降低该数据源上的规则
密度或提高其查询性能。

### 结果不完整：截断与部分结果

任一查询被 `max_series` 截断，或上游声明是部分结果（VictoriaMetrics 的 `isPartial`）时，这一轮**没返回的序列是
「未知」而不是「消失」**（部分结果只识别 VictoriaMetrics 的 `isPartial`：Thanos / Mimir 用响应里的 `warnings`
表达部分响应，引擎不解析 `warnings`，这类结果按完整结果处理——见「已知限制」）：

- 返回了的序列照常判定（可以进入待触发、触发、刷新）
- 没返回的序列：告警中的保持告警中（不进入留观）、留观中的不恢复且留观从本轮重新起算、待触发的不删除
  （下次见到时持续时长重新起算；连续 1 小时没见到才清理）
- 不执行恢复取值查询
- 评估结果（`last_status` / 判定记录 `status`）仍是正常的 1 / 2 / 3；任务的 `last_error` 与判定记录的 `error`
  写明原因，固定前缀 `incomplete result: `（如 `incomplete result: query 0 truncated to 2000 series; series not
  returned were left unchanged`）；判定记录明细的 `queries[].truncated` / `queries[].partial` 标出是哪条查询
- 进入 / 离开该状态各记一条日志（WARN / INFO），持续期间每 10 分钟提醒一次

截断是确定的：超出上限时保留**按标签集排序最靠前的 `max_series` 条**（`kit/promclient`），与上游的返回顺序、
由哪个实例评估无关；响应流式解码，内存中至多保留 `max_series` 条序列（响应体另有 16MiB 上限）。
被截断的规则应当拆分或收窄表达式——排在上限之外的序列在截断期间不会触发新告警。

### 上游标签上限

上游序列的标签（去掉 `__name__` 之后、合并规则附加标签之前）有上限，超出的**删减 / 截断后照常判定**：

- 单条序列至多 64 个标签：超出时保留名称排序最靠前的 64 个（与上游返回顺序无关，指纹稳定）
- 标签名至多 128 字节：超出的标签整个丢弃
- 标签值至多 1024 字节：超出的截断（不切断字符），并以 `...[truncated]` 结尾

有序列超限的轮次，任务的 `last_error` 与判定记录的 `error` 带一段固定前缀 `label limit: ` 的说明
（`label limit: 3 series exceeded the label limits (at most 64 labels per series, 128 bytes per name, 1024 bytes
per value); extra labels were dropped and long values truncated`）；结果同时不完整时它接在 `incomplete result: …`
之后，以 `; ` 分隔。它只是提示：评估结果照常、结果仍算完整（没出现的序列照常留观 / 恢复）。删减后标签集相同的
序列是同一个事件。

### 状态机（`internal/biz/state.go` 的 `Advance`，纯函数）

| 上一轮 | 本轮出现 | 本轮未出现 |
|---|---|---|
| 无状态 | → 待触发；`for_s = 0` 直接走「触发」 | — |
| 待触发 | 未满 `for_s`：保持；已满：**触发** | 删除状态行（无事件、无通知） |
| 告警中 | 事件内容有变化或到了心跳间隔才写事件行（见下）；到达重复间隔且未达 `max_notify` → 重复通知；首次通知被丢弃 → 按间隔重试（见下） | → 留观中；留观时长为 0 时直接**恢复** |
| 留观中 | → 回到告警中（不新发通知，留观重新计时） | 未满留观：保持；已满：**恢复** |

「本轮未出现」一列只适用于结果完整的轮次；结果不完整时见上一节。

- 留观时长 = `max(recover_s, nodata_keep_s)`
- **计时只累计观察到的时间**：待触发的序列上一轮没被观察到（查询错误、不在生效时间、排不上并发槽被顺延、
  结果不完整且没返回它）时，持续时长从本轮重新起算（`active_since` 重置）；留观中的序列跨过查询错误 /
  不在生效时间 / 被顺延的轮次后，留观从本轮重新起算。数据源故障期间既不算「持续异常」也不算「持续正常」
- **活跃事件行不是每轮都写**：仅当 value / severity / 屏蔽标记（`muted`、`mute_id`）有变化，或 `last_fire_time`
  距上次写入已满 `max(60s, 评估周期)` 时才 UPDATE `monitor_alert`。因此 `last_fire_time` 的含义是「最近一次确认
  仍在告警的时刻，精度为该心跳间隔」，`value` 是最近一次变化后的值。判定状态行（`monitor_eval_state`）仍每轮刷新
- **触发**：命中「屏蔽事件」（mode 2）→ 不建事件，状态保持待触发；否则建事件（活跃事件部分唯一索引兜底），
  命中「只屏蔽通知」（mode 1）→ `muted = true`、不写发件箱，否则写发件箱 kind 1 / seq 1
- **恢复**：事件置已恢复并写恢复值；`notify_recovery` 且未被屏蔽且此前通知过（已发出或已入箱待发）→ 发件箱 kind 3
- 事件已存在时，任何方式的屏蔽规则都只屏蔽它的通知；屏蔽解除而仍在告警 → 下一轮立即按一次新通知处理
- 多条屏蔽规则同时命中：屏蔽事件优先于只屏蔽通知，同一方式取 ID 最小者
- 发件箱 `seq` = 已发送的触发 / 重复通知数 + 1（恢复通知同）；`(alert_id, kind, seq)` 唯一，重复写入是空操作
- **首次通知被丢弃后的重试**：事件未被屏蔽、`notify_count = 0`、发件箱里没有待派发的触发 / 重复通知、且首次
  通知行（kind 1 / seq 1）处于已丢弃状态（core 派发失败次数用尽）时，距丢弃（该行的 `done_time`）每满一个重复
  通知间隔——规则没有配置重复通知时为 10 分钟——把**同一行**重新置为待派发一次。节奏有上限：不是每轮都重试，
  不新增行；重试后的行是待派发，core 再次用尽次数丢弃后要再等一个间隔；首次通知还在排队（含 core 失败重试中）
  的事件不动。一旦送达，重复通知与恢复通知照常
- **重新激活**：屏蔽解除后的那次通知、每一次重复通知、以及上面的首次通知重试，写入时若同键的行已被 core
  丢弃（status 3），会把它重新置为待派发（`attempts` 清零）。屏蔽解除的那一轮恒执行这次写入，即使发件箱里还有一条待派发的行——
  core 可能正按「事件被屏蔽」丢弃它；两边在该行的行锁上串行，通知不会丢。已派发的行永不重新激活
- 规则参数中途变更不重置已有状态，下一轮按新参数继续判定；规则保存后至多一个物化周期（5s）即被重新评估，
  不必等旧的评估周期走完

### 需要知道的行为

- **级别升级不单独通知**：开启级别抑制时同一序列的各档是同一个事件，从警告升到严重只更新事件的 `severity`，
  不新发通知（下一次重复通知会带上新级别）
- **生效时间之外，已触发的告警保持告警中**：不在生效时间的轮次不查询、不改状态，既不恢复也不发重复通知；
  回到生效时间后接着判定
- **修改规则的附加标签或切换级别抑制，该规则的活跃告警会先恢复再重新触发**：两者都会改变事件指纹，
  原事件按「序列消失」走留观 → 恢复（发恢复通知），新指纹从待触发重新走 `for_s`
- **附加信息只在首次触发时渲染一次**：之后 value 变化、规则的附加信息模板被修改都不会更新已有事件的
  `annotations`（新事件用新模板）
- 停用规则、把数据源移出规则范围、停用 / 删除数据源：活跃告警置为已关闭（status 3），不发恢复通知

### 事件流水线（第二、三期）

一个样本依次经过（顺序固定，与 core 的模拟触发同一份 `kit/monitorpipeline`）：

| 阶段 | 在哪 | 说明 |
|---|---|---|
| 标签改写 | `mergeSamples` | 规则的 `relabel`（replace / labelmap / labeldrop / labelkeep）。**在指纹之前**：改写结果进指纹，改写规则一变，已有的活跃告警按新指纹重新产生 |
| 关联资源 + 富化 | `mergeSamples` | 按规则的 `ci_match`（缺省 `ident` → 资源名称，其次 `instance` → `ip` 属性，`host:port` 自动去端口）在资源索引里找唯一的资源；写入 `ci_env` / `ci_owner` / `ci_model` / `ci_tag_<标签>`。**在指纹之后**：`ci_` 标签不进指纹，资源属性变化不会让告警重建。对应到多个资源 = 不关联 |
| 条件丢弃 | `mergeSamples` | `drop_expr` 渲染结果恰为 `true` 即丢弃（能读到 `ci_` 标签）。渲染失败**不丢弃**，条数记进判定记录 / 任务的 `error`（前缀 `drop expression: `） |
| 资源范围 | `mergeSamples` | `resource_scope` 启用时，没有关联到资源或资源不满足模型 / 环境 / 标签条件的样本不产生事件。**资源索引尚未加载时这类规则不产生任何事件** |
| 级别抑制 | `mergeSamples` | 第一期行为：同指纹只留最高级别。被丢弃 / 范围外的样本不参与（它们不会抑制别的级别） |
| 规则间抑制 / 拓扑抑制 | `Advance` | 见「抑制」 |
| 屏蔽 | `Advance` | 屏蔽规则（第一期）；另外**关联的资源处于维护态（`status = maintenance`）时恒按「只屏蔽通知」处理**：`muted = true` 而 `mute_id` 为空。屏蔽规则同时命中时记屏蔽规则 |
| 落库、通知 | `Advance` + data | |

被丢弃、范围外的样本不进状态机：对已有的事件而言等同于「本轮没有出现」（走留观 → 恢复）。判定记录明细里它们的
`step` 是 `dropped` / `out_of_scope`；已建事件的点另有 `inhibited_rule` / `inhibited_topology` / `muted_maintenance`，
并带 `ci_id`、`inhibit_alert_id`。

### 抑制

- **规则间抑制**（`monitor_inhibit`）：语义同 Alertmanager——目标在规则的作用分组内、满足目标条件，且有同在作用分组内、
  满足源条件、相等标签取值全部相同的活跃告警。**拓扑抑制**：规则开了 `topology_inhibit`、事件关联了资源，且该资源的
  任一上游资源（`monitor_ci_index.upstream_ids`）上有活跃告警
- 被抑制的事件**照常建立并留档**：`inhibited = true`、`inhibit_kind`（1 规则 / 2 拓扑）、`inhibit_id`（抑制规则）、
  `inhibit_alert_id`（源告警）；不通知、不升级、不入队事件动作；恢复时不发恢复通知。先判抑制、后判屏蔽，两者可以
  同时成立（判定记录的 `step` 记抑制）
- **解除**后仍在告警：按一次新通知处理（同屏蔽解除）——从未通知过的发首次通知；通知过的补一条重复通知，但**距上次
  通知不足 5 分钟时不补**（源告警反复触发 / 恢复时，被它抑制的每个事件至多每 5 分钟通知一次，不会风暴）。已认领、
  已达 `max_notify`、「接替」策略已升级的不补

**一致性模型。** 抑制是「此刻全部活跃告警」的一个确定函数，不读告警行上的 `inhibited` 标记：设 U 为活跃告警
（含被屏蔽的、留观中的），求集合 S ⊆ U 使得「S 里的告警都不被 S 抑制，S 之外的都被 S 抑制」——这正是「被抑制的告警
不能再抑制别人」（a 抑制 b、b 抑制 c 时 S = {a, c}）。S 用交替不动点求出；抑制关系无环时唯一，与告警的评估顺序、
由哪个实例评估无关。**有环**（A 抑制 B、B 抑制 A）时没有唯一解：把环上「抑制它的告警比它自己更晚触发」的告警
（按事件 ID，UUIDv7）定为未被抑制，于是每个环上最早触发的那个照常通知、其余被抑制，结果只取决于数据；
`monitor_inhibit_cycle_alerts_total` 与一条 WARN 即是「抑制规则成环」的信号。遍数有上限（32，足够三十级左右的抑制链），
用尽时剩下的一律按未被抑制处理（宁可多通知）。

各实例各自每 `inhibit_refresh_interval`（缺省 5s）读一次活跃告警并求 S，评估时每个样本只做一次按事件 ID 的查表；
快照里没有的事件（快照之后才触发的）对着 S 现算。**抑制的生效与解除最多滞后「刷新周期 + 该规则的评估周期」**；
各实例的快照不必一致——结论只取决于库里的活跃告警，不同实例先后评估同一个任务不会让标记来回翻转（除非数据真的变了）。
尚未成功加载过快照的实例不评估（否则会把已被抑制的告警当成「抑制解除」补发通知）。

规模：一次刷新 ≈ 活跃告警数 × 抑制规则数 × 2–3 遍（2100 个告警 × 20 条规则约 3ms）。快照至多装入
`inhibit_max_alerts`（缺省 20000）个活跃告警，超出的仍可被抑制但不能作为抑制源；抑制规则至多加载 500 条。
两者都顶到上限时一次刷新是秒级，刷新周期从算完起计。超限时记 WARN 并累加 `monitor_cache_truncated_total`。

### 认领、手动关闭

- **认领**（core 写 `ack_user` / `ack_time`）：引擎每轮随状态一起读到。已认领的事件停止规则自身的全部通知（重复通知、
  屏蔽 / 抑制解除后的新通知、首次通知的重试）与「未认领」类的升级；恢复通知照发。**取消认领**后按原节奏接续：
  距上次通知已超过重复间隔的立刻补一条，之后回到每个间隔一条，不会把认领期间攒下的次数一次发完
- **手动关闭**：core 写一条 `monitor_alert_cmd`（`kind = 1`），引擎的 `command` 循环执行——一个事务里：锁指令行
  （`FOR UPDATE SKIP LOCKED`）→ 锁事件所属的评估任务行 → 事件置 `status = 3` 并写 `closed_by` / `close_reason` /
  `resolve_time` → 删除它的判定状态 → 时间线记 `close`（`{"cause":"manual","text":<原因>}`，`operator` = 操作人）→
  已关联工单的入队一条 `ticket_note` → 指令置已执行（`status = 2`）。事件已不在告警中（或不存在）→ 指令置未执行
  （`status = 3`，`result` = `alert is not firing` / `alert not found`）。**不发恢复通知**
- 关闭之后**条件仍成立**：状态行已删，下一轮该序列按新序列进入待触发，重新走 `for_s`，满了建一条新事件
- 多实例：每个评估器实例都跑 `command` 循环；指令行的行锁保证一条指令只被执行一次，任务行的行锁让关闭与该任务的
  评估提交串行（关闭要么在某一轮提交之前、要么在之后）。事务失败则指令仍是待处理，下一轮重试

### 升级

规则引用了启用的升级策略（`escalation_id`；策略不存在 / 已停用 / 已删除 / 不合法 = 没有策略）时，仍在告警的事件每轮
调一次 `monitorpipeline.Escalate`（输入：策略、首次触发时间、级别、是否认领、是否被屏蔽 / 抑制、各级的通知进度），
到期的通知写发件箱 `kind = 4`、`level` = 级数、`seq = level × 100000 + 级内序号`（`monitorcontract.EscalationOutboxSeq`）。

- 各级进度 = core 回写的 `monitor_alert_escalation`（已通知次数、时间）+ 发件箱里该级未派发成功的行（待派发 → 不追加；
  下一次通知的行已被丢弃 → 过一个该级的重复间隔，未配置为 10 分钟，把**同一行**重新置为待派发）。两者在同一条语句里
  读出（同一个快照）
- 叠加：各级与规则的重复通知各按各的间隔与上限；接替：升级后只提醒到达的最高级，规则的重复通知与更低的级停止
- 认领：停止「未认领」类的级与规则的重复通知，「未恢复」类的级不受影响；低于策略 `min_severity` 的事件不升级
- 被屏蔽、被抑制、留观中（序列本轮没出现）的事件不升级；恢复 / 关闭后不再有任何升级
- `monitor_alert.escalation_level` = 到达过的最高级，只增不减

### 时间线与事件动作

引擎写的时间线行（`monitor_alert_event`，与状态变更同一事务）：`fire`、`resolve`（`{"severity"}`）、`close`、
`mute` / `unmute`（`{"mute_id"}`，因维护态时为 `{"ci_id"}`）、`inhibit` / `uninhibit`（`{"inhibit_kind","inhibit_id",
"inhibit_alert_id"}`）、`escalate`（`{"level"}`，只在升到该级时记一次；级内的重复提醒在通知记录里）。
**有上限**：`fire` / `resolve` / `close` 每个事件各至多一条；会随抖动反复产生的五类（`mute` / `unmute` / `inhibit` /
`uninhibit` / `escalate`）每个事件合计约 200 条，达到后不再写（告警行上的状态照常更新）。

事件动作（`monitor_alert_action`，`(alert_id, kind)` 唯一，重复写入是空操作）：

- 事件**第一次可以对外通知时**（触发时未被屏蔽 / 抑制，或屏蔽 / 抑制解除的那一轮）按规则当时的 `actions` 入队
  `ticket`（1）/ `workflow`（2）/ `ai_summary`（3），`payload` 是入队那一刻的规则配置
- **被屏蔽（含维护态）、被抑制期间不入队**——没人该被打扰的事件不自动建单；命中「屏蔽事件」的根本不建事件
- 恢复时：事件已关联工单（`ticket_id`），或规则开了自动建单（工单可能还在 core 的队列里）→ 入队 `ticket_note`（4）。
  手动关闭时只有已关联工单的入队
- 模拟触发的事件不经过引擎，没有动作

### 平台自监控

- **心跳**：每个实例每 `heartbeat_interval` upsert `monitor_engine_instance`：`instance`（主机名 + 启动时的随机串）、
  `roles`、`version`、`leader`（最近是否以主身份执行过任务物化）、`task_count` / `eval_lag_ms`（上一个心跳周期内完成的
  评估数与最大评估延迟）、`eval_count` / `fail_count`（启动以来；记录规则的执行也计入）。正常停机删除自己的行；
  被强杀的由 `watch` 在 `instance_stale_after` 后清理
- **数据源持续不可用**：探活连续失败满 `datasource_down_after` → `monitor.engine.datasource_down`；恢复 →
  `monitor.engine.datasource_recovered`
- **规则持续评估失败**：任务连续查询错误满 `rule_failing_after` 且不少于 `rule_failing_min_failures` 次
  （`monitor_eval_task.error_since` / `error_count`；不在生效时间的轮次不计也不清零），**且其数据源同样长时间保持
  可用** → `monitor.engine.rule_failing`；评估成功的那一轮 → `monitor.engine.rule_recovered`。数据源不可用期间只报
  数据源，不逐条报它上面的规则
- **每次状态翻转恰好一条**：`dedup_key = <事件类型>:<对象 ID…>:<故障起始时刻的微秒数>`（规则类事件的对象 = 规则 ID +
  数据源 ID），起始时刻落在库里（`health_since` / `error_since`），换哪个实例检测、检测多少遍都是同一个键。恢复事件
  与故障事件用同一个起始时刻配对，只在故障事件发过时才写，并且与状态翻转同一事务；故障事件的写入与状态翻转在
  数据源行 / 任务行上加锁串行
- `detail` 是 `monitorcontract.EngineEventPayload`；`since` 为 UTC 文本（`2006-01-02 15:04:05 +00:00`），`duration`
  为中文短语（`3 小时 20 分钟`）

### 记录规则

`monitor_record_rule` × 其数据源 → `monitor_record_task`，领取 / 租约 / 围栏 / 抖动与评估任务是同一套代码。一轮：
instant 查询（与评估查询共用该数据源的并发槽）→ 每条结果序列去掉原指标名、套上规则标签（取值为空 = 删除该标签）、
指标名改为规则名、带最新的一个采样点 → 经数据源的 `remote_write_url` 以 Prometheus Remote Write 写回（每个请求
至多 500 条序列 / 约 4MiB 标签，大结果分多次）→ 写回任务（`last_status` 1 已写回 / 2 查询无结果 / 3 查询失败 /
4 写回失败、`last_error`、`last_series`）。

- **结果不完整（被 `max_series` 截断，或上游声明是部分结果）时整轮不写**，`last_status = 3`、`last_error` 以
  `incomplete result: ` 开头。只写一部分序列的话，其余序列在新指标里就成了「消失」，引用它的告警规则会据此判恢复；
  整轮缺一个点只是一次普通的数据缺口。所以**结果超过 `max_series` 条序列的记录规则永远写不出去**，应收窄表达式
- 标签写不成合法指标的序列（标签名不合法或以 `__` 开头、取值非 UTF-8 / 含 NUL / 超过 1024 字节、标签超过 84 个）
  被跳过并在 `last_error` 里计数；全部被跳过算写回失败。处理后标签完全相同的两条序列使整轮失败（查询失败）
- 标量结果写成一条只有规则标签的序列；区间向量 / 字符串是查询失败
- 写回是**至少一次**：写入发生在围栏检查之前，租约过期被重领时同一轮可能写两次（各自查询得到的采样点）
- 分片写到一半失败：已写的分片不回滚，`last_series` 记已写的条数，`last_error` 带 `wrote N of M series`
- 没有配置写入地址、写入地址非法：写回失败（core 保存记录规则时已拒绝没有写入地址的数据源，这里是兜底）

### 模板（附加信息、恢复取值 PromQL、条件丢弃）

Go `text/template`，前置注入 `$labels` / `$value`，兼容 `{{ $labels.instance }}`、`{{ $value }}`。
校验规则只有一份：`kit/monitorcontract.ValidateTemplate`，执行也只有一份：`kit/monitorpipeline.CompileTemplate` /
`Template.Render`（引擎不再有自己的模板实现），core 保存规则、模拟触发与引擎渲染调用同一套函数；
未通过校验的模板引擎一律不执行，该项取值为 `<template error>`。规则：

- 函数白名单：`printf`、`humanize`、`humanizePercentage`、`humanizeDuration`、`toUpper`、`toLower`、`title`，
  以及 `if` 判断用的 `eq ne lt le gt ge and or not`；其余内建函数（`index` / `len` / `call` / `print` …）不可用
- `printf` 的格式串必须是字符串字面量：宽度 / 精度 ≤ 256，不允许 `*` 与显式参数索引 `%[n]`
- 不得声明或赋值 `$labels`、`$value`、`$`；任何变量都不得用 `=` 重新赋值（`{{ $x := … }}` 声明新变量可以）
- `range` 只能写成 `{{ range $labels }}` / `{{ range $k, $v := $labels }}`，且不得嵌套；`{{ range .Labels }}`
  不允许（点可以被 `with` 改绑），取值仍可用 `{{ .Labels.instance }}`
- 不允许 `template` / `block` 调用；`{{ define }}` 的子模板按同样规则检查
- 模板源 ≤ 8KB

渲染在数据库事务之外进行：输出 ≤ 4KB、≤ 100ms，失败时该项取值为 `<template error>`，不影响触发。
不存在的标签渲染为空串。

## 配置

`configs/config.yaml`（`internal/conf/conf.proto`）。未设置或 ≤ 0 的值取缺省；密钥类一律走环境变量。

| 键 | 缺省 | 说明 |
|---|---|---|
| `server.http.addr` | `0.0.0.0:8040` | |
| `data.database.source` | — | 必填，`KRATOS_DATA_DATABASE_SOURCE` |
| `data.database.max_conns` | 16 | 须大于 `evaluator.concurrency + 3` |
| `engine.roles` | 全部 | `planner` / `evaluator`；`SERVER_ROLES` 优先 |
| `engine.planner.materialize_interval` | 5s | |
| `engine.planner.probe_interval` / `probe_timeout` | 30s / 5s | |
| `engine.planner.watch_interval` | 30s | 自监控检测周期 |
| `engine.planner.datasource_down_after` | 5m | 探活连续失败多久写 `datasource_down` |
| `engine.planner.rule_failing_after` / `rule_failing_min_failures` | 10m / 3 | 连续查询错误多久、至少多少次写 `rule_failing` |
| `engine.planner.instance_stale_after` | 1h | 心跳行多久没更新即清理 |
| `engine.evaluator.poll_interval` | 1s | 无到期任务时的轮询周期 |
| `engine.evaluator.concurrency` | 4 | 评估循环数 |
| `engine.evaluator.batch` | 10 | 单次领取的任务数，也是一条循环内并发评估的任务数（单实例在途评估上限 = `concurrency × batch`） |
| `engine.evaluator.lease` | 60s | 租约下限；实际租约 = max(它, 3 × 数据源查询超时 + 10s) |
| `engine.evaluator.max_series` | 2000 | 单条查询的序列数上限；超出时保留按标签排序最靠前的这么多条，本轮按「结果不完整」处理 |
| `engine.evaluator.query_timeout_max` | 30s | 单次查询超时上限 |
| `engine.evaluator.inhibit_refresh_interval` | 5s | 抑制快照的刷新周期 |
| `engine.evaluator.inhibit_max_alerts` | 20000 | 抑制快照装入的活跃告警数上限 |
| `engine.evaluator.max_ci` | 100000 | 资源索引加载的资源数上限（按资源 ID 升序保留；约 0.6 KB / 资源，10 万个约 55 MiB） |
| `engine.evaluator.command_interval` / `command_batch` | 2s / 100 | 事件指令的轮询周期与每轮条数 |
| `engine.recorder.concurrency` / `batch` | 1 / 10 | 记录规则循环数与单次领取数 |
| `engine.heartbeat_interval` | 10s | 心跳周期 |
| `engine.maintenance.interval` | 1h | |
| `engine.maintenance.partitions_ahead` | 2 | 预建未来分区天数 |
| `engine.maintenance.eval_record_retention` | 7d | |
| `engine.maintenance.alert_retention` | 180d | 显式 `0s` = 不删除。也是时间线 / 事件动作 / 事件指令 / 升级进度（随告警）与自监控事件的保留期 |
| `notify.master.key` | 空 | `KRATOS_NOTIFY_MASTER_KEY`，与 core 同一主密钥，用于解密数据源的 Basic 密码 / Bearer 令牌 |

主密钥未配置（或与 core 不一致）时进程照常启动：需要凭据的数据源每轮记为查询错误
（`datasource credential unavailable: master key missing or mismatched`）、探活记为异常，其余数据源不受影响。

数据源地址：私网与回环地址放行（数据源天然在内网）；解析后的实际地址若是链路本地 / 云元数据地址
（169.254.0.0/16、fe80::/10、fd00:ec2::254、阿里云 100.100.100.200、Oracle Cloud 192.0.0.192）、未指定地址或
组播地址，连接前即被拒绝；IPv4 映射的 IPv6 地址与 NAT64 知名前缀 `64:ff9b::/96` 下的地址按内含的 IPv4 地址
判定（`promclient.GuardLinkLocal`，评估查询、恢复取值查询与探活同一口径），错误类别 `blocked`。

数据源返回非 2xx 时，错误信息只含状态码及其标准说明（`promclient: http (http 502): Bad Gateway`）；响应体是
Prometheus 错误结构（`"status":"error"`）时取其中的 `errorType` / `error`。响应体的其余内容不进错误信息、
判定记录与日志。

屏蔽规则至多加载 10000 条（启用且未过期，按 ID 升序即先创建的优先）；超出时其余规则**不生效**，
引擎记 WARN 并累加 `monitor_mutes_truncated_total`。抑制规则至多 500 条、升级策略至多 5000 条、资源至多 `max_ci`
个，口径相同（指标 `monitor_cache_truncated_total{cache}`）。

资源索引（`monitor_ci_index`）增量加载：版本号变化时只读 `sync_time` 晚于游标（回退 1 分钟）的行并入内存里的集合，
`valid = false` 的行从集合里去掉；每 10 分钟整表重读一次，兜住增量读不到的情况（行被物理删除、物化事务提交得比
游标晚 1 分钟以上）。所以 core **改了一行的内容或 `valid` 就必须把该行的 `sync_time` 写成 `now()`**，否则这次修改
最迟 10 分钟后才生效。

## 快速开始

```bash
# 1. 数据库迁移到 0076 及以后（在 backend/core：go run ./cmd/migrate -conf configs up），并先发布 core
# 2. DSN 与 core 相同（本地取 backend/core/configs/config.yaml 的 data.database.source）
export KRATOS_DATA_DATABASE_SOURCE='<DSN>'
# 3. 可选：数据源带凭据时
export KRATOS_NOTIFY_MASTER_KEY=<与 core 相同的主密钥>
# 4. 运行（在 backend/monitor）
make run                                   # kratos run
go run ./cmd/monitor -conf configs         # 或直接运行
SERVER_ROLES=evaluator go run ./cmd/monitor -conf configs   # 只跑评估器
# 5. 自检
curl http://127.0.0.1:8040/healthz
curl -s http://127.0.0.1:8040/metrics | grep '^monitor_'
```

Go 模块在本环境须经 `GOPROXY=https://goproxy.cn,direct` 下载。

## DB 角色

生产环境的登录账号只需是 `monitor_engine_role` 的成员（迁移 0069 创建并授权，0076 增补）：

```sql
CREATE ROLE monitor_engine LOGIN PASSWORD '…' IN ROLE monitor_engine_role;
```

| 表 | 权限 |
|---|---|
| `monitor_datasource` | SELECT；UPDATE 仅 `health_status, health_error, health_time, health_since` |
| `monitor_rule_group` / `monitor_rule` / `monitor_mute` / `monitor_cfg_version` | SELECT |
| `monitor_escalation` / `monitor_inhibit` / `monitor_record_rule` / `monitor_ci_index` / `monitor_calendar` | SELECT |
| `monitor_record_task` / `monitor_engine_instance` | SELECT / INSERT / UPDATE / DELETE |
| `monitor_alert_event` / `monitor_alert_action` / `monitor_engine_event` | SELECT / INSERT / DELETE（写入后不再修改；状态由 core 按列回写） |
| `monitor_alert_cmd` | SELECT / DELETE；UPDATE 仅 `status, result, done_time` |
| `monitor_alert_escalation` | SELECT / DELETE（次数由 core 写） |
| `monitor_eval_task` / `monitor_eval_state` / `monitor_alert` / `monitor_eval_record` / `monitor_notify_outbox` | SELECT / INSERT / UPDATE / DELETE |
| `monitor_notify_log` | SELECT / DELETE（随过期告警清理） |
| `monitor_eval_record` 及其分区 | 属主（建 / 挂接 / 删除分区），另有 `public` schema 的 CREATE |

引擎新建的分区会把属主对齐为父表属主（`monitor_engine_role`），更换登录账号后仍可维护。
集成测试的全部仓储都以一个临时的真实 LOGIN 角色登录（密码认证、非超级用户、自身没有任何授权，
仅是 `monitor_engine_role` 的成员；每个测试新建、结束即删），并断言越权语句（改配置表、读订阅 / 其他域的表、
写通知记录等）被数据库拒绝（`TestEngineRoleIsRestricted`、`TestEngineRunsAsARestrictedLoginRole`、第二期的
`TestP2EngineRoleGrants`）。告警行上 `ack_user` / `ack_time` / `ticket_id` / `workflow_instance_id` / `ai_summary` 与
两个通知计数列由 core 写，引擎从不写它们。

### 分区清理为什么是 DROP

过期分区用 `DROP TABLE` 删除，需要父表与 DEFAULT 分区的 ACCESS EXCLUSIVE。不持有该锁的做法只有
`DETACH PARTITION … CONCURRENTLY`，它在本表上不可用（PostgreSQL 18 实测，`TestDetachConcurrentlyIsUnavailable`
钉住该结论）：父表有 DEFAULT 分区时直接报错 `cannot detach partitions concurrently when a default partition
exists`；它也不能在事务块内执行；而普通 `DETACH` 拿的锁与 `DROP` 完全相同。DEFAULT 分区是分区维护失效时的兜底
（写入不会因缺分区而失败），不为此去掉。影响已压到最小：每个分区一个极短的事务、`lock_timeout 2s`，拿不到锁
就放弃且本轮不再尝试其余分区，下一轮（1h）重试；评估写入最多被排队的锁请求挡 2s。

## 指标

| 指标 | 说明 |
|---|---|
| `monitor_evaluations_total{result}` | 评估次数：`ok` / `firing` / `nodata` / `query_error` / `inactive` / `skipped`（规则或数据源待清理）/ `busy`（本实例对该数据源的并发槽在等待预算内排不上，任务归还 5s 后重试，不记查询错误；下一轮计时重新起算）/ `lease_lost`（写回被围栏拒绝）/ `failed`（引擎自身故障） |
| `monitor_evaluation_duration_seconds` | 单次评估耗时（查询 + 状态转移）直方图 |
| `monitor_evaluation_lag_seconds` | 评估延迟：领取时刻 − 应评估时刻 |
| `monitor_outbox_rows_total{kind}` | 写入发件箱的行数：`firing` / `repeat` / `resolved` / `escalation` |
| `monitor_cache_truncated_total{cache}` | 缓存加载被上限截断的次数：`inhibit`（抑制规则）/ `escalation`（升级策略）/ `ci`（资源索引）/ `inhibit_alert`（抑制快照里的活跃告警）。非 0 = 有配置或数据没有生效 |
| `monitor_inhibit_cycle_alerts_total` | 因抑制规则成环而被定为「未被抑制」的告警数（每次快照刷新累加）。持续增长 = 有成环的抑制规则 |
| `monitor_record_runs_total{result}` | 记录规则执行次数：`ok` / `nodata` / `query_error` / `incomplete`（结果不完整，整轮未写）/ `write_error` / `busy` / `skipped` / `lease_lost` / `failed` |
| `monitor_record_duration_seconds` / `monitor_record_series_total` | 记录规则单次耗时直方图 / 写回的序列数 |
| `monitor_alert_commands_total{result}` | 事件指令：`done`（已关闭）/ `rejected`（事件已不在告警中）/ `failed`（引擎故障，下一轮重试） |
| `monitor_engine_events_total{type}` | 写入的自监控事件数（`rule_recovered` 随评估写回入箱，不计） |
| `monitor_mutes_truncated_total` | 屏蔽规则加载因超过 10000 条上限被截断的次数（非 0 = 有屏蔽规则没有生效） |
| `monitor_loop_runs_total{loop,result}` / `monitor_loop_items_total{loop}` | 各后台循环的迭代次数与处理条数 |

日志为 slog JSON：不含凭据（数据源错误信息由 `promclient` 脱敏），info 级别不逐序列打印。故障类日志只在状态
变化时记录，不逐轮刷屏：

| 日志（WARN） | 何时 | 限频 |
|---|---|---|
| `datasource health probe is failing` | 探活失败 | 按数据源：进入故障一条，持续每 10 分钟一条；恢复记一条 INFO |
| `datasource queries are failing…` | 评估查询因数据源侧原因失败（连不上、超时、上游 5xx、凭据不可用、地址被拒） | 按数据源每 10 分钟至多一条 |
| `rule evaluation is failing` | 规则自身的问题（表达式错误、结果不是瞬时向量、查询配置非法） | 按任务：进入查询错误的那一轮一条；恢复记一条 INFO（`rule evaluation recovered`） |
| `rule evaluation sees an incomplete result…` | 截断 / 部分结果 | 按任务：进入一条，持续每 10 分钟一条；恢复记一条 INFO |
| `datasource query slots are saturated…` | 本实例对某数据源的 8 个并发槽持续排满，有评估等满预算后被顺延 | 按数据源：进入一条，持续每 10 分钟一条 |
| `too many active mute rules…` | 屏蔽规则超过加载上限 | 每次重读缓存一条 |
| `too many inhibit rules…` / `too many escalation policies…` / `too many CMDB resources…` | 对应的缓存超过加载上限 | 每次重读一条 |
| `too many active alerts for the inhibit snapshot…` | 活跃告警超过 `inhibit_max_alerts` | 进入一条，持续每 10 分钟一条 |
| `inhibit rules form a cycle…` | 抑制规则成环 | 同上 |
| `datasource has been down for a while…` / `rule evaluation has been failing for a while…` | 写入了一条自监控故障事件 | 每次故障一条 |
| `record rule is failing` | 记录规则查询或写回失败 | 按记录规则：进入一条，持续每 10 分钟一条；恢复记一条 INFO |

## 多实例影响说明

| 状态 / 任务 | 存储 | 多实例正确性 |
|---|---|---|
| 评估任务分配 | `monitor_eval_task` 租约 | 单语句 `FOR UPDATE SKIP LOCKED` 领取；写回以 `lease_owner` 围栏（状态事务先锁任务行并校验属主，仅写判定记录的路径以写回语句的影响行数判定），被拒即整轮回滚；属主令牌 = 主机名 + 随机串 + 循环序号，进程重启后必不相同 |
| 判定状态 / 告警事件 | PG | 一个任务的整轮状态转移是一个事务，状态行 `FOR UPDATE`；活跃事件部分唯一索引 `ON CONFLICT … DO UPDATE` 兜底；锁序固定「任务行 → 状态行 → 告警行」，与规划器清理一致 |
| 通知发件箱 | PG | `(alert_id, kind, seq)` 唯一 + `ON CONFLICT DO NOTHING`：租约过期被重领、core 尚未回写通知计数时的重复评估不会写出第二条。首次通知的重试以库里那一行的状态与 `done_time` 为准（单语句 `ON CONFLICT … DO UPDATE … WHERE status = 已丢弃`），哪个实例评估都是同一个节奏 |
| 任务物化 / 探活 / 维护 | PG advisory lock 选主 | 语句自身幂等（选主只是省重复劳动）；持锁连接每轮探活，失联即放弃领导权；探活使用派生锁键 `monitor:planner:probe`，与物化互不阻塞 |
| 配置缓存（数据源及查询客户端、屏蔽规则、抑制规则、升级策略、资源索引、工作日历） | 进程内只读，轮询 `monitor_cfg_version`（≤ 2s）失效，每分钟兜底重读（资源索引每 10 分钟整表重读，其间增量） | 最终一致，不作为正确性依赖；数据源配置变更后换发新客户端，旧客户端只释放空闲连接，在途查询不受影响 |
| 抑制结论 | 进程内只读，每 `inhibit_refresh_interval` 从库里的活跃告警重算 | 结论是库中数据的确定函数（见「抑制」的一致性模型），各实例不必同步；`inhibited` 标记只是留档，不参与判定 |
| 事件指令（手动关闭） | `monitor_alert_cmd` | 每个评估器实例都消费；指令行 `FOR UPDATE SKIP LOCKED` + 状态条件保证只执行一次，与评估提交在任务行上串行 |
| 升级通知 | 发件箱 `(alert_id, kind, seq)` 唯一 | 序号只取决于库里的进度（core 回写的次数 + 发件箱行的状态），重复评估写出同一个键；被丢弃后的重试是把同一行重新置为待派发 |
| 时间线 / 事件动作 | PG，与状态变更同一事务 | 围栏拒绝即整轮回滚，不会留下没有对应状态变更的行；动作以 `(alert_id, kind)` 唯一去重 |
| 自监控事件 | `monitor_engine_event.dedup_key` 唯一 | 检测经 `monitor:engine-watch` 选主；即使两个实例同时检测，同一次故障也只有一行。恢复事件随状态翻转同事务写入 |
| 心跳 | `monitor_engine_instance`，主键 = 实例 ID | 各实例只写自己的行；过期行由 `watch`（选主）清理，语句幂等 |
| 记录规则 | `monitor_record_task` 租约 | 与评估任务同一套领取 / 围栏；写回时序库是至少一次 |
| 数据源并发槽（单实例单数据源：评估查询 8 + 探活专用 1） | 进程内 | 仅实例自我保护，不承担全局配额。「这一轮被顺延过」的标记在任务行上（PG），下一轮由哪个实例评估都能读到 |
| 日志限频状态 | 进程内 | 只影响日志条数；任务级的「进入查询错误」判断用的是任务行里的上一轮结果，换实例评估也不重复记 |
| 评估时刻抖动 | 任务行的 `next_eval_time` | 新任务首次评估与每轮的下一次评估都带随机抖动：任务到期的相位不会锁在某个实例的轮询节拍上，多实例稳态下各实例都有产出（没有抖动时，任务数少到一个实例一次轮询就能领完、且各实例轮询节拍错开的部署下，全部评估会一直由同一个实例完成——联调中的现象，`TestTwoEvaluatorInstancesShareTheWork` 复现并验证） |
| 时钟 | 数据库 `now()` | 到期判定、租约、状态与告警时间列、屏蔽规则与生效时间窗判定均取数据库时钟，应用时钟只用于耗时统计与本地节流 |

停机：循环 Stop 后不再领取新任务；领取后尚未开始便收到停机信号的那一批立即归还（其他实例可马上重领）；
已开始评估的任务在不受取消的 ctx 上跑完（上限 = 3 × 查询超时上限 + 30s）。进程被强杀时租约到期即被重领，状态从库里接续，
不会重复发首次通知。滚动发布期间新旧实例共存安全：分片靠租约，无进程内共享状态。

## 测试

```bash
go test ./... -race -shuffle=on                       # 单元测试（状态机表驱动、模板、屏蔽、评估器、缓存）
go test ./internal/biz/ -run xxx -bench . -benchmem   # 状态机基准（2000 条序列；AdvanceP2* / MergeSamplesP2 为第二期全开）
go test ./internal/biz/ -run TestCIIndexMemory -v     # 资源索引的内存占用（10 万个资源）
# 集成测试：真实 PG，库名须含 test，会 TRUNCATE monitor_* 表；与其它模块共库时 -p 1 串行
TEST_DATABASE_SOURCE='<指向测试库的 DSN>' go test -tags integration -p 1 ./...
```

集成测试用 `internal/promtest` 的 httptest 假 Prometheus，不需要真实时序库；覆盖任务物化与清理、
领取互斥、租约接管、围栏回滚、端到端（待触发 → 告警中 → 重复 → 恢复）、崩溃接续、2000 条序列的大规则、
屏蔽规则、数据源凭据、探活、分区维护与保留期清理，以及第一轮评审的回归（`review_integration_test.go`）：
2000 序列截断与部分结果不恢复 / 不删状态、发件箱重新激活的真实冲突分支（重复通知被丢弃、屏蔽解除与派发并发）、
未变化的轮次不写告警行、计时跨故障重置、规则修改后提前评估、两个评估器实例都有产出（约 1 分钟）、
攻击模板不卡事务、以受限登录角色运行、分区清理的锁行为；以及第二轮评审的回归（`round2_integration_test.go`）：
首次通知被丢弃后按有上限的节奏重试直至送达、顺延标记的落库 / 带回 / 清除与计时重新起算、慢数据源在并发槽
排满时不产生查询错误（约 4 秒）、标签上限与部分结果标记的落库；第二、三期（`p2_integration_test.go`）：各缓存的
加载与失效、流水线端到端（改写 / 富化 / 丢弃 / 范围 / 维护态 / 工作日历）、跨规则抑制与解除、时间线上限、升级端到端
（core 的写入以 `monitor_core_role` 的身份执行：认领、回写级内次数、丢弃升级通知）、两个实例并发消费手动关闭指令、
关闭与评估提交的串行、两个规划器实例下自监控事件恰好一次、心跳与清理、记录规则写到解 Snappy + protobuf 的假接收端、
记录规则任务的互斥领取与围栏、保留期清理、0076 的授权边界。

基准（Apple M3 Pro，2000 条序列的一轮；第二期 = 改写 + 5000 资源的索引里关联与富化 + 条件丢弃 + 资源范围 +
拓扑抑制 + 20 条抑制规则 + 两级升级策略全开）：

| | 第一期代码（基线） | 现在，规则不用第二期功能 | 现在，第二期全开 |
|---|---|---|---|
| `Advance` 稳态告警中（不写事件行） | 0.11 ms | 0.16–0.18 ms | 0.25–0.27 ms |
| `Advance` 稳态、每条取值都变 | 0.10 ms | 0.15–0.17 ms | 0.23–0.24 ms |
| `Advance` 全部新进入待触发 | 0.025 ms | 0.025 ms | 0.03–0.035 ms |
| `Advance` 全部触发（含附加信息渲染） | 2.4–2.7 ms | 2.5–2.6 ms | 3.0–3.2 ms |
| `Advance` 全部恢复 | 0.21 ms | 0.26–0.28 ms | 0.27 ms |
| `mergeSamples`（流水线前半段） | 0.80 ms | 0.83–0.86 ms | 4.2–4.3 ms |
| 抑制快照刷新的计算（2100 个活跃告警） | — | — | 2 ms |

不用第二期功能的规则也慢了一些：状态行多带了认领 / 抑制 / 资源 / 升级各列（每轮的分配从 391 KB 增到 539 KB）。
`mergeSamples` 全开时每个样本约 2 µs，主要是条件丢弃表达式的模板执行与富化。

## 已知限制

- **抑制有滞后**：生效与解除最多滞后「抑制快照的刷新周期（5s）+ 该规则的评估周期」。源告警与目标告警在同一刷新
  周期内先后触发时，目标的首次通知可能已经入箱——core 派发时若已读到 `inhibited = true` 会丢弃它，否则这一条会发出
- **只被升级通知过的事件没有恢复通知**：触发时被屏蔽 / 抑制、解除时又正被认领的事件，规则自身的通知一次都没发过
  （`notify_count = 0`）；「未恢复」类的级仍会通知。它恢复时不发恢复通知（恢复通知以规则通知发出过为前提，core 派发时
  同样按 `notify_count` 把关）
- **留观中的事件不升级、不刷新屏蔽 / 抑制 / 认领带来的变化**：序列本轮没有出现时只推进留观计时
- **记录规则的结果超过 `max_series` 条序列时永远写不出去**（见「记录规则」）；写回是至少一次
- **时间线里会抖动的几类行每个事件约 200 条封顶**，之后的屏蔽 / 抑制 / 升级变化只体现在告警行上
- 数据源停用期间 `health_since` 不动：重新启用后第一次探活仍失败的，持续时长从停用前的那次故障起算

- **首次通知**行被 core 因重试超限丢弃后，按重复通知间隔（未配置时 10 分钟）重试；在重试送达之前事件就恢复了
  的，视为从未通知过，不发恢复通知
- **部分结果只识别 VictoriaMetrics 的 `isPartial`**：Thanos / Mimir 的部分响应经 `warnings` 表达，引擎不解析，
  这类结果按完整结果判定（没返回的序列会被当作消失而进入留观 / 恢复）。接这类数据源时应在其侧关闭部分响应
  ，让它以查询错误的形式暴露
- **「没观察的轮次不让计时前进」只覆盖引擎记录在案的轮次**：查询错误、不在生效时间、结果不完整、排不上并发槽
  被顺延。引擎整体停机、评估积压、评估器崩溃后租约过期被接管、引擎自身故障（数据库不可用）造成的空档不在
  其列——恢复评估后计时按墙上时间接续
- 被 `max_series` 截断的规则：排在上限之外的序列在截断期间不会触发新告警（已有的告警保持原状）。
  判定记录与任务的错误信息会一直提示，应拆分规则或收窄表达式
- 一条循环领到的一批任务全部评估完才领下一批：同批里有任务卡在慢数据源上时，该循环要等它超时；
  其余循环与实例不受影响
- 判定记录明细至多 50 个异常点（状态发生变化的点优先保留）
