# job — 作业执行引擎与执行节点（数据面）

Kratos v3 单二进制，按 `SERVER_ROLES` 启用两种角色：

- **engine（执行引擎）**：与 core 共用 PostgreSQL，面向执行节点提供 HTTP 协议（领取 / 上报 / 结束 / 心跳），
  推进执行记录的状态、写主机结果与日志，并清扫失联的执行、清理过期日志。
- **runner（执行节点）**：部署在目标主机所在的网络区域，向引擎领取任务，拉取 Git 仓库里的 Playbook，
  运行 `ansible-playbook`，把输出（凭据已掩码）分块上报。**不连接数据库、不持有主密钥**，可以放在 DMZ。

引擎**只读写 `job_*` 表**，不 import core / notify / cmdb / monitor；模板、脚本库、高危规则、策略、定时作业、
触发器对引擎都不可见——执行内容在提交时已固化进 `job_execution.spec`（分发文件的内容固化在
`job_execution_payload`）。与 core 共享的只有表结构（`core/migrations/0070_job_init`、`0075_job_phase2`）与契约包
`backend/kit/jobcontract`（jsonb 结构、状态字面量、引擎 ↔ 节点协议、校验口径、终态判定、接入令牌）。
工程规范见 [`../notify/AGENTS.md`](../notify/AGENTS.md)。

## 组件与角色

| 角色 | 组件 | 职责 |
|---|---|---|
| `engine` | 执行节点协议监听（8051，可配 TLS） | `jobcontract` 的四个 POST 接口；每个请求都校验接入令牌 |
| `engine` | `lease-sweep` 循环（5s，每个实例都跑） | 租约到期未续的执行 → 结果未知（8）。**绝不重新排队、绝不自动重跑**。进程启动后的一个租约时长内不清扫：引擎停机超过一个租约再启动时，仍在跑的节点来得及续租，不会被整批误判为失联 |
| `engine` | `log-purge` 循环（1h，选主，启动即跑一轮） | 分批删除已结束的执行里超过保留期的日志块 |
| `runner` | 心跳循环（10s）+ 领取循环（2s） | 有空位且最近一次心跳说节点已启用才领取 |
| `runner` | 每个作业一组 goroutine | 拉取代码 → 生成清单与变量文件 → 运行 → 上报 → 结束 → 清理 |
| — | HTTP 恒启动（8050） | `/healthz`（engine：数据库可达；纯 runner：恒为 ok；失败时应答只有笼统说明，原因进日志；只作就绪探针）、`/livez`（不查数据库，恒为 ok；存活探针用它——用 `/healthz` 做存活探针，数据库一抖动所有实例都会被重启、在途作业被杀）、`/metrics` |

`SERVER_ROLES`（逗号分隔，优先于 `server.roles`）为空 = 两种角色都启动（仅用于本地开发）。

## 作业类型

| 类型（`spec.kind`） | 内容来源 | 节点上怎么执行 |
|---|---|---|
| 1 作业模板（缺省；一期的记录没有这个字段） | 项目的 Git 仓库里的 Playbook + 执行参数 | 拉取代码，运行仓库里的 Playbook |
| 2 执行命令 | `spec.adhoc.command` | 不拉取代码；平台生成 Playbook：一个 `ansible.builtin.shell` 任务 |
| 3 执行脚本 | `spec.adhoc.script` / `interpreter` / `script_args` | 脚本写到作业目录的 `script`（0700，CRLF 统一为 LF）；一个 `ansible.builtin.script` 任务，`executable` = `bash` / `sh` / `python3` |
| 4 分发文件 | `spec.adhoc` 的文件名、目标目录、权限、大小与 SHA-256；内容在 `job_execution_payload` | 内容写到作业目录的 `payload/<文件名>`（0600）；`ansible.builtin.file`（目标目录不存在则创建）+ `ansible.builtin.copy` |

快速执行（2–4）没有项目、没有提交号、没有执行参数；凭据、主机、选项（并发、超时、提权、预检、滚动）与作业模板相同。

- **引擎**按类型派发：快速执行不读 `job_project`，任务里的仓库字段留空；派发前用 `Adhoc.Validate` 复核。分发文件另读
  `job_execution_payload`：没有内容、长度不等于 `file_size` 或 SHA-256 对不上 → 该执行置为失败，不下发
- **执行节点**再复核一遍（包括再算一次文件的 SHA-256），不一致即以 `error` 结束
- 生成的 Playbook 结构由平台写死；命令、脚本参数、目标路径只作为带 `!unsafe` 标记的字符串出现在模块参数里——
  既不能改变 YAML 结构，也不会被 Ansible 当 Jinja2 模板求值（`echo {{ 7*7 }}` 在目标主机上输出的就是 `{{ 7*7 }}`）
- 命令 / 脚本的标准输出与标准错误由随后的两个 `debug` 任务打印进作业日志（缺省的输出回调只在失败时显示任务结果）；
  预检模式（`--check`）下命令与脚本被 Ansible 跳过
- 作业日志开头是一行平台消息（`[runner] 执行命令（主机 N 台，并发 M）` 等），**不回显脚本与文件内容**

### 滚动执行（`options.batch_size > 0`，对所有类型生效）

执行节点按任务里的主机顺序切成每批 `batch_size` 台，逐批运行 `ansible-playbook … --limit @limit-<i>.txt`
（清单始终是完整的；限定文件每行一个主机，IPv6 与主机名原样）。代码只拉取一次。批次之间写一行
`[runner] 第 i/N 批（k 台）`，各批的 PLAY RECAP 汇总后一并上报。

- 每批结束后核对阈值：已执行主机里失败 + 不可达的台数 `bad` 满足 `bad × 100 > max_fail_percent × 已执行台数` 即中止
  （`max_fail_percent = 0`：出现任何失败即中止）。日志写 `[runner] 失败比例超过阈值（x% > y%），剩余 n 台主机未执行`，
  `error` 为 `滚动执行中止：a 台主机失败，b 台不可达，剩余 n 台未执行`，以非零退出码结束——终态为失败或部分失败。
  未执行批次的主机不上报，保持「未执行」
- 某一批以非零退出码结束、却没有新增任何失败 / 不可达的主机（Playbook 自身出错等）同样中止，不在剩余批次上重复同一个错误
- 最后一批不做「中止」判定（没有剩余主机），按普通结果上报
- 上报的退出码 = 最后一个非零的批次退出码（全部为 0 则为 0）；中止时若它是 0（Playbook 忽略了失败）则报 2
- 取消、超时、节点停止对整个作业生效：正在跑的那一批被终止，之后的批次不再开始；已跑完批次的主机结果照常上报
- 与 Playbook 自身的 `serial` 无关；批量不小于主机数时不分批、不加 `--limit`

## 引擎 ↔ 执行节点协议

HTTP + JSON，全部 POST，`Authorization: Bearer <节点 ID>.<密钥>`；结构定义在 `kit/jobcontract`。

| 路径 | 作用 |
|---|---|
| `/runner/v1/heartbeat` | 回写节点的版本、主机名、并发数、在途作业数与 `last_seen`（数据库时钟），应答节点是否启用 |
| `/runner/v1/claim` | 领取最早一条该节点可执行的排队中的执行，应答任务（含凭据明文；分发文件时含文件内容，应答至多约 6MiB）；没有则 `{}` |
| `/runner/v1/report` | 续租 + 追加日志块 + 记提交号；应答是否已请求取消与已确认的日志序号 |
| `/runner/v1/finish` | 写各主机结果，推出终态并落账；重复发送是空操作 |

| 状态码 | 含义 | 节点的处理 |
|---|---|---|
| 200 | 成功 | |
| 400 / 413 | 请求不合法（超出日志块限额、字段非法）/ 请求体超过 8MiB | 按可重试处理；结束请求连续被拒 10 次后放弃 |
| 401 | 令牌无效（节点不存在、已删除、密钥不对——三者应答相同） | 停止领取；在途作业继续 |
| 403 | 节点已停用（只有领取会返回；在途任务的上报与结束照常接受） | 停止领取 |
| 409 | 租约已失效（领取令牌不匹配、执行不属于该节点、执行已结束或不存在） | 终止该作业并丢弃 |
| 其余 | 引擎故障（应答体不含内部细节） | 退避重试 |

### 领取（claim）

1. 单语句原子领取：`status = 1 AND NOT cancel_requested AND (runner_zone = '' OR runner_zone = 节点区域)`，
   `ORDER BY create_time, id`，`FOR UPDATE SKIP LOCKED LIMIT 1` → 置为执行中，写节点、新的随机领取令牌、
   `lease_until = now() + lease`、`start_time = now()`。待审批（9）与审批未通过（10）的执行引擎不领取、不清扫、
   不接受上报
2. 组装任务：仓库地址 / 分支 / Playbook / 参数 / 选项取自固化的 `spec`；认证方式、用户名、密文取自**当前的**
   凭据行与项目行并解密；主机取自 `job_execution_host`，注入 `cmdb_id / cmdb_name / cmdb_env`。
   未开启提权时不下发提权方式与提权密码
3. 派发前按 `jobcontract` 的校验口径复核一遍（纵深防御）
4. 永远无法派发的执行（凭据或项目已删除、解密失败 / 主密钥未配置、内容不合法、没有主机）→ 置为失败并写明原因
   （不含凭据），继续领取下一条（单次请求至多连续处理 20 条）；组装时遇到数据库故障 → 放回队列并报 500。同一条执行在本进程内连续 5 次组装失败
   且持续超过 1 分钟 → 置为失败（`引擎多次无法读取该执行所需的数据，已放弃派发`）：队列严格按提交顺序领取，
   不这样做它会一直堵住所在区域的队首

### 上报（report）

一个事务：锁定执行行 → 校验领取令牌、领取节点与状态（执行中或结果未知）→ 追加日志 → 续租 → 记提交号。

- 日志块按序号接收：不大于已确认序号的忽略（重传），恰为已确认序号 + 1 的接收，遇到第一个空洞即停
- 单块 ≤ 256KiB、单次 ≤ 16 块，超出返回 400；一次执行的日志总量超过 32MiB 后内容丢弃并置 `log_truncated`，
  序号照常确认（保留的日志始终是完整的前缀）
- 执行此前因租约到期被判为结果未知的，节点带着同一领取令牌回来即**恢复为执行中**（清掉结束时间与原因，并把
  `notify_time` 置空——core 会按真正的终态再通知一次）。
  例外见下文「禁止并发的模板」

### 结束（finish）

一个事务：锁定 → 执行已是终态且领取令牌一致 → 空操作（200）；否则校验后按 `(execution_id, address)` 更新主机行
（未上报的主机保持「未执行」，不属于本次执行的地址被忽略），`status = jobcontract.FinalStatus(...)`，
三个主机统计列从更新后的主机行汇总。结果未知的执行在这里直接落为真实终态。

### 禁止并发的模板（`uk_job_execution_exclusive`）

模板禁止并发时，排队中 / 执行中 / 待审批的执行占用互斥位。执行被判为结果未知后互斥位即释放，core 可能已为同一模板接受了
新的执行；失联的节点此后再上报时**无法恢复为执行中**。此时引擎不报错、也不让节点取消：该执行保持结果未知、
原因与结束时间都不清，本次上报照常收日志、续租、记提交号并应答 200；结束请求直接把它落为真实终态（终态不占互斥位）。
实现上恢复语句自带「没有别的行占着互斥位」的条件，并放在保存点里执行——条件判定与对方提交之间的竞态由唯一索引裁决，
撞上唯一约束即回滚到保存点，事务的其余部分照常提交。租约清扫只看执行中的行，这样的执行不会被再次处理。

## 执行节点的一次作业

在全新的私有目录 `work_dir/<执行 ID>`（0700）里进行，结束后无论成败一律删除。下面是作业模板的流程；快速执行没有
第 2 步，Playbook 由平台生成在作业目录里（见「作业类型」），工作目录 = 作业目录：

1. **复核任务**：仓库地址、分支、Playbook 路径、主机地址与端口、参数名、选项、凭据与认证方式逐项按契约口径校验，
   任何一项不合规即拒绝执行
2. **拉取代码**：`git clone --depth 1 --single-branch --branch <ref> -- <repo> repo`，环境从零构造
   （`GIT_TERMINAL_PROMPT=0`、`GIT_ALLOW_PROTOCOL`、不读系统与用户级 git 配置、`HOME` 指向作业目录）。
   令牌认证经环境变量里的 `http.extraHeader` 传递（不进命令行、不进地址），并禁止跟随重定向；私钥认证用单独的
   ssh-agent，拉完即停。记录 `git rev-parse HEAD`。Playbook 解析符号链接后必须仍在仓库目录之内
3. **生成文件**：`inventory.json`（主机、端口、`cmdb_*` 变量、登录用户、ssh 选项、提权设置）、`vars.json`（执行参数）、
   `secrets.json`（SSH 密码 / 提权密码，仅在需要时）、`ansible.cfg`（平台自己写，经 `ANSIBLE_CONFIG` 指定——
   仓库里的 `ansible.cfg` 不生效）。文件均为 0600
4. **凭据**：SSH 私钥经标准输入送进本次执行独占的 `ssh-agent`，不落盘，结束即终止 agent
5. **运行**：`ansible-playbook -i inventory.json <playbook> -e @vars.json [-e @secrets.json] --forks N [--check] [--diff] [-v…]`，
   工作目录 = 仓库目录，独立进程组，最小环境（`PATH`、`HOME`、UTF-8 locale、`ANSIBLE_*`、`PYTHONUNBUFFERED`、
   `SSH_AUTH_SOCK`），**不继承节点进程的环境**
6. **上报**：输出先掩码，再攒成块（约 32KB 或每秒封一块，尽量在行尾切、不切坏 UTF-8），带序号上报；未确认的块留在
   有界缓冲（每个作业 4MiB）里，从引擎确认的序号之后重传；缓冲满了就不再读子进程的输出（背压），**不丢弃**。
   没有新输出时每 `min(租约/4, 3s)` 上报一次（续租，并借应答得知取消请求）
7. **结束**：解析输出里的 `PLAY RECAP`（多段时每台主机以最后一次为准，只认本次任务的主机），等全部日志被确认后上报
   结束，重试到引擎答复 200 或 409 为止

| 情形 | 处理 | 结束方式 |
|---|---|---|
| 引擎转达取消 | 向进程组发 SIGTERM，`cancel_grace` 后 SIGKILL | `canceled` |
| 超过 `options.timeout_s`（从作业开始算，含拉取代码） | 同上 | `timeout` |
| 引擎答复 409 | 终止并丢弃，不再上报 | — |
| 引擎不可达 | 作业继续跑，上报与结束按退避重试（1s 起，至多 15s） | 照常 |
| 拉取失败、Playbook 不存在、任务不合规、无法启动 | 不执行 | `error`，原因写入 `error` 与作业日志 |
| 节点进程停止 | 停止领取 → 等在途作业至多 `drain_timeout` → 取消它们 | `canceled`，`error` 注明节点已停止 |

## 安全模型

### 先说清楚：仓库里的 Playbook 就是在节点上运行的代码

作业模板的 Playbook 来自项目的 Git 仓库，`ansible-playbook` 以**执行节点进程的操作系统用户**运行它。Playbook 不需要
任何漏洞就能在节点本机执行任意代码：`connection: local` / `delegate_to: localhost`、`lookup('pipe', …)` 等查找插件、
仓库里与 Playbook 相邻的 `library/`、`filter_plugins/`、`callback_plugins/`、`roles/`，以及用 `group_vars/` 覆盖
`ansible_ssh_common_args`（比如塞进 `ProxyCommand`）。平台写的 `ANSIBLE_CONFIG` **只**让仓库里的 `ansible.cfg` 不生效，
挡不住以上任何一条。因此这样的 Playbook 可以读到：

- 本次执行的凭据（agent 套接字、`secrets.json`）——这本来就是交给它用的；
- **同一节点上其它并发作业**的作业目录与 agent 套接字（都在 `work_dir` 下，同一个用户）；
- **节点的接入令牌**：它在节点进程的环境里，同一用户的进程读得到（Linux 上 `/proc/<pid>/environ`）。子进程的环境不继承它，
  但这不是隔离；
- `known_hosts`（也可以改写它）。

结论与部署建议——不要指望这里有并不存在的隔离：

- **能改某个项目仓库的人，实际上就持有与它一起使用的凭据**，也能冒充该节点向引擎领取同区域的其它任务。仓库的写权限与
  分支保护要按这个分量管理
- 节点按**信任域**划分（区域 `zone`）：不同团队 / 环境的凭据不要交给同一个节点；把接入令牌视为对 Playbook 可见，
  令牌泄露的影响面 = 该节点所在区域能领到的全部任务
- `capacity: 1`（或每个节点一个容器）**只能**把同时运行的作业的文件与 agent 套接字彼此隔开。它挡不住的：
  Playbook 仍能读到本节点的接入令牌与 `known_hosts`；先后运行的作业之间，前一个留在机器上的东西（改过的
  `known_hosts`、落在作业目录之外的文件、脱离进程组的后台进程）后一个仍能碰到，除非每次作业后丢弃容器。
  在进程内没有办法把令牌放到 Playbook 读不到的地方
- 节点用专用的非特权用户运行，机器上不放别的秘密
- 快速执行（命令 / 脚本 / 文件）的 Playbook 由平台生成，不执行仓库内容，没有上述问题；但命令与脚本本身当然在**目标主机**上
  以登录用户（或提权用户）的身份运行

### 各项做法

| 项 | 做法 |
|---|---|
| 节点接入 | 令牌 = `<节点 ID>.<随机密钥>`，库里只存密钥的 SHA-256，常数时间比较；未知节点与密钥不对的应答相同。令牌不进日志 |
| 凭据密文 | 只有引擎解密（`kit/secretbox`，主密钥与 core 相同）；明文只存在于领取应答与节点内存里，不落库、不进日志 |
| 传输 | 协议监听可配 TLS（1.2+）；未配置时启动会告警——该端口上传输令牌与凭据明文，生产环境必须加密 |
| 快速执行的落盘 | 生成的 Playbook（含命令原文）、脚本（`script`，0700）、待分发的文件（`payload/<文件名>`，0600）、滚动执行的限定文件都在作业目录里，随作业目录删除 |
| 作业目录的清理 | `ansible-playbook` 一结束就删除作业目录，不等日志交完与结束上报（引擎不可达时那两步会一直重试）。只删本作业自己建的目录；执行 ID 不是 UUID 的任务在拼任何路径之前就被丢弃 |
| 节点崩溃 / 被强杀 | Linux 上子进程带 `PR_SET_PDEATHSIG`：节点进程一死，`ansible-playbook`、`git`、`ssh-agent` 即被内核杀死。各作业子进程的进程组记在作业目录的 `pgids` 文件里，节点下次启动时先杀掉残留的进程组（Linux 逐个进程核对进程组号与环境里的 `HOME=<作业目录>`；其它系统只能核对组长的启动时刻，尽力而为）再删目录。`ssh-agent` 里的私钥带寿命（`-t`，作业时限 + 5 分钟） |
| 节点上的落盘 | **SSH 私钥、Git 令牌、Git 私钥不落盘**。SSH 密码 / 提权密码落在作业目录的 `secrets.json`（0600，Ansible 没有不落盘的传法），随作业目录删除——`work_dir` 应挂载为 tmpfs。进程崩溃残留的作业目录在下次启动时清除 |
| 子进程环境 | 从零构造，不继承节点的环境（其中有接入令牌）；Git 令牌只出现在 `git clone` 的环境里 |
| 命令行 | 凭据与参数取值都不进 argv；参数经变量文件传递，不做任何字符串拼接 |
| 模板注入 | 变量文件与清单里来自任务的字符串（参数取值、CMDB 名称、用户名、口令）一律带 `!unsafe` 标记，Ansible 不会把它们当 Jinja2 模板求值——否则只有执行权限的用户可以用 `{{ lookup('pipe', …) }}` 在节点上执行命令 |
| 主机公钥 | `StrictHostKeyChecking=accept-new` + 节点持久化的 `known_hosts`：首次连接自动记录，之后公钥变化即拒绝。目标主机与 Git 服务器同此 |
| 日志掩码 | 输出进入缓冲之前，私钥的每一行、密码、提权密码、Git 令牌及其 Basic 编码、它们的 JSON 转义与 URL 编码形式都替换为 `********`（短于 4 个字符的不参与；PEM 的 `-----BEGIN/END …-----` 标记行不掩码；相互重叠的凭据合并成一段掩码）；上报给引擎的 `error` 同样掩码 |
| 指标 | 标签只有固定枚举值，不含执行 ID、节点 ID |
| 结束上报 | 引擎只接受本次执行主机快照里的地址，终态与统计都按它们计算 |

## 配置

`configs/config.yaml`（`internal/conf/conf.proto`）。未设置或 ≤ 0 的值取缺省；密钥类一律走环境变量。

| 键 | 缺省 | 说明 |
|---|---|---|
| `server.roles` | 全部 | `engine` / `runner`；`SERVER_ROLES` 优先 |
| `server.http.addr` | `0.0.0.0:8050` | `/healthz`、`/metrics` |
| `server.runner.addr` | `0.0.0.0:8051` | 执行节点协议监听（engine） |
| `server.runner.timeout` | 15s | 单个请求的处理超时 |
| `server.runner.cert_file` / `key_file` | 空 | TLS 证书与私钥（PEM），须成对；皆空 = 明文 HTTP |
| `data.database.source` | — | engine 必填，`KRATOS_DATA_DATABASE_SOURCE`；runner 不使用 |
| `data.database.max_conns` | 16 | |
| `notify.master.key` | 空 | engine，`KRATOS_NOTIFY_MASTER_KEY`，与 core 同一主密钥 |
| `engine.lease` | 60s | 执行租约，下限 10s |
| `engine.sweep_interval` | 5s | 租约清扫周期 |
| `engine.log_retention` | 90d | 显式 `0s` = 永久保留 |
| `engine.log_purge_interval` | 1h | |
| `runner.engine_url` | — | runner 必填，`KRATOS_RUNNER_ENGINE_URL` |
| `runner.token` | — | runner 必填，`KRATOS_RUNNER_TOKEN` |
| `runner.work_dir` | — | runner 必填，`KRATOS_RUNNER_WORK_DIR`；至多 52 字节，不含空白、引号、反斜杠与 `%`；一个目录只给一个节点进程用 |
| `runner.capacity` | 2 | 可同时执行的作业数 |
| `runner.ansible_playbook` / `git` / `ssh_agent` / `ssh_add` | 按名称在 `PATH` 中查找 | |
| `runner.known_hosts` | `<work_dir>/known_hosts` | |
| `runner.ca_file` | 空 | 校验引擎证书的 CA；空 = 系统根证书 |
| `runner.insecure_skip_verify` | false | 仅限开发 |
| `runner.poll_interval` | 2s | 没有任务时的领取周期 |
| `runner.heartbeat_interval` | 10s | |
| `runner.cancel_grace` | 30s | SIGTERM 与 SIGKILL 之间的宽限期 |
| `runner.drain_timeout` | 10min | 进程停止时等待在途作业的时长 |
| `runner.git_allow_protocols` | `http:https:ssh` | `GIT_ALLOW_PROTOCOL`；不建议放宽 |

主密钥未配置（或与 core 不一致）时引擎照常启动：被领取的执行记为失败
（`凭据解密失败：引擎的主密钥未配置或与管理面不一致`）。

## 快速开始

```bash
# —— 引擎（在 backend/job）——
# 1. 数据库迁移到 0070 及以后（在 backend/core：go run ./cmd/migrate -conf configs up）
export KRATOS_DATA_DATABASE_SOURCE='<DSN>'            # 本地与 core 相同
export KRATOS_NOTIFY_MASTER_KEY=<与 core 相同的主密钥>
SERVER_ROLES=engine go run ./cmd/job -conf configs
curl http://127.0.0.1:8050/healthz
curl -s http://127.0.0.1:8050/metrics | grep '^job_'

# —— 执行节点 ——
export KRATOS_RUNNER_TOKEN='<在管理面创建节点时得到的令牌>'
export KRATOS_RUNNER_ENGINE_URL=http://127.0.0.1:8051
export KRATOS_RUNNER_WORK_DIR=/dev/shm/reborn-job
SERVER_ROLES=runner go run ./cmd/job -conf configs
```

构建时用 `-ldflags "-X main.Version=<版本>"` 注入版本号，随心跳上报为 `agent_version`；未注入时为模块版本或 `dev`。
Go 模块在本环境须经 `GOPROXY=https://goproxy.cn,direct` 下载。

## 执行节点部署说明

- 依赖：`ansible-core`、`git`、`openssh-client`（`ssh`、`ssh-agent`、`ssh-add`；需 OpenSSH ≥ 7.6 以支持
  `accept-new`）；使用 **SSH 密码凭据**时另需 `sshpass`。Playbook 用到的 collection / role 需预装在节点上
  （节点不执行 `ansible-galaxy install`）
- `work_dir` 挂载为 tmpfs（如 `/dev/shm/reborn-job`），属主为节点进程的账号；`known_hosts` 缺省也在其中——
  tmpfs 重启即清空，希望主机公钥跨重启保留时把 `runner.known_hosts` 指到持久化的路径
- 以专用的非 root 账号运行；节点只需出站访问引擎的 8051、Git 服务器与目标主机的 SSH 端口，不需要入站端口
  （8050 的健康检查除外）、不需要访问数据库
- 节点的 `SERVER_ROLES` 必须设为 `runner`：否则进程会同时尝试以引擎身份连接数据库
- 停机用 SIGTERM，并给足时间（`drain_timeout` + `cancel_grace` + 30s）；systemd 的 `TimeoutStopSec`、
  K8s 的 `terminationGracePeriodSeconds` 要相应调大。被强杀时在途作业的子进程可能残留，对应的执行在租约到期后
  显示为结果未知
- 生产环境给协议监听配 TLS（或在前面终结 TLS），节点用 `https://` 地址与 `runner.ca_file`

## DB 角色

生产环境引擎的登录账号只需是 `job_engine_role` 的成员（迁移 0070 创建并授权）：

```sql
CREATE ROLE job_engine LOGIN PASSWORD '…' IN ROLE job_engine_role;
```

| 表 | 权限 |
|---|---|
| `job_credential` / `job_project` / `job_execution_payload` | SELECT |
| `job_runner` | SELECT；UPDATE 仅 `agent_version, hostname, capacity, running, last_seen` |
| `job_execution` / `job_execution_host` | SELECT / UPDATE（不建单、不删除） |
| `job_execution_log` | SELECT / INSERT / DELETE |
| `job_template` / `job_script` / `job_risk_rule` / `job_policy` / `job_schedule` / `job_trigger` / `job_trigger_fire` | 无（不可见） |

集成测试的全部仓储都跑在 `SET ROLE` 到一个「仅是 `job_engine_role` 成员」的临时角色的连接上，并断言越权语句
（读模板、建单 / 删单、改凭据 / 项目、改节点的启用状态 / 令牌 / 区域、改日志等）被数据库拒绝（`TestEngineRoleIsRestricted`）。

## 指标

| 指标 | 说明 |
|---|---|
| `job_runner_api_requests_total{op,result}` | 协议调用：`op` = `heartbeat` / `claim` / `report` / `finish`；`result` = `ok` / `empty`（没有任务）/ `unauthorized` / `disabled` / `lease_lost` / `invalid` / `error` |
| `job_executions_total{event}` | 执行事件：`claimed` / `dispatch_failed` / `revived` / `lease_expired` / `log_truncated` / `finished_success` / `finished_failed` / `finished_partial` / `finished_canceled` / `finished_timeout` |
| `job_loop_runs_total{loop,result}` / `job_loop_items_total{loop}` | 后台循环的迭代次数与处理条数（`lease-sweep` 的条数 = 判为结果未知的执行数） |

日志为 slog JSON：不含接入令牌、领取令牌与凭据。执行节点目前只有日志，没有自己的业务指标。

## 多实例影响说明

| 状态 / 任务 | 存储 | 多实例正确性 |
|---|---|---|
| 执行的领取 | `job_execution` | 单语句：`MATERIALIZED` CTE 里 `FOR UPDATE SKIP LOCKED LIMIT 1` 选行，外层更新。并发领取互不阻塞、不会领到同一条，一次只动一条 |
| 领取后的撤销（置失败 / 放回队列） | PG | 条件更新，以领取令牌围栏；用脱离请求取消的独立超时执行。失败时该执行在租约到期后转为结果未知 |
| 上报 / 结束 | PG | 一个事务，执行行 `FOR UPDATE`；以 `lease_token` + `runner_id` 围栏；日志 `ON CONFLICT DO NOTHING`；重复的结束请求是空操作。请求可以落到任意引擎实例 |
| 结果未知 → 执行中的恢复 | PG | 在行锁之内；与互斥位的冲突由带条件的更新 + 保存点 + 唯一索引裁决（见上文） |
| 租约清扫 | PG | 每个实例都跑，不选主：条件更新 + `SKIP LOCKED`，到期判定取数据库时钟；正在处理上报的行（被行锁持有）本轮跳过 |
| 日志清理 | PG advisory lock 选主（`job:log-purge`） | 语句自身幂等，选主只是省重复劳动 |
| 节点心跳 | `job_runner` | 单语句更新，`last_seen` 取数据库 `now()` |
| 引擎进程内状态 | 无 | 引擎不保存任何跨请求状态；鉴权每次读库 |
| 执行节点 | 进程内（在途作业、日志缓冲） | 节点之间互不相干，各自凭自己的令牌领取；一条执行只会被一个节点领到。日志缓冲随进程丢失——引擎侧已确认的日志不受影响 |
| 时钟 | 数据库 `now()` | 租约、到期判定、开始 / 结束时间、心跳时间、日志保留期都取数据库时钟；节点的时钟只用于本地的超时与节奏 |

停机：引擎的循环 Stop 后不再开始新一轮，HTTP 监听优雅关闭；节点见上文。滚动发布期间新旧引擎实例共存安全
（无进程内共享状态，协议忽略未知字段）。两种角色同在一个进程时，协议监听排在执行节点之后关闭。

## 测试

```bash
go test ./... -race -shuffle=on        # 单元测试 + 执行节点端到端
# 集成测试：真实 PG，库名须含 test，会 TRUNCATE job_* 表；与其它模块共库时 -p 1 串行
TEST_DATABASE_SOURCE='<指向测试库的 DSN>' go test -tags integration -p 1 ./...
```

- 引擎的协议处理（`internal/service`）跑在 biz 仓储接口的内存实现（`internal/biztest`）上：鉴权、领取顺序与区域、
  租约围栏、从结果未知恢复、互斥位冲突、日志空洞、截断、幂等结束
- 执行节点（`internal/runner`）：PLAY RECAP 解析、掩码、分块 / 重传 / 背压、清单与命令行、终止序列；端到端测试用
  假的 `ansible-playbook` 脚本、真实的 `git`（经 `git-http-backend` 起的本地 HTTP 仓库）与 `ssh-agent`，对接真实的
  引擎处理器。变量文件另用 PyYAML（Ansible 的 YAML 解析器）读回比对，解释器经 `JOB_TEST_PYTHON` 指定，没有则跳过
- 集成测试（`internal/data`）：领取（顺序、区域、并发互斥、一次一条）、撤销、上报、结束、租约清扫、恢复及其与互斥位的
  竞态、日志清理、快速执行与文件内容、待审批 / 审批未通过的执行不被触碰、DB 角色权限。测试库须迁移到 0075
  （0075 要往通知域的表里写缺省数据，不能只单独应用 0070 与 0075——按顺序应用全部迁移）：

```bash
docker exec postgres psql -U postgres -c 'CREATE DATABASE reborn_jobengine_test'
for f in $(ls ../core/migrations/*.up.sql | sort); do
  docker exec -i postgres psql -U postgres -d reborn_jobengine_test -q -v ON_ERROR_STOP=1 < "$f"
done
```

- 真实 Ansible（`internal/runner/ansible_test.go`，`JOB_TEST_ANSIBLE=<ansible-playbook 路径>`，未设置则跳过）：对平台生成的
  清单、变量文件与三种快速执行的 Playbook 做 `--syntax-check`，并以 `-c local` 实际执行——验证 `!unsafe` 标记下模板
  语法不被求值、整数保持整数、脚本参数分词、文件内容与权限、`--limit` 文件（含 IPv6 主机）、PLAY RECAP 解析：

```bash
python3 -m venv /tmp/ansible && /tmp/ansible/bin/pip install ansible-core
JOB_TEST_ANSIBLE=/tmp/ansible/bin/ansible-playbook go test ./internal/runner/ -run TestRealAnsible -v
```

## 已知限制

- **仓库里的 Playbook 在节点上以节点用户的身份执行任意代码**，同一节点的并发作业之间、作业与接入令牌之间没有隔离
  （见「安全模型」开头）
- 拉取代码没有大小上限：浅克隆、单分支，但仓库本身很大时会占满 `work_dir`（tmpfs 即内存）；只有 10 分钟的时限
- 节点被强杀后的残留进程清理：Linux 上直接子进程立即随节点死亡，它们已脱离进程组的后代（`setsid`）要等节点下次启动、
  且环境里仍带着 `HOME=<作业目录>` 才清得掉；Linux 以外只在组长进程还活着时才能确认并清理
- 脱离了进程组的后代若一直握着输出管道，进程退出 2 秒后节点不再读它的输出（之后的输出丢弃）
- 「组装持续失败即放弃」的计数在各引擎实例的内存里，多实例各记各的；全库性的故障持续超过 1 分钟时，被反复领到的队首
  执行也会被置为失败（此时领取请求一直在报 500，问题本身更显眼）
- **结果未知不会自动收敛**：节点彻底丢失（机器宕机、进程被强杀）的执行停在结果未知，需要人工确认目标主机上的实际情况；
  引擎不重跑
- **禁止并发的模板在节点失联后可能出现两条执行同时在跑**：执行被判为结果未知即释放互斥位，而原来的作业可能仍在运行
  （见上文）。需要严格互斥时，在确认失联节点的作业已停止之前不要重新执行
- 领取应答在网络上丢失时，该执行已是执行中但没有节点在跑：租约到期后转为结果未知
- 排队中的执行由 core 直接取消；执行中的取消要等节点下一次上报才生效（至多约 3 秒），再加上 `cancel_grace`
- 日志掩码按字面匹配：凭据被目标主机上的程序变形后输出（如 base64、分行）则掩不住；短于 4 个字符的凭据不掩码
- 分发文件至多 4MiB，内容随领取应答下发并整个放在节点内存里一次；目标目录不存在时以登录用户（或提权用户）的缺省权限创建
- 执行脚本经 `ansible.builtin.script` 传到目标主机的临时目录再执行；`python3` 解释器要求目标主机上有 `python3`
- 滚动执行的批次之间没有停顿与健康检查钩子；中止后不会自动回滚已执行的批次
- 带口令的 SSH 私钥不支持（`ssh-add` 直接失败）
- 节点不安装 collection / role，不支持仓库子模块（浅克隆、单分支）
- 执行节点没有自己的业务指标；`/healthz` 在纯 runner 角色下恒为 ok，不反映引擎是否可达
- 两种角色同进程只用于本地开发：停机时引擎的后台循环与节点同时停止
- 结束请求持续被引擎以 4xx 拒绝（令牌在作业期间被重置等）10 次后节点放弃，该执行在租约到期后显示为结果未知
