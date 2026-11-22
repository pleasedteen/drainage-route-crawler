# 市政排水入流口下游设施采集服务

值班员选定一个入流口后，服务从该入口出发，沿各养护站台账页面中的下游链接遍历设施网络，
核实可达设施并支持进度查询。路线可**跨站、汇合、回流成环**；支持**全量**与**增量**两种采集；
进程崩溃后重启可**接着完成原轮次**。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 每轮开始确定各站登记截止点 | `Engine::start_run` 逐站读取 `latest_seq`，固化进 `runs.watermarks`（JSON），轮次内不变 |
| 变更清单按登记顺序可续读，业务时间可早于登记 | 一切以登记 `seq` 排序；`change_log (station_id, seq)` 唯一，分页游标 `after` 续读；页面用 `effective_seq` 支持按截止点 `as_of` 取版本 |
| 全量从入口重走 | 从入口页入队，按下游链接做去重遍历 |
| 增量复用未修订页面、补齐修订与新路线 | 同一套遍历引擎；目标修订由“截止点+变更清单”解析，未变页命中 `page_revisions` 缓存零请求，变化页才抓取（测试实证增量仅抓变化页） |
| 同一页面同一修订跨入口共享抓取结果 | `page_revisions` 对 `(station, page, revision)` 唯一约束，全局共享 |
| 同截止点两种模式可达关系相同 | 两种模式共用同一遍历代码路径，差异仅在缓存命中；`tests/equivalence.rs` 逐页比对 |
| 跨站、汇合、成环 | 链接带 `station_id`；`run_pages (run, station, page)` 唯一约束天然去重，环不会死循环 |
| 每站独立频率与在途上限 | `StationController`：每站独立的最小发起间隔 + `Semaphore` 在途闸门 |
| 暂时失败退避重试 | 指数退避 + 抖动，最多 5 次 |
| 站点要求等待则暂停该站、其他站继续 | 识别 `Retry-After`（秒数或 HTTP-date）写入该站控制器；**页面/状态/清单三类请求共用同一等待期**，等待结束前该站不再新发，耗尽重试后的等待同样生效 |
| 状态失败不伪造截止点 | 状态端点请求失败/响应缺字段一律返回错误，轮次中止，绝不把截止点填成 0 |
| 故障站不占满页面工作槽 | worker 只认领未处于等待期站点的页面；发起前发现暂停立即退回 `pending` 让出工作槽，其他站同阶段任务照常推进（每轮仍最多 8 个页面槽） |
| 进程退出后重启续跑，保留进度与抓取结果 | 所有状态确认后落库；启动时 `reset_open_run_pages` 把残留 `fetching/failed` 收回 `pending`，`resume_on_boot` 续跑 |
| 生产 PostgreSQL、本地嵌入式持久化替身 | sqlx `Any` 驱动同一套 SQL 同时支持 Postgres 与 SQLite（本地文件型，零外部依赖） |
| 分页 HTML 与 JSON 两种模拟站 | `src/mock.rs` + `mock-server` 二进制，两种格式各一站 |

## 目录结构

```
src/
  main.rs            CLI 与服务入口
  lib.rs             模块汇总
  config.rs          TOML 配置
  model.rs           领域模型
  store.rs           持久层（PostgreSQL / SQLite，schema 自动初始化）
  schema/schema.sql  建表语句（方言占位符在连接时替换）
  fetch.rs           抓取：每站频率/并发、退避、Retry-After 统一等待期
  parse.rs           HTML / JSON 页面解析
  engine.rs          轮次编排、截止点、遍历、断点续跑
  mock.rs            内存模拟站与 HTTP 模拟站（三类端点故障注入 + 请求时间线）
  verify.rs          本地 HTTP 模拟站一键验证（请求时间线 + 每轮结果断言）
  api.rs             值班员 HTTP 接口
  seed.rs            演示数据
  bin/mock-server.rs 本地 HTML/JSON 模拟站服务
tests/               集成测试（一致性、缓存、续跑、退避、Retry-After 统一等待期、HTTP API）
scripts/             一键测试、Retry-After 验证与崩溃续跑演示脚本
config/              本地与生产配置示例
```

## 快速开始

### 前置

- Rust stable（含 cargo）。本机若 `/usr/local/bin/cc` 不可用，项目 `.cargo/config.toml`
  已把链接器指向 `/usr/bin/gcc`；正常环境可删除该文件。

### 一条命令跑完本地测试（依赖就绪后 80 秒内）

```bash
./scripts/run_tests.sh
```

依赖已编译时测试阶段约 2–3 秒；即使包含本项目增量编译也在 30 秒左右。
首次运行会先编译第三方依赖，耗时取决于网络与机器，不计入 80 秒口径。

### 一条命令验证 Retry-After 统一等待期（本地 HTTP 模拟站）

```bash
./scripts/verify_retry_after.sh          # 默认 Retry-After=1s
# 或：cargo run -- verify --wait-secs 1
```

脚本会在临时端口起两个**真实 HTTP 模拟站**（HTML/JSON 各一），采集器走完整
HTTP 链路，自动断言并打印请求时间线，覆盖四个场景：

1. 页面请求返回 Retry-After：该站页面/状态/清单在等待期内零新发，恢复后 `reachable=8`；
2. 状态请求返回 Retry-After：截止点读取进入同一等待期，固化值为真实 `latest_seq`（非 0）；
3. 变更清单返回 Retry-After：翻页静默期间其他站同阶段任务继续推进；
4. 状态请求持续失败：轮次中止、不创建轮次记录，截止点绝不被填成 0。

集成测试 `tests/retry_after.rs` 另覆盖：耗尽重试后等待仍生效、故障站不占满
每轮 8 个页面工作槽、每站频率/在途预算保持等边界。

### 启动服务（本地演示，零外部依赖）

本地配置使用**进程内模拟站 + 文件型 SQLite**，无需起任何外部服务：

```bash
cargo run -- serve
# 监听 127.0.0.1:8080，数据库文件 ./data/drainage.db
```

启动时若发现上次未完成的轮次，会自动恢复并继续执行。

### 操作采集（HTTP）

```bash
# 查看入流口
curl -s http://127.0.0.1:8080/entries

# 启动全量采集
curl -s -X POST http://127.0.0.1:8080/runs/start \
  -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"full"}'
# -> {"run_id":1,"status":"running"}

# 启动增量采集
curl -s -X POST http://127.0.0.1:8080/runs/start \
  -H 'content-type: application/json' \
  -d '{"entry_id":1,"mode":"incremental"}'

# 查询轮次进度
curl -s http://127.0.0.1:8080/runs/1/progress

# 查询该轮次已核实可达设施
curl -s http://127.0.0.1:8080/runs/1/reachable

# 按入口查询最近成功轮次的可达设施
curl -s http://127.0.0.1:8080/entries/1/reachable
```

### 操作采集（CLI，无需常驻服务）

```bash
cargo run -- entries
cargo run -- start 1 --mode full --wait          # 全量并等待结束
cargo run -- start 1 --mode incremental --wait   # 增量
cargo run -- progress 1
cargo run -- reachable 1
cargo run -- entry-reachable 1
```

### 独立的 HTML / JSON 模拟站点（可选）

需要验证真实 HTTP 链路、分页与故障注入时，单独起模拟站：

```bash
cargo run --bin mock-server
# HTML 站 http://127.0.0.1:9100 （station-a）
# JSON 站 http://127.0.0.1:9101 （station-b）

# 按修订取页 / 按登记截止点取页
curl 'http://127.0.0.1:9100/pages/a1?revision=1'
curl 'http://127.0.0.1:9100/pages/a1?as_of=2'
# 分页变更清单（每页 3 条，next_after 为下一页游标）
curl 'http://127.0.0.1:9100/changes?after=0'
# 故障注入（旧接口，默认 page 端点）：临时失败 / Retry-After
curl -X POST 'http://127.0.0.1:9100/inject/fail?times=2'
curl -X POST 'http://127.0.0.1:9101/inject/retry-after?secs=2'
# 统一故障注入：endpoint=state|changes|page，kind=retry-after|fail
curl -X POST 'http://127.0.0.1:9100/inject/fault?endpoint=state&kind=retry-after&times=2&secs=1'
curl -X POST 'http://127.0.0.1:9101/inject/fault?endpoint=changes&kind=fail&times=1'
# 请求时间线与故障/日志重置
curl 'http://127.0.0.1:9100/requests'
curl -X POST 'http://127.0.0.1:9100/reset'
```

### 崩溃—重启续跑演示

```bash
./scripts/demo_resume.sh
```

脚本会启动服务、发起轮次、在中途 `SIGKILL` 进程，再重启服务并展示原轮次自动续跑到完成、
已确认的抓取结果不丢失。

## 生产部署（PostgreSQL）

1. 准备 Postgres 并建库（表结构由服务启动时自动创建）：

   ```sql
   CREATE DATABASE drainage;
   ```

2. 复制并修改配置：

   ```bash
   cp config/production.example.toml config/production.toml
   # 填写 database.url（postgres://...）与各站 base_url、频率、并发
   ```

3. 启动：

   ```bash
   cargo run --release -- --config config/production.toml serve
   ```

SQL 方言差异（自增主键等）在连接初始化时按驱动自动替换，业务代码与查询语句保持一套。

## 核心设计说明

### 为什么全量/增量天然等价

引擎不区分两种模式的遍历逻辑：

1. 轮次开始固化各站截止点 `watermarks`；
2. 续读变更清单到截止点，构建 `(站点, 页面) -> 目标修订` 索引；
3. 从入口按链接去重遍历，每页先查共享缓存：
   - 命中目标修订 → 零请求直接用；
   - 未命中 → 按站节奏抓取并落库；
4. 解析下游链接并入队，直到队列排空。

“全量从入口重走”与“增量补齐变化”因此只是**缓存命中率不同**，到达相同截止点时遍历的图、
每页的目标修订、最终可达关系逐页相同。

### 断点续跑的正确性边界

- 页面状态机：`pending → fetching → done/failed`，全部在数据库中；
- 抓取结果、链接、设施、可达关系均在确认后立即落库；
- 重启只把没有走完的 `fetching`（进程死亡时不可能有结论）和 `failed`（允许重试）收回；
- 已 `done` 的页面与已缓存修订不会重复抓取，唯一约束兜底幂等。

### 抓取节奏与隔离

每个站点一个 `StationController`，各自维护最小请求间隔、在途信号量、暂停时刻。
`Retry-After` 只写入对应站点的控制器，因此 A 站被要求等待时，B 站请求照常进行。

#### Retry-After 统一等待期（三类请求共享）

- **同一暂停时刻**：页面、状态截止点、变更清单三类请求任一收到 `Retry-After`
  （支持秒数与 HTTP-date），都写入该站控制器的同一个 `paused_until`，取更长者；
  等待结束前该站任何请求类型都不会新发。
- **耗尽重试也生效**：最后一次允许尝试命中 `Retry-After` 时，先落等待期再返回错误，
  不会因为预算耗尽而跳过等待。
- **不占工作槽**：`acquire()` 发现站点暂停时**立即**返回 `StationPaused` 而非抱槽睡眠；
  worker 只认领未暂停站点的页面，认领后若发现刚暂停则把页面退回 `pending`。
  因此故障站积压再多页面，其他站同阶段任务也能拿到每轮最多 8 个页面槽继续推进。
- **截止点不伪造**：状态请求连接失败、非成功状态或响应缺少合法 `latest_seq`
  都返回错误并中止轮次，绝不会把截止点默认为 0。
- 预算约束不变：每站 `min_interval_ms` 频率与 `max_inflight` 在途上限照旧，
  每轮页面 worker 数仍为各站在途之和并夹紧到 8。

## HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/stations` | 站点配置（含水位） |
| GET | `/entries` | 入流口列表 |
| POST | `/runs/start` | 启动轮次，body `{"entry_id":1,"mode":"full|incremental"}` |
| GET | `/runs?limit=20` | 轮次列表 |
| GET | `/runs/:id` | 轮次详情（含固化截止点） |
| GET | `/runs/:id/progress` | 进度（总数/完成/在途/失败/可达数） |
| GET | `/runs/:id/reachable` | 该轮次已核实可达设施 |
| GET | `/entries/:id/reachable` | 该入口最近成功轮次的可达设施 |

## 测试覆盖

| 文件 | 覆盖点 |
| --- | --- |
| `tests/equivalence.rs` | 全量/增量同截止点可达关系一致；截止点固化使旧轮次不受后续登记影响 |
| `tests/cache.rs` | 同修订跨入口/跨轮次只抓一次；增量只抓变化页（请求计数实证）；新路线补齐 |
| `tests/resume.rs` | SIGKILL 后续跑且保留进度；Retry-After 只暂停本站；临时失败退避重试 |
| `tests/retry_after.rs` | 三类端点 Retry-After 统一等待期；状态失败不写 0 截止点；故障站不占满 8 个页面槽且他站继续；耗尽重试后等待仍生效；频率/在途预算保持 |
| `tests/http_api.rs` | 真实 HTTP 监听下端到端采集与查询、参数校验 |
