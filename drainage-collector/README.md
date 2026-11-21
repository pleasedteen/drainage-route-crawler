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
| 有成功轮次后，增量只核查受新登记影响的路线 | 以最近成功轮次为基线：比对脏页的旧边/新边，只沿变更边做前向闭包得到“受影响区域”，区域外继承基线结果、区域内按新图重算（`run_delta*` 表持久化计划，可续跑） |
| 跨站回流环失去最后入口时增量仍与全量一致 | 删除边沿旧图前向闭包，环整体进入受影响区域并重算失达；`tests/incremental_delta.rs` 与同截止点全量逐页比对 |
| 增量读写的设施/边记录有界 | 增量只读写受影响区域；2000+ 页基线上改动 10 页内独立支线，设施+边读写各 ≤100 条（`Store::io` 计数实证） |
| 同一页面同一修订跨入口共享抓取结果 | `page_revisions` 对 `(station, page, revision)` 唯一约束，全局共享 |
| 同截止点两种模式可达关系相同 | 目标修订均由“截止点+变更清单”解析，增量在区域外复用基线、区域内重算新图；`tests/equivalence.rs`、`tests/incremental_delta.rs` 逐页比对 |
| 跨站、汇合、成环 | 链接带 `station_id`；`run_pages (run, station, page)` 唯一约束天然去重，环不会死循环 |
| 每站独立频率与在途上限 | `StationController`：每站独立的最小发起间隔 + `Semaphore` 在途闸门 |
| 暂时失败退避重试 | 指数退避 + 抖动，最多 5 次 |
| 站点要求等待则暂停该站、其他站继续 | 识别 `Retry-After`（秒数或 HTTP-date），只暂停对应站的控制器，其余站不受影响 |
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
  fetch.rs           抓取：每站频率/并发、退避、Retry-After 暂停
  parse.rs           HTML / JSON 页面解析
  engine.rs          轮次编排、截止点、遍历、断点续跑
  mock.rs            内存模拟站与 HTTP 模拟站
  api.rs             值班员 HTTP 接口
  seed.rs            演示数据
  bin/mock-server.rs 本地 HTML/JSON 模拟站服务
tests/               6 个集成测试文件（一致性、增量边界、随机图、缓存、续跑、退避、HTTP API）
scripts/             一键测试、增量验收、崩溃续跑演示脚本
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
# 故障注入：临时失败 / Retry-After
curl -X POST 'http://127.0.0.1:9100/inject/fail?times=2'
curl -X POST 'http://127.0.0.1:9101/inject/retry-after?secs=2'
```

### 崩溃—重启续跑演示

```bash
./scripts/demo_resume.sh
```

脚本会启动服务、发起轮次、在中途 `SIGKILL` 进程，再重启服务并展示原轮次自动续跑到完成、
已确认的抓取结果不丢失。

### 增量“只核查受影响路线”验收（一条命令，80 秒内）

```bash
./scripts/demo_incremental.sh
```

覆盖：2000+ 页基线上改动 ≤10 页独立支线时，增量累计读写的设施与边记录各 ≤100 条
（基线准备与结果查询另计）；跨站回流环失去最后一条入口路线时，增量结果仍与同截止点
全量逐页一致；增量中途退出后重启接着原轮次继续，旧轮次照常查询。

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

### 增量：只核查受影响的路线

有成功轮次后，增量不再从入口重走全图，而是以**最近成功轮次**为基线：

1. 轮次开始固化各站截止点 `watermarks`；续读变更清单到截止点，构建
   `(站点, 页面) -> 目标修订` 索引；
2. **脏页** = 基线截止点到本轮截止点之间被登记（修订/新增/移除）的页面；
3. 对每个基线可达的脏页，比对它在基线修订下的**旧边**与本轮**新边**，只把
   **变更边**（新增/删除）的另一端作为闭包种子：
   - 新增边沿新图前向闭包，只进入基线不可达的新页面，并入主干即停；
   - 删除边沿旧图前向闭包，只沿基线可达者传播——**跨站回流环失去最后一条入口
     路线时，整环因此进入受影响区域并被重算为失达**；
   - 脏页上未变化的边不参与扩散，避免一条支线改动牵动整片下游；
4. 受影响区域**之外**的可达关系原样继承基线；**区域内**的页面以“区域外基线
   可达页面的入边”为种子，按新图重算最短路并覆盖。

任何可达性变化都必然经过某条变更边，因此“区域外继承 + 区域内重算”与同截止点
全量**逐页一致**；受影响区域之外一次也不读、不写。实测 2000+ 页基线、10 页内
独立支线的增量，设施+边读写各在 20 条以内（预算 100）。

计划写入 `run_delta` / `run_delta_pages` 并以 `prepared` 标记；进程中途退出时
该标记未落，重启后对同一 `run_id` 重新计算计划（已抓取的修订走缓存不重复请求），
续跑完成后结果与全量一致；旧轮次的结果行从不改写，照常查询。

### 断点续跑的正确性边界

- 页面状态机：`pending → fetching → done/failed`，全部在数据库中；
- 抓取结果、链接、设施、可达关系均在确认后立即落库；
- 重启只把没有走完的 `fetching`（进程死亡时不可能有结论）和 `failed`（允许重试）收回；
- 已 `done` 的页面与已缓存修订不会重复抓取，唯一约束兜底幂等；
- 增量计划在 `prepared` 落库前可重入：重启会从基线确定性重建，不重复抓取。

### 抓取节奏与隔离

每个站点一个 `StationController`，各自维护最小请求间隔、在途信号量、暂停时刻。
`Retry-After` 只写入对应站点的控制器，因此 A 站被要求等待时，B 站请求照常进行。

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
| `tests/incremental_delta.rs` | 跨站回流环失去最后入口时增量与同截止点全量一致；2000+ 页基线改 10 页内支线，增量设施+边读写各 ≤100 条；增量中途退出后续跑原轮次且旧轮次可查；悬空前向引用补全、页面移除与全量一致 |
| `tests/randomized_delta.rs` | 确定性随机小图（跨站/汇合/成环/增删）上，增量与同截止点全量逐页一致 |
| `tests/resume.rs` | SIGKILL 后续跑且保留进度；Retry-After 只暂停本站；临时失败退避重试 |
| `tests/http_api.rs` | 真实 HTTP 监听下端到端采集与查询、参数校验 |
