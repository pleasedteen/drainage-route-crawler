-- 采集服务持久化 schema。
-- __PK_AUTO__ 占位符在初始化时按方言替换：
--   SQLite   -> INTEGER PRIMARY KEY AUTOINCREMENT
--   Postgres -> BIGSERIAL PRIMARY KEY

CREATE TABLE stations (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    base_url         TEXT NOT NULL,
    format           TEXT NOT NULL,
    min_interval_ms  INTEGER NOT NULL,
    max_inflight     INTEGER NOT NULL,
    watermark_seq    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE entry_points (
    id         __PK_AUTO__,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    label      TEXT NOT NULL
);

-- 页面修订缓存：同一 (站点, 页面, 修订) 只抓一次，跨入口/跨轮次共享。
CREATE TABLE page_revisions (
    id           __PK_AUTO__,
    station_id   TEXT NOT NULL,
    page_key     TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    body         TEXT NOT NULL,
    fetched_at   INTEGER NOT NULL,
    UNIQUE (station_id, page_key, revision)
);

-- 某修订解析出的下游链接（路线可跨站）。
CREATE TABLE links (
    id              __PK_AUTO__,
    from_station_id TEXT NOT NULL,
    from_page_key   TEXT NOT NULL,
    from_revision   INTEGER NOT NULL,
    position        INTEGER NOT NULL,
    to_station_id   TEXT NOT NULL,
    to_page_key     TEXT NOT NULL
);
CREATE UNIQUE INDEX ux_links ON links
    (from_station_id, from_page_key, from_revision, to_station_id, to_page_key);
-- 增量轮次计算“受影响路线”时按入边查找边界，必须有反向索引。
CREATE INDEX ix_links_to ON links (to_station_id, to_page_key);

-- 最新修订上的设施台账信息。
CREATE TABLE facilities (
    id         __PK_AUTO__,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    name       TEXT,
    kind       TEXT,
    UNIQUE (station_id, page_key, revision)
);

-- 各站可续读变更清单（登记顺序由 seq 给出）。
CREATE TABLE change_log (
    id         __PK_AUTO__,
    seq        INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    kind       TEXT NOT NULL,
    UNIQUE (station_id, seq)
);
CREATE INDEX ix_change_station_seq ON change_log (station_id, seq);

-- 采集轮次。watermarks 为 JSON：轮次开始时固化的各站登记截止点。
CREATE TABLE runs (
    id         __PK_AUTO__,
    entry_id   INTEGER NOT NULL,
    mode       TEXT NOT NULL,
    status     TEXT NOT NULL,
    watermarks TEXT NOT NULL,
    error      TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX ix_runs_entry ON runs (entry_id, id);

-- 轮次内的页面工作队列；汇合与回流成环由唯一约束天然去重。
CREATE TABLE run_pages (
    id         __PK_AUTO__,
    run_id     INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    state      TEXT NOT NULL,
    depth      INTEGER NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at INTEGER NOT NULL,
    UNIQUE (run_id, station_id, page_key)
);
CREATE INDEX ix_run_pages_state ON run_pages (run_id, state);

-- 增量轮次的“受影响路线”计划：只核查受新登记影响的页面。
-- 有成功轮次后，增量轮次从上一成功轮次继承未受影响的可达关系，
-- 仅对 affected 页面重新核验可达性。
CREATE TABLE run_delta (
    run_id      INTEGER PRIMARY KEY,
    base_run_id INTEGER NOT NULL,
    -- 从基线轮次原样继承的可达关系条数（进度统计用）。
    inherited   INTEGER NOT NULL DEFAULT 0,
    prepared    INTEGER NOT NULL DEFAULT 0
);

-- 受影响页面集合（新登记页面、修订页面、以及旧图中其下游页面）。
CREATE TABLE run_delta_pages (
    id         __PK_AUTO__,
    run_id     INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    role       TEXT NOT NULL,
    UNIQUE (run_id, station_id, page_key)
);
CREATE INDEX ix_run_delta_pages ON run_delta_pages (run_id, role);

-- 每轮已核实的可达关系。
CREATE TABLE reachable (
    id         __PK_AUTO__,
    run_id     INTEGER NOT NULL,
    entry_id   INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    depth      INTEGER NOT NULL,
    UNIQUE (run_id, station_id, page_key)
);
