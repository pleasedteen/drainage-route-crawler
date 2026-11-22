//! 持久化层。
//!
//! 生产用 PostgreSQL（`postgres://...`），本地测试用嵌入式持久化替身 SQLite
//! （`sqlite://...` 或 `sqlite::memory:`）。两者通过 sqlx 的 `Any` 驱动在同一套
//! SQL 下工作，业务代码不感知方言差异。
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use serde_json::Value;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use sqlx::{AnyPool, Row};

use crate::model::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Sqlite,
    Postgres,
}

#[derive(Clone)]
pub struct Store {
    pub pool: AnyPool,
    pub kind: DbKind,
}

const DDL: &str = include_str!("../schema/schema.sql");

/// schema v2：为 `reachable` 增加“结果代次”，为 `runs` 增加“已发布代次”指针。
///
/// 旧库（v1）的 `reachable` 上有 `(run_id, station_id, page_key)` 唯一约束，
/// 无法直接再容纳一份修复结果，因此重建该表；旧行归入第 1 代，行为不变。
/// 同时新增修复任务表、修复工作队列与“截止点 → 修订”记忆表。
const MIGRATION_V2: &str = r#"
ALTER TABLE runs ADD COLUMN published_generation INTEGER NOT NULL DEFAULT 1;
CREATE TABLE reachable_v2 (
    id         __PK_AUTO__,
    run_id     INTEGER NOT NULL,
    entry_id   INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    depth      INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    UNIQUE (run_id, generation, station_id, page_key)
);
INSERT INTO reachable_v2 (run_id, entry_id, station_id, page_key, revision, depth, generation)
    SELECT r.run_id, r.entry_id, r.station_id, r.page_key, r.revision, r.depth,
           COALESCE((SELECT published_generation FROM runs WHERE runs.id = r.run_id), 1)
    FROM reachable r;
DROP TABLE reachable;
ALTER TABLE reachable_v2 RENAME TO reachable;

CREATE TABLE repair_runs (
    run_id     INTEGER PRIMARY KEY,
    status     TEXT NOT NULL,
    generation INTEGER NOT NULL,
    error      TEXT,
    started_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE repair_pages (
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
CREATE INDEX ix_repair_pages_state ON repair_pages (run_id, state);

-- 已核实过的“某站某页在给定截止点下的修订”记忆：修复/续跑时直接复用，
-- 不再向站点重复抓取。
CREATE TABLE page_as_of (
    station_id TEXT NOT NULL,
    page_key   TEXT NOT NULL,
    as_of_seq  INTEGER NOT NULL,
    revision   INTEGER NOT NULL,
    UNIQUE (station_id, page_key, as_of_seq)
);
"#;

/// 修复任务内部记录。
#[derive(Debug, Clone)]
pub struct RepairRecord {
    pub run_id: i64,
    pub status: String,
    pub generation: i64,
    pub error: Option<String>,
    pub started_at: i64,
    pub updated_at: i64,
}

impl Store {
    /// 连接数据库并完成 schema 初始化 / 升级（幂等，可重复执行）。
    pub async fn connect(url: &str) -> Result<Self> {
        // sqlx Any 不会自动注册驱动，全局安装一次即可。
        static DRIVERS: std::sync::Once = std::sync::Once::new();
        DRIVERS.call_once(sqlx::any::install_default_drivers);
        let opts = AnyConnectOptions::from_str(url)?;
        let kind = if url.starts_with("postgres") {
            DbKind::Postgres
        } else {
            DbKind::Sqlite
        };
        if kind == DbKind::Sqlite {
            // sqlx 0.8 的 SQLite 默认 open 标志不含 SQLITE_OPEN_CREATE，
            // 经 Any 驱动又无法设置 create_if_missing，故在连接前预建文件与目录。
            // 解析顺序与 SqliteConnectOptions::from_str 保持一致：
            // 先去 "sqlite://" 再去 "sqlite:"，取 '?' 之前的部分为文件路径。
            let rest = url
                .trim_start_matches("sqlite://")
                .trim_start_matches("sqlite:");
            let path = rest.split('?').next().unwrap_or(rest);
            if !path.is_empty() && path != ":memory:" {
                let p = std::path::Path::new(path);
                if let Some(parent) = p.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                if !p.exists() {
                    std::fs::File::create(p)?;
                }
            }
        }
        let max_conn = if kind == DbKind::Sqlite { 1 } else { 8 };
        let pool = AnyPoolOptions::new()
            .max_connections(max_conn)
            .connect_lazy_with(opts);
        let store = Store { pool, kind };
        store.init_schema().await?;
        Ok(store)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS schema_versions (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)")
            .execute(&self.pool)
            .await?;
        let current: i64 =
            sqlx::query("SELECT COALESCE(MAX(version), 0) AS v FROM schema_versions")
                .fetch_one(&self.pool)
                .await?
                .try_get("v")?;
        if current < 1 {
            let pk = match self.kind {
                DbKind::Sqlite => "INTEGER PRIMARY KEY AUTOINCREMENT",
                DbKind::Postgres => "BIGSERIAL PRIMARY KEY",
            };
            let ddl = DDL.replace("__PK_AUTO__", pk);
            for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                sqlx::query(stmt).execute(&self.pool).await?;
            }
            sqlx::query("INSERT INTO schema_versions (version, applied_at) VALUES (1, ?)")
                .bind(now_millis())
                .execute(&self.pool)
                .await?;
        }
        if current < 2 {
            // 升级旧库：一次性完成，迁移语句与 schema v2 完全一致。
            let pk = match self.kind {
                DbKind::Sqlite => "INTEGER PRIMARY KEY AUTOINCREMENT",
                DbKind::Postgres => "BIGSERIAL PRIMARY KEY",
            };
            let ddl = MIGRATION_V2.replace("__PK_AUTO__", pk);
            let mut tx = self.pool.begin().await?;
            for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                sqlx::query(stmt).execute(&mut *tx).await?;
            }
            sqlx::query("INSERT INTO schema_versions (version, applied_at) VALUES (2, ?)")
                .bind(now_millis())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        if self.kind == DbKind::Sqlite {
            // 写串行化后 WAL 即可；打开外键约束。
            for pragma in ["PRAGMA foreign_keys = ON", "PRAGMA journal_mode = WAL"] {
                let _ = sqlx::query(pragma).execute(&self.pool).await;
            }
        }
        Ok(())
    }

    // ---------- 站点 / 入流口 ----------

    pub async fn upsert_station(&self, cfg: &StationConfig) -> Result<()> {
        sqlx::query(
            "INSERT INTO stations (id, name, base_url, format, min_interval_ms, max_inflight, watermark_seq) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, base_url=excluded.base_url, \
               format=excluded.format, min_interval_ms=excluded.min_interval_ms, max_inflight=excluded.max_inflight",
        )
        .bind(&cfg.id)
        .bind(&cfg.name)
        .bind(&cfg.base_url)
        .bind(cfg.format.as_str())
        .bind(cfg.min_interval_ms as i64)
        .bind(cfg.max_inflight as i64)
        .bind(cfg.watermark_seq)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_stations(&self) -> Result<Vec<StationConfig>> {
        let rows = sqlx::query(
            "SELECT id, name, base_url, format, min_interval_ms, max_inflight, watermark_seq \
             FROM stations ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                let fmt: String = r.try_get("format")?;
                Ok(StationConfig {
                    id: r.try_get("id")?,
                    name: r.try_get("name")?,
                    base_url: r.try_get("base_url")?,
                    format: SiteFormat::parse(&fmt)?,
                    min_interval_ms: r.try_get::<i64, _>("min_interval_ms")? as u64,
                    max_inflight: r.try_get::<i64, _>("max_inflight")? as u32,
                    watermark_seq: r.try_get("watermark_seq")?,
                })
            })
            .collect()
    }

    pub async fn get_station(&self, id: &str) -> Result<Option<StationConfig>> {
        Ok(self.list_stations().await?.into_iter().find(|s| s.id == id))
    }

    pub async fn set_station_watermark(&self, id: &str, seq: i64) -> Result<()> {
        sqlx::query("UPDATE stations SET watermark_seq = ? WHERE id = ?")
            .bind(seq)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn add_entry(&self, station_id: &str, page_key: &str, label: &str) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO entry_points (station_id, page_key, label) VALUES (?, ?, ?) RETURNING id",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(label)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("id")?)
    }

    pub async fn get_entry(&self, id: i64) -> Result<Option<EntryPoint>> {
        let row =
            sqlx::query("SELECT id, station_id, page_key, label FROM entry_points WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| EntryPoint {
            id: r.try_get("id").unwrap_or_default(),
            station_id: r.try_get("station_id").unwrap_or_default(),
            page_key: r.try_get("page_key").unwrap_or_default(),
            label: r.try_get("label").unwrap_or_default(),
        }))
    }

    pub async fn list_entries(&self) -> Result<Vec<EntryPoint>> {
        let rows =
            sqlx::query("SELECT id, station_id, page_key, label FROM entry_points ORDER BY id")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|r| EntryPoint {
                id: r.try_get("id").unwrap_or_default(),
                station_id: r.try_get("station_id").unwrap_or_default(),
                label: r.try_get("label").unwrap_or_default(),
                page_key: r.try_get("page_key").unwrap_or_default(),
            })
            .collect())
    }

    // ---------- 页面修订缓存 ----------

    /// 取某页面某修订的已确认抓取结果（同一修订跨入口/跨轮次共享）。
    pub async fn get_page_revision(
        &self,
        station_id: &str,
        page_key: &str,
        revision: i64,
    ) -> Result<Option<PageRevision>> {
        let row = sqlx::query(
            "SELECT id, station_id, page_key, revision, content_type, body, fetched_at \
             FROM page_revisions WHERE station_id = ? AND page_key = ? AND revision = ?",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(revision)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| PageRevision {
            id: r.try_get("id").unwrap_or_default(),
            station_id: r.try_get("station_id").unwrap_or_default(),
            page_key: r.try_get("page_key").unwrap_or_default(),
            revision: r.try_get("revision").unwrap_or_default(),
            content_type: r.try_get("content_type").unwrap_or_default(),
            body: r.try_get("body").unwrap_or_default(),
            fetched_at: r.try_get("fetched_at").unwrap_or_default(),
        }))
    }

    /// 取某页面最新已缓存修订（增量模式用来复用未修订页面）。
    pub async fn latest_revision(
        &self,
        station_id: &str,
        page_key: &str,
    ) -> Result<Option<(i64, PageRevision)>> {
        let row = sqlx::query(
            "SELECT id, station_id, page_key, revision, content_type, body, fetched_at \
             FROM page_revisions WHERE station_id = ? AND page_key = ? \
             ORDER BY revision DESC LIMIT 1",
        )
        .bind(station_id)
        .bind(page_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            let rev = r.try_get("revision").unwrap_or_default();
            (
                rev,
                PageRevision {
                    id: r.try_get("id").unwrap_or_default(),
                    station_id: r.try_get("station_id").unwrap_or_default(),
                    page_key: r.try_get("page_key").unwrap_or_default(),
                    revision: rev,
                    content_type: r.try_get("content_type").unwrap_or_default(),
                    body: r.try_get("body").unwrap_or_default(),
                    fetched_at: r.try_get("fetched_at").unwrap_or_default(),
                },
            )
        }))
    }

    pub async fn insert_page_revision(
        &self,
        station_id: &str,
        page_key: &str,
        revision: i64,
        content_type: &str,
        body: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO page_revisions (station_id, page_key, revision, content_type, body, fetched_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(station_id, page_key, revision) DO UPDATE SET \
               content_type=excluded.content_type, body=excluded.body, fetched_at=excluded.fetched_at",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(revision)
        .bind(content_type)
        .bind(body)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 记录某修订解析出的下游链接（幂等替换）。
    pub async fn replace_links(
        &self,
        station_id: &str,
        page_key: &str,
        revision: i64,
        links: &[LinkRef],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM links WHERE from_station_id = ? AND from_page_key = ? AND from_revision = ?",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(revision)
        .execute(&mut *tx)
        .await?;
        for (i, l) in links.iter().enumerate() {
            sqlx::query(
                "INSERT INTO links (from_station_id, from_page_key, from_revision, position, to_station_id, to_page_key) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(station_id)
            .bind(page_key)
            .bind(revision)
            .bind(i as i64)
            .bind(&l.station_id)
            .bind(&l.page_key)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_facility(
        &self,
        station_id: &str,
        page_key: &str,
        revision: i64,
        facility: &FacilityInfo,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO facilities (station_id, page_key, revision, name, kind) VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(station_id, page_key, revision) DO UPDATE SET name=excluded.name, kind=excluded.kind",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(revision)
        .bind(&facility.name)
        .bind(&facility.kind)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ---------- 变更清单（按登记顺序可续读） ----------

    pub async fn count_changes(&self, station_id: &str, after_seq: i64) -> Result<i64> {
        let row =
            sqlx::query("SELECT COUNT(*) AS c FROM change_log WHERE station_id = ? AND seq > ?")
                .bind(station_id)
                .bind(after_seq)
                .fetch_one(&self.pool)
                .await?;
        Ok(row.try_get("c")?)
    }

    pub async fn list_changes(
        &self,
        station_id: &str,
        after_seq: i64,
    ) -> Result<Vec<ChangeLogEntry>> {
        let rows = sqlx::query(
            "SELECT seq, station_id, page_key, revision, kind FROM change_log \
             WHERE station_id = ? AND seq > ? ORDER BY seq",
        )
        .bind(station_id)
        .bind(after_seq)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ChangeLogEntry {
                seq: r.try_get("seq").unwrap_or_default(),
                station_id: r.try_get("station_id").unwrap_or_default(),
                page_key: r.try_get("page_key").unwrap_or_default(),
                revision: r.try_get("revision").unwrap_or_default(),
                kind: r.try_get("kind").unwrap_or_default(),
            })
            .collect())
    }

    pub async fn insert_change(&self, e: &ChangeLogEntry) -> Result<()> {
        sqlx::query(
            "INSERT INTO change_log (seq, station_id, page_key, revision, kind) VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(station_id, seq) DO NOTHING",
        )
        .bind(e.seq)
        .bind(&e.station_id)
        .bind(&e.page_key)
        .bind(e.revision)
        .bind(&e.kind)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ---------- 采集轮次 ----------

    pub async fn create_run(
        &self,
        entry_id: i64,
        mode: RunMode,
        watermarks: &BTreeMap<String, i64>,
    ) -> Result<i64> {
        let wm = serde_json::to_string(watermarks)?;
        let now = now_millis();
        let row = sqlx::query(
            "INSERT INTO runs (entry_id, mode, status, watermarks, error, created_at, updated_at) \
             VALUES (?, ?, 'running', ?, NULL, ?, ?) RETURNING id",
        )
        .bind(entry_id)
        .bind(mode.as_str())
        .bind(wm)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("id")?)
    }

    pub async fn get_run(&self, run_id: i64) -> Result<Run> {
        let row = sqlx::query(
            "SELECT id, entry_id, mode, status, watermarks, error, published_generation, created_at, updated_at \
             FROM runs WHERE id = ?",
        )
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?;
        run_from_row(&row)
    }

    pub async fn list_runs(&self, limit: i64) -> Result<Vec<Run>> {
        let rows = sqlx::query(
            "SELECT id, entry_id, mode, status, watermarks, error, published_generation, created_at, updated_at \
             FROM runs ORDER BY id DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(run_from_row).collect()
    }

    pub async fn update_run_status(
        &self,
        run_id: i64,
        status: RunStatus,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE runs SET status = ?, error = ?, updated_at = ? WHERE id = ?")
            .bind(status.as_str())
            .bind(error)
            .bind(now_millis())
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 重启时把崩溃残留的 fetching 收回为 pending，失败页也允许重试。
    pub async fn reset_open_run_pages(&self) -> Result<()> {
        sqlx::query(
            "UPDATE run_pages SET state = 'pending' WHERE run_id IN \
             (SELECT id FROM runs WHERE status = 'running') AND state IN ('fetching','failed')",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_run_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
    ) -> Result<Option<(String, i64)>> {
        let row = sqlx::query(
            "SELECT state, depth FROM run_pages WHERE run_id = ? AND station_id = ? AND page_key = ?",
        )
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.try_get::<String, _>("state").unwrap_or_default(),
                r.try_get::<i64, _>("depth").unwrap_or_default(),
            )
        }))
    }

    /// 登记一个待处理页面；已存在则忽略（回流成环 / 多路线汇合天然去重）。
    pub async fn enqueue_run_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
        depth: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO run_pages (run_id, station_id, page_key, state, depth, attempts, last_error, updated_at) \
             VALUES (?, ?, ?, 'pending', ?, 0, NULL, ?) \
             ON CONFLICT(run_id, station_id, page_key) DO NOTHING",
        )
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .bind(depth)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 原子认领一个 pending 页面（两种方言同一 SQL；Any 驱动负责占位符转换，
    /// 并发 worker 间靠行锁串行化，不会重复认领）。
    pub async fn claim_run_page(&self, run_id: i64) -> Result<Option<(String, String, i64, i64)>> {
        let row = sqlx::query(
            "UPDATE run_pages SET state = 'fetching', updated_at = ? \
             WHERE id = (SELECT id FROM run_pages WHERE run_id = ? AND state = 'pending' LIMIT 1) \
             RETURNING station_id, page_key, depth, attempts",
        )
        .bind(now_millis())
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.try_get::<String, _>("station_id").unwrap_or_default(),
                r.try_get::<String, _>("page_key").unwrap_or_default(),
                r.try_get::<i64, _>("depth").unwrap_or_default(),
                r.try_get::<i64, _>("attempts").unwrap_or_default(),
            )
        }))
    }

    pub async fn complete_run_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE run_pages SET state = 'done', attempts = attempts + 1, last_error = NULL, updated_at = ? \
             WHERE run_id = ? AND station_id = ? AND page_key = ?",
        )
        .bind(now_millis())
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_run_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
        err: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE run_pages SET state = 'failed', attempts = attempts + 1, last_error = ?, updated_at = ? \
             WHERE run_id = ? AND station_id = ? AND page_key = ?",
        )
        .bind(err)
        .bind(now_millis())
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_run_pages(&self, run_id: i64) -> Result<RunPageCounts> {
        let rows = sqlx::query(
            "SELECT state, COUNT(*) AS c FROM run_pages WHERE run_id = ? GROUP BY state",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let mut c = RunPageCounts::default();
        for r in rows {
            let state: String = r.try_get("state")?;
            let n: i64 = r.try_get("c")?;
            match state.as_str() {
                "pending" => c.pending += n,
                "fetching" => c.fetching += n,
                "done" => c.done += n,
                "failed" => c.failed += n,
                _ => {}
            }
        }
        c.total = c.pending + c.fetching + c.done + c.failed;
        Ok(c)
    }

    // ---------- 可达关系 ----------

    /// 修复或重算时整代替换（只动指定代次，发布代次指针另行翻转）。
    pub async fn replace_generation_reachable(
        &self,
        run_id: i64,
        generation: i64,
        entry_id: i64,
        items: &[(LinkRef, i64, i64)],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM reachable WHERE run_id = ? AND generation = ?")
            .bind(run_id)
            .bind(generation)
            .execute(&mut *tx)
            .await?;
        for (l, revision, depth) in items {
            sqlx::query(
                "INSERT INTO reachable (run_id, generation, entry_id, station_id, page_key, revision, depth) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(run_id, generation, station_id, page_key) DO UPDATE SET \
                   revision=excluded.revision, depth=excluded.depth, entry_id=excluded.entry_id",
            )
            .bind(run_id)
            .bind(generation)
            .bind(entry_id)
            .bind(&l.station_id)
            .bind(&l.page_key)
            .bind(*revision)
            .bind(*depth)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 页面核实完成后随到随写，写入当前构建代次。
    pub async fn upsert_reachable(
        &self,
        run_id: i64,
        generation: i64,
        entry_id: i64,
        station_id: &str,
        page_key: &str,
        revision: i64,
        depth: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO reachable (run_id, generation, entry_id, station_id, page_key, revision, depth) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(run_id, generation, station_id, page_key) DO UPDATE SET \
               revision=excluded.revision, depth=MIN(reachable.depth, excluded.depth), entry_id=excluded.entry_id",
        )
        .bind(run_id)
        .bind(generation)
        .bind(entry_id)
        .bind(station_id)
        .bind(page_key)
        .bind(revision)
        .bind(depth)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 查询某轮次已发布代次的可达设施。
    pub async fn list_reachable(&self, run_id: i64) -> Result<Vec<ReachableFacility>> {
        let generation: i64 = sqlx::query("SELECT published_generation FROM runs WHERE id = ?")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await?
            .try_get("published_generation")?;
        self.list_reachable_generation(run_id, generation).await
    }

    /// 查询某轮次**指定代次**的可达设施（切换前可用于核对新结果）。
    pub async fn list_reachable_generation(
        &self,
        run_id: i64,
        generation: i64,
    ) -> Result<Vec<ReachableFacility>> {
        let rows = sqlx::query(
            "SELECT r.station_id, s.name AS station_name, r.page_key, r.revision, r.depth, \
                    f.name AS facility_name, f.kind AS facility_kind \
             FROM reachable r \
             LEFT JOIN stations s ON s.id = r.station_id \
             LEFT JOIN facilities f ON f.station_id = r.station_id AND f.page_key = r.page_key AND f.revision = r.revision \
             WHERE r.run_id = ? AND r.generation = ? ORDER BY r.depth, r.station_id, r.page_key",
        )
        .bind(run_id)
        .bind(generation)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ReachableFacility {
                station_id: r.try_get("station_id").unwrap_or_default(),
                station_name: r.try_get("station_name").unwrap_or_default(),
                page_key: r.try_get("page_key").unwrap_or_default(),
                revision: r.try_get("revision").ok(),
                name: r.try_get("facility_name").ok(),
                kind: r.try_get("facility_kind").ok(),
                depth: r.try_get("depth").unwrap_or_default(),
            })
            .collect())
    }

    pub async fn count_reachable(&self, run_id: i64) -> Result<i64> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS c FROM reachable r JOIN runs u ON u.id = r.run_id \
             WHERE r.run_id = ? AND r.generation = u.published_generation",
        )
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("c")?)
    }

    /// 取某轮次已发布代次的可达页面集合（含修订与深度），供增量轮次复用。
    pub async fn reachable_set(&self, run_id: i64) -> Result<Vec<(String, String, i64, i64)>> {
        let rows = sqlx::query(
            "SELECT r.station_id, r.page_key, r.revision, r.depth FROM reachable r \
             JOIN runs u ON u.id = r.run_id \
             WHERE r.run_id = ? AND r.generation = u.published_generation",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.try_get::<String, _>("station_id").unwrap_or_default(),
                    r.try_get::<String, _>("page_key").unwrap_or_default(),
                    r.try_get::<i64, _>("revision").unwrap_or_default(),
                    r.try_get::<i64, _>("depth").unwrap_or_default(),
                )
            })
            .collect())
    }

    pub async fn distinct_reachable_keys(&self, run_id: i64) -> Result<BTreeSet<(String, String)>> {
        let rows = sqlx::query(
            "SELECT DISTINCT r.station_id, r.page_key FROM reachable r \
             JOIN runs u ON u.id = r.run_id \
             WHERE r.run_id = ? AND r.generation = u.published_generation",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.try_get::<String, _>("station_id").unwrap_or_default(),
                    r.try_get::<String, _>("page_key").unwrap_or_default(),
                )
            })
            .collect())
    }

    pub async fn latest_succeeded_run(
        &self,
        entry_id: i64,
        mode: Option<RunMode>,
    ) -> Result<Option<Run>> {
        let rows = sqlx::query(
            "SELECT id, entry_id, mode, status, watermarks, error, published_generation, created_at, updated_at \
             FROM runs WHERE entry_id = ? AND status = 'succeeded' ORDER BY id DESC LIMIT 20",
        )
        .bind(entry_id)
        .fetch_all(&self.pool)
        .await?;
        for r in &rows {
            let mode_str: String = r.try_get("mode")?;
            let m = RunMode::parse(&mode_str)?;
            if let Some(want) = mode {
                if m != want {
                    continue;
                }
            }
            return Ok(Some(run_from_row(r)?));
        }
        Ok(None)
    }

    /// 某入口在给定截止点下，已核实可达的页面（取最新成功轮次）。
    pub async fn latest_reachable_for_entry(
        &self,
        entry_id: i64,
    ) -> Result<Option<(i64, Vec<ReachableFacility>)>> {
        if let Some(run) = self.latest_succeeded_run(entry_id, None).await? {
            let items = self.list_reachable(run.id).await?;
            return Ok(Some((run.id, items)));
        }
        Ok(None)
    }
}

// ---------- 历史轮次修复：代次、修复队列、截止点记忆 ----------

impl Store {
    /// 读取已固化的“截止点 → 修订”记忆。
    pub async fn get_page_as_of(
        &self,
        station_id: &str,
        page_key: &str,
        as_of_seq: i64,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT revision FROM page_as_of WHERE station_id = ? AND page_key = ? AND as_of_seq = ?",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(as_of_seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.try_get::<i64, _>("revision").ok()))
    }

    pub async fn put_page_as_of(
        &self,
        station_id: &str,
        page_key: &str,
        as_of_seq: i64,
        revision: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO page_as_of (station_id, page_key, as_of_seq, revision) VALUES (?, ?, ?, ?) \
             ON CONFLICT(station_id, page_key, as_of_seq) DO NOTHING",
        )
        .bind(station_id)
        .bind(page_key)
        .bind(as_of_seq)
        .bind(revision)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 读取修复任务记录。
    pub async fn get_repair(&self, run_id: i64) -> Result<Option<RepairRecord>> {
        let row = sqlx::query(
            "SELECT run_id, status, generation, error, started_at, updated_at \
             FROM repair_runs WHERE run_id = ?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| RepairRecord {
            run_id: r.try_get("run_id").unwrap_or_default(),
            status: r.try_get("status").unwrap_or_default(),
            generation: r.try_get("generation").unwrap_or_default(),
            error: r.try_get("error").ok().flatten(),
            started_at: r.try_get("started_at").unwrap_or_default(),
            updated_at: r.try_get("updated_at").unwrap_or_default(),
        }))
    }

    /// 记录（或续写）一个修复任务。
    /// - 同一代次：保留队列，作为中断后的续修；
    /// - 不同代次（重新发起一次修复）：清空旧队列，从入口重新遍历，
    ///   否则会复用上一代残留的 `done` 状态而漏写本代结果。
    pub async fn begin_repair(&self, run_id: i64, generation: i64) -> Result<()> {
        let existing_gen: Option<i64> =
            sqlx::query("SELECT generation FROM repair_runs WHERE run_id = ?")
                .bind(run_id)
                .fetch_optional(&self.pool)
                .await?
                .and_then(|r| r.try_get::<i64, _>("generation").ok());
        if matches!(existing_gen, Some(g) if g != generation) {
            sqlx::query("DELETE FROM repair_pages WHERE run_id = ?")
                .bind(run_id)
                .execute(&self.pool)
                .await?;
        }
        let now = now_millis();
        sqlx::query(
            "INSERT INTO repair_runs (run_id, status, generation, error, started_at, updated_at) \
             VALUES (?, 'running', ?, NULL, ?, ?) \
             ON CONFLICT(run_id) DO UPDATE SET status='running', error=NULL, \
               generation=excluded.generation, updated_at=excluded.updated_at",
        )
        .bind(run_id)
        .bind(generation)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_repair_status(
        &self,
        run_id: i64,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query("UPDATE repair_runs SET status = ?, error = ?, updated_at = ? WHERE run_id = ?")
            .bind(status)
            .bind(error)
            .bind(now_millis())
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 列出尚未完成的修复任务（启动续修用）。
    pub async fn list_open_repairs(&self) -> Result<Vec<RepairRecord>> {
        let rows = sqlx::query(
            "SELECT run_id, status, generation, error, started_at, updated_at \
             FROM repair_runs WHERE status = 'running' ORDER BY run_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|r| {
                Ok(RepairRecord {
                    run_id: r.try_get("run_id")?,
                    status: r.try_get("status")?,
                    generation: r.try_get("generation")?,
                    error: r.try_get("error").ok().flatten(),
                    started_at: r.try_get("started_at")?,
                    updated_at: r.try_get("updated_at")?,
                })
            })
            .collect()
    }

    /// 重启时把修复任务残留的 fetching/failed 收回 pending。
    pub async fn reset_open_repair_pages(&self, run_id: i64) -> Result<()> {
        sqlx::query(
            "UPDATE repair_pages SET state = 'pending' WHERE run_id = ? AND state IN ('fetching','failed')",
        )
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 修复队列中登记入口页（已有则忽略）。
    pub async fn enqueue_repair_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
        depth: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO repair_pages (run_id, station_id, page_key, state, depth, attempts, last_error, updated_at) \
             VALUES (?, ?, ?, 'pending', ?, 0, NULL, ?) \
             ON CONFLICT(run_id, station_id, page_key) DO NOTHING",
        )
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .bind(depth)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn claim_repair_page(&self, run_id: i64) -> Result<Option<(String, String, i64, i64)>> {
        let row = sqlx::query(
            "UPDATE repair_pages SET state = 'fetching', updated_at = ? \
             WHERE id = (SELECT id FROM repair_pages WHERE run_id = ? AND state = 'pending' LIMIT 1) \
             RETURNING station_id, page_key, depth, attempts",
        )
        .bind(now_millis())
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            (
                r.try_get::<String, _>("station_id").unwrap_or_default(),
                r.try_get::<String, _>("page_key").unwrap_or_default(),
                r.try_get::<i64, _>("depth").unwrap_or_default(),
                r.try_get::<i64, _>("attempts").unwrap_or_default(),
            )
        }))
    }

    pub async fn complete_repair_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE repair_pages SET state = 'done', attempts = attempts + 1, last_error = NULL, updated_at = ? \
             WHERE run_id = ? AND station_id = ? AND page_key = ?",
        )
        .bind(now_millis())
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_repair_page(
        &self,
        run_id: i64,
        station_id: &str,
        page_key: &str,
        err: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE repair_pages SET state = 'failed', attempts = attempts + 1, last_error = ?, updated_at = ? \
             WHERE run_id = ? AND station_id = ? AND page_key = ?",
        )
        .bind(err)
        .bind(now_millis())
        .bind(run_id)
        .bind(station_id)
        .bind(page_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_repair_pages(&self, run_id: i64) -> Result<RunPageCounts> {
        let rows = sqlx::query(
            "SELECT state, COUNT(*) AS c FROM repair_pages WHERE run_id = ? GROUP BY state",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let mut c = RunPageCounts::default();
        for r in rows {
            let state: String = r.try_get("state")?;
            let n: i64 = r.try_get("c")?;
            match state.as_str() {
                "pending" => c.pending += n,
                "fetching" => c.fetching += n,
                "done" => c.done += n,
                "failed" => c.failed += n,
                _ => {}
            }
        }
        c.total = c.pending + c.fetching + c.done + c.failed;
        Ok(c)
    }

    /// 原子发布：只翻转一个指针，写入量与设施数无关。
    /// 仅当构建代次已完整就绪（调用方保证）时使用。
    pub async fn publish_generation(&self, run_id: i64, generation: i64) -> Result<()> {
        sqlx::query("UPDATE runs SET published_generation = ?, updated_at = ? WHERE id = ?")
            .bind(generation)
            .bind(now_millis())
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RunPageCounts {
    pub total: i64,
    pub done: i64,
    pub pending: i64,
    pub fetching: i64,
    pub failed: i64,
}

fn run_from_row(row: &sqlx::any::AnyRow) -> Result<Run> {
    let mode_str: String = row.try_get("mode")?;
    let status_str: String = row.try_get("status")?;
    let wm_str: String = row.try_get("watermarks")?;
    let value: Value = serde_json::from_str(&wm_str).unwrap_or(Value::Object(Default::default()));
    let mut watermarks = BTreeMap::new();
    if let Value::Object(map) = value {
        for (k, v) in map {
            if let Some(n) = v.as_i64() {
                watermarks.insert(k, n);
            }
        }
    }
    Ok(Run {
        id: row.try_get("id")?,
        entry_id: row.try_get("entry_id")?,
        mode: RunMode::parse(&mode_str)?,
        status: RunStatus::parse(&status_str),
        watermarks,
        error: row.try_get("error")?,
        published_generation: row.try_get("published_generation")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub fn map_err(e: sqlx::Error) -> anyhow::Error {
    anyhow!(e).context("database error")
}
