-- 跨进程在途抓取租约（schema v2 迁移）。与 schema.sql 中的定义保持一致：
-- __PK_AUTO__ 占位符在初始化时按方言替换。
CREATE TABLE IF NOT EXISTS fetch_leases (
    id              __PK_AUTO__,
    station_id      TEXT NOT NULL,
    page_key        TEXT NOT NULL,
    target_kind     TEXT NOT NULL,
    target_value    INTEGER NOT NULL,
    owner_id        TEXT NOT NULL,
    state           TEXT NOT NULL,
    actual_revision INTEGER,
    expires_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    UNIQUE (station_id, page_key, target_kind, target_value)
)
