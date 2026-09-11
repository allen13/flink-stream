-- =============================================================================================================
-- Serving schema in YugabyteDB (YSQL).
--
-- YSQL is PostgreSQL-compatible at the wire and syntax level, but the storage underneath is a distributed,
-- tablet-sharded key-value store. The primary key clause is where that shows: YSQL extends it with per-column
-- sharding directives.
--
--   col HASH   hash-shard on this column. Rows spread uniformly across tablets; point lookups are fast, range
--              scans on the column are not possible.
--   col ASC    range-shard, sorted ascending. Range scans are fast; a monotonically increasing column (like a
--              timestamp) concentrates all new writes on one tablet - the distributed-database version of a
--              Kafka hot partition.
--
-- The pattern used below - hash on the entity, range on time - is the standard way to get both: writes spread
-- across tablets by entity, and "last hour for this entity" stays a single ordered scan.
-- =============================================================================================================

-- ------------------------------------------------------------------------------------------- DataStream sinks

CREATE TABLE IF NOT EXISTS enriched_transactions (
    txn_id            TEXT           NOT NULL,
    account_id        TEXT           NOT NULL,
    merchant_id       TEXT,
    merchant_name     TEXT,
    merchant_category TEXT,
    amount_usd        NUMERIC(12, 2) NOT NULL,
    currency          TEXT           NOT NULL,
    channel           TEXT           NOT NULL,
    auth_decision     TEXT,
    auth_latency_ms   INTEGER,
    risk_score        DOUBLE PRECISION,
    shard_id          INTEGER,
    subtask_index     INTEGER,
    event_time        TIMESTAMP      NOT NULL,
    processed_at      TIMESTAMP      NOT NULL,
    PRIMARY KEY (txn_id HASH)
);

-- Secondary index so "what did this account do recently" is a range scan rather than a full table scan.
CREATE INDEX IF NOT EXISTS enriched_transactions_by_account
    ON enriched_transactions ((account_id) HASH, event_time DESC);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id    TEXT           NOT NULL,
    account_id  TEXT           NOT NULL,
    rule        TEXT           NOT NULL,
    severity    TEXT           NOT NULL,
    description TEXT,
    txn_count   INTEGER,
    total_usd   NUMERIC(14, 2),
    detected_at TIMESTAMP      NOT NULL,
    event_time  TIMESTAMP      NOT NULL,
    PRIMARY KEY (alert_id HASH),
    -- The Avro schema carries severity as a plain string because Flink SQL cannot read Avro enums. The value
    -- set has to be enforced somewhere, so it is enforced here.
    CONSTRAINT fraud_alerts_severity_check CHECK (severity IN ('INFO', 'WARN', 'CRITICAL'))
);

CREATE INDEX IF NOT EXISTS fraud_alerts_by_account
    ON fraud_alerts ((account_id) HASH, detected_at DESC);

-- ------------------------------------------------------------------------------------------------- SQL sinks

CREATE TABLE IF NOT EXISTS region_revenue (
    region            TEXT           NOT NULL,
    window_start      TIMESTAMP      NOT NULL,
    window_end        TIMESTAMP      NOT NULL,
    txn_count         BIGINT,
    total_usd         NUMERIC(14, 2),
    avg_usd           NUMERIC(14, 4),
    distinct_accounts BIGINT,
    shards_touched    BIGINT,
    -- Hash on region, range on time: five regions spread over tablets, and each region's minutes stay in order.
    PRIMARY KEY (region HASH, window_start ASC)
);

CREATE TABLE IF NOT EXISTS merchant_leaderboard (
    window_start  TIMESTAMP      NOT NULL,
    rnk           BIGINT         NOT NULL,
    window_end    TIMESTAMP,
    merchant_id   TEXT,
    merchant_name TEXT,
    category      TEXT,
    total_usd     NUMERIC(14, 2),
    txn_count     BIGINT,
    PRIMARY KEY (window_start HASH, rnk ASC)
);

CREATE TABLE IF NOT EXISTS account_live_state (
    account_id      TEXT      NOT NULL,
    region          TEXT,
    risk_band       TEXT,
    txn_count       BIGINT,
    total_usd       NUMERIC(14, 2),
    max_usd         NUMERIC(12, 2),
    last_merchant   TEXT,
    last_event_time TIMESTAMP,
    PRIMARY KEY (account_id HASH)
);

-- --------------------------------------------------------------------------------- lookup-join source table
--
-- Read by Flink as a LOOKUP table (FOR SYSTEM_TIME AS OF proc_time). Unlike every other table here, Flink
-- queries this one per row instead of keeping it in state - so it must be indexed for point lookups, which the
-- hash primary key already gives.

CREATE TABLE IF NOT EXISTS merchant_risk (
    merchant_id     TEXT NOT NULL,
    chargeback_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    watchlist       BOOLEAN          NOT NULL DEFAULT FALSE,
    updated_at      TIMESTAMP        NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id HASH)
);

-- Seed it so the lookup join has something to find. Every 7th merchant is risky, every 13th is watchlisted.
INSERT INTO merchant_risk (merchant_id, chargeback_rate, watchlist)
SELECT
    'mch-' || lpad(i::TEXT, 4, '0'),
    CASE WHEN i % 7 = 0 THEN 0.035 + (i % 5) * 0.004 ELSE 0.002 END,
    (i % 13 = 0)
FROM generate_series(0, 199) AS i
ON CONFLICT (merchant_id) DO NOTHING;

-- ----------------------------------------------------------------------------------------- convenience views

CREATE OR REPLACE VIEW v_region_revenue_latest AS
SELECT DISTINCT ON (region) region, window_start, window_end, txn_count, total_usd, shards_touched
FROM region_revenue
ORDER BY region, window_start DESC;

CREATE OR REPLACE VIEW v_top_risk_accounts AS
SELECT account_id, region, risk_band, txn_count, total_usd, last_event_time
FROM account_live_state
ORDER BY total_usd DESC NULLS LAST
LIMIT 50;
