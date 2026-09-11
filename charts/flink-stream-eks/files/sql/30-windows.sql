-- =============================================================================================================
-- 30 - Windowed aggregation across every shard, written to three sinks at once
--
-- This file is the SQL counterpart of DataStream labs D/E/I, and the centrepiece of the dual-sink story.
--
-- WHY THIS IS A CROSS-SHARD AGGREGATION
--   txn.transactions has 12 shards, keyed by account_id. 'region' is not in the transaction at all - it comes
--   from the account dimension. So GROUP BY region forces a full redistribution: every one of the 12 shards
--   sends rows to whichever subtask owns that region's key group. Look for the "Hash" exchange in EXPLAIN, and
--   for shards_touched = 12 in the output - that column exists purely to prove the shuffle happened.
--
-- WHY ONE VIEW AND THREE INSERTS
--   SqlRunnerJob collects every INSERT in this directory into one StatementSet. The planner then sees that all
--   three read the same view, computes the join and the window ONCE, and fans the result out to three sinks.
--   Submitted as three separate jobs instead, you would pay for three Kafka consumer groups, three copies of
--   the join state and three windows - for identical results.
-- =============================================================================================================

-- Mini-batch turns per-record state access into per-batch state access. On a window aggregate with high key
-- cardinality this is often a 2-5x throughput difference, bought with up to 'allow-latency' of extra delay.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2 s';
SET 'table.exec.mini-batch.size' = '2000';

-- Split the aggregate into a local pre-aggregate (before the shuffle) and a global one (after it). With only
-- five region keys, a single-phase aggregate would send every row to five subtasks and idle the rest.
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
SET 'table.exec.source.idle-timeout' = '15 s';

-- ------------------------------------------------------------------------------------------------------------
-- The shared computation. A temporary view is not materialised - it is a named query fragment the planner
-- inlines into every statement that references it.
-- ------------------------------------------------------------------------------------------------------------

CREATE TEMPORARY VIEW txn_with_account AS
SELECT
    t.txn_id,
    t.account_id,
    t.merchant_id,
    t.amount,
    t.currency,
    t.channel,
    t.shard_id,
    t.event_time,
    t.proc_time,
    -- FOR SYSTEM_TIME AS OF t.event_time is an event-time temporal join: it looks up the version of the account
    -- that was current at the transaction's own timestamp. A plain JOIN would use whatever version happens to be
    -- in state when the row is processed, which makes replays non-deterministic.
    COALESCE(a.region, 'UNKNOWN')    AS region,
    COALESCE(a.risk_band, 'UNKNOWN') AS risk_band
FROM transactions AS t
LEFT JOIN accounts FOR SYSTEM_TIME AS OF t.event_time AS a
       ON t.account_id = a.account_id;

CREATE TEMPORARY VIEW region_revenue_1m AS
SELECT
    region,
    window_start,
    window_end,
    COUNT(*)                                        AS txn_count,
    CAST(SUM(amount)   AS DECIMAL(14, 2))           AS total_usd,
    CAST(AVG(amount)   AS DECIMAL(14, 4))           AS avg_usd,
    COUNT(DISTINCT account_id)                      AS distinct_accounts,
    COUNT(DISTINCT shard_id)                        AS shards_touched
FROM TABLE(
    -- Window TVFs (TUMBLE / HOP / CUMULATE) replaced the old GROUP BY TUMBLE(...) grouped-window syntax. They
    -- are strictly more capable: window_start/window_end become real columns, so Window Top-N and window joins
    -- become expressible, and the result is append-only rather than retracting.
    TUMBLE(TABLE txn_with_account, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY region, window_start, window_end;

-- ------------------------------------------------------------------------------------------------------------
-- Sink 1 of 3: Kafka. Append-only, exactly-once, replayable by any consumer.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO region_revenue_kafka
SELECT region, window_start, window_end, txn_count, total_usd, avg_usd, distinct_accounts, shards_touched
FROM region_revenue_1m;

-- ------------------------------------------------------------------------------------------------------------
-- Sink 2 of 3: Iceberg. Same rows, plus the partition column, landed as Parquet in object storage.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO iceberg.lakehouse.region_revenue_1m
SELECT
    region, window_start, window_end, txn_count, total_usd, avg_usd, distinct_accounts, shards_touched,
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS dt
FROM region_revenue_1m;

-- ------------------------------------------------------------------------------------------------------------
-- Sink 3 of 3: YugabyteDB. Same rows, upserted, so a dashboard can point-query them.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO region_revenue_ysql
SELECT region, window_start, window_end, txn_count, total_usd, avg_usd, distinct_accounts, shards_touched
FROM region_revenue_1m;

-- ------------------------------------------------------------------------------------------------------------
-- Window Top-N: the merchant leaderboard per minute.
--
-- The ROW_NUMBER() ... PARTITION BY window_start, window_end shape is what the planner recognises as a *window*
-- Top-N, which it can emit once per window and then forget. Partitioning by anything other than the window
-- columns gives an ordinary Top-N instead: unbounded state and a retraction every time the ranking changes.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO merchant_leaderboard_ysql
SELECT window_start, window_end, rnk, merchant_id, merchant_name, category, total_usd, txn_count
FROM (
    SELECT
        window_start,
        window_end,
        merchant_id,
        merchant_name,
        category,
        total_usd,
        txn_count,
        ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total_usd DESC) AS rnk
    FROM (
        SELECT
            w.window_start,
            w.window_end,
            w.merchant_id,
            MAX(m.name)     AS merchant_name,
            MAX(m.category) AS category,
            CAST(SUM(w.amount) AS DECIMAL(14, 2)) AS total_usd,
            COUNT(*)        AS txn_count
        FROM TABLE(
            TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        ) AS w
        -- A second cross-shard join: transactions are sharded by account_id, merchants by merchant_id.
        LEFT JOIN merchants FOR SYSTEM_TIME AS OF w.event_time AS m
               ON w.merchant_id = m.merchant_id
        GROUP BY w.window_start, w.window_end, w.merchant_id
    )
)
WHERE rnk <= 10;

-- ------------------------------------------------------------------------------------------------------------
-- A regular (non-windowed) GROUP BY: running totals per account, never finished, always updating.
--
-- This is the *updating* changelog mode, and the reason it can only go to YugabyteDB and not to Iceberg. It is
-- also the query to watch state on: without table.exec.state.ttl it keeps a row per account forever.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO account_live_state_ysql
SELECT
    account_id,
    MAX(region)    AS region,
    MAX(risk_band) AS risk_band,
    COUNT(*)       AS txn_count,
    CAST(SUM(amount) AS DECIMAL(14, 2)) AS total_usd,
    MAX(amount)    AS max_usd,
    -- LAST_VALUE is only meaningful because mini-batch preserves arrival order within a key.
    LAST_VALUE(merchant_id) AS last_merchant,
    MAX(event_time)         AS last_event_time
FROM txn_with_account
GROUP BY account_id;
