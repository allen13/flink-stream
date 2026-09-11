-- =============================================================================================================
-- 40 - Every join Flink SQL can do, and when each one is the right answer
--
--   Regular join        JOIN b ON ...                               both sides kept in state forever (bound it
--                                                                   with table.exec.state.ttl)
--   Interval join       ... AND b.ts BETWEEN a.ts - x AND a.ts + y  state bounded by the interval
--   Temporal join       JOIN b FOR SYSTEM_TIME AS OF a.event_time   versioned dimension, replay-deterministic
--   Lookup join         JOIN b FOR SYSTEM_TIME AS OF a.proc_time    external query per row, no Flink state
--   Window join         two window TVFs joined on window_start/end  bounded, aligned to window boundaries
--
-- The one that bites people is the regular join: it is the only one whose state is unbounded, and it is also
-- the one you get by writing the join you would write in a database.
-- =============================================================================================================

SET 'table.exec.state.ttl' = '1 h';

-- ------------------------------------------------------------------------------------------------------------
-- 1. Event-time temporal join against a versioned table: convert every amount to USD at the rate that was in
--    force when the transaction happened.
--
--    Change FOR SYSTEM_TIME AS OF t.event_time to a plain JOIN and the numbers stop being reproducible: a
--    replay would price yesterday's transactions at today's rates.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO fx_normalized_kafka
SELECT
    t.txn_id,
    t.account_id,
    t.currency,
    t.amount,
    COALESCE(r.rate_to_usd, CAST(1.0 AS DECIMAL(12, 6)))                 AS rate_to_usd,
    CAST(t.amount * COALESCE(r.rate_to_usd, CAST(1.0 AS DECIMAL(12, 6)))
         AS DECIMAL(18, 6))                                              AS amount_usd,
    r.event_time                                                         AS rate_as_of,
    t.event_time
FROM transactions AS t
LEFT JOIN fx_rates FOR SYSTEM_TIME AS OF t.event_time AS r
       ON t.currency = r.currency;

-- ------------------------------------------------------------------------------------------------------------
-- 2. Interval join + lookup join + temporal join in one statement, landing in Iceberg.
--
--    * transactions x auth_events is an INTERVAL join. Both topics are sharded by account_id but the join key
--      is txn_id, so this is a cross-shard shuffle. The BETWEEN clause is what makes the state bounded: Flink
--      can drop a buffered row once the watermark proves no partner can still arrive.
--    * merchants is a TEMPORAL join (dimension held in Flink state, versioned).
--    * merchant_risk_lookup is a LOOKUP join (dimension queried in YugabyteDB per row, cached).
--
--    Running the last two side by side is the point: same logical enrichment, completely different cost model.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO iceberg.lakehouse.enriched_transactions
SELECT
    t.txn_id,
    t.account_id,
    t.merchant_id,
    m.name                           AS merchant_name,
    m.category                       AS merchant_category,
    COALESCE(a.region, 'UNKNOWN')    AS region,
    COALESCE(a.risk_band, 'UNKNOWN') AS risk_band,
    t.channel,
    t.amount,
    t.currency,
    au.decision                      AS auth_decision,
    t.shard_id,
    t.event_time,
    DATE_FORMAT(t.event_time, 'yyyy-MM-dd') AS dt
FROM transactions AS t
-- interval join: the auth decision lands 0-45s after the transaction
JOIN auth_events AS au
  ON  t.txn_id = au.txn_id
  AND au.event_time BETWEEN t.event_time - INTERVAL '2' SECOND
                        AND t.event_time + INTERVAL '45' SECOND
-- temporal joins: dimensions versioned in Flink state
LEFT JOIN merchants FOR SYSTEM_TIME AS OF t.event_time AS m
       ON t.merchant_id = m.merchant_id
LEFT JOIN accounts  FOR SYSTEM_TIME AS OF t.event_time AS a
       ON t.account_id = a.account_id;

-- ------------------------------------------------------------------------------------------------------------
-- 3. Lookup join against YugabyteDB.
--
--    FOR SYSTEM_TIME AS OF t.proc_time - processing time, not event time - is what makes this a lookup rather
--    than a temporal join. Flink issues "SELECT ... WHERE merchant_id = ?" per uncached row.
--
--    Because it reads whatever the database holds right now, a replay does NOT reproduce the original result.
--    That is the trade for zero join state, and it is the right trade for reference data that is authoritative
--    elsewhere and rarely changes.
-- ------------------------------------------------------------------------------------------------------------

CREATE TEMPORARY VIEW risky_transactions AS
SELECT
    t.txn_id,
    t.account_id,
    t.merchant_id,
    t.amount,
    t.channel,
    t.event_time,
    r.chargeback_rate,
    r.watchlist
FROM transactions AS t
LEFT JOIN merchant_risk_lookup FOR SYSTEM_TIME AS OF t.proc_time AS r
       ON t.merchant_id = r.merchant_id
WHERE COALESCE(r.watchlist, FALSE) OR COALESCE(r.chargeback_rate, 0.0) > 0.02;

-- ------------------------------------------------------------------------------------------------------------
-- 4. Window join: two window TVFs joined on their window boundaries.
--
--    Both sides are bucketed into the same 1-minute windows first, then joined on (window_start, window_end).
--    Unlike an interval join this can never produce a match that straddles a boundary, which makes it the right
--    tool when the comparison is inherently per-window ("did approvals drop in the same minute spend spiked?").
-- ------------------------------------------------------------------------------------------------------------

CREATE TEMPORARY VIEW approval_rate_1m AS
SELECT
    txns.window_start,
    txns.window_end,
    txns.account_id,
    txns.txn_count,
    COALESCE(auths.approved, 0) AS approved,
    COALESCE(auths.declined, 0) AS declined
FROM (
    SELECT window_start, window_end, account_id, COUNT(*) AS txn_count
    FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, window_end, account_id
) AS txns
LEFT JOIN (
    SELECT
        window_start, window_end, account_id,
        COUNT(*) FILTER (WHERE decision = 'APPROVED') AS approved,
        COUNT(*) FILTER (WHERE decision = 'DECLINED') AS declined
    FROM TABLE(TUMBLE(TABLE auth_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY window_start, window_end, account_id
) AS auths
  ON  txns.account_id   = auths.account_id
  AND txns.window_start = auths.window_start
  AND txns.window_end   = auths.window_end;
