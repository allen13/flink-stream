-- =============================================================================================================
-- 50 - MATCH_RECOGNIZE: complex event processing in SQL
--
-- This is the SQL form of DataStream lab G. Reading the two side by side is the fastest way to understand what
-- the clause compiles to - it is the same NFA, driven by the same watermarks, with the same bounded state.
--
-- Anatomy:
--   PARTITION BY   the key. Without it the pattern runs at parallelism 1 over the whole stream.
--   ORDER BY       must be the event-time attribute, ascending. This is not optional.
--   MEASURES       the output columns, computed from matched rows. FIRST/LAST/aggregates work here.
--   PATTERN        a regex over row classifiers: A B+ C, quantifiers {2,}, alternation, optional.
--   WITHIN         bounds how long a partial match may stay open - and therefore bounds state. Omit it and a
--                  half-finished pattern per account lives forever.
--   DEFINE         the predicate for each classifier. LAST(x, 1) is the previous row matched to that
--                  classifier, which is how "each one larger than the last" is written.
--   AFTER MATCH    where matching resumes. SKIP PAST LAST ROW gives non-overlapping matches;
--                  SKIP TO NEXT ROW would report every overlapping variant of the same burst.
-- =============================================================================================================

-- ------------------------------------------------------------------------------------------------------------
-- IMPORTANT: both patterns below are combined into ONE INSERT with UNION ALL, not written as two INSERTs into
-- the same table.
--
-- A Flink Kafka sink derives its transactional ids as <sink.transactional-id-prefix>-<subtask>-<checkpoint>.
-- Two sink operators writing to the same table therefore generate *identical* ids, and Kafka's transactional
-- protocol does exactly what it is designed to do:
--
--   ProducerFencedException: There is a newer producer with the same transactionalId which fences the current one
--
-- The job then restart-loops. Two INSERTs into one exactly-once Kafka table is always wrong; either UNION them
-- (here) or give each its own sink table with a distinct prefix.
-- ------------------------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------------------------
-- Pattern 1: card testing. A tiny card-not-present probe, then two or more escalating charges, within 10
-- minutes on the same account.
-- ------------------------------------------------------------------------------------------------------------

CREATE TEMPORARY VIEW card_testing_alerts AS
SELECT
    account_id,
    'CARD_TESTING_ESCALATION' AS pattern_name,
    first_txn_id,
    last_txn_id,
    hop_count,
    total_usd,
    span_seconds,
    detected_at
FROM transactions
MATCH_RECOGNIZE (
    PARTITION BY account_id
    ORDER BY event_time
    MEASURES
        FIRST(probe.txn_id)          AS first_txn_id,
        LAST(escalation.txn_id)      AS last_txn_id,
        COUNT(escalation.txn_id)     AS hop_count,
        CAST(SUM(escalation.amount) + FIRST(probe.amount) AS DECIMAL(14, 2)) AS total_usd,
        TIMESTAMPDIFF(SECOND, FIRST(probe.event_time), LAST(escalation.event_time)) AS span_seconds,
        LAST(escalation.event_time)  AS detected_at
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    -- escalation{2,}? is *reluctant* (note the trailing ?), and it has to be. Flink rejects a greedy
    -- quantifier as the last element of a pattern:
    --   "Greedy quantifiers are not allowed as the last element of a Pattern yet."
    -- The reason is that a greedy final quantifier can never decide it is done - there might always be one
    -- more matching row - so the match could only be emitted at end of stream. Reluctant means "emit as soon
    -- as the minimum is satisfied", which with AFTER MATCH SKIP PAST LAST ROW gives one alert per burst.
    PATTERN (probe escalation{2,}?) WITHIN INTERVAL '10' MINUTE
    DEFINE
        probe AS probe.channel = 'CARD_NOT_PRESENT'
             AND probe.amount <= 5.00,
        -- LAST(escalation.amount, 1) is NULL on the first escalation row, so fall back to comparing against the
        -- probe. Forgetting the COALESCE is the classic MATCH_RECOGNIZE bug: the predicate evaluates to NULL,
        -- never true, and the pattern silently never fires.
        escalation AS escalation.channel = 'CARD_NOT_PRESENT'
                  AND escalation.amount > COALESCE(LAST(escalation.amount, 1), LAST(probe.amount))
);

-- ------------------------------------------------------------------------------------------------------------
-- Pattern 2: impossible travel. Two card-present transactions far apart in space but close in time.
--
-- Shows a UDF used inside DEFINE - haversine_km() is the same Java class the DataStream job could call, and the
-- planner pushes it into the NFA predicate rather than evaluating it over the whole stream.
-- ------------------------------------------------------------------------------------------------------------

CREATE TEMPORARY VIEW impossible_travel_alerts AS
SELECT
    account_id,
    'IMPOSSIBLE_TRAVEL' AS pattern_name,
    first_txn_id,
    last_txn_id,
    CAST(2 AS BIGINT) AS hop_count,
    total_usd,
    span_seconds,
    detected_at
FROM (
    SELECT txn_id, account_id, amount, channel, event_time, geo.lat AS lat, geo.lon AS lon
    FROM transactions
    WHERE channel = 'CARD_PRESENT' AND geo IS NOT NULL
)
MATCH_RECOGNIZE (
    PARTITION BY account_id
    ORDER BY event_time
    MEASURES
        first_leg.txn_id  AS first_txn_id,
        second_leg.txn_id AS last_txn_id,
        CAST(first_leg.amount + second_leg.amount AS DECIMAL(14, 2)) AS total_usd,
        TIMESTAMPDIFF(SECOND, first_leg.event_time, second_leg.event_time) AS span_seconds,
        second_leg.event_time AS detected_at
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (first_leg second_leg) WITHIN INTERVAL '1' HOUR
    DEFINE
        first_leg AS TRUE,
        -- > 500 km apart in under an hour: no one drove that.
        second_leg AS haversine_km(
                          LAST(first_leg.lat), LAST(first_leg.lon),
                          second_leg.lat,      second_leg.lon) > 500.0
);

-- ------------------------------------------------------------------------------------------------------------
-- One sink, one transactional id prefix, both detectors.
-- ------------------------------------------------------------------------------------------------------------

INSERT INTO pattern_alerts_kafka
SELECT account_id, pattern_name, first_txn_id, last_txn_id, hop_count, total_usd, span_seconds, detected_at
FROM card_testing_alerts
UNION ALL
SELECT account_id, pattern_name, first_txn_id, last_txn_id, hop_count, total_usd, span_seconds, detected_at
FROM impossible_travel_alerts;
