-- =============================================================================================================
-- 20 - Sink tables: Kafka, Apache Iceberg and YugabyteDB
--
-- The project writes each result to two places at once, because streams and tables answer different questions:
--
--   Kafka / Iceberg   the immutable log and the lakehouse table. Append-only, replayable, cheap to keep,
--                     readable by other engines. This is the history.
--   YugabyteDB        the serving store. Upserted in place, indexed, joinable, queryable with a point lookup.
--                     This is the current state.
--
-- The important constraint is which sink can accept which *changelog mode*:
--
--   append-only  every row is final. Window TVF aggregates, MATCH_RECOGNIZE, interval joins.
--   updating     rows are retracted and re-emitted. Regular GROUP BY, Top-N, regular joins.
--
-- Iceberg and plain 'kafka' accept append-only only. A JDBC table with a PRIMARY KEY, and upsert-kafka, accept
-- updating streams because they can overwrite a row. Trying to sink an updating stream into an append-only sink
-- fails at *plan* time with "does not support consuming update changes" - a good error to have seen once.
-- =============================================================================================================

-- ------------------------------------------------------------------------------------------- Kafka (append)

CREATE TEMPORARY TABLE region_revenue_kafka (
    region        STRING,
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    txn_count     BIGINT,
    total_usd     DECIMAL(14, 2),
    avg_usd       DECIMAL(14, 4),
    distinct_accounts BIGINT,
    shards_touched    BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'sql.region-revenue',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}',
    -- Transactional producer, so downstream read_committed consumers see whole checkpoints or nothing.
    'sink.delivery-guarantee'          = 'exactly-once',
    'sink.transactional-id-prefix'     = 'sql-region-revenue',
    'properties.transaction.timeout.ms'= '900000'
);

CREATE TEMPORARY TABLE pattern_alerts_kafka (
    account_id    STRING,
    pattern_name  STRING,
    first_txn_id  STRING,
    last_txn_id   STRING,
    hop_count     BIGINT,
    total_usd     DECIMAL(14, 2),
    span_seconds  BIGINT,
    detected_at   TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'sql.pattern-alerts',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}',
    'sink.delivery-guarantee'          = 'exactly-once',
    'sink.transactional-id-prefix'     = 'sql-pattern-alerts',
    'properties.transaction.timeout.ms'= '900000'
);

CREATE TEMPORARY TABLE fx_normalized_kafka (
    txn_id       STRING,
    account_id   STRING,
    currency     STRING,
    amount       DECIMAL(12, 2),
    rate_to_usd  DECIMAL(12, 6),
    amount_usd   DECIMAL(18, 6),
    rate_as_of   TIMESTAMP(3),
    event_time   TIMESTAMP(3)
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'sql.fx-normalized',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}',
    'sink.delivery-guarantee'          = 'exactly-once',
    'sink.transactional-id-prefix'     = 'sql-fx-normalized',
    'properties.transaction.timeout.ms'= '900000'
);

-- ------------------------------------------------------------------------------ Iceberg (append, lakehouse)
--
-- PARTITIONED BY (dt) uses an *identity* partition on a column the query materialises. Iceberg also supports
-- hidden partition transforms (days(ts), bucket(16, id)) but Flink DDL cannot express them - create those
-- through Spark or the REST API if you need them.
--
-- Every checkpoint produces a new Iceberg snapshot and a set of small files. At a 30s checkpoint interval that
-- is 2,880 snapshots a day, so a real deployment needs periodic compaction and snapshot expiry; see
-- docs/08-operations.md.

CREATE TABLE IF NOT EXISTS iceberg.lakehouse.region_revenue_1m (
    region            STRING,
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    txn_count         BIGINT,
    total_usd         DECIMAL(14, 2),
    avg_usd           DECIMAL(14, 4),
    distinct_accounts BIGINT,
    shards_touched    BIGINT,
    dt                STRING
) PARTITIONED BY (dt) WITH (
    'format-version'            = '2',
    'write.format.default'      = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '67108864'
);

CREATE TABLE IF NOT EXISTS iceberg.lakehouse.enriched_transactions (
    txn_id            STRING,
    account_id        STRING,
    merchant_id       STRING,
    merchant_name     STRING,
    merchant_category STRING,
    region            STRING,
    risk_band         STRING,
    channel           STRING,
    amount            DECIMAL(12, 2),
    currency          STRING,
    auth_decision     STRING,
    shard_id          INT,
    event_time        TIMESTAMP(3),
    dt                STRING
) PARTITIONED BY (dt) WITH (
    'format-version'       = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

-- --------------------------------------------------------------------------- YugabyteDB (upsert, serving)
--
-- YSQL speaks the PostgreSQL wire protocol, so 'connector' = 'jdbc' with the Postgres dialect works unchanged.
-- What makes these tables *upsert* sinks rather than append sinks is the PRIMARY KEY ... NOT ENFORCED clause:
-- with it, Flink emits INSERT ... ON CONFLICT DO UPDATE and can therefore accept a retracting stream.
--
-- "NOT ENFORCED" means Flink trusts the declaration instead of checking it - Flink never validates uniqueness,
-- the database does.

CREATE TEMPORARY TABLE region_revenue_ysql (
    region            STRING,
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    txn_count         BIGINT,
    total_usd         DECIMAL(14, 2),
    avg_usd           DECIMAL(14, 4),
    distinct_accounts BIGINT,
    shards_touched    BIGINT,
    PRIMARY KEY (region, window_start) NOT ENFORCED
) WITH (
    'connector'                   = 'jdbc',
    'url'                         = '${YUGABYTE_JDBC_URL:-jdbc:postgresql://yugabyte-ysql:5433/flink_stream}',
    'table-name'                  = 'region_revenue',
    'username'                    = '${YUGABYTE_USER:-yugabyte}',
    'password'                    = '${YUGABYTE_PASSWORD:-yugabyte}',
    'sink.buffer-flush.max-rows'  = '500',
    'sink.buffer-flush.interval'  = '1s',
    'sink.max-retries'            = '3'
);

CREATE TEMPORARY TABLE merchant_leaderboard_ysql (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    rnk           BIGINT,
    merchant_id   STRING,
    merchant_name STRING,
    category      STRING,
    total_usd     DECIMAL(14, 2),
    txn_count     BIGINT,
    PRIMARY KEY (window_start, rnk) NOT ENFORCED
) WITH (
    'connector'                   = 'jdbc',
    'url'                         = '${YUGABYTE_JDBC_URL:-jdbc:postgresql://yugabyte-ysql:5433/flink_stream}',
    'table-name'                  = 'merchant_leaderboard',
    'username'                    = '${YUGABYTE_USER:-yugabyte}',
    'password'                    = '${YUGABYTE_PASSWORD:-yugabyte}',
    'sink.buffer-flush.max-rows'  = '200',
    'sink.buffer-flush.interval'  = '1s'
);

CREATE TEMPORARY TABLE account_live_state_ysql (
    account_id      STRING,
    region          STRING,
    risk_band       STRING,
    txn_count       BIGINT,
    total_usd       DECIMAL(14, 2),
    max_usd         DECIMAL(12, 2),
    last_merchant   STRING,
    last_event_time TIMESTAMP(3),
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector'                   = 'jdbc',
    'url'                         = '${YUGABYTE_JDBC_URL:-jdbc:postgresql://yugabyte-ysql:5433/flink_stream}',
    'table-name'                  = 'account_live_state',
    'username'                    = '${YUGABYTE_USER:-yugabyte}',
    'password'                    = '${YUGABYTE_PASSWORD:-yugabyte}',
    'sink.buffer-flush.max-rows'  = '500',
    'sink.buffer-flush.interval'  = '1s'
);

-- ------------------------------------------------------------------ YugabyteDB as a *lookup source*
--
-- The same connector reads as well as writes. Used with FOR SYSTEM_TIME AS OF <proctime> it becomes a lookup
-- join: Flink queries the database per row (with a cache in front) instead of keeping the dimension in state.
--
-- Trade-off against the broadcast/upsert-kafka approach used elsewhere: no Flink state at all and always the
-- freshest value, paid for with a network round trip per cache miss and a hard dependency on the database
-- being up. The cache TTL is the knob between the two extremes.

CREATE TEMPORARY TABLE merchant_risk_lookup (
    merchant_id   STRING,
    chargeback_rate DOUBLE,
    watchlist     BOOLEAN,
    PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
    'connector'                     = 'jdbc',
    'url'                           = '${YUGABYTE_JDBC_URL:-jdbc:postgresql://yugabyte-ysql:5433/flink_stream}',
    'table-name'                    = 'merchant_risk',
    'username'                      = '${YUGABYTE_USER:-yugabyte}',
    'password'                      = '${YUGABYTE_PASSWORD:-yugabyte}',
    'lookup.cache'                  = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '5000',
    'lookup.partial-cache.expire-after-write' = '5min',
    'lookup.max-retries'            = '3'
);
