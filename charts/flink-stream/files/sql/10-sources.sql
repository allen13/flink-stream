-- =============================================================================================================
-- 10 - Source tables
--
-- Every table below reads Confluent-framed Avro. Note what is NOT here: no column types are duplicated from the
-- .avsc files by hand for validation - Schema Registry holds the writer schema, and Flink derives a reader
-- schema from this DDL. Avro's resolution rules then bridge the two, which is what makes it safe for a producer
-- to add a field without redeploying this job.
--
-- Three DDL features worth studying:
--   * METADATA columns expose Kafka's own record attributes (partition, offset, timestamp) as ordinary columns.
--     'VIRTUAL' keeps them out of any INSERT into this table.
--   * Computed columns (AS <expr>) are evaluated on read; they cost nothing to store and can be used in
--     WHERE, GROUP BY, and even as the watermark source.
--   * WATERMARK FOR ... turns a plain TIMESTAMP column into Flink's event-time attribute. Without it, every
--     window and temporal join below would be a planning error.
-- =============================================================================================================

-- ------------------------------------------------------------------------------------------------ facts

CREATE TEMPORARY TABLE transactions (
    txn_id        STRING,
    account_id    STRING,
    merchant_id   STRING,
    amount        DECIMAL(12, 2),
    currency      STRING,
    channel       STRING,
    device_id     STRING,
    geo           ROW<lat DOUBLE, lon DOUBLE>,
    tags          MAP<STRING, STRING>,
    shard_id      INT,
    event_time    TIMESTAMP(3),

    -- Kafka's own shard id, straight from the record. Comparing it with shard_id (which the producer computed)
    -- is how the docs show that Kafka's partitioner really is murmur2(key) % partitions.
    `partition`   INT                     METADATA VIRTUAL,
    `offset`      BIGINT                  METADATA VIRTUAL,
    ingest_time   TIMESTAMP_LTZ(3)        METADATA FROM 'timestamp' VIRTUAL,

    -- Computed columns: derived on read, never stored.
    amount_band   AS CASE WHEN amount <   25.00 THEN 'MICRO'
                          WHEN amount <  250.00 THEN 'SMALL'
                          WHEN amount < 2500.00 THEN 'MEDIUM'
                          ELSE                       'LARGE' END,
    proc_time     AS PROCTIME(),

    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'txn.transactions',
    'properties.bootstrap.servers'  = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'           = 'sql-labs-transactions',
    -- read_committed is what makes the exactly-once Kafka sinks of the DataStream job invisible until their
    -- checkpoint commits. Without it this query would see aborted transactions too.
    'properties.isolation.level'    = 'read_committed',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'avro-confluent',
    'avro-confluent.url'            = '${SCHEMA_REGISTRY_URL}'
);

CREATE TEMPORARY TABLE auth_events (
    auth_id     STRING,
    txn_id      STRING,
    account_id  STRING,
    decision    STRING,
    reason_code STRING,
    latency_ms  INT,
    event_time  TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'txn.auth-events',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'sql-labs-auth',
    'properties.isolation.level'   = 'read_committed',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = '${SCHEMA_REGISTRY_URL}'
);

-- ------------------------------------------------------------------------------------------------ dimensions
--
-- upsert-kafka, not kafka. The difference is the whole point:
--
--   kafka         interprets the topic as a stream of independent INSERTs (append-only).
--   upsert-kafka  interprets it as a CHANGELOG: each record replaces the previous one with the same key, and a
--                 null value is a DELETE. That requires a PRIMARY KEY, and it is what makes the table usable as
--                 the right side of a temporal join.
--
-- The key is read with the 'raw' format because the producer writes plain UTF-8 keys and registers no key
-- schema - the common Confluent convention for string keys.

CREATE TEMPORARY TABLE accounts (
    account_id   STRING,
    customer_id  STRING,
    region       STRING,
    risk_band    STRING,
    credit_limit DECIMAL(12, 2),
    status       STRING,
    opened_at    TIMESTAMP(3),
    updated_at   TIMESTAMP(3),
    WATERMARK FOR updated_at AS updated_at - INTERVAL '1' SECOND,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector'                      = 'upsert-kafka',
    'topic'                          = 'ref.accounts',
    'properties.bootstrap.servers'   = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'            = 'sql-labs-accounts',
    'key.format'                     = 'raw',
    'value.format'                   = 'avro-confluent',
    'value.avro-confluent.url'       = '${SCHEMA_REGISTRY_URL}',
    'value.fields-include'           = 'ALL'
);

CREATE TEMPORARY TABLE merchants (
    merchant_id STRING,
    name        STRING,
    category    STRING,
    country     STRING,
    mcc         INT,
    trust_score DOUBLE,
    updated_at  TIMESTAMP(3),
    WATERMARK FOR updated_at AS updated_at - INTERVAL '1' SECOND,
    PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ref.merchants',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'sql-labs-merchants',
    'key.format'                   = 'raw',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = '${SCHEMA_REGISTRY_URL}',
    'value.fields-include'         = 'ALL'
);

-- A *versioned* table: primary key + event-time watermark. FOR SYSTEM_TIME AS OF against this gives the rate
-- that was current when the transaction happened, not the rate that is current now - so a replay of history
-- produces the same numbers it produced live.
CREATE TEMPORARY TABLE fx_rates (
    currency    STRING,
    rate_to_usd DECIMAL(12, 6),
    event_time  TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND,
    PRIMARY KEY (currency) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'ref.fx-rates',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'sql-labs-fx',
    'key.format'                   = 'raw',
    'value.format'                 = 'avro-confluent',
    'value.avro-confluent.url'     = '${SCHEMA_REGISTRY_URL}',
    'value.fields-include'         = 'ALL'
);

-- ------------------------------------------------------------------------------------------------ UDFs
--
-- Registered here as well as in SqlRunnerJob so these files also work verbatim in the SQL client.

CREATE TEMPORARY FUNCTION IF NOT EXISTS mask_pan      AS 'io.flinkstream.udf.MaskPan';
CREATE TEMPORARY FUNCTION IF NOT EXISTS haversine_km  AS 'io.flinkstream.udf.Haversine';
CREATE TEMPORARY FUNCTION IF NOT EXISTS explode_tags  AS 'io.flinkstream.udf.ExplodeTags';
CREATE TEMPORARY FUNCTION IF NOT EXISTS weighted_avg  AS 'io.flinkstream.udf.WeightedAvg';
