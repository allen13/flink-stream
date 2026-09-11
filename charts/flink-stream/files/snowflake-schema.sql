-- =============================================================================================================
-- Warehouse schema in Snowflake, plus the identity the Flink job authenticates as.
--
-- Unlike charts/flink-stream/files/yugabyte-schema.sql, nothing applies this for you: Snowflake is not part of
-- the local cluster, so there is no pod to run it from. Paste it into a Snowsight worksheet as ACCOUNTADMIN
-- (or split it - the ACCOUNT-level grants at the top need that role; the tables at the bottom do not).
--
-- The interesting half is the first half. A warehouse charges for compute and holds everyone's data, so "which
-- credential, holding which role, reachable from where" is a design decision here in a way it is not for a
-- single-node YugabyteDB in the next pod over.
-- =============================================================================================================

-- ------------------------------------------------------------------------------------ 1. compute and storage

-- XSMALL, and it auto-suspends after a minute. A streaming sink writes on every checkpoint, so the warehouse is
-- resumed more or less continuously while the job runs - the size is what decides the bill, not the uptime.
CREATE WAREHOUSE IF NOT EXISTS FLINK_STREAM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Ingest warehouse for the flink-stream DataStream job';

CREATE DATABASE IF NOT EXISTS FLINK_STREAM;
CREATE SCHEMA IF NOT EXISTS FLINK_STREAM.STREAMING;

-- --------------------------------------------------------------------------------------------- 2. the role
--
-- One role, with exactly the privileges the sink needs: use the warehouse, see the schema, write the two
-- tables. No CREATE TABLE, no access to any other database. This is the role the token will be pinned to, so
-- the blast radius of a leaked token is the blast radius of this grant list.

CREATE ROLE IF NOT EXISTS FLINK_STREAM_WRITER;

GRANT USAGE ON WAREHOUSE FLINK_STREAM_WH        TO ROLE FLINK_STREAM_WRITER;
GRANT USAGE ON DATABASE FLINK_STREAM            TO ROLE FLINK_STREAM_WRITER;
GRANT USAGE ON SCHEMA FLINK_STREAM.STREAMING    TO ROLE FLINK_STREAM_WRITER;

-- ------------------------------------------------------------------------------------- 3. the service user
--
-- TYPE = SERVICE is not cosmetic: a service user cannot have a password, cannot sign in through the UI, and is
-- excluded from MFA enrolment. It can only authenticate programmatically - which is exactly what a Flink
-- TaskManager is - and Snowflake enforces that rather than trusting a naming convention.

CREATE USER IF NOT EXISTS FLINK_STREAM_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = FLINK_STREAM_WRITER
    DEFAULT_WAREHOUSE = FLINK_STREAM_WH
    DEFAULT_NAMESPACE = FLINK_STREAM.STREAMING
    COMMENT = 'Flink DataStream job: writes enriched transactions and fraud alerts';

GRANT ROLE FLINK_STREAM_WRITER TO USER FLINK_STREAM_SVC;

-- ------------------------------------------------------------------------ 4. the policies that permit a PAT
--
-- Two gates, and a token works only if both are open.
--
-- The authentication policy says which methods this user may use at all. Listing PROGRAMMATIC_ACCESS_TOKEN and
-- nothing else means a token is the only way in: no password fallback to forget about, and no quiet downgrade
-- if someone later sets one.

CREATE AUTHENTICATION POLICY IF NOT EXISTS FLINK_STREAM.STREAMING.PAT_ONLY
    AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN')
    COMMENT = 'Service users that authenticate with a programmatic access token and nothing else';

ALTER USER FLINK_STREAM_SVC SET AUTHENTICATION POLICY FLINK_STREAM.STREAMING.PAT_ONLY;

-- The network policy says where from. Snowflake *requires* one on a TYPE = SERVICE user before it will accept a
-- token: a bearer credential with no network constraint is usable by anyone who obtains it, from anywhere, and
-- Snowflake declines to let you build that by accident.
--
-- Replace the range below with the egress address your cluster actually leaves from - the NAT gateway or egress
-- IP in front of the nodes, not the pod CIDR, which Snowflake never sees.
CREATE NETWORK POLICY IF NOT EXISTS FLINK_STREAM_EGRESS
    ALLOWED_IP_LIST = ('203.0.113.0/24')
    COMMENT = 'Egress addresses of the Kubernetes cluster running the Flink jobs';

ALTER USER FLINK_STREAM_SVC SET NETWORK_POLICY = FLINK_STREAM_EGRESS;

-- ------------------------------------------------------------------------------------------ 5. the token
--
-- Run this and copy the `token_secret` column out of the result. It is displayed once and never again -
-- Snowflake stores only a hash, so a lost token is regenerated, not recovered.
--
--   kubectl -n flink-stream create secret generic snowflake-pat --from-literal=pat='<token_secret>'
--   helm upgrade ... --set snowflake.enabled=true --set snowflake.pat.existingSecret=snowflake-pat
--
-- ROLE_RESTRICTION is what makes this a scoped credential rather than a password by another name: the token can
-- assume FLINK_STREAM_WRITER and no other role the user may hold, now or later. It has to match
-- `snowflake.role` in values.yaml, because the JDBC URL names the role explicitly.
--
-- DAYS_TO_EXPIRY is the other half. A PAT expires on its own schedule; 90 days here means rotation is a routine
-- event with a deadline rather than something that happens after an incident.

ALTER USER IF EXISTS FLINK_STREAM_SVC
    ADD PROGRAMMATIC ACCESS TOKEN FLINK_STREAM_JOB
    ROLE_RESTRICTION = 'FLINK_STREAM_WRITER'
    DAYS_TO_EXPIRY = 90
    COMMENT = 'flink-stream DataStream job';

-- Rotation, later, without downtime: add a second token, patch the Secret, restart the job, then remove the
-- first. Two live tokens is the point - a single-token rotation is an outage.
--
--   ALTER USER IF EXISTS FLINK_STREAM_SVC ADD PROGRAMMATIC ACCESS TOKEN FLINK_STREAM_JOB_NEXT
--       ROLE_RESTRICTION = 'FLINK_STREAM_WRITER' DAYS_TO_EXPIRY = 90;
--   ALTER USER IF EXISTS FLINK_STREAM_SVC REMOVE PROGRAMMATIC ACCESS TOKEN FLINK_STREAM_JOB;
--
-- What is live, and when it expires:
--   SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER FLINK_STREAM_SVC;

-- ------------------------------------------------------------------------------------------- 6. the tables

USE DATABASE FLINK_STREAM;
USE SCHEMA STREAMING;

-- Append-only, and deliberately so. The sink is at-least-once, so a replayed batch after a restart writes a
-- txn_id twice; v_enriched_transactions below removes the duplicate at read time.
--
-- No primary key, and no unique constraint: Snowflake accepts both as documentation but enforces neither, and a
-- constraint that does nothing is worse than no constraint. Deduplication happens where it is real - in the
-- view below.
--
-- CLUSTER BY is the one thing here that changes how queries run: it keeps each day's rows in the same
-- micro-partitions, so a date-filtered scan prunes the rest. It is not free - Snowflake's Automatic Clustering
-- service reorganises the table in the background and bills for it, and a streaming append is exactly the write
-- pattern that keeps it busy. Drop the clause if the table is small or the bill matters more than the scan.
CREATE TABLE IF NOT EXISTS enriched_transactions (
    txn_id            VARCHAR        NOT NULL,
    account_id        VARCHAR        NOT NULL,
    merchant_id       VARCHAR,
    merchant_name     VARCHAR,
    merchant_category VARCHAR,
    amount_usd        NUMBER(12, 2)  NOT NULL,
    currency          VARCHAR        NOT NULL,
    channel           VARCHAR        NOT NULL,
    auth_decision     VARCHAR,
    auth_latency_ms   NUMBER(10, 0),
    risk_score        FLOAT,
    shard_id          NUMBER(10, 0),
    subtask_index     NUMBER(10, 0),
    event_time        TIMESTAMP_NTZ  NOT NULL,
    processed_at      TIMESTAMP_NTZ  NOT NULL
)
CLUSTER BY (TO_DATE(event_time), account_id)
COMMENT = 'Enriched transaction history, appended by the Flink DataStream job (at-least-once)';

-- The dedup that the INSERT does not do. QUALIFY filters on a window function without a subquery: rank each
-- txn_id's copies by how recently the job wrote them, keep the newest. Reading this view instead of the table
-- is what makes "at-least-once into an append-only table" correct rather than approximately correct.
CREATE OR REPLACE VIEW v_enriched_transactions AS
SELECT *
FROM enriched_transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY processed_at DESC) = 1;

-- Alerts are low volume, so this one is deduplicated on write instead: the sink issues a MERGE keyed on
-- alert_id, Snowflake's equivalent of ON CONFLICT DO NOTHING. Affordable here precisely because the stream is
-- small - the same MERGE against enriched_transactions would rewrite micro-partitions on every checkpoint.
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id    VARCHAR        NOT NULL,
    account_id  VARCHAR        NOT NULL,
    rule        VARCHAR        NOT NULL,
    severity    VARCHAR        NOT NULL,
    description VARCHAR,
    txn_count   NUMBER(10, 0),
    total_usd   NUMBER(14, 2),
    detected_at TIMESTAMP_NTZ  NOT NULL,
    event_time  TIMESTAMP_NTZ  NOT NULL
)
CLUSTER BY (TO_DATE(detected_at))
COMMENT = 'Fraud alerts, deduplicated on write by MERGE on alert_id';

GRANT SELECT, INSERT, UPDATE ON TABLE enriched_transactions TO ROLE FLINK_STREAM_WRITER;
GRANT SELECT, INSERT, UPDATE ON TABLE fraud_alerts          TO ROLE FLINK_STREAM_WRITER;

-- ------------------------------------------------------------------------------------- 7. is it working?
--
-- Run these as a role that can read the schema (ACCOUNTADMIN will do) once the job has been up for a checkpoint
-- interval or two.

-- Rows arriving, and how much duplication the at-least-once sink is actually producing.
--   SELECT COUNT(*) AS rows_written,
--          COUNT(DISTINCT txn_id) AS distinct_txns,
--          MAX(processed_at) AS newest
--   FROM enriched_transactions;

-- What the job is doing to the warehouse. Each row is one batch: watch ROWS_INSERTED against the sink's batch
-- size, and EXECUTION_TIME to see what the writes are costing.
--   SELECT START_TIME, QUERY_TYPE, ROWS_INSERTED, EXECUTION_TIME, QUERY_TEXT
--   FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER('FLINK_STREAM_SVC', RESULT_LIMIT => 50))
--   ORDER BY START_TIME DESC;

-- Which credential each connection used. AUTHENTICATION_METHOD should read PROGRAMMATIC_ACCESS_TOKEN; a failed
-- connection lands here too, with ERROR_MESSAGE - the first place to look when the job cannot log in.
--   SELECT EVENT_TIMESTAMP, USER_NAME, AUTHENTICATION_METHOD, IS_SUCCESS, ERROR_MESSAGE, CLIENT_IP
--   FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
--   WHERE USER_NAME = 'FLINK_STREAM_SVC'
--   ORDER BY EVENT_TIMESTAMP DESC
--   LIMIT 20;
