# 07 — Dual sinks: Apache Iceberg, YugabyteDB and Snowflake

Several queries in this project write their result to **two or three places at once**. That is not redundancy —
streams and tables answer different questions, and no single store answers both well.

| | Kafka | Apache Iceberg | YugabyteDB | Snowflake |
|---|---|---|---|---|
| Shape | append-only log | append-only table on object storage | mutable rows | append-only micro-partitions |
| Answers | "what happened, in order" | "what happened, over months" | "what is true **now**" | "what happened, ask it anything" |
| Reader | another stream job | Spark, Trino, DuckDB, Flink batch | an application, a dashboard | an analyst, a BI tool |
| Cost per row | cheap, bounded by retention | cheapest | most expensive | storage cheap, **queries metered** |
| Latency to read | milliseconds | seconds (snapshot commit) | milliseconds | seconds |
| Accepts updates | only via compaction | v2 upserts, awkwardly | natively | `MERGE`, by rewriting partitions |

The rule that follows: **history goes to the log and the lakehouse; current state goes to the serving store.**

## Two shapes of dual sink

### Fan-out from one operator (DataStream)

```java
withAuth.sinkTo(Kafka.sink(...));                          // immutable log, exactly-once
withAuth.sinkTo(Yugabyte.enrichedTransactions(config));    // queryable state, upsert
withAuth.sinkTo(Snowflake.enrichedTransactions(config));   // warehouse history, append + read-time dedup
```

Both sinks read the same operator output. Flink does **not** re-run the upstream pipeline per sink — the stream
is forked after the last shared operator.

### Fan-out from one query (SQL statement set)

```sql
CREATE TEMPORARY VIEW region_revenue_1m AS SELECT ... ;

INSERT INTO region_revenue_kafka             SELECT ... FROM region_revenue_1m;
INSERT INTO iceberg.lakehouse.region_revenue_1m SELECT ..., dt FROM region_revenue_1m;
INSERT INTO region_revenue_ysql              SELECT ... FROM region_revenue_1m;
```

Because `SqlRunnerJob` collects every `INSERT` into one `StatementSet`, the planner computes the join and the
window **once**. Three separate jobs would pay for three consumer groups, three copies of the join state and
three windows, for identical results.

Check it in the job graph: one `WindowAggregate`, three sinks hanging off it.

## Delivery guarantees differ per sink, on purpose

| Sink | Guarantee | Mechanism |
|---|---|---|
| Kafka | **exactly-once** | two-phase commit; a transaction per checkpoint, committed when the checkpoint completes |
| Iceberg | **exactly-once** | one Iceberg snapshot per checkpoint; the metadata swap is atomic |
| YugabyteDB (DataStream) | at-least-once, **idempotent result** | `INSERT ... ON CONFLICT DO UPDATE` on the natural key |
| YugabyteDB (SQL) | upsert | `PRIMARY KEY ... NOT ENFORCED` makes Flink emit an upsert |
| Snowflake — `enriched_transactions` | at-least-once, **duplicates removed on read** | plain `INSERT`; `QUALIFY ROW_NUMBER()` in `v_enriched_transactions` |
| Snowflake — `fraud_alerts` | at-least-once, **idempotent result** | `MERGE ... WHEN NOT MATCHED` on `alert_id` |

The YugabyteDB row is the interesting one. Flink's JDBC connector *can* do true exactly-once through XA
two-phase commit (`buildExactlyOnce`), but YugabyteDB does not expose an `XADataSource`. So the sink is
at-least-once and the **result** is made idempotent instead: replaying a batch after a restart overwrites rather
than duplicates. That is the pragmatic way to get effectively-once into a database, and it is worth knowing that
the guarantee depends on the sink system, not on Flink.

## Iceberg

```
MinIO (S3)  ──  object storage: Parquet data files + Avro manifests + metadata.json
Iceberg REST ── the catalog: which metadata.json is current for each table
Flink        ── writes files, then asks the catalog to swap the pointer
```

The catalog is only the atomic-swap point. Data and metadata live in object storage; a commit is "write new
files, then atomically move the table pointer". That is why the catalog needs durable storage but very little of
it, and why any engine that can read the catalog can read the table.

```bash
kubectl -n flink-stream port-forward svc/fs-flink-stream-iceberg-rest 8181:8181 &
curl -s localhost:8181/v1/namespaces/lakehouse/tables | jq
curl -s localhost:8181/v1/namespaces/lakehouse/tables/region_revenue_1m | jq '.metadata.snapshots | length'

# the files themselves
kubectl -n flink-stream exec deploy/fs-flink-stream-minio -- sh -c \
  'mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls --recursive l/warehouse'
```

You will see `data/dt=2026-09-11/*.parquet` alongside `metadata/*.metadata.json`, `*-m0.avro` (manifests) and
`snap-*.avro` (snapshot lists).

### The small-file problem is real here

Every checkpoint produces a new snapshot and a new set of files. At a 30-second checkpoint interval that is
**2,880 snapshots a day**, most of them a few kilobytes. A production deployment needs:

- periodic **compaction** (`rewrite_data_files`) to merge small files,
- **snapshot expiry** to drop old metadata,
- **orphan file cleanup**.

Flink cannot do those; they are Spark/Trino procedures or a catalog-side maintenance service. Lengthening the
checkpoint interval helps but trades against output latency. This is the main operational cost of streaming into
a lakehouse, and it is worth meeting on a laptop rather than in production.

### Partitioning

`PARTITIONED BY (dt)` is an **identity** partition on a column the query materialises
(`DATE_FORMAT(window_start, 'yyyy-MM-dd')`). Iceberg also supports hidden partition transforms
(`days(ts)`, `bucket(16, id)`) but Flink DDL cannot express them — create those through Spark or the REST API.

## YugabyteDB

YSQL is PostgreSQL-compatible at the wire and syntax level, so `'connector' = 'jdbc'` with the Postgres dialect
and the stock driver work unchanged. What differs is underneath: a YSQL table is sharded ("tableted") across
nodes, and the primary key clause says how.

```sql
PRIMARY KEY (region HASH, window_start ASC)
```

- `HASH` — hash-shard on this column. Rows spread uniformly across tablets; point lookups are fast, range scans
  on the column are impossible.
- `ASC` — range-shard, sorted. Range scans are fast; a monotonically increasing column concentrates all new
  writes on one tablet.

Hash on the entity, range on time: writes spread by entity, and "last hour for this entity" stays one ordered
scan. It is the same reasoning as choosing a Kafka message key, one layer down — and range-sharding on a
timestamp alone is the same mistake as keying a Kafka topic by time.

```bash
./scripts/ysql.sh "\d+ region_revenue"
./scripts/ysql.sh "EXPLAIN ANALYZE SELECT * FROM region_revenue WHERE region = 'WEST' ORDER BY window_start DESC LIMIT 10;"
```

The second one shows an Index Scan with a `Sort Key` it does not have to sort — because the range component
already stores the rows in that order.

### YugabyteDB as a *source* too

`merchant_risk` is read by Flink as a **lookup** table (`FOR SYSTEM_TIME AS OF t.proc_time`): Flink queries the
database per uncached row instead of keeping the dimension in state. Same connector, opposite direction. Its
hash primary key is what makes those point lookups cheap, and `lookup.partial-cache.*` is the knob between
"always fresh" and "never hits the database".

## Snowflake

The odd one out: the only sink in this project that is not a pod in the cluster. It is **off by default**, and
everything else works without it.

```bash
helm upgrade fs charts/flink-stream --reuse-values \
  --set snowflake.enabled=true \
  --set snowflake.account=myorg-myaccount \
  --set snowflake.pat.existingSecret=snowflake-pat
```

### No connector, and none needed

There is no Flink connector for Snowflake. There does not have to be one, because the DataStream `JdbcSink` is
**dialect-free** — you hand it SQL text and a binder, and it batches `PreparedStatement` executions:

```java
JdbcSink.<EnrichedTransaction>builder()
        .withQueryStatement(insert, bind)
        .withExecutionOptions(executionOptions(config))
        .buildAtLeastOnce(connection(config));
```

The Table/SQL `'connector' = 'jdbc'` is the one that needs a registered `JdbcDialect`, which is why the SQL labs
sink to YugabyteDB and not here. Anything with a JDBC driver is reachable through the DataStream API, as long as
you are willing to write the statement yourself — and writing it yourself is what lets the two Snowflake sinks
use *different* statements for different reasons.

### Authentication: programmatic access tokens

A password belongs to a person. A Flink TaskManager is not a person, and giving one a human's credential means
it inherits that human's roles, their expiry and their MFA exemption. Snowflake's answer is the **programmatic
access token**: issued per service user, pinned to a **single role**, given its own expiry, revocable on its own.

| | Password | Key pair (`snowflake_jwt`) | **PAT** |
|---|---|---|---|
| What the job holds | a person's secret | a private key file | one opaque string |
| Scope | every role the user has | every role the user has | **one role** |
| Expiry | the person's rotation policy | key rotation, by hand | set per token |
| Revoke just this job | no | yes, with key juggling | yes |
| Fits in a Kubernetes Secret | — | awkwardly | yes |

It is presented **in the password slot** of the JDBC connection — not a bearer header, not `token=`:

```
jdbc:snowflake://myorg-myaccount.snowflakecomputing.com/?db=FLINK_STREAM&schema=STREAMING
    &warehouse=FLINK_STREAM_WH&role=FLINK_STREAM_WRITER&authenticator=programmatic_access_token
```

`authenticator=programmatic_access_token` tells a recent driver what kind of secret it is holding. Set
`snowflake.authenticator=snowflake` to send it as an ordinary password instead, which is how a driver older than
that authenticator accepts it — Snowflake validates the token either way.

Two gates on the Snowflake side have to be open before any of this works, and both are in
[`charts/flink-stream/files/snowflake-schema.sql`](../charts/flink-stream/files/snowflake-schema.sql):

- an **authentication policy** listing `PROGRAMMATIC_ACCESS_TOKEN`, and
- a **network policy** — Snowflake requires one on a `TYPE = SERVICE` user before it will accept a token at all.
  A bearer credential with no network constraint is usable by anyone who obtains it, from anywhere, and
  Snowflake declines to let you build that by accident.

### Where the token is, and is not

It reaches the job as a **file**, `SNOWFLAKE_PAT_FILE=/etc/snowflake/pat`, from a Secret volume. Two things it is
deliberately not:

- **Not a job argument.** `JobConfig` puts the `ParameterTool` into the job's global parameters, and Flink
  renders those verbatim on the job's page in the web UI. A `--snowflake-pat` flag is a credential on a
  dashboard.
- **Not an environment variable** (though the sink accepts `SNOWFLAKE_PAT` as a fallback). An env var shows up
  in `kubectl describe pod`, in a dump of the process environment, and in every child process. A file does none
  of that. The sink reads it once when it is built, so rotating the token still means restarting the job — but
  it means `kubectl patch secret` and a restart rather than a redeploy.

Rotation wants two live tokens at once; a single-token rotation is an outage. The `ALTER USER ... ADD
PROGRAMMATIC ACCESS TOKEN` / `REMOVE` pair for that is at the bottom of the schema file.

### Two statements, because a warehouse is not a database

The Yugabyte sink upserts every row. The Snowflake sink does not, and the split is the whole difference between
the two kinds of store:

```sql
-- enriched_transactions: high volume, plain INSERT, deduplicated at read time
CREATE OR REPLACE VIEW v_enriched_transactions AS
SELECT * FROM enriched_transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY processed_at DESC) = 1;

-- fraud_alerts: low volume, MERGE on the way in (Snowflake has no ON CONFLICT)
MERGE INTO fraud_alerts t
USING (SELECT ?::VARCHAR AS alert_id, ...) s ON t.alert_id = s.alert_id
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

Yugabyte upserts everything because a row costs one key-value write. Snowflake rewrites whole micro-partitions,
so a `MERGE` per checkpoint on the fact stream would burn credits to save a `QUALIFY` clause. On the alert
stream — a handful of rows a minute — the `MERGE` is free and worth having.

### Batch size is a cost decision here

`snowflake.batchSize: 5000`, against Yugabyte's 500. Snowflake bills compute by the second and pays a fixed cost
per statement, so what matters is the number of statements, not the rows in them. 5,000 rows × 15 bound columns
is 75,000 binds, which also clears `CLIENT_STAGE_ARRAY_BINDING_THRESHOLD` (65,280) — above it the driver stops
sending one enormous `INSERT` and instead uploads the batch to a temporary stage and `COPY`s it. Checkpoints
still flush regardless, so nothing waits longer than a checkpoint interval to become visible.

For volumes where even that is too expensive, the next step is **Snowpipe Streaming** — Snowflake's own
row-oriented ingest SDK, which bypasses the warehouse entirely and charges per ingested byte. It is a different
client library, not a JDBC setting, and it is where a real high-throughput pipeline ends up.

### Is it working?

```sql
-- rows arriving, and how much the at-least-once sink is actually duplicating
SELECT COUNT(*) AS rows_written, COUNT(DISTINCT txn_id) AS distinct_txns, MAX(processed_at) AS newest
FROM FLINK_STREAM.STREAMING.enriched_transactions;

-- which credential each connection used; failed logins land here too, with ERROR_MESSAGE
SELECT EVENT_TIMESTAMP, AUTHENTICATION_METHOD, IS_SUCCESS, ERROR_MESSAGE, CLIENT_IP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE USER_NAME = 'FLINK_STREAM_SVC' ORDER BY EVENT_TIMESTAMP DESC LIMIT 20;
```

`AUTHENTICATION_METHOD` should read `PROGRAMMATIC_ACCESS_TOKEN`. If the job cannot log in at all, that view is
the first place to look — the error there names which of the two policies refused.

## Exercises

1. Stop the SQL job for five minutes, then restart it. Kafka and Iceberg gain a gap; YugabyteDB's
   `region_revenue` gains nothing for those windows and then resumes. Which of the three can you reconstruct the
   gap from?
2. Count Iceberg snapshots, wait ten minutes, count again. Multiply by 24 hours.
3. Change `region_revenue`'s primary key to `(window_start HASH, region ASC)` and re-run the `EXPLAIN ANALYZE`
   above. The per-region query becomes a full scan.
4. Point `pattern_alerts_kafka`'s INSERT at an Iceberg table instead. It works — `MATCH_RECOGNIZE` is
   append-only. Then try it with `account_live_state`'s query and read the planner's refusal.
5. With the Snowflake sink running, kill the JobManager pod and let it restart from its last checkpoint. Then
   compare `COUNT(*)` against `COUNT(DISTINCT txn_id)` in `enriched_transactions`, and the same two counts
   through `v_enriched_transactions`. The gap is exactly the replayed batch.
6. Issue a second PAT with `ROLE_RESTRICTION` set to a role the service user does not hold. The connection is
   refused — and `LOGIN_HISTORY` says why. That is the difference between a scoped token and a password.

## Next

[08 — Operations](08-operations.md)
