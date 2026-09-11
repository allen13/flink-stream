# 07 — Dual sinks: Apache Iceberg and YugabyteDB

Several queries in this project write their result to **two or three places at once**. That is not redundancy —
streams and tables answer different questions, and no single store answers both well.

| | Kafka | Apache Iceberg | YugabyteDB |
|---|---|---|---|
| Shape | append-only log | append-only table on object storage | mutable rows |
| Answers | "what happened, in order" | "what happened, over months" | "what is true **now**" |
| Reader | another stream job | Spark, Trino, DuckDB, Flink batch | an application, a dashboard |
| Cost per row | cheap, bounded by retention | cheapest | most expensive |
| Latency to read | milliseconds | seconds (snapshot commit) | milliseconds |
| Accepts updates | only via compaction | v2 upserts, awkwardly | natively |

The rule that follows: **history goes to the log and the lakehouse; current state goes to the serving store.**

## Two shapes of dual sink

### Fan-out from one operator (DataStream)

```java
withAuth.sinkTo(Kafka.sink(...));              // immutable log, exactly-once
withAuth.sinkTo(Yugabyte.enrichedTransactions(config));   // queryable state, upsert
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

## Exercises

1. Stop the SQL job for five minutes, then restart it. Kafka and Iceberg gain a gap; YugabyteDB's
   `region_revenue` gains nothing for those windows and then resumes. Which of the three can you reconstruct the
   gap from?
2. Count Iceberg snapshots, wait ten minutes, count again. Multiply by 24 hours.
3. Change `region_revenue`'s primary key to `(window_start HASH, region ASC)` and re-run the `EXPLAIN ANALYZE`
   above. The per-region query becomes a full scan.
4. Point `pattern_alerts_kafka`'s INSERT at an Iceberg table instead. It works — `MATCH_RECOGNIZE` is
   append-only. Then try it with `account_live_state`'s query and read the planner's refusal.

## Next

[08 — Operations](08-operations.md)
