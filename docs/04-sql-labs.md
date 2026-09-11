# 04 — The SQL labs

The SQL pipeline lives in [`charts/flink-stream/files/sql/`](../charts/flink-stream/files/sql/) and is executed
by [`SqlRunnerJob`](../flink-jobs/src/main/java/io/flinkstream/sql/SqlRunnerJob.java) as a normal Flink
application deployment.

The files are in the **chart**, not in the image. They reach the job through a ConfigMap, so changing a query is
a `helm upgrade` rather than a rebuild — and there is exactly one copy of each query in the repository.

```bash
vi charts/flink-stream/files/sql/30-windows.sql
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values
kubectl -n flink-stream delete flinkdeployment fs-flink-stream-sql && ./scripts/deploy.sh
```

## How a pure-SQL pipeline gets deployed

`SqlRunnerJob` does three things that matter:

1. **Splits the files into statements** ([`SqlScript`](../flink-jobs/src/main/java/io/flinkstream/sql/SqlScript.java)).
   Not `split(";")` — a Kafka bootstrap list is a semicolon-separated string literal, and naive splitting cuts
   the DDL in half.
2. **Collects every `INSERT` into one `StatementSet`.** This is the important one. Submitted individually you
   would get one Flink job per INSERT, each with its own Kafka consumer group reading the same topic and its own
   copy of the join state. A statement set compiles them into a **single job graph** where the shared source is
   read once and fanned out.
3. **Applies `SET 'k' = 'v'` to the `TableConfig`** before planning, so the files can turn on mini-batch, change
   state TTL, or pick a join strategy exactly as you would in the SQL client.

`${VAR}` and `${VAR:-default}` in the SQL are substituted from the environment, which is how the chart injects
Kafka and Schema Registry coordinates.

---

## 00 — Catalogs

Flink has no storage of its own; a *catalog* says where table metadata lives.

| Catalog | Lifetime |
|---|---|
| `default_catalog` | in-memory, dies with the job. Every Kafka table lives here — the DDL **is** the schema, versioned in git rather than in a metastore. |
| `iceberg` | a REST catalog on object storage. Tables outlive the job and other engines can read them. |

The Iceberg catalog is deliberately never made current with `USE CATALOG`: that would make unqualified
`CREATE TABLE` land in Iceberg. Iceberg tables are always addressed by their full three-part name.

---

## 10 — Sources

Notice what is *not* in the DDL: no hand-copied Avro field types to keep in sync. Schema Registry holds the
writer schema, Flink derives a reader schema from the DDL, and Avro's resolution rules bridge them. That is what
makes it safe for a producer to add a field without redeploying this job.

Three DDL features worth studying:

- **Metadata columns** expose Kafka's own record attributes as ordinary columns. `VIRTUAL` keeps them out of any
  `INSERT` into the table.

  ```sql
  `partition`  INT              METADATA VIRTUAL,
  `offset`     BIGINT           METADATA VIRTUAL,
  ingest_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
  ```

- **Computed columns** are evaluated on read, cost nothing to store, and can be used in `WHERE`, `GROUP BY`, and
  as the watermark source. `amount_band` and `proc_time` are both computed.

- **`WATERMARK FOR`** turns a plain `TIMESTAMP` into the event-time attribute. Without it every window and
  temporal join below is a *planning* error, not a runtime one.

### `kafka` vs `upsert-kafka`

| Connector | Reads the topic as | Needs |
|---|---|---|
| `kafka` | a stream of independent INSERTs (append-only) | nothing |
| `upsert-kafka` | a **changelog**: each record replaces the previous one with the same key; a null value is a DELETE | `PRIMARY KEY ... NOT ENFORCED` |

Only `upsert-kafka` can be the right side of a temporal join, because only it has a notion of "the current
version of key K".

Keys are read with `'key.format' = 'raw'` because the producer writes plain UTF-8 keys and registers no key
schema — the common Confluent convention for string keys.

---

## 20 — Sinks, and changelog modes

The constraint that governs which query can go to which sink:

| Changelog mode | Produced by | Accepted by |
|---|---|---|
| **append-only** | window TVF aggregates, `MATCH_RECOGNIZE`, interval joins, temporal joins | everything |
| **updating** | regular `GROUP BY`, Top-N, regular joins | `upsert-kafka`, JDBC **with a primary key** |

Sending an updating stream to an append-only sink fails at *plan* time:

```
does not support consuming update changes which is produced by node GroupAggregate
```

That is a good error to have seen once. Try it: point `account_live_state_ysql`'s INSERT at
`region_revenue_kafka` and read what the planner says.

`PRIMARY KEY ... NOT ENFORCED` is what turns the JDBC table into an upsert sink — Flink emits
`INSERT ... ON CONFLICT DO UPDATE`. "NOT ENFORCED" means Flink trusts the declaration rather than checking it;
the database enforces uniqueness.

---

## 30 — Windows, and the three-sink fan-out

This is the centrepiece. One view, three `INSERT`s:

```
                           ┌──▶ region_revenue_kafka          (Kafka, append, exactly-once)
txn_with_account ──▶ TUMBLE 1m ──▶ iceberg.lakehouse.region_revenue_1m  (Parquet on S3)
                           └──▶ region_revenue_ysql           (YugabyteDB, upsert)
```

Because all three are in one statement set, the planner computes the join and the window **once** and fans the
result out. Three separate jobs would pay for three consumer groups, three copies of the join state and three
windows, for identical results.

Verify all three landed:

```bash
./scripts/kafka.sh tail sql.region-revenue 3
./scripts/ysql.sh "SELECT region, window_start, txn_count, shards_touched FROM region_revenue ORDER BY window_start DESC LIMIT 6;"
kubectl -n flink-stream exec deploy/fs-flink-stream-minio -- sh -c \
  'mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null; mc ls --recursive l/warehouse/lakehouse/region_revenue_1m/data'
```

### Window TVFs

`TUMBLE` / `HOP` / `CUMULATE` as table-valued functions replaced the old `GROUP BY TUMBLE(...)` grouped-window
syntax. They are strictly more capable: `window_start` and `window_end` become real columns, which is what makes
Window Top-N and window joins expressible, and the result is **append-only** rather than retracting.

### Window Top-N

```sql
ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total_usd DESC)
```

Partitioning by the *window columns* is what the planner recognises as a **window** Top-N: it can emit once per
window and then forget. Partition by anything else and you get an ordinary Top-N — unbounded state, and a
retraction every time the ranking changes.

### Tuning knobs, and what they actually do

| Setting | Effect |
|---|---|
| `table.exec.mini-batch.*` | turns per-record state access into per-batch. Often 2–5× on a high-cardinality aggregate, paid for with up to `allow-latency` of delay |
| `table.optimizer.agg-phase-strategy = TWO_PHASE` | pre-aggregate before the shuffle, combine after. Keeps a five-key `GROUP BY` from hot-spotting one subtask |
| `table.exec.state.ttl` | bounds regular joins and regular `GROUP BY`. Without it they grow forever |
| `table.exec.source.idle-timeout` | lets the watermark advance when some shards are quiet. The usual reason a windowed job "produces nothing" on a lightly loaded cluster |

---

## 40 — The five joins

Covered in depth in [docs/05](05-joins-and-shards.md). The file runs all of them against the same data so the
costs are comparable side by side — in particular the temporal join against `merchants` (dimension in Flink
state, replay-deterministic) next to the lookup join against `merchant_risk` in YugabyteDB (no Flink state,
always current, *not* replay-deterministic).

---

## 50 — MATCH_RECOGNIZE

The SQL form of DataStream lab G. Same NFA, same watermarks, same bounded state.

Two things in this file are there because of errors worth meeting:

**Greedy quantifiers cannot end a pattern.**

```
PATTERN (probe escalation{2,})    -- Greedy quantifiers are not allowed as the last element of a Pattern yet.
PATTERN (probe escalation{2,}?)   -- reluctant: emit as soon as the minimum is satisfied
```

A greedy final quantifier can never decide it is finished — there might always be one more matching row — so the
match could only be emitted at end of stream.

**`LAST(x, 1)` is NULL on the first row of its classifier.**

```sql
escalation AS escalation.amount > COALESCE(LAST(escalation.amount, 1), LAST(probe.amount))
```

Without the `COALESCE` the predicate evaluates to NULL, never true, and the pattern silently never fires. This is
the classic `MATCH_RECOGNIZE` bug.

**Two INSERTs into one exactly-once Kafka table fence each other.** Flink derives transactional ids as
`<prefix>-<subtask>-<checkpoint>`, so two sink operators on the same table generate identical ids and Kafka
does what it is designed to do:

```
ProducerFencedException: There is a newer producer with the same transactionalId which fences the current one
```

and the job restart-loops. The file therefore `UNION ALL`s both detectors into a single INSERT.

---

## Using the SQL client interactively

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values --set flink.session.enabled=true
./scripts/sql-client.sh
```

The client attaches to the **session cluster**, which unlike the application deployments starts empty and hosts
many jobs. Inside:

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'STREAMING';
```

Then paste the DDL from `10-sources.sql` (the lab files are mounted in the pod at `/opt/flink/sql-labs`) and
explore:

```sql
-- Does the producer's shard_id agree with Kafka's own partitioner? (It does.)
SELECT `partition`, shard_id, COUNT(*) AS n
FROM transactions
GROUP BY `partition`, shard_id;

-- What does the planner actually do with a cross-shard aggregate?
EXPLAIN CHANGELOG_MODE
SELECT region, window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE txn_with_account, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY region, window_start, window_end;

-- The table function, through a lateral join
SELECT t.txn_id, tag.k, tag.v
FROM transactions AS t, LATERAL TABLE(explode_tags(t.tags)) AS tag(k, v)
LIMIT 20;
```

`scripts/sql-client.sh` passes the job jar with `-j`, which puts the Kafka connector, the `avro-confluent`
format and this project's UDFs into the client's user classloader. The fat `flink-sql-*` connectors are
deliberately **not** installed in `/opt/flink/lib` — see [docs/09](09-troubleshooting.md) for the
`NullPointerException` that causes.

## Exercises

1. Change `30-windows.sql`'s `TUMBLE` to `CUMULATE(TABLE ..., INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)` and
   watch running 10-minute totals emit every minute.
2. Set `table.exec.mini-batch.enabled = false` and compare checkpoint duration and throughput in the UI.
3. Remove `table.exec.state.ttl` from `40-joins.sql` and watch the state size of the regular join climb.
4. Add a field with a default to `Transaction.avsc`, rebuild, redeploy the generator — and confirm the SQL job
   keeps running untouched. That is Avro schema resolution earning its keep.

## Next

[05 — Joins and shards](05-joins-and-shards.md)
