# 05 — Joins and shards

This is the core of the project. Everything else is an illustration of it.

## The claim

> Every `keyBy` is a full network shuffle. There is no such thing as a co-partitioned shortcut in Flink, and a
> join between two streams keyed differently is a shuffle of *both* of them.

That sounds obvious and it is routinely got wrong, because people reason as though Flink subtask *i* reads Kafka
partition *i* and therefore "already has" the right data.

## Why there is no shortcut

Kafka and Flink shard with **different hash functions over different spaces**.

```
Kafka:  partition   = (murmur2(keyBytes) & 0x7fffffff) % numPartitions
Flink:  keyGroup    = murmurHash(key.hashCode()) % maxParallelism
        subtask     = keyGroup * parallelism / maxParallelism
```

Kafka hashes the serialized **bytes**; Flink hashes the deserialized object's **`hashCode()`**, maps it onto a
fixed number of *key groups* (`maxParallelism`, set once and frozen in the state snapshot), and then deals key
groups out to subtasks in contiguous ranges. Two different hash functions, two different moduli. Even when a job
keys by exactly the field Kafka partitioned on, the record almost certainly has to move.

Key groups are why rescaling works at all: parallelism can change, but key groups cannot, so restoring a
savepoint at a new parallelism is just re-dealing existing key-group ranges to a different number of subtasks.
It is also why `pipeline.max-parallelism` is chosen once, up front, and never changed.

## Seeing it

Every transaction carries `shard_id`, the Kafka partition its key hashes to, computed by the producer itself.
Downstream operators report `subtask_index`. Comparing them makes the shuffle concrete.

**Before any keyBy** — each source subtask owns a slice of the shards:

```bash
kubectl -n flink-stream port-forward svc/fs-flink-stream-datastream-rest 8081:8081
# Flink UI > the "observe shards (pre-shuffle)" operator > Metrics
#   shardObserver.source.distinctShardsSeen  ->  6 per subtask at parallelism 2 (12 shards / 2)
```

**After a keyBy** — every subtask sees data from every shard:

```sql
-- the SQL job writes this; shards_touched is COUNT(DISTINCT shard_id) within the window
SELECT region, window_start, txn_count, shards_touched
FROM region_revenue
ORDER BY window_start DESC
LIMIT 10;
```

```bash
./scripts/ysql.sh "SELECT region, window_start, txn_count, shards_touched FROM region_revenue ORDER BY window_start DESC LIMIT 10;"
```

`shards_touched` reaching 12 is the shuffle, measured. Five region keys, 12 source shards, and every region's
rows arriving from all of them.

And the enriched output records which subtask produced each row:

```bash
./scripts/ysql.sh "SELECT shard_id, subtask_index, COUNT(*) FROM enriched_transactions GROUP BY 1,2 ORDER BY 1,2;"
```

A dense shard × subtask matrix, not a diagonal.

## The five joins, and what each one costs

| Join | State | Deterministic on replay? | Use it when |
|---|---|---|---|
| **Regular** `JOIN b ON ...` | **unbounded** — every row of both sides, forever | yes | last resort; always set `table.exec.state.ttl` |
| **Interval** `AND b.ts BETWEEN a.ts - x AND a.ts + y` | bounded by the interval × rate | yes | the match is inherently time-local (transaction ⋈ auth) |
| **Temporal** `b FOR SYSTEM_TIME AS OF a.event_time` | one version per dimension key | **yes** | a versioned dimension (FX rates, account attributes) |
| **Lookup** `b FOR SYSTEM_TIME AS OF a.proc_time` | none — queried per row, cached | **no** | dimension is authoritative elsewhere and rarely changes |
| **Window** two window TVFs joined on `window_start` | bounded by one window | yes | the comparison is inherently per-window |

The regular join is the one that bites, because it is the join you would write in a database. It is also the
only one whose state grows without bound.

**Temporal vs lookup** is the trade worth sitting with. `40-joins.sql` runs both against the same dimension:

- `merchants FOR SYSTEM_TIME AS OF t.event_time` keeps the dimension in Flink state, versioned. Replaying last
  week reproduces last week's answers exactly.
- `merchant_risk_lookup FOR SYSTEM_TIME AS OF t.proc_time` queries YugabyteDB per uncached row. No Flink state
  at all, always the freshest value — and a replay produces *today's* answer for last week's data.

Neither is right. Which you want depends on whether "correct" means "reproducible" or "current".

## The labs

### DataStream

| Lab | Join | Key change |
|---|---|---|
| A | broadcast enrichment | none — the dimension moves to every subtask instead |
| C | interval join with auth events | `account_id` → `txn_id`: **both sides reshuffled** |
| H | hand-written versioned join with accounts | `account_id` → `account_id`: still a shuffle |
| I | cross-shard aggregation | `account_id` → `region`: five keys drawing from 12 shards |

Lab A is the interesting counter-example: **broadcast is how you avoid a shuffle.** The merchant dimension is
small and slow-moving, so every subtask keeps a full copy in broadcast state and the fact stream never moves.
That works up to thousands of dimension rows; past that, the per-subtask copy and its checkpoint cost stop being
worth it and a keyed join wins.

### SQL

`30-windows.sql` builds `txn_with_account` — transactions temporally joined to accounts — and then aggregates it
by `region`. Look at the plan:

```sql
EXPLAIN CHANGELOG_MODE
SELECT region, window_start, COUNT(*), SUM(amount)
FROM TABLE(TUMBLE(TABLE txn_with_account, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY region, window_start, window_end;
```

`Exchange(distribution=[hash[region]])` is the shuffle. `GlobalWindowAggregate` over `LocalWindowAggregate` is
the two-phase aggregation that keeps it affordable.

## Low-cardinality keys are where shuffles stop scaling

Five regions means at most five subtasks can do work in the region aggregate, however high you set parallelism.
That skew is visible in the per-subtask record counts in the Flink UI, and it is not a bug to hide — it is the
shape of the problem.

Two-phase aggregation (`table.optimizer.agg-phase-strategy: TWO_PHASE`) is the mitigation: pre-aggregate on the
source side *before* the shuffle, then combine after it. The shuffle then carries one partial aggregate per key
per subtask per mini-batch instead of one message per record. Turn it off and watch the network numbers on the
exchange:

```sql
SET 'table.optimizer.agg-phase-strategy' = 'ONE_PHASE';
```

The DataStream equivalent is `aggregate(AggregateFunction, ProcessWindowFunction)` rather than
`process(ProcessWindowFunction)` — the accumulator is what crosses the network, not the records.

## Sharding does not stop at Kafka

The serving schema ([`yugabyte-schema.sql`](../charts/flink-stream/files/yugabyte-schema.sql)) makes the same
decision one layer down. YSQL extends `PRIMARY KEY` with per-column directives:

```sql
PRIMARY KEY (region HASH, window_start ASC)
```

`HASH` spreads rows across tablets; `ASC` keeps them range-ordered within one. Hashing on the entity and ranging
on time gives spread writes *and* fast "last hour for this entity" scans. Range-sharding on a timestamp alone
would put every new write on one tablet — the same hot-partition problem as keying a Kafka topic by time.

## Exercises

1. Set `flink.datastream.parallelism` to 1 and re-read `distinctShardsSeen` before and after the keyBy. Then set
   it to 12. What changes, and what does not?
2. Change Lab I to key by `account_id` instead of `region`. The shuffle is still total — but now with 400 keys
   instead of 5, so it spreads. Compare per-subtask record counts.
3. Take `40-joins.sql`'s lookup join and change `t.proc_time` to `t.event_time`. Read the planner's error
   carefully: it is telling you a JDBC table has no versioning, so there is no "as of event time" to look up.
4. Delete a row from `merchant_risk` in YugabyteDB while the job runs. How long until the lookup join notices?
   (`lookup.partial-cache.expire-after-write` is 5 minutes.)

## Next

[06 — State, checkpoints and savepoints](06-state-and-checkpoints.md)
