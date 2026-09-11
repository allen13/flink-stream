# 03 — The DataStream labs

All ten labs live in one job:
[`TransactionPipelineJob`](../flink-jobs/src/main/java/io/flinkstream/datastream/TransactionPipelineJob.java).

They share a job graph on purpose. A Flink job is a *graph*, not a pipeline: one source feeds many independent
branches, many branches land in many sinks, and the whole thing shares one set of checkpoints and one restart
policy. Ten separate jobs would be ten JobManagers on a laptop.

Run a subset while experimenting:

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.datastream.labs="a,b,c"
```

Open the UI first — most of what follows is about reading it:

```bash
kubectl -n flink-stream port-forward svc/fs-flink-stream-datastream-rest 8081:8081
```

---

## Lab A — Broadcast state

[`MerchantEnrichmentFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/MerchantEnrichmentFunction.java)

The merchant dimension is small and slow-moving, so rather than shuffling the fact stream by `merchant_id`,
every subtask keeps a full copy of the dimension in **broadcast state** and the facts never move.

Three things the API forces on you, each for a reason:

- `processBroadcastElement` may write broadcast state; `processElement` gets a `ReadOnlyBroadcastState`. Every
  subtask has to converge on identical broadcast state — if the keyed side could mutate it, subtasks would
  diverge and a restore would be non-deterministic.
- There is no ordering guarantee between the two inputs. An early transaction can arrive before the merchant
  that explains it. That is what the DLQ side output is for, and you will see entries in `out.dead-letter` for
  the first few seconds after every start.
- Broadcast state is on-heap and checkpointed *once per subtask*. Fine for thousands of rows, not for millions.

```bash
./scripts/kafka.sh tail out.dead-letter 5
# reason=UNKNOWN_MERCHANT, one per transaction that beat its merchant into the operator
```

In the UI, the operator's `merchantHits` and `merchantMisses` counters are custom metrics registered in `open()`.

---

## Lab B — Async I/O

[`AsyncRiskScoreFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/AsyncRiskScoreFunction.java)

A `map` that calls a remote service blocks its subtask for the whole call, so throughput collapses to
`parallelism / latency`. `AsyncDataStream` keeps `capacity` requests in flight per subtask instead.

The rules:

- **Never block in `asyncInvoke`.** Hand off to an executor and return.
- **Always override `timeout`.** The default fails the job; completing with a fallback keeps a slow dependency
  from becoming a restart loop.
- `unorderedWait` emits as results finish and is faster; `orderedWait` buffers back into input order. Under event
  time, "unordered" still respects watermarks — results never cross a watermark boundary out of order.
- In-flight requests are checkpointed, so exactly-once still holds: on restore they are replayed.

Try raising the simulated latency and watching throughput *not* collapse:

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.datastream.args.risk-latency-ms=250
```

Then set `--set flink.datastream.args.async-capacity=1` and watch it collapse anyway — capacity is the knob that
matters.

---

## Lab C — Interval join

[`AuthJoinFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/AuthJoinFunction.java)

Pairs each transaction with the authorization decision that followed it, within `[-2s, +45s]`.

Flink buffers both sides in keyed state and evicts a record as soon as the watermark proves no partner can still
arrive — so unlike a regular join, state is bounded by the interval width times the event rate, not by history.

This is also the job's **second shuffle**: both streams arrive keyed by `account_id` and are joined on `txn_id`.

Interval join is inner-join only: transactions whose auth never arrives simply vanish. The generator deliberately
sends about 10% of auth events *outside* the window and drops 8% entirely, so the difference between the
pre-join and post-join record counts in the UI is real, not noise.

---

## Lab D — Tumbling windows, lateness, side outputs

[`TransactionAggregate`](../flink-jobs/src/main/java/io/flinkstream/functions/TransactionAggregate.java) +
[`WindowStatsFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/WindowStatsFunction.java)

The pattern to internalise is `aggregate(AggregateFunction, ProcessWindowFunction)`:

- the `AggregateFunction` folds records into an accumulator as they arrive, so a window holding a thousand
  transactions costs one accumulator;
- the `ProcessWindowFunction` runs once per window with the key and the window metadata.

A bare `process(ProcessWindowFunction)` buffers every record until the window fires. That is the difference
between a window that costs bytes and one that costs megabytes.

**Lateness** has three tiers:

| Timing | What happens |
|---|---|
| within `maxOutOfOrderness` (5s) | absorbed; the watermark has not passed the window yet |
| within `allowedLateness` (10s) | the window fires **again**, emitting a corrected result |
| later | routed to `sideOutputLateData` → `out.late-events` |

Without `sideOutputLateData` those records vanish and the only trace is the `numLateRecordsDropped` metric. The
generator stamps 3% of transactions 60–180s in the past specifically so this topic has traffic:

```bash
./scripts/kafka.sh tail out.late-events 5
```

The "fires again" tier is worth seeing: `out.account-window-stats` contains *two* records for some
(account, window) pairs. Downstream consumers must be able to handle that — which is exactly why the YugabyteDB
tables are upserts.

---

## Lab E — Session windows

Same aggregate, `EventTimeSessionWindows.withGap(2 min)`.

Session windows **merge**: each record initially opens its own window and Flink merges overlapping ones. That is
why `AggregateFunction.merge` has to be correct — and why an incorrect `merge` produces silently wrong session
results while tumbling windows stay fine.

---

## Lab F — Keyed state, event-time timers, state TTL

[`VelocityRuleFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/VelocityRuleFunction.java)

"Five transactions totalling $2,500 within three minutes." The hand-written counterpart to Lab D, and the
comparison is the point:

- A `KeyedProcessFunction` controls *when* to emit. The window operator can only emit at boundaries; here the
  alert fires the moment the threshold is crossed.
- The timer registered at `eventTime + window` expires the counters. Timers are keyed, checkpointed state — they
  survive restarts and fire on watermark advance, not wall clock.
- `StateTtlConfig` is the safety net. An account that goes quiet forever would leave counters in RocksDB
  indefinitely, since the timer only fires if a watermark passes it. `cleanupInRocksdbCompactFilter` removes
  them during normal compaction.

```bash
./scripts/kafka.sh tail out.fraud-alerts 5
./scripts/ysql.sh "SELECT rule, severity, COUNT(*) FROM fraud_alerts GROUP BY 1,2 ORDER BY 3 DESC;"
```

---

## Lab G — CEP

[`EscalatingSpendPattern`](../flink-jobs/src/main/java/io/flinkstream/functions/EscalatingSpendPattern.java)

Card testing: a sub-$5 card-not-present probe, then two or more escalating charges, within ten minutes.

- `next` = strictly contiguous; `followedBy` = allows unrelated events between; `followedByAny` = allows
  overlapping matches. The choice drives semantics *and* how much partial-match state the NFA holds.
- `IterativeCondition` can inspect events already matched by earlier parts of the pattern — that is how "each
  charge larger than the last" is expressed.
- `within` bounds the state. Without it, partial matches accumulate forever.
- CEP needs a keyed stream to be parallel at all; on a non-keyed stream the NFA runs at parallelism 1.

Compare its output with the SQL `MATCH_RECOGNIZE` version:

```bash
./scripts/kafka.sh tail out.fraud-alerts 10 | grep CARD_TESTING_CEP
./scripts/kafka.sh tail sql.pattern-alerts 10
```

Same detector, two languages, same NFA underneath.

---

## Lab H — A hand-written versioned join

[`AccountJoinFunction`](../flink-jobs/src/main/java/io/flinkstream/functions/AccountJoinFunction.java)

`FOR SYSTEM_TIME AS OF` written out in full, as a `KeyedCoProcessFunction`. Read it next to
`sql/30-windows.sql` — the SQL clause compiles to almost exactly this operator.

Two problems any such join has to solve:

- **Arrival order.** A transaction can arrive before the account that describes it. Those are parked in
  `pendingState` and released when the account shows up. Dropping them instead is the most common bug in
  hand-written enrichment joins, and it only shows up at startup or after a rescale.
- **Unbounded state.** The dimension side never expires on its own. TTL bounds it, at the cost of re-reading the
  compacted topic after a long idle period.

Its `accountJoined` / `accountBuffered` / `accountDropped` counters show all three paths happening.

---

## Lab I — Cross-shard aggregation

`keyBy(region)` over the output of Lab H, sliding 5m/1m windows.

Transactions are sharded by `account_id` and regions are spread uniformly over accounts, so every one of the
five region keys draws from all 12 shards. This is a complete all-to-all exchange, and
[docs/05](05-joins-and-shards.md) is about what that costs.

Only five distinct keys exist, so above parallelism 5 some subtasks idle. That skew is not hidden: low-cardinality
keys are exactly where a shuffle stops scaling, and the per-subtask record counts in the UI show it.

---

## Lab J — Custom metrics

[`ShardObserver`](../flink-jobs/src/main/java/io/flinkstream/functions/ShardObserver.java)

One of each metric type, so all four appear in the UI and in Prometheus:

| Type | Here |
|---|---|
| `Counter` | records seen |
| `Meter` | records/second, derived from the counter by a `MeterView` |
| `Gauge` | `distinctShardsSeen` — the number of distinct Kafka shards this subtask has seen |
| `Histogram` | end-to-end lag in milliseconds |

`distinctShardsSeen` is the one to watch. Placed before any `keyBy` it reads `12 / parallelism`. Placed after
one it approaches 12 on *every* subtask. That difference is the shuffle, measured.

```bash
# Prometheus endpoint on every TaskManager
kubectl -n flink-stream port-forward fs-flink-stream-datastream-taskmanager-1-1 9249:9249
curl -s localhost:9249/metrics | grep shardObserver
```

---

## Exercises

1. Set `flink.datastream.labs=d` and compare the job graph with `labs=all`. One source, many branches.
2. Set `allowed-lateness-s=0` and watch `out.late-events` volume jump.
3. Break `TransactionAggregate.merge` (return `a` unchanged) and compare tumbling vs session results. Only the
   session numbers go wrong — silently.
4. Kill a TaskManager (`kubectl delete pod ...-taskmanager-1-1`) and watch the job restart from its last
   checkpoint. The velocity counters resume mid-burst; they are state, not memory.

## Next

[04 — The SQL labs](04-sql-labs.md)
