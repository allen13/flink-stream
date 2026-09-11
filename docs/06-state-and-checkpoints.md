# 06 — State, checkpoints and savepoints

## What is actually in state

Not "the data". Per key, and only what the operator needs to answer its next question:

| Operator | State |
|---|---|
| broadcast enrichment (A) | one full copy of the merchant dimension, **per subtask**, on heap |
| async risk score (B) | the in-flight requests, so exactly-once survives a restore |
| interval join (C) | both sides buffered for the join interval, evicted as the watermark passes |
| tumbling window (D) | one accumulator per (account, open window) — *not* the records |
| session window (E) | one accumulator per open session, plus merge bookkeeping |
| velocity rule (F) | count, total, a timer and a small list per active account |
| CEP (G) | partial NFA matches per account, bounded by `within` |
| account join (H) | the current account version per key, plus any pending unmatched transactions |
| SQL regular `GROUP BY` | one row per group, **forever**, unless `table.exec.state.ttl` says otherwise |

The line that matters: the incremental aggregate keeps one accumulator; a `ProcessWindowFunction` used alone
keeps every record. That is the difference between a window that costs bytes and one that costs megabytes.

```bash
# State size per operator, per checkpoint, in the UI
kubectl -n flink-stream port-forward svc/fs-flink-stream-datastream-rest 8081:8081
# Flink UI > Checkpoints > History > (a checkpoint) > click an operator
```

## Checkpoints

Checkpoints are Flink's internal recovery mechanism: periodic, automatic, and owned by the running job.

The settings this project uses, and why:

| Setting | Value | Why |
|---|---|---|
| `execution.checkpointing.interval` | 30s | also the visibility lag of the exactly-once Kafka sinks |
| `mode` | `EXACTLY_ONCE` | barrier alignment; `AT_LEAST_ONCE` skips it and is faster but can double-count |
| `unaligned.enabled` | true | barriers may overtake in-flight records. Matters here because the cross-shard joins create wide exchanges, which is exactly where alignment stalls |
| `aligned-checkpoint-timeout` | 10s | stay aligned (cheaper state) until alignment is actually slow, then switch |
| `state.backend.type` | `rocksdb` | state here outgrows the heap backend: keyed windows, join buffers, TTL'd maps |
| `state.backend.incremental` | true | only changed RocksDB SST files are uploaded per checkpoint |
| `externalized-checkpoint-retention` | `RETAIN_ON_CANCELLATION` | a cancelled job can be resumed from its last checkpoint |
| `num-retained` | 5 | how far back you can manually rewind |

**Exactly-once is end-to-end, not just internal.** The Kafka sinks open a transaction per checkpoint interval and
commit it when the checkpoint completes. Consumers reading `read_committed` therefore see output lag by up to one
checkpoint interval. That is the price, not a bug — and it is why `transaction.timeout.ms` (15 min) must
comfortably exceed the checkpoint interval, or the broker expires the transaction and the job fails on commit.

The two-phase commit means a checkpoint failure is not just a lost checkpoint: it is output that never becomes
visible. `setTolerableCheckpointFailureNumber(3)` says how many consecutive failures to accept before failing the
job instead of quietly falling behind.

## Savepoints

A savepoint is a *portable* copy of state, taken deliberately:

| | Checkpoint | Savepoint |
|---|---|---|
| Owner | the running job | you |
| Format | backend-specific, incremental | canonical, self-contained |
| Cost | cheap, frequent | expensive, occasional |
| Restore at a different parallelism | no | **yes** |
| Restore into a modified job graph | no | **yes**, if operator UIDs match |
| Survives job deletion | only if retained | yes |

The operator takes one hourly (`kubernetes.operator.periodic.savepoint.interval`) and keeps five.

```bash
kubectl -n flink-stream get flinkdeployment fs-flink-stream-datastream \
  -o jsonpath='{.status.jobStatus.savepointInfo}' | jq
```

### Operator UIDs are the contract

Every operator in `TransactionPipelineJob` sets `.uid("lab-a-broadcast-enrich")` and so on. That string is what
a savepoint stores state against. Without it Flink generates a UID from the operator's position in the graph, so
*adding an operator anywhere upstream* renames every UID downstream and the restore silently starts from empty
state.

Set a UID on every stateful operator, from the first commit, and never change one. It is the cheapest thing in
this project and the most expensive to retrofit.

### Taking one on demand

`FlinkStateSnapshot` makes a savepoint a Kubernetes object:

```bash
kubectl apply -f k8s-examples/flinkstatesnapshot.yaml
kubectl -n flink-stream get flinkstatesnapshot -w
```

### Upgrade modes

`spec.job.upgradeMode` decides what happens to state when the spec changes:

| Mode | Behaviour | Use when |
|---|---|---|
| `savepoint` | stop with a savepoint, restart from it | the default here: state survives a config change |
| `last-state` | restart from the last checkpoint without a clean stop | faster; only safe if the job graph is unchanged |
| `stateless` | throw state away | the logic changed incompatibly, or you want a clean slate |

## Rescaling

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.datastream.parallelism=4
```

With `upgradeMode: savepoint` the operator stops the job with a savepoint and restores it at the new
parallelism. That works because keyed state is stored per **key group**, not per subtask: rescaling re-deals
existing key-group ranges to a different number of subtasks. `pipeline.max-parallelism` (120 here) is the key
group count — chosen once, frozen in the state, and not changeable afterwards without discarding state.

## Failure drills

```bash
# 1. Kill a TaskManager. The job restarts from its last checkpoint; velocity counters resume mid-burst.
kubectl -n flink-stream delete pod fs-flink-stream-datastream-taskmanager-1-1
kubectl -n flink-stream get flinkdeployment -w

# 2. Kill the JobManager. With Kubernetes HA the leader election and job-graph pointers are in ConfigMaps,
#    so a new JobManager picks up where the old one left off rather than resubmitting from scratch.
kubectl -n flink-stream delete pod -l component=jobmanager
kubectl -n flink-stream get cm | grep config-map   # the HA ConfigMaps

# 3. Kill a Kafka broker. With replication factor 3 and min.insync.replicas=2 the job keeps running;
#    leadership moves and the sink's transactions carry on.
kubectl -n flink-stream delete pod fs-flink-stream-kafka-1
./scripts/kafka.sh describe txn.transactions | head -5

# 4. Kill two brokers. Now ISR drops below min.insync.replicas, produces fail, and the job enters its
#    exponential-backoff restart loop instead of silently losing data. That is the intended behaviour.
```

Drill 3 is the one worth doing slowly. Watch `./scripts/kafka.sh describe txn.transactions` before and after:
the `Leader:` column changes for about a third of the shards, `Isr:` shrinks to two, and the job never notices.

## Next

[07 — Dual sinks](07-dual-sinks.md)
