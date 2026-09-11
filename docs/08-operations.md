# 08 — Operations

Everything here is done through the Confluent Flink Kubernetes Operator, which reconciles `FlinkDeployment`
resources the way Kubernetes reconciles Deployments: you describe the desired state and it works out the
stop/savepoint/restart dance.

```bash
kubectl -n flink-stream get flinkdeployment
kubectl -n flink-stream describe flinkdeployment fs-flink-stream-datastream
kubectl -n flink-operator logs -f deploy/flink-kubernetes-operator
```

The two status fields that matter:

| Field | Meaning |
|---|---|
| `status.lifecycleState` | what the **operator** is doing: `DEPLOYED`, `STABLE`, `ROLLING_BACK`, `FAILED` |
| `status.jobStatus.state` | what **Flink** is doing: `RUNNING`, `RESTARTING`, `FAILED`, `FINISHED` |

`STABLE` + `RUNNING` is the healthy pair. `DEPLOYED` + `RECONCILING` means the operator has applied the spec and
is waiting for the job to report in.

## Application mode vs session mode

| | Application (`spec.job` present) | Session (no `spec.job`) |
|---|---|---|
| Cluster per job | yes | no, shared |
| `main()` runs on | the JobManager | the client |
| Blast radius of an OOM | that job only | every job on the cluster |
| Resource accounting | per job | impossible |
| Submit more jobs | no | `FlinkSessionJob`, or the SQL client |

Both are in the chart. Application mode for the two pipelines; the session cluster (off by default) for the
interactive SQL client:

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values --set flink.session.enabled=true
kubectl apply -f k8s-examples/flinksessionjob.yaml   # submit a job into it declaratively
./scripts/sql-client.sh                              # or attach interactively
```

## Changing a job

Any change to `spec` triggers the operator's upgrade flow. With `upgradeMode: savepoint` it stops the job with a
savepoint and restarts from it, so **state survives**.

```bash
# a config change - state survives
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.datastream.args.velocity-count=3

# a rescale - state survives, redistributed across key groups
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.datastream.parallelism=4

# new code
make redeploy
```

Watch it happen:

```bash
kubectl -n flink-stream get flinkdeployment -w
kubectl -n flink-operator logs -f deploy/flink-kubernetes-operator | grep -i savepoint
```

**When state cannot be carried forward** — you removed an operator, changed a UID, or changed a keyed state's
type — set `upgradeMode: stateless` for that one upgrade, or the restore fails with a state-incompatibility error.

## Changing a SQL query

The SQL files are a ConfigMap, so this is not a rebuild:

```bash
vi charts/flink-stream/files/sql/30-windows.sql
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values
kubectl -n flink-stream delete flinkdeployment fs-flink-stream-sql
./scripts/deploy.sh
```

The delete-and-recreate is deliberate. A savepoint restore into a **changed SQL plan** is not generally safe:
the planner assigns operator ids from the plan's structure, so editing a query renames operators and the restore
either fails or silently drops state. Flink's `COMPILED PLAN` feature exists to pin a plan across upgrades; until
you use it, treat a query edit as a stateless redeploy.

## The autoscaler

The operator has a built-in autoscaler that reads source lag and per-vertex busyness and rewrites parallelism
per operator — not per job. It is enabled here in **advisory mode**:

```yaml
job.autoscaler.enabled: "true"
job.autoscaler.scaling.enabled: "false"   # compute and log, change nothing
```

Advisory mode is the right setting for learning what it reacts to:

```bash
kubectl -n flink-operator logs deploy/flink-kubernetes-operator | grep -i autoscaler
kubectl -n flink-stream get cm | grep autoscaler        # it stores its metric history in a ConfigMap
kubectl -n flink-stream get flinkdeployment fs-flink-stream-datastream \
  -o jsonpath='{.metadata.annotations}' | jq
```

Give it something to react to:

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set generator.transactionsPerSecond=200
```

Then let it act:

```bash
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.autoscaler.scaling=true
```

It needs `pipeline.max-parallelism` headroom (120 here) and it will not scale past the source's shard count
usefully — 12 shards means 12 useful source subtasks, no more.

## Savepoints on demand

```bash
kubectl apply -f k8s-examples/flinkstatesnapshot.yaml
kubectl -n flink-stream get flinkstatesnapshot -w
kubectl -n flink-stream get flinkstatesnapshot manual-savepoint -o jsonpath='{.status.path}'
```

Periodic savepoints are already on (hourly, five retained). See [docs/06](06-state-and-checkpoints.md).

## Metrics

The Prometheus reporter is enabled on every Flink pod:

```bash
kubectl -n flink-stream port-forward fs-flink-stream-datastream-taskmanager-1-1 9249:9249
curl -s localhost:9249/metrics | grep -E "numRecordsIn|shardObserver|checkpoint" | head -20
```

Worth watching:

| Metric | Tells you |
|---|---|
| `flink_taskmanager_job_task_numRecordsInPerSecond` | throughput per operator |
| `..._busyTimeMsPerSecond` | how close an operator is to saturated. Over ~900 means it is the bottleneck |
| `..._backPressuredTimeMsPerSecond` | this operator is waiting on a *downstream* one |
| `flink_jobmanager_job_lastCheckpointDuration` | rising means growing state or a slow shuffle |
| `flink_jobmanager_job_numberOfFailedCheckpoints` | any sustained value is a problem — output is not becoming visible |
| `..._currentEmitEventTimeLag` | how far behind real time the pipeline is |
| `shardObserver_*_distinctShardsSeen` | the shuffle, made visible (see [docs/05](05-joins-and-shards.md)) |

The pods carry `prometheus.io/scrape` annotations, so a Prometheus in the cluster picks them up with no extra
configuration.

## Failure drills

See [docs/06](06-state-and-checkpoints.md) for TaskManager, JobManager and broker failures.

## Scaling the environment itself

```bash
make deploy-lite                                            # ~5 GiB: one broker, DataStream job only
helm upgrade fs charts/flink-stream -n flink-stream --reuse-values \
  --set flink.taskManager.resources.memory=3072m \
  --set flink.taskManager.numberOfTaskSlots=4 \
  --set flink.datastream.parallelism=4
```

`scripts/deploy.sh` refuses to proceed if the node does not have room, rather than leaving you with Pending pods
and an "Insufficient memory" event buried three levels down.

## Tearing down

```bash
./scripts/teardown.sh          # remove the release, keep the volumes
./scripts/teardown.sh --all    # volumes, namespace, operator and cert-manager too
```

CRDs are left alone on purpose: deleting them deletes every `FlinkDeployment` in every namespace on the cluster.

## Next

[09 — Troubleshooting](09-troubleshooting.md)
