# flink-stream

A hands-on Apache Flink learning project that runs entirely on a local Kubernetes cluster (OrbStack).

It is built around one idea: **almost every interesting thing in stream processing is a consequence of data being
sharded.** Windows, joins, watermarks, state, exactly-once — each of them is doing something specific about the
fact that your data arrives split across partitions, out of order, on several machines at once. So the whole
project is a single sharded dataset with three different processing styles pointed at it, and the labs are
arranged to make the shuffle visible.

```
                                              ┌──────────────────────────┐
  generator ──▶ Kafka (3 brokers, KRaft) ────▶│ Flink                    │──▶ Kafka    (log)
                  12 shards, Avro values      │  · DataStream API        │──▶ Iceberg  (lakehouse)
                       │                      │  · Table API / SQL       │──▶ Yugabyte (serving)
                       ▼                      │  · hybrid of both        │──▶ Snowflake(warehouse, opt-in)
              Confluent Schema Registry       └──────────────────────────┘
                                                Confluent Flink K8s Operator
```

## What is in here

| | |
|---|---|
| **Runtime** | `confluentinc/cp-flink:1.20.5-cp2-java17` — Confluent Platform's build of Apache Flink |
| **Operator** | `confluentinc/cp-flink-kubernetes-operator:1.15.0-cp3` (Confluent chart `flink-kubernetes-operator` 1.150.3) |
| **Streaming** | Kafka 3 brokers in KRaft mode, Confluent Schema Registry, all values Avro |
| **Sinks** | Kafka, **Apache Iceberg** (REST catalog + MinIO), **YugabyteDB** (YSQL) — several queries write to all three, plus an opt-in **Snowflake** sink over JDBC with programmatic-access-token auth |
| **Jobs** | a 10-lab DataStream job, a 6-file SQL job, and a hybrid job that converts between the two |
| **Packaging** | one Helm chart for everything, plus a Maven wrapper so no local JDK or Maven is required |

## Quick start

```bash
make up
```

That runs three steps you can also run individually:

```bash
./scripts/bootstrap.sh     # cert-manager + the Confluent Flink Kubernetes Operator
./scripts/build-image.sh   # builds the job jar inside Docker, on the cp-flink base image
./scripts/deploy.sh        # helm upgrade --install the chart
```

Then:

```bash
./scripts/status.sh        # pods, Flink jobs, topics, registered schemas
./scripts/port-forward.sh  # every web UI at once
```

Expect 4–6 minutes on a cold start: Kafka has to form a KRaft quorum, YugabyteDB initialises its catalog, and
the Flink operator retries the jobs until their dependencies answer.

**Tight on memory?** `make deploy-lite` drops to one broker and one Flink job (~5 GiB instead of ~11 GiB).

## Where to look first

Once it is up, these four commands show the whole thing working:

```bash
# 1. The data is sharded 12 ways, and the generator's shard_id agrees with Kafka's own partitioner.
./scripts/kafka.sh count txn.transactions

# 2. The DataStream job is enriching, joining and alerting.
./scripts/kafka.sh tail out.fraud-alerts 5

# 3. The SQL job aggregated across every shard - watch shards_touched reach 12.
./scripts/ysql.sh "SELECT region, window_start, txn_count, shards_touched
                   FROM region_revenue ORDER BY window_start DESC LIMIT 10;"

# 4. The same rows also landed in Iceberg, as Parquet in object storage.
kubectl -n flink-stream port-forward svc/fs-flink-stream-iceberg-rest 8181:8181 &
curl -s localhost:8181/v1/namespaces/lakehouse/tables | jq
```

## The labs

### DataStream API — `flink-jobs/src/main/java/io/flinkstream/datastream/TransactionPipelineJob.java`

Ten labs in one job graph, because a Flink job is a graph rather than a pipeline: one source can feed many
branches, and they all share one set of checkpoints.

| Lab | Concept | Where |
|-----|---------|-------|
| A | Broadcast state | [`MerchantEnrichmentFunction`](flink-jobs/src/main/java/io/flinkstream/functions/MerchantEnrichmentFunction.java) |
| B | Async I/O | [`AsyncRiskScoreFunction`](flink-jobs/src/main/java/io/flinkstream/functions/AsyncRiskScoreFunction.java) |
| C | Interval join | [`AuthJoinFunction`](flink-jobs/src/main/java/io/flinkstream/functions/AuthJoinFunction.java) |
| D | Tumbling windows, lateness, side outputs | [`TransactionAggregate`](flink-jobs/src/main/java/io/flinkstream/functions/TransactionAggregate.java) + [`WindowStatsFunction`](flink-jobs/src/main/java/io/flinkstream/functions/WindowStatsFunction.java) |
| E | Session (merging) windows | same, via `EventTimeSessionWindows` |
| F | Keyed state, event-time timers, state TTL | [`VelocityRuleFunction`](flink-jobs/src/main/java/io/flinkstream/functions/VelocityRuleFunction.java) |
| G | CEP | [`EscalatingSpendPattern`](flink-jobs/src/main/java/io/flinkstream/functions/EscalatingSpendPattern.java) |
| H | Hand-written versioned join | [`AccountJoinFunction`](flink-jobs/src/main/java/io/flinkstream/functions/AccountJoinFunction.java) |
| I | Cross-shard aggregation | `labI_crossShardAggregate` |
| J | Custom metrics | [`ShardObserver`](flink-jobs/src/main/java/io/flinkstream/functions/ShardObserver.java) |

### Table API / SQL — `charts/flink-stream/files/sql/`

| File | Covers |
|------|--------|
| [`00-catalog.sql`](charts/flink-stream/files/sql/00-catalog.sql) | Iceberg REST catalog |
| [`10-sources.sql`](charts/flink-stream/files/sql/10-sources.sql) | `avro-confluent` DDL, `upsert-kafka`, watermarks, metadata + computed columns, UDF registration |
| [`20-sinks.sql`](charts/flink-stream/files/sql/20-sinks.sql) | Kafka / Iceberg / JDBC sinks, append vs updating changelog modes, JDBC lookup source |
| [`30-windows.sql`](charts/flink-stream/files/sql/30-windows.sql) | `TUMBLE` window TVF, Window Top-N, regular `GROUP BY`, mini-batch, two-phase aggregation, **one view → three sinks** |
| [`40-joins.sql`](charts/flink-stream/files/sql/40-joins.sql) | interval / temporal / lookup / window joins, side by side |
| [`50-patterns.sql`](charts/flink-stream/files/sql/50-patterns.sql) | `MATCH_RECOGNIZE`, with a UDF in `DEFINE` |

These live in the chart, not in the image — they reach the job through a ConfigMap, so changing a query is a
`helm upgrade`, not a rebuild.

### Hybrid — `flink-jobs/src/main/java/io/flinkstream/hybrid/HybridTableJob.java`

DataStream → Table → SQL → DataStream in one job, and the difference between `toDataStream` (append-only) and
`toChangelogStream` (gives you the `RowKind`). Off by default: `--set flink.hybrid.enabled=true`.

## Documentation

| | |
|---|---|
| [01 — Setup and architecture](docs/01-setup.md) | what runs where, and why each piece is there |
| [02 — Kafka, Avro and Schema Registry](docs/02-kafka-and-avro.md) | the contract layer, and the Avro rules that shaped these schemas |
| [03 — The DataStream labs](docs/03-datastream-labs.md) | lab by lab, with what to look at in the UI |
| [04 — The SQL labs](docs/04-sql-labs.md) | same, for SQL, plus using the SQL client interactively |
| [05 — Joins and shards](docs/05-joins-and-shards.md) | the core idea: what a shuffle costs and how to see it |
| [06 — State, checkpoints and savepoints](docs/06-state-and-checkpoints.md) | what is in state, and how to move it |
| [07 — Dual sinks: Iceberg, YugabyteDB and Snowflake](docs/07-dual-sinks.md) | why several, what each one is good at, and how a job holds a credential |
| [08 — Operations](docs/08-operations.md) | rescaling, upgrades, the autoscaler, failure drills |
| [09 — Troubleshooting](docs/09-troubleshooting.md) | the errors you will actually hit, and what they mean |
| [Shard to sink](https://allen13.github.io/flink-stream/) | an animated job graph: dispatch a scenario and watch the payloads move (source: [docs/shard-to-sink.html](docs/shard-to-sink.html)) |

A standalone write-up of the aggregation and join machinery, with measurements taken from this environment
while it ran, is in [docs/pdf/](docs/pdf/aggregation-joins-and-the-operator.md). Render it with `make pdf`.

## Repository layout

```
charts/flink-stream/         Helm chart: Kafka, Schema Registry, Yugabyte, MinIO, Iceberg, generator, Flink jobs
  files/sql/                 the SQL labs (mounted into the SQL job as a ConfigMap)
  files/yugabyte-schema.sql  the serving schema, with YSQL HASH/ASC sharding directives
  files/snowflake-schema.sql the warehouse schema, plus the service user, policies and PAT that reach it
  values.yaml                fully commented; values-lite.yaml is the small profile
charts/flink-stream-eks/     the same Flink jobs on EKS: S3 state, IRSA, MSK, Glue - and no data infrastructure,
                             because on AWS you already have it. See its README.md.
flink-jobs/                  the Maven module
  src/main/avro/             the Avro schemas - the contracts for every topic
  src/main/java/.../common/  Kafka + Avro plumbing, watermarks, config
  src/main/java/.../functions/  one class per Flink concept
  src/main/java/.../datastream/ the DataStream job
  src/main/java/.../sql/     the SQL runner
  src/main/java/.../udf/     scalar, table and aggregate UDFs
  src/main/java/.../generator/  the data generator
  Dockerfile                 3-stage build on confluentinc/cp-flink
  src/test/java/             unit, operator-harness and MiniCluster tests
docs/                        the written labs
k8s-examples/                FlinkStateSnapshot and FlinkSessionJob, applied by hand
scripts/                     bootstrap, build, deploy, inspect, tear down
```

## Tests

```bash
make test
```

34 tests, no cluster needed — they run in about 15 seconds:

| | |
|---|---|
| `AvroLogicalTypeTest` | pins the three Avro rules the schema design rests on, including why no schema uses an enum |
| `ShardsTest` | asserts our partitioner against Kafka's real `Utils.murmur2`, and that account keys spread over all 12 shards |
| `MoneyTest` | the `BigDecimal` scale rule that Avro decimals require |
| `SqlScriptTest` | statement splitting, and that every shipped lab file parses |
| `VelocityRuleFunctionTest` | the timer-and-TTL logic, through an operator test harness |
| `WindowAggregationTest` | windows, lateness tiers and side outputs, on a MiniCluster |
| `SqlSemanticsTest` | the UDFs, window TVFs, Window Top-N and `MATCH_RECOGNIZE` |

## Requirements

- OrbStack with Kubernetes enabled (or any local cluster: kind, minikube, Docker Desktop)
- `kubectl`, `helm` 3.8+, `docker`
- About 12 GiB of memory available to the cluster for the full profile, 6 GiB for `--lite`
- No local JDK or Maven needed — the image build carries its own. A JDK 17 is only needed for `make test`.
