# 01 — Setup and architecture

## What you need

- A local Kubernetes cluster. OrbStack is what this was built against; kind, minikube and Docker Desktop work
  too. The one OrbStack-specific convenience is that **images built with `docker build` are immediately visible
  to the Kubernetes node**, so the chart can use `flink-stream-jobs:1.0.0` with `imagePullPolicy: IfNotPresent`
  and no registry. On kind you need `kind load docker-image flink-stream-jobs:1.0.0` after each build; on
  minikube, `eval $(minikube docker-env)` before it.
- `kubectl`, `helm` 3.8 or newer, `docker`.
- Memory: roughly 12 GiB for the full profile, 6 GiB for `--lite`. Check what your VM actually has:

  ```bash
  kubectl get node -o jsonpath='{.items[0].status.allocatable}' | jq
  ```

You do **not** need a JDK or Maven. The image build runs Maven inside a container. A JDK 17 is only needed if you
want to run `make test` or open the project in an IDE.

## The three steps

```bash
make up
```

is these, and it is worth knowing what each one does.

### 1. `scripts/bootstrap.sh` — cluster prerequisites

Installs **cert-manager**, then the **Confluent Flink Kubernetes Operator**.

cert-manager is there for one specific reason: the operator ships a validating/mutating admission webhook, and
its `MutatingWebhookConfiguration` carries a `cert-manager.io/inject-ca-from` annotation. Without cert-manager
the webhook has no certificate, the API server refuses to call it, and every `FlinkDeployment` you create is
rejected. If you would rather not run cert-manager, install the operator with `--set webhook.create=false` — the
operator itself works fine, you just lose admission-time defaulting and validation, so mistakes surface later in
the operator log instead of at `kubectl apply`.

The operator is Confluent's build of the Apache Flink Kubernetes Operator:

```
chart:    confluentinc/flink-kubernetes-operator  1.150.3
image:    confluentinc/cp-flink-kubernetes-operator:1.15.0-cp3
```

It is installed cluster-scoped (`watchNamespaces` left empty), so it reconciles `FlinkDeployment` resources in
any namespace. It registers four CRDs:

| CRD | What it is for |
|-----|----------------|
| `flinkdeployments` | a Flink cluster, optionally with a job. The main one. |
| `flinksessionjobs` | a job submitted into an existing session cluster |
| `flinkstatesnapshots` | an on-demand savepoint or checkpoint, as a Kubernetes object |
| `flinkbluegreendeployments` | run two versions side by side and cut over |

### 2. `scripts/build-image.sh` — the job image

A three-stage Docker build (`flink-jobs/Dockerfile`):

1. **build** — `maven:3.9-eclipse-temurin-17` compiles the Avro schemas into Java, compiles the jobs, and shades
   everything into one jar.
2. **connectors** — downloads the jars that belong in `/opt/flink/lib`. This is a separate stage because the
   Confluent runtime image is minimal and has no `curl`.
3. **runtime** — `confluentinc/cp-flink:1.20.5-cp2-java17`, with the job jar at
   `/opt/flink/usrlib/flink-stream-jobs.jar` and the connectors in `lib/`.

The split between "bundled in the job jar" and "installed in `/opt/flink/lib`" is not arbitrary:

| Where | What | Why |
|-------|------|-----|
| job jar | `flink-connector-kafka`, `flink-avro`, `flink-avro-confluent-registry`, `avro`, `kafka-clients` | the DataStream code compiles against the unshaded APIs |
| `/opt/flink/lib` | `flink-sql-connector-kafka`, `flink-sql-avro-confluent-registry` | so the **SQL client** can create Kafka tables with no user jar at all |
| `/opt/flink/lib` | `flink-connector-jdbc`, `postgresql` | `DriverManager` is loaded by the bootstrap classloader and cannot see a driver that only exists in a child classloader |
| `/opt/flink/lib` | `iceberg-flink-runtime`, `iceberg-aws-bundle` | shaded uber jars; bundling them *as well* would put two copies of Iceberg on the classpath |
| *not bundled* | `flink-cep`, `flink-table-*`, `flink-streaming-java` | already in `cp-flink`'s `lib/`. Bundling a second copy is the classic cause of `LinkageError` at runtime |

Note that `flink-cep` being in `lib/` is a **Confluent-specific** detail: vanilla `apache/flink` keeps CEP in
`/opt/flink/opt`, where it is *not* on the classpath, and a job using CEP has to bundle it. That is exactly the
kind of thing that makes a job work on one distribution and fail on another.

### 3. `scripts/deploy.sh` — the chart

One `helm upgrade --install`. The chart brings up everything else and then three post-install hooks create the
Kafka topics, apply the YugabyteDB schema, and make the MinIO bucket. The hooks wait for their dependencies, so
a cold start takes a few minutes.

## What is running

```
namespace: flink-operator
  flink-kubernetes-operator        watches FlinkDeployments everywhere

namespace: cert-manager
  cert-manager, -webhook, -cainjector

namespace: flink-stream
  fs-flink-stream-kafka-0..2       3 brokers, KRaft combined mode (broker + controller in one process)
  fs-flink-stream-schema-registry  the Avro contract store (backed by a compacted Kafka topic)
  fs-flink-stream-kafka-ui         browse topics and decoded Avro messages
  fs-flink-stream-yugabyte-0       single-node yugabyted (YSQL on 5433)
  fs-flink-stream-minio            S3-compatible object storage
  fs-flink-stream-iceberg-rest     Iceberg REST catalog
  fs-flink-stream-generator        the data producer (a plain Kafka producer, not a Flink job)
  fs-flink-stream-datastream-*     JobManager + TaskManagers for the DataStream job
  fs-flink-stream-sql-*            JobManager + TaskManagers for the SQL job
```

## Design choices worth knowing about

**Three Kafka brokers, not one.** The entire project is about shards, and one broker cannot show you leadership
distribution, replication, or what happens to a transactional sink when a broker dies. Replication factor 3 with
`min.insync.replicas=2` is what makes those drills meaningful. `--lite` drops to one broker and loses them.

**`auto.create.topics.enable=false`.** Auto-created topics get one partition. Every sharding lab in this project
would silently become a single-shard lab.

**Application mode, not session mode, for the jobs.** Each `FlinkDeployment` with a `job` block is its own
cluster: the job's `main()` runs on the JobManager and nothing else shares those TaskManagers. A failing job
cannot take its neighbours down, and resources are accounted per job. The session cluster exists too (off by
default) because that is what the interactive SQL client attaches to — and comparing the two is instructive.

**One PVC per Flink cluster for checkpoints.** In production `state.checkpoints.dir` points at S3 or GCS. On a
single-node cluster a `ReadWriteOnce` local-path volume is the closest equivalent: RWO means "one *node*", and
since JobManager and TaskManagers all land on the one node they can share it. This does **not** work on a
multi-node cluster — there you need real object storage.

**The generator is not a Flink job.** It is a plain `KafkaProducer` in a `Deployment`, sharing the job image
only for its jar. Keeping it out of Flink means a lab that is not working is unambiguously the job's fault.

## Next

[02 — Kafka, Avro and Schema Registry](02-kafka-and-avro.md)
