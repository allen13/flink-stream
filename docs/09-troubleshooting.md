# 09 — Troubleshooting

Every entry below is a failure this project actually hit while being built, with the symptom you would see and
the reason underneath. They are grouped by where they bite.

---

## Cluster and deployment

### Pods stuck `Pending`, "Insufficient memory"

```
0/1 nodes are available: 1 Insufficient memory.
```

The full profile wants roughly 11 GiB of requests plus ~1 GiB for the operator. `scripts/deploy.sh` checks this
before installing and tells you, but if you hit it anyway:

```bash
kubectl get node -o jsonpath='{.items[0].status.allocatable.memory}'
kubectl get pods -A -o custom-columns='NS:.metadata.namespace,NAME:.metadata.name,MEM:.spec.containers[*].resources.requests.memory'
```

Options: `make deploy-lite`, raise the VM's memory (OrbStack → Settings → Resources), or stop other workloads.

Note the Flink operator's chart defaults to a **3 GiB** request. `scripts/bootstrap.sh` overrides it to 1 GiB;
if you installed the operator by hand, check it.

### Kafka brokers never become ready; `UnknownHostException` for their peers

```
WARN [RaftManager id=0] Error connecting to node ...-kafka-1...:9093
java.net.UnknownHostException: ...-kafka-1...
```

A KRaft quorum forms only once a majority of controllers can reach each other, so **no single broker can become
Ready on its own**. With the StatefulSet default `podManagementPolicy: OrderedReady`, pods 1 and 2 are never
created because pod 0 is not Ready — and pod 0 is waiting for them. Deadlock.

The chart uses `podManagementPolicy: Parallel` for exactly this. It also uses a `startupProbe` rather than a long
`initialDelaySeconds` on liveness, because quorum formation can outlast the liveness probe and get the broker
killed mid-election.

### `helm upgrade` fails with "another operation is in progress"

A previous install is stuck, usually on a hook waiting for a dependency that never came up. That is why the init
Jobs in this chart are **ordinary Jobs, not Helm hooks** — a hook blocks the release. If you are wedged anyway:

```bash
helm uninstall fs -n flink-stream --wait
kubectl -n flink-stream delete job --all
./scripts/deploy.sh
```

### `retention.ms: Not a number of type LONG`, value shown as `6.048e+08`

Helm round-trips large unquoted YAML integers through a float. Quote them in `values.yaml`
(`retentionMs: "604800000"`) — the chart also passes them through `| int64` as a belt-and-braces.

### YugabyteDB stays `0/1`; `YSQL Status: Not Ready` forever

Two distinct causes, both fixed in the chart:

**1. The advertise address is too long.** YSQL embeds PostgreSQL, which truncates `listen_addresses` at
`NAMEDATALEN-1 = 63` characters. The natural StatefulSet FQDN is 81:

```
FATAL:  could not create any TCP/IP sockets
could not translate host name "fs-flink-stream-yugabyte-0.fs-flink-stream-yugabyte-headless.fl"
```

Note the name cut off mid-word. The chart uses a short headless service (`-yb`) and the two-label form, and
`fail`s at template time if the result would exceed 63 characters.

**2. The advertise address is a pod IP.** `yugabyted` writes it into the universe metadata under `--base_dir`.
The IP changes on every restart, the persisted universe still names the old one, and a volume that worked
yesterday never comes back. Use the StatefulSet's stable DNS name.

If you hit either on an existing volume, the metadata is already poisoned:

```bash
kubectl -n flink-stream delete sts fs-flink-stream-yugabyte
kubectl -n flink-stream delete pvc data-fs-flink-stream-yugabyte-0
./scripts/deploy.sh
```

---

## Image and classpath

### `NullPointerException` in `AvroDeserializationSchema.getProducedType`

```
java.lang.NullPointerException
  at org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo.<init>
  at org.apache.flink.formats.avro.AvroDeserializationSchema.getProducedType
```

Caused by putting **`flink-sql-avro-confluent-registry` in `/opt/flink/lib`**.

That jar relocates Avro into its own package, but it also carries ~100 *unrelocated*
`org.apache.flink.formats.avro.*` classes compiled against the relocated copy. In `lib` they shadow the
identically-named classes the job jar needs. `AvroDeserializationSchema` then evaluates

```java
SpecificRecord.class.isAssignableFrom(recordClazz)
```

with the **shaded** `SpecificRecord` against our unshaded generated `Transaction`, concludes the record is
generic, and dereferences a null schema.

`flink-sql-connector-kafka` has the same shape. Neither is installed in this image. The deployed jobs bundle the
unshaded connectors; the SQL client gets the same coverage from
`sql-client.sh -j /opt/flink/usrlib/flink-stream-jobs.jar`.

### `NoClassDefFoundError: org/apache/hadoop/conf/Configuration`

Iceberg's Flink catalog constructs a Hadoop `Configuration` even for a REST catalog on S3, and
`iceberg-flink-runtime` treats Hadoop as provided. The Dockerfile adds `flink-shaded-hadoop-2-uber`.

### `NoClassDefFoundError: io/confluent/kafka/schemaregistry/rules/RuleConditionException`

Version skew. `flink-avro-confluent-registry` pulls `kafka-schema-registry-client:7.5.3`, and Maven's
nearest-wins resolution lets that override the newer client a `kafka-avro-serializer:7.9.x` needs. Align them:

```bash
cd flink-jobs && ./mvnw dependency:tree | grep schema-registry-client
```

and set `<confluent.version>` in `pom.xml` to whatever Flink pulls in.

### `base name (${CP_FLINK_IMAGE}) should not be blank`

An `ARG` used in a `FROM` must be declared **before the first `FROM`** — that is the only scope Docker resolves
base-image names from.

### `curl: command not found` during the image build

The Confluent runtime image is minimal. Download in an earlier stage and `COPY` the result.

### The image builds but Kubernetes cannot find it

OrbStack shares its Docker image store with its Kubernetes node, so a locally built tag just works. Other local
clusters do not:

```bash
kind load docker-image flink-stream-jobs:1.0.0     # kind
eval $(minikube docker-env)                        # minikube: build inside its daemon
```

---

## Avro and Schema Registry

### `AvroTypeException: Found Channel, expecting string`

An Avro **enum** cannot be read into a `string` reader schema, and Flink SQL's `avro-confluent` format derives
its reader schema from the table DDL — where a symbolic column can only be `STRING`. So an enum on the wire makes
the topic unreadable from Flink SQL.

None of the schemas in this project use enums; the allowed values live in
[`Domains`](../flink-jobs/src/main/java/io/flinkstream/common/Domains.java) and are enforced by a CHECK
constraint in the serving schema. Pinned by
[`AvroLogicalTypeTest`](../flink-jobs/src/test/java/io/flinkstream/avro/AvroLogicalTypeTest.java).

### `SerializationException: Unknown magic byte!`

Something is trying to Avro-decode a value that has no Confluent framing — nearly always the **key**. This
project writes plain UTF-8 keys and Avro values, but `kafka-avro-console-consumer` defaults to Avro for both:

```bash
kafka-avro-console-consumer ... \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

`scripts/kafka.sh tail` already does this.

### An Avro decimal fails to serialize

`BigDecimal.add` keeps the **larger** scale of its operands, and an Avro `decimal(p,2)` refuses a value whose
scale is not exactly 2. Every aggregate in this project re-normalizes through
[`Money`](../flink-jobs/src/main/java/io/flinkstream/common/Money.java).

---

## Jobs producing nothing, or the wrong thing

### A windowed job produces no output at all

Almost always **watermarks**, and almost always idleness. A source subtask reading several shards emits the
*minimum* watermark across them, so one quiet shard freezes event time for the whole job and no window ever
fires. On a lightly loaded local cluster that is the normal state.

```java
WatermarkStrategy.forBoundedOutOfOrderness(...).withIdleness(Duration.ofSeconds(15))
```
```sql
SET 'table.exec.source.idle-timeout' = '15 s';
```

Both are set here. Check the watermark is actually moving: Flink UI → the operator → *Watermarks*. A watermark of
`-9223372036854775808` means none has been emitted at all.

### A temporal join returns NULL for everything

```sql
COALESCE(a.region, 'UNKNOWN')   -- comes out 'UNKNOWN' for every row
```

`FOR SYSTEM_TIME AS OF t.event_time` can only answer "as of T" if the versioned table holds a version stamped at
or before T. If the dimension rows were written with `updated_at = now` and the job is replaying a backlog of
older facts, **no version is valid for any of them**.

This is why the generator backdates its dimension seed by 24 hours. In a real system the equivalent question is
"does my dimension topic go back far enough to cover my fact retention?" — and the answer is usually no until
someone checks.

If you are debugging one: compare the timestamps directly.

```bash
./scripts/kafka.sh tail ref.accounts 1     # look at updated_at
./scripts/kafka.sh tail txn.transactions 1 # look at event_time
```

### A temporal join resolves for *most* rows but a steady fraction stays NULL

Different cause, same symptom, and a subtler one: **compaction is eating the version history.**

A compacted topic is a *snapshot* — compaction keeps only the newest record per key. An event-time temporal join
needs the opposite: the version that was current when the fact happened. So every time a dimension row is
updated, the older version becomes eligible for deletion, and facts older than that update stop resolving.

This project hit it with an aggressive compaction config (`min.cleanable.dirty.ratio=0.1`, `segment.ms=60000`):
about 17% of `region_revenue` rows came out as `UNKNOWN`, steadily, across every window. The fix is
`min.compaction.lag.ms`, which protects records younger than it from compaction entirely:

```
cleanup.policy=compact
min.compaction.lag.ms=3600000     # an hour of version history is always available
min.cleanable.dirty.ratio=0.5     # the default; 0.1 compacts far too eagerly
```

It should comfortably exceed how far back your jobs replay. Check what a topic is actually configured with:

```bash
./scripts/kafka.sh describe ref.accounts
```

A residual fraction well under 1% is normal and is the genuine arrival-order case: a transaction processed
before its account record was consumed. That is what Lab H's `pendingState` buffer exists for on the DataStream
side.

### `ProducerFencedException: There is a newer producer with the same transactionalId`

Two exactly-once Kafka sinks share a transactional-id prefix. Flink derives ids as
`<prefix>-<subtask>-<checkpoint>`, so two sink operators writing the same table generate identical ids and Kafka
fences one — then the job restart-loops.

Causes: two `INSERT`s into the same Kafka table (union them instead, as `50-patterns.sql` does), or two
DataStream sinks sharing `setTransactionalIdPrefix`. `Kafka.sink()` takes a `sinkId` per sink for this reason.

### `Greedy quantifiers are not allowed as the last element of a Pattern yet`

`MATCH_RECOGNIZE` with `PATTERN (a b{2,})`. A greedy final quantifier can never decide it is finished, so the
match could only be emitted at end of stream. Make it reluctant: `b{2,}?`.

### A `MATCH_RECOGNIZE` pattern silently never fires

`LAST(x, 1)` is NULL on the first row matched to its classifier, so a predicate like `b.amount > LAST(b.amount, 1)`
evaluates to NULL — never true. `COALESCE` it against an earlier classifier.

### "does not support consuming update changes"

You pointed an **updating** query (regular `GROUP BY`, Top-N, regular join) at an append-only sink (Iceberg, or a
plain `kafka` table). Either use a window TVF so the result is append-only, or sink to something with a primary
key. See [docs/04](04-sql-labs.md#20--sinks-and-changelog-modes).

### Output topics look empty but the job is healthy

The Kafka sinks are `EXACTLY_ONCE`: records become visible to `read_committed` consumers only when the checkpoint
that produced them commits. At a 30-second checkpoint interval the output lags by up to 30 seconds. Check for
actual records rather than trusting a consumer that has not caught up:

```bash
./scripts/kafka.sh count out.enriched-transactions
```

### A Flink job fails immediately on a cold cluster

In application mode the job's `main()` runs on the JobManager and connects to Kafka, Schema Registry, Iceberg and
YugabyteDB straight away. A dependency that is not up yet throws in `main()` — which is a *deployment* failure,
not a job failure, so Flink's restart strategy does not apply. The chart handles this two ways: a
`wait-for-dependencies` initContainer, and `kubernetes.operator.job.restart.failed: "true"`.

### Snowflake: `Programmatic access token is invalid` / `JWT token is invalid`

The token itself is fine more often than not — one of the two policy gates is shut. `LOGIN_HISTORY` names which:

```sql
SELECT EVENT_TIMESTAMP, AUTHENTICATION_METHOD, IS_SUCCESS, ERROR_MESSAGE, CLIENT_IP
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE USER_NAME = 'FLINK_STREAM_SVC' ORDER BY EVENT_TIMESTAMP DESC LIMIT 20;
```

- **`CLIENT_IP` is not what you expected** — the network policy is rejecting it. Snowflake sees your cluster's
  *egress* address (a NAT gateway or egress IP), never the pod IP. Put the address from this column into
  `ALLOWED_IP_LIST`.
- **Authentication method refused** — the user's authentication policy does not list
  `PROGRAMMATIC_ACCESS_TOKEN`.
- **Nothing at all in `LOGIN_HISTORY`** — the request never arrived. Check the account identifier in
  `snowflake.account`, and whether the cluster has egress to `*.snowflakecomputing.com` at all.
- **The token expired.** `SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER FLINK_STREAM_SVC;` — PATs have a
  `DAYS_TO_EXPIRY` and do exactly that, quietly, on a Sunday.

### Snowflake: connects, then `No active warehouse selected in the current session`

The role reached Snowflake but the warehouse did not. `warehouse=` is a JDBC URL parameter, not a driver default,
and it is dropped from the URL when `snowflake.warehouse` is empty. Check what the job actually built:

```bash
kubectl -n flink-stream exec deploy/fs-flink-stream-datastream -- env | grep SNOWFLAKE_
```

The same shape of error with `Object does not exist, or operation cannot be performed` on the first INSERT means
the *role* is wrong instead: `snowflake.role` has to match the `ROLE_RESTRICTION` the PAT was issued with, and
that role needs `GRANT INSERT` on both tables.

### Snowflake: `No suitable driver found for jdbc:snowflake://`

The driver is not on the parent classloader. `java.sql.DriverManager` is loaded by the bootstrap classloader and
cannot see anything that exists only in the job jar, which is why `snowflake-jdbc` is `provided` in `pom.xml` and
downloaded into `/opt/flink/lib` by the Dockerfile instead. If the image predates the Snowflake sink, rebuild it:

```bash
./scripts/build-image.sh
kubectl -n flink-stream exec deploy/fs-flink-stream-datastream -- ls /opt/flink/lib | grep snowflake
```

### The generator logs nothing at all

It runs as a bare `java -cp`, so log4j2 falls back to its built-in ERROR-only console config. The chart passes
`-Dlog4j.configurationFile=file:/opt/flink/conf/log4j-console.properties`.

---

## Useful one-liners

```bash
./scripts/status.sh                                   # everything at a glance
./scripts/kafka.sh groups                             # consumer lag per job
./scripts/kafka.sh count <topic>                      # records per shard
./scripts/kafka.sh tail <topic> 5                     # Avro-decoded records
./scripts/ysql.sh "SELECT ..."                        # query the serving store

kubectl -n flink-stream get flinkdeployment -o wide
kubectl -n flink-stream describe flinkdeployment fs-flink-stream-sql | tail -40
kubectl -n flink-operator logs -f deploy/flink-kubernetes-operator
kubectl -n flink-stream logs deploy/fs-flink-stream-sql --tail=300 | grep -A20 "Caused by"
kubectl -n flink-stream get events --sort-by=.lastTimestamp | tail -20
```

The single most useful one when a Flink job misbehaves: the **JobManager** log, not the TaskManager log. Planning
errors, savepoint problems and `main()` failures all land there.
