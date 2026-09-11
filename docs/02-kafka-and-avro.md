# 02 — Kafka, Avro and Schema Registry

## The topics

Everything in this project reads and writes these. `shards` is Kafka's partition count — the docs call them
shards because that is what they are: the unit both Kafka and Flink split state by.

| Topic | Shards | Key | Kind | Notes |
|---|---|---|---|---|
| `txn.transactions` | 12 | `account_id` | fact | the main stream |
| `txn.auth-events` | 12 | `account_id` | fact | arrives 0.5–40s after its transaction |
| `ref.accounts` | 12 | `account_id` | **compacted** | slowly-changing dimension |
| `ref.merchants` | 6 | `merchant_id` | **compacted** | a *different* key — hence a cross-shard join |
| `ref.fx-rates` | 3 | `currency` | **compacted** | versioned rate table |
| `out.*` | 3–12 | varies | output | written by the DataStream job |
| `sql.*` | 3–12 | — | output | written by the SQL job |

The compacted topics matter more than they look. Compaction means Kafka keeps only the newest record per key, so
the topic *is* a table: replaying it from the beginning reconstructs current state. That is what makes
`upsert-kafka` and `FOR SYSTEM_TIME AS OF` possible, and it is why the dimension sources in the DataStream job
start from the earliest offset.

It also creates a tension worth understanding. A compacted topic is a **snapshot**; an event-time temporal join
needs **history** — the version that was current when the fact happened. Compact too eagerly and older facts stop
resolving, silently, as NULLs. `min.compaction.lag.ms` is what reconciles the two: records younger than it are
never compacted, so that much version history is always available.

```yaml
# charts/flink-stream/values.yaml
kafka:
  defaults:
    minCompactionLagMs: "3600000"   # an hour of version history
```

Set it to comfortably more than how far back your jobs replay. Getting this wrong produced a steady 17% of
`UNKNOWN` regions in this project before it was fixed — see
[docs/09](09-troubleshooting.md#a-temporal-join-resolves-for-most-rows-but-a-steady-fraction-stays-null).

```bash
./scripts/kafka.sh topics                      # shard counts
./scripts/kafka.sh describe txn.transactions   # leader and ISR per shard
./scripts/kafka.sh count txn.transactions      # records per shard
```

## Keys, and which shard a record lands in

Kafka's default partitioner is:

```
partition = (murmur2(keyBytes) & 0x7fffffff) % numPartitions
```

The generator computes exactly this itself and stamps the answer on each record as `shard_id`, so you can check
the theory against reality. In SQL:

```sql
SELECT `partition`, shard_id, COUNT(*)
FROM transactions
GROUP BY `partition`, shard_id;
```

`partition` is a metadata column — Kafka's own answer. `shard_id` is the producer's prediction. They agree on
every row.

(`& 0x7fffffff`, not `Math.abs`. They differ for every negative hash, and `Math.abs(Integer.MIN_VALUE)` is still
negative. `ShardsTest` asserts our implementation against Kafka's real `Utils.murmur2`.)

**Keys are plain UTF-8 strings, not Avro.** Registering a `<topic>-key` schema for what is always one string
buys nothing, and it is the standard Confluent convention. That is why the SQL DDL uses `'key.format' = 'raw'`.
Only values carry Avro schemas.

## Schema Registry

Confluent-framed Avro is: one magic byte, a 4-byte schema id, then the Avro payload. The schema itself lives in
Schema Registry. A consumer that has never seen the writer schema can still decode the record, because it can
fetch the writer schema by id and let Avro's resolution rules bridge it to whatever the consumer expects.

Schemas are registered under `<topic>-value` on first write. There is no migration step in this project — the
topics get their schemas from running the jobs.

```bash
kubectl -n flink-stream port-forward svc/fs-flink-stream-schema-registry 8081:8081 &

curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/txn.transactions-value/versions/latest | jq -r .schema | jq
curl -s localhost:8081/config | jq          # compatibility level
```

The compatibility level is `BACKWARD`, which means *a new schema must be able to read data written with the
previous one*. In practice: you may **add a field with a default** and **remove a field**; you may not add a
required field or change a type. Try it and watch it get rejected:

```bash
curl -s -X POST localhost:8081/compatibility/subjects/txn.transactions-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Transaction\",\"fields\":[{\"name\":\"txn_id\",\"type\":\"string\"},{\"name\":\"brand_new_required\",\"type\":\"string\"}]}"}' | jq
```

## The three Avro rules that shaped these schemas

These are not trivia — each one changed the design, and each one is pinned by a test in
[`AvroLogicalTypeTest`](../flink-jobs/src/test/java/io/flinkstream/avro/AvroLogicalTypeTest.java).

### 1. No Avro enums on any topic SQL reads

`channel`, `region`, `decision`, `severity` are all `string`, with the allowed values documented in
[`Domains`](../flink-jobs/src/main/java/io/flinkstream/common/Domains.java) rather than enforced by the
schema. That looks like a step backwards. It is forced.

Flink SQL's `avro-confluent` format derives its **reader** schema from the table DDL, and the only sane column
type for a symbolic value is `STRING`. Avro's schema-resolution rules do not allow a writer's `enum` to be read
into a reader's `string` — enum is not promotable. So an Avro enum on the wire makes the topic unreadable from
Flink SQL:

```
org.apache.avro.AvroTypeException: Found io.flinkstream.avro.Channel, expecting string
```

The constraint has to live somewhere, so it moved to the serving store: `fraud_alerts` in YugabyteDB has a
`CHECK (severity IN ('INFO','WARN','CRITICAL'))`.

### 2. Record *names* do not have to match, field names do

Flink generates reader schemas with names like `org.apache.flink.avro.generated.record` and `record_geo`, which
match nothing in our schemas. That is fine — Avro resolves records structurally, by field name. So
`Transaction.geo` (an Avro record called `Geo`) reads cleanly into a Flink `ROW<lat DOUBLE, lon DOUBLE>`.

### 3. Logical types work in the generated classes, but not by the route you would guess

`decimal` arrives as `BigDecimal` and `timestamp-millis` as `Instant` with no conversions registered anywhere.
That works because `SpecificDatumWriter` asks the record itself (the generated `getConversion(int)` method)
rather than consulting the global `SpecificData`. The equivalent code over `GenericRecord` does **not** work
without `GenericData.addLogicalTypeConversion` — an asymmetry that costs people an afternoon.

One knock-on effect, handled by [`Money`](../flink-jobs/src/main/java/io/flinkstream/common/Money.java):
`BigDecimal.add` keeps the **larger** scale of its operands, and an Avro `decimal(p,2)` refuses to serialize a
value whose scale is not exactly 2. Every aggregate re-normalizes.

## The schemas themselves

[`flink-jobs/src/main/avro/`](../flink-jobs/src/main/avro/). They are compiled to Java by `avro-maven-plugin`
at build time with `stringType=String` and `enableDecimalLogicalType=true`, so the generated classes use
`String`, `BigDecimal` and `Instant` rather than `CharSequence`, `ByteBuffer` and `long`.

`Transaction` is deliberately the awkward one — it carries a nullable nested record, a map, a decimal and a
timestamp — because those are the four things that break naive Avro handling.

## Watching data flow

```bash
# Avro-decoded, with partition and key
./scripts/kafka.sh tail txn.transactions 5

# What the DataStream job produced
./scripts/kafka.sh tail out.enriched-transactions 5
./scripts/kafka.sh tail out.fraud-alerts 5

# Consumer groups and lag
./scripts/kafka.sh groups
```

If `out.*` looks empty but the job is healthy: the Kafka sinks are `EXACTLY_ONCE`, so records only become
visible to `read_committed` consumers when the checkpoint that produced them commits. At a 30s checkpoint
interval, output lags by up to 30 seconds. That is the cost of end-to-end exactly-once, not a bug.

## Next

[03 — The DataStream labs](03-datastream-labs.md)
