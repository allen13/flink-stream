# flink-stream-eks

The flink-stream workloads on an EKS cluster that already runs the Confluent Flink Kubernetes Operator.

`charts/flink-stream` installs an entire environment - Kafka, Schema Registry, MinIO, an Iceberg REST fixture,
YugabyteDB - because on a laptop there is nothing else. This chart installs none of that. On AWS every one of
those is a managed service you already have, and what is left is the part that is genuinely yours: the jobs,
their configuration, their AWS identity, and where their state lives.

| | `flink-stream` | `flink-stream-eks` |
|---|---|---|
| Kafka | 3 brokers in the release | MSK / MSK Serverless / Confluent Cloud / your own |
| Schema Registry | in the release | yours, over HTTP(S) |
| Checkpoints, savepoints, HA | one RWO PVC per cluster | S3, one prefix per cluster |
| Iceberg catalog | REST fixture on MinIO | AWS Glue, or any REST catalog |
| Serving store | YugabyteDB pod | Aurora/RDS PostgreSQL, or YugabyteDB |
| AWS credentials | MinIO access keys in env | IRSA, no keys anywhere |
| Flink UI | `kubectl port-forward` | internal ALB Ingress |
| Image | local tag, `IfNotPresent` | ECR, immutable tag |

## Prerequisites

1. **The operator, already installed**, with its CRDs registered:

   ```bash
   kubectl get crd flinkdeployments.flink.apache.org
   ```

   This chart never installs it, never installs cert-manager, and never creates the CRDs. If the operator watches
   specific namespaces (`watchNamespaces` in its own chart), the namespace you release into must be one of them -
   otherwise the FlinkDeployment is created and simply never reconciled, which looks exactly like nothing
   happening.

2. **The job image in ECR.** Build it as usual and push:

   ```bash
   ./scripts/build-image.sh
   aws ecr get-login-password --region us-east-1 \
     | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
   docker tag flink-stream-jobs:1.0.0 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-stream-jobs:1.0.0
   docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-stream-jobs:1.0.0
   ```

   Build on the architecture your nodegroup runs (`--platform linux/amd64` from an Apple Silicon machine, unless
   the nodes are Graviton). Never deploy `:latest`: the operator restarts a JobManager when the *spec* changes,
   and a tag that never changes is a spec that never changes.

3. **An S3 bucket**, and **an IAM role for the pods** (IRSA or Pod Identity). Minimum policy:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "FlinkState",
         "Effect": "Allow",
         "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": "arn:aws:s3:::my-flink-state"
       },
       {
         "Sid": "FlinkStateObjects",
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"],
         "Resource": "arn:aws:s3:::my-flink-state/*"
       },
       {
         "Sid": "IcebergOnGlue",
         "Effect": "Allow",
         "Action": ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
                    "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
                    "glue:DeleteTable"],
         "Resource": "*"
       },
       {
         "Sid": "MskIam",
         "Effect": "Allow",
         "Action": ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster",
                    "kafka-cluster:*Topic*", "kafka-cluster:WriteData", "kafka-cluster:ReadData",
                    "kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup",
                    "kafka-cluster:WriteDataIdempotently"],
         "Resource": "arn:aws:kafka:us-east-1:123456789012:*/my-cluster/*"
       }
     ]
   }
   ```

   The trust policy must name the namespace and ServiceAccount this chart creates, e.g.
   `system:serviceaccount:flink-stream:flink`. Drop the `IcebergOnGlue` statement for a REST catalog and the
   `MskIam` one for any other Kafka auth. `WriteDataIdempotently` is not optional padding - the exactly-once
   Kafka sinks are transactional producers and fail without it.

4. **Network.** The nodes need to reach the brokers, the Schema Registry, the database and S3. An S3 gateway
   endpoint on the VPC is worth having before the first checkpoint, not after: without it every checkpoint byte
   is billed as NAT gateway traffic, and a RocksDB job checkpointing incrementally moves a surprising amount.

## Install

```bash
helm upgrade --install flink-stream charts/flink-stream-eks \
  --namespace flink-stream --create-namespace \
  -f my-values.yaml
```

A minimal `my-values.yaml`:

```yaml
image:
  repository: 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-stream-jobs
  tag: "1.0.0"
aws:
  region: us-east-1
serviceAccount:
  roleArn: arn:aws:iam::123456789012:role/flink-stream-irsa
kafka:
  bootstrapServers: "b-1.mycluster.abc123.c2.kafka.us-east-1.amazonaws.com:9098,b-2..."
  auth: msk_iam
schemaRegistry:
  url: https://schema-registry.internal.example.com
stateStore:
  bucket: my-flink-state
database:
  jdbcUrl: jdbc:postgresql://aurora.cluster-abc.us-east-1.rds.amazonaws.com:5432/flink_stream?sslmode=require
  username: flink
secret:
  existingSecret: flink-stream-credentials    # synced from Secrets Manager
```

`values-msk-iam.yaml` is a fuller worked example (MSK IAM, Glue, Aurora, an internal ALB, Karpenter placement).

Render before you deploy - most mistakes in this chart are visible in the output:

```bash
helm template flink-stream charts/flink-stream-eks -n flink-stream -f my-values.yaml | less
```

## The three things that actually differ from the local chart

### State in S3

Every Flink cluster writes to `s3://<bucket>/<path>/<cluster>/{checkpoints,savepoints,ha}`, one prefix each -
two jobs sharing a checkpoint directory will overwrite each other's metadata. Checkpoint paths carry an
`_entropy_` segment that Flink replaces with four random characters per file, because S3 partitions keys by
their leading characters and a parallelism-12 checkpoint writes every file at the same instant.

The `s3://` scheme comes from a Flink *filesystem plugin*, loaded only from its own directory under
`/opt/flink/plugins`. The image ships the jar in `/opt/flink/opt` but does not enable it; `ENABLE_BUILT_IN_PLUGINS`
(set from `stateStore.builtInPlugins`) makes the entrypoint copy it into place. **The filename must be exact**,
and the Confluent build carries a `-cpN` suffix that the default here does not:

```bash
docker run --rm --entrypoint ls 123456789012.dkr.ecr.us-east-1.amazonaws.com/flink-stream-jobs:1.0.0 /opt/flink/opt
```

Get it wrong and every job dies with `Could not find a file system implementation for scheme 's3'`.

### Identity from the ServiceAccount

Nothing in this chart holds an AWS access key. The ServiceAccount carries `eks.amazonaws.com/role-arn`, the EKS
webhook injects a projected token, and the AWS SDK's default credential chain does the rest - for the S3
filesystem plugin, for Iceberg's `S3FileIO` and Glue client, and for MSK IAM. The token expiry is set to an hour
rather than the 15-minute default so that a TaskManager holding a multipart upload open across a long checkpoint
does not lose its credentials mid-flight.

### Kafka authentication

`kafka.auth` picks a property set (`none`, `msk_iam`, `sasl_scram`, `sasl_plain`, `tls`) and `kafka.properties`
is merged on top of it. Those properties reach three places that must agree, all derived from one helper: the
topics init Job's `--command-config`, the `KAFKA_CLIENT_PROPS` env var, and the `properties.*` options that the
SQL ConfigMap injects into every Kafka table's DDL.

Passwords are never rendered into the ConfigMap. For SCRAM and PLAIN the JAAS string contains a literal
`${KAFKA_SASL_PASSWORD}`, which the SQL runner resolves from the process environment, and the environment gets it
from a Secret.

## Known gaps - read before you trust `kafka.auth`

**The DataStream job and the generator ignore `KAFKA_CLIENT_PROPS`.** The SQL job honours every auth mode,
because its Kafka properties come from DDL this chart generates. The DataStream job builds its sources and sinks
in Java, and `JobConfig.consumerProperties()` / `producerProperties()` return a fixed set. With `kafka.auth: none`
this costs nothing. With any other mode, those two workloads will not authenticate until `JobConfig` merges the
variable, which is about ten lines:

```java
/** Client properties supplied by the deployment, as Java .properties text in KAFKA_CLIENT_PROPS. */
private Properties fromEnvironment() {
    Properties p = new Properties();
    String raw = System.getenv("KAFKA_CLIENT_PROPS");
    if (raw != null && !raw.isBlank()) {
        try {
            p.load(new StringReader(raw));
        } catch (IOException e) {
            throw new IllegalStateException("KAFKA_CLIENT_PROPS is not valid properties text", e);
        }
    }
    return p;
}
```

called at the end of both `consumerProperties()` and `producerProperties()` (`p.putAll(fromEnvironment())`), and
in `KafkaDataGenerator`'s producer config. Until then, either leave `kafka.auth: none` in-VPC or accept that only
the SQL job runs.

**MSK IAM needs a jar that is not in the image.** `sasl.jaas.config` names
`software.amazon.msk.auth.iam.IAMLoginModule`, which lives in `aws-msk-iam-auth`. Add it to the connectors stage
of `flink-jobs/Dockerfile` alongside the others:

```
curl -fsSL -O ${MAVEN_CENTRAL}/software/amazon/msk/aws-msk-iam-auth/2.3.0/aws-msk-iam-auth-2.3.0-all.jar
```

The same applies to the topics init Job: `confluentinc/cp-kafka` has no IAM login module either, so with
`kafka.auth: msk_iam` either point `initJobs.topics.image` at an image that does, or create the topics with
Terraform and set `initJobs.topics.enabled: false`.

**`database.enabled: false` does not remove the JDBC sinks from the SQL labs.** The DDL in `20-sinks.sql` still
declares them and the job will fail to plan. Disabling the database is only useful alongside an edited copy of
the lab SQL.

**`iceberg.name` is referenced by hand in the lab SQL.** `20-sinks.sql` writes to `iceberg.lakehouse.*`. Renaming
the catalog in values without editing that file breaks the SQL job.

## The SQL labs, and why they are duplicated

`files/sql/10-*.sql` through `50-*.sql` are **byte-identical copies** of `charts/flink-stream/files/sql/`. Helm
cannot read files outside a chart directory, so the copy is the price of having two charts. `make lint` diffs
them and fails on drift; `make sync-eks-sql` re-copies them.

`00-catalog.sql` is *not* copied - it is generated from values, because the catalog is the one piece of the lab
SQL that is genuinely different on AWS (Glue or REST, IRSA rather than MinIO keys).

Everything else the SQL needs arrives through the environment, exactly as in the local chart: `SqlRunnerJob`
substitutes `${VAR}` and `${VAR:-default}` from the process environment before executing each file.

## Operating it

`docs/08-operations.md` applies unchanged, with S3 URIs where it says paths.

```bash
# What the operator thinks
kubectl -n flink-stream get flinkdeployment
kubectl -n flink-stream describe flinkdeployment flink-stream-flink-stream-eks-datastream

# Take a savepoint by hand (the FlinkStateSnapshot CRD; see k8s-examples/)
kubectl -n flink-stream get flinkstatesnapshot

# What the autoscaler would do, in advisory mode
kubectl -n flink-operator logs deploy/flink-kubernetes-operator | grep -i autoscaler
```

Turning `flink.autoscaler.scaling` on is the point at which EKS starts to feel different from the laptop: the
operator rewrites parallelism, asks for more TaskManager pods, and Karpenter (or the cluster autoscaler) turns
that into nodes. Watch a rescale once with `scaling: false` and the log lines in front of you before you let it
happen unattended.
