# syntax=docker/dockerfile:1.7
#
# Three stages:
#   1. build      Maven + JDK 17, produces the shaded job jar. Nothing but a Docker daemon is needed locally.
#   2. connectors downloads the jars that belong in /opt/flink/lib.
#   3. runtime    Confluent's Flink distribution with the jar and the connectors in place.
#
# The runtime base is confluentinc/cp-flink, Confluent Platform's build of Apache Flink. It is the image the
# Confluent Flink Kubernetes Operator expects: the same layout as apache/flink, with Confluent's patch level
# (the "-cpN" suffix), Confluent's support lifecycle, and CEP already present in /opt/flink/lib.
#
# It is also deliberately minimal - no curl, no package manager in the final layer - which is why every download
# happens in an earlier stage and arrives here as a COPY.

# An ARG used in a FROM must be declared before the first FROM: that is the only scope Docker resolves
# base-image names from.
ARG CP_FLINK_IMAGE=confluentinc/cp-flink:1.20.5-cp2-java17

# ----------------------------------------------------------------------------- 1. build the job jar
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /src

# Resolve dependencies from the POM alone first, so editing Java does not re-download the world.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q clean package -DskipTests
RUN cp target/flink-stream-jobs-*.jar /flink-stream-jobs.jar

# ----------------------------------------------------------------------------- 2. collect /opt/flink/lib jars
FROM maven:3.9-eclipse-temurin-17 AS connectors

ARG FLINK_VERSION=1.20.5
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG JDBC_CONNECTOR_VERSION=3.3.0-1.20
ARG POSTGRES_DRIVER_VERSION=42.7.5
# Keep in sync with <snowflake.jdbc.version> in pom.xml.
ARG SNOWFLAKE_JDBC_VERSION=4.3.4
# Must match the Iceberg REST catalog image in charts/flink-stream/values.yaml.
ARG ICEBERG_VERSION=1.10.1
# Iceberg's Flink catalog constructs a org.apache.hadoop.conf.Configuration even for a REST catalog on S3, and
# iceberg-flink-runtime treats Hadoop as provided. This uber jar is the usual way to satisfy that without
# dragging in thirty Hadoop artefacts.
ARG FLINK_SHADED_HADOOP_VERSION=2.8.3-10.0
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

# ---- jars for /opt/flink/lib: available to every job and to the SQL client, on the parent classloader ------
WORKDIR /libs
RUN set -eux; \
    # --- sink 1: YugabyteDB over the PostgreSQL wire protocol ----------------------------------------------
    # YSQL is Postgres-compatible, so the stock driver and Flink's Postgres dialect work unchanged. The driver
    # must live in /opt/flink/lib rather than in the job jar: java.sql.DriverManager is loaded by the bootstrap
    # classloader and will not see a driver that only exists in a child classloader.
    curl -fsSL -O ${MAVEN_CENTRAL}/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar; \
    curl -fsSL -O ${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar; \
    \
    # --- sink 2: Apache Iceberg through a REST catalog on S3 (MinIO) ---------------------------------------
    # iceberg-flink-runtime is a shaded uber jar meant for /opt/flink/lib; it must NOT also be bundled in a job
    # jar. iceberg-aws-bundle supplies S3FileIO plus the AWS SDK that talks to MinIO.
    curl -fsSL -O ${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar; \
    curl -fsSL -O ${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSL -O ${MAVEN_CENTRAL}/org/apache/flink/flink-shaded-hadoop-2-uber/${FLINK_SHADED_HADOOP_VERSION}/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar; \
    \
    # --- sink 3: Snowflake over its own JDBC driver (PAT auth) --------------------------------------------
    # No Flink connector exists for Snowflake and none is needed: the DataStream JdbcSink takes raw SQL, so any
    # JDBC driver works. This is the shaded fat jar and, like the Postgres driver, it has to be on the parent
    # classloader for DriverManager to find it.
    #
    # It is ~105 MB, which is most of what this image adds on top of cp-flink, and it is downloaded even though
    # the sink is off by default. That is the deliberate trade: enabling Snowflake is then a `helm upgrade`,
    # not a rebuild-and-reload cycle, and image size is disk rather than the memory this cluster is short of.
    curl -fsSL -O ${MAVEN_CENTRAL}/net/snowflake/snowflake-jdbc/${SNOWFLAKE_JDBC_VERSION}/snowflake-jdbc-${SNOWFLAKE_JDBC_VERSION}.jar; \
    chmod 644 /libs/*.jar; \
    ls -la /libs

# NOTE on what is deliberately NOT here: flink-sql-connector-kafka and flink-sql-avro-confluent-registry.
#
# Those fat connectors relocate Kafka and Avro into their own packages, but they also carry ~100 *unrelocated*
# org.apache.flink.formats.avro and org.apache.flink.connector.kafka classes compiled against those relocated
# copies. In /opt/flink/lib they shadow the identically-named classes the job jar needs, and the DataStream job
# dies with a NullPointerException inside AvroDeserializationSchema.getProducedType - its
# `SpecificRecord.class.isAssignableFrom(Transaction.class)` check compares the *shaded* SpecificRecord against
# our unshaded generated class, concludes the record is generic, and dereferences a null schema.
#
# Nothing needs them: the deployed jobs bundle the unshaded connectors, and the interactive SQL client gets the
# same coverage (plus the UDFs) from `sql-client.sh -j /opt/flink/usrlib/flink-stream-jobs.jar`.

# ----------------------------------------------------------------------------- 3. runtime
FROM ${CP_FLINK_IMAGE}

USER root

COPY --from=connectors --chown=9999:9999 /libs/ /opt/flink/lib/

# The Prometheus reporter ships in the image but, like every Flink plugin, is only loaded from its own
# directory under /opt/flink/plugins. It already is - this just makes the dependency explicit and fails the
# build early if a future base image drops it.
RUN test -n "$(ls /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-*.jar)"

# Application-mode jobs are launched as local:///opt/flink/usrlib/<jar>. The operator also appends anything in
# usrlib to the classpath of session clusters started from this image.
RUN mkdir -p /opt/flink/usrlib
COPY --from=build --chown=9999:9999 /flink-stream-jobs.jar /opt/flink/usrlib/flink-stream-jobs.jar

# The SQL lab files are deliberately NOT baked in. They live in charts/flink-stream/files/sql and reach the job
# through a ConfigMap mount, so changing a query is a `helm upgrade` rather than a rebuild - and there is
# exactly one copy of each query in the repository.

USER 9999
