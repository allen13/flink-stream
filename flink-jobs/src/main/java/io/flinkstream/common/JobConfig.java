package io.flinkstream.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

/**
 * Resolves job settings from (in order of precedence) command-line args, environment variables, then defaults.
 *
 * <p>The chart passes Kafka/Schema-Registry coordinates as env vars so the same jar runs unchanged from an IDE,
 * from the SQL client, or from a {@code FlinkDeployment}.
 */
public final class JobConfig {

    private final ParameterTool params;

    private JobConfig(ParameterTool params) {
        this.params = params;
    }

    public static JobConfig from(String[] args) {
        return new JobConfig(ParameterTool.fromArgs(args));
    }

    public ParameterTool params() {
        return params;
    }

    // ------------------------------------------------------------------ endpoints

    public String bootstrapServers() {
        return resolve("bootstrap-servers", "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    }

    public String schemaRegistryUrl() {
        return resolve("schema-registry-url", "SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
    }

    public String consumerGroup() {
        return resolve("group-id", "KAFKA_GROUP_ID", "flink-stream");
    }

    /** Prefix for Kafka transactional ids. Must be unique per sink, per job, or EXACTLY_ONCE sinks fence each other. */
    public String txnIdPrefix() {
        return resolve("transactional-id-prefix", "KAFKA_TXN_ID_PREFIX", "flink-stream");
    }

    // ------------------------------------------------------------------ tuning knobs

    public int parallelism() {
        return Integer.parseInt(resolve("parallelism", "JOB_PARALLELISM", "4"));
    }

    public Duration checkpointInterval() {
        return Duration.ofSeconds(Long.parseLong(resolve("checkpoint-interval-s", "CHECKPOINT_INTERVAL_S", "30")));
    }

    public Duration maxOutOfOrderness() {
        return Duration.ofSeconds(Long.parseLong(resolve("out-of-orderness-s", "OUT_OF_ORDERNESS_S", "5")));
    }

    /** How long a source partition may be silent before it stops holding the global watermark back. */
    public Duration sourceIdleTimeout() {
        return Duration.ofSeconds(Long.parseLong(resolve("source-idle-timeout-s", "SOURCE_IDLE_TIMEOUT_S", "15")));
    }

    public Duration allowedLateness() {
        return Duration.ofSeconds(Long.parseLong(resolve("allowed-lateness-s", "ALLOWED_LATENESS_S", "10")));
    }

    public Duration stateTtl() {
        return Duration.ofMinutes(Long.parseLong(resolve("state-ttl-min", "STATE_TTL_MIN", "60")));
    }

    /** Whether the DataStream job also writes to YugabyteDB alongside Kafka. */
    public boolean yugabyteEnabled() {
        return Boolean.parseBoolean(resolve("yugabyte-enabled", "YUGABYTE_ENABLED", "true"));
    }

    public boolean exactlyOnceSinks() {
        return Boolean.parseBoolean(resolve("exactly-once-sinks", "EXACTLY_ONCE_SINKS", "true"));
    }

    public String get(String key, String defaultValue) {
        return resolve(key, key.toUpperCase().replace('-', '_'), defaultValue);
    }

    public boolean labEnabled(String lab) {
        String enabled = resolve("labs", "LABS", "all");
        return "all".equalsIgnoreCase(enabled) || ("," + enabled.toLowerCase() + ",").contains("," + lab.toLowerCase() + ",");
    }

    // ------------------------------------------------------------------ Kafka client props

    /** Consumer properties shared by every {@code KafkaSource} in the project. */
    public Properties consumerProperties() {
        Properties p = new Properties();
        // Commit offsets back to Kafka purely so the lag shows up in Kafka UI; Flink's own checkpoints are the
        // source of truth for restart positions.
        p.setProperty("commit.offsets.on.checkpoint", "true");
        p.setProperty("partition.discovery.interval.ms", "30000");
        p.setProperty("auto.offset.reset", "earliest");
        return p;
    }

    /** Producer properties shared by every {@code KafkaSink}. */
    public Properties producerProperties() {
        Properties p = new Properties();
        p.setProperty("compression.type", "lz4");
        p.setProperty("linger.ms", "50");
        // EXACTLY_ONCE sinks keep a transaction open for a whole checkpoint interval, so the broker-side
        // timeout has to comfortably exceed it or the transaction expires and the job fails on commit.
        p.setProperty("transaction.timeout.ms", String.valueOf(Duration.ofMinutes(15).toMillis()));
        return p;
    }

    // ------------------------------------------------------------------ environment

    /**
     * Applies the checkpointing / state-backend / restart settings that every job in this project shares.
     *
     * <p>Deliberately set in code rather than only in {@code flink-conf.yaml} so the behaviour is visible next to
     * the pipeline that depends on it.
     */
    public StreamExecutionEnvironment configureEnvironment() {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(parallelism());

        env.enableCheckpointing(checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(Duration.ofSeconds(5).toMillis());
        cp.setCheckpointTimeout(Duration.ofMinutes(5).toMillis());
        cp.setMaxConcurrentCheckpoints(1);
        cp.setTolerableCheckpointFailureNumber(3);
        // Keep checkpoints on cancel so a cancelled job can be resumed from its last checkpoint.
        cp.setExternalizedCheckpointRetention(
                org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Unaligned checkpoints let barriers overtake in-flight data, which keeps checkpoints fast when a
        // cross-shard shuffle is backpressured. The alignment timeout means we only pay the extra state cost
        // when alignment is actually slow.
        cp.enableUnalignedCheckpoints(true);
        cp.setAlignedCheckpointTimeout(Duration.ofSeconds(10));

        // RocksDB with incremental checkpoints: state here (keyed windows, join buffers, TTL'd maps) grows
        // beyond what the heap backend would hold comfortably.
        EmbeddedRocksDBStateBackend rocksDb = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDb);

        env.getConfig().setGlobalJobParameters(params);
        // Generic types are fine here, but leaving the warning on makes accidental Kryo fallbacks visible in the logs.
        env.getConfig().enableObjectReuse();

        return env;
    }

    // ------------------------------------------------------------------ internals

    private String resolve(String paramKey, String envKey, String defaultValue) {
        if (params.has(paramKey)) {
            return params.get(paramKey);
        }
        String fromEnv = System.getenv(envKey);
        return fromEnv != null && !fromEnv.isBlank() ? fromEnv : defaultValue;
    }
}
