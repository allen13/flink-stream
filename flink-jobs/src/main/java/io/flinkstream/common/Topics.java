package io.flinkstream.common;

/**
 * Every Kafka topic this project uses.
 *
 * <p>Partition counts live in {@code charts/flink-stream/values.yaml} ({@code kafka.topics}). Throughout the
 * docs a Kafka <em>partition</em> is called a <em>shard</em>: it is the unit of parallelism that both Kafka and
 * Flink shard state by, and the thing a cross-shard join has to shuffle across.
 */
public final class Topics {

    /** Fact stream. 12 shards, keyed by {@code account_id}. */
    public static final String TRANSACTIONS = "txn.transactions";

    /** Authorization outcomes, arriving 0-30s after the matching transaction. 12 shards, keyed by {@code account_id}. */
    public static final String AUTH_EVENTS = "txn.auth-events";

    /** Compacted account dimension. 12 shards, keyed by {@code account_id}. */
    public static final String ACCOUNTS = "ref.accounts";

    /** Compacted merchant dimension. 6 shards, keyed by {@code merchant_id} - a different key, hence a cross-shard join. */
    public static final String MERCHANTS = "ref.merchants";

    /** Versioned FX rates. 3 shards, keyed by {@code currency}. */
    public static final String FX_RATES = "ref.fx-rates";

    /** DataStream output: transaction x merchant x risk score x auth decision. */
    public static final String ENRICHED = "out.enriched-transactions";

    /** DataStream output: per-account window aggregates. */
    public static final String ACCOUNT_STATS = "out.account-window-stats";

    /** DataStream output: alerts from the velocity rule and the CEP pattern. */
    public static final String FRAUD_ALERTS = "out.fraud-alerts";

    /** DataStream output: events that arrived after their window had already been emitted. */
    public static final String LATE_EVENTS = "out.late-events";

    /** DataStream output: records that could not be processed (poison pill / enrichment miss). */
    public static final String DLQ = "out.dead-letter";

    /** SQL output: per-region revenue, aggregated across every shard. */
    public static final String REGION_REVENUE = "sql.region-revenue";

    /** SQL output: merchant leaderboard per window. */
    public static final String MERCHANT_TOPN = "sql.merchant-topn";

    /** SQL output: MATCH_RECOGNIZE results. */
    public static final String PATTERN_ALERTS = "sql.pattern-alerts";

    /** SQL output: transactions converted to USD through a versioned temporal join. */
    public static final String FX_NORMALIZED = "sql.fx-normalized";

    /** Hybrid (DataStream + Table API) job output. */
    public static final String HYBRID_OUT = "hybrid.account-risk";

    private Topics() {}
}
