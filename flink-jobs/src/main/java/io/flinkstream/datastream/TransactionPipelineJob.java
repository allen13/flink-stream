package io.flinkstream.datastream;

import io.flinkstream.avro.Account;
import io.flinkstream.avro.AccountWindowStats;
import io.flinkstream.avro.AuthEvent;
import io.flinkstream.avro.DeadLetter;
import io.flinkstream.avro.EnrichedTransaction;
import io.flinkstream.avro.FraudAlert;
import io.flinkstream.avro.Merchant;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.JobConfig;
import io.flinkstream.common.Kafka;
import io.flinkstream.common.RegionalTxn;
import io.flinkstream.common.Topics;
import io.flinkstream.common.Watermarks;
import io.flinkstream.functions.AccountJoinFunction;
import io.flinkstream.functions.AsyncRiskScoreFunction;
import io.flinkstream.functions.AuthJoinFunction;
import io.flinkstream.functions.EscalatingSpendPattern;
import io.flinkstream.functions.LateEventToDeadLetter;
import io.flinkstream.functions.MerchantEnrichmentFunction;
import io.flinkstream.functions.RegionalAggregate;
import io.flinkstream.functions.ShardObserver;
import io.flinkstream.functions.TransactionAggregate;
import io.flinkstream.functions.VelocityRuleFunction;
import io.flinkstream.functions.WindowStatsFunction;
import io.flinkstream.sinks.Yugabyte;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The DataStream half of the project: one job graph that wires up ten labs over the same Kafka topics.
 *
 * <p>They live in a single job on purpose. A Flink job is a <em>graph</em>, not a pipeline - one source can feed
 * many independent branches, and many branches can land in many sinks, all sharing one set of checkpoints and one
 * restart policy. Splitting them into ten jobs would be ten sets of TaskManagers on a laptop.
 *
 * <p>Pass {@code --labs a,d,f} (or set {@code LABS}) to run a subset while experimenting.
 *
 * <pre>
 *   ref.merchants ──broadcast──┐
 *                              ▼
 *   txn.transactions ──▶ [A] enrich ──▶ [B] async risk ──▶ [C] interval join ──▶ out.enriched-transactions
 *          │                    │                              ▲
 *          │                    └──side output──▶ out.dead-letter
 *          │                                                   │
 *          │                                    txn.auth-events┘
 *          │
 *          ├──▶ [D] tumbling 1m ─┬─▶ out.account-window-stats
 *          │                     └─late─▶ out.late-events
 *          ├──▶ [E] session 2m ────▶ out.account-window-stats
 *          ├──▶ [F] velocity rule ─▶ out.fraud-alerts
 *          ├──▶ [G] CEP pattern ───▶ out.fraud-alerts
 *          └──▶ [H] join ref.accounts ──▶ [I] keyBy(region) sliding 5m/1m ──▶ out.account-window-stats
 * </pre>
 */
public final class TransactionPipelineJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(args);
        StreamExecutionEnvironment env = config.configureEnvironment();

        // ------------------------------------------------------------------ sources
        //
        // Every source gets its own watermark strategy. The dimension topics are compacted and bursty, so their
        // idleness timeout matters most: without it, a silent ref.merchants partition pins the watermark of the
        // whole connected stream at its last value and nothing downstream ever fires.

        DataStream<Transaction> transactions = env
                .fromSource(
                        Kafka.source(config, Topics.TRANSACTIONS, Transaction.class, "txn", true),
                        Watermarks.aligned(config, txn -> txn.getEventTime().toEpochMilli(), "facts"),
                        "transactions")
                .name("source: " + Topics.TRANSACTIONS)
                .uid("source-transactions")
                .map(new ShardObserver("source"))
                .name("observe shards (pre-shuffle)")
                .uid("observe-source");

        DataStream<AuthEvent> authEvents = env
                .fromSource(
                        Kafka.source(config, Topics.AUTH_EVENTS, AuthEvent.class, "auth", true),
                        Watermarks.aligned(config, auth -> auth.getEventTime().toEpochMilli(), "facts"),
                        "auth-events")
                .name("source: " + Topics.AUTH_EVENTS)
                .uid("source-auth-events");

        DataStream<Merchant> merchants = env
                .fromSource(
                        Kafka.source(config, Topics.MERCHANTS, Merchant.class, "merchant", true),
                        // The dimension carries no meaningful watermark - it must never hold back event time.
                        WatermarkStrategy.noWatermarks(),
                        "merchants")
                .name("source: " + Topics.MERCHANTS)
                .uid("source-merchants")
                .setParallelism(1);

        DataStream<Account> accounts = env
                .fromSource(
                        Kafka.source(config, Topics.ACCOUNTS, Account.class, "account", true),
                        Watermarks.boundedOutOfOrder(config, a -> a.getUpdatedAt().toEpochMilli()),
                        "accounts")
                .name("source: " + Topics.ACCOUNTS)
                .uid("source-accounts");

        // ------------------------------------------------------------------ labs

        SingleOutputStreamOperator<EnrichedTransaction> enriched = null;
        if (config.labEnabled("a")) {
            enriched = labA_broadcastEnrichment(config, transactions, merchants);

            DataStream<DeadLetter> unknownMerchants = enriched.getSideOutput(MerchantEnrichmentFunction.DLQ);
            unknownMerchants
                    .sinkTo(Kafka.sink(config, Topics.DLQ, DeadLetter.class, DeadLetter::getRecordKey, "dlq"))
                    .name("sink: " + Topics.DLQ)
                    .uid("sink-dlq");
        }

        if (enriched != null) {
            DataStream<EnrichedTransaction> scored = config.labEnabled("b")
                    ? labB_asyncRiskScore(config, enriched)
                    : enriched;

            DataStream<EnrichedTransaction> withAuth = config.labEnabled("c")
                    ? labC_intervalJoinWithAuth(scored, authEvents)
                    : scored;

            // Dual sink, fan-out flavour: one stream, two independent sinks with different guarantees.
            // Kafka gets the immutable log (exactly-once, transactional); YugabyteDB gets the queryable
            // latest-state table (at-least-once writes made idempotent by ON CONFLICT DO UPDATE).
            // Both read the same operator output - Flink does not re-run the upstream pipeline per sink.
            withAuth
                    .sinkTo(Kafka.sink(config, Topics.ENRICHED, EnrichedTransaction.class,
                            EnrichedTransaction::getAccountId, "enriched"))
                    .name("sink 1/2: kafka " + Topics.ENRICHED)
                    .uid("sink-enriched-kafka");

            if (config.yugabyteEnabled()) {
                withAuth
                        .sinkTo(Yugabyte.enrichedTransactions(config))
                        .name("sink 2/2: yugabyte enriched_transactions")
                        .uid("sink-enriched-yugabyte");
            }
        }

        if (config.labEnabled("d")) {
            labD_tumblingWindow(config, transactions);
        }
        if (config.labEnabled("e")) {
            labE_sessionWindow(transactions)
                    .sinkTo(Kafka.sink(config, Topics.ACCOUNT_STATS, AccountWindowStats.class,
                            AccountWindowStats::getAccountId, "stats-session"))
                    .name("sink: " + Topics.ACCOUNT_STATS + " (session)")
                    .uid("sink-stats-session");
        }

        DataStream<FraudAlert> alerts = null;
        if (config.labEnabled("f")) {
            alerts = labF_velocityRule(config, transactions);
        }
        if (config.labEnabled("g")) {
            DataStream<FraudAlert> cepAlerts = labG_cepPattern(transactions);
            alerts = alerts == null ? cepAlerts : alerts.union(cepAlerts);
        }
        if (alerts != null) {
            alerts.sinkTo(Kafka.sink(config, Topics.FRAUD_ALERTS, FraudAlert.class,
                            FraudAlert::getAccountId, "alerts"))
                    .name("sink 1/2: kafka " + Topics.FRAUD_ALERTS)
                    .uid("sink-alerts-kafka");

            if (config.yugabyteEnabled()) {
                alerts.sinkTo(Yugabyte.fraudAlerts(config))
                        .name("sink 2/2: yugabyte fraud_alerts")
                        .uid("sink-alerts-yugabyte");
            }
        }

        if (config.labEnabled("h")) {
            DataStream<RegionalTxn> regional = labH_joinAccountDimension(config, transactions, accounts);
            if (config.labEnabled("i")) {
                labI_crossShardAggregate(regional)
                        .sinkTo(Kafka.sink(config, Topics.ACCOUNT_STATS, AccountWindowStats.class,
                                AccountWindowStats::getAccountId, "stats-region"))
                        .name("sink: " + Topics.ACCOUNT_STATS + " (region)")
                        .uid("sink-stats-region");
            }
        }

        env.execute("transaction-pipeline (DataStream)");
    }

    // ====================================================================== Lab A

    /**
     * Enriches the fact stream from a broadcast dimension. See {@link MerchantEnrichmentFunction} for why this
     * beats a keyed join for a small, slow-moving table.
     */
    private static SingleOutputStreamOperator<EnrichedTransaction> labA_broadcastEnrichment(
            JobConfig config, DataStream<Transaction> transactions, DataStream<Merchant> merchants) {

        BroadcastStream<Merchant> broadcast = merchants.broadcast(MerchantEnrichmentFunction.MERCHANT_STATE);

        return transactions
                .keyBy(Transaction::getAccountId)
                .connect(broadcast)
                .process(new MerchantEnrichmentFunction())
                .name("A: enrich from broadcast merchants")
                .uid("lab-a-broadcast-enrich");
    }

    // ====================================================================== Lab B

    private static DataStream<EnrichedTransaction> labB_asyncRiskScore(
            JobConfig config, DataStream<EnrichedTransaction> enriched) {

        int latencyMs = Integer.parseInt(config.get("risk-latency-ms", "25"));
        int capacity = Integer.parseInt(config.get("async-capacity", "200"));

        return AsyncDataStream
                .unorderedWait(enriched, new AsyncRiskScoreFunction(latencyMs), 10, TimeUnit.SECONDS, capacity)
                .name("B: async risk score")
                .uid("lab-b-async-risk");
    }

    // ====================================================================== Lab C

    /**
     * Re-keys both sides on {@code txn_id} and joins them on an event-time interval.
     *
     * <p>The lower bound is negative because a fast authorizer can stamp its decision a beat <em>before</em> the
     * transaction's own timestamp; the generator reproduces that skew deliberately.
     */
    private static DataStream<EnrichedTransaction> labC_intervalJoinWithAuth(
            DataStream<EnrichedTransaction> enriched, DataStream<AuthEvent> authEvents) {

        return enriched
                .keyBy(EnrichedTransaction::getTxnId)
                .intervalJoin(authEvents.keyBy(AuthEvent::getTxnId))
                .between(Duration.ofSeconds(-2), Duration.ofSeconds(45))
                .process(new AuthJoinFunction())
                .name("C: interval join with auth decisions")
                .uid("lab-c-interval-join");
    }

    // ====================================================================== Lab D

    /**
     * Tumbling one-minute windows per account, with lateness handling.
     *
     * <p>{@code allowedLateness} keeps the window state alive past the watermark so a straggler can update an
     * already-emitted result (producing a second, corrected output record). Anything later than that goes to the
     * side output instead of being dropped on the floor.
     */
    private static void labD_tumblingWindow(JobConfig config, DataStream<Transaction> transactions) {

        SingleOutputStreamOperator<AccountWindowStats> stats = transactions
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(config.allowedLateness())
                .sideOutputLateData(LATE_TRANSACTIONS)
                .aggregate(new TransactionAggregate(), new WindowStatsFunction("TUMBLE", false))
                .name("D: tumbling 1m per account")
                .uid("lab-d-tumbling");

        stats.sinkTo(Kafka.sink(config, Topics.ACCOUNT_STATS, AccountWindowStats.class,
                        AccountWindowStats::getAccountId, "stats-tumble"))
                .name("sink: " + Topics.ACCOUNT_STATS + " (tumble)")
                .uid("sink-stats-tumble");

        stats.getSideOutput(LATE_TRANSACTIONS)
                .map(new LateEventToDeadLetter())
                .name("D: late events -> dead letter")
                .uid("lab-d-late")
                .sinkTo(Kafka.sink(config, Topics.LATE_EVENTS, DeadLetter.class,
                        DeadLetter::getRecordKey, "late"))
                .name("sink: " + Topics.LATE_EVENTS)
                .uid("sink-late");
    }

    private static final org.apache.flink.util.OutputTag<Transaction> LATE_TRANSACTIONS =
            new org.apache.flink.util.OutputTag<>("late-transactions", TypeInformation.of(Transaction.class));

    // ====================================================================== Lab E

    /**
     * Session windows: a window per burst of activity, closed by {@code gap} of silence.
     *
     * <p>Unlike tumbling windows, session windows are <em>merging</em>. Each record initially opens its own
     * window and Flink merges overlapping ones, which is why {@code AggregateFunction#merge} has to be correct.
     */
    private static DataStream<AccountWindowStats> labE_sessionWindow(DataStream<Transaction> transactions) {
        return transactions
                .keyBy(Transaction::getAccountId)
                .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(2)))
                .aggregate(new TransactionAggregate(), new WindowStatsFunction("SESSION", false))
                .name("E: session windows (2m gap)")
                .uid("lab-e-session");
    }

    // ====================================================================== Lab F

    private static DataStream<FraudAlert> labF_velocityRule(JobConfig config, DataStream<Transaction> transactions) {
        return transactions
                .keyBy(Transaction::getAccountId)
                .process(new VelocityRuleFunction(
                        Integer.parseInt(config.get("velocity-count", "5")),
                        new BigDecimal(config.get("velocity-amount", "2500.00")),
                        Duration.ofMinutes(Long.parseLong(config.get("velocity-window-min", "3"))),
                        config.stateTtl()))
                .name("F: velocity rule (timers + TTL)")
                .uid("lab-f-velocity");
    }

    // ====================================================================== Lab G

    private static DataStream<FraudAlert> labG_cepPattern(DataStream<Transaction> transactions) {
        KeyedStream<Transaction, String> byAccount = transactions.keyBy(Transaction::getAccountId);
        return CEP.pattern(byAccount, EscalatingSpendPattern.pattern())
                .inEventTime()
                .process(EscalatingSpendPattern.alertBuilder())
                .name("G: CEP card-testing pattern")
                .uid("lab-g-cep");
    }

    // ====================================================================== Lab H

    private static DataStream<RegionalTxn> labH_joinAccountDimension(
            JobConfig config, DataStream<Transaction> transactions, DataStream<Account> accounts) {

        return transactions
                .keyBy(Transaction::getAccountId)
                .connect(accounts.keyBy(Account::getAccountId))
                .process(new AccountJoinFunction(config.stateTtl()))
                .name("H: join ref.accounts (versioned, TTL'd)")
                .uid("lab-h-account-join");
    }

    // ====================================================================== Lab I

    /**
     * The cross-shard aggregate. {@code keyBy(region)} sends records from all 12 Kafka shards to whichever
     * subtask owns that region's key group - a genuine all-to-all exchange.
     *
     * <p>Only five distinct region keys exist, so with parallelism above five some subtasks idle. That skew is
     * not a bug to hide: low-cardinality keys are exactly where a shuffle stops scaling, and seeing it in the
     * subtask-level metrics is more useful than papering over it with a two-phase aggregation.
     */
    private static DataStream<AccountWindowStats> labI_crossShardAggregate(DataStream<RegionalTxn> regional) {
        return regional
                .keyBy(txn -> txn.region)
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofMinutes(1)))
                .aggregate(new RegionalAggregate(), new WindowStatsFunction("HOP", true))
                .name("I: sliding 5m/1m per region (cross-shard)")
                .uid("lab-i-region-hop");
    }

    private TransactionPipelineJob() {}
}
