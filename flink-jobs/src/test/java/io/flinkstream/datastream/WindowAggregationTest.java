package io.flinkstream.datastream;

import io.flinkstream.avro.AccountWindowStats;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Shards;
import io.flinkstream.functions.TransactionAggregate;
import io.flinkstream.functions.WindowStatsFunction;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for the windowing labs, on a MiniCluster.
 *
 * <p>Unlike the operator-harness tests, these run a real job graph: a real shuffle, real watermarks, real window
 * assignment. That is what makes them able to catch the things a harness cannot - a broken {@code merge}, a
 * lateness policy that drops the wrong records, an aggregate that is not associative.
 */
class WindowAggregationTest {

    private static final Instant BASE = Instant.parse("2026-01-01T10:00:00Z");

    private static final OutputTag<Transaction> LATE =
            new OutputTag<>("late", TypeInformation.of(Transaction.class));

    @Test
    void tumblingWindowsAggregatePerAccountPerMinute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

        // Order matters: the watermark advances with every element, so anything belonging to minute 0 has to
        // appear before the minute-1 record that closes it. Getting this wrong is what the lateness tests below
        // exercise deliberately.
        List<Transaction> input = List.of(
                // minute 0
                txn("acct-1", "mch-1", 10.00, BASE),
                txn("acct-2", "mch-1", 5.00, BASE.plusSeconds(5)),
                txn("acct-1", "mch-2", 20.00, BASE.plusSeconds(10)),
                txn("acct-1", "mch-1", 30.00, BASE.plusSeconds(20)),
                // minute 1 - closes both minute-0 windows
                txn("acct-1", "mch-3", 40.00, BASE.plusSeconds(70)));

        List<AccountWindowStats> results = collect(env, input, Duration.ZERO);

        assertThat(results).hasSize(3);

        AccountWindowStats acct1Minute0 = find(results, "acct-1", BASE);
        assertThat(acct1Minute0.getTxnCount()).isEqualTo(3);
        assertThat(acct1Minute0.getTotalUsd()).isEqualByComparingTo(new BigDecimal("60.00"));
        assertThat(acct1Minute0.getMaxUsd()).isEqualByComparingTo(new BigDecimal("30.00"));
        // mch-1 appears twice; the distinct count must not double it.
        assertThat(acct1Minute0.getDistinctMerchants()).isEqualTo(2);
        assertThat(acct1Minute0.getWindowEnd()).isEqualTo(BASE.plusSeconds(60));

        assertThat(find(results, "acct-1", BASE.plusSeconds(60)).getTxnCount()).isEqualTo(1);
        assertThat(find(results, "acct-2", BASE).getTxnCount()).isEqualTo(1);
    }

    /**
     * A record whose timestamp falls in an already-emitted window, past allowed lateness, must reach the side
     * output rather than disappear.
     */
    @Test
    void recordsPastAllowedLatenessGoToTheSideOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        List<Transaction> input = new ArrayList<>(List.of(
                txn("acct-1", "mch-1", 10.00, BASE),
                // pushes the watermark well past minute 0
                txn("acct-1", "mch-1", 10.00, BASE.plusSeconds(600)),
                // belongs to minute 0, which closed long ago
                txn("acct-1", "mch-9", 99.00, BASE.plusSeconds(5))));

        SingleOutputStreamOperator<AccountWindowStats> stats = pipeline(env, input, Duration.ZERO);
        List<Transaction> late = new ArrayList<>();
        try (CloseableIterator<Transaction> it = stats.getSideOutput(LATE).executeAndCollect()) {
            it.forEachRemaining(late::add);
        }

        assertThat(late).hasSize(1);
        assertThat(late.get(0).getMerchantId()).isEqualTo("mch-9");
    }

    /**
     * Within allowed lateness the window fires a *second* time with a corrected result. Downstream has to cope
     * with two records for one (key, window) - which is why the serving tables are upserts.
     */
    @Test
    void allowedLatenessEmitsACorrectedResult() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        List<Transaction> input = List.of(
                txn("acct-1", "mch-1", 10.00, BASE),
                txn("acct-1", "mch-1", 10.00, BASE.plusSeconds(65)),     // closes minute 0
                txn("acct-1", "mch-2", 90.00, BASE.plusSeconds(30)));    // late, but within lateness

        List<AccountWindowStats> results = collect(env, input, Duration.ofSeconds(30));

        List<AccountWindowStats> minute0 = results.stream()
                .filter(r -> r.getAccountId().equals("acct-1") && r.getWindowStart().equals(BASE))
                .toList();

        assertThat(minute0).as("the window fires again when the straggler arrives").hasSize(2);
        assertThat(minute0.get(0).getTotalUsd()).isEqualByComparingTo(new BigDecimal("10.00"));
        assertThat(minute0.get(1).getTotalUsd()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    /**
     * The property the cross-shard labs rest on: keys spread over all shards, and a keyBy therefore mixes every
     * shard into every subtask. Asserted on the data rather than on the runtime, since it is the *data* that
     * makes the shuffle non-trivial.
     */
    @Test
    void accountKeysSpreadAcrossAllShards() {
        Map<Integer, Integer> perShard = new HashMap<>();
        for (int i = 0; i < 400; i++) {
            perShard.merge(Shards.kafkaPartitionFor("acct-" + String.format("%05d", i), 12), 1, Integer::sum);
        }
        assertThat(perShard.keySet()).hasSize(12);
    }

    // ------------------------------------------------------------------------------------------------ helpers

    /**
     * Emits a watermark after <em>every</em> element rather than on a timer.
     *
     * <p>This matters more than it looks. The built-in strategies emit watermarks periodically (200ms by
     * default), and a bounded source of five records is exhausted long before the first timer fires - so every
     * record lands in its window, end-of-input emits {@code MAX_WATERMARK}, and nothing is ever late. A lateness
     * test written against a periodic strategy therefore passes whatever the lateness policy does, which makes
     * it worse than no test at all.
     *
     * <p>The same effect shows up in production as "my lateness handling works in the IDE but not in the
     * cluster", or the reverse: watermark timing is a property of the <em>strategy</em>, not of the data.
     */
    private static WatermarkStrategy<Transaction> punctuated() {
        return WatermarkStrategy
                .<Transaction>forGenerator(ctx -> new WatermarkGenerator<>() {
                    private long max = Long.MIN_VALUE;

                    @Override
                    public void onEvent(Transaction event, long eventTimestamp, WatermarkOutput output) {
                        max = Math.max(max, eventTimestamp);
                        output.emitWatermark(new Watermark(max));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // nothing - every watermark is emitted from onEvent
                    }
                })
                .withTimestampAssigner((txn, ts) -> txn.getEventTime().toEpochMilli());
    }

    private static SingleOutputStreamOperator<AccountWindowStats> pipeline(
            StreamExecutionEnvironment env, List<Transaction> input, Duration allowedLateness) {

        DataStream<Transaction> source = env
                .fromData(input)
                .returns(TypeInformation.of(Transaction.class))
                .assignTimestampsAndWatermarks(punctuated());

        return source
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE)
                .aggregate(new TransactionAggregate(), new WindowStatsFunction("TUMBLE", false));
    }

    private static List<AccountWindowStats> collect(
            StreamExecutionEnvironment env, List<Transaction> input, Duration allowedLateness) throws Exception {

        List<AccountWindowStats> results = new ArrayList<>();
        try (CloseableIterator<AccountWindowStats> it = pipeline(env, input, allowedLateness).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        return results;
    }

    private static AccountWindowStats find(List<AccountWindowStats> results, String account, Instant windowStart) {
        return results.stream()
                .filter(r -> r.getAccountId().equals(account) && r.getWindowStart().equals(windowStart))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no result for " + account + " at " + windowStart));
    }

    private static Transaction txn(String accountId, String merchantId, double amount, Instant eventTime) {
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setMerchantId(merchantId)
                .setAmount(BigDecimal.valueOf(amount).setScale(2))
                .setCurrency("USD")
                .setChannel("CARD_PRESENT")
                .setDeviceId(null)
                .setGeo(null)
                .setTags(new HashMap<>())
                .setShardId(Shards.kafkaPartitionFor(accountId, 12))
                .setEventTime(eventTime)
                .build();
    }
}
