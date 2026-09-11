package io.flinkstream.hybrid;

import io.flinkstream.avro.AccountWindowStats;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.JobConfig;
import io.flinkstream.common.Kafka;
import io.flinkstream.common.Money;
import io.flinkstream.common.Topics;
import io.flinkstream.common.Watermarks;
import io.flinkstream.functions.ShardObserver;
import io.flinkstream.udf.MaskPan;
import io.flinkstream.udf.WeightedAvg;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * The third style: one job that moves back and forth between the DataStream API and the Table API.
 *
 * <p>This is how most real Flink codebases end up looking. SQL is the right tool for joins, windows and
 * aggregation; the DataStream API is the right tool for anything with bespoke state, timers, or a connector the
 * Table API does not cover. Mixing them costs nothing at runtime - both compile into the same operator graph -
 * and the conversion points are just type conversions.
 *
 * <pre>
 *   Kafka ──▶ DataStream&lt;Transaction&gt; ──▶ DataStream&lt;Row&gt; ──▶ Table ──▶ SQL (window agg + UDF)
 *                                                                          │
 *                          Kafka ◀── DataStream&lt;AccountWindowStats&gt; ◀── toDataStream
 * </pre>
 *
 * <p>Two conversions, and the difference between them is the thing to learn:
 *
 * <ul>
 *   <li>{@code toDataStream} - append-only. Every row is a fact. Fails at plan time if the query retracts.</li>
 *   <li>{@code toChangelogStream} - gives you the {@link RowKind} too ({@code +I}, {@code -U}, {@code +U},
 *       {@code -D}), which is the only honest way to consume an updating query from the DataStream API.</li>
 * </ul>
 */
public final class HybridTableJob {

    /**
     * The bridge type. Going through {@link Row} with an explicit {@link RowTypeInfo}, rather than handing the
     * Avro class straight to {@code fromDataStream}, keeps the SQL-visible schema under our control: Avro
     * {@code SpecificRecord} would be reflected into a structured type whose {@code BigDecimal} and
     * {@code Instant} fields do not map to the SQL types we want.
     */
    private static final RowTypeInfo TXN_ROW = new RowTypeInfo(
            new TypeInformation[] {
                    Types.STRING,                         // txn_id
                    Types.STRING,                         // account_id
                    Types.STRING,                         // merchant_id
                    Types.STRING,                         // device_id
                    Types.BIG_DEC,                        // amount
                    Types.STRING,                         // channel
                    Types.INT,                            // shard_id
                    Types.LOCAL_DATE_TIME                 // event_time -> TIMESTAMP(3)
            },
            new String[] {
                    "txn_id", "account_id", "merchant_id", "device_id", "amount", "channel", "shard_id", "event_time"
            });

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(args);
        StreamExecutionEnvironment env = config.configureEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporarySystemFunction("mask_pan", MaskPan.class);
        tEnv.createTemporarySystemFunction("weighted_avg", WeightedAvg.class);
        tEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "15 s");

        // ---------------------------------------------------------------- DataStream side

        DataStream<Transaction> transactions = env
                .fromSource(
                        Kafka.source(config, Topics.TRANSACTIONS, Transaction.class, "hybrid", true),
                        Watermarks.boundedOutOfOrder(config, txn -> txn.getEventTime().toEpochMilli()),
                        "transactions")
                .name("source: " + Topics.TRANSACTIONS)
                .uid("hybrid-source")
                .map(new ShardObserver("hybrid"))
                .name("observe shards")
                .uid("hybrid-observe");

        DataStream<Row> rows = transactions
                .map(txn -> Row.of(
                        txn.getTxnId(),
                        txn.getAccountId(),
                        txn.getMerchantId(),
                        txn.getDeviceId(),
                        txn.getAmount(),
                        txn.getChannel(),
                        txn.getShardId(),
                        LocalDateTime.ofInstant(txn.getEventTime(), ZoneOffset.UTC)))
                .returns(TXN_ROW)
                .name("to Row")
                .uid("hybrid-to-row");

        // ---------------------------------------------------------------- into the Table API
        //
        // The watermark is re-declared here rather than inherited. It could be inherited - passing
        // SOURCE_WATERMARK() in the Schema reuses the DataStream watermarks directly - but declaring it makes
        // the event-time attribute explicit at the SQL boundary, which is where a missing rowtime attribute
        // otherwise turns into a confusing planner error.

        Table txnTable = tEnv.fromDataStream(rows, Schema.newBuilder()
                .column("txn_id", DataTypes.STRING())
                .column("account_id", DataTypes.STRING())
                .column("merchant_id", DataTypes.STRING())
                .column("device_id", DataTypes.STRING())
                .column("amount", DataTypes.DECIMAL(12, 2))
                .column("channel", DataTypes.STRING())
                .column("shard_id", DataTypes.INT())
                .column("event_time", DataTypes.TIMESTAMP(3))
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build());

        tEnv.createTemporaryView("txn", txnTable);

        // ---------------------------------------------------------------- SQL in the middle

        Table windowed = tEnv.sqlQuery("""
                SELECT
                    account_id,
                    window_start,
                    window_end,
                    COUNT(*)                                  AS txn_count,
                    CAST(SUM(amount) AS DECIMAL(14, 2))       AS total_usd,
                    CAST(MAX(amount) AS DECIMAL(14, 2))       AS max_usd,
                    COUNT(DISTINCT merchant_id)               AS distinct_merchants,
                    COUNT(DISTINCT channel)                   AS distinct_channels,
                    COUNT(DISTINCT shard_id)                  AS shards_touched,
                    MAX(mask_pan(device_id))                  AS sample_device
                FROM TABLE(TUMBLE(TABLE txn, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
                GROUP BY account_id, window_start, window_end
                """);

        // ---------------------------------------------------------------- back to DataStream
        //
        // A window-TVF aggregate is append-only, so toDataStream is legal. Swap the query for a plain
        // "GROUP BY account_id" and this line fails at plan time with "only insert-only" - which is exactly the
        // moment to reach for toChangelogStream instead. See changelogExample() below.

        DataStream<AccountWindowStats> stats = tEnv
                .toDataStream(windowed)
                .map(row -> AccountWindowStats.newBuilder()
                        .setAccountId(str(row, "account_id"))
                        .setRegion(null)
                        .setWindowStart(instant(row, "window_start"))
                        .setWindowEnd(instant(row, "window_end"))
                        .setWindowKind("TUMBLE/hybrid")
                        .setTxnCount((Long) row.getField("txn_count"))
                        .setTotalUsd(Money.normalize((BigDecimal) row.getField("total_usd")))
                        .setMaxUsd(Money.normalize((BigDecimal) row.getField("max_usd")))
                        .setDistinctMerchants(Math.toIntExact((Long) row.getField("distinct_merchants")))
                        .setDistinctChannels(Math.toIntExact((Long) row.getField("distinct_channels")))
                        .setLateEvents(0L)
                        .setSubtaskIndex(Math.toIntExact((Long) row.getField("shards_touched")))
                        .build())
                .name("Row -> AccountWindowStats")
                .uid("hybrid-to-avro");

        stats.sinkTo(Kafka.sink(config, Topics.HYBRID_OUT, AccountWindowStats.class,
                        AccountWindowStats::getAccountId, "hybrid"))
                .name("sink: " + Topics.HYBRID_OUT)
                .uid("hybrid-sink");

        if (config.labEnabled("changelog")) {
            changelogExample(tEnv);
        }

        env.execute("hybrid DataStream + Table API");
    }

    /**
     * The other conversion direction: an <em>updating</em> query consumed as a changelog.
     *
     * <p>Every row arrives tagged with a {@link RowKind}. An account whose total changes from 100 to 150 emits
     * {@code -U(100)} then {@code +U(150)}; a consumer that ignores the tag double-counts. This is the same
     * contract that upsert-kafka and the JDBC sink implement internally.
     */
    private static void changelogExample(StreamTableEnvironment tEnv) {
        Table running = tEnv.sqlQuery("""
                SELECT account_id,
                       COUNT(*)                            AS txn_count,
                       CAST(SUM(amount) AS DECIMAL(14, 2)) AS total_usd
                FROM txn
                GROUP BY account_id
                """);

        tEnv.toChangelogStream(running)
                .filter(row -> row.getKind() == RowKind.UPDATE_AFTER)
                .name("changelog: keep +U only")
                .uid("hybrid-changelog")
                .print("running-total")
                .name("print running totals")
                .uid("hybrid-changelog-print");
    }

    private static String str(Row row, String field) {
        Object value = row.getField(field);
        return value == null ? null : value.toString();
    }

    private static Instant instant(Row row, String field) {
        return ((LocalDateTime) row.getField(field)).toInstant(ZoneOffset.UTC);
    }

    private HybridTableJob() {}
}
