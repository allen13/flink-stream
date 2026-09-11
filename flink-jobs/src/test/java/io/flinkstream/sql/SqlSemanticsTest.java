package io.flinkstream.sql;

import io.flinkstream.udf.ExplodeTags;
import io.flinkstream.udf.Haversine;
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
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the SQL constructs the lab files depend on, over a small in-memory table.
 *
 * <p>These run in seconds and need no cluster, so they are the right place to catch a UDF signature change or a
 * planner rule that a Flink upgrade altered - rather than finding out from a deployment that fails three minutes
 * into a `helm upgrade`.
 */
class SqlSemanticsTest {

    private StreamTableEnvironment tEnv;

    /**
     * The bounded test table, shaped like {@code txn.transactions}.
     *
     * <p>Built from a {@code DataStream<Row>} rather than from a {@code VALUES} view, because a window TVF
     * needs a real <em>time attribute</em> - a TIMESTAMP column with a watermark - and a VALUES list only
     * produces a plain {@code TIMESTAMP(0)}:
     *
     * <pre>The window function requires the timecol is a time attribute type, but is TIMESTAMP(0).</pre>
     *
     * <p>This is also the conversion {@code HybridTableJob} performs in production code, so the test covers it
     * incidentally.
     */
    private static final RowTypeInfo TXN_ROW = new RowTypeInfo(
            new TypeInformation[] {
                    Types.STRING, Types.STRING, Types.STRING, Types.BIG_DEC,
                    Types.STRING, Types.INT, Types.LOCAL_DATE_TIME, Types.MAP(Types.STRING, Types.STRING)
            },
            new String[] {
                    "txn_id", "account_id", "merchant_id", "amount",
                    "channel", "shard_id", "event_time", "tags"
            });

    @BeforeEach
    void setUp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporarySystemFunction("mask_pan", MaskPan.class);
        tEnv.createTemporarySystemFunction("haversine_km", Haversine.class);
        tEnv.createTemporarySystemFunction("explode_tags", ExplodeTags.class);
        tEnv.createTemporarySystemFunction("weighted_avg", WeightedAvg.class);

        // Arrays.asList, not List.of: one row carries a null map on purpose and List.of rejects nulls.
        DataStream<Row> rows = env.fromData(Arrays.asList(
                        row("t1", "acct-1", "mch-1", "10.00", "CARD_PRESENT", 0, "10:00:00",
                                Map.of("channel", "pos", "source", "gen")),
                        row("t2", "acct-1", "mch-2", "20.00", "CARD_NOT_PRESENT", 3, "10:00:20",
                                Map.of("source", "gen")),
                        // a NULL map, to exercise the null branch of ExplodeTags
                        row("t3", "acct-2", "mch-1", "30.00", "CARD_PRESENT", 7, "10:00:40", null),
                        row("t4", "acct-1", "mch-3", "40.00", "ATM", 11, "10:01:10",
                                Map.of("source", "gen"))),
                // The TypeInformation has to be passed *into* fromData. A trailing .returns() is too late:
                // fromData serializes the collection eagerly, and without the RowTypeInfo it falls back to Kryo,
                // which cannot even reflect over java.time.LocalDateTime on a modern JDK.
                TXN_ROW);

        tEnv.createTemporaryView("txn", tEnv.fromDataStream(rows, Schema.newBuilder()
                .column("txn_id", DataTypes.STRING())
                .column("account_id", DataTypes.STRING())
                .column("merchant_id", DataTypes.STRING())
                .column("amount", DataTypes.DECIMAL(12, 2))
                .column("channel", DataTypes.STRING())
                .column("shard_id", DataTypes.INT())
                .column("event_time", DataTypes.TIMESTAMP(3))
                .column("tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                .watermark("event_time", "event_time - INTERVAL '1' SECOND")
                .build()));
    }

    private static Row row(String txnId, String accountId, String merchantId, String amount,
                           String channel, int shardId, String timeOfDay, Map<String, String> tags) {
        return Row.of(txnId, accountId, merchantId, new BigDecimal(amount), channel, shardId,
                LocalDateTime.parse("2026-01-01T" + timeOfDay), tags);
    }

    @Test
    void scalarUdfMasksAllButTheLastFourCharacters() {
        assertThat(single("SELECT mask_pan('4111111111111234')")).isEqualTo("************1234");
        assertThat(single("SELECT mask_pan('abc')")).isEqualTo("***");
        assertThat(single("SELECT mask_pan('4111111111111234', 2)")).isEqualTo("**************34");
        assertThat(single("SELECT mask_pan(CAST(NULL AS STRING))")).isNull();
    }

    @Test
    void haversineComputesRealDistances() {
        // Phoenix to Zurich, the "impossible travel" pair the generator injects.
        double km = (Double) single("SELECT haversine_km(33.4484, -112.0740, 47.3769, 8.5417)");
        assertThat(km).isBetween(9100.0, 9400.0);

        assertThat(single("SELECT haversine_km(33.4484, -112.0740, 33.4484, -112.0740)"))
                .isEqualTo(0.0);
        assertThat(single("SELECT haversine_km(CAST(NULL AS DOUBLE), 1.0, 2.0, 3.0)")).isNull();
    }

    /**
     * A plain {@code LATERAL TABLE} is an inner join, so rows with an empty map vanish. That is the behaviour
     * the doc comment on {@link ExplodeTags} warns about, pinned here.
     */
    @Test
    void tableUdfExpandsAMapAndDropsEmptyOnes() {
        List<Row> rows = collect("""
                SELECT t.txn_id, tag.k, tag.v
                FROM txn AS t, LATERAL TABLE(explode_tags(t.tags)) AS tag(k, v)
                """);

        // t1 has 2 tags, t2 and t4 have 1 each, t3's map is NULL and is dropped entirely.
        assertThat(rows).hasSize(4);
        assertThat(rows).noneMatch(r -> "t3".equals(r.getField(0)));

        List<Row> withLeft = collect("""
                SELECT t.txn_id, tag.k
                FROM txn AS t LEFT JOIN LATERAL TABLE(explode_tags(t.tags)) AS tag(k, v) ON TRUE
                """);
        assertThat(withLeft).hasSize(5);
        assertThat(withLeft).anyMatch(r -> "t3".equals(r.getField(0)) && r.getField(1) == null);
    }

    @Test
    void aggregateUdfWeightsByItsSecondArgument() {
        // A non-windowed aggregate over a bounded input is still a *streaming* query: it emits a running
        // result after every row (+I, then -U/+U pairs). The last row is the final answer, and needing to say
        // so is itself the lesson - this is the "updating" changelog mode that decides which sinks a query can
        // be written to.
        List<Row> rows = collect("""
                SELECT weighted_avg(v, w) FROM (
                    VALUES (10.0E0, 1.0E0), (20.0E0, 3.0E0)
                ) AS t (v, w)
                """);

        assertThat(rows).hasSizeGreaterThan(1);
        // (10*1 + 20*3) / 4
        assertThat((Double) rows.get(rows.size() - 1).getField(0)).isEqualTo(17.5);
    }

    /**
     * The window TVF shape used throughout {@code 30-windows.sql}, including the COUNT(DISTINCT shard_id) that
     * the docs use as evidence of a cross-shard shuffle.
     */
    @Test
    void tumblingWindowTvfGroupsByMinute() {
        List<Row> rows = collect("""
                SELECT window_start, account_id, COUNT(*) AS n, SUM(amount) AS total,
                       COUNT(DISTINCT shard_id) AS shards
                FROM TABLE(TUMBLE(TABLE txn, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
                GROUP BY window_start, window_end, account_id
                """);

        assertThat(rows).hasSize(3);   // acct-1 minute 0, acct-2 minute 0, acct-1 minute 1

        Row acct1Minute0 = rows.stream()
                .filter(r -> "acct-1".equals(r.getField(1))
                        && r.getField(0).toString().startsWith("2026-01-01T10:00"))
                .findFirst().orElseThrow();
        assertThat(acct1Minute0.getField(2)).isEqualTo(2L);
        assertThat(acct1Minute0.getField(4)).as("two distinct shards").isEqualTo(2L);
    }

    /** Window Top-N: the ROW_NUMBER-over-window-columns shape the planner recognises. */
    @Test
    void windowTopNRanksWithinEachWindow() {
        List<Row> rows = collect("""
                SELECT window_start, rnk, merchant_id FROM (
                    SELECT window_start, merchant_id, total,
                           ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total DESC) AS rnk
                    FROM (
                        SELECT window_start, window_end, merchant_id, SUM(amount) AS total
                        FROM TABLE(TUMBLE(TABLE txn, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
                        GROUP BY window_start, window_end, merchant_id
                    )
                ) WHERE rnk <= 2
                """);

        // Minute 0: mch-1 (10+30=40) then mch-2 (20). Minute 1: mch-3 only.
        assertThat(rows).hasSize(3);
        Row top = rows.stream()
                .filter(r -> r.getField(0).toString().startsWith("2026-01-01T10:00") && Long.valueOf(1L).equals(r.getField(1)))
                .findFirst().orElseThrow();
        assertThat(top.getField(2)).isEqualTo("mch-1");
    }

    /**
     * Reproduces the greedy-quantifier restriction that {@code 50-patterns.sql} documents, so a Flink upgrade
     * that relaxes it does not go unnoticed.
     */
    @Test
    void aGreedyQuantifierCannotEndAMatchRecognizePattern() {
        assertThatThrownBy(() -> tEnv.executeSql("""
                SELECT * FROM txn
                MATCH_RECOGNIZE (
                    PARTITION BY account_id
                    ORDER BY event_time
                    MEASURES FIRST(a.txn_id) AS first_id
                    ONE ROW PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (a b{2,})
                    DEFINE a AS a.amount > 0, b AS b.amount > 0
                )
                """))
                .hasStackTraceContaining("Greedy quantifiers are not allowed as the last element");
    }

    @Test
    void theReluctantFormIsAccepted() {
        List<Row> rows = collect("""
                SELECT account_id, first_id, last_id FROM txn
                MATCH_RECOGNIZE (
                    PARTITION BY account_id
                    ORDER BY event_time
                    MEASURES FIRST(a.txn_id) AS first_id, LAST(b.txn_id) AS last_id
                    ONE ROW PER MATCH
                    AFTER MATCH SKIP PAST LAST ROW
                    PATTERN (a b{2,}?)
                    DEFINE
                        a AS a.amount <= 10.00,
                        b AS b.amount > 10.00
                )
                """);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("acct-1");
        assertThat(rows.get(0).getField(1)).isEqualTo("t1");
        assertThat(rows.get(0).getField(2)).isEqualTo("t4");
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private Object single(String sql) {
        List<Row> rows = collect(sql);
        assertThat(rows).hasSize(1);
        return rows.get(0).getField(0);
    }

    private List<Row> collect(String sql) {
        Table table = tEnv.sqlQuery(sql);
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = table.execute().collect()) {
            it.forEachRemaining(rows::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rows;
    }
}
