package io.flinkstream.functions;

import io.flinkstream.avro.AccountWindowStats;
import io.flinkstream.common.Money;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Turns a finished window's accumulator into an output record.
 *
 * <p>Pairing a {@code ProcessWindowFunction} with an {@code AggregateFunction} is the pattern worth internalising:
 * the aggregate keeps state small, and this function runs once per window with access to the window metadata
 * ({@code start}, {@code end}), the key, and per-window state. On its own, a {@code ProcessWindowFunction} would
 * have to buffer every record in the window.
 *
 * @param windowKind label carried into the output so TUMBLE / HOP / SESSION results can share one topic
 * @param keyIsRegion whether the key is a region (cross-shard aggregate) or an account id
 */
public class WindowStatsFunction
        extends ProcessWindowFunction<TxnAccumulator, AccountWindowStats, String, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final String windowKind;
    private final boolean keyIsRegion;

    private transient int subtask;

    public WindowStatsFunction(String windowKind, boolean keyIsRegion) {
        this.windowKind = windowKind;
        this.keyIsRegion = keyIsRegion;
    }

    @Override
    public void open(OpenContext openContext) {
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public void process(String key, Context ctx, Iterable<TxnAccumulator> accumulators,
                        Collector<AccountWindowStats> out) {

        // With an AggregateFunction upstream the iterable always holds exactly one pre-folded accumulator.
        TxnAccumulator acc = accumulators.iterator().next();

        out.collect(AccountWindowStats.newBuilder()
                .setAccountId(keyIsRegion ? "REGION:" + key : key)
                .setRegion(keyIsRegion ? key : acc.region)
                .setWindowStart(Instant.ofEpochMilli(ctx.window().getStart()))
                .setWindowEnd(Instant.ofEpochMilli(ctx.window().getEnd()))
                .setWindowKind(windowKind)
                .setTxnCount(acc.count)
                .setTotalUsd(widen(acc.total))
                .setMaxUsd(widen(acc.max))
                .setDistinctMerchants(acc.merchants.size())
                .setDistinctChannels(acc.channels.size())
                .setLateEvents(0L)
                .setSubtaskIndex(subtask)
                .build());
    }

    private static BigDecimal widen(BigDecimal value) {
        return Money.normalize(value);
    }
}
