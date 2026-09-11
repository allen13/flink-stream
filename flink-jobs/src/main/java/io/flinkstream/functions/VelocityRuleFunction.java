package io.flinkstream.functions;

import io.flinkstream.avro.FraudAlert;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Domains;
import io.flinkstream.common.Money;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * <b>Lab F - keyed state, event-time timers and state TTL.</b> Raises an alert when an account spends more than
 * {@code amountThreshold} across {@code countThreshold}+ transactions inside a rolling window.
 *
 * <p>This is the "do it by hand" counterpart to Lab D's window operator, and the comparison is the point:
 *
 * <ul>
 *   <li>A {@code KeyedProcessFunction} gives full control over <em>when</em> to emit. The window operator can
 *       only emit at window boundaries; here the alert fires the moment the threshold is crossed.</li>
 *   <li>The timer registered at {@code eventTime + window} is what expires the counters. Timers are keyed,
 *       checkpointed state - they survive restarts, and they fire on watermark advance, not wall clock.</li>
 *   <li>{@link StateTtlConfig} is the safety net. An account that goes quiet forever would otherwise leave its
 *       counters in RocksDB indefinitely, since the timer only fires if a watermark passes it. TTL with the
 *       RocksDB compaction filter cleans those up in the background.</li>
 * </ul>
 */
public class VelocityRuleFunction extends KeyedProcessFunction<String, Transaction, FraudAlert> {

    private static final long serialVersionUID = 1L;

    private final int countThreshold;
    private final BigDecimal amountThreshold;
    private final Duration window;
    private final Duration ttl;

    private transient ValueState<Long> countState;
    private transient ValueState<BigDecimal> totalState;
    private transient ValueState<Long> timerState;
    private transient ListState<String> txnIdsState;
    private transient Counter alertsFired;

    public VelocityRuleFunction(int countThreshold, BigDecimal amountThreshold, Duration window, Duration ttl) {
        this.countThreshold = countThreshold;
        this.amountThreshold = amountThreshold;
        this.window = window;
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                // Without an explicit cleanup strategy, expired entries are only dropped when read. The RocksDB
                // compaction filter removes them during normal compaction instead, so state actually shrinks.
                .cleanupInRocksdbCompactFilter(1_000L)
                .build();

        ValueStateDescriptor<Long> count = new ValueStateDescriptor<>("velocity-count", Types.LONG);
        ValueStateDescriptor<BigDecimal> total = new ValueStateDescriptor<>("velocity-total", Types.BIG_DEC);
        ValueStateDescriptor<Long> timer = new ValueStateDescriptor<>("velocity-timer", Types.LONG);
        ListStateDescriptor<String> txnIds = new ListStateDescriptor<>("velocity-txn-ids", Types.STRING);

        count.enableTimeToLive(ttlConfig);
        total.enableTimeToLive(ttlConfig);
        timer.enableTimeToLive(ttlConfig);
        txnIds.enableTimeToLive(ttlConfig);

        countState = getRuntimeContext().getState(count);
        totalState = getRuntimeContext().getState(total);
        timerState = getRuntimeContext().getState(timer);
        txnIdsState = getRuntimeContext().getListState(txnIds);

        alertsFired = getRuntimeContext().getMetricGroup().counter("velocityAlerts");
    }

    @Override
    public void processElement(Transaction txn, Context ctx, Collector<FraudAlert> out) throws Exception {

        long count = orZero(countState.value()) + 1;
        BigDecimal total = Money.add(totalState.value(), txn.getAmount());

        countState.update(count);
        totalState.update(total);
        txnIdsState.add(txn.getTxnId());

        // One timer per burst, not one per record: registering the same timestamp twice is a no-op, but tracking
        // it explicitly keeps the timer count proportional to bursts rather than to throughput.
        if (timerState.value() == null) {
            long fireAt = ctx.timestamp() + window.toMillis();
            ctx.timerService().registerEventTimeTimer(fireAt);
            timerState.update(fireAt);
        }

        if (count >= countThreshold && total.compareTo(amountThreshold) >= 0) {
            List<String> ids = new ArrayList<>();
            txnIdsState.get().forEach(ids::add);

            alertsFired.inc();
            out.collect(FraudAlert.newBuilder()
                    .setAlertId(UUID.randomUUID().toString())
                    .setAccountId(ctx.getCurrentKey())
                    .setRule("VELOCITY")
                    .setSeverity(total.compareTo(amountThreshold.multiply(BigDecimal.valueOf(2))) >= 0
                            ? Domains.SEVERITY_CRITICAL : Domains.SEVERITY_WARN)
                    .setDescription(count + " transactions totalling " + total + " USD within "
                            + window.toMinutes() + "m")
                    .setTxnIds(ids)
                    .setTotalUsd(Money.normalize(total))
                    .setDetectedAt(Instant.now())
                    .setEventTime(txn.getEventTime())
                    .build());

            // Reset so one sustained spender does not emit an alert per transaction forever.
            clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAlert> out) throws Exception {
        // The burst window closed without crossing the threshold - forget it.
        clear();
    }

    private void clear() {
        countState.clear();
        totalState.clear();
        timerState.clear();
        txnIdsState.clear();
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
    }
}
