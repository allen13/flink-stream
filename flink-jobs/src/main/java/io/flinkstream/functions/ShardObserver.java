package io.flinkstream.functions;

import io.flinkstream.avro.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import java.util.BitSet;

/**
 * <b>Lab J - custom metrics.</b> A pass-through operator that reports which shards each subtask is reading.
 *
 * <p>Registers one of each metric type so all four show up in the Flink UI and in Prometheus:
 *
 * <ul>
 *   <li>{@code Counter} - monotonic total</li>
 *   <li>{@code Meter} - rate, derived from a counter by a {@link MeterView}</li>
 *   <li>{@code Gauge} - sampled value, here the number of distinct shards this subtask has seen</li>
 *   <li>{@code Histogram} - distribution, here end-to-end lag in milliseconds</li>
 * </ul>
 *
 * <p>The gauge is the interesting one. With 12 Kafka shards and parallelism 4, each source subtask owns 3 shards,
 * so this gauge reads 3 upstream of any {@code keyBy}. Placed <em>after</em> a {@code keyBy} it reads closer to
 * 12 on every subtask, because the shuffle mixes all shards into every downstream subtask. That difference is
 * what "joining across shards" costs, made visible.
 */
public class ShardObserver extends RichMapFunction<Transaction, Transaction> {

    private static final long serialVersionUID = 1L;

    private final String stage;

    private transient Counter records;
    private transient Meter rate;
    private transient Histogram lagMillis;
    private transient BitSet shardsSeen;

    public ShardObserver(String stage) {
        this.stage = stage;
    }

    @Override
    public void open(OpenContext openContext) {
        var group = getRuntimeContext().getMetricGroup().addGroup("shardObserver", stage);

        shardsSeen = new BitSet();
        records = group.counter("records");
        rate = group.meter("recordsPerSecond", new MeterView(records, 10));
        lagMillis = group.histogram("eventTimeLagMs", new DescriptiveStatisticsHistogram(1000));
        group.gauge("distinctShardsSeen", () -> shardsSeen.cardinality());
        group.gauge("subtaskIndex", () -> getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public Transaction map(Transaction txn) {
        records.inc();
        shardsSeen.set(txn.getShardId());
        lagMillis.update(Math.max(0L, System.currentTimeMillis() - txn.getEventTime().toEpochMilli()));
        return txn;
    }
}
