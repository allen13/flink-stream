package io.flinkstream.functions;

import io.flinkstream.avro.AuthEvent;
import io.flinkstream.avro.EnrichedTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * <b>Lab C - interval join.</b> Matches each transaction with the authorization decision that followed it.
 *
 * <p>An interval join pairs records whose timestamps satisfy
 * {@code left.ts + lowerBound <= right.ts <= left.ts + upperBound}. Flink buffers both sides in keyed state and
 * evicts a record as soon as the watermark proves no partner can still arrive - so unlike a regular (unbounded)
 * join, state is bounded by the interval width and the event rate rather than by the whole history.
 *
 * <p>This is also a <em>second</em> shuffle: the streams arrive keyed by {@code account_id} from Kafka but are
 * joined on {@code txn_id}, so both sides are redistributed across every subtask.
 *
 * <p>Interval join is inner-join only. Transactions whose auth never arrives simply vanish, which is why the job
 * sinks the pre-join stream too.
 */
public class AuthJoinFunction extends ProcessJoinFunction<EnrichedTransaction, AuthEvent, EnrichedTransaction> {

    private static final long serialVersionUID = 1L;

    private transient Counter matched;

    @Override
    public void open(OpenContext openContext) {
        matched = getRuntimeContext().getMetricGroup().counter("authMatched");
    }

    @Override
    public void processElement(EnrichedTransaction txn, AuthEvent auth, Context ctx,
                               Collector<EnrichedTransaction> out) {
        matched.inc();
        out.collect(EnrichedTransaction.newBuilder(txn)
                .setAuthDecision(auth.getDecision())
                .setAuthLatencyMs(auth.getLatencyMs())
                .build());
    }
}
