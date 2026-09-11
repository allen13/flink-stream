package io.flinkstream.functions;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * Accumulator for the windowed aggregates.
 *
 * <p>This is what {@code aggregate()} keeps in state <em>per open window</em> - not the records themselves. A
 * window holding a thousand transactions still costs one accumulator, which is the entire reason to prefer
 * {@code aggregate(AggregateFunction, ProcessWindowFunction)} over a bare {@code process(ProcessWindowFunction)}:
 * the latter buffers every record until the window fires.
 *
 * <p>The {@link Set} fields are the exception: they grow with cardinality, and Flink serializes them with Kryo
 * because {@code HashSet} is not a POJO field type it can analyse. For a production job with high merchant
 * cardinality you would swap them for a HyperLogLog sketch; here the exact count is more instructive.
 */
public class TxnAccumulator {

    public long count;
    public BigDecimal total = BigDecimal.ZERO;
    public BigDecimal max = BigDecimal.ZERO;
    public Set<String> merchants = new HashSet<>();
    public Set<String> channels = new HashSet<>();
    public String region;

    public TxnAccumulator() {}
}
