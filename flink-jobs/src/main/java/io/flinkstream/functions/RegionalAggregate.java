package io.flinkstream.functions;

import io.flinkstream.common.Money;
import io.flinkstream.common.RegionalTxn;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * <b>Lab I - cross-shard aggregation.</b> Same accumulator as {@link TransactionAggregate}, but fed by a stream
 * keyed on {@code region}.
 *
 * <p>Transactions are sharded by {@code account_id}, and accounts in one region are spread uniformly over all 12
 * shards. So every one of the five region keys draws from all 12 shards, and the {@code keyBy(region)} ahead of
 * this aggregate is a complete all-to-all shuffle. The instructive part is that the aggregate still runs
 * <em>incrementally at the shuffle target</em>: only accumulators live in state, and the amount of data crossing
 * the network is the full transaction stream exactly once, not once per window.
 */
public class RegionalAggregate implements AggregateFunction<RegionalTxn, TxnAccumulator, TxnAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public TxnAccumulator createAccumulator() {
        return new TxnAccumulator();
    }

    @Override
    public TxnAccumulator add(RegionalTxn txn, TxnAccumulator acc) {
        acc.count++;
        acc.total = Money.add(acc.total, txn.amount);
        acc.max = Money.max(acc.max, txn.amount);
        acc.merchants.add(txn.merchantId);
        acc.channels.add(txn.channel);
        acc.region = txn.region;
        return acc;
    }

    @Override
    public TxnAccumulator getResult(TxnAccumulator acc) {
        return acc;
    }

    @Override
    public TxnAccumulator merge(TxnAccumulator a, TxnAccumulator b) {
        return new TransactionAggregate().merge(a, b);
    }
}
