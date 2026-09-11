package io.flinkstream.functions;

import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Money;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * <b>Lab D - incremental aggregation.</b> Folds transactions into a {@link TxnAccumulator} as they arrive.
 *
 * <p>{@link #merge} is only called for session windows, where two in-flight windows turn out to be part of the
 * same session and have to be combined. Getting it wrong is a classic source of silently wrong session results,
 * so it is implemented here even though the tumbling-window path never calls it.
 */
public class TransactionAggregate implements AggregateFunction<Transaction, TxnAccumulator, TxnAccumulator> {

    private static final long serialVersionUID = 1L;

    @Override
    public TxnAccumulator createAccumulator() {
        return new TxnAccumulator();
    }

    @Override
    public TxnAccumulator add(Transaction txn, TxnAccumulator acc) {
        acc.count++;
        acc.total = Money.add(acc.total, txn.getAmount());
        acc.max = Money.max(acc.max, txn.getAmount());
        acc.merchants.add(txn.getMerchantId());
        acc.channels.add(txn.getChannel());
        return acc;
    }

    @Override
    public TxnAccumulator getResult(TxnAccumulator acc) {
        return acc;
    }

    @Override
    public TxnAccumulator merge(TxnAccumulator a, TxnAccumulator b) {
        TxnAccumulator merged = new TxnAccumulator();
        merged.count = a.count + b.count;
        merged.total = Money.add(a.total, b.total);
        merged.max = Money.max(a.max, b.max);
        merged.merchants.addAll(a.merchants);
        merged.merchants.addAll(b.merchants);
        merged.channels.addAll(a.channels);
        merged.channels.addAll(b.channels);
        merged.region = a.region != null ? a.region : b.region;
        return merged;
    }
}
