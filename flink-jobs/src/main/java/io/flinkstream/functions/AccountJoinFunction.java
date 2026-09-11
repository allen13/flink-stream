package io.flinkstream.functions;

import io.flinkstream.avro.Account;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.RegionalTxn;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * <b>Lab H - hand-written stream-stream join with a dimension.</b> Joins transactions to the latest version of
 * their account.
 *
 * <p>This is a temporal (versioned) join written out in full, and it is worth reading beside the SQL version in
 * {@code sql/03-temporal-joins.sql}: {@code FOR SYSTEM_TIME AS OF} compiles to almost exactly this operator.
 *
 * <p>The two problems any such join must solve:
 *
 * <ul>
 *   <li><b>Arrival order.</b> A transaction can arrive before the account record that describes it. Those are
 *       parked in {@code pendingState} and released when the account shows up. Dropping them instead is the most
 *       common bug in hand-written enrichment joins, and it only shows up at startup or after a rescale.</li>
 *   <li><b>Unbounded state.</b> The dimension side never expires on its own - a closed account would be kept
 *       forever. TTL bounds it, at the cost of re-reading the compacted topic after a long idle period.</li>
 * </ul>
 */
public class AccountJoinFunction extends KeyedCoProcessFunction<String, Transaction, Account, RegionalTxn> {

    private static final long serialVersionUID = 1L;

    private final Duration ttl;

    private transient ValueState<Account> accountState;
    private transient ListState<Transaction> pendingState;
    private transient Counter joined;
    private transient Counter buffered;
    private transient Counter dropped;

    public AccountJoinFunction(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1_000L)
                .build();

        ValueStateDescriptor<Account> account =
                new ValueStateDescriptor<>("account-version", TypeInformation.of(Account.class));
        account.enableTimeToLive(ttlConfig);
        accountState = getRuntimeContext().getState(account);

        // Buffer is deliberately short-lived: a transaction waiting more than a few minutes for its account is
        // a data problem, not a timing problem.
        ListStateDescriptor<Transaction> pending =
                new ListStateDescriptor<>("pending-txns", TypeInformation.of(Transaction.class));
        pending.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMinutes(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1_000L)
                .build());
        pendingState = getRuntimeContext().getListState(pending);

        joined = getRuntimeContext().getMetricGroup().counter("accountJoined");
        buffered = getRuntimeContext().getMetricGroup().counter("accountBuffered");
        dropped = getRuntimeContext().getMetricGroup().counter("accountDropped");
    }

    /** Fact side. */
    @Override
    public void processElement1(Transaction txn, Context ctx, Collector<RegionalTxn> out) throws Exception {
        Account account = accountState.value();
        if (account == null) {
            buffered.inc();
            pendingState.add(txn);
            // Give the dimension a grace period; after that the transaction is released unenriched rather than
            // silently lost.
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + Duration.ofMinutes(2).toMillis());
            return;
        }
        joined.inc();
        out.collect(toRegional(txn, account));
    }

    /** Dimension side. */
    @Override
    public void processElement2(Account account, Context ctx, Collector<RegionalTxn> out) throws Exception {
        Account current = accountState.value();
        // Compacted topics replay out of order after a restart, so only move the version forward.
        if (current == null || account.getUpdatedAt().isAfter(current.getUpdatedAt())) {
            accountState.update(account);
        }

        Account effective = accountState.value();
        List<Transaction> released = new ArrayList<>();
        for (Transaction pending : pendingState.get()) {
            released.add(pending);
        }
        if (!released.isEmpty()) {
            pendingState.clear();
            for (Transaction txn : released) {
                joined.inc();
                out.collect(toRegional(txn, effective));
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RegionalTxn> out) throws Exception {
        List<Transaction> stranded = new ArrayList<>();
        for (Transaction txn : pendingState.get()) {
            stranded.add(txn);
        }
        if (stranded.isEmpty()) {
            return;
        }
        pendingState.clear();
        for (Transaction txn : stranded) {
            dropped.inc();
            out.collect(toRegional(txn, null));
        }
    }

    private static RegionalTxn toRegional(Transaction txn, Account account) {
        return new RegionalTxn(
                txn.getTxnId(),
                txn.getAccountId(),
                txn.getMerchantId(),
                account == null ? "UNKNOWN" : account.getRegion(),
                account == null ? "UNKNOWN" : account.getRiskBand(),
                txn.getChannel(),
                txn.getAmount(),
                txn.getShardId(),
                txn.getEventTime().toEpochMilli());
    }
}
