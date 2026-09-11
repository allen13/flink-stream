package io.flinkstream.functions;

import io.flinkstream.avro.DeadLetter;
import io.flinkstream.avro.EnrichedTransaction;
import io.flinkstream.avro.Merchant;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Money;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

/**
 * <b>Lab A - broadcast state.</b> Enriches every transaction with its merchant.
 *
 * <p>The merchant dimension is small and changes slowly, so instead of shuffling it by {@code merchant_id} and
 * paying for a keyed join, every subtask keeps a full copy in <em>broadcast state</em>. The dimension stream is
 * replicated to all subtasks; the fact stream stays keyed by {@code account_id} and never moves.
 *
 * <p>Things worth noticing:
 *
 * <ul>
 *   <li>{@code processBroadcastElement} may write broadcast state; {@code processElement} only gets a
 *       {@link ReadOnlyBroadcastState}. Flink enforces this because every subtask must converge on identical
 *       broadcast state - if subtasks could mutate it from the keyed side they would diverge and restores
 *       would be non-deterministic.</li>
 *   <li>There is no ordering guarantee between the two inputs. Early transactions can arrive before the
 *       merchant that explains them, which is exactly what the DLQ side output is for.</li>
 *   <li>Broadcast state is kept on heap and checkpointed once per subtask, so this pattern only works while
 *       the dimension is small (thousands, not millions, of rows).</li>
 * </ul>
 */
public class MerchantEnrichmentFunction
        extends KeyedBroadcastProcessFunction<String, Transaction, Merchant, EnrichedTransaction> {

    private static final long serialVersionUID = 1L;

    /** Shared with the job so the same descriptor object identifies the state on both sides of the connect. */
    public static final MapStateDescriptor<String, Merchant> MERCHANT_STATE =
            new MapStateDescriptor<>("merchants", Types.STRING, TypeInformation.of(Merchant.class));

    public static final OutputTag<DeadLetter> DLQ = new OutputTag<>("dlq-unknown-merchant") {};

    private transient Counter hits;
    private transient Counter misses;
    private transient int subtask;

    @Override
    public void open(OpenContext openContext) {
        // Custom metrics land under flink_taskmanager_job_task_operator_<name> in Prometheus, so they can be
        // graphed next to the built-in numRecordsIn/Out.
        hits = getRuntimeContext().getMetricGroup().counter("merchantHits");
        misses = getRuntimeContext().getMetricGroup().counter("merchantMisses");
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public void processBroadcastElement(Merchant merchant, Context ctx, Collector<EnrichedTransaction> out)
            throws Exception {
        ctx.getBroadcastState(MERCHANT_STATE).put(merchant.getMerchantId(), merchant);
    }

    @Override
    public void processElement(Transaction txn, ReadOnlyContext ctx, Collector<EnrichedTransaction> out)
            throws Exception {

        Merchant merchant = ctx.getBroadcastState(MERCHANT_STATE).get(txn.getMerchantId());

        if (merchant == null) {
            misses.inc();
            ctx.output(DLQ, DeadLetter.newBuilder()
                    .setDlqId(UUID.randomUUID().toString())
                    .setSourceTopic(io.flinkstream.common.Topics.TRANSACTIONS)
                    .setRecordKey(txn.getAccountId())
                    .setReason("UNKNOWN_MERCHANT")
                    .setDetail("merchant_id=" + txn.getMerchantId() + " not yet in broadcast state")
                    .setStage("MerchantEnrichmentFunction")
                    .setSubtaskIndex(subtask)
                    .setOccurredAt(Instant.now())
                    .build());
        } else {
            hits.inc();
        }

        out.collect(EnrichedTransaction.newBuilder()
                .setTxnId(txn.getTxnId())
                .setAccountId(txn.getAccountId())
                .setMerchantId(txn.getMerchantId())
                .setMerchantName(merchant == null ? null : merchant.getName())
                .setMerchantCategory(merchant == null ? null : merchant.getCategory())
                .setMerchantTrust(merchant == null ? null : merchant.getTrustScore())
                .setAmountUsd(Money.normalize(txn.getAmount()))
                .setCurrency(txn.getCurrency())
                .setChannel(txn.getChannel())
                .setAuthDecision(null)
                .setAuthLatencyMs(null)
                .setRiskScore(0.0d)
                .setRulesFired(new ArrayList<>())
                .setShardId(txn.getShardId())
                .setSubtaskIndex(subtask)
                .setEventTime(txn.getEventTime())
                .setProcessedAt(Instant.now())
                .build());
    }
}
