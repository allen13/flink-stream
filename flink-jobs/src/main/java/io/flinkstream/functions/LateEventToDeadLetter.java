package io.flinkstream.functions;

import io.flinkstream.avro.DeadLetter;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Topics;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.Instant;
import java.util.UUID;

/**
 * Converts records dropped by a window's lateness policy into {@link DeadLetter}s.
 *
 * <p>A record is "late" when its timestamp falls into a window whose {@code end + allowedLateness} the watermark
 * has already passed. Without {@code sideOutputLateData} those records are silently discarded and the only
 * evidence is the {@code numLateRecordsDropped} metric. Routing them to a topic instead means a real pipeline can
 * reprocess or at least account for them.
 */
public class LateEventToDeadLetter extends RichMapFunction<Transaction, DeadLetter> {

    private static final long serialVersionUID = 1L;

    private transient int subtask;

    @Override
    public void open(OpenContext openContext) {
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public DeadLetter map(Transaction txn) {
        return DeadLetter.newBuilder()
                .setDlqId(UUID.randomUUID().toString())
                .setSourceTopic(Topics.TRANSACTIONS)
                .setRecordKey(txn.getAccountId())
                .setReason("LATE_EVENT")
                .setDetail("txn_id=" + txn.getTxnId() + " event_time=" + txn.getEventTime()
                        + " arrived after its window had been emitted")
                .setStage("TumblingWindow(1m)")
                .setSubtaskIndex(subtask)
                .setOccurredAt(Instant.now())
                .build();
    }
}
