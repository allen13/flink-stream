package io.flinkstream.functions;

import io.flinkstream.avro.EnrichedTransaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * <b>Lab B - Async I/O.</b> Attaches a risk score that "comes from" an external scoring service.
 *
 * <p>A plain {@code map} that calls a remote service blocks its whole subtask for the duration of the call, so
 * throughput collapses to {@code parallelism / latency}. {@code AsyncDataStream} instead keeps up to
 * {@code capacity} requests in flight per subtask and only the completion is handled on the operator thread.
 *
 * <p>Rules this class exists to demonstrate:
 *
 * <ul>
 *   <li><b>Never block in {@code asyncInvoke}.</b> The work happens on a separate executor; {@code asyncInvoke}
 *       only hands off and returns.</li>
 *   <li><b>Always override {@code timeout}.</b> The default implementation fails the job. Completing with a
 *       fallback keeps a slow dependency from turning into a restart loop.</li>
 *   <li><b>Unordered vs ordered.</b> {@code unorderedWait} emits results as they finish, which is faster;
 *       {@code orderedWait} buffers them back into input order. Under event time, "unordered" still respects
 *       watermarks - results are never emitted across a watermark boundary out of order.</li>
 *   <li>The in-flight requests are part of the operator's checkpointed state, so exactly-once still holds:
 *       on restore, outstanding requests are replayed.</li>
 * </ul>
 */
public class AsyncRiskScoreFunction extends RichAsyncFunction<EnrichedTransaction, EnrichedTransaction> {

    private static final long serialVersionUID = 1L;

    private final int simulatedLatencyMs;

    private transient ExecutorService executor;

    public AsyncRiskScoreFunction(int simulatedLatencyMs) {
        this.simulatedLatencyMs = simulatedLatencyMs;
    }

    @Override
    public void open(Configuration parameters) {
        // A real job would open an async HTTP client or a connection pool here. The pool must be sized to at
        // least the operator capacity or requests queue up behind each other and the async operator gains nothing.
        executor = Executors.newFixedThreadPool(16, r -> {
            Thread t = new Thread(r, "risk-score-client");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void asyncInvoke(EnrichedTransaction txn, ResultFuture<EnrichedTransaction> resultFuture) {
        CompletableFuture
                .supplyAsync(() -> score(txn), executor)
                .whenComplete((scored, throwable) -> {
                    if (throwable != null) {
                        // Completing exceptionally fails the job. For a score we would rather degrade than stop,
                        // so a failure here is turned into the neutral score instead.
                        resultFuture.complete(Collections.singletonList(withScore(txn, 0.5d)));
                    } else {
                        resultFuture.complete(Collections.singletonList(scored));
                    }
                });
    }

    @Override
    public void timeout(EnrichedTransaction txn, ResultFuture<EnrichedTransaction> resultFuture) {
        resultFuture.complete(Collections.singletonList(withScore(txn, 0.5d)));
    }

    private EnrichedTransaction score(EnrichedTransaction txn) {
        try {
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1, Math.max(2, simulatedLatencyMs)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // A stand-in model: unfamiliar merchants and card-not-present traffic score higher.
        double trust = txn.getMerchantTrust() == null ? 0.0d : txn.getMerchantTrust();
        double base = 1.0d - trust;
        double channelWeight = switch (txn.getChannel()) {
            case "CARD_NOT_PRESENT" -> 0.35d;
            case "WIRE" -> 0.25d;
            case "ATM" -> 0.15d;
            default -> 0.05d;
        };
        double amountWeight = Math.min(0.3d, txn.getAmountUsd().doubleValue() / 10_000d);
        return withScore(txn, Math.min(1.0d, base * 0.5d + channelWeight + amountWeight));
    }

    private static EnrichedTransaction withScore(EnrichedTransaction txn, double score) {
        // Avro's generated builder copy-constructor gives us an immutable-style update without mutating the
        // input record - important because object reuse is enabled on this job.
        return EnrichedTransaction.newBuilder(txn).setRiskScore(round(score)).build();
    }

    private static double round(double v) {
        return Math.round(v * 1000d) / 1000d;
    }
}
