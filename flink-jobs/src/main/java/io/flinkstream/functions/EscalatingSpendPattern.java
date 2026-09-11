package io.flinkstream.functions;

import io.flinkstream.avro.FraudAlert;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Domains;
import io.flinkstream.common.Money;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * <b>Lab G - CEP.</b> Detects card-testing: a small card-not-present probe followed by two or more escalating
 * charges on the same account within ten minutes.
 *
 * <p>This is the DataStream counterpart to the {@code MATCH_RECOGNIZE} query in
 * {@code sql/04-pattern-matching.sql}; running both over the same topic and diffing the alerts is a good way to
 * build intuition for what the SQL clause actually compiles to.
 *
 * <p>Worth knowing:
 *
 * <ul>
 *   <li>{@code next} means strictly contiguous; {@code followedBy} allows unrelated events in between;
 *       {@code followedByAny} additionally allows overlapping matches. The choice drives both semantics and how
 *       much partial-match state NFA keeps.</li>
 *   <li>{@link IterativeCondition} can look at events already matched by earlier parts of the pattern - that is
 *       how "each charge larger than the last" is expressed.</li>
 *   <li>{@code within} is what bounds the state. A pattern without it accumulates partial matches forever.</li>
 *   <li>CEP requires a keyed stream to be parallel at all; on a non-keyed stream the NFA runs at parallelism 1.</li>
 * </ul>
 */
public final class EscalatingSpendPattern {

    public static final BigDecimal PROBE_MAX = new BigDecimal("5.00");

    private EscalatingSpendPattern() {}

    public static Pattern<Transaction, Transaction> pattern() {
        return Pattern.<Transaction>begin("probe")
                .where(SimpleCondition.of(txn ->
                        Domains.CHANNEL_CARD_NOT_PRESENT.equals(txn.getChannel())
                                && txn.getAmount().compareTo(PROBE_MAX) <= 0))
                .followedBy("escalation")
                .where(new IterativeCondition<>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(Transaction txn, Context<Transaction> ctx) throws Exception {
                        if (!Domains.CHANNEL_CARD_NOT_PRESENT.equals(txn.getChannel())) {
                            return false;
                        }
                        // Must be strictly larger than every charge already matched by this pattern instance.
                        BigDecimal highest = PROBE_MAX;
                        for (Transaction previous : ctx.getEventsForPattern("escalation")) {
                            highest = Money.max(highest, previous.getAmount());
                        }
                        return txn.getAmount().compareTo(highest) > 0;
                    }
                })
                .timesOrMore(2)
                .within(Duration.ofMinutes(10));
    }

    public static PatternProcessFunction<Transaction, FraudAlert> alertBuilder() {
        return new PatternProcessFunction<>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void processMatch(Map<String, List<Transaction>> match, Context ctx,
                                     Collector<FraudAlert> out) {

                List<Transaction> all = new ArrayList<>();
                match.getOrDefault("probe", List.of()).forEach(all::add);
                match.getOrDefault("escalation", List.of()).forEach(all::add);

                BigDecimal total = Money.ZERO;
                List<String> ids = new ArrayList<>(all.size());
                for (Transaction txn : all) {
                    total = Money.add(total, txn.getAmount());
                    ids.add(txn.getTxnId());
                }

                out.collect(FraudAlert.newBuilder()
                        .setAlertId(UUID.randomUUID().toString())
                        .setAccountId(all.get(0).getAccountId())
                        .setRule("CARD_TESTING_CEP")
                        .setSeverity(Domains.SEVERITY_CRITICAL)
                        .setDescription("Probe of " + all.get(0).getAmount() + " followed by "
                                + (all.size() - 1) + " escalating card-not-present charges")
                        .setTxnIds(ids)
                        .setTotalUsd(Money.normalize(total))
                        .setDetectedAt(Instant.now())
                        .setEventTime(all.get(all.size() - 1).getEventTime())
                        .build());
            }
        };
    }
}
