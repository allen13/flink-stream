package io.flinkstream.functions;

import io.flinkstream.avro.FraudAlert;
import io.flinkstream.avro.Transaction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit-tests {@link VelocityRuleFunction} through a {@link KeyedOneInputStreamOperatorTestHarness}.
 *
 * <p>The harness drives a single operator with full control over element timestamps, watermarks and processing
 * time - which is the only practical way to test timer-driven logic. A MiniCluster test would have to wait for
 * real watermarks to arrive; here a watermark is one method call.
 */
class VelocityRuleFunctionTest {

    private static final String ACCOUNT = "acct-00001";

    private KeyedOneInputStreamOperatorTestHarness<String, Transaction, FraudAlert> harness;

    @BeforeEach
    void setUp() throws Exception {
        VelocityRuleFunction function = new VelocityRuleFunction(
                3,                              // countThreshold
                new BigDecimal("1000.00"),      // amountThreshold
                Duration.ofMinutes(3),          // burst window
                Duration.ofHours(1));           // state TTL

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                Transaction::getAccountId,
                Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void firesOnceBothThresholdsAreCrossed() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        // Two transactions: enough value, not enough count.
        harness.processElement(txn(600.00), t0);
        harness.processElement(txn(600.00), t0 + 10_000);
        assertThat(harness.extractOutputValues()).isEmpty();

        // The third crosses the count threshold with the total already past 1000.
        harness.processElement(txn(600.00), t0 + 20_000);

        List<FraudAlert> alerts = harness.extractOutputValues();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRule()).isEqualTo("VELOCITY");
        assertThat(alerts.get(0).getAccountId()).isEqualTo(ACCOUNT);
        assertThat(alerts.get(0).getTxnIds()).hasSize(3);
        assertThat(alerts.get(0).getTotalUsd()).isEqualByComparingTo(new BigDecimal("1800.00"));
        // 1800 is past the 1000 threshold but short of 2x it.
        assertThat(alerts.get(0).getSeverity()).isEqualTo("WARN");
    }

    @Test
    void escalatesToCriticalAtTwiceTheAmountThreshold() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        harness.processElement(txn(700.00), t0);
        harness.processElement(txn(700.00), t0 + 1_000);
        harness.processElement(txn(700.00), t0 + 2_000);   // 2100 >= 2 x 1000

        List<FraudAlert> alerts = harness.extractOutputValues();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getSeverity()).isEqualTo("CRITICAL");
    }

    @Test
    void doesNotFireWhenTheValueThresholdIsNotReached() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        for (int i = 0; i < 6; i++) {
            harness.processElement(txn(10.00), t0 + i * 1_000L);
        }

        assertThat(harness.extractOutputValues()).isEmpty();
    }

    /**
     * The behaviour the event-time timer exists for: a burst that fails to complete inside the window must not
     * combine with a later, unrelated burst.
     */
    @Test
    void theTimerForgetsAnIncompleteBurst() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        harness.processElement(txn(600.00), t0);
        harness.processElement(txn(600.00), t0 + 1_000);

        // Advance past t0 + 3 minutes, which fires the timer registered on the first element and clears state.
        harness.processWatermark(t0 + Duration.ofMinutes(4).toMillis());
        assertThat(harness.extractOutputValues()).isEmpty();

        // A single later transaction must not resurrect the earlier count.
        harness.processElement(txn(600.00), t0 + Duration.ofMinutes(5).toMillis());
        assertThat(harness.extractOutputValues())
                .as("state should have been cleared by the timer")
                .isEmpty();
    }

    /** After firing, counters reset - one sustained spender must not emit an alert per transaction forever. */
    @Test
    void resetsAfterFiring() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        for (int i = 0; i < 3; i++) {
            harness.processElement(txn(600.00), t0 + i * 1_000L);
        }
        assertThat(harness.extractOutputValues()).hasSize(1);

        // Two more are not enough to re-trip the count threshold from zero.
        harness.processElement(txn(600.00), t0 + 10_000);
        harness.processElement(txn(600.00), t0 + 11_000);
        assertThat(harness.extractOutputValues()).hasSize(1);

        harness.processElement(txn(600.00), t0 + 12_000);
        assertThat(harness.extractOutputValues()).hasSize(2);
    }

    /** Keyed state is per account: one account's burst must not leak into another's counters. */
    @Test
    void stateIsIsolatedPerAccount() throws Exception {
        long t0 = Instant.parse("2026-01-01T10:00:00Z").toEpochMilli();

        harness.processElement(txn(600.00), t0);
        harness.processElement(txnFor("acct-00002", 600.00), t0 + 1_000);
        harness.processElement(txn(600.00), t0 + 2_000);
        assertThat(harness.extractOutputValues()).isEmpty();

        harness.processElement(txn(600.00), t0 + 3_000);
        assertThat(harness.extractOutputValues()).hasSize(1);
    }

    private static Transaction txn(double amount) {
        return txnFor(ACCOUNT, amount);
    }

    private static Transaction txnFor(String accountId, double amount) {
        return Transaction.newBuilder()
                .setTxnId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setMerchantId("mch-0001")
                .setAmount(BigDecimal.valueOf(amount).setScale(2))
                .setCurrency("USD")
                .setChannel("CARD_NOT_PRESENT")
                .setDeviceId("dev-1")
                .setGeo(null)
                .setTags(new HashMap<>())
                .setShardId(0)
                .setEventTime(Instant.ofEpochMilli(0))
                .build();
    }
}
