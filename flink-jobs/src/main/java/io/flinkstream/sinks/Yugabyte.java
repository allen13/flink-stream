package io.flinkstream.sinks;

import io.flinkstream.avro.EnrichedTransaction;
import io.flinkstream.avro.FraudAlert;
import io.flinkstream.common.JobConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import java.sql.Timestamp;
import java.sql.Types;

/**
 * Sinks into YugabyteDB, the serving side of the dual-sink pattern.
 *
 * <p>Yugabyte's YSQL layer speaks the PostgreSQL wire protocol, so the stock Postgres driver and Flink's
 * Postgres dialect work against it unchanged. What differs is underneath: a YSQL table is sharded ("tableted")
 * across nodes by the hash of its primary key, so the choice of primary key here decides how writes spread
 * across the cluster - the same reasoning as choosing a Kafka message key, one layer down.
 *
 * <p><b>Delivery semantics.</b> These sinks are built with {@code buildAtLeastOnce}, and every statement is an
 * {@code INSERT ... ON CONFLICT DO UPDATE} keyed on the record's natural id. Replaying a batch after a restart
 * therefore overwrites rather than duplicates: the sink is at-least-once but the <em>result</em> is idempotent,
 * which is the pragmatic way to get effectively-once into a database.
 *
 * <p>Flink's JDBC connector can also do true exactly-once via XA two-phase commit ({@code buildExactlyOnce}).
 * It is not used here because YugabyteDB does not expose an {@code XADataSource} - a good example of a
 * guarantee that depends on the sink system, not on Flink.
 */
public final class Yugabyte {

    private Yugabyte() {}

    public static JdbcConnectionOptions connection(JobConfig config) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(config.get("yugabyte-url", envOr("YUGABYTE_JDBC_URL",
                        "jdbc:postgresql://yugabyte-ysql:5433/flink_stream")))
                .withDriverName("org.postgresql.Driver")
                .withUsername(config.get("yugabyte-user", envOr("YUGABYTE_USER", "yugabyte")))
                .withPassword(config.get("yugabyte-password", envOr("YUGABYTE_PASSWORD", "yugabyte")))
                .withConnectionCheckTimeoutSeconds(30)
                .build();
    }

    private static JdbcExecutionOptions executionOptions() {
        return JdbcExecutionOptions.builder()
                // Batches are flushed on size, on interval, or at a checkpoint - whichever comes first.
                .withBatchSize(500)
                .withBatchIntervalMs(1_000)
                .withMaxRetries(3)
                .build();
    }

    /** Latest-state table: one row per transaction, upserted. */
    public static JdbcSink<EnrichedTransaction> enrichedTransactions(JobConfig config) {
        String upsert = """
                INSERT INTO enriched_transactions (
                    txn_id, account_id, merchant_id, merchant_name, merchant_category,
                    amount_usd, currency, channel, auth_decision, auth_latency_ms,
                    risk_score, shard_id, subtask_index, event_time, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (txn_id) DO UPDATE SET
                    auth_decision   = EXCLUDED.auth_decision,
                    auth_latency_ms = EXCLUDED.auth_latency_ms,
                    risk_score      = EXCLUDED.risk_score,
                    processed_at    = EXCLUDED.processed_at
                """;

        JdbcStatementBuilder<EnrichedTransaction> bind = (ps, txn) -> {
            ps.setString(1, txn.getTxnId());
            ps.setString(2, txn.getAccountId());
            ps.setString(3, txn.getMerchantId());
            ps.setString(4, txn.getMerchantName());
            ps.setString(5, txn.getMerchantCategory());
            ps.setBigDecimal(6, txn.getAmountUsd());
            ps.setString(7, txn.getCurrency());
            ps.setString(8, txn.getChannel());
            ps.setString(9, txn.getAuthDecision());
            if (txn.getAuthLatencyMs() == null) {
                ps.setNull(10, Types.INTEGER);
            } else {
                ps.setInt(10, txn.getAuthLatencyMs());
            }
            ps.setDouble(11, txn.getRiskScore());
            ps.setInt(12, txn.getShardId());
            ps.setInt(13, txn.getSubtaskIndex());
            ps.setTimestamp(14, Timestamp.from(txn.getEventTime()));
            ps.setTimestamp(15, Timestamp.from(txn.getProcessedAt()));
        };

        return JdbcSink.<EnrichedTransaction>builder()
                .withQueryStatement(upsert, bind)
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(connection(config));
    }

    /** Alert table: append-only, deduplicated on the alert id. */
    public static JdbcSink<FraudAlert> fraudAlerts(JobConfig config) {
        String upsert = """
                INSERT INTO fraud_alerts (
                    alert_id, account_id, rule, severity, description, txn_count, total_usd, detected_at, event_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (alert_id) DO NOTHING
                """;

        JdbcStatementBuilder<FraudAlert> bind = (ps, alert) -> {
            ps.setString(1, alert.getAlertId());
            ps.setString(2, alert.getAccountId());
            ps.setString(3, alert.getRule());
            ps.setString(4, alert.getSeverity());
            ps.setString(5, alert.getDescription());
            ps.setInt(6, alert.getTxnIds() == null ? 0 : alert.getTxnIds().size());
            ps.setBigDecimal(7, alert.getTotalUsd());
            ps.setTimestamp(8, Timestamp.from(alert.getDetectedAt()));
            ps.setTimestamp(9, Timestamp.from(alert.getEventTime()));
        };

        return JdbcSink.<FraudAlert>builder()
                .withQueryStatement(upsert, bind)
                .withExecutionOptions(executionOptions())
                .buildAtLeastOnce(connection(config));
    }

    private static String envOr(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
