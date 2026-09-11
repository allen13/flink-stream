package io.flinkstream.sinks;

import io.flinkstream.avro.EnrichedTransaction;
import io.flinkstream.avro.FraudAlert;
import io.flinkstream.common.JobConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Sinks into Snowflake, the warehouse side of the fan-out - authenticating with a programmatic access token.
 *
 * <p>There is no Flink connector for Snowflake, and none is needed. Snowflake ships a JDBC driver, and the
 * DataStream {@link JdbcSink} is <em>dialect-free</em>: it takes the SQL text you hand it and binds parameters
 * into it. (The Table/SQL {@code 'connector' = 'jdbc'} is the one that needs a registered {@code JdbcDialect},
 * which is why the SQL labs sink to YugabyteDB and not here.) Anything with a JDBC driver is reachable this way,
 * as long as you are willing to write the statement yourself.
 *
 * <h2>Authentication: programmatic access tokens</h2>
 *
 * <p>A PAT is Snowflake's credential for things that are not people. It is presented <b>in the password slot</b>
 * - not in a bearer header, not as {@code token=} - and {@code authenticator=programmatic_access_token} tells a
 * recent driver to treat it as one rather than as an account password. Compared with the alternatives:
 *
 * <ul>
 *   <li><b>Password</b> - belongs to a human, expires on that human's schedule, carries all their roles.</li>
 *   <li><b>Key pair ({@code snowflake_jwt})</b> - no secret crosses the wire, but the job needs a private key
 *       file on disk and a rotation story for it.</li>
 *   <li><b>PAT</b> - issued per service user, scoped to a <em>single role</em>, given its own expiry, and
 *       revocable on its own without touching anything else. One string, so it fits a Kubernetes Secret.</li>
 * </ul>
 *
 * <p>Snowflake will only <em>use</em> a token if the user's authentication policy lists
 * {@code PROGRAMMATIC_ACCESS_TOKEN}, and a {@code TYPE = SERVICE} user additionally needs a network policy
 * attached. Both are in {@code charts/flink-stream/files/snowflake-schema.sql}.
 *
 * <p><b>The token is read from the environment or from a file, never from job arguments.</b>
 * {@code JobConfig.configureEnvironment} puts the {@code ParameterTool} into the job's global parameters, which
 * Flink renders verbatim on the job's page in the web UI and stores in the job graph. A {@code --snowflake-pat}
 * flag would be a credential on a dashboard. {@code SNOWFLAKE_PAT_FILE} - a mounted Secret volume - is the
 * better of the two remaining options: an environment variable is visible in {@code kubectl describe pod}, in a
 * dump of the process environment and in every child process, where a file is not. The token is read once, when
 * the sink is built, so rotating it still means restarting the job - but it means patching a Secret rather than
 * redeploying.
 *
 * <h2>Delivery semantics</h2>
 *
 * <p>Both sinks are {@code buildAtLeastOnce}, as with {@link Yugabyte} - Snowflake exposes no XA - but they
 * reconcile the resulting replays differently, because the two streams cost very different amounts to write:
 *
 * <ul>
 *   <li>{@code enriched_transactions} is high volume and lands as a plain append-only {@code INSERT}. Duplicates
 *       from a replayed batch are removed <em>at read time</em> by {@code v_enriched_transactions} with
 *       {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY processed_at DESC) = 1}.</li>
 *   <li>{@code fraud_alerts} is low volume and lands as a {@code MERGE} keyed on {@code alert_id} - Snowflake's
 *       equivalent of {@code ON CONFLICT DO NOTHING}, and affordable precisely because the stream is small.</li>
 * </ul>
 *
 * <p>That split is the whole difference between a warehouse and an OLTP store. Yugabyte upserts every row
 * because a row costs a key-value write; Snowflake rewrites whole micro-partitions, so a {@code MERGE} per
 * checkpoint on the fact stream would burn credits to save a {@code QUALIFY} clause.
 */
public final class Snowflake {

    /** Timestamps are bound in UTC so the warehouse does not inherit the TaskManager's local timezone. */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private Snowflake() {}

    // ------------------------------------------------------------------ connection

    public static JdbcConnectionOptions connection(JobConfig config) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl(config))
                // The legacy driver class name. It is deprecated in favour of
                // net.snowflake.client.api.driver.SnowflakeDriver in JDBC 4.x but still registered there, and it
                // is the only name that also works on a 3.x driver - which matters when the driver version is
                // whatever an internal mirror carries rather than what the Dockerfile pins.
                .withDriverName("net.snowflake.client.jdbc.SnowflakeDriver")
                .withUsername(config.get("snowflake-user", ""))
                .withPassword(programmaticAccessToken())
                .withConnectionCheckTimeoutSeconds(30)
                .build();
    }

    /**
     * Builds the JDBC URL. Everything except the credential goes here: {@link JdbcConnectionOptions} has slots
     * for a URL, a user and a password and nothing else, so driver properties have to ride in the query string.
     *
     * <p>Set {@code snowflake-url} to bypass this entirely - the escape hatch for private connectivity, where
     * the host is a PrivateLink or proxy name rather than {@code <account>.snowflakecomputing.com}.
     */
    public static String jdbcUrl(JobConfig config) {
        String explicit = config.get("snowflake-url", "");
        if (!explicit.isBlank()) {
            return explicit;
        }

        String account = config.get("snowflake-account", "");
        if (account.isBlank()) {
            throw new IllegalArgumentException(
                    "Snowflake sink is enabled but no account is configured. Set SNOWFLAKE_ACCOUNT to the "
                            + "account identifier (for example 'myorg-myaccount'), or SNOWFLAKE_URL to a full "
                            + "jdbc:snowflake:// URL.");
        }
        String host = config.get("snowflake-host", account + ".snowflakecomputing.com");

        Map<String, String> params = new LinkedHashMap<>();
        // Snowflake resolves unqualified table names against these, so they are what makes the statements below
        // portable between a dev schema and a production one without editing SQL.
        putIfSet(params, "db", config.get("snowflake-database", ""));
        putIfSet(params, "schema", config.get("snowflake-schema", ""));
        putIfSet(params, "warehouse", config.get("snowflake-warehouse", ""));
        // The PAT is bound to one role. Naming it here makes a mismatch fail at connect time with a clear
        // message instead of at the first INSERT with a permissions error.
        putIfSet(params, "role", config.get("snowflake-role", ""));
        // Override to "snowflake" to send the token as a plain password, which is how a driver older than the
        // PAT authenticator accepts it (Snowflake validates the token either way - the authenticator only tells
        // the driver what kind of secret it is holding).
        putIfSet(params, "authenticator", config.get("snowflake-authenticator", "programmatic_access_token"));
        // Shows up in Snowflake's QUERY_HISTORY.CLIENT_APPLICATION_ID, so warehouse spend can be attributed to
        // this job rather than to "JDBC".
        putIfSet(params, "application", config.get("snowflake-application", "flink-stream"));

        StringBuilder url = new StringBuilder("jdbc:snowflake://").append(host).append("/?");
        boolean first = true;
        for (Map.Entry<String, String> param : params.entrySet()) {
            if (!first) {
                url.append('&');
            }
            url.append(param.getKey()).append('=').append(encode(param.getValue()));
            first = false;
        }
        return url.toString();
    }

    /**
     * Resolves the token from {@code SNOWFLAKE_PAT_FILE} (a mounted Secret) or {@code SNOWFLAKE_PAT}.
     *
     * <p>Deliberately not routed through {@link JobConfig#get} - see the class javadoc for why a command-line
     * flag is the wrong place for this.
     */
    static String programmaticAccessToken() {
        String file = System.getenv("SNOWFLAKE_PAT_FILE");
        if (file != null && !file.isBlank()) {
            try {
                String token = Files.readString(Path.of(file), StandardCharsets.UTF_8).strip();
                if (token.isEmpty()) {
                    throw new IllegalStateException("SNOWFLAKE_PAT_FILE points at an empty file: " + file);
                }
                return token;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read the Snowflake token from " + file, e);
            }
        }

        String token = System.getenv("SNOWFLAKE_PAT");
        if (token == null || token.isBlank()) {
            throw new IllegalStateException(
                    "Snowflake sink is enabled but no programmatic access token was provided. Set "
                            + "SNOWFLAKE_PAT_FILE to a mounted secret file, or SNOWFLAKE_PAT to the token. The "
                            + "token is never read from job arguments: those are visible in the Flink web UI.");
        }
        return token.strip();
    }

    // ------------------------------------------------------------------ execution options

    /**
     * Batching, sized for a warehouse rather than for a database.
     *
     * <p>Snowflake charges for compute by the second and rewrites a micro-partition per statement, so the cost
     * of a write is dominated by the number of statements, not by the number of rows in them. The batch is an
     * order of magnitude larger than Yugabyte's for that reason - and specifically large enough to cross
     * {@code CLIENT_STAGE_ARRAY_BINDING_THRESHOLD} (65,280 bind values by default). Above it the driver stops
     * sending one enormous {@code INSERT} and instead uploads the batch to a temporary stage and {@code COPY}s
     * it, which is the fast path. At 15 bound columns, 5,000 rows is 75,000 binds - comfortably over.
     *
     * <p>The interval is the ceiling on end-to-end latency when the stream is slow; the checkpoint flush still
     * happens regardless, so no record waits longer than a checkpoint to become visible.
     */
    private static JdbcExecutionOptions executionOptions(JobConfig config) {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(config.get("snowflake-batch-size", "5000")))
                .withBatchIntervalMs(Long.parseLong(config.get("snowflake-batch-interval-ms", "10000")))
                .withMaxRetries(3)
                .build();
    }

    // ------------------------------------------------------------------ sinks

    /** History table: append-only, deduplicated at read time by {@code v_enriched_transactions}. */
    public static JdbcSink<EnrichedTransaction> enrichedTransactions(JobConfig config) {
        String insert = """
                INSERT INTO enriched_transactions (
                    txn_id, account_id, merchant_id, merchant_name, merchant_category,
                    amount_usd, currency, channel, auth_decision, auth_latency_ms,
                    risk_score, shard_id, subtask_index, event_time, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            setUtcTimestamp(ps, 14, txn.getEventTime());
            setUtcTimestamp(ps, 15, txn.getProcessedAt());
        };

        return JdbcSink.<EnrichedTransaction>builder()
                .withQueryStatement(insert, bind)
                .withExecutionOptions(executionOptions(config))
                .buildAtLeastOnce(connection(config));
    }

    /**
     * Alert table: deduplicated on write.
     *
     * <p>Snowflake has no {@code ON CONFLICT}, so the idempotent insert is a {@code MERGE} whose source is a
     * one-row {@code SELECT} of bind variables. Every bind is aliased because {@code MERGE} needs named columns
     * on both sides, and {@code ?::TYPE} casts keep the source column types explicit rather than inferred.
     */
    public static JdbcSink<FraudAlert> fraudAlerts(JobConfig config) {
        String merge = """
                MERGE INTO fraud_alerts t
                USING (SELECT
                           ?::VARCHAR       AS alert_id,
                           ?::VARCHAR       AS account_id,
                           ?::VARCHAR       AS rule,
                           ?::VARCHAR       AS severity,
                           ?::VARCHAR       AS description,
                           ?::NUMBER(10,0)  AS txn_count,
                           ?::NUMBER(14,2)  AS total_usd,
                           ?::TIMESTAMP_NTZ AS detected_at,
                           ?::TIMESTAMP_NTZ AS event_time) s
                   ON t.alert_id = s.alert_id
                WHEN NOT MATCHED THEN INSERT (
                    alert_id, account_id, rule, severity, description, txn_count, total_usd, detected_at, event_time)
                VALUES (
                    s.alert_id, s.account_id, s.rule, s.severity, s.description, s.txn_count, s.total_usd,
                    s.detected_at, s.event_time)
                """;

        JdbcStatementBuilder<FraudAlert> bind = (ps, alert) -> {
            ps.setString(1, alert.getAlertId());
            ps.setString(2, alert.getAccountId());
            ps.setString(3, alert.getRule());
            ps.setString(4, alert.getSeverity());
            ps.setString(5, alert.getDescription());
            ps.setInt(6, alert.getTxnIds() == null ? 0 : alert.getTxnIds().size());
            ps.setBigDecimal(7, alert.getTotalUsd());
            setUtcTimestamp(ps, 8, alert.getDetectedAt());
            setUtcTimestamp(ps, 9, alert.getEventTime());
        };

        return JdbcSink.<FraudAlert>builder()
                .withQueryStatement(merge, bind)
                .withExecutionOptions(executionOptions(config))
                .buildAtLeastOnce(connection(config));
    }

    // ------------------------------------------------------------------ internals

    /**
     * Binds an {@link Instant} into a {@code TIMESTAMP_NTZ} column as its UTC wall-clock time.
     *
     * <p>{@code setTimestamp(i, ts)} without a calendar splits the instant using the JVM's default timezone, so
     * the same record written from a laptop and from a TaskManager pod would land as two different values. The
     * columns are {@code TIMESTAMP_NTZ} - no zone stored - which is only safe if every writer agrees on one, and
     * this is where that is enforced.
     *
     * <p>A fresh {@link Calendar} per call, rather than a shared field: {@code Calendar} is mutable and this runs
     * on every sink subtask at once.
     */
    private static void setUtcTimestamp(PreparedStatement ps, int index, Instant value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(index, Timestamp.from(value), Calendar.getInstance(UTC));
        }
    }

    private static void putIfSet(Map<String, String> params, String key, String value) {
        if (value != null && !value.isBlank()) {
            params.put(key, value);
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
