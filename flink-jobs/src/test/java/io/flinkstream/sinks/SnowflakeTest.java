package io.flinkstream.sinks;

import io.flinkstream.common.JobConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Covers the part of the Snowflake sink that can be wrong without an account: the JDBC URL.
 *
 * <p>Everything the driver needs beyond a user and a token rides in that URL's query string, because
 * {@code JdbcConnectionOptions} has nowhere else to put it. A missing {@code role} or {@code warehouse} surfaces
 * as a permission error or a "no active warehouse" error on the first INSERT - well after the job has started
 * and looked healthy - so it is worth pinning here.
 */
class SnowflakeTest {

    private static JobConfig config(String... args) {
        return JobConfig.from(args);
    }

    @Test
    void buildsTheUrlFromTheAccountIdentifier() {
        String url = Snowflake.jdbcUrl(config(
                "--snowflake-account", "myorg-myaccount",
                "--snowflake-database", "FLINK_STREAM",
                "--snowflake-schema", "STREAMING",
                "--snowflake-warehouse", "FLINK_STREAM_WH",
                "--snowflake-role", "FLINK_STREAM_WRITER"));

        assertThat(url).startsWith("jdbc:snowflake://myorg-myaccount.snowflakecomputing.com/?");
        assertThat(url).contains("db=FLINK_STREAM");
        assertThat(url).contains("schema=STREAMING");
        assertThat(url).contains("warehouse=FLINK_STREAM_WH");
        assertThat(url).contains("role=FLINK_STREAM_WRITER");
    }

    /**
     * The default that makes this a PAT sink rather than a password sink. A driver that does not recognise the
     * value would reject it outright, which is the failure mode we want - an account password quietly accepted
     * in the same slot is the one we do not.
     */
    @Test
    void authenticatesWithAProgrammaticAccessTokenByDefault() {
        assertThat(Snowflake.jdbcUrl(config("--snowflake-account", "myorg-myaccount")))
                .contains("authenticator=programmatic_access_token");
    }

    @Test
    void allowsTheAuthenticatorToBeOverriddenForOlderDrivers() {
        assertThat(Snowflake.jdbcUrl(config(
                "--snowflake-account", "myorg-myaccount",
                "--snowflake-authenticator", "snowflake")))
                .contains("authenticator=snowflake");
    }

    /** Empty settings are left out entirely rather than sent as {@code role=}, which the driver rejects. */
    @Test
    void omitsUnsetParameters() {
        String url = Snowflake.jdbcUrl(config("--snowflake-account", "myorg-myaccount"));

        assertThat(url).doesNotContain("role=");
        assertThat(url).doesNotContain("db=");
        assertThat(url).doesNotContain("warehouse=");
    }

    /** PrivateLink and proxy hostnames are not {@code <account>.snowflakecomputing.com}. */
    @Test
    void honoursAnExplicitHostAndUrl() {
        assertThat(Snowflake.jdbcUrl(config(
                "--snowflake-account", "myorg-myaccount",
                "--snowflake-host", "myorg-myaccount.privatelink.snowflakecomputing.com")))
                .startsWith("jdbc:snowflake://myorg-myaccount.privatelink.snowflakecomputing.com/?");

        assertThat(Snowflake.jdbcUrl(config("--snowflake-url", "jdbc:snowflake://proxy.internal/?db=D")))
                .isEqualTo("jdbc:snowflake://proxy.internal/?db=D");
    }

    /**
     * A misconfigured sink should fail in {@code main()} with a message naming the setting, not later with a
     * driver-level "no such host". An application-mode job that throws in main() never starts, so the operator
     * surfaces this immediately instead of it looking like a transient connection problem.
     */
    @Test
    void failsLoudlyWithoutAnAccount() {
        assertThatThrownBy(() -> Snowflake.jdbcUrl(config()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SNOWFLAKE_ACCOUNT");
    }

    /**
     * The token is read from the environment or a mounted file and never from job arguments: those land in the
     * job's global parameters, which Flink renders on the job's page in the web UI.
     */
    @Test
    void neverTakesTheTokenFromJobArguments() {
        // Skipped rather than failed on a machine that really does have a token in its environment.
        assumeThat(System.getenv("SNOWFLAKE_PAT")).isNull();
        assumeThat(System.getenv("SNOWFLAKE_PAT_FILE")).isNull();

        config("--snowflake-pat", "not-a-real-token");

        assertThatThrownBy(Snowflake::programmaticAccessToken)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SNOWFLAKE_PAT");
    }
}
