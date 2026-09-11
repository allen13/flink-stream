package io.flinkstream.sql;

import io.flinkstream.common.JobConfig;
import io.flinkstream.udf.ExplodeTags;
import io.flinkstream.udf.Haversine;
import io.flinkstream.udf.MaskPan;
import io.flinkstream.udf.WeightedAvg;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Runs one or more {@code .sql} files as a single Flink application-mode job.
 *
 * <p>This is the standard "SQL runner" pattern the Flink Kubernetes Operator documents, and it is how pure-SQL
 * pipelines get deployed without an interactive SQL client:
 *
 * <pre>
 *   args: ["--sql-dir", "/opt/flink/sql-labs"]
 * </pre>
 *
 * <p>The two behaviours that make it useful:
 *
 * <ul>
 *   <li><b>Every {@code INSERT} goes into one {@link StatementSet}.</b> Submitting them individually would
 *       produce one Flink job per INSERT, each with its own Kafka source reading the same topic. A statement set
 *       compiles them into a single job graph where the shared source is read once and its result fanned out -
 *       usually a several-fold reduction in Kafka traffic and state.</li>
 *   <li><b>{@code SET 'k' = 'v'} is applied to the {@code TableConfig}</b> before anything is planned, so the
 *       lab files can turn on mini-batch, change state TTL, or pick a join strategy the same way you would in
 *       the SQL client.</li>
 * </ul>
 *
 * <p>Placeholders of the form <code>${VAR}</code> are substituted from the environment, which is how the chart
 * injects the Kafka bootstrap servers and Schema Registry URL into the DDL.
 */
public final class SqlRunnerJob {

    private static final Logger LOG = LoggerFactory.getLogger(SqlRunnerJob.class);

    private static final Pattern SET_STATEMENT =
            Pattern.compile("^SET\\s+'([^']+)'\\s*=\\s*'([^']*)'$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(?::-([^}]*))?}");

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(args);

        StreamExecutionEnvironment env = config.configureEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        registerFunctions(tEnv);
        applyDefaults(tEnv);

        List<Path> files = resolveFiles(config);
        if (files.isEmpty()) {
            throw new IllegalArgumentException(
                    "No SQL files found. Pass --sql-file <path> (repeatable, comma separated) or --sql-dir <dir>.");
        }

        StatementSet statementSet = tEnv.createStatementSet();
        int inserts = 0;

        for (Path file : files) {
            LOG.info("Executing SQL file {}", file);
            String script = substitute(Files.readString(file, StandardCharsets.UTF_8));

            for (String statement : SqlScript.statements(script)) {
                String head = statement.stripLeading().toUpperCase(Locale.ROOT);

                Matcher set = SET_STATEMENT.matcher(statement.trim());
                if (set.matches()) {
                    LOG.info("SET {} = {}", set.group(1), set.group(2));
                    tEnv.getConfig().getConfiguration().setString(set.group(1), set.group(2));
                    continue;
                }

                if (head.startsWith("INSERT ") || head.startsWith("INSERT\n")) {
                    LOG.info("Adding INSERT to the statement set:\n{}", statement);
                    statementSet.addInsertSql(statement);
                    inserts++;
                    continue;
                }

                LOG.info("Executing:\n{}", statement);
                TableResult result = tEnv.executeSql(statement);
                // DDL returns immediately; a stray SELECT in a deployed script would otherwise hang forever
                // waiting for a result nobody reads, so it is logged and skipped.
                if (head.startsWith("SELECT") || head.startsWith("WITH")) {
                    LOG.warn("SELECT statements do not belong in a deployed script - "
                            + "use the SQL client for exploration. Skipping result collection.");
                }
                result.getJobClient().ifPresent(client ->
                        LOG.info("statement submitted as job {}", client.getJobID()));
            }
        }

        if (inserts == 0) {
            throw new IllegalStateException("The script contained no INSERT statements, so there is no job to run.");
        }

        LOG.info("Submitting a statement set with {} INSERT statement(s) as one job graph", inserts);
        statementSet.execute();
    }

    /**
     * Registers the Java UDFs under the names the lab files use.
     *
     * <p>The files also contain the equivalent {@code CREATE TEMPORARY FUNCTION ... AS '<class>'} statements, so
     * they work unchanged in the SQL client where no Java main method runs. Registering here as well means the
     * deployed job does not depend on those statements having been run first.
     */
    private static void registerFunctions(TableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("mask_pan", MaskPan.class);
        tEnv.createTemporarySystemFunction("haversine_km", Haversine.class);
        tEnv.createTemporarySystemFunction("explode_tags", ExplodeTags.class);
        tEnv.createTemporarySystemFunction("weighted_avg", WeightedAvg.class);
    }

    /** Defaults a lab file can still override with its own {@code SET}. */
    private static void applyDefaults(TableEnvironment tEnv) {
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("pipeline.name", "sql-labs (Table API)");
        // Bound the state of unbounded (regular) joins and GROUP BYs. Without this a regular join keeps every
        // row it has ever seen, forever - the single most common way a SQL streaming job runs out of disk.
        conf.setString("table.exec.state.ttl", "1 h");
        // Mini-batch trades a little latency for far fewer state accesses on aggregations.
        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "2 s");
        conf.setString("table.exec.mini-batch.size", "1000");
        // Two-phase aggregation: pre-aggregate before the shuffle, combine after it. This is what keeps a
        // low-cardinality GROUP BY (five regions) from hot-spotting one subtask.
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        // Sources that go quiet must not freeze event time for the whole query.
        conf.setString("table.exec.source.idle-timeout", "15 s");
    }

    private static List<Path> resolveFiles(JobConfig config) throws IOException {
        List<Path> files = new ArrayList<>();

        String fileArg = config.get("sql-file", "");
        if (!fileArg.isBlank()) {
            for (String part : fileArg.split(",")) {
                files.add(Path.of(part.trim()));
            }
        }

        String dirArg = config.get("sql-dir", "");
        if (!dirArg.isBlank()) {
            Path dir = Path.of(dirArg);
            try (Stream<Path> walk = Files.list(dir)) {
                walk.filter(p -> p.getFileName().toString().endsWith(".sql"))
                        // Lexical order, which is why the lab files are numbered: DDL must run before DML.
                        .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                        .forEach(files::add);
            }
        }
        return files;
    }

    /** Replaces <code>${VAR}</code> and <code>${VAR:-default}</code> from the process environment. */
    static String substitute(String script) {
        Map<String, String> env = System.getenv();
        Matcher m = PLACEHOLDER.matcher(script);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String value = env.get(m.group(1));
            if (value == null) {
                value = m.group(2);
            }
            if (value == null) {
                throw new IllegalStateException("SQL references ${" + m.group(1)
                        + "} but that environment variable is not set and no default was given");
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    private SqlRunnerJob() {}
}
