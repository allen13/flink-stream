package io.flinkstream.sql;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SqlScriptTest {

    @Test
    void splitsOnSemicolonsAndDropsComments() {
        List<String> statements = SqlScript.statements("""
                -- a leading comment
                CREATE TABLE a (x INT);
                /* a block
                   comment */
                INSERT INTO a SELECT 1;   -- trailing comment
                """);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0)).startsWith("CREATE TABLE a");
        assertThat(statements.get(1)).startsWith("INSERT INTO a");
        assertThat(statements).noneMatch(s -> s.contains("comment"));
    }

    /**
     * The reason this splitter exists rather than {@code script.split(";")}: a Kafka bootstrap list is a
     * semicolon-separated string literal, and naive splitting cuts the DDL in half.
     */
    @Test
    void doesNotSplitInsideAStringLiteral() {
        List<String> statements = SqlScript.statements(
                "CREATE TABLE t WITH ('servers' = 'a:9092;b:9092;c:9092', 'x' = 'y');");

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).contains("a:9092;b:9092;c:9092");
    }

    @Test
    void handlesEscapedQuotesInsideLiterals() {
        List<String> statements = SqlScript.statements(
                "SELECT 'it''s fine; really' AS x; SELECT 2;");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0)).contains("it''s fine; really");
    }

    @Test
    void ignoresADoubleDashInsideALiteral() {
        List<String> statements = SqlScript.statements("SELECT 'a--b' AS x;");

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).contains("a--b");
    }

    @Test
    void substitutesEnvironmentPlaceholdersWithDefaults() {
        String resolved = SqlRunnerJob.substitute(
                "'uri' = '${DEFINITELY_NOT_SET_12345:-http://fallback:8181}'");

        assertThat(resolved).isEqualTo("'uri' = 'http://fallback:8181'");
    }

    /**
     * Parses every lab file that ships with the chart. Catches an unbalanced quote or a stray semicolon before
     * it becomes a job that fails on submission, minutes into a deploy.
     */
    @Test
    void everyShippedLabFileParses() throws Exception {
        Path sqlDir = Path.of("..", "charts", "flink-stream", "files", "sql");
        assertThat(Files.isDirectory(sqlDir))
                .as("chart SQL directory at %s", sqlDir.toAbsolutePath())
                .isTrue();

        try (Stream<Path> files = Files.list(sqlDir)) {
            List<Path> labs = files.filter(p -> p.toString().endsWith(".sql")).sorted().toList();
            assertThat(labs).isNotEmpty();

            for (Path lab : labs) {
                List<String> statements = SqlScript.statements(Files.readString(lab));
                assertThat(statements)
                        .as("statements in %s", lab.getFileName())
                        .isNotEmpty();
                assertThat(statements)
                        .as("no statement in %s should be only whitespace", lab.getFileName())
                        .allMatch(s -> !s.isBlank());
            }
        }
    }
}
