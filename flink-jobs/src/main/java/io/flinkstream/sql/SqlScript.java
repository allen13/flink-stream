package io.flinkstream.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a {@code .sql} file into executable statements.
 *
 * <p>Flink's {@code executeSql} takes exactly one statement, so anything that runs a script has to do this.
 * Naive {@code split(";")} breaks on semicolons inside string literals - and the Kafka DDL in this project is
 * full of them (for example {@code 'properties.bootstrap.servers' = 'a:9092;b:9092'}) - so the splitter tracks
 * quoting and comments.
 */
public final class SqlScript {

    private SqlScript() {}

    public static List<String> statements(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        boolean inSingleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < script.length(); i++) {
            char c = script.charAt(i);
            char next = i + 1 < script.length() ? script.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (!inSingleQuote && c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }
            if (!inSingleQuote && c == '/' && next == '*') {
                inBlockComment = true;
                i++;
                continue;
            }
            if (c == '\'') {
                // '' inside a quoted literal is an escaped quote, not the end of the literal.
                if (inSingleQuote && next == '\'') {
                    current.append(c).append(next);
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                current.append(c);
                continue;
            }
            if (c == ';' && !inSingleQuote) {
                addIfNotBlank(statements, current);
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        addIfNotBlank(statements, current);
        return statements;
    }

    private static void addIfNotBlank(List<String> out, StringBuilder buffer) {
        String statement = buffer.toString().trim();
        if (!statement.isEmpty()) {
            out.add(statement);
        }
    }
}
