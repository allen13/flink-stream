package io.flinkstream.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * <b>Table UDF.</b> Turns the Avro {@code MAP<STRING,STRING>} tag field into one row per entry.
 *
 * <p>A {@code TableFunction} emits zero or more rows per input row, which is how SQL gets a one-to-many
 * expansion. It is used through a lateral join:
 *
 * <pre>{@code
 * SELECT t.txn_id, tag.k, tag.v
 * FROM transactions AS t,
 *      LATERAL TABLE(explode_tags(t.tags)) AS tag(k, v)
 * }</pre>
 *
 * <p>A plain {@code LATERAL TABLE(...)} is an inner join - rows with an empty map disappear. Use
 * {@code LEFT JOIN LATERAL TABLE(...) ON TRUE} to keep them.
 */
@FunctionHint(output = @DataTypeHint("ROW<k STRING, v STRING>"))
public class ExplodeTags extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;

    public void eval(Map<String, String> tags) {
        if (tags == null) {
            return;
        }
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            collect(Row.of(entry.getKey(), entry.getValue()));
        }
    }
}
