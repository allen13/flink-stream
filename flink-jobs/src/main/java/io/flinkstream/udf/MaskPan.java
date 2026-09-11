package io.flinkstream.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * <b>Scalar UDF.</b> Masks all but the last four characters of an identifier.
 *
 * <p>A {@code ScalarFunction} is one row in, one value out. Flink infers the signature by reflecting on the
 * {@code eval} method, so overloads and varargs work; {@link DataTypeHint} is only needed when reflection cannot
 * express the type (nullability, precision, {@code ROW} structure).
 *
 * <p>Register with {@code CREATE TEMPORARY FUNCTION mask_pan AS 'io.flinkstream.udf.MaskPan'} - the SQL
 * lab files do exactly that, which is why a UDF written in Java is usable from a pure-SQL deployment.
 */
public class MaskPan extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public @DataTypeHint("STRING") String eval(@DataTypeHint("STRING") String value) {
        return eval(value, 4);
    }

    public @DataTypeHint("STRING") String eval(@DataTypeHint("STRING") String value, Integer visible) {
        if (value == null) {
            return null;
        }
        int keep = visible == null ? 4 : Math.max(0, visible);
        if (value.length() <= keep) {
            return "*".repeat(value.length());
        }
        return "*".repeat(value.length() - keep) + value.substring(value.length() - keep);
    }
}
