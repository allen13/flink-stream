package io.flinkstream.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Money arithmetic.
 *
 * <p>Amounts are Avro {@code decimal(12,2)}, which the generated classes expose as {@link BigDecimal}. Every sum
 * here re-applies the scale, because {@code BigDecimal.add} keeps the larger scale of its operands and an Avro
 * decimal whose scale does not match its schema fails to serialize.
 */
public final class Money {

    public static final int SCALE = 2;
    public static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE, RoundingMode.HALF_UP);

    private Money() {}

    public static BigDecimal of(double value) {
        return BigDecimal.valueOf(value).setScale(SCALE, RoundingMode.HALF_UP);
    }

    public static BigDecimal add(BigDecimal a, BigDecimal b) {
        return normalize(nullToZero(a).add(nullToZero(b)));
    }

    public static BigDecimal max(BigDecimal a, BigDecimal b) {
        return nullToZero(a).compareTo(nullToZero(b)) >= 0 ? normalize(a) : normalize(b);
    }

    public static BigDecimal multiply(BigDecimal a, BigDecimal b) {
        return normalize(nullToZero(a).multiply(nullToZero(b)));
    }

    /** Forces the scale an Avro {@code decimal(p,2)} field requires. */
    public static BigDecimal normalize(BigDecimal value) {
        return nullToZero(value).setScale(SCALE, RoundingMode.HALF_UP);
    }

    /** Widens to the {@code decimal(14,2)} scale used by the aggregate output schemas. */
    public static BigDecimal nullToZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
