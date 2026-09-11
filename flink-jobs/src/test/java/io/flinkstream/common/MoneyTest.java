package io.flinkstream.common;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class MoneyTest {

    /**
     * {@code BigDecimal.add} keeps the larger scale of its operands, and an Avro {@code decimal(p,2)} field
     * fails to serialize if the value's scale is not exactly 2. Every aggregate in this project therefore
     * re-normalizes - this test is what stops that being quietly dropped.
     */
    @Test
    void additionAlwaysNormalizesTheScale() {
        BigDecimal sum = Money.add(new BigDecimal("1.005"), new BigDecimal("2.1"));

        assertThat(sum.scale()).isEqualTo(2);
        assertThat(sum).isEqualTo(new BigDecimal("3.11"));
    }

    @Test
    void nullsAreTreatedAsZero() {
        assertThat(Money.add(null, new BigDecimal("5.00"))).isEqualTo(new BigDecimal("5.00"));
        assertThat(Money.add(new BigDecimal("5.00"), null)).isEqualTo(new BigDecimal("5.00"));
        assertThat(Money.max(null, null)).isEqualTo(Money.ZERO);
    }

    @Test
    void maxComparesByValueNotScale() {
        assertThat(Money.max(new BigDecimal("10.0"), new BigDecimal("10.00")))
                .isEqualByComparingTo(new BigDecimal("10.00"));
        assertThat(Money.max(new BigDecimal("9.99"), new BigDecimal("10.00")))
                .isEqualTo(new BigDecimal("10.00"));
    }
}
