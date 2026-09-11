package io.flinkstream.udf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * <b>Aggregate UDF.</b> Average of {@code value} weighted by {@code weight}.
 *
 * <p>Three methods matter:
 *
 * <ul>
 *   <li>{@code accumulate} - required; called per input row.</li>
 *   <li>{@code retract} - required whenever the function is used over a <em>changelog</em> input (a regular
 *       {@code GROUP BY} downstream of a join, an OVER window, a retracting upstream). Without it, planning fails
 *       with "does not implement retract" the moment the input is not append-only.</li>
 *   <li>{@code merge} - required for session windows and for local/global two-phase aggregation. Providing it
 *       lets the planner split the aggregate and cut shuffle volume.</li>
 * </ul>
 */
public class WeightedAvg extends AggregateFunction<Double, WeightedAvg.Accumulator> {

    private static final long serialVersionUID = 1L;

    /** Public fields + public no-arg constructor so Flink treats this as a structured type, not Kryo. */
    public static class Accumulator {
        public double weightedSum;
        public double weightTotal;

        public Accumulator() {}
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Double getValue(Accumulator acc) {
        return acc.weightTotal == 0d ? null : acc.weightedSum / acc.weightTotal;
    }

    public void accumulate(Accumulator acc, Double value, Double weight) {
        if (value == null || weight == null || weight == 0d) {
            return;
        }
        acc.weightedSum += value * weight;
        acc.weightTotal += weight;
    }

    public void retract(Accumulator acc, Double value, Double weight) {
        if (value == null || weight == null || weight == 0d) {
            return;
        }
        acc.weightedSum -= value * weight;
        acc.weightTotal -= weight;
    }

    public void merge(Accumulator acc, Iterable<Accumulator> others) {
        for (Accumulator other : others) {
            acc.weightedSum += other.weightedSum;
            acc.weightTotal += other.weightTotal;
        }
    }

    public void resetAccumulator(Accumulator acc) {
        acc.weightedSum = 0d;
        acc.weightTotal = 0d;
    }
}
