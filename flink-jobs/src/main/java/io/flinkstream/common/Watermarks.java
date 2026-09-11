package io.flinkstream.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;
import java.util.function.Function;

/**
 * Watermark strategies for the event-time streams.
 *
 * <p>Two settings matter more than anything else in this project:
 *
 * <ul>
 *   <li><b>Out-of-orderness</b> - how far back in event time the watermark trails the newest timestamp seen.
 *       Too small and correct-but-late records get dropped; too large and every window emits late.</li>
 *   <li><b>Idleness</b> - a source subtask that reads several shards emits the <em>minimum</em> watermark across
 *       them. One quiet shard therefore freezes the watermark for the whole job and no window ever fires. Marking
 *       a shard idle after a timeout takes it out of that minimum. This is the single most common reason a
 *       windowed job "produces nothing" on a lightly loaded local cluster.</li>
 * </ul>
 */
public final class Watermarks {

    private Watermarks() {}

    public static <T> WatermarkStrategy<T> boundedOutOfOrder(
            JobConfig config, SerializableTimestampOf<T> timestampOf) {

        return WatermarkStrategy.<T>forBoundedOutOfOrderness(config.maxOutOfOrderness())
                .withTimestampAssigner((event, recordTimestamp) -> timestampOf.apply(event))
                .withIdleness(config.sourceIdleTimeout());
    }

    /**
     * Same as {@link #boundedOutOfOrder} but additionally aligned across sources.
     *
     * <p>Watermark alignment stops a fast shard from racing ahead of a slow one. Without it, replaying a topic
     * from the beginning lets whichever shard is fastest push the watermark forward, which buffers - or drops -
     * everything the slower shards have not caught up to yet.
     */
    public static <T> WatermarkStrategy<T> aligned(
            JobConfig config, SerializableTimestampOf<T> timestampOf, String alignmentGroup) {

        return Watermarks.<T>boundedOutOfOrder(config, timestampOf)
                .withWatermarkAlignment(alignmentGroup, Duration.ofSeconds(30), Duration.ofSeconds(1));
    }

    /** A serializable {@code T -> epoch millis} extractor. */
    public interface SerializableTimestampOf<T> extends Function<T, Long>, java.io.Serializable {}
}
