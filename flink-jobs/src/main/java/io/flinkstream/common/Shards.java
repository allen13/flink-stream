package io.flinkstream.common;

import java.nio.charset.StandardCharsets;

/**
 * Helpers for reasoning about shards (Kafka partitions) and Flink key groups.
 *
 * <p>Kafka and Flink shard data with <em>different</em> hash functions:
 *
 * <ul>
 *   <li>Kafka's default partitioner: {@code murmur2(keyBytes) % numPartitions}</li>
 *   <li>Flink's {@code keyBy}: {@code murmurHash(key.hashCode())} mapped onto {@code maxParallelism} key groups,
 *       and key groups are then dealt out to subtasks</li>
 * </ul>
 *
 * <p>So even when a Flink job keys by exactly the field Kafka partitioned on, subtask <i>i</i> does not receive
 * shard <i>i</i>. There is no co-partitioning shortcut - every {@code keyBy} is a real network shuffle. That is
 * the fact the sharded-join lab is built around.
 */
public final class Shards {

    private Shards() {}

    /**
     * Reproduces Kafka's default partitioner so a producer and a doc can agree on which shard a key lands in.
     *
     * <p>The sign is stripped with {@code & 0x7fffffff}, exactly as Kafka's {@code Utils.toPositive} does -
     * <em>not</em> with {@link Math#abs}. They are not interchangeable: {@code abs(-5) = 5} while
     * {@code -5 & 0x7fffffff = 2147483643}, and those land in different partitions. (Worse, {@code abs} of
     * {@link Integer#MIN_VALUE} is still negative, which yields a negative partition index.)
     */
    public static int kafkaPartitionFor(String key, int numPartitions) {
        return (murmur2(key.getBytes(StandardCharsets.UTF_8)) & 0x7fffffff) % numPartitions;
    }

    /** Kafka's {@code org.apache.kafka.common.utils.Utils#murmur2}, reproduced so this class has no Kafka dependency. */
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
                // fall through
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
                // fall through
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
                break;
            default:
                break;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
