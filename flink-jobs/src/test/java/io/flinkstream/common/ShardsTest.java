package io.flinkstream.common;

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ShardsTest {

    /**
     * The generator stamps {@code shard_id} on every transaction by computing Kafka's partitioner itself, and
     * the SQL labs compare that field against the {@code partition} metadata column to show they agree. If this
     * reimplementation ever drifted from Kafka's, that comparison would silently start failing in a way that
     * looks like a Flink bug.
     *
     * <p>Asserted against Kafka's real {@code Utils.murmur2} rather than against copied constants, so the test
     * keeps its meaning across Kafka upgrades.
     */
    @Test
    void reproducesKafkasMurmur2() {
        for (String key : new String[] {"", "a", "kafka", "acct-00042", "mch-0007", "USD"}) {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            assertThat(Shards.murmur2(bytes))
                    .as("murmur2(%s)", key)
                    .isEqualTo(Utils.murmur2(bytes));
        }
    }

    /**
     * Kafka strips the sign with {@code & 0x7fffffff}, not {@code Math.abs}. The two disagree for every
     * negative hash, so getting this wrong puts records in a different shard than Kafka does - and
     * {@code Math.abs(Integer.MIN_VALUE)} is still negative, which would yield a negative partition index.
     */
    @Test
    void stripsTheSignTheWayKafkaDoes() {
        for (int i = 0; i < 500; i++) {
            String key = "acct-" + i;
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            int kafkaPartition = Utils.toPositive(Utils.murmur2(bytes)) % 12;
            assertThat(Shards.kafkaPartitionFor(key, 12))
                    .as("partition for %s", key)
                    .isEqualTo(kafkaPartition);
        }
    }

    @Test
    void partitionsAreStableForAKey() {
        int first = Shards.kafkaPartitionFor("acct-00042", 12);
        for (int i = 0; i < 100; i++) {
            assertThat(Shards.kafkaPartitionFor("acct-00042", 12)).isEqualTo(first);
        }
        assertThat(first).isBetween(0, 11);
    }

    /**
     * The property the cross-shard labs depend on: 400 accounts spread over 12 shards well enough that no
     * shard is empty and none dominates. If the distribution were lopsided, "aggregate across every shard"
     * would not be demonstrating anything.
     */
    @Test
    void accountKeysSpreadAcrossEveryShard() {
        Map<Integer, Integer> perShard = new HashMap<>();
        int accounts = 400;
        int shards = 12;

        for (int i = 0; i < accounts; i++) {
            int shard = Shards.kafkaPartitionFor(String.format("acct-%05d", i), shards);
            perShard.merge(shard, 1, Integer::sum);
        }

        assertThat(perShard).hasSize(shards);
        int expected = accounts / shards;
        assertThat(perShard.values()).allSatisfy(count ->
                assertThat(count).isBetween(expected / 2, expected * 2));
    }
}
