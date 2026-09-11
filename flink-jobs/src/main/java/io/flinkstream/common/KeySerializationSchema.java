package io.flinkstream.common;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Turns a record into its Kafka message key (plain UTF-8, no schema).
 *
 * <p>Keys are deliberately <em>not</em> Avro: registering a {@code <topic>-key} schema for what is always a single
 * string buys nothing, and plain string keys are what {@code key.format = 'raw'} in the SQL DDL expects. Only
 * values carry Avro schemas.
 *
 * <p>The key is what decides the shard, so this class is the single place that controls how records are
 * distributed across Kafka partitions.
 */
public class KeySerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    /** {@link Function} is not {@link Serializable}; this narrows it so Flink can ship it to the TaskManagers. */
    public interface KeyOf<T> extends Function<T, String>, Serializable {}

    private final KeyOf<T> keyOf;

    public KeySerializationSchema(KeyOf<T> keyOf) {
        this.keyOf = keyOf;
    }

    @Override
    public byte[] serialize(T element) {
        String key = keyOf.apply(element);
        return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    }
}
