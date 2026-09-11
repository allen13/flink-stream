package io.flinkstream.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * Factory methods for Kafka sources and sinks that speak Confluent-framed Avro.
 *
 * <p>"Confluent-framed" means each value is a magic byte, a 4-byte schema id, then the Avro payload. The schema
 * itself lives in Schema Registry, so a consumer that has never seen the writer schema can still decode the
 * record - that indirection is the whole point of using the registry rather than embedding schemas in messages.
 */
public final class Kafka {

    private Kafka() {}

    /**
     * A source reading Confluent Avro values into generated {@link SpecificRecord} classes.
     *
     * @param startFromEarliest replay the whole topic on a fresh start (what you want for the compacted
     *                          dimension topics, and convenient for the fact topics in a lab)
     */
    public static <T extends SpecificRecord> KafkaSource<T> source(
            JobConfig config, String topic, Class<T> type, String groupSuffix, boolean startFromEarliest) {

        return KafkaSource.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(topic)
                .setGroupId(config.consumerGroup() + "-" + groupSuffix)
                .setStartingOffsets(startFromEarliest
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(type, config.schemaRegistryUrl()))
                .setProperties(config.consumerProperties())
                .build();
    }

    /**
     * A sink writing Confluent Avro values, keyed by {@code keyOf}.
     *
     * <p>The value schema is registered under {@code <topic>-value} on first write (TopicNameStrategy), so the
     * output topics get their schemas from running the job - there is no separate schema-publishing step.
     *
     * <p>With {@code EXACTLY_ONCE}, records become visible to {@code read_committed} consumers only when the
     * checkpoint that produced them completes. Reading the output with Kafka UI therefore lags by up to one
     * checkpoint interval - that is the cost of end-to-end exactly-once, not a bug.
     */
    public static <T extends SpecificRecord> KafkaSink<T> sink(
            JobConfig config, String topic, Class<T> type, KeySerializationSchema.KeyOf<T> keyOf, String sinkId) {

        return KafkaSink.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(new KeySerializationSchema<>(keyOf))
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(
                                type, topic + "-value", config.schemaRegistryUrl()))
                        .build())
                .setDeliveryGuarantee(config.exactlyOnceSinks()
                        ? DeliveryGuarantee.EXACTLY_ONCE
                        : DeliveryGuarantee.AT_LEAST_ONCE)
                // Each sink needs its own prefix: two sinks sharing one prefix will fence each other's
                // transactions and the job will fail on the first checkpoint.
                .setTransactionalIdPrefix(config.txnIdPrefix() + "-" + sinkId)
                .setKafkaProducerConfig(config.producerProperties())
                .build();
    }
}
