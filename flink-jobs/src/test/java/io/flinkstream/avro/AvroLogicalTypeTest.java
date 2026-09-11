package io.flinkstream.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks in the two Avro behaviours this project's schema design depends on.
 *
 * <p><b>1. Logical types survive a round trip through the generated classes.</b> {@code decimal} arrives as a
 * {@link BigDecimal} and {@code timestamp-millis} as an {@link Instant}, with no conversions registered
 * anywhere by us. That works because {@code SpecificDatumWriter} asks the record itself - the generated
 * {@code getConversion(int)} - rather than consulting the global {@link SpecificData}. Worth pinning: the
 * equivalent code over {@code GenericRecord} does <em>not</em> work without
 * {@code GenericData.addLogicalTypeConversion}, and that asymmetry costs people an afternoon.
 *
 * <p><b>2. An Avro enum cannot be read into a string.</b> This is why none of the schemas in
 * {@code src/main/avro} use enums: Flink SQL's {@code avro-confluent} format derives its reader schema from the
 * table DDL, where a symbolic column can only be {@code STRING}. If a producer ever reintroduces an enum, this
 * test documents the exact failure the SQL jobs would hit.
 */
class AvroLogicalTypeTest {

    @Test
    void decimalAndTimestampSurviveARoundTrip() throws Exception {
        Instant detectedAt = Instant.ofEpochMilli(1_700_000_000_123L);
        FraudAlert original = FraudAlert.newBuilder()
                .setAlertId("alert-1")
                .setAccountId("acct-1")
                .setRule("VELOCITY")
                .setSeverity("WARN")
                .setDescription("5 transactions totalling 3000.00 USD within 3m")
                .setTxnIds(List.of("txn-1", "txn-2"))
                .setTotalUsd(new BigDecimal("3000.00"))
                .setDetectedAt(detectedAt)
                .setEventTime(detectedAt)
                .build();

        Schema schema = FraudAlert.getClassSchema();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<FraudAlert>(schema).write(original, encoder);
        encoder.flush();

        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        FraudAlert restored = new SpecificDatumReader<FraudAlert>(schema, schema).read(null, decoder);

        assertThat(restored.getTotalUsd()).isEqualTo(new BigDecimal("3000.00"));
        assertThat(restored.getTotalUsd().scale()).isEqualTo(2);
        assertThat(restored.getDetectedAt()).isEqualTo(detectedAt);
        assertThat(restored.getTxnIds()).containsExactly("txn-1", "txn-2");
    }

    /**
     * Reproduces {@code AvroTypeException: Found Channel, expecting string} - the error that would appear in
     * every SQL job if the wire schemas used Avro enums. See {@link io.flinkstream.common.Domains}.
     */
    @Test
    void anAvroEnumCannotBeReadIntoAString() {
        Schema writer = new Schema.Parser().parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"channel","type":{"type":"enum","name":"Channel","symbols":["ATM","WIRE"]}}]}
                """);
        // What Flink SQL derives from a `channel STRING` column.
        Schema reader = new Schema.Parser().parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"channel","type":["null","string"],"default":null}]}
                """);

        org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(writer);
        record.put("channel", new org.apache.avro.generic.GenericData.EnumSymbol(
                writer.getField("channel").schema(), "WIRE"));

        assertThatThrownBy(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>(writer)
                    .write(record, encoder);
            encoder.flush();
            new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>(writer, reader)
                    .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
        }).isInstanceOf(org.apache.avro.AvroTypeException.class);
    }

    /**
     * The evolution guarantee the project relies on: a producer may add fields without breaking a consumer whose
     * table DDL predates them. Avro drops writer-only fields during resolution.
     */
    @Test
    void aConsumerIgnoresFieldsItDoesNotKnowAbout() throws Exception {
        Schema writer = new Schema.Parser().parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"a","type":"string"},{"name":"added_later","type":"int"}]}
                """);
        Schema reader = new Schema.Parser().parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"a","type":["null","string"],"default":null}]}
                """);

        org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(writer);
        record.put("a", "kept");
        record.put("added_later", 42);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>(writer)
                .write(record, encoder);
        encoder.flush();

        org.apache.avro.generic.GenericRecord restored =
                new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>(writer, reader)
                        .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

        assertThat(restored.get("a").toString()).isEqualTo("kept");
        assertThat(restored.getSchema().getField("added_later")).isNull();
    }
}
