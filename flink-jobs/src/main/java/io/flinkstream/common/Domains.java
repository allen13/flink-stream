package io.flinkstream.common;

import java.util.List;

/**
 * The value domains that the Avro schemas carry as plain strings.
 *
 * <p>The schemas deliberately do not use Avro enums. Avro's schema-resolution rules do not allow a writer's
 * {@code enum} to be read into a reader's {@code string}, and Flink SQL's {@code avro-confluent} format derives
 * its reader schema from the table DDL - where the only reasonable column type for a symbolic value is
 * {@code STRING}. An enum on the wire therefore makes the topic unreadable from SQL with
 * {@code AvroTypeException: Found Channel, expecting string}.
 *
 * <p>Consequence, and the reason this class exists: the schema no longer validates the value set, so the
 * constraint has to live somewhere a human can find it. In a real system the same reasoning usually pushes you
 * to a string on the wire plus a check constraint in the serving store - which is exactly what
 * {@code charts/flink-stream/files/yugabyte-schema.sql} does.
 */
public final class Domains {

    public static final List<String> REGIONS = List.of("NORTHEAST", "SOUTHEAST", "MIDWEST", "SOUTHWEST", "WEST");
    public static final List<String> RISK_BANDS = List.of("LOW", "MEDIUM", "HIGH");
    public static final List<String> ACCOUNT_STATUSES = List.of("OPEN", "FROZEN", "CLOSED");
    public static final List<String> MERCHANT_CATEGORIES =
            List.of("GROCERY", "FUEL", "TRAVEL", "DINING", "ONLINE", "ATM", "OTHER");
    public static final List<String> CHANNELS =
            List.of("CARD_PRESENT", "CARD_NOT_PRESENT", "ATM", "ACH", "WIRE");
    public static final List<String> AUTH_DECISIONS = List.of("APPROVED", "DECLINED", "STEP_UP", "TIMEOUT");

    public static final String SEVERITY_INFO = "INFO";
    public static final String SEVERITY_WARN = "WARN";
    public static final String SEVERITY_CRITICAL = "CRITICAL";

    public static final String CHANNEL_CARD_NOT_PRESENT = "CARD_NOT_PRESENT";

    private Domains() {}
}
