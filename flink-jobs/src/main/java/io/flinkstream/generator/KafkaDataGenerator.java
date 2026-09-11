package io.flinkstream.generator;

import io.flinkstream.avro.Account;
import io.flinkstream.avro.AuthEvent;
import io.flinkstream.avro.FxRate;
import io.flinkstream.avro.Geo;
import io.flinkstream.avro.Merchant;
import io.flinkstream.avro.Transaction;
import io.flinkstream.common.Domains;
import io.flinkstream.common.Shards;
import io.flinkstream.common.Topics;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Produces the data every lab reads. Not a Flink job - a plain Kafka producer, so the labs can be reasoned
 * about without a second Flink cluster in the way.
 *
 * <p>The data is shaped so that each lab has something to find:
 *
 * <ul>
 *   <li><b>Uniform baseline traffic</b> across 12 shards, so cross-shard aggregates have all shards present.</li>
 *   <li><b>Auth events delayed 0.5-40s</b> behind their transaction, straddling the 45s interval-join bound -
 *       some transactions deliberately never get a match.</li>
 *   <li><b>Velocity bursts</b>: an account fires 6-10 transactions inside a minute, tripping lab F.</li>
 *   <li><b>Card-testing escalations</b>: a sub-$5 card-not-present probe followed by rising charges, matching
 *       both the CEP pattern (lab G) and MATCH_RECOGNIZE (SQL lab 50).</li>
 *   <li><b>Impossible travel</b>: two card-present transactions 1500+ km apart minutes apart.</li>
 *   <li><b>Late events</b>: a slice of transactions stamped 60-180s in the past, arriving after their window
 *       has closed, so the lateness side output and the dead-letter topic actually receive something.</li>
 *   <li><b>Out-of-order events</b> within a few seconds, so the watermark's out-of-orderness bound matters.</li>
 * </ul>
 *
 * <p>Schemas are registered under {@code <topic>-value} on first send. That, not a migration step, is how the
 * topics get their schemas.
 */
public final class KafkaDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataGenerator.class);

    private static final List<String> CURRENCIES = List.of("USD", "EUR", "GBP", "JPY", "CAD", "MXN");

    private final Config config;
    private final KafkaProducer<String, SpecificRecord> producer;
    private final List<Account> accounts = new ArrayList<>();
    private final List<Merchant> merchants = new ArrayList<>();
    /** Auth events waiting for their delay to elapse, ordered by due time. */
    private final Deque<PendingAuth> pendingAuths = new ArrayDeque<>();

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnvironment();
        LOG.info("Starting generator: {}", config);
        new KafkaDataGenerator(config).run();
    }

    private KafkaDataGenerator(Config config) {
        this.config = config;
        this.producer = new KafkaProducer<>(producerProperties(config));
    }

    private static Properties producerProperties(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl);
        // Registering from the producer is convenient for a lab. In production you would set this false and
        // register schemas through CI, so an accidental code change cannot evolve a shared contract.
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        return props;
    }

    private void run() throws Exception {
        seedDimensions();

        long nextDimensionRefresh = System.currentTimeMillis() + config.dimensionRefresh.toMillis();
        long nextFxTick = System.currentTimeMillis();
        long nextAnomaly = System.currentTimeMillis() + 20_000;

        long intervalNanos = 1_000_000_000L / Math.max(1, config.transactionsPerSecond);
        long nextSend = System.nanoTime();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Flushing and closing producer");
            producer.flush();
            producer.close(Duration.ofSeconds(10));
        }));

        while (true) {
            long now = System.currentTimeMillis();

            drainDueAuths(now);

            if (now >= nextFxTick) {
                emitFxRates(now);
                nextFxTick = now + config.fxInterval.toMillis();
            }
            if (now >= nextDimensionRefresh) {
                refreshDimensions(now);
                nextDimensionRefresh = now + config.dimensionRefresh.toMillis();
            }
            if (now >= nextAnomaly) {
                injectAnomaly(now);
                nextAnomaly = now + config.anomalyInterval.toMillis();
            }

            emitTransaction(now);

            nextSend += intervalNanos;
            long sleepNanos = nextSend - System.nanoTime();
            if (sleepNanos > 0) {
                Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
            } else {
                // Fell behind the target rate; resync rather than accumulating debt forever.
                nextSend = System.nanoTime();
            }
        }
    }

    // ---------------------------------------------------------------------------------------- dimensions

    private void seedDimensions() {
        // Deliberately backdated. A versioned table can only answer "as of T" if it holds a version whose
        // timestamp is at or before T - so if the seed rows were stamped `now`, every temporal join over the
        // replayed transaction backlog would return NULL, and region/risk_band would come out 'UNKNOWN'
        // everywhere. Backdating the seed past the topic's retention makes the dimension valid for any
        // transaction a job can still read. Later refreshes use the real clock, which is what makes the
        // *versioned* part of the join observable.
        Instant now = Instant.now().minus(Duration.ofHours(24));

        for (int i = 0; i < config.merchantCount; i++) {
            String id = String.format(Locale.ROOT, "mch-%04d", i);
            merchants.add(Merchant.newBuilder()
                    .setMerchantId(id)
                    .setName(merchantName(i))
                    .setCategory(Domains.MERCHANT_CATEGORIES.get(i % Domains.MERCHANT_CATEGORIES.size()))
                    .setCountry(i % 11 == 0 ? "MX" : "US")
                    .setMcc(5000 + (i % 900))
                    .setTrustScore(Math.round((0.35d + (i % 60) / 100d) * 100d) / 100d)
                    .setUpdatedAt(now)
                    .build());
        }
        merchants.forEach(m -> send(Topics.MERCHANTS, m.getMerchantId(), m));

        for (int i = 0; i < config.accountCount; i++) {
            String id = String.format(Locale.ROOT, "acct-%05d", i);
            accounts.add(Account.newBuilder()
                    .setAccountId(id)
                    .setCustomerId(String.format(Locale.ROOT, "cust-%05d", i / 2))
                    // Regions are assigned round-robin over accounts, and accounts hash uniformly over the 12
                    // Kafka shards. So every region's data is spread across every shard - which is what makes
                    // GROUP BY region a genuine all-to-all shuffle rather than a shard-local aggregate.
                    .setRegion(Domains.REGIONS.get(i % Domains.REGIONS.size()))
                    .setRiskBand(Domains.RISK_BANDS.get(i % Domains.RISK_BANDS.size()))
                    .setCreditLimit(money(2_000 + (i % 40) * 500))
                    .setStatus(i % 97 == 0 ? "FROZEN" : "OPEN")
                    .setOpenedAt(now.minus(Duration.ofDays(30 + (i % 900))))
                    .setUpdatedAt(now)
                    .build());
        }
        accounts.forEach(a -> send(Topics.ACCOUNTS, a.getAccountId(), a));

        emitFxRates(System.currentTimeMillis(), true);

        LOG.info("Seeded {} merchants, {} accounts and {} FX rates, all backdated 24h",
                merchants.size(), accounts.size(), CURRENCIES.size());
        producer.flush();
    }

    /**
     * Publishes a new version of a handful of dimension rows.
     *
     * <p>This is what makes the versioned/temporal joins non-trivial: without updates, {@code FOR SYSTEM_TIME
     * AS OF} and a plain join would produce identical output and the distinction would be invisible.
     */
    private void refreshDimensions(long now) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Instant ts = Instant.ofEpochMilli(now);

        for (int i = 0; i < 5 && !accounts.isEmpty(); i++) {
            int idx = rnd.nextInt(accounts.size());
            Account updated = Account.newBuilder(accounts.get(idx))
                    .setRiskBand(Domains.RISK_BANDS.get(rnd.nextInt(Domains.RISK_BANDS.size())))
                    .setUpdatedAt(ts)
                    .build();
            accounts.set(idx, updated);
            send(Topics.ACCOUNTS, updated.getAccountId(), updated);
        }

        for (int i = 0; i < 3 && !merchants.isEmpty(); i++) {
            int idx = rnd.nextInt(merchants.size());
            Merchant updated = Merchant.newBuilder(merchants.get(idx))
                    .setTrustScore(Math.round(rnd.nextDouble(0.1d, 1.0d) * 100d) / 100d)
                    .setUpdatedAt(ts)
                    .build();
            merchants.set(idx, updated);
            send(Topics.MERCHANTS, updated.getMerchantId(), updated);
        }
    }

    private void emitFxRates(long now) {
        emitFxRates(now, false);
    }

    /**
     * @param backdated stamp the rates 24h in the past. Used once at startup for the same reason the account
     *                  and merchant seeds are backdated: without a version older than the facts, the
     *                  FOR SYSTEM_TIME AS OF join in sql/40-joins.sql matches nothing.
     */
    private void emitFxRates(long now, boolean backdated) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Instant ts = backdated
                ? Instant.ofEpochMilli(now).minus(Duration.ofHours(24))
                : Instant.ofEpochMilli(now);
        for (String currency : CURRENCIES) {
            BigDecimal rate = switch (currency) {
                case "USD" -> BigDecimal.ONE;
                case "EUR" -> BigDecimal.valueOf(1.05d + rnd.nextDouble(-0.02d, 0.02d));
                case "GBP" -> BigDecimal.valueOf(1.24d + rnd.nextDouble(-0.02d, 0.02d));
                case "JPY" -> BigDecimal.valueOf(0.0067d + rnd.nextDouble(-0.0002d, 0.0002d));
                case "CAD" -> BigDecimal.valueOf(0.73d + rnd.nextDouble(-0.01d, 0.01d));
                default -> BigDecimal.valueOf(0.058d + rnd.nextDouble(-0.002d, 0.002d));
            };
            send(Topics.FX_RATES, currency, FxRate.newBuilder()
                    .setCurrency(currency)
                    .setRateToUsd(rate.setScale(6, RoundingMode.HALF_UP))
                    .setEventTime(ts)
                    .build());
        }
    }

    // ---------------------------------------------------------------------------------------- facts

    private void emitTransaction(long now) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Account account = accounts.get(rnd.nextInt(accounts.size()));
        Merchant merchant = merchants.get(rnd.nextInt(merchants.size()));

        long eventTime = now;
        if (rnd.nextInt(100) < config.lateEventPercent) {
            // Stamped well before the current watermark: this record will miss its window entirely and show up
            // on the late-data side output.
            eventTime = now - rnd.nextLong(60_000L, 180_000L);
        } else {
            // Normal jitter: a few seconds of out-of-orderness, which the watermark strategy absorbs.
            eventTime = now - rnd.nextLong(0L, 4_000L);
        }

        String channel = Domains.CHANNELS.get(weightedChannelIndex(rnd));
        BigDecimal amount = money(rnd.nextDouble(1.50d, 900.00d));
        String currency = rnd.nextInt(100) < 20
                ? CURRENCIES.get(rnd.nextInt(CURRENCIES.size()))
                : "USD";

        emit(account, merchant, amount, currency, channel, eventTime, geoFor(account, rnd), Map.of(
                "source", "generator",
                "band", amount.compareTo(BigDecimal.valueOf(250)) > 0 ? "high" : "normal"));
    }

    /** The three anomaly shapes, rotated so every detector has traffic to find. */
    private void injectAnomaly(long now) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Account account = accounts.get(rnd.nextInt(accounts.size()));

        switch (rnd.nextInt(3)) {
            case 0 -> {
                // Velocity burst: enough count and value inside the rule window to trip lab F.
                int burst = rnd.nextInt(6, 11);
                LOG.info("Injecting velocity burst of {} on {}", burst, account.getAccountId());
                for (int i = 0; i < burst; i++) {
                    emit(account, merchants.get(rnd.nextInt(merchants.size())),
                            money(rnd.nextDouble(300d, 900d)), "USD", "CARD_NOT_PRESENT",
                            now + i * 1_500L, geoFor(account, rnd), Map.of("anomaly", "velocity"));
                }
            }
            case 1 -> {
                // Card testing: a sub-$5 probe, then strictly escalating charges. Matches the CEP pattern and
                // MATCH_RECOGNIZE; the amounts must strictly increase or neither fires.
                Merchant merchant = merchants.get(rnd.nextInt(merchants.size()));
                LOG.info("Injecting card-testing escalation on {}", account.getAccountId());
                emit(account, merchant, money(rnd.nextDouble(0.5d, 4.5d)), "USD", "CARD_NOT_PRESENT",
                        now, geoFor(account, rnd), Map.of("anomaly", "card-testing-probe"));
                double amount = 25d;
                for (int i = 0; i < 3; i++) {
                    amount *= rnd.nextDouble(2.0d, 4.0d);
                    emit(account, merchant, money(amount), "USD", "CARD_NOT_PRESENT",
                            now + (i + 1) * 20_000L, geoFor(account, rnd), Map.of("anomaly", "card-testing"));
                }
            }
            default -> {
                // Impossible travel: Phoenix then Zurich, eight minutes apart.
                LOG.info("Injecting impossible travel on {}", account.getAccountId());
                Merchant merchant = merchants.get(rnd.nextInt(merchants.size()));
                emit(account, merchant, money(rnd.nextDouble(20d, 200d)), "USD", "CARD_PRESENT",
                        now, new Geo(33.4484d, -112.0740d), Map.of("anomaly", "travel-leg-1"));
                emit(account, merchant, money(rnd.nextDouble(20d, 200d)), "USD", "CARD_PRESENT",
                        now + 480_000L, new Geo(47.3769d, 8.5417d), Map.of("anomaly", "travel-leg-2"));
            }
        }
    }

    private void emit(Account account, Merchant merchant, BigDecimal amount, String currency, String channel,
                      long eventTimeMillis, Geo geo, Map<String, String> tags) {

        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String txnId = UUID.randomUUID().toString();

        Transaction txn = Transaction.newBuilder()
                .setTxnId(txnId)
                .setAccountId(account.getAccountId())
                .setMerchantId(merchant.getMerchantId())
                .setAmount(amount)
                .setCurrency(currency)
                .setChannel(channel)
                .setDeviceId("dev-" + Math.abs(account.getAccountId().hashCode() % 5000))
                .setGeo(geo)
                .setTags(new HashMap<>(tags))
                // Computed with Kafka's own partitioner so the field always agrees with the partition the record
                // actually lands in. The SQL labs compare it against the `partition` metadata column to prove it.
                .setShardId(Shards.kafkaPartitionFor(account.getAccountId(), config.transactionPartitions))
                .setEventTime(Instant.ofEpochMilli(eventTimeMillis))
                .build();

        send(Topics.TRANSACTIONS, txn.getAccountId(), txn);

        // Most transactions get an auth decision after a realistic delay; a few never do, so the interval join
        // has genuine non-matches.
        if (rnd.nextInt(100) < 92) {
            long delayMillis = rnd.nextInt(100) < 90
                    ? rnd.nextLong(500L, 20_000L)     // inside the 45s join window
                    : rnd.nextLong(50_000L, 90_000L); // deliberately outside it
            pendingAuths.addLast(new PendingAuth(
                    System.currentTimeMillis() + Math.min(delayMillis, 30_000L),
                    txn, eventTimeMillis + delayMillis));
        }
    }

    private void drainDueAuths(long now) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (!pendingAuths.isEmpty() && pendingAuths.peekFirst().dueAtMillis <= now) {
            PendingAuth pending = pendingAuths.pollFirst();
            Transaction txn = pending.transaction;

            int roll = rnd.nextInt(100);
            String decision = roll < 88 ? "APPROVED" : roll < 95 ? "DECLINED" : roll < 99 ? "STEP_UP" : "TIMEOUT";

            send(Topics.AUTH_EVENTS, txn.getAccountId(), AuthEvent.newBuilder()
                    .setAuthId(UUID.randomUUID().toString())
                    .setTxnId(txn.getTxnId())
                    .setAccountId(txn.getAccountId())
                    .setDecision(decision)
                    .setReasonCode("APPROVED".equals(decision) ? null : "R" + rnd.nextInt(100, 999))
                    .setLatencyMs(rnd.nextInt(8, 450))
                    .setEventTime(Instant.ofEpochMilli(pending.eventTimeMillis))
                    .build());
        }
    }

    // ---------------------------------------------------------------------------------------- helpers

    private void send(String topic, String key, SpecificRecord value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Failed to produce to {}", topic, exception);
            }
        });
    }

    /** Card-present dominates, with enough card-not-present traffic to feed the fraud patterns. */
    private static int weightedChannelIndex(ThreadLocalRandom rnd) {
        int roll = rnd.nextInt(100);
        if (roll < 55) {
            return 0;   // CARD_PRESENT
        } else if (roll < 85) {
            return 1;   // CARD_NOT_PRESENT
        } else if (roll < 93) {
            return 2;   // ATM
        } else if (roll < 98) {
            return 3;   // ACH
        }
        return 4;       // WIRE
    }

    private static Geo geoFor(Account account, ThreadLocalRandom rnd) {
        if (rnd.nextInt(100) < 25) {
            return null;    // exercises the nullable nested record
        }
        double[] centre = switch (account.getRegion()) {
            case "NORTHEAST" -> new double[] {42.36d, -71.06d};
            case "SOUTHEAST" -> new double[] {33.75d, -84.39d};
            case "MIDWEST" -> new double[] {41.88d, -87.63d};
            case "SOUTHWEST" -> new double[] {33.45d, -112.07d};
            default -> new double[] {37.77d, -122.42d};
        };
        return new Geo(centre[0] + rnd.nextDouble(-0.4d, 0.4d), centre[1] + rnd.nextDouble(-0.4d, 0.4d));
    }

    private static BigDecimal money(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    private static String merchantName(int i) {
        String[] prefixes = {"Northgate", "Riverside", "Parkway", "Lakeview", "Summit", "Harbor", "Meridian"};
        String[] suffixes = {"Market", "Fuel", "Travel", "Grill", "Online", "ATM", "Supply"};
        return prefixes[i % prefixes.length] + " " + suffixes[(i / prefixes.length) % suffixes.length] + " " + i;
    }

    private record PendingAuth(long dueAtMillis, Transaction transaction, long eventTimeMillis) {}

    /** All generator settings, from the environment so the chart can tune them without a rebuild. */
    private record Config(
            String bootstrapServers,
            String schemaRegistryUrl,
            int accountCount,
            int merchantCount,
            int transactionsPerSecond,
            int transactionPartitions,
            int lateEventPercent,
            Duration fxInterval,
            Duration dimensionRefresh,
            Duration anomalyInterval) {

        static Config fromEnvironment() {
            return new Config(
                    env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                    env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
                    intEnv("ACCOUNT_COUNT", 400),
                    intEnv("MERCHANT_COUNT", 60),
                    intEnv("TRANSACTIONS_PER_SECOND", 40),
                    intEnv("TRANSACTION_PARTITIONS", 12),
                    intEnv("LATE_EVENT_PERCENT", 3),
                    Duration.ofSeconds(intEnv("FX_INTERVAL_S", 30)),
                    Duration.ofSeconds(intEnv("DIMENSION_REFRESH_S", 60)),
                    Duration.ofSeconds(intEnv("ANOMALY_INTERVAL_S", 45)));
        }

        private static String env(String key, String defaultValue) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static int intEnv(String key, int defaultValue) {
            String value = System.getenv(key);
            return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
        }
    }
}
