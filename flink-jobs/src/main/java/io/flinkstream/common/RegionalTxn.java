package io.flinkstream.common;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A transaction that has been joined to its account and therefore knows its region.
 *
 * <p>Intermediate type only - it never touches Kafka, so it is a plain Flink POJO rather than an Avro record.
 * Flink recognises it as a POJO (public no-arg constructor, public fields) and serializes it with the efficient
 * {@code PojoSerializer} instead of falling back to Kryo.
 *
 * <p>The interesting property is {@link #region}: transactions are sharded by {@code account_id}, so keying by
 * region forces a full shuffle in which every source shard sends data to every downstream subtask.
 */
public class RegionalTxn {

    public String txnId;
    public String accountId;
    public String merchantId;
    public String region;
    public String riskBand;
    public String channel;
    public BigDecimal amount;
    public int shardId;
    public long eventTime;

    public RegionalTxn() {}

    public RegionalTxn(String txnId, String accountId, String merchantId, String region, String riskBand,
                       String channel, BigDecimal amount, int shardId, long eventTime) {
        this.txnId = txnId;
        this.accountId = accountId;
        this.merchantId = merchantId;
        this.region = region;
        this.riskBand = riskBand;
        this.channel = channel;
        this.amount = amount;
        this.shardId = shardId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "RegionalTxn{" + txnId + ", account=" + accountId + ", region=" + region
                + ", amount=" + amount + ", shard=" + shardId + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RegionalTxn other)) {
            return false;
        }
        return Objects.equals(txnId, other.txnId) && Objects.equals(region, other.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnId, region);
    }
}
