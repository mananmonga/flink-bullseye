package io.github.mananmonga.bullseye.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/**
 * A side-input record together with the Kafka coordinates the barrier needs.
 *
 * <p>Use {@link KafkaEnvelopeDeserializer} to produce these from a {@code KafkaSource}, then
 * register the lane with {@code KafkaEnvelope::getPartition} and {@code KafkaEnvelope::getOffset}.
 *
 * <p>{@link #value} is {@code null} for a tombstone. Tombstones are <em>deliberately</em> kept: a
 * tombstone at the end of a compacted partition still occupies the last offset, and dropping it
 * would leave that partition permanently one short of its target.
 *
 * <p>Flink POJO with a generic field, so build its type information with {@link #typeInfo}
 * rather than relying on extraction (which would fall back to Kryo).
 */
public final class KafkaEnvelope<T> {

    public int partition;
    public long offset;
    public long timestamp;
    public T value;

    public KafkaEnvelope() {}

    public KafkaEnvelope(int partition, long offset, long timestamp, T value) {
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getValue() {
        return value;
    }

    /** {@code true} for a Kafka tombstone (null value). */
    public boolean isTombstone() {
        return value == null;
    }

    /** POJO type information for an envelope of {@code valueType}. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> TypeInformation<KafkaEnvelope<T>> typeInfo(TypeInformation<T> valueType) {
        Map<String, TypeInformation<?>> fields = new LinkedHashMap<>();
        fields.put("partition", Types.INT);
        fields.put("offset", Types.LONG);
        fields.put("timestamp", Types.LONG);
        fields.put("value", valueType);
        return (TypeInformation) Types.POJO(KafkaEnvelope.class, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KafkaEnvelope)) {
            return false;
        }
        KafkaEnvelope<?> that = (KafkaEnvelope<?>) o;
        return partition == that.partition
                && offset == that.offset
                && timestamp == that.timestamp
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offset, timestamp, value);
    }

    @Override
    public String toString() {
        return "KafkaEnvelope{" + partition + "@" + offset + " " + value + "}";
    }
}
