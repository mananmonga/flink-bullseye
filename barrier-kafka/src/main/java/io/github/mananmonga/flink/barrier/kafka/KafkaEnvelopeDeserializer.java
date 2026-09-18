package io.github.mananmonga.flink.barrier.kafka;

import java.io.IOException;
import java.util.Objects;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Wraps a value {@link DeserializationSchema} so a {@code KafkaSource} emits
 * {@link KafkaEnvelope}s carrying partition and offset alongside the value.
 *
 * <pre>{@code
 * KafkaSource<KafkaEnvelope<Rule>> rules = KafkaSource.<KafkaEnvelope<Rule>>builder()
 *     .setDeserializer(KafkaEnvelopeDeserializer.of(new RuleSchema()))
 *     ...
 * }</pre>
 *
 * <p>Tombstones (null values) are emitted as envelopes with a null {@link KafkaEnvelope#value};
 * the inner schema is not invoked for them. See {@link KafkaEnvelope} for why they are kept.
 */
public final class KafkaEnvelopeDeserializer<T> implements KafkaRecordDeserializationSchema<KafkaEnvelope<T>> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> inner;

    private KafkaEnvelopeDeserializer(DeserializationSchema<T> inner) {
        this.inner = Objects.requireNonNull(inner, "inner");
    }

    public static <T> KafkaEnvelopeDeserializer<T> of(DeserializationSchema<T> valueSchema) {
        return new KafkaEnvelopeDeserializer<>(valueSchema);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        inner.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaEnvelope<T>> out)
            throws IOException {
        T value = record.value() == null ? null : inner.deserialize(record.value());
        out.collect(new KafkaEnvelope<>(record.partition(), record.offset(), record.timestamp(), value));
    }

    @Override
    public TypeInformation<KafkaEnvelope<T>> getProducedType() {
        return KafkaEnvelope.typeInfo(inner.getProducedType());
    }
}
