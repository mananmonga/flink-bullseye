package io.github.mananmonga.flink.barrier.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class KafkaEnvelopeDeserializerTest {

    private static final class ListCollector<T> implements Collector<T> {
        final List<T> items = new ArrayList<>();

        @Override
        public void collect(T record) {
            items.add(record);
        }

        @Override
        public void close() {}
    }

    @Test
    void carriesPartitionOffsetTimestampAndValue() throws Exception {
        KafkaEnvelopeDeserializer<String> d = KafkaEnvelopeDeserializer.of(new SimpleStringSchema());
        ListCollector<KafkaEnvelope<String>> out = new ListCollector<>();
        d.deserialize(new ConsumerRecord<>("t", 3, 41L, "k".getBytes(), "hello".getBytes()), out);
        assertThat(out.items).hasSize(1);
        KafkaEnvelope<String> e = out.items.get(0);
        assertThat(e.getPartition()).isEqualTo(3);
        assertThat(e.getOffset()).isEqualTo(41L);
        assertThat(e.getValue()).isEqualTo("hello");
        assertThat(e.isTombstone()).isFalse();
    }

    @Test
    void keepsTombstonesSoTheLastOffsetStillCounts() throws Exception {
        KafkaEnvelopeDeserializer<String> d = KafkaEnvelopeDeserializer.of(new SimpleStringSchema());
        ListCollector<KafkaEnvelope<String>> out = new ListCollector<>();
        d.deserialize(new ConsumerRecord<>("t", 0, 7L, "k".getBytes(), null), out);
        assertThat(out.items).hasSize(1);
        assertThat(out.items.get(0).isTombstone()).isTrue();
        assertThat(out.items.get(0).getOffset()).isEqualTo(7L);
    }

    @Test
    void producedTypeIsAPojoNotKryoAndRoundTripsNulls() throws Exception {
        TypeInformation<KafkaEnvelope<String>> type = KafkaEnvelope.typeInfo(Types.STRING);
        assertThat(type).isInstanceOf(PojoTypeInfo.class);
        TypeSerializer<KafkaEnvelope<String>> ser = type.createSerializer(new org.apache.flink.api.common.serialization.SerializerConfigImpl());

        for (KafkaEnvelope<String> in : List.<KafkaEnvelope<String>>of(new KafkaEnvelope<>(1, 2L, 3L, "v"), new KafkaEnvelope<>(1, 2L, 3L, null))) {
            DataOutputSerializer buf = new DataOutputSerializer(64);
            ser.serialize(in, buf);
            KafkaEnvelope<String> back = ser.deserialize(new DataInputDeserializer(buf.getSharedBuffer(), 0, buf.length()));
            assertThat(back).isEqualTo(in);
        }
    }
}
