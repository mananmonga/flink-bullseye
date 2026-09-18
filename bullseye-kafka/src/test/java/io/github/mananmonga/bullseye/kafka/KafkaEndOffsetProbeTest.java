package io.github.mananmonga.bullseye.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaEndOffsetProbeTest {

    private static final Map<String, Object> CONFIG = Map.of("bootstrap.servers", "localhost:1", "group.id", "probe-test");

    private static MockConsumer<byte[], byte[]> mock(String topic, long... ends) {
        MockConsumer<byte[], byte[]> c = new MockConsumer<>(OffsetResetStrategy.LATEST);
        java.util.ArrayList<PartitionInfo> infos = new java.util.ArrayList<>();
        java.util.HashMap<TopicPartition, Long> endOffsets = new java.util.HashMap<>();
        for (int p = 0; p < ends.length; p++) {
            infos.add(new PartitionInfo(topic, p, null, null, null));
            endOffsets.put(new TopicPartition(topic, p), ends[p]);
        }
        c.updatePartitions(topic, infos);
        c.updateEndOffsets(endOffsets);
        return c;
    }

    @Test
    void readsExclusiveEndOffsetPerPartition() {
        KafkaEndOffsetProbe probe = KafkaEndOffsetProbe.of("rules", CONFIG, cfg -> mock("rules", 5, 0, 12));
        assertThat(probe.probe()).containsExactlyInAnyOrderEntriesOf(Map.of(0, 5L, 1, 0L, 2, 12L));
    }

    @Test
    void returnsNullNeverThrowsOnFailure() {
        KafkaEndOffsetProbe probe = KafkaEndOffsetProbe.of("rules", CONFIG, cfg -> {
            throw new KafkaException("broker unreachable");
        });
        assertThat(probe.probe()).isNull();

        KafkaEndOffsetProbe unknownTopic = KafkaEndOffsetProbe.of("missing", CONFIG, cfg -> mock("rules", 1));
        assertThat(unknownTopic.probe()).isNull();
    }

    @Test
    void requiresBootstrapServersAndGroupId() {
        assertThatThrownBy(() -> KafkaEndOffsetProbe.of("rules", Map.of("group.id", "g")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bootstrap.servers");
        assertThatThrownBy(() -> KafkaEndOffsetProbe.of("rules", Map.of("bootstrap.servers", "h:1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("group.id");
    }

    @Test
    void forcesSafeConsumerSettings() {
        KafkaEndOffsetProbe probe = KafkaEndOffsetProbe.of("rules", CONFIG, cfg -> {
            assertThat(cfg).containsEntry("enable.auto.commit", false);
            assertThat(cfg).containsEntry("default.api.timeout.ms", KafkaEndOffsetProbe.DEFAULT_API_TIMEOUT_MS);
            return mock("rules", 1);
        });
        assertThat(probe.probe()).isNotNull();
    }

    @Test
    void isJavaSerializable() throws Exception {
        KafkaEndOffsetProbe probe = KafkaEndOffsetProbe.of("rules", CONFIG);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(probe);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(((KafkaEndOffsetProbe) in.readObject()).topic()).isEqualTo("rules");
        }
    }

    @Test
    void partitionCountThrowsLoudlyAtSubmission() {
        // localhost:1 with a short timeout: the real consumer cannot fetch metadata and must throw,
        // because a job that cannot learn its lane's partition count must not submit.
        Map<String, Object> cfg = Map.of(
                "bootstrap.servers", "localhost:1", "group.id", "g", "default.api.timeout.ms", 500, "request.timeout.ms", 300);
        assertThatThrownBy(() -> KafkaEndOffsetProbe.partitionCount("rules", cfg)).isInstanceOf(RuntimeException.class);
        assertThat(List.of()).isEmpty();
    }
}
