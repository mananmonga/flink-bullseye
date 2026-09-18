package io.github.mananmonga.flink.barrier.kafka;

import io.github.mananmonga.flink.barrier.EndOffsetProbe;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link EndOffsetProbe} for a Kafka topic: {@code partitionsFor} + {@code endOffsets} on a
 * short-lived consumer built from the supplied config.
 *
 * <p>Each call builds a consumer, makes two blocking round-trips and closes it, on the task
 * thread. That is acceptable only because the tracker calls it at most once per subtask per epoch;
 * do not call it from anywhere hotter.
 *
 * <p>Config notes:
 *
 * <ul>
 *   <li>{@code bootstrap.servers} is required. {@code group.id} is required too, so that brokers
 *       with group-prefix ACLs can authorise the metadata calls; the probe never commits.
 *   <li>{@code default.api.timeout.ms} defaults to {@value #DEFAULT_API_TIMEOUT_MS} here (Kafka's
 *       own default is 60s, which is too long to block a task thread). Override if you must.
 *   <li>{@code enable.auto.commit} is forced off.
 * </ul>
 *
 * <p><b>Transactional producers.</b> {@code endOffsets} returns the log end offset, which for a
 * topic written by a transactional producer sits one past the commit marker — an offset no
 * consumer ever receives. A lane fed that way would never reach its target. If that is your topic,
 * wrap this probe and subtract the marker, or supply your own {@link EndOffsetProbe}.
 */
public final class KafkaEndOffsetProbe implements EndOffsetProbe {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaEndOffsetProbe.class);

    /** Applied when the caller does not set {@code default.api.timeout.ms}. */
    public static final int DEFAULT_API_TIMEOUT_MS = 15_000;

    /** Seam for tests; production always uses {@link KafkaConsumer}. */
    @FunctionalInterface
    interface ConsumerFactory extends Serializable {
        Consumer<byte[], byte[]> create(Map<String, Object> config);
    }

    private final String topic;
    private final HashMap<String, Object> config;
    private final ConsumerFactory factory;

    private KafkaEndOffsetProbe(String topic, Map<String, Object> config, ConsumerFactory factory) {
        this.topic = topic;
        this.config = new HashMap<>(config);
        this.factory = factory;
    }

    /**
     * @param topic the side-input topic
     * @param consumerConfig Kafka consumer config; must contain {@code bootstrap.servers} and
     *     {@code group.id}. Values must be serializable (strings and numbers are).
     */
    public static KafkaEndOffsetProbe of(String topic, Map<String, ?> consumerConfig) {
        return new KafkaEndOffsetProbe(topic, normalise(topic, consumerConfig), KafkaEndOffsetProbe::newConsumer);
    }

    static KafkaEndOffsetProbe of(String topic, Map<String, ?> consumerConfig, ConsumerFactory factory) {
        return new KafkaEndOffsetProbe(topic, normalise(topic, consumerConfig), factory);
    }

    /**
     * Submission-time helper for {@link io.github.mananmonga.flink.barrier.Lane#of}: the number of
     * partitions {@code topic} has. Unlike {@link #probe()} this <em>throws</em> on failure, because
     * a job that cannot learn its lane's partition count at submission should not submit.
     */
    public static int partitionCount(String topic, Map<String, ?> consumerConfig) {
        Map<String, Object> cfg = normalise(topic, consumerConfig);
        try (Consumer<byte[], byte[]> consumer = newConsumer(cfg)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("topic '" + topic + "' has no partitions or does not exist");
            }
            return partitions.size();
        }
    }

    public String topic() {
        return topic;
    }

    @Override
    public Map<Integer, Long> probe() {
        try (Consumer<byte[], byte[]> consumer = factory.create(config)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                LOG.warn("probe: topic '{}' reported no partitions", topic);
                return null;
            }
            List<TopicPartition> tps =
                    partitions.stream().map(p -> new TopicPartition(topic, p.partition())).collect(Collectors.toList());
            Map<TopicPartition, Long> ends = consumer.endOffsets(tps);
            Map<Integer, Long> out = new HashMap<>(ends.size());
            for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
                out.put(e.getKey().partition(), e.getValue());
            }
            return out;
        } catch (RuntimeException e) {
            LOG.warn("probe: could not read end offsets for topic '{}'; keeping previous targets", topic, e);
            return null;
        }
    }

    private static Map<String, Object> normalise(String topic, Map<String, ?> consumerConfig) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(consumerConfig, "consumerConfig");
        Map<String, Object> cfg = new HashMap<>(consumerConfig);
        require(cfg, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        require(cfg, ConsumerConfig.GROUP_ID_CONFIG);
        cfg.putIfAbsent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, DEFAULT_API_TIMEOUT_MS);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return cfg;
    }

    private static void require(Map<String, Object> cfg, String key) {
        Object v = cfg.get(key);
        if (v == null || v.toString().isBlank()) {
            throw new IllegalArgumentException("consumerConfig must set '" + key + "'");
        }
    }

    private static Consumer<byte[], byte[]> newConsumer(Map<String, Object> config) {
        return new KafkaConsumer<>(config);
    }

    @Override
    public String toString() {
        return "KafkaEndOffsetProbe{" + topic + "}";
    }
}
