package smoke;

import io.github.mananmonga.bullseye.EndOffsetProbe;
import io.github.mananmonga.bullseye.Lane;
import io.github.mananmonga.bullseye.Bullseye;
import io.github.mananmonga.bullseye.kafka.KafkaEnvelope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/**
 * Runs a tiny gated job on the local runtime. Exists to prove the published jars load and run on a
 * Java 17 JVM; the assertion at the end is a sanity check, not a test suite.
 */
public final class SmokeJob {

    /** Probe with fixed targets; a real job uses KafkaEndOffsetProbe. */
    static final class FixedProbe implements EndOffsetProbe {
        private static final long serialVersionUID = 1L;

        @Override
        public Map<Integer, Long> probe() {
            Map<Integer, Long> m = new HashMap<>();
            m.put(0, 2L);
            m.put(1, 1L);
            return m;
        }
    }

    public static void main(String[] args) throws Exception {
        String runtime = System.getProperty("java.specification.version");
        System.out.println("smoke: running on Java " + runtime);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<KafkaEnvelope<String>> side = env.fromData(
                        List.of(new KafkaEnvelope<>(0, 0L, 0L, "a"), new KafkaEnvelope<>(0, 1L, 0L, "b"), new KafkaEnvelope<>(1, 0L, 0L, "c")),
                        KafkaEnvelope.typeInfo(Types.STRING));
        DataStream<String> main = env.fromData("x", "y", "z");

        DataStream<String> gated = Bullseye.forType(Types.STRING)
                .lane(Lane.of("smoke", 2, new FixedProbe()), side, KafkaEnvelope::getPartition, KafkaEnvelope::getOffset)
                .barrierUid("smoke-gate")
                .syncInterval(Duration.ofDays(3650))
                .apply(main);

        List<String> out = new ArrayList<>();
        try (CloseableIterator<String> it = gated.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        if (out.size() != 3) {
            throw new IllegalStateException("expected 3 records through the barrier, got " + out);
        }
        System.out.println("smoke: OK " + out);
    }
}
