package io.github.mananmonga.bullseye;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

class BullseyeTest {

    private static DataStream<SideRecord> side(StreamExecutionEnvironment env) {
        return env.fromData(new SideRecord(0, 0, "x"));
    }

    @Test
    void uidDerivationReproducesTheFrozenContract() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> main = env.fromData("a");
        Bullseye.forType(Types.STRING)
                .lane(Lane.of("rule", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset)
                .lane(Lane.of("portfolio", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset)
                .lane(Lane.of("cve", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset)
                .barrierUid("eval-gate")
                .apply(main)
                .sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();
        Set<String> uids = graph.getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .filter(u -> u != null)
                .collect(Collectors.toSet());
        assertThat(uids).containsExactlyInAnyOrder(
                "eval-gate",
                "rule-offsets", "rule-readiness",
                "portfolio-offsets", "portfolio-readiness",
                "cve-offsets", "cve-readiness");
    }

    @Test
    void gateInheritsMainParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> main = env.fromData("a").map(s -> s).setParallelism(2);
        DataStream<String> gated = Bullseye.forType(Types.STRING)
                .lane(Lane.of("rule", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset)
                .barrierUid("gate")
                .apply(main);
        assertThat(gated.getParallelism()).isEqualTo(2);
    }

    @Test
    void barrierUidIsRequired() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Bullseye<String> b = Bullseye.forType(Types.STRING)
                .lane(Lane.of("rule", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset);
        assertThatThrownBy(() -> b.apply(env.fromData("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("barrierUid");
        assertThatThrownBy(() -> b.barrierUid(" ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void atLeastOneLaneIsRequired() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Bullseye<String> b = Bullseye.forType(Types.STRING).barrierUid("gate");
        assertThatThrownBy(() -> b.apply(env.fromData("a"))).hasMessageContaining("lane");
    }

    @Test
    void duplicateLaneIdsAreRejected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Bullseye<String> b = Bullseye.forType(Types.STRING)
                .lane(Lane.of("rule", 3, StubProbe.of()), side(env), r -> r.partition, r -> r.offset);
        assertThatThrownBy(() -> b.lane(Lane.of("rule", 1, StubProbe.of()), side(env), r -> r.partition, r -> r.offset))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rule");
    }

    @Test
    void knobsAreValidated() {
        Bullseye<String> b = Bullseye.forType(Types.STRING);
        assertThatThrownBy(() -> b.syncInterval(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.idleBackstop(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.maxBuffered(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.clock(new Clock() {
            @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public java.time.Instant instant() { return java.time.Instant.EPOCH; }
        })).as("non-serializable clock").isInstanceOf(IllegalArgumentException.class);
        assertThat(b.clock(Clock.systemUTC())).isSameAs(b);
        assertThat(b.clock(Clock.fixed(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC))).isSameAs(b);
    }

    @Test
    void endToEndReleasesEveryMainRecordOnceLanesAreConsumedToTarget() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Partition 0 has 3 records, partition 1 has 1, partition 2 is empty: targets 3, 1, 0.
        DataStream<SideRecord> rules = env.fromData(
                new SideRecord(0, 0, "r0"), new SideRecord(0, 1, "r1"), new SideRecord(0, 2, "r2"),
                new SideRecord(1, 0, "r3"));
        List<String> mainRecords = java.util.stream.IntStream.range(0, 200).mapToObj(i -> "obs-" + i).collect(Collectors.toList());
        DataStream<String> main = env.fromData(mainRecords);

        DataStream<String> gated = Bullseye.forType(Types.STRING)
                .lane(Lane.of("rule", 3, StubProbe.of(3, 1, 0)), rules, r -> r.partition, r -> r.offset)
                .barrierUid("gate")
                .syncInterval(Duration.ofDays(3650)) // no boundary during the test
                .idleBackstop(Duration.ofMinutes(10))
                .apply(main);

        List<String> collected = new java.util.ArrayList<>();
        org.apache.flink.util.CloseableIterator<String> it = gated.executeAndCollect();
        try {
            it.forEachRemaining(collected::add);
        } finally {
            it.close();
        }
        assertThat(collected).containsExactlyInAnyOrderElementsOf(mainRecords);
    }
}
