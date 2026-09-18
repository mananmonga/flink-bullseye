package io.github.mananmonga.flink.barrier.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mananmonga.flink.barrier.Lane;
import io.github.mananmonga.flink.barrier.ManualClock;
import io.github.mananmonga.flink.barrier.StubProbe;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LaneReadinessTrackerTest {

    private static final long INTERVAL = 60_000L;

    private final ManualClock clock = new ManualClock(INTERVAL * 10 + 100); // epoch 10
    private KeyedOneInputStreamOperatorTestHarness<Integer, LaneOffset, LaneReady> harness;

    private KeyedOneInputStreamOperatorTestHarness<Integer, LaneOffset, LaneReady> start(StubProbe probe, boolean enabled)
            throws Exception {
        Lane lane = Lane.of("rule", 3, probe);
        LaneReadinessTracker fn =
                new LaneReadinessTracker(lane, new SyncEpoch(Duration.ofMillis(INTERVAL)), clock, enabled);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(fn), o -> o.partition, Types.INT);
        harness.setProcessingTime(clock.millis());
        harness.open();
        return harness;
    }

    private void tick(long millis) throws Exception {
        clock.advance(millis);
        harness.setProcessingTime(clock.millis());
    }

    private void record(int partition, long offset) throws Exception {
        harness.processElement(new StreamRecord<>(new LaneOffset("rule", partition, offset)));
    }

    private List<LaneReady> markers() {
        return harness.extractOutputValues().stream().map(LaneReady.class::cast).collect(Collectors.toList());
    }

    @AfterEach
    void close() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void declaresWhenConsumedToTargetAndOnlyOncePerEpoch() throws Exception {
        StubProbe probe = StubProbe.of(3, 1, 0); // partition 2 is empty
        start(probe, true);

        record(0, 0);
        record(0, 1);
        assertThat(markers()).as("behind target 3").containsExactly(new LaneReady("rule", 2, 10)); // empty partition declared on our behalf
        record(0, 2);
        assertThat(markers()).containsExactly(new LaneReady("rule", 2, 10), new LaneReady("rule", 0, 10));
        record(0, 2); // duplicate delivery: no second marker
        record(0, 3); // beyond the probed target: still no second marker this epoch
        assertThat(markers()).hasSize(2);

        record(1, 0);
        assertThat(markers()).containsExactly(
                new LaneReady("rule", 2, 10), new LaneReady("rule", 0, 10), new LaneReady("rule", 1, 10));
        assertThat(probe.calls()).as("one probe per epoch per subtask").isEqualTo(1);
    }

    @Test
    void quietPartitionRedeclaresAtEpochBoundaryViaTimer() throws Exception {
        StubProbe probe = StubProbe.of(1, 1, 1);
        start(probe, true);
        record(0, 0);
        record(1, 0);
        record(2, 0);
        assertThat(markers()).hasSize(3);

        tick(INTERVAL); // now in epoch 11, no new records
        assertThat(markers()).hasSize(6);
        assertThat(markers().subList(3, 6)).extracting(m -> m.epoch).containsOnly(11L);
        assertThat(probe.calls()).isEqualTo(2);
    }

    @Test
    void driftedPartitionDoesNotDeclareTheNewEpoch() throws Exception {
        StubProbe probe = StubProbe.of(1, 1, 1);
        start(probe, true);
        record(0, 0);
        record(1, 0);
        record(2, 0);
        probe.set(1, 5); // producer wrote 4 more records to partition 1 that the consumer has not seen
        tick(INTERVAL);
        assertThat(markers().stream().filter(m -> m.epoch == 11).map(m -> m.partition))
                .containsExactlyInAnyOrder(0, 2);
        record(1, 4); // consumer catches up
        assertThat(markers().stream().filter(m -> m.epoch == 11).map(m -> m.partition))
                .containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    void nullProbeKeepsPreviousTargetsAndBacksOff() throws Exception {
        StubProbe probe = StubProbe.of(2, 1, 1);
        start(probe, true);
        record(0, 1);
        assertThat(markers()).hasSize(1);
        assertThat(probe.calls()).isEqualTo(1);

        probe.failing(true);
        tick(INTERVAL); // epoch 11: probe fails, previous targets reused → partition 0 re-declares
        assertThat(probe.calls()).isEqualTo(2);
        assertThat(markers().stream().filter(m -> m.epoch == 11).map(m -> m.partition)).containsExactly(0);

        record(1, 0); // inside the backoff window: no re-probe, stale targets still answer
        assertThat(probe.calls()).isEqualTo(2);
        assertThat(markers().stream().filter(m -> m.epoch == 11).map(m -> m.partition)).containsExactly(0, 1);

        tick(LaneReadinessTracker.PROBE_RETRY_MILLIS);
        record(2, 0); // backoff elapsed and an undeclared partition asks: re-probe (still failing)
        assertThat(probe.calls()).isEqualTo(3);
        assertThat(markers().stream().filter(m -> m.epoch == 11).map(m -> m.partition)).containsExactly(0, 1, 2);
    }

    @Test
    void nullProbeWithNoPreviousTargetsIsNotReady() throws Exception {
        StubProbe probe = StubProbe.of(1, 1, 1).failing(true);
        start(probe, true);
        record(0, 0);
        assertThat(markers()).isEmpty();
        probe.failing(false);
        tick(LaneReadinessTracker.PROBE_RETRY_MILLIS);
        assertThat(markers()).containsExactly(new LaneReady("rule", 0, 10));
    }

    @Test
    void disabledTrackerTracksOffsetsButNeverProbesOrDeclares() throws Exception {
        StubProbe probe = StubProbe.of(1, 1, 1);
        start(probe, false);
        record(0, 0);
        tick(INTERVAL);
        assertThat(markers()).isEmpty();
        assertThat(probe.calls()).isZero();
    }

    @Test
    void stateSurvivesSnapshotAndRestore() throws Exception {
        StubProbe probe = StubProbe.of(2, 1, 1);
        start(probe, true);
        record(0, 0);
        var snapshot = harness.snapshot(1L, clock.millis());
        harness.close();

        Lane lane = Lane.of("rule", 3, probe);
        LaneReadinessTracker fn =
                new LaneReadinessTracker(lane, new SyncEpoch(Duration.ofMillis(INTERVAL)), clock, true);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(fn), o -> o.partition, Types.INT);
        harness.setProcessingTime(clock.millis());
        harness.initializeState(snapshot);
        harness.open();

        record(0, 1); // highestOffset restored as 0, so this makes consumedTo == 2 == target
        assertThat(markers()).containsExactly(new LaneReady("rule", 0, 10));
    }
}
