package io.github.mananmonga.bullseye.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mananmonga.bullseye.Lane;
import io.github.mananmonga.bullseye.ManualClock;
import io.github.mananmonga.bullseye.StubProbe;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BullseyeGateTest {

    private static final long INTERVAL = 600_000L;
    private static final long IDLE = 30_000L;
    private static final int MAX = 3;

    private final ManualClock clock = new ManualClock(INTERVAL * 10 + 100); // epoch 10
    private final List<Lane> lanes = List.of(Lane.of("rule", 2, StubProbe.of()), Lane.of("portfolio", 1, StubProbe.of()));
    private TwoInputStreamOperatorTestHarness<String, LaneReady, String> harness;

    private BullseyeGate<String> gate(boolean failOnOverflow, boolean enabled) {
        return new BullseyeGate<>(
                lanes, new SyncEpoch(Duration.ofMillis(INTERVAL)), clock, Types.STRING, MAX, failOnOverflow, IDLE, enabled);
    }

    private TwoInputStreamOperatorTestHarness<String, LaneReady, String> start(BullseyeGate<String> gate) throws Exception {
        harness = new TwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithNonKeyedOperator<>(gate, List.of(BullseyeGate.READY_STATE)));
        harness.open();
        return harness;
    }

    private void main(String s) throws Exception {
        harness.processElement1(new StreamRecord<>(s));
    }

    private void ready(String lane, int partition, long epoch) throws Exception {
        harness.processElement2(new StreamRecord<>(new LaneReady(lane, partition, epoch)));
    }

    private void allReady(long epoch) throws Exception {
        ready("rule", 0, epoch);
        ready("rule", 1, epoch);
        ready("portfolio", 0, epoch);
    }

    private List<String> out() {
        return harness.extractOutputValues().stream().map(String.class::cast).collect(Collectors.toList());
    }

    @AfterEach
    void close() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void holdsUntilEveryPartitionOfEveryLaneDeclaresTheCurrentEpoch() throws Exception {
        start(gate(false, true));
        main("a");
        main("b");
        ready("rule", 0, 10);
        ready("rule", 1, 10);
        assertThat(out()).as("portfolio missing").isEmpty();
        ready("portfolio", 0, 9); // stale epoch
        assertThat(out()).isEmpty();
        ready("portfolio", 0, 10);
        assertThat(out()).containsExactly("a", "b");
        main("c");
        assertThat(out()).as("open: passthrough in order").containsExactly("a", "b", "c");
    }

    @Test
    void reArmsAtTheEpochBoundary() throws Exception {
        start(gate(false, true));
        allReady(10);
        main("a");
        clock.advance(INTERVAL); // epoch 11
        main("b");
        assertThat(out()).containsExactly("a");
        allReady(11);
        assertThat(out()).containsExactly("a", "b");
    }

    @Test
    void overflowReleasesEarlyAndStaysOpenForTheEpoch() throws Exception {
        start(gate(false, true));
        main("a");
        main("b");
        main("c");
        assertThat(out()).isEmpty();
        main("d"); // buffer at MAX → release
        assertThat(out()).containsExactly("a", "b", "c", "d");
        main("e");
        assertThat(out()).as("forced open for the rest of the epoch").containsExactly("a", "b", "c", "d", "e");
        clock.advance(INTERVAL);
        main("f");
        assertThat(out()).as("re-armed in the next epoch").hasSize(5);
    }

    @Test
    void overflowFailsTheTaskWhenAsked() throws Exception {
        start(gate(true, true));
        main("a");
        main("b");
        main("c");
        assertThatThrownBy(() -> main("d")).isInstanceOf(IllegalStateException.class).hasMessageContaining("maxBuffered=3");
    }

    @Test
    void idleBackstopMeasuresHoldDurationNotMarkerSilence() throws Exception {
        start(gate(false, true));
        allReady(10);
        clock.advance(IDLE * 3); // long marker silence while OPEN must not matter
        main("a");
        assertThat(out()).containsExactly("a");

        clock.set(INTERVAL * 11 + 100); // epoch 11: closed again
        main("b");
        clock.advance(IDLE - 1);
        main("c");
        assertThat(out()).as("held < backstop").containsExactly("a");
        clock.advance(1);
        main("d");
        assertThat(out()).as("held == backstop, anchored at first buffered record").containsExactly("a", "b", "c", "d");
    }

    @Test
    void restoredBufferDrainsEvenWhenDisabled() throws Exception {
        start(gate(false, true));
        main("a");
        main("b");
        OperatorSubtaskState snapshot = harness.snapshot(1L, clock.millis());
        harness.close();

        harness = new TwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithNonKeyedOperator<>(gate(false, false), List.of(BullseyeGate.READY_STATE)));
        harness.initializeState(snapshot);
        harness.open();
        main("c");
        assertThat(out()).containsExactly("a", "b", "c");
    }

    @Test
    void restoredBufferIsHeldUntilReadyWhenEnabled() throws Exception {
        start(gate(false, true));
        main("a");
        OperatorSubtaskState snapshot = harness.snapshot(1L, clock.millis());
        harness.close();

        harness = new TwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithNonKeyedOperator<>(gate(false, true), List.of(BullseyeGate.READY_STATE)));
        harness.initializeState(snapshot);
        harness.open();
        main("b");
        assertThat(out()).isEmpty();
        allReady(10);
        assertThat(out()).containsExactly("a", "b");
    }

    @Test
    void disabledIsPassthrough() throws Exception {
        start(gate(false, false));
        main("a");
        assertThat(out()).containsExactly("a");
    }
}
