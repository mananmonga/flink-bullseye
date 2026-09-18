package io.github.mananmonga.bullseye.internal;

import io.github.mananmonga.bullseye.Lane;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The barrier itself: holds main-stream records in operator {@link ListState} until every gated
 * lane has declared the current epoch, then releases in arrival order.
 *
 * <p>Non-keyed on purpose. It must sit <em>upstream</em> of the keyed joins that read side-input
 * state, and its buffer is operator state so that a restore redistributes it round-robin without
 * a key. The buffer is bounded per subtask by {@code maxBuffered}.
 *
 * <p>Release paths: normal (all lanes ready), overflow (buffer full and {@code failOnOverflow} is
 * off) and idle backstop (held longer than {@code idleBackstop}). The latter two force the gate
 * open for the remainder of the current epoch, then it re-arms at the boundary. Both are logged at
 * WARN and counted: they are degradation, not normal operation.
 *
 * <p>The idle backstop measures <em>hold duration</em>, not marker silence. A partition emits at
 * most one marker per epoch, so "time since last marker" is almost always large during normal
 * operation and would idle-release at every rollover; worse, a consumer that has genuinely drifted
 * behind emits no fresh marker either, so it would idle-release in exactly the case the barrier
 * exists to catch.
 *
 * <p>{@link #drain} runs unconditionally on the open path. It covers the idle and overflow releases
 * and — easy to miss — a redeploy with the gate <em>disabled</em> after a last-state restore, where
 * {@code isOpen()} is trivially true and the restored buffer would otherwise be silently dropped
 * while {@code snapshotState} kept rewriting it into every checkpoint.
 *
 * <p>Metric names <em>and semantics</em> are frozen; dashboards depend on them.
 * {@code evalGateReleased} counts every record that leaves the gate, drained or passed straight
 * through, so in steady state it is throughput. {@code evalGateHeld} counts records that entered
 * the buffer.
 */
@Internal
public final class BullseyeGate<T> extends BroadcastProcessFunction<T, LaneReady, T>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BullseyeGate.class);

    /** Broadcast state: {@link Readiness#key(String, int)} → highest epoch declared. Name frozen. */
    public static final MapStateDescriptor<String, Long> READY_STATE =
            new MapStateDescriptor<>("readyState", Types.STRING, Types.LONG);

    private final ArrayList<Lane> lanes;
    private final SyncEpoch epochs;
    // The builder guarantees the clock instance is Serializable; the static type cannot say so.
    @SuppressWarnings("serial")
    private final Clock clock;
    private final TypeInformation<T> type;
    private final int maxBuffered;
    private final boolean failOnOverflow;
    private final long idleBackstopMillis;
    private final boolean enabled;

    private transient ListState<T> bufferState;
    private transient List<T> buffer;
    private transient long heldSince;
    private transient long forcedOpenEpoch;
    private transient boolean lastOpen;
    private transient long lastMissingLogAt;

    private transient Counter held;
    private transient Counter released;
    private transient Counter overflow;
    private transient Counter idleRelease;

    public BullseyeGate(
            List<Lane> lanes,
            SyncEpoch epochs,
            Clock clock,
            TypeInformation<T> type,
            int maxBuffered,
            boolean failOnOverflow,
            long idleBackstopMillis,
            boolean enabled) {
        this.lanes = new ArrayList<>(lanes);
        this.epochs = epochs;
        this.clock = clock;
        this.type = type;
        this.maxBuffered = maxBuffered;
        this.failOnOverflow = failOnOverflow;
        this.idleBackstopMillis = idleBackstopMillis;
        this.enabled = enabled;
    }

    @Override
    public void open(OpenContext openContext) {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        held = g.counter("evalGateHeld");
        released = g.counter("evalGateReleased");
        overflow = g.counter("evalGateOverflow");
        idleRelease = g.counter("evalGateIdleRelease");
        g.gauge("evalGateWaiting", () -> buffer == null ? 0 : buffer.size());
        g.gauge("evalGateOpen", () -> lastOpen ? 1 : 0);
        heldSince = -1L;
        forcedOpenEpoch = Long.MIN_VALUE;
        lastOpen = !enabled;
        lastMissingLogAt = Long.MIN_VALUE;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        bufferState =
                context.getOperatorStateStore().getListState(new ListStateDescriptor<>("barrierBuffer", type));
        buffer = new ArrayList<>();
        if (context.isRestored()) {
            for (T t : bufferState.get()) {
                buffer.add(t);
            }
            if (!buffer.isEmpty()) {
                LOG.info("restored {} buffered records; they drain on the next open evaluation", buffer.size());
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bufferState.update(buffer);
    }

    @Override
    public void processElement(T value, ReadOnlyContext ctx, Collector<T> out) throws Exception {
        long now = clock.millis();
        if (isOpen(ctx.getBroadcastState(READY_STATE), now)) {
            drain(out);
            emit(value, out);
            return;
        }
        if (heldSince < 0) {
            heldSince = now;
        }
        if (now - heldSince >= idleBackstopMillis) {
            long epoch = epochs.epochAt(now);
            List<String> missing = Readiness.missing(lanes, epoch, ctx.getBroadcastState(READY_STATE)::get);
            String msg = "barrier held for {} ms (backstop {} ms) with {} buffered; releasing UNGATED for epoch {}"
                    + " — downstream joins may now miss. Missing: {}";
            if (idleRelease.getCount() == 0) {
                // The first idle release on a fresh job is almost always a cold start that outran the
                // backstop, which is the exact failure this gate exists to prevent. Shout.
                LOG.error(msg, now - heldSince, idleBackstopMillis, buffer.size(), epoch, missing);
            } else {
                LOG.warn(msg, now - heldSince, idleBackstopMillis, buffer.size(), epoch, missing);
            }
            idleRelease.inc();
            forcedOpenEpoch = epoch;
            lastOpen = true;
            drain(out);
            emit(value, out);
            return;
        }
        if (buffer.size() >= maxBuffered) {
            long epoch = epochs.epochAt(now);
            List<String> missing = Readiness.missing(lanes, epoch, ctx.getBroadcastState(READY_STATE)::get);
            if (failOnOverflow) {
                throw new IllegalStateException(
                        "barrier buffer overflow: " + buffer.size() + " records held per subtask (maxBuffered="
                                + maxBuffered + ") while waiting for " + missing);
            }
            LOG.warn(
                    "barrier buffer full ({} per subtask); releasing early for epoch {} — missing: {}",
                    buffer.size(),
                    epoch,
                    missing);
            overflow.inc();
            forcedOpenEpoch = epoch;
            lastOpen = true;
            drain(out);
            emit(value, out);
            return;
        }
        buffer.add(value);
        held.inc();
    }

    @Override
    public void processBroadcastElement(LaneReady marker, Context ctx, Collector<T> out) throws Exception {
        BroadcastState<String, Long> ready = ctx.getBroadcastState(READY_STATE);
        String key = Readiness.key(marker.lane, marker.partition);
        Long previous = ready.get(key);
        if (previous == null || marker.epoch > previous) {
            ready.put(key, marker.epoch);
        }
        if (isOpen(ready, clock.millis())) {
            drain(out);
        }
    }

    private boolean isOpen(ReadOnlyBroadcastState<String, Long> ready, long now) throws Exception {
        if (!enabled) {
            lastOpen = true;
            return true;
        }
        long epoch = epochs.epochAt(now);
        boolean open = forcedOpenEpoch == epoch || Readiness.allReady(lanes, epoch, ready::get);
        if (!open && !lastOpen && now - lastMissingLogAt >= idleBackstopMillis) {
            lastMissingLogAt = now;
            LOG.info("barrier closed for epoch {}; waiting on {}", epoch, Readiness.missing(lanes, epoch, ready::get));
        }
        lastOpen = open;
        return open;
    }

    private void emit(T value, Collector<T> out) {
        out.collect(value);
        released.inc();
    }

    private void drain(Collector<T> out) {
        heldSince = -1L;
        if (buffer.isEmpty()) {
            return;
        }
        int n = buffer.size();
        for (T t : buffer) {
            emit(t, out);
        }
        buffer.clear();
        LOG.info("barrier released {} buffered records", n);
    }

    /** Test seam: current value of {@code evalGateReleased}. */
    long releasedCount() {
        return released.getCount();
    }

    /** Test seam: current value of {@code evalGateHeld}. */
    long heldCount() {
        return held.getCount();
    }
}
