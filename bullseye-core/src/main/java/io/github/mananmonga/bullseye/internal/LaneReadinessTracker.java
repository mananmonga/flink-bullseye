package io.github.mananmonga.bullseye.internal;

import io.github.mananmonga.bullseye.EndOffsetProbe;
import io.github.mananmonga.bullseye.Lane;
import java.time.Clock;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-partition readiness for one lane. Keyed by partition, so a partition's maximum consumed
 * offset is observable (a business-key {@code keyBy} would destroy that) and so timers are
 * available (Flink only offers them in a keyed context), which is what lets a <em>quiet</em>
 * partition re-declare at each epoch boundary. The quiet case is the common one and resolves
 * instantly.
 *
 * <p>Emits exactly one {@link LaneReady} per partition per epoch. Never per record: reporting
 * every record's offset forces a choice between broadcasting one marker per side-input record
 * (amplified by parallelism) and throttling — and throttling is unsafe, because a stream that goes
 * quiet after a throttled emit leaves the barrier believing the lane is further behind than it is.
 *
 * <p>Probe results are cached per epoch across keys in a subtask. Without that cache, a partition
 * that is behind (the bootstrap case) would probe on <em>every</em> record — each probe building a
 * client, making blocking round-trips and closing it, on the task thread.
 *
 * <p>Partitions the probe reports as empty (end offset 0) never produce a record and therefore
 * never own a key here. Whichever key first computes the epoch's targets declares them ready on
 * their behalf, once per subtask per epoch. A lane whose partitions are <em>all</em> empty has no
 * key at all and is left to the barrier's idle backstop; that is a deliberate "is this side input
 * really wired?" signal rather than an oversight.
 */
@Internal
public final class LaneReadinessTracker extends KeyedProcessFunction<Integer, LaneOffset, LaneReady> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LaneReadinessTracker.class);

    /** How long to wait before re-probing after a {@code null} probe result. */
    static final long PROBE_RETRY_MILLIS = 5_000L;

    private final Lane lane;
    private final SyncEpoch epochs;
    // The builder guarantees the clock instance is Serializable; the static type cannot say so.
    @SuppressWarnings("serial")
    private final Clock clock;
    private final boolean enabled;

    private transient ValueState<Long> highestOffset;
    private transient ValueState<Long> declaredEpoch;

    // Per-subtask probe cache (transient by design: rebuilt on restore, re-probed next epoch).
    private transient long cachedEpoch;
    private transient Map<Integer, Long> targets;
    private transient long nextProbeAllowedAt;
    private transient long emptyDeclaredEpoch;

    private transient Counter probes;
    private transient Counter probeFailures;
    private transient Counter markers;

    public LaneReadinessTracker(Lane lane, SyncEpoch epochs, Clock clock, boolean enabled) {
        this.lane = lane;
        this.epochs = epochs;
        this.clock = clock;
        this.enabled = enabled;
    }

    @Override
    public void open(OpenContext openContext) {
        highestOffset =
                getRuntimeContext().getState(new ValueStateDescriptor<>("highestOffset", Types.LONG));
        declaredEpoch =
                getRuntimeContext().getState(new ValueStateDescriptor<>("declaredEpoch", Types.LONG));
        cachedEpoch = Long.MIN_VALUE;
        targets = null;
        nextProbeAllowedAt = Long.MIN_VALUE;
        emptyDeclaredEpoch = Long.MIN_VALUE;
        var group = getRuntimeContext().getMetricGroup().addGroup("bullseye").addGroup("lane", lane.id());
        probes = group.counter("probes");
        probeFailures = group.counter("probeFailures");
        markers = group.counter("markers");
    }

    @Override
    public void processElement(LaneOffset value, Context ctx, Collector<LaneReady> out) throws Exception {
        Long highest = highestOffset.value();
        if (highest == null || value.offset > highest) {
            highestOffset.update(value.offset);
        }
        if (enabled) {
            evaluate(ctx.getCurrentKey(), ctx.timerService(), out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LaneReady> out) throws Exception {
        if (enabled) {
            evaluate(ctx.getCurrentKey(), ctx.timerService(), out);
        }
    }

    private void evaluate(int partition, TimerService timers, Collector<LaneReady> out) throws Exception {
        long now = clock.millis();
        long epoch = epochs.epochAt(now);
        // Always re-arm at the next boundary so a quiet partition re-declares without new records.
        // Registering the same timestamp twice is idempotent.
        timers.registerProcessingTimeTimer(epochs.startOf(epoch + 1));

        Long declared = declaredEpoch.value();
        if (declared != null && declared >= epoch) {
            return;
        }

        Map<Integer, Long> epochTargets = targetsFor(epoch, now);
        if (epochTargets == null) {
            timers.registerProcessingTimeTimer(now + PROBE_RETRY_MILLIS);
            return;
        }
        declareEmptyPartitions(epochTargets, epoch, partition, out);

        Long target = epochTargets.get(partition);
        if (target == null) {
            LOG.warn(
                    "lane '{}': partition {} produced records but the probe did not report it; holding",
                    lane.id(),
                    partition);
            timers.registerProcessingTimeTimer(now + PROBE_RETRY_MILLIS);
            return;
        }
        Long highest = highestOffset.value();
        long consumedTo = highest == null ? 0L : highest + 1;
        if (consumedTo >= target) {
            out.collect(new LaneReady(lane.id(), partition, epoch));
            declaredEpoch.update(epoch);
            markers.inc();
        }
        // else: behind. The next record for this partition re-evaluates against the cached targets.
    }

    /**
     * Targets for {@code epoch}, probing at most once per epoch per subtask. On a {@code null}
     * probe keeps the previous targets (if any) and backs off before probing again.
     */
    private Map<Integer, Long> targetsFor(long epoch, long now) {
        if (cachedEpoch == epoch) {
            return targets;
        }
        if (now < nextProbeAllowedAt) {
            return targets; // still backing off from a failed probe; previous targets or null
        }
        EndOffsetProbe probe = lane.probe();
        Map<Integer, Long> result;
        try {
            probes.inc();
            result = probe.probe();
        } catch (RuntimeException e) {
            // The contract says return null, not throw; be forgiving rather than fail the task.
            LOG.warn("lane '{}': probe threw; keeping previous targets", lane.id(), e);
            result = null;
        }
        if (result == null) {
            probeFailures.inc();
            nextProbeAllowedAt = now + PROBE_RETRY_MILLIS;
            if (targets != null) {
                LOG.warn("lane '{}': probe returned null for epoch {}; reusing previous targets", lane.id(), epoch);
            }
            return targets;
        }
        targets = result;
        cachedEpoch = epoch;
        nextProbeAllowedAt = Long.MIN_VALUE;
        LOG.info("lane '{}': epoch {} targets {}", lane.id(), epoch, result);
        return targets;
    }

    private void declareEmptyPartitions(
            Map<Integer, Long> epochTargets, long epoch, int currentPartition, Collector<LaneReady> out) {
        if (emptyDeclaredEpoch >= epoch) {
            return;
        }
        emptyDeclaredEpoch = epoch;
        for (Map.Entry<Integer, Long> e : epochTargets.entrySet()) {
            if (e.getValue() <= 0 && e.getKey() != currentPartition) {
                out.collect(new LaneReady(lane.id(), e.getKey(), epoch));
                markers.inc();
            }
        }
    }
}
