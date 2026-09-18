package io.github.mananmonga.bullseye;

import io.github.mananmonga.bullseye.internal.BullseyeGate;
import io.github.mananmonga.bullseye.internal.LaneOffset;
import io.github.mananmonga.bullseye.internal.LaneReadinessTracker;
import io.github.mananmonga.bullseye.internal.LaneReady;
import io.github.mananmonga.bullseye.internal.OffsetStripper;
import io.github.mananmonga.bullseye.internal.SyncEpoch;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.function.SerializableFunction;

/**
 * A periodic re-sync barrier for keyed-state side inputs.
 *
 * <p>Holds a main stream until every gated side-input lane has been consumed to the end offsets
 * probed at the start of the current epoch, then releases; re-arms every {@link #syncInterval}.
 * This turns "complete once at t=0" into <b>bounded staleness</b> and is the only variant that also
 * detects a consumer silently drifting behind.
 *
 * <pre>{@code
 * DataStream<Observation> gated = Bullseye.forType(TypeInformation.of(Observation.class))
 *     .lane(Lane.of("rule", 6, KafkaEndOffsetProbe.of("rules", kafkaProps)),
 *           rules, KafkaEnvelope::getPartition, KafkaEnvelope::getOffset)
 *     .lane(Lane.of("portfolio", 12, KafkaEndOffsetProbe.of("portfolios", kafkaProps)),
 *           portfolios, KafkaEnvelope::getPartition, KafkaEnvelope::getOffset)
 *     .barrierUid("eval-gate")
 *     .syncInterval(Duration.ofHours(1))
 *     .apply(observations);
 *
 * gated.keyBy(...).connect(rules.broadcast(...)).process(...);   // the joins go AFTER the barrier
 * }</pre>
 *
 * <h2>Things to know before production</h2>
 *
 * <ul>
 *   <li><b>Place it upstream of the keyed joins.</b> Offset completeness cannot be computed after
 *       a {@code keyBy} on a business key, and a gate downstream of the join is structurally too
 *       late — the wrong answer has already been produced.
 *   <li><b>{@code barrierUid} is required</b> and, together with the per-lane uids derived from
 *       {@link Lane#id()} (see {@link BullseyeUids}), is frozen: it is your savepoint contract.
 *   <li><b>The buffer is operator {@code ListState} of {@code T}.</b> If {@code T} falls back to
 *       Kryo, learn that from your job's serializer warnings before production, not after.
 *   <li><b>{@link #maxBuffered} is per subtask.</b> Multiply by parallelism for the real heap
 *       ceiling.
 *   <li><b>It is not a hard fence.</b> A marker means "consumed to target", not "state applied":
 *       the side-input state lives downstream of a {@code keyBy}, so a record released exactly at
 *       a boundary races with side-input records still in flight. The race is heavily one-sided but
 *       is not a guarantee. Do not build a stricter promise on top.
 *   <li><b>Releases happen on events.</b> The gate is non-keyed and has no timers, so a buffered
 *       record is released when the next main-stream record or readiness marker arrives at that
 *       subtask. A subtask whose main input goes completely quiet while closed keeps its buffer
 *       until the next event.
 * </ul>
 *
 * @param <T> main-stream element type
 */
public final class Bullseye<T> {

    private static final int DEFAULT_MAX_BUFFERED = 500_000;
    private static final Duration DEFAULT_SYNC_INTERVAL = Duration.ofHours(1);
    private static final Duration DEFAULT_IDLE_BACKSTOP = Duration.ofSeconds(30);

    private final TypeInformation<T> type;
    private final List<Registration<?>> lanes = new ArrayList<>();
    private String barrierUid;
    private Duration syncInterval = DEFAULT_SYNC_INTERVAL;
    private int maxBuffered = DEFAULT_MAX_BUFFERED;
    private boolean failOnOverflow = false;
    private Duration idleBackstop = DEFAULT_IDLE_BACKSTOP;
    private Clock clock = Clock.systemUTC();
    private boolean enabled = true;

    private Bullseye(TypeInformation<T> type) {
        this.type = type;
    }

    /**
     * Starts a barrier for a main stream of {@code type}. The type is explicit because it names the
     * operator-state buffer's serializer; pass the same {@code TypeInformation} the stream carries.
     */
    public static <T> Bullseye<T> forType(TypeInformation<T> type) {
        return new Bullseye<>(Objects.requireNonNull(type, "type"));
    }

    /**
     * Gates the main stream on {@code lane}, fed by {@code sideInput}.
     *
     * @param lane the lane declaration (id, expected partitions, probe)
     * @param sideInput the side-input stream, <em>before</em> any business-key {@code keyBy}
     * @param partitionOf extracts the source partition of a side-input record
     * @param offsetOf extracts the source offset of a side-input record (inclusive)
     * @param <E> side-input element type
     */
    public <E> Bullseye<T> lane(
            Lane lane,
            DataStream<E> sideInput,
            SerializableFunction<E, Integer> partitionOf,
            SerializableFunction<E, Long> offsetOf) {
        Objects.requireNonNull(lane, "lane");
        Objects.requireNonNull(sideInput, "sideInput");
        Objects.requireNonNull(partitionOf, "partitionOf");
        Objects.requireNonNull(offsetOf, "offsetOf");
        for (Registration<?> r : lanes) {
            if (r.lane.id().equals(lane.id())) {
                throw new IllegalArgumentException("lane id registered twice: '" + lane.id() + "'");
            }
        }
        lanes.add(new Registration<>(lane, sideInput, partitionOf, offsetOf));
        return this;
    }

    /** Required. Used verbatim as the barrier operator's uid; see {@link BullseyeUids}. */
    public Bullseye<T> barrierUid(String uid) {
        Objects.requireNonNull(uid, "barrierUid");
        if (uid.isBlank()) {
            throw new IllegalArgumentException("barrierUid must not be blank");
        }
        this.barrierUid = uid;
        return this;
    }

    /** Re-sync cadence; bounds the staleness a consumer can silently drift into. Default 1h. */
    public Bullseye<T> syncInterval(Duration d) {
        Objects.requireNonNull(d, "syncInterval");
        if (d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("syncInterval must be positive: " + d);
        }
        this.syncInterval = d;
        return this;
    }

    /** Maximum records held <b>per subtask</b> before overflow handling. Default 500,000. */
    public Bullseye<T> maxBuffered(int perSubtask) {
        if (perSubtask <= 0) {
            throw new IllegalArgumentException("maxBuffered must be positive: " + perSubtask);
        }
        this.maxBuffered = perSubtask;
        return this;
    }

    /** On overflow: {@code true} fails the task, {@code false} (default) releases early with a WARN. */
    public Bullseye<T> failOnOverflow(boolean b) {
        this.failOnOverflow = b;
        return this;
    }

    /**
     * How long a subtask may be <em>held</em> (measured from its first buffered record in the
     * current closed spell) before it releases with a WARN. Default 30s. This is the degradation
     * path that keeps a wedged side input from stalling the job forever.
     */
    public Bullseye<T> idleBackstop(Duration d) {
        Objects.requireNonNull(d, "idleBackstop");
        if (d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("idleBackstop must be positive: " + d);
        }
        this.idleBackstop = d;
        return this;
    }

    /**
     * Test seam. Must be {@link Serializable} (the JDK's system, fixed and offset clocks are) and
     * must agree with the processing-time service the tracker's timers run on. Default
     * {@link Clock#systemUTC()}.
     */
    public Bullseye<T> clock(Clock clock) {
        Objects.requireNonNull(clock, "clock");
        if (!(clock instanceof Serializable)) {
            throw new IllegalArgumentException("clock must be Serializable: " + clock.getClass().getName());
        }
        this.clock = clock;
        return this;
    }

    /**
     * {@code false} makes the barrier a passthrough. The operator graph (and every uid) is kept so
     * that savepoints remain restorable and a restored buffer still drains; only the hold is off.
     * Default {@code true}.
     */
    public Bullseye<T> enabled(boolean b) {
        this.enabled = b;
        return this;
    }

    /**
     * Wires the barrier into {@code main} and returns the gated stream.
     *
     * <p>Per lane: strip {@code (partition, offset)} → {@code keyBy(partition)} → readiness tracker.
     * Then union the markers, broadcast them, connect to {@code main} and gate. The marker stream
     * never leaves this method; that is what keeps the transport swappable.
     */
    public DataStream<T> apply(DataStream<T> main) {
        Objects.requireNonNull(main, "main");
        if (barrierUid == null) {
            throw new IllegalStateException("barrierUid is required: it is the savepoint contract for the barrier operator");
        }
        if (lanes.isEmpty()) {
            throw new IllegalStateException("at least one lane is required");
        }
        SyncEpoch epochs = new SyncEpoch(syncInterval);

        DataStream<LaneReady> markers = null;
        List<Lane> laneSpecs = new ArrayList<>(lanes.size());
        for (Registration<?> r : lanes) {
            DataStream<LaneReady> ready = r.wire(epochs, clock, enabled);
            markers = markers == null ? ready : markers.union(ready);
            laneSpecs.add(r.lane);
        }

        BroadcastStream<LaneReady> broadcast = markers.broadcast(BullseyeGate.READY_STATE);
        BullseyeGate<T> gate =
                new BullseyeGate<>(
                        laneSpecs,
                        epochs,
                        clock,
                        type,
                        maxBuffered,
                        failOnOverflow,
                        idleBackstop.toMillis(),
                        enabled);
        SingleOutputStreamOperator<T> gated =
                main.connect(broadcast).process(gate, type).uid(barrierUid).name("bullseye");
        if (main.getParallelism() > 0) {
            // Keep the gate forward-chained to main so arrival order and per-subtask buffer sizing
            // are what the caller expects.
            gated.setParallelism(main.getParallelism());
        }
        return gated;
    }

    private static final class Registration<E> {
        final Lane lane;
        final DataStream<E> sideInput;
        final SerializableFunction<E, Integer> partitionOf;
        final SerializableFunction<E, Long> offsetOf;

        Registration(
                Lane lane,
                DataStream<E> sideInput,
                SerializableFunction<E, Integer> partitionOf,
                SerializableFunction<E, Long> offsetOf) {
            this.lane = lane;
            this.sideInput = sideInput;
            this.partitionOf = partitionOf;
            this.offsetOf = offsetOf;
        }

        DataStream<LaneReady> wire(SyncEpoch epochs, Clock clock, boolean enabled) {
            DataStream<LaneOffset> offsets =
                    sideInput
                            .map(new OffsetStripper<>(lane.id(), partitionOf, offsetOf))
                            .returns(TypeInformation.of(LaneOffset.class))
                            .uid(BullseyeUids.offsets(lane.id()))
                            .name(lane.id() + " offsets");
            return offsets.keyBy(o -> o.partition)
                    .process(new LaneReadinessTracker(lane, epochs, clock, enabled))
                    .returns(TypeInformation.of(LaneReady.class))
                    .uid(BullseyeUids.readiness(lane.id()))
                    .name(lane.id() + " readiness");
        }
    }
}
