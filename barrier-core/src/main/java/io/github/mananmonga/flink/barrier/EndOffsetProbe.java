package io.github.mananmonga.flink.barrier;

import java.io.Serializable;
import java.util.Map;

/**
 * Answers "how far does a side-input lane currently extend?" for every partition of that lane.
 *
 * <p>This is the only seam a non-Kafka adopter has to implement. It is deliberately free of Flink
 * and Kafka types so that a Pulsar, Fluss or JDBC-changelog user can plug in without pulling either
 * client onto their classpath. The {@code barrier-kafka} module ships the Kafka implementation.
 *
 * <p>The probe is invoked on the task thread of the readiness tracker, at most once per subtask
 * per sync epoch (the result is cached across keys, see the tracker). It must therefore be
 * <em>bounded</em> in latency; a probe that can hang for minutes will stall the lane it serves.
 *
 * <p>Contract for the result:
 *
 * <ul>
 *   <li>Key: partition number. Value: the <b>exclusive</b> end offset of that partition, i.e. the
 *       offset the next record written to it would receive. An empty partition reports {@code 0}.
 *   <li>Every partition of the lane must be present. A partition the probe omits is treated as
 *       <em>not ready</em> — absence never counts as ready.
 *   <li>{@code null} means "cannot tell right now". The tracker keeps the previous epoch's targets
 *       (or, if there are none yet, stays not-ready) and retries later. <b>Never throw</b> from a
 *       transient failure: an exception fails the task, and a guessed value either holds the main
 *       stream against a target that may not exist (guess high) or opens the barrier on a claim it
 *       cannot support (guess low).
 * </ul>
 *
 * <p>Implementations must be {@link Serializable} because they travel inside Flink operators.
 */
@FunctionalInterface
public interface EndOffsetProbe extends Serializable {

    /**
     * Current exclusive end offset per partition, or {@code null} if it cannot be determined now.
     *
     * @return partition → exclusive end offset, or {@code null} to keep the previous targets
     */
    Map<Integer, Long> probe();
}
