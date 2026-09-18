package io.github.mananmonga.bullseye;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One gated side input: a stable id, how many partitions it has, and how to probe its end offsets.
 *
 * <p>All three live in one value object on purpose. Kept separately (an expected-partitions map
 * here, a probe there, the stream wiring somewhere else) it is possible to register a lane with no
 * probe, or with the wrong partition count, and the barrier would then hold or release on a claim
 * it cannot support. Binding them together makes "a lane cannot look ready merely because none of
 * its partitions have reported" structurally true rather than a javadoc promise.
 *
 * <p>{@link #id()} is baked into operator uids (see {@link BullseyeUids}). Renaming a lane renames
 * its operators and makes existing savepoints unrestorable for those operators. Treat it as frozen
 * once a job has taken a savepoint with it.
 */
public final class Lane implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Lower-case slug: letters, digits, single {@code -} or {@code _} separators. */
    private static final Pattern SLUG = Pattern.compile("[a-z0-9]+(?:[-_][a-z0-9]+)*");

    private final String id;
    private final int expectedPartitions;
    private final EndOffsetProbe probe;

    private Lane(String id, int expectedPartitions, EndOffsetProbe probe) {
        this.id = id;
        this.expectedPartitions = expectedPartitions;
        this.probe = probe;
    }

    /**
     * Declares a lane.
     *
     * @param id stable slug, {@code [a-z0-9]} with single {@code -}/{@code _} separators; it is
     *     used verbatim in operator uids and must never change for a deployed job
     * @param expectedPartitions the number of partitions the lane has, probed at submission time.
     *     Readiness requires every one of them to report; a partition that never reports keeps the
     *     barrier closed until the idle backstop releases it
     * @param probe how to read the lane's current end offsets; see {@link EndOffsetProbe}
     * @return the lane
     */
    public static Lane of(String id, int expectedPartitions, EndOffsetProbe probe) {
        Objects.requireNonNull(id, "lane id");
        Objects.requireNonNull(probe, "probe");
        if (!SLUG.matcher(id).matches()) {
            throw new IllegalArgumentException(
                    "lane id must match " + SLUG.pattern() + " (it becomes an operator uid): '" + id + "'");
        }
        if (expectedPartitions <= 0) {
            throw new IllegalArgumentException(
                    "lane '" + id + "' must have at least one partition, got " + expectedPartitions);
        }
        return new Lane(id, expectedPartitions, probe);
    }

    /** The stable slug this lane was declared with. */
    public String id() {
        return id;
    }

    /** Number of partitions that must each report readiness before the lane counts as ready. */
    public int expectedPartitions() {
        return expectedPartitions;
    }

    /** The probe used to obtain per-partition end offsets at the start of each sync epoch. */
    public EndOffsetProbe probe() {
        return probe;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Lane && ((Lane) o).id.equals(id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Lane{" + id + ", partitions=" + expectedPartitions + "}";
    }
}
