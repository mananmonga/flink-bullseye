package io.github.mananmonga.flink.barrier.internal;

import java.util.Objects;
import org.apache.flink.annotation.Internal;

/**
 * Readiness marker: "lane {@code lane}, partition {@code partition} has been consumed to the end
 * offset probed for epoch {@code epoch}". Exactly one per (lane, partition) per epoch, never per
 * record — see the tracker for why per-record reporting is unsafe, not merely wasteful.
 *
 * <p>Transport is an implementation detail (broadcast stream on Flink 1.20; generalized
 * watermarks are the intended Flink 2.x spelling). Nothing in the public API exposes it.
 *
 * <p>Flink POJO: public no-arg constructor, public fields.
 */
@Internal
public final class LaneReady {

    public String lane;
    public int partition;
    public long epoch;

    public LaneReady() {}

    public LaneReady(String lane, int partition, long epoch) {
        this.lane = lane;
        this.partition = partition;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LaneReady)) {
            return false;
        }
        LaneReady that = (LaneReady) o;
        return partition == that.partition && epoch == that.epoch && Objects.equals(lane, that.lane);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lane, partition, epoch);
    }

    @Override
    public String toString() {
        return "LaneReady{" + lane + "/" + partition + " epoch=" + epoch + "}";
    }
}
