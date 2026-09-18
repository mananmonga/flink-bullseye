package io.github.mananmonga.bullseye.internal;

import java.util.Objects;
import org.apache.flink.annotation.Internal;

/**
 * One side-input record reduced to what the tracker needs: which lane, which partition, which
 * offset. Emitted per record by the offset stripper, <em>before</em> any business-key {@code keyBy}
 * (that is the whole point: after a keyBy on a business key no subtask can observe a partition's
 * maximum offset).
 *
 * <p>Flink POJO: public no-arg constructor, public fields.
 */
@Internal
public final class LaneOffset {

    public String lane;
    public int partition;
    /** The record's own offset (inclusive). "Consumed to" is {@code offset + 1}. */
    public long offset;

    public LaneOffset() {}

    public LaneOffset(String lane, int partition, long offset) {
        this.lane = lane;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LaneOffset)) {
            return false;
        }
        LaneOffset that = (LaneOffset) o;
        return partition == that.partition && offset == that.offset && Objects.equals(lane, that.lane);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lane, partition, offset);
    }

    @Override
    public String toString() {
        return "LaneOffset{" + lane + "/" + partition + "@" + offset + "}";
    }
}
