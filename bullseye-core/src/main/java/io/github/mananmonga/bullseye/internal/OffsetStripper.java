package io.github.mananmonga.bullseye.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.function.SerializableFunction;

/** Reduces a side-input record to its {@link LaneOffset}. Runs pre-keyBy at source parallelism. */
@Internal
public final class OffsetStripper<E> implements MapFunction<E, LaneOffset> {

    private static final long serialVersionUID = 1L;

    private final String lane;
    private final SerializableFunction<E, Integer> partitionOf;
    private final SerializableFunction<E, Long> offsetOf;

    public OffsetStripper(
            String lane,
            SerializableFunction<E, Integer> partitionOf,
            SerializableFunction<E, Long> offsetOf) {
        this.lane = lane;
        this.partitionOf = partitionOf;
        this.offsetOf = offsetOf;
    }

    @Override
    public LaneOffset map(E value) {
        Integer partition = partitionOf.apply(value);
        Long offset = offsetOf.apply(value);
        if (partition == null || offset == null) {
            throw new IllegalStateException(
                    "lane '" + lane + "': partitionOf/offsetOf returned null for " + value);
        }
        return new LaneOffset(lane, partition, offset);
    }
}
