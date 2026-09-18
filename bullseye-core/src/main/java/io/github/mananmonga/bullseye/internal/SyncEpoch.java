package io.github.mananmonga.bullseye.internal;

import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.annotation.Internal;

/**
 * Derives the current sync epoch from wall-clock time: {@code floorDiv(now, interval)}.
 *
 * <p>Epochs are <em>derived, not coordinated</em>. There is no leader, no handshake and no shared
 * counter, so subtasks whose clocks differ by milliseconds still agree on the epoch except in the
 * instant around a boundary, and the tracker's boundary timer re-declares immediately after it.
 */
@Internal
public final class SyncEpoch implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long intervalMillis;

    public SyncEpoch(Duration interval) {
        long millis = interval.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException("sync interval must be positive, got " + interval);
        }
        this.intervalMillis = millis;
    }

    public long intervalMillis() {
        return intervalMillis;
    }

    /** The epoch containing {@code nowMillis}. */
    public long epochAt(long nowMillis) {
        return Math.floorDiv(nowMillis, intervalMillis);
    }

    /** First millisecond of {@code epoch}. */
    public long startOf(long epoch) {
        return Math.multiplyExact(epoch, intervalMillis);
    }

    /** First millisecond of the epoch after the one containing {@code nowMillis}. */
    public long nextBoundaryAfter(long nowMillis) {
        return startOf(epochAt(nowMillis) + 1);
    }
}
