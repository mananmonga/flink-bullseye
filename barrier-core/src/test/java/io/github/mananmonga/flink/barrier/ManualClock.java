package io.github.mananmonga.flink.barrier;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** Settable clock for tests. Serializable so it passes the builder's check. */
public final class ManualClock extends Clock implements Serializable {

    private static final long serialVersionUID = 1L;

    private long nowMillis;

    public ManualClock(long nowMillis) {
        this.nowMillis = nowMillis;
    }

    public void set(long millis) {
        this.nowMillis = millis;
    }

    public void advance(long millis) {
        this.nowMillis += millis;
    }

    @Override
    public long millis() {
        return nowMillis;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(nowMillis);
    }
}
