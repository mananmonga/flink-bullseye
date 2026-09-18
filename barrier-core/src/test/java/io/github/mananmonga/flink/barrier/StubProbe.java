package io.github.mananmonga.flink.barrier;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Probe whose answers are scripted from the test; counts calls. Not thread-safe (single subtask). */
public final class StubProbe implements EndOffsetProbe {

    private static final long serialVersionUID = 1L;

    private final HashMap<Integer, Long> targets = new HashMap<>();
    private boolean failing;
    private final AtomicInteger calls = new AtomicInteger();

    public static StubProbe of(long... endOffsets) {
        StubProbe p = new StubProbe();
        for (int i = 0; i < endOffsets.length; i++) {
            p.targets.put(i, endOffsets[i]);
        }
        return p;
    }

    public StubProbe set(int partition, long endOffset) {
        targets.put(partition, endOffset);
        return this;
    }

    public StubProbe failing(boolean f) {
        this.failing = f;
        return this;
    }

    public int calls() {
        return calls.get();
    }

    @Override
    public Map<Integer, Long> probe() {
        calls.incrementAndGet();
        return failing ? null : new HashMap<>(targets);
    }
}
