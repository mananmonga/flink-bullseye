package io.github.mananmonga.bullseye.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mananmonga.bullseye.Lane;
import io.github.mananmonga.bullseye.StubProbe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReadinessTest {

    private final List<Lane> lanes =
            List.of(Lane.of("rule", 2, StubProbe.of()), Lane.of("portfolio", 1, StubProbe.of()));

    @Test
    void absenceIsNotReady() throws Exception {
        Map<String, Long> declared = new HashMap<>();
        assertThat(Readiness.allReady(lanes, 5, declared::get)).isFalse();
        assertThat(Readiness.missing(lanes, 5, declared::get))
                .containsExactly("rule:0(never)", "rule:1(never)", "portfolio:0(never)");
    }

    @Test
    void everyPartitionOfEveryLaneMustDeclareTheEpoch() throws Exception {
        Map<String, Long> declared = new HashMap<>();
        declared.put("rule:0", 5L);
        declared.put("rule:1", 5L);
        assertThat(Readiness.allReady(lanes, 5, declared::get)).isFalse();
        declared.put("portfolio:0", 4L); // stale
        assertThat(Readiness.allReady(lanes, 5, declared::get)).isFalse();
        assertThat(Readiness.missing(lanes, 5, declared::get)).containsExactly("portfolio:0(epoch 4)");
        declared.put("portfolio:0", 5L);
        assertThat(Readiness.allReady(lanes, 5, declared::get)).isTrue();
    }

    @Test
    void aLaterEpochSatisfiesAnEarlierOne() throws Exception {
        Map<String, Long> declared = Map.of("rule:0", 9L, "rule:1", 9L, "portfolio:0", 9L);
        assertThat(Readiness.allReady(lanes, 5, declared::get)).isTrue();
        assertThat(Readiness.allReady(lanes, 10, declared::get)).isFalse();
    }
}
