package io.github.mananmonga.flink.barrier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class LaneTest {

    @Test
    void acceptsSlugs() {
        assertThat(Lane.of("rule", 3, StubProbe.of()).id()).isEqualTo("rule");
        assertThat(Lane.of("cve-intrinsics_v2", 3, StubProbe.of()).id()).isEqualTo("cve-intrinsics_v2");
    }

    @Test
    void rejectsIdsThatWouldMakeBadUids() {
        for (String bad : new String[] {"", "Rule", "rule offsets", "rule--x", "-rule", "rule.", "rüle"}) {
            assertThatThrownBy(() -> Lane.of(bad, 3, StubProbe.of()))
                    .as(bad)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsZeroPartitionsAndMissingProbe() {
        assertThatThrownBy(() -> Lane.of("rule", 0, StubProbe.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Lane.of("rule", 1, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void uidDerivationIsFrozen() {
        assertThat(BarrierUids.offsets("rule")).isEqualTo("rule-offsets");
        assertThat(BarrierUids.readiness("rule")).isEqualTo("rule-readiness");
    }
}
