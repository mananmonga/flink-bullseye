package io.github.mananmonga.bullseye.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SyncEpochTest {

    @Test
    void epochIsFloorDivOfInterval() {
        SyncEpoch e = new SyncEpoch(Duration.ofMillis(1000));
        assertThat(e.epochAt(0)).isZero();
        assertThat(e.epochAt(999)).isZero();
        assertThat(e.epochAt(1000)).isEqualTo(1);
        assertThat(e.epochAt(-1)).isEqualTo(-1);
        assertThat(e.startOf(3)).isEqualTo(3000);
        assertThat(e.nextBoundaryAfter(2500)).isEqualTo(3000);
        assertThat(e.nextBoundaryAfter(3000)).isEqualTo(4000);
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new SyncEpoch(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SyncEpoch(Duration.ofMillis(-5))).isInstanceOf(IllegalArgumentException.class);
    }
}
