package io.github.oscarleetech.tapewatch.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RequestPacerTest {

    @Test
    @DisplayName("Spaces out requests of one group and leaves other groups alone")
    void spacesOutOneGroupAndLeavesOtherGroupsAlone() {
        RequestPacer pacer = new RequestPacer();

        Duration first = pacer.reserve(RateLimitGroup.STOCK);       // 5 per second: 200 ms apart
        Duration second = pacer.reserve(RateLimitGroup.STOCK);
        Duration third = pacer.reserve(RateLimitGroup.STOCK);
        Duration otherGroup = pacer.reserve(RateLimitGroup.RANKING);

        assertThat(first).isLessThan(Duration.ofMillis(5));
        assertThat(second).isBetween(Duration.ofMillis(190), Duration.ofMillis(200));
        assertThat(third).isBetween(Duration.ofMillis(390), Duration.ofMillis(400));
        assertThat(otherGroup).isLessThan(Duration.ofMillis(5));
    }
}