package io.github.oscarleetech.tapewatch.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RequestPacer")
class RequestPacerTest {

    @Test
    @DisplayName("Spaces out requests of one group and leaves other groups alone")
    void spacesOutOneGroupAndLeavesOtherGroupsAlone() {
        RequestPacer pacer = new RequestPacer();
        Duration gap = Duration.ofNanos(RateLimitGroup.STOCK.intervalNanos());

        Duration first = pacer.reserve(RateLimitGroup.STOCK);
        Duration second = pacer.reserve(RateLimitGroup.STOCK);
        Duration third = pacer.reserve(RateLimitGroup.STOCK);
        Duration otherGroup = pacer.reserve(RateLimitGroup.RANKING);

        assertThat(first).isLessThan(Duration.ofMillis(5));
        assertThat(second).isBetween(gap.minusMillis(10), gap);
        assertThat(third).isBetween(gap.multipliedBy(2).minusMillis(10), gap.multipliedBy(2));
        assertThat(otherGroup).isLessThan(Duration.ofMillis(5));
    }

    @Test
    @DisplayName("Uses 80% of each documented rate, leaving room for timer and network jitter")
    void usesEightyPercentOfTheDocumentedRate() {
        assertThat(Duration.ofNanos(RateLimitGroup.STOCK.intervalNanos())).isEqualTo(Duration.ofMillis(250));
        assertThat(Duration.ofNanos(RateLimitGroup.MARKET_DATA_CHART.intervalNanos())).isEqualTo(Duration.ofNanos(62_500_000));
    }
}