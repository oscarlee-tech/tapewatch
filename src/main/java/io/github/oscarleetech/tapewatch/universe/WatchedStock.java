package io.github.oscarleetech.tapewatch.universe;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A stock we decided to watch, with the ranking numbers at the moment it was added.
 * tradingVolume and tradingAmount are today's totals so far, which become the starting
 * point for "today's volume" features when we subscribe in the middle of the day.
 */
public record WatchedStock(
        String symbol,
        WatchSide side,
        Instant addedAt,
        BigDecimal changeRate,
        BigDecimal tradingVolume,
        BigDecimal tradingAmount) {
}