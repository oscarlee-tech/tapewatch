package io.github.oscarleetech.tapewatch.client;

/**
 * Toss Rate Limits Groups we call, with the documented requests per second.
 * We send at 80% of that rate: at exactly 100%, timer and network jitter put 21 requests
 * into some 1-second windows of a 20-per-second group, and Toss answered 429.
 */
public enum RateLimitGroup {

    AUTH(5),
    STOCK(5),
    STOCK_ALL(1),
    MARKET_INFO(3),
    MARKET_DATA(15),
    MARKET_DATA_CHART(20),
    RANKING(5),
    MARKET_INDICATOR_CHART(5);

    private static final long USED_PERCENT = 80;

    private final int requestsPerSecond;

    RateLimitGroup(int requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
    }

    /** Gap between two requests of this group: 5 per second at 80% is 4 per second, 250 ms apart. */
    long intervalNanos() {
        return 1_000_000_000L * 100 / (requestsPerSecond * USED_PERCENT);
    }
}