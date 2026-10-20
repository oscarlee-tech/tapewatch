package io.github.oscarleetech.tapewatch.client;

/** Toss Rate Limits Groups we call, with the documented requests per second. */
public enum RateLimitGroup {

    AUTH(5),
    STOCK(5),
    STOCK_ALL(1),
    MARKET_INFO(3),
    MARKET_DATA(15),
    MARKET_DATA_CHART(20),
    RANKING(5),
    MARKET_INDICATOR_CHART(5);

    private final int requestsPerSecond;

    RateLimitGroup(int requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
    }

    /** Gap between two requests of this group: 5 per second means 200 ms. */
    long intervalNanos() {
        return 1_000_000_000L / requestsPerSecond;
    }
}