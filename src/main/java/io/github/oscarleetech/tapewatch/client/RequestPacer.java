package io.github.oscarleetech.tapewatch.client;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Spaces out requests so each Rate Limits Group stays at its documented requests per second.
 * Every request reserves the next free slot of its group and waits until then, so a burst
 * (40 stocks added at 09:00) becomes an evenly spaced queue instead of 429 errors.
 */
final class RequestPacer {

    private final Map<RateLimitGroup, Long> nextFreeNanos = new EnumMap<>(RateLimitGroup.class);

    /** Completes when a request of this group may be sent. The slot is reserved on subscribe. */
    Mono<Long> waitFor(RateLimitGroup group) {
        return Mono.defer(() -> Mono.delay(reserve(group)));
    }

    /** Reserves the next free slot and returns how long to wait for it. */
    synchronized Duration reserve(RateLimitGroup group) {
        long now = System.nanoTime();
        long slot = Math.max(now, nextFreeNanos.getOrDefault(group, now));
        nextFreeNanos.put(group, slot + group.intervalNanos());
        return Duration.ofNanos(slot - now);
    }
}