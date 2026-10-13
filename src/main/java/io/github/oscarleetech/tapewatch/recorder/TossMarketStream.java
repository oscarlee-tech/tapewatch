package io.github.oscarleetech.tapewatch.recorder;

import io.github.oscarleetech.tapewatch.client.TossClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * One Toss WebSocket connection. Subscribes trade + orderbook for a list of stocks
 * and publishes every received frame to the FrameBus, stamped the moment it arrived.
 *
 * Reconnects with exponential backoff (1s, 2s, 4s ... up to 30s, plus jitter). The latest
 * subscription list is remembered and sent again automatically after every reconnect.
 */
public class TossMarketStream {

    private static final Logger log = LoggerFactory.getLogger(TossMarketStream.class);
    private static final int MAX_TOPICS = 100;                                // per connection: stocks x channels
    private static final long FIRST_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final long HEALTHY_SESSION_NANOS = 60_000_000_000L;        // lived 60s: reset the backoff

    private final String name;
    private final URI wsUrl;
    private final Duration pingInterval;
    private final TossClient tossClient;
    private final FrameBus frameBus;
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

    // Remembers only the latest declaration and replays it to every new connection
    private final Sinks.Many<String> declarations = Sinks.many().replay().latest();

    private volatile boolean running;
    private volatile Disposable connection;
    private volatile WebSocketSession session;
    private volatile long connectedAtNanos;
    private volatile int failedAttempts;
    private int declarationCount;

    public TossMarketStream(String name, URI wsUrl, Duration pingInterval,
                            TossClient tossClient, FrameBus frameBus) {
        this.name = name;
        this.wsUrl = wsUrl;
        this.pingInterval = pingInterval;
        this.tossClient = tossClient;
        this.frameBus = frameBus;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        connect(0);
    }

    public synchronized void stop() {
        running = false;
        Disposable current = connection;
        if (current != null) {
            current.dispose();   // cancelling the subscription closes the WebSocket
        }
        log.info("{} stopped", name);
    }

    /** Replaces the whole subscription list. Toss declarations are full-replace, never add/remove. */
    public synchronized void updateSubscriptions(List<String> codes) {
        if (codes.size() * 2 > MAX_TOPICS) {
            throw new IllegalArgumentException(
                    name + ": " + codes.size() + " stocks x 2 channels exceeds " + MAX_TOPICS + " topics");
        }
        String quoted = codes.stream().map(code -> "\"" + code + "\"").collect(Collectors.joining(","));
        String declaration = codes.isEmpty() ? "[]"
                : "[{\"id\":\"" + name + "-" + (++declarationCount) + "\"},"
                + "{\"type\":\"trade:kr\",\"codes\":[" + quoted + "]},"
                + "{\"type\":\"orderbook:kr\",\"codes\":[" + quoted + "]}]";

        declarations.tryEmitNext(declaration).orThrow();
        log.info("{} subscriptions updated: {} stocks", name, codes.size());
    }

    /** Test hook: drop the current connection as if the network failed. */
    void dropConnection() {
        WebSocketSession current = session;
        if (current != null) {
            current.close().subscribe();
        }
    }

    private synchronized void connect(long delayMs) {
        connectedAtNanos = 0;   // a failed handshake must not count as a healthy session
        connection = Mono.delay(Duration.ofMillis(delayMs))
                .then(Mono.defer(this::connectOnce))
                .subscribe(
                        unused -> { },
                        error -> reconnect("failed: " + error.getMessage()),
                        () -> reconnect("connection closed"));
    }

    private synchronized void reconnect(String reason) {
        if (!running) {
            return;
        }
        // A connection that stayed up for a while was healthy, so start the backoff from 1s again
        if (connectedAtNanos != 0 && System.nanoTime() - connectedAtNanos >= HEALTHY_SESSION_NANOS) {
            failedAttempts = 0;
        }
        failedAttempts++;
        long base = Math.min(MAX_BACKOFF_MS, FIRST_BACKOFF_MS << Math.min(failedAttempts - 1, 10));
        long delayMs = base + ThreadLocalRandom.current().nextLong(base / 2 + 1);   // jitter spreads out reconnects

        log.warn("{} {}, reconnecting in {} ms (attempt {})", name, reason, delayMs, failedAttempts);
        connect(delayMs);
    }

    private Mono<Void> connectOnce() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(tossClient.requireAccessToken());   // auth happens only at the handshake

        return client.execute(wsUrl, headers, ws -> {
            session = ws;
            connectedAtNanos = System.nanoTime();
            log.info("{} connected", name);

            // Latest subscription list (replayed on every reconnect) + a text PING to keep the connection alive
            Flux<WebSocketMessage> outgoing = Flux.merge(
                            declarations.asFlux(),
                            Flux.interval(pingInterval).map(tick -> "PING")
                    )
                    .map(ws::textMessage);

            Mono<Void> incoming = ws.receive()
                    .doOnNext(message -> {
                        String raw = message.getPayloadAsText();
                        frameBus.publish(name, Instant.now(), raw);
                        if (!raw.startsWith("{\"type\":\"message\"")) {
                            log.info("{} control frame: {}", name, raw);   // ack, pong, error
                        }
                    })
                    .then();

            // PING never completes, so the session ends when the server stops sending or an error happens
            return Mono.firstWithSignal(ws.send(outgoing), incoming);
        });
    }
}