package io.github.oscarleetech.tapewatch.client;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * One-off probe: connects to the Toss WebSocket, subscribes to trades and order books
 * for two stocks, and writes every raw frame to build/ws-probe/ for inspection.
 * Run it while the market is open. It takes about 2.5 minutes.
 * Warning: issuing a token here invalidates the token of any running tapewatch app.
 */
@Disabled("One-off probe: run manually while the market is open")
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class TossWebSocketProbeTest {

    private static final Logger log = LoggerFactory.getLogger(TossWebSocketProbeTest.class);

    private static final URI WS_URL = URI.create("wss://openapi-ws.tossinvest.com/ws/v1");
    private static final Duration PROBE_DURATION = Duration.ofSeconds(150);
    private static final Duration PING_INTERVAL = Duration.ofSeconds(60);

    // 005930 = Samsung (always busy), 036930 = today's most-traded gainer. 2 stocks x 2 channels = 4 topics.
    private static final String SUBSCRIBE_MESSAGE = """
            [
              {"id": "probe-1"},
              {"type": "trade:kr", "codes": ["005930", "036930"]},
              {"type": "orderbook:kr", "codes": ["005930", "036930"]}
            ]
            """;

    @Autowired
    private TossClient tossClient;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Test
    void recordRawFrames() throws IOException {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping WebSocket probe");
        tossClient.issueAccessToken();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(tossClient.requireAccessToken());

        Path out = Path.of("build", "ws-probe", "frames-" + Instant.now().getEpochSecond() + ".tsv");
        Files.createDirectories(out.getParent());

        AtomicLong seq = new AtomicLong();
        AtomicLong trades = new AtomicLong();
        AtomicLong orderbooks = new AtomicLong();
        List<String> controlFrames = new CopyOnWriteArrayList<>();   // ack, pong, error

        try (BufferedWriter writer = Files.newBufferedWriter(out)) {
            WebSocketHandler handler = session -> {
                // Declare once, then PING every 60s (the server drops us after 180s without client frames)
                Flux<WebSocketMessage> outgoing = Flux.concat(
                        Mono.just(session.textMessage(SUBSCRIBE_MESSAGE)),
                        Flux.interval(PING_INTERVAL).map(i -> session.textMessage("PING")));

                Mono<Void> incoming = session.receive()
                        .map(WebSocketMessage::getPayloadAsText)
                        .doOnNext(raw -> {
                            long n = seq.incrementAndGet();
                            try {
                                // Arrival order + local time, because server timestamps are only per second
                                writer.write(n + "\t" + Instant.now() + "\t" + raw.replace("\n", " "));
                                writer.newLine();
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }

                            String compact = raw.replace(" ", "");
                            if (compact.contains("\"topic\":\"trade:")) {
                                trades.incrementAndGet();
                            } else if (compact.contains("\"topic\":\"orderbook:")) {
                                orderbooks.incrementAndGet();
                            } else {
                                controlFrames.add(raw);
                            }

                            if (n <= 20) {
                                log.info("#{} {}", n, raw);
                            }
                        })
                        .take(PROBE_DURATION)
                        .then();

                // The PING stream never completes, so finish when receiving finishes, then close.
                return Mono.firstWithSignal(session.send(outgoing), incoming).then(session.close());
            };

            new ReactorNettyWebSocketClient()
                    .execute(WS_URL, headers, handler)
                    .block(PROBE_DURATION.plusSeconds(30));
        }

        log.info("Probe finished: frames={}, trades={}, orderbooks={}, file={}",
                seq.get(), trades.get(), orderbooks.get(), out.toAbsolutePath());
        controlFrames.forEach(frame -> log.info("Control frame: {}", frame));

        assertThat(controlFrames).as("subscription ack")
                .anyMatch(frame -> frame.replace(" ", "").contains("\"type\":\"subscriptions\""));
        assertThat(trades.get() + orderbooks.get()).as("market data frames").isPositive();
    }
}