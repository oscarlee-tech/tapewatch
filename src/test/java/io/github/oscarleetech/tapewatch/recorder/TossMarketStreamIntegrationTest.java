package io.github.oscarleetech.tapewatch.recorder;

import io.github.oscarleetech.tapewatch.client.TossClient;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Talks to the real Toss WebSocket. Works on weekends too: it checks connect, ack, pong,
 * and reconnect. Market data frames are only counted, not required.
 * Warning: opens one of the two connections allowed per account, and issues a new token.
 */
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class TossMarketStreamIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TossMarketStreamIntegrationTest.class);

    @Autowired
    private TossClient tossClient;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Value("${recorder.toss-invest.ws-url:wss://openapi-ws.tossinvest.com/ws/v1}")
    private String wsUrl;

    @Test
    void subscribesKeepsAliveAndResubscribesAfterADrop() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real WebSocket test");
        tossClient.issueAccessToken();

        Condition<RawFrame> ack = new Condition<>(
                frame -> frame.raw().contains("\"type\":\"subscriptions\""), "subscription ack");
        Condition<RawFrame> pong = new Condition<>(
                frame -> frame.raw().contains("\"type\":\"pong\""), "pong");

        // Safe to read while the handler thread is still adding: every read sees a snapshot
        List<RawFrame> frames = new CopyOnWriteArrayList<>();
        FrameBus bus = new FrameBus(1 << 12, List.of((event, sequence, endOfBatch) -> frames.add(event.frame())));
        TossMarketStream stream = new TossMarketStream(
                FrameKind.WS_GAINERS, URI.create(wsUrl), Duration.ofSeconds(2), tossClient, bus);   // fast PING

        try {
            stream.updateSubscriptions(List.of("005930"));
            stream.start();
            await().atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> assertThat(frames).haveAtLeast(1, ack).haveAtLeast(1, pong));

            stream.dropConnection();   // like a network drop
            await().atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> assertThat(frames).haveAtLeast(2, ack));   // re-declared after reconnect
        } finally {
            stream.stop();     // always release the connection, even when the test fails
            bus.shutdown();
        }

        log.info("Frames received: total={}, market data={}", frames.size(),
                frames.stream().filter(frame -> frame.raw().contains("\"type\":\"message\"")).count());

        assertThat(frames).filteredOn(ack)
                .extracting(RawFrame::raw)
                .allSatisfy(raw -> assertThat(raw).contains(
                        "\"id\":\"WS_GAINERS-1\"", "trade:kr:005930", "orderbook:kr:005930", "\"rejected\":[]"));
        assertThat(frames).extracting(RawFrame::kind).containsOnly(FrameKind.WS_GAINERS);
    }
}