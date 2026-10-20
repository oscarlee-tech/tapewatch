package io.github.oscarleetech.tapewatch.client;

import com.lmax.disruptor.EventHandler;
import io.github.oscarleetech.tapewatch.dto.RankingsResponse;
import io.github.oscarleetech.tapewatch.recorder.FrameEvent;
import io.github.oscarleetech.tapewatch.recorder.FrameKind;
import io.github.oscarleetech.tapewatch.recorder.RawFrame;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Calls the real Toss API and checks that every GET lands on the FrameBus in an envelope,
 * holding exactly the body the caller received. Issues a new token.
 */
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class TossClientRecordingIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final List<RawFrame> frames = new CopyOnWriteArrayList<>();

    /** An extra FrameBus handler, next to RawFrameWriter, that keeps every frame for the test. */
    @TestConfiguration
    static class CaptureFrames {
        @Bean
        EventHandler<FrameEvent> frameCapture() {
            return (event, sequence, endOfBatch) -> frames.add(event.frame());
        }
    }

    @Autowired
    private TossClient tossClient;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Test
    @DisplayName("Records every GET in an envelope holding exactly the body the caller got")
    void recordsEveryGetInAnEnvelopeHoldingTheBodyTheCallerGot() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real API test");
        tossClient.issueAccessToken();

        RankingsResponse gainers = tossClient.fetchTopGainersKr(5).block(TIMEOUT);
        String candles = tossClient.fetchMinuteCandles("005930", 3, null).block(TIMEOUT);
        String warnings = tossClient.fetchWarnings("005930").block(TIMEOUT);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(frames)
                .extracting(RawFrame::kind)
                .contains(FrameKind.REST_RANKINGS, FrameKind.REST_CANDLES_1M, FrameKind.REST_WARNINGS));

        RawFrame rankingsFrame = frames.stream().filter(f -> f.kind() == FrameKind.REST_RANKINGS).findFirst().orElseThrow();
        RawFrame candlesFrame = frames.stream().filter(f -> f.kind() == FrameKind.REST_CANDLES_1M).findFirst().orElseThrow();
        RawFrame warningsFrame = frames.stream().filter(f -> f.kind() == FrameKind.REST_WARNINGS).findFirst().orElseThrow();

        // The envelope says what was asked and that it succeeded
        assertThat(rankingsFrame.raw()).startsWith("{\"path\":\"/api/v1/rankings\"").contains("\"status\":200");
        assertThat(candlesFrame.raw()).contains("\"symbol\":\"005930\"", "\"interval\":\"1m\"", "\"status\":200");
        assertThat(warningsFrame.raw()).contains("\"path\":\"/api/v1/stocks/005930/warnings\"", "\"status\":200");

        // The recorded body is exactly what the caller got
        assertThat(candlesFrame.raw()).endsWith(",\"body\":" + candles.strip() + "}");
        assertThat(warningsFrame.raw()).endsWith(",\"body\":" + warnings.strip() + "}");
        assertThat(rankingsFrame.raw()).contains(gainers.result().rankings().getFirst().symbol());

        // The token request is never recorded
        assertThat(frames).noneMatch(frame -> frame.raw().contains("access_token"));
    }
}