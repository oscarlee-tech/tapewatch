package io.github.oscarleetech.tapewatch.scheduler;

import com.lmax.disruptor.EventHandler;
import io.github.oscarleetech.tapewatch.client.TossClient;
import io.github.oscarleetech.tapewatch.recorder.FrameEvent;
import io.github.oscarleetech.tapewatch.recorder.FrameKind;
import io.github.oscarleetech.tapewatch.recorder.RawFrame;
import io.github.oscarleetech.tapewatch.universe.WatchSide;
import io.github.oscarleetech.tapewatch.universe.WatchedStock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("RecordingScheduler onboarding (real Toss API)")
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class RecordingSchedulerIntegrationTest {

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

    @Autowired
    private RecordingScheduler scheduler;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Test
    @DisplayName("A new stock is recorded as UNIVERSE first, then its whole onboarding package follows")
    void newStockIsRecordedBeforeItsOnboardingPackage() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real API test");
        tossClient.issueAccessToken();

        WatchedStock samsung = new WatchedStock("005930", WatchSide.GAINERS, Instant.now(),
                new BigDecimal("0.1034"), new BigDecimal("1523400"), new BigDecimal("321000000000"));

        scheduler.onStocksAdded(List.of(samsung));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(frames)
                .extracting(RawFrame::kind)
                .contains(FrameKind.UNIVERSE, FrameKind.REST_STOCKS, FrameKind.REST_TRADES,
                        FrameKind.REST_CANDLES_1D, FrameKind.REST_CANDLES_1M, FrameKind.REST_WARNINGS));

        RawFrame universe = frames.stream().filter(f -> f.kind() == FrameKind.UNIVERSE).findFirst().orElseThrow();
        assertThat(universe.raw()).contains("\"symbol\":\"005930\"", "\"side\":\"GAINERS\"");

        // Every REST frame is about Samsung, succeeded, and came after the UNIVERSE line
        assertThat(frames).filteredOn(f -> f.kind() != FrameKind.UNIVERSE)
                .allSatisfy(frame -> {
                    assertThat(frame.raw()).contains("005930", "\"status\":200");
                    assertThat(frame.seq()).isGreaterThan(universe.seq());
                });
    }
}