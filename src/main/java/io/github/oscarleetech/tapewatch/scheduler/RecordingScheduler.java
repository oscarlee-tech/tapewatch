package io.github.oscarleetech.tapewatch.scheduler;

import io.github.oscarleetech.tapewatch.client.TossClient;
import io.github.oscarleetech.tapewatch.recorder.FrameBus;
import io.github.oscarleetech.tapewatch.recorder.FrameKind;
import io.github.oscarleetech.tapewatch.recorder.TossMarketStream;
import io.github.oscarleetech.tapewatch.universe.UniverseSelector;
import io.github.oscarleetech.tapewatch.universe.WatchSide;
import io.github.oscarleetech.tapewatch.universe.WatchedStock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs the recording day on Korea time:
 * 09:00 start both WebSocket connections and pick the first stocks,
 * every 5 minutes add new movers (until 14:50) with their onboarding package,
 * every minute fetch the latest 1-minute bars of watched stocks and indexes, 15:20 stop.
 * Everything goes through TossClient or TossMarketStream, so all of it is recorded on the FrameBus.
 */
@Component
public class RecordingScheduler {

    private static final Logger log = LoggerFactory.getLogger(RecordingScheduler.class);
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 0);
    private static final LocalTime LAST_SELECTION = LocalTime.of(14, 50);
    private static final LocalTime RECORDING_END = LocalTime.of(15, 20);
    private static final LocalTime FIRST_PAGE_COVERS_OPEN_UNTIL = LocalTime.of(12, 20);   // 200 bars = 09:01 ~ 12:20
    private static final int MAX_BARS_PER_CALL = 200;
    private static final int LATEST_BARS = 3;
    private static final Duration PING_INTERVAL = Duration.ofSeconds(60);
    private static final Duration CALENDAR_TIMEOUT = Duration.ofSeconds(10);
    private static final List<String> INDEXES = List.of("KOSPI", "KOSDAQ");

    private final TossClient tossClient;
    private final UniverseSelector selector;
    private final FrameBus frameBus;
    private final JsonMapper jsonMapper;
    private final boolean runOnStartup;
    private final Map<WatchSide, TossMarketStream> streams;

    private volatile boolean recording;

    public RecordingScheduler(TossClient tossClient,
                              UniverseSelector selector,
                              FrameBus frameBus,
                              JsonMapper jsonMapper,
                              @Value("${recorder.toss-invest.ws-url:wss://openapi-ws.tossinvest.com/ws/v1}") URI wsUrl,
                              @Value("${recorder.prepare-on-startup:true}") boolean runOnStartup) {
        this.tossClient = tossClient;
        this.selector = selector;
        this.frameBus = frameBus;
        this.jsonMapper = jsonMapper;
        this.runOnStartup = runOnStartup;
        this.streams = Map.of(
                WatchSide.GAINERS, new TossMarketStream(FrameKind.WS_GAINERS, wsUrl, PING_INTERVAL, tossClient, frameBus),
                WatchSide.LOSERS, new TossMarketStream(FrameKind.WS_LOSERS, wsUrl, PING_INTERVAL, tossClient, frameBus));
    }

    /**
     * A restart in the middle of the session starts recording right away instead of waiting for tomorrow.
     * Runs after PreMarketScheduler has issued the token and loaded the whitelist.
     */
    @EventListener(ApplicationReadyEvent.class)
    @Order(1)
    public void startOnStartup() {
        if (!runOnStartup) {
            log.info("Startup recording skipped (recorder.prepare-on-startup=false)");
            return;
        }
        ZonedDateTime now = ZonedDateTime.now(SEOUL);
        boolean weekday = now.getDayOfWeek() != DayOfWeek.SATURDAY && now.getDayOfWeek() != DayOfWeek.SUNDAY;
        if (weekday && !now.toLocalTime().isBefore(MARKET_OPEN) && now.toLocalTime().isBefore(RECORDING_END)) {
            log.info("Started during market hours - recording now");
            startRecording();
        }
    }

    @Scheduled(cron = "0 0 9 * * MON-FRI", zone = "Asia/Seoul")
    public synchronized void startRecording() {
        if (recording) {
            return;
        }

        // A failed calendar check must not cost a whole day, so it only stops us on a confirmed holiday
        boolean open = true;
        try {
            String calendar = tossClient.fetchMarketCalendarKr().block(CALENDAR_TIMEOUT);
            open = jsonMapper.readTree(calendar).path("result").path("today").path("integrated").isObject();
        } catch (RuntimeException e) {
            log.warn("Market calendar check failed, recording anyway", e);
        }
        if (!open) {
            log.info("Market is closed today - not recording");
            return;
        }

        selector.reset();
        streams.values().forEach(TossMarketStream::start);
        recording = true;
        log.info("Recording started");
        refreshUniverse();
    }

    @Scheduled(cron = "0 */5 9-14 * * MON-FRI", zone = "Asia/Seoul")
    public synchronized void refreshUniverse() {
        if (!recording || LocalTime.now(SEOUL).isAfter(LAST_SELECTION)) {
            return;   // after 14:50 a new stock can no longer get a full 30-minute label
        }
        List<WatchedStock> added = selector.refresh();
        if (!added.isEmpty()) {
            onStocksAdded(added);
        }
    }

    @Scheduled(cron = "2 * 9-15 * * MON-FRI", zone = "Asia/Seoul")
    public void fetchLatestMinuteBars() {
        if (!recording) {
            return;
        }
        // Two seconds after the minute, so the bar that just ended is complete on Toss's side
        List<Mono<Void>> calls = new ArrayList<>();
        for (WatchSide side : WatchSide.values()) {
            for (WatchedStock stock : selector.watched(side)) {
                calls.add(tossClient.fetchMinuteCandles(stock.symbol(), LATEST_BARS, null).then());
            }
        }
        INDEXES.forEach(index -> calls.add(tossClient.fetchIndexMinuteCandles(index, LATEST_BARS).then()));
        sendAll("Minute bars", calls);
    }

    @Scheduled(cron = "0 20 15 * * MON-FRI", zone = "Asia/Seoul")
    public synchronized void stopRecording() {
        if (!recording) {
            return;
        }
        recording = false;
        streams.values().forEach(TossMarketStream::stop);
        log.info("Recording stopped");
    }

    /**
     * Stops the streams before Spring shuts down the web server and its Netty resources.
     * That shutdown also closes our WebSocket sockets, and the streams would take it for a network
     * drop and reconnect. ContextClosedEvent is published before any of it.
     */
    @EventListener(ContextClosedEvent.class)
    public void shutdown() {
        stopRecording();
    }

    /**
     * Records who was added, re-declares the sides that changed, and fetches each new stock's onboarding package.
     * Package-private so a test can drive it without waiting for the market clock.
     */
    synchronized void onStocksAdded(List<WatchedStock> added) {
        // 1) Who was added, before any of their data: replay builds the stock's card from this line
        for (WatchedStock stock : added) {
            frameBus.publish(FrameKind.UNIVERSE, stock.addedAt(), jsonMapper.writeValueAsString(stock));
        }

        // 2) Re-declare only the sides that changed (declarations are full-replace)
        for (WatchSide side : WatchSide.values()) {
            if (added.stream().anyMatch(stock -> stock.side() == side)) {
                streams.get(side).updateSubscriptions(
                        selector.watched(side).stream().map(WatchedStock::symbol).toList());
            }
        }

        // 3) Onboarding package: what a row needs about the time before we subscribed
        boolean needsSecondPage = LocalTime.now(SEOUL).isAfter(FIRST_PAGE_COVERS_OPEN_UNTIL);
        OffsetDateTime secondPageEnd = LocalDate.now(SEOUL).atTime(FIRST_PAGE_COVERS_OPEN_UNTIL).atZone(SEOUL).toOffsetDateTime();

        List<Mono<Void>> calls = new ArrayList<>();
        calls.add(tossClient.fetchStocks(added.stream().map(WatchedStock::symbol).toList()).then());
        for (WatchedStock stock : added) {
            String symbol = stock.symbol();
            calls.add(tossClient.fetchTrades(symbol).then());
            calls.add(tossClient.fetchDailyCandles(symbol, 2).then());
            calls.add(tossClient.fetchMinuteCandles(symbol, MAX_BARS_PER_CALL, null).then());
            if (needsSecondPage) {
                calls.add(tossClient.fetchMinuteCandles(symbol, MAX_BARS_PER_CALL, secondPageEnd).then());
            }
            calls.add(tossClient.fetchWarnings(symbol).then());
        }
        sendAll("Onboarding", calls);
        log.info("Onboarding {} new stocks", added.size());
    }

    /** Sends every call without waiting. One failure is logged and does not cancel the others. */
    private static void sendAll(String what, List<Mono<Void>> calls) {
        Flux.fromIterable(calls)
                .flatMap(call -> call.onErrorResume(error -> {
                    log.warn("{} request failed: {}", what, error.getMessage());
                    return Mono.empty();
                }))
                .subscribe();
    }
}