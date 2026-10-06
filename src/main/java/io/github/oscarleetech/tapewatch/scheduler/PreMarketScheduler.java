package io.github.oscarleetech.tapewatch.scheduler;

import io.github.oscarleetech.tapewatch.client.TossClient;
import io.github.oscarleetech.tapewatch.dto.StocksResponse;
import io.github.oscarleetech.tapewatch.universe.OrdinaryStockWhitelist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class PreMarketScheduler {

    private static final Logger log = LoggerFactory.getLogger(PreMarketScheduler.class);
    private static final Duration WHITELIST_TIMEOUT = Duration.ofSeconds(30);

    private final TossClient tossClient;
    private final OrdinaryStockWhitelist whitelist;
    private final boolean runOnStartup;

    public PreMarketScheduler(TossClient tossClient,
                              OrdinaryStockWhitelist whitelist,
                              @Value("${recorder.prepare-on-startup:true}") boolean runOnStartup) {
        this.tossClient = tossClient;
        this.whitelist = whitelist;
        this.runOnStartup = runOnStartup;
    }

    /** Runs every weekday at 08:30 Korea time, 30 minutes before the market opens. */
    @Scheduled(cron = "0 30 8 * * MON-FRI", zone = "Asia/Seoul")
    public void prepareTradingDay() {
        log.info("Pre-market preparation started");

        tossClient.issueAccessToken();

        Map<String, StocksResponse.Stock> stocks = tossClient.fetchOrdinaryStocksKr()
                .block(WHITELIST_TIMEOUT);

        if (stocks == null || stocks.isEmpty()) {
            throw new IllegalStateException("Ordinary stock list is empty - whitelist not updated");
        }

        whitelist.replace(stocks);
        log.info("Pre-market preparation finished: whitelistSize={}", stocks.size());
    }

    /**
     * Also prepare once on startup, so a restart or a local run does not wait for 08:30 KST.
     * Tests turn this off with recorder.prepare-on-startup=false to avoid calling the real Toss API.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void prepareOnStartup() {
        if (!runOnStartup) {
            log.info("Startup preparation skipped (recorder.prepare-on-startup=false)");
            return;
        }
        prepareTradingDay();
    }
}