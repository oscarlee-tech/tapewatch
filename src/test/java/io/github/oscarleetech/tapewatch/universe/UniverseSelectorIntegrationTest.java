package io.github.oscarleetech.tapewatch.universe;

import io.github.oscarleetech.tapewatch.scheduler.PreMarketScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the selector against the real Toss rankings (no mocks).
 * Warning: issuing a token here invalidates the token of any running tapewatch app.
 */
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class UniverseSelectorIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(UniverseSelectorIntegrationTest.class);

    @Value("${recorder.universe.min-abs-change-rate:0.10}")
    private BigDecimal minAbsChangeRate;

    @Autowired
    private PreMarketScheduler preMarketScheduler;

    @Autowired
    private UniverseSelector selector;

    @Autowired
    private OrdinaryStockWhitelist whitelist;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Value("${recorder.universe.max-per-side:45}")
    private int maxPerSide;

    @BeforeEach
    void prepare() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real Toss API test");
        preMarketScheduler.prepareTradingDay();   // real token + real whitelist
        selector.reset();
    }

    @Test
    void refresh_addsOnlyOrdinaryStocksThatMovedTenPercent() {
        List<WatchedStock> added = selector.refresh();
        List<WatchedStock> gainers = selector.watched(WatchSide.GAINERS);
        List<WatchedStock> losers = selector.watched(WatchSide.LOSERS);

        gainers.forEach(s -> log.info("GAINER {} rate={} amount={}", s.symbol(), s.changeRate(), s.tradingAmount()));
        losers.forEach(s -> log.info("LOSER  {} rate={} amount={}", s.symbol(), s.changeRate(), s.tradingAmount()));

        assertThat(added).as("Korea almost always has +/-10% movers").isNotEmpty();
        assertThat(added).allSatisfy(s -> assertThat(whitelist.contains(s.symbol())).isTrue());

        assertThat(gainers).allSatisfy(s -> assertThat(s.changeRate()).isGreaterThanOrEqualTo(minAbsChangeRate));
        assertThat(losers).allSatisfy(s -> assertThat(s.changeRate()).isLessThanOrEqualTo(minAbsChangeRate.negate()));

        assertThat(gainers).hasSizeLessThanOrEqualTo(maxPerSide);
        assertThat(losers).hasSizeLessThanOrEqualTo(maxPerSide);

        // A stock must never be watched on both sides
        Set<String> gainerSymbols = new HashSet<>(gainers.stream().map(WatchedStock::symbol).toList());
        assertThat(losers).noneMatch(s -> gainerSymbols.contains(s.symbol()));

        // First refresh of the day: each side is ordered by trading amount, biggest first
        assertThat(gainers).isSortedAccordingTo((a, b) -> b.tradingAmount().compareTo(a.tradingAmount()));
        assertThat(losers).isSortedAccordingTo((a, b) -> b.tradingAmount().compareTo(a.tradingAmount()));
    }

    @Test
    void refresh_neverRemovesWatchedStocks() {
        selector.refresh();
        List<String> before = symbols();

        selector.refresh();
        List<String> after = symbols();

        log.info("Watched before={}, after={}", before.size(), after.size());
        assertThat(after).containsAll(before);
    }

    private List<String> symbols() {
        return Stream.of(WatchSide.values())
                .flatMap(side -> selector.watched(side).stream())
                .map(WatchedStock::symbol)
                .toList();
    }
}