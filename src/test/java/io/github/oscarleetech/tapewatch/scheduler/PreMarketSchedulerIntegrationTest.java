package io.github.oscarleetech.tapewatch.scheduler;

import io.github.oscarleetech.tapewatch.universe.OrdinaryStockWhitelist;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the real pre-market flow against the real Toss Open API (no mocks).
 * Warning: issuing a token here invalidates the token of any running tapewatch app.
 */
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class PreMarketSchedulerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(PreMarketSchedulerIntegrationTest.class);

    private static final String SAMSUNG_COMMON = "005930";
    private static final String SAMSUNG_PREFERRED = "005935";
    private static final String KODEX_200_ETF = "069500";

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final ZoneId CALIFORNIA = ZoneId.of("America/Los_Angeles");

    @Autowired
    private PreMarketScheduler scheduler;

    @Autowired
    private OrdinaryStockWhitelist whitelist;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @Test
    void prepareTradingDay_fillsWhitelistFromRealToss() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real Toss API test");

        scheduler.prepareTradingDay();

        log.info("Whitelist size after preparation: {}", whitelist.size());

        assertThat(whitelist.contains(SAMSUNG_COMMON)).isTrue();
        assertThat(whitelist.contains(SAMSUNG_PREFERRED)).isFalse();
        assertThat(whitelist.contains(KODEX_200_ETF)).isFalse();

        // Observed 2,604 on 2026-10-04. A big jump either way means the filter or the API changed.
        assertThat(whitelist.size()).isBetween(2_000, 3_500);
    }

    @Test
    void schedule_runsAt0830KoreaTimeOnWeekdaysOnly() throws NoSuchMethodException {
        // Read the real annotation, so this test breaks if someone edits the cron or the zone.
        Scheduled scheduled = PreMarketScheduler.class
                .getMethod("prepareTradingDay")
                .getAnnotation(Scheduled.class);
        CronExpression cron = CronExpression.parse(scheduled.cron());
        ZoneId zone = ZoneId.of(scheduled.zone());

        // Friday 10:00 KST, after that day's run -> skips the weekend -> Monday 08:30 KST
        ZonedDateTime fridayMorning = ZonedDateTime.of(2026, 10, 2, 10, 0, 0, 0, zone);
        assertThat(cron.next(fridayMorning))
                .isEqualTo(ZonedDateTime.of(2026, 10, 5, 8, 30, 0, 0, SEOUL));

        // Sunday 21:00 in California is already Monday 13:00 in Korea -> next run is Tuesday 08:30 KST
        ZonedDateTime sundayNightInCalifornia = ZonedDateTime.of(2026, 10, 4, 21, 0, 0, 0, CALIFORNIA)
                .withZoneSameInstant(zone);
        assertThat(cron.next(sundayNightInCalifornia))
                .isEqualTo(ZonedDateTime.of(2026, 10, 6, 8, 30, 0, 0, SEOUL));
    }
}