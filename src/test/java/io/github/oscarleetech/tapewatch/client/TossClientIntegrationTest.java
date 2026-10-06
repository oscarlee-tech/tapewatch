package io.github.oscarleetech.tapewatch.client;

import io.github.oscarleetech.tapewatch.dto.OrderbookResponse;
import io.github.oscarleetech.tapewatch.dto.RankingsResponse;
import io.github.oscarleetech.tapewatch.dto.StocksResponse;
import io.github.oscarleetech.tapewatch.dto.TradesResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Calls the real Toss Open API with credentials from .env.
 * Skipped when TOSS_CLIENT_ID is missing (for example, a fresh clone without .env).
 *
 * Warning: issuing a token here invalidates the token of any running tapewatch app,
 * because Toss allows only one valid token per client.
 */
@SpringBootTest(properties = "recorder.prepare-on-startup=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TossClientIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(TossClientIntegrationTest.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String SAMSUNG_COMMON = "005930";
    private static final String SAMSUNG_PREFERRED = "005935";
    private static final String KODEX_200_ETF = "069500";

    @Autowired
    private TossClient tossClient;

    @Value("${recorder.toss-invest.client-id:}")
    private String clientId;

    @BeforeAll
    void issueTokenOnce() {
        assumeTrue(!clientId.isBlank(), "TOSS_CLIENT_ID is not set - skipping real Toss API tests");
        tossClient.issueAccessToken();
    }

    @Test
    void ordinaryStocksKr_keepsOnlyOrdinaryStocksFromBothMarkets() {
        List<StocksResponse.Stock> kospi = tossClient.fetchAllStocks("KOSPI").block(TIMEOUT).result();
        List<StocksResponse.Stock> kosdaq = tossClient.fetchAllStocks("KOSDAQ").block(TIMEOUT).result();
        int kospiOrdinary = (int) kospi.stream().filter(StocksResponse.Stock::isOrdinaryStock).count();

        Map<String, StocksResponse.Stock> whitelist = tossClient.fetchOrdinaryStocksKr().block(TIMEOUT);

        log.info("KOSPI all={}, KOSDAQ all={}, KOSPI ordinary={}, whitelist={}",
                kospi.size(), kosdaq.size(), kospiOrdinary, whitelist.size());

        // Known answers: common stock stays, preferred stock and ETF are filtered out
        assertThat(whitelist).containsKey(SAMSUNG_COMMON);
        assertThat(whitelist).doesNotContainKeys(SAMSUNG_PREFERRED, KODEX_200_ETF);

        // The filter removed something, and KOSDAQ was merged in
        assertThat(whitelist.size()).isLessThan(kospi.size() + kosdaq.size());
        assertThat(whitelist.size()).isGreaterThan(kospiOrdinary);
    }

    @Test
    void topLosersAndGainersKr_returnResponses() {
        RankingsResponse losers = tossClient.fetchTopLosersKr(20).block(TIMEOUT);
        RankingsResponse gainers = tossClient.fetchTopGainersKr(20).block(TIMEOUT);

        log.info("Top losers: {}", losers);
        log.info("Top gainers: {}", gainers);

        // TODO: assert a key field (for example, the list is not empty) once checked against RankingsResponse
        assertThat(losers).isNotNull();
        assertThat(gainers).isNotNull();
    }

    @Test
    void trades_returnResponseForSamsung() {
        TradesResponse trades = tossClient.fetchTrades(SAMSUNG_COMMON).block(TIMEOUT);

        log.info("Trades: {}", trades);
        assertThat(trades).isNotNull();
    }

    @Test
    void orderbook_returnsResponseForSamsung() {
        OrderbookResponse orderbook = tossClient.fetchOrderbook(SAMSUNG_COMMON).block(TIMEOUT);

        log.info("Orderbook: {}", orderbook);
        assertThat(orderbook).isNotNull();
    }
}