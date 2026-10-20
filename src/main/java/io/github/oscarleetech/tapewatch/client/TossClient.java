package io.github.oscarleetech.tapewatch.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.oscarleetech.tapewatch.dto.OrderbookResponse;
import io.github.oscarleetech.tapewatch.dto.RankingsResponse;
import io.github.oscarleetech.tapewatch.dto.StocksResponse;
import io.github.oscarleetech.tapewatch.dto.TradesResponse;
import io.github.oscarleetech.tapewatch.recorder.FrameBus;
import io.github.oscarleetech.tapewatch.recorder.FrameKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Toss Open API client. Every GET goes through one path: wait for a slot of its Rate Limits Group,
 * send, record the response on the FrameBus in an envelope, then hand the same body to the caller.
 * Recorded envelope: {"path":...,"query":{...},"status":200,"sentAt":"...","body":<Toss JSON as received>}
 * The token request (POST) is never recorded, so secrets never reach a recording.
 */
@Component
public class TossClient {

    private static final Logger log = LoggerFactory.getLogger(TossClient.class);

    // Treat the token as expired a bit early so a request never reaches Toss with a dying token.
    private static final Duration EXPIRY_MARGIN = Duration.ofMinutes(1);
    private static final Duration TOKEN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_RESPONSE_BYTES = 10 * 1024 * 1024;
    private static final int MAX_RATE_LIMIT_RETRIES = 3;
    private static final Duration FIRST_RETRY_DELAY = Duration.ofSeconds(1);
    private static final int MAX_SYMBOLS_PER_STOCKS_CALL = 200;

    private final WebClient webClient;
    private final String clientId;
    private final String clientSecret;
    private final FrameBus frameBus;
    private final JsonMapper jsonMapper;
    private final RequestPacer pacer = new RequestPacer();

    // Token and expiry live in one immutable object, so readers never see a mixed pair.
    private volatile TokenState token;

    public TossClient(
            @Value("${recorder.toss-invest.base-url}") String baseUrl,
            @Value("${recorder.toss-invest.client-id}") String clientId,
            @Value("${recorder.toss-invest.client-secret}") String clientSecret,
            FrameBus frameBus,
            JsonMapper jsonMapper) {

        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES))
                .build();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.frameBus = frameBus;
        this.jsonMapper = jsonMapper;
    }

    /**
     * Issues a new access token. Toss allows only one valid token per client,
     * so calling this invalidates any previously issued token immediately.
     */
    public void issueAccessToken() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "client_credentials");
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);

        TokenResponse response = pacer.waitFor(RateLimitGroup.AUTH)
                .then(webClient.post()
                        .uri("/oauth2/token")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .bodyValue(form)
                        .retrieve()
                        .bodyToMono(TokenResponse.class))
                .block(TOKEN_TIMEOUT);

        if (response == null || response.accessToken() == null) {
            throw new IllegalStateException("Empty token response from Toss");
        }

        Instant expiresAt = Instant.now().plusSeconds(response.expiresIn());
        this.token = new TokenState(response.accessToken(), expiresAt);
        log.info("Toss access token issued: expiresIn={}s, expiresAt={}", response.expiresIn(), expiresAt);
    }

    public String requireAccessToken() {
        TokenState current = token;
        if (current == null) {
            throw new IllegalStateException("No access token available - call issueAccessToken() first");
        }
        if (Instant.now().isAfter(current.expiresAt().minus(EXPIRY_MARGIN))) {
            throw new IllegalStateException("Access token expired at " + current.expiresAt());
        }
        return current.value();
    }

    // ---- Typed calls: parsed from the same body that was recorded ----

    public Mono<TradesResponse> fetchTrades(String symbol) {
        return getJson(FrameKind.REST_TRADES, RateLimitGroup.MARKET_DATA, "/api/v1/trades",
                Map.of("symbol", symbol, "count", "50"), TradesResponse.class);
    }

    public Mono<OrderbookResponse> fetchOrderbook(String symbol) {
        return getJson(FrameKind.REST_ORDERBOOK, RateLimitGroup.MARKET_DATA, "/api/v1/orderbook",
                Map.of("symbol", symbol), OrderbookResponse.class);
    }

    public Mono<StocksResponse> fetchAllStocks(String market) {
        return getJson(FrameKind.REST_STOCKS_ALL, RateLimitGroup.STOCK_ALL, "/api/v1/stocks/all",
                Map.of("market", market), StocksResponse.class);
    }

    public Mono<RankingsResponse> fetchRankings(String type,
                                                String marketCountry,
                                                String duration,
                                                boolean excludeInvestmentCaution,
                                                int count) {
        return getJson(FrameKind.REST_RANKINGS, RateLimitGroup.RANKING, "/api/v1/rankings",
                Map.of("type", type,
                        "marketCountry", marketCountry,
                        "duration", duration,
                        "excludeInvestmentCaution", String.valueOf(excludeInvestmentCaution),
                        "count", String.valueOf(count)),
                RankingsResponse.class);
    }

    public Mono<Map<String, StocksResponse.Stock>> fetchOrdinaryStocksKr() {
        return Flux.just("KOSPI", "KOSDAQ")
                .concatMap(this::fetchAllStocks)
                .flatMapIterable(StocksResponse::result)
                .filter(StocksResponse.Stock::isOrdinaryStock)
                .collectMap(StocksResponse.Stock::symbol);
    }

    public Mono<RankingsResponse> fetchTopLosersKr(int count) {
        return fetchRankings("TOP_LOSERS", "KR", "1d", true, count);
    }

    public Mono<RankingsResponse> fetchTopGainersKr(int count) {
        return fetchRankings("TOP_GAINERS", "KR", "1d", true, count);
    }

    // ---- Raw calls: consumers read these from the FrameBus, so the body is returned as received ----

    /**
     * 1-minute bars at actual prices, newest first, up to 200. Pass before = null for the latest bars,
     * or a time to page further back (bars ending at or before it).
     */
    public Mono<String> fetchMinuteCandles(String symbol, int count, OffsetDateTime before) {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("symbol", symbol);
        query.put("interval", "1m");
        query.put("count", String.valueOf(count));
        query.put("adjusted", "false");
        if (before != null) {
            query.put("before", before.toString());
        }
        return getRaw(FrameKind.REST_CANDLES_1M, RateLimitGroup.MARKET_DATA_CHART, "/api/v1/candles", query);
    }

    /** Daily bars with adjusted prices, so yesterday's close is today's base price. */
    public Mono<String> fetchDailyCandles(String symbol, int count) {
        return getRaw(FrameKind.REST_CANDLES_1D, RateLimitGroup.MARKET_DATA_CHART, "/api/v1/candles",
                Map.of("symbol", symbol, "interval", "1d", "count", String.valueOf(count), "adjusted", "true"));
    }

    /** 1-minute bars of KOSPI or KOSDAQ. */
    public Mono<String> fetchIndexMinuteCandles(String indexSymbol, int count) {
        return getRaw(FrameKind.REST_INDEX_CANDLES_1M, RateLimitGroup.MARKET_INDICATOR_CHART,
                "/api/v1/market-indicators/" + indexSymbol + "/candles",
                Map.of("interval", "1m", "count", String.valueOf(count)));
    }

    /** Active buying warnings, including VI. */
    public Mono<String> fetchWarnings(String symbol) {
        return getRaw(FrameKind.REST_WARNINGS, RateLimitGroup.STOCK,
                "/api/v1/stocks/" + symbol + "/warnings", Map.of());
    }

    /** Basic info (shares outstanding, market) for up to 200 symbols in one call. */
    public Mono<String> fetchStocks(List<String> symbols) {
        if (symbols.isEmpty() || symbols.size() > MAX_SYMBOLS_PER_STOCKS_CALL) {
            throw new IllegalArgumentException("Need 1 to " + MAX_SYMBOLS_PER_STOCKS_CALL + " symbols, got " + symbols.size());
        }
        return getRaw(FrameKind.REST_STOCKS, RateLimitGroup.STOCK, "/api/v1/stocks",
                Map.of("symbols", String.join(",", symbols)));
    }

    /** Korean market sessions for yesterday, today and tomorrow. Today's "integrated" is null on a holiday. */
    public Mono<String> fetchMarketCalendarKr() {
        return getRaw(FrameKind.REST_MARKET_CALENDAR, RateLimitGroup.MARKET_INFO, "/api/v1/market-calendar/KR", Map.of());
    }

    // ---- The one path every GET takes ----

    private <T> Mono<T> getJson(FrameKind kind, RateLimitGroup group, String path,
                                Map<String, String> query, Class<T> type) {
        return getRaw(kind, group, path, query).map(body -> jsonMapper.readValue(body, type));
    }

    private Mono<String> getRaw(FrameKind kind, RateLimitGroup group, String path, Map<String, String> query) {
        return pacer.waitFor(group)
                .then(Mono.defer(() -> {
                    String accessToken = requireAccessToken();
                    Instant sentAt = Instant.now();
                    return webClient.get()
                            .uri(builder -> {
                                builder.path(path);
                                // Values go in as URI variables so they are fully encoded ('+' in a time offset becomes %2B)
                                query.keySet().forEach(name -> builder.queryParam(name, "{" + name + "}"));
                                return builder.build(query);
                            })
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                            .exchangeToMono(response -> response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .map(body -> {
                                        Instant receivedAt = Instant.now();
                                        int status = response.statusCode().value();

                                        // Record what we asked, what came back, and when. Toss JSON goes in untouched;
                                        // anything else (an empty body, an HTML error page) goes in as a JSON string.
                                        String trimmed = body.strip();
                                        String bodyJson = trimmed.startsWith("{") || trimmed.startsWith("[")
                                                ? trimmed : jsonMapper.writeValueAsString(body);
                                        frameBus.publish(kind, receivedAt, "{\"path\":" + jsonMapper.writeValueAsString(path)
                                                + ",\"query\":" + jsonMapper.writeValueAsString(query)
                                                + ",\"status\":" + status
                                                + ",\"sentAt\":\"" + sentAt + "\""
                                                + ",\"body\":" + bodyJson + "}");

                                        if (status == 429) {
                                            throw new RateLimited(response.headers().header("Retry-After").stream()
                                                    .findFirst()
                                                    .map(seconds -> Duration.ofSeconds(Long.parseLong(seconds.strip())))
                                                    .orElse(null));
                                        }
                                        if (status >= 400) {
                                            throw new IllegalStateException("Toss GET " + path + " failed with " + status + ": " + body);
                                        }
                                        return body;
                                    }))
                            .timeout(REQUEST_TIMEOUT);
                }))
                .retryWhen(Retry.from(signals -> signals.concatMap(signal -> {
                    if (!(signal.failure() instanceof RateLimited limited) || signal.totalRetries() >= MAX_RATE_LIMIT_RETRIES) {
                        return Mono.error(signal.failure());
                    }
                    // Wait what Toss asks for (Retry-After), but at least 1s, 2s, then 4s
                    Duration backoff = FIRST_RETRY_DELAY.multipliedBy(1L << signal.totalRetries());
                    Duration wait = limited.retryAfter != null && limited.retryAfter.compareTo(backoff) > 0
                            ? limited.retryAfter : backoff;
                    log.warn("Toss rate limit hit (429) on {}, retry {}/{} in {} ms",
                            path, signal.totalRetries() + 1, MAX_RATE_LIMIT_RETRIES, wait.toMillis());
                    return Mono.delay(wait);
                })));
    }

    /** A 429 response, with the wait Toss asked for (null if it gave none). */
    private static final class RateLimited extends RuntimeException {
        private final Duration retryAfter;

        private RateLimited(Duration retryAfter) {
            super("Toss rate limit exceeded (429)", null, false, false);
            this.retryAfter = retryAfter;
        }
    }

    private record TokenState(String value, Instant expiresAt) {}

    record TokenResponse(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("expires_in") long expiresIn) {}
}