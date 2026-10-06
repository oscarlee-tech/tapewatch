package io.github.oscarleetech.tapewatch.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.oscarleetech.tapewatch.dto.OrderbookResponse;
import io.github.oscarleetech.tapewatch.dto.RankingsResponse;
import io.github.oscarleetech.tapewatch.dto.StocksResponse;
import io.github.oscarleetech.tapewatch.dto.TradesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

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

    private final WebClient webClient;
    private final String clientId;
    private final String clientSecret;

    // Token and expiry live in one immutable object, so readers never see a mixed pair.
    private volatile TokenState token;

    public TossClient(
            @Value("${recorder.toss-invest.base-url}") String baseUrl,
            @Value("${recorder.toss-invest.client-id}") String clientId,
            @Value("${recorder.toss-invest.client-secret}") String clientSecret) {

        this.webClient = WebClient.builder()
                .baseUrl(baseUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES))
                .build();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
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

        TokenResponse response = webClient.post()
                .uri("/oauth2/token")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .retrieve()
                .bodyToMono(TokenResponse.class)
                .block(TOKEN_TIMEOUT);

        if (response == null || response.accessToken() == null) {
            throw new IllegalStateException("Empty token response from Toss");
        }

        Instant expiresAt = Instant.now().plusSeconds(response.expiresIn());
        this.token = new TokenState(response.accessToken(), expiresAt);
        log.info("Toss access token issued: expiresIn={}s, expiresAt={}", response.expiresIn(), expiresAt);
    }

    private String requireAccessToken() {
        TokenState current = token;
        if (current == null) {
            throw new IllegalStateException("No access token available - call issueAccessToken() first");
        }
        if (Instant.now().isAfter(current.expiresAt().minus(EXPIRY_MARGIN))) {
            throw new IllegalStateException("Access token expired at " + current.expiresAt());
        }
        return current.value();
    }

    public Mono<TradesResponse> fetchTrades(String symbol) {
        return authorizedGet(uriBuilder -> uriBuilder.path("/api/v1/trades")
                .queryParam("symbol", symbol)
                .queryParam("count", 50)
                .build(), TradesResponse.class);
    }

    public Mono<OrderbookResponse> fetchOrderbook(String symbol) {
        return authorizedGet(uriBuilder -> uriBuilder.path("/api/v1/orderbook")
                .queryParam("symbol", symbol)
                .build(), OrderbookResponse.class);
    }

    public Mono<StocksResponse> fetchAllStocks(String market) {
        return authorizedGet(uriBuilder -> uriBuilder.path("/api/v1/stocks/all")
                .queryParam("market", market)
                .build(), StocksResponse.class);
    }

    public Mono<RankingsResponse> fetchRankings(String type,
                                                String marketCountry,
                                                String duration,
                                                boolean excludeInvestmentCaution,
                                                int count) {
        return authorizedGet(uriBuilder -> uriBuilder.path("/api/v1/rankings")
                .queryParam("type", type)
                .queryParam("marketCountry", marketCountry)
                .queryParam("duration", duration)
                .queryParam("excludeInvestmentCaution", excludeInvestmentCaution)
                .queryParam("count", count)
                .build(), RankingsResponse.class);
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

    private <T> Mono<T> authorizedGet(Function<UriBuilder, URI> uri, Class<T> type) {
        return Mono.fromCallable(this::requireAccessToken)
                .flatMap(accessToken -> webClient.get()
                        .uri(uri)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                        .retrieve()
                        .bodyToMono(type))
                .timeout(REQUEST_TIMEOUT)          // timeout applies to each attempt
                .retryWhen(rateLimitRetry());
    }

    /**
     * Retries only on 429 Too Many Requests, waiting about 1s, 2s, then 4s (with jitter).
     * Other errors (401, 404, 5xx, timeout) fail right away.
     */
    private Retry rateLimitRetry() {
        return Retry.backoff(MAX_RATE_LIMIT_RETRIES, FIRST_RETRY_DELAY)
                .filter(error -> error instanceof WebClientResponseException.TooManyRequests)
                .doBeforeRetry(signal -> log.warn("Toss rate limit hit (429), retry {}/{}",
                        signal.totalRetries() + 1, MAX_RATE_LIMIT_RETRIES));
    }

    private record TokenState(String value, Instant expiresAt) {}

    record TokenResponse(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("expires_in") long expiresIn) {}
}