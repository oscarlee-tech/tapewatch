package io.github.oscarleetech.tapewatch.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public record RankingsResponse(Result result) {

    public record Result(OffsetDateTime rankedAt, List<Entry> rankings) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Entry(
            int rank,
            String symbol,
            String currency,
            Price price,
            long tradingVolume,
            long tradingAmount) {}

    public record Price(
            BigDecimal lastPrice,
            BigDecimal basePrice,
            BigDecimal changeRate) {}
}