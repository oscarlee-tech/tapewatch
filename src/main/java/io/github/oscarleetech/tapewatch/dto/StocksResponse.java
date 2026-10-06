package io.github.oscarleetech.tapewatch.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record StocksResponse(List<Stock> result) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Stock(
            String symbol,
            String name,
            String securityType,
            @JsonProperty("isCommonShare") boolean commonShare,
            String isinCode) {

        /** Ordinary common share only - excludes ETF, ETN, and preferred shares. */
        public boolean isOrdinaryStock() {
            return "STOCK".equals(securityType) && commonShare;
        }
    }
}