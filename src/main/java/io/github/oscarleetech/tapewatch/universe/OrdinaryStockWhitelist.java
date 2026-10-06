package io.github.oscarleetech.tapewatch.universe;

import io.github.oscarleetech.tapewatch.dto.StocksResponse;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Holds today's KOSPI + KOSDAQ ordinary stocks.
 * The whole map is swapped at once, so readers never see a half-built list.
 */
@Component
public class OrdinaryStockWhitelist {

    private volatile Map<String, StocksResponse.Stock> stocks = Map.of();

    public void replace(Map<String, StocksResponse.Stock> newStocks) {
        this.stocks = Map.copyOf(newStocks);
    }

    public boolean contains(String symbol) {
        return stocks.containsKey(symbol);
    }

    public int size() {
        return stocks.size();
    }
}