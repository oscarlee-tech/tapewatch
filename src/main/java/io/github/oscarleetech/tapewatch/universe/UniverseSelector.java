package io.github.oscarleetech.tapewatch.universe;

import io.github.oscarleetech.tapewatch.client.TossClient;
import io.github.oscarleetech.tapewatch.dto.RankingsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Picks today's movers from the Toss rankings.
 * Add-only: once a stock is watched, it stays watched until reset() at the start of the next day.
 */
@Component
public class UniverseSelector {

    private static final Logger log = LoggerFactory.getLogger(UniverseSelector.class);
    private static final Duration RANKINGS_TIMEOUT = Duration.ofSeconds(30);

    private final TossClient tossClient;
    private final OrdinaryStockWhitelist whitelist;
    private final int rankingCount;
    private final int maxPerSide;
    private final BigDecimal minAbsChangeRate;

    // LinkedHashMap keeps the order in which stocks were added.
    private final Map<WatchSide, LinkedHashMap<String, WatchedStock>> watched = new EnumMap<>(WatchSide.class);

    public UniverseSelector(TossClient tossClient,
                            OrdinaryStockWhitelist whitelist,
                            @Value("${recorder.universe.ranking-count:100}") int rankingCount,
                            @Value("${recorder.universe.max-per-side:45}") int maxPerSide,
                            @Value("${recorder.universe.min-abs-change-rate:0.10}") BigDecimal minAbsChangeRate) {
        this.tossClient = tossClient;
        this.whitelist = whitelist;
        this.rankingCount = rankingCount;
        this.maxPerSide = maxPerSide;
        this.minAbsChangeRate = minAbsChangeRate;
        for (WatchSide side : WatchSide.values()) {
            watched.put(side, new LinkedHashMap<>());
        }
    }

    /**
     * Fetches top gainers and top losers, then adds new qualifying stocks.
     * Never removes anything. Returns only the stocks added in this call.
     */
    public synchronized List<WatchedStock> refresh() {
        RankingsResponse gainers = tossClient.fetchTopGainersKr(rankingCount).block(RANKINGS_TIMEOUT);
        RankingsResponse losers = tossClient.fetchTopLosersKr(rankingCount).block(RANKINGS_TIMEOUT);

        Instant now = Instant.now();
        List<WatchedStock> added = new ArrayList<>();
        added.addAll(addNew(WatchSide.GAINERS, gainers, now));
        added.addAll(addNew(WatchSide.LOSERS, losers, now));

        log.info("Universe refreshed: added={}, gainers={}/{}, losers={}/{}",
                added.size(),
                watched.get(WatchSide.GAINERS).size(), maxPerSide,
                watched.get(WatchSide.LOSERS).size(), maxPerSide);
        return added;
    }

    /** Watched stocks on one side, in the order they were added. */
    public synchronized List<WatchedStock> watched(WatchSide side) {
        return List.copyOf(watched.get(side).values());
    }

    /** Clears both lists for a new trading day. */
    public synchronized void reset() {
        watched.values().forEach(Map::clear);
        log.info("Universe reset for a new trading day");
    }

    private List<WatchedStock> addNew(WatchSide side, RankingsResponse response, Instant now) {
        if (response == null) {
            return List.of();   // empty body from Toss
        }

        // Ordinary stocks that moved at least 10% in this side's direction, biggest trading amount first
        List<WatchedStock> candidates = response.result().rankings().stream()
                .filter(entry -> whitelist.contains(entry.symbol()))
                .filter(entry -> side == WatchSide.GAINERS
                        ? entry.price().changeRate().compareTo(minAbsChangeRate) >= 0
                        : entry.price().changeRate().compareTo(minAbsChangeRate.negate()) <= 0)
                .sorted(Comparator.comparing(RankingsResponse.Entry::tradingAmount).reversed())
                .map(entry -> new WatchedStock(entry.symbol(), side, now,
                        entry.price().changeRate(), entry.tradingVolume(), entry.tradingAmount()))
                .toList();

        LinkedHashMap<String, WatchedStock> sideList = watched.get(side);
        List<WatchedStock> added = new ArrayList<>();
        int skippedBecauseFull = 0;

        for (WatchedStock candidate : candidates) {
            // A stock that flips from gainer to loser (or back) must not be subscribed twice
            boolean alreadyWatched = watched.values().stream()
                    .anyMatch(list -> list.containsKey(candidate.symbol()));
            if (alreadyWatched) {
                continue;
            }
            if (sideList.size() >= maxPerSide) {
                skippedBecauseFull++;
                continue;
            }
            sideList.put(candidate.symbol(), candidate);
            added.add(candidate);
            log.info("Watch added: side={}, symbol={}, changeRate={}, tradingAmount={}",
                    side, candidate.symbol(), candidate.changeRate(), candidate.tradingAmount());
        }

        if (skippedBecauseFull > 0) {
            log.warn("{} list is full ({}), skipped {} candidates", side, maxPerSide, skippedBecauseFull);
        }
        return added;
    }
}