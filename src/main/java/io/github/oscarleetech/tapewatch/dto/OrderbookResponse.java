package io.github.oscarleetech.tapewatch.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public record OrderbookResponse(Result result) {

    public record Result(
            OffsetDateTime timestamp,
            String currency,
            List<Level> asks,
            List<Level> bids) {}

    public record Level(BigDecimal price, BigDecimal volume) {}
}
