package io.github.oscarleetech.tapewatch.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public record TradesResponse(List<Trade> result) {

    public record Trade(
            BigDecimal price,
            BigDecimal volume,
            OffsetDateTime timestamp,
            String currency) {}
}
