package io.github.oscarleetech.tapewatch.recorder;

/**
 * What a frame on the FrameBus carries.
 * Recordings store the constant name, so constants are only ever added - never renamed or removed,
 * or old recordings can no longer be read.
 */
public enum FrameKind {

    /** Gainers WebSocket connection: trade, orderbook, ack, pong and error frames. */
    WS_GAINERS,

    /** Losers WebSocket connection. */
    WS_LOSERS,

    /** A stock was added to the watch list. */
    UNIVERSE,

    // REST responses, each wrapped in an envelope with the request and the HTTP status
    REST_RANKINGS,
    REST_TRADES,
    REST_CANDLES_1M,
    REST_CANDLES_1D,
    REST_INDEX_CANDLES_1M,
    REST_WARNINGS,
    REST_STOCKS,
    REST_STOCKS_ALL,
    REST_MARKET_CALENDAR,
    REST_ORDERBOOK
}