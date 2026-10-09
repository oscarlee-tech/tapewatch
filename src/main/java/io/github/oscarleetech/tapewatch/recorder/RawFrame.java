package io.github.oscarleetech.tapewatch.recorder;

import java.time.Instant;

/** One WebSocket frame as it arrived. Immutable, so every handler can safely share it. */
public record RawFrame(long seq, Instant receivedAt, String connection, String raw) {
}