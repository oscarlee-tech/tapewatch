package io.github.oscarleetech.tapewatch.recorder;

import java.time.Instant;

/**
 * One frame as it arrived: a WebSocket message, a REST response envelope, or a universe record.
 * Immutable, so every handler can safely share it and keep it as long as it needs.
 */
public record RawFrame(long seq, Instant receivedAt, FrameKind kind, String raw) {
}