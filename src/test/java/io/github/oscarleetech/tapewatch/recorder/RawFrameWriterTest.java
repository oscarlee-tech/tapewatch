package io.github.oscarleetech.tapewatch.recorder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class RawFrameWriterTest {

    @TempDir
    Path dir;

    @Test
    @DisplayName("Writes a whole Korea-time day to one file, after the format header")
    void writesTheWholeDayToOneFileAfterTheFormatHeader() throws IOException {
        RawFrameWriter writer = new RawFrameWriter(dir);
        writer.onEvent(event(0, at(9, 0, 1), FrameKind.WS_GAINERS, "morning"), 0, true);
        writer.onEvent(event(1, at(15, 19, 59), FrameKind.WS_LOSERS, "afternoon"), 1, true);
        writer.onShutdown();

        assertThat(readLines(dir.resolve("2026-10-08_090001.tsv.gz"))).containsExactly(
                RawFrameWriter.FORMAT_HEADER,
                "0\t" + at(9, 0, 1) + "\tWS_GAINERS\tmorning",
                "1\t" + at(15, 19, 59) + "\tWS_LOSERS\tafternoon");
    }

    @Test
    @DisplayName("After a crash only unflushed frames are lost, and the restart opens a new file")
    void crashThenRestartLosesOnlyUnflushedFrames() throws IOException {
        RawFrameWriter crashed = new RawFrameWriter(dir);
        crashed.onEvent(event(0, at(9, 10, 0), FrameKind.WS_GAINERS, "flushed"), 0, false);
        crashed.onTimeout(0);                                                     // the market went quiet
        crashed.onEvent(event(1, at(9, 10, 1), FrameKind.WS_GAINERS, "not-flushed"), 1, false);
        // no onShutdown(): this is what a crash looks like

        RawFrameWriter restarted = new RawFrameWriter(dir);
        restarted.onEvent(event(0, at(11, 20, 0), FrameKind.WS_GAINERS, "after-restart"), 0, true);
        restarted.onShutdown();

        assertThat(readLines(dir.resolve("2026-10-08_091000.tsv.gz")))
                .filteredOn(line -> !line.startsWith("#"))
                .extracting(line -> line.split("\t")[3])
                .containsExactly("flushed");
        assertThat(readLines(dir.resolve("2026-10-08_112000.tsv.gz")))
                .filteredOn(line -> !line.startsWith("#"))
                .extracting(line -> line.split("\t")[3])
                .containsExactly("after-restart");
    }

    @Test
    @DisplayName("Records frames published through the FrameBus, one line per frame")
    void recordsFramesPublishedThroughTheBus() throws IOException {
        FrameBus bus = new FrameBus(1 << 10, List.of(new RawFrameWriter(dir)));
        bus.publish(FrameKind.WS_GAINERS, Instant.now(), "first");
        bus.publish(FrameKind.WS_LOSERS, Instant.now(), "second");
        bus.publish(FrameKind.REST_CANDLES_1M, Instant.now(), "{\"body\":\r\n\"third\"}");
        bus.shutdown();   // drains the ring, then the writer closes its file in onShutdown

        List<Path> files;
        try (Stream<Path> listing = Files.list(dir)) {
            files = listing.toList();
        }
        assertThat(files).hasSize(1);
        assertThat(readLines(files.getFirst()))
                .filteredOn(line -> !line.startsWith("#"))
                .extracting(line -> line.split("\t")[0] + " " + line.split("\t")[2] + " " + line.split("\t")[3])
                .containsExactly(
                        "0 WS_GAINERS first",
                        "1 WS_LOSERS second",
                        "2 REST_CANDLES_1M {\"body\":  \"third\"}");   // CR and LF became spaces, still one line
    }

    private static FrameEvent event(long seq, Instant receivedAt, FrameKind kind, String raw) {
        FrameEvent event = new FrameEvent();
        event.set(new RawFrame(seq, receivedAt, kind, raw));
        return event;
    }

    /**
     * Reads every complete line, even from a file whose writer crashed.
     * Bytes are collected first, because a line reader would throw away lines it already
     * decoded when it hits the cut-off end. A half-written last line is dropped.
     */
    private static List<String> readLines(Path gz) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream in = new GZIPInputStream(Files.newInputStream(gz))) {
            in.transferTo(bytes);
        } catch (EOFException cutOff) {
            // Expected for a crashed file: keep every byte read before the cut
        }

        String text = bytes.toString(UTF_8);
        int lastNewline = text.lastIndexOf('\n');
        return lastNewline < 0 ? List.of() : List.of(text.substring(0, lastNewline).split("\n"));
    }

    private static Instant at(int hour, int minute, int second) {
        return ZonedDateTime.of(2026, 10, 8, hour, minute, second, 0, ZoneId.of("Asia/Seoul")).toInstant();
    }
}