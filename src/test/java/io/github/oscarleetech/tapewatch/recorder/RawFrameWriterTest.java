package io.github.oscarleetech.tapewatch.recorder;

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
    void writesTheWholeDayToOneFile() throws IOException {
        RawFrameWriter writer = new RawFrameWriter(dir);
        writer.onEvent(event(0, at(9, 0, 1), "GAINERS", "morning"), 0, true);
        writer.onEvent(event(1, at(15, 19, 59), "LOSERS", "afternoon"), 1, true);
        writer.onShutdown();

        assertThat(readLines(dir.resolve("2026-10-08_090001.tsv.gz"))).containsExactly(
                "0\t" + at(9, 0, 1) + "\tGAINERS\tmorning",
                "1\t" + at(15, 19, 59) + "\tLOSERS\tafternoon");
    }

    @Test
    void crashThenRestartLosesOnlyUnflushedFrames() throws IOException {
        RawFrameWriter crashed = new RawFrameWriter(dir);
        crashed.onEvent(event(0, at(9, 10, 0), "GAINERS", "flushed"), 0, false);
        crashed.onTimeout(0);                                                     // the market went quiet
        crashed.onEvent(event(1, at(9, 10, 1), "GAINERS", "not-flushed"), 1, false);
        // no onShutdown(): this is what a crash looks like

        RawFrameWriter restarted = new RawFrameWriter(dir);
        restarted.onEvent(event(0, at(11, 20, 0), "GAINERS", "after-restart"), 0, true);
        restarted.onShutdown();

        assertThat(readLines(dir.resolve("2026-10-08_091000.tsv.gz")))
                .extracting(line -> line.split("\t")[3])
                .containsExactly("flushed");
        assertThat(readLines(dir.resolve("2026-10-08_112000.tsv.gz")))
                .extracting(line -> line.split("\t")[3])
                .containsExactly("after-restart");
    }

    @Test
    void recordsFramesPublishedThroughTheBus() throws IOException {
        FrameBus bus = new FrameBus(1 << 10, List.of(new RawFrameWriter(dir)));
        bus.publish("GAINERS", Instant.now(), "first");
        bus.publish("LOSERS", Instant.now(), "second");
        bus.publish("GAINERS", Instant.now(), "third");
        bus.shutdown();   // drains the ring, then the writer closes its file in onShutdown

        List<Path> files;
        try (Stream<Path> listing = Files.list(dir)) {
            files = listing.toList();
        }
        assertThat(files).hasSize(1);
        assertThat(readLines(files.getFirst()))
                .extracting(line -> line.split("\t")[0] + " " + line.split("\t")[2] + " " + line.split("\t")[3])
                .containsExactly("0 GAINERS first", "1 LOSERS second", "2 GAINERS third");
    }

    private static FrameEvent event(long seq, Instant receivedAt, String connection, String raw) {
        FrameEvent event = new FrameEvent();
        event.set(new RawFrame(seq, receivedAt, connection, raw));
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