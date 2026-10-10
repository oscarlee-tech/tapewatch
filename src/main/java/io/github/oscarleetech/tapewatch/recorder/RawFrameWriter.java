package io.github.oscarleetech.tapewatch.recorder;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * FrameBus handler that records every frame to one gzip file per Korea-time day,
 * named by when the file was opened: {@code data/raw/2026-10-08_085512.tsv.gz}.
 * Each line is tab separated: seq, receivedAt (UTC), connection, raw frame.
 *
 * A restart always opens a new file instead of appending. After a crash the old file ends with
 * a cut-off gzip block, and anything appended behind it could not be read back.
 */
@Component
public class RawFrameWriter implements EventHandler<FrameEvent> {

    private static final Logger log = LoggerFactory.getLogger(RawFrameWriter.class);
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter FILE_NAME = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
    private static final long FLUSH_INTERVAL_NANOS = 1_000_000_000L;   // 1 second

    private final Path rawDir;

    // Only the Disruptor handler thread touches these fields, so no locks are needed
    private Writer current;
    private Path currentPath;
    private LocalDate currentDay;
    private long framesInFile;
    private long lastFlushNanos = System.nanoTime();   // nanoTime never goes back, unlike the wall clock

    public RawFrameWriter(@Value("${recorder.raw-dir:data/raw}") Path rawDir) {
        this.rawDir = rawDir;
    }

    @Override
    public void onEvent(FrameEvent event, long sequence, boolean endOfBatch) throws IOException {
        RawFrame frame = event.frame();
        ZonedDateTime kst = frame.receivedAt().atZone(SEOUL);

        // First frame of this run, or a new Korea-time day: start a new file (never append)
        if (!kst.toLocalDate().equals(currentDay)) {
            closeCurrent();
            Files.createDirectories(rawDir);
            Path path = rawDir.resolve(kst.format(FILE_NAME) + ".tsv.gz");
            current = new OutputStreamWriter(new GZIPOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(path, CREATE_NEW, WRITE)), true), UTF_8);
            currentPath = path;
            currentDay = kst.toLocalDate();
            log.info("Recording raw frames to {}", path.toAbsolutePath());
        }

        current.write(frame.seq() + "\t" + frame.receivedAt() + "\t" + frame.connection() + "\t"
                + frame.raw().replace('\n', ' ') + "\n");
        framesInFile++;

        // Busy market: flush at the end of a batch, at most once per second
        if (endOfBatch && System.nanoTime() - lastFlushNanos >= FLUSH_INTERVAL_NANOS) {
            flush();
        }
    }

    /** Quiet market: the ring was idle for a second, so push whatever is buffered to disk. */
    @Override
    public void onTimeout(long sequence) throws IOException {
        flush();
    }

    /** Called once when FrameBus shuts down, after every frame already in the ring was handled. */
    @Override
    public void onShutdown() {
        try {
            closeCurrent();
        } catch (IOException e) {
            log.error("Failed to close {}", currentPath, e);
        }
    }

    private void flush() throws IOException {
        if (current != null) {
            current.flush();
        }
        lastFlushNanos = System.nanoTime();
    }

    private void closeCurrent() throws IOException {
        if (current == null) {
            return;
        }
        current.close();
        log.info("Closed {} ({} frames)", currentPath, framesInFile);
        current = null;
        currentPath = null;
        currentDay = null;
        framesInFile = 0;
    }
}