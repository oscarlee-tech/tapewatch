package io.github.oscarleetech.tapewatch.recorder;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class FrameBusTest {

    @Test
    void everyHandlerSeesEveryFrameInTheSameOrder() throws InterruptedException {
        List<RawFrame> writerSeen = Collections.synchronizedList(new ArrayList<>());
        List<RawFrame> pipelineSeen = Collections.synchronizedList(new ArrayList<>());
        FrameBus bus = new FrameBus(1 << 15, List.of(
                (event, sequence, endOfBatch) -> writerSeen.add(event.frame()),
                (event, sequence, endOfBatch) -> pipelineSeen.add(event.frame())));

        // Two "WebSocket connections" publishing at the same time
        Thread gainers = Thread.ofPlatform().start(() -> publishMany(bus, "GAINERS", 10_000));
        Thread losers = Thread.ofPlatform().start(() -> publishMany(bus, "LOSERS", 10_000));
        gainers.join();
        losers.join();
        bus.shutdown();   // waits until both handlers have read everything in the ring

        assertThat(writerSeen).hasSize(20_000);
        assertThat(pipelineSeen).containsExactlyElementsOf(writerSeen);   // same frames, same order
        assertThat(writerSeen).extracting(RawFrame::seq)
                .containsExactlyElementsOf(LongStream.range(0, 20_000).boxed().toList());

        // Each connection's own frames keep the order they were published in
        assertThat(writerSeen.stream().filter(f -> f.connection().equals("GAINERS")).map(RawFrame::raw))
                .containsExactlyElementsOf(LongStream.range(0, 10_000).mapToObj(i -> "GAINERS-" + i).toList());
    }

    @Test
    void fullRingDropsInsteadOfBlockingTheWebSocketThread() {
        CountDownLatch release = new CountDownLatch(1);
        FrameBus bus = new FrameBus(8, List.of((event, sequence, endOfBatch) -> release.await())); // stuck handler

        long start = System.nanoTime();
        int published = 0;
        for (int i = 0; i < 20; i++) {
            if (bus.publish("GAINERS", Instant.now(), "frame-" + i)) {
                published++;
            }
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        release.countDown();   // unstick the handler so shutdown can drain the ring
        bus.shutdown();

        assertThat(published).isEqualTo(8);          // the ring has 8 slots
        assertThat(elapsedMs).isLessThan(1_000);     // never waited for the stuck handler
    }

    private static void publishMany(FrameBus bus, String connection, int count) {
        for (int i = 0; i < count; i++) {
            bus.publish(connection, Instant.now(), connection + "-" + i);
        }
    }
}