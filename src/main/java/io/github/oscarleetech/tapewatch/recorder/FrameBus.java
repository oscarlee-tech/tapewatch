package io.github.oscarleetech.tapewatch.recorder;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fans out every WebSocket frame to every handler through one Disruptor ring.
 * A frame is either seen by all handlers or by none, so the recording and the live pipeline
 * always see the same frames in the same order.
 * Any Spring bean that implements {@code EventHandler<FrameEvent>} is subscribed automatically.
 */
@Component
public class FrameBus {

    private static final Logger log = LoggerFactory.getLogger(FrameBus.class);

    private final Disruptor<FrameEvent> disruptor;
    private final AtomicLong dropped = new AtomicLong();

    @SuppressWarnings("unchecked")
    public FrameBus(@Value("${recorder.bus.ring-size:262144}") int ringSize,
                    List<EventHandler<FrameEvent>> handlers) {
        disruptor = new Disruptor<>(
                FrameEvent::new,
                ringSize,                                                  // must be a power of 2
                Thread.ofPlatform().daemon(true).name("frame-bus-", 0).factory(),
                ProducerType.MULTI,                                        // two WebSocket connections publish
                new TimeoutBlockingWaitStrategy(1, TimeUnit.SECONDS));     // idle handlers get onTimeout every 1s

        // The default handler stops the handler thread on the first exception. Log and keep going instead.
        disruptor.setDefaultExceptionHandler(new ExceptionHandler<>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, FrameEvent event) {
                log.error("Frame handler failed at seq {}", sequence, ex);
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                log.error("Frame handler failed to start", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                log.error("Frame handler failed to shut down", ex);
            }
        });

        disruptor.handleEventsWith(handlers.toArray(new EventHandler[0]));
        disruptor.start();
        log.info("Frame bus started: ringSize={}, handlers={}", ringSize, handlers.size());
    }

    /**
     * Called from WebSocket threads. Never blocks: if the ring is full the frame is dropped and counted.
     * The ring sequence becomes the frame's seq, so seq is global across both connections.
     */
    public boolean publish(String connection, Instant receivedAt, String raw) {
        boolean published = disruptor.getRingBuffer().tryPublishEvent(
                (event, sequence, conn, time, text) -> event.set(new RawFrame(sequence, time, conn, text)),
                connection, receivedAt, raw);

        if (!published) {
            long total = dropped.incrementAndGet();
            if (total == 1 || total % 1_000 == 0) {
                log.error("Ring buffer full, frames dropped so far: {}", total);
            }
        }
        return published;
    }

    /** Lets handlers finish what is already in the ring, then stops their threads. */
    @PreDestroy
    public void shutdown() {
        try {
            disruptor.shutdown(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Frame bus did not drain in 5s, halting");
            disruptor.halt();
        }
    }
}