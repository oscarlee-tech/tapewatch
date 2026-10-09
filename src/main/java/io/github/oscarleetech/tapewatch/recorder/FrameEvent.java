package io.github.oscarleetech.tapewatch.recorder;

/**
 * One slot of the ring buffer. Slots are reused for later frames,
 * so handlers must take {@link #frame()} out and never keep the slot itself.
 */
public final class FrameEvent {

    private RawFrame frame;

    public RawFrame frame() {
        return frame;
    }

    void set(RawFrame frame) {
        this.frame = frame;
    }
}