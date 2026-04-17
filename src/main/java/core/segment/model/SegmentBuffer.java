package core.segment.model;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Double-buffer container for one biz_tag.
 *
 * <p>State flags:
 * <ul>
 *   <li>{@code currentIndex} – which of the two slots is active (0 or 1)</li>
 *   <li>{@code nextReady}    – true once the background loader has finished writing the next slot</li>
 *   <li>{@code preloadPending} – CAS guard; ensures at most one async submission per slot cycle</li>
 * </ul>
 *
 * <p>Lock discipline:
 * <ul>
 *   <li>Slot switch and the "nextReady" flag mutation are guarded by {@code lock}.</li>
 *   <li>{@code segmentLoadedCondition} is used by the async loader to wake threads
 *       that are spin-waiting for the next slot.</li>
 * </ul>
 */
@Getter
@Setter
public class SegmentBuffer {
    private String key;
    private final Segment[] segments;
    private volatile int currentIndex;
    private volatile boolean nextReady;
    private final AtomicBoolean preloadPending;
    private final ReentrantLock lock;
    private final Condition segmentLoadedCondition;

    public SegmentBuffer() {
        segments = new Segment[]{new Segment(), new Segment()};
        currentIndex = 0;
        nextReady = false;
        preloadPending = new AtomicBoolean(false);
        lock = new ReentrantLock();
        segmentLoadedCondition = lock.newCondition();
    }

    public int nextIndex() {
        return (currentIndex + 1) % 2;
    }

    public Segment current() {
        return segments[currentIndex];
    }

    public Segment next() {
        return segments[nextIndex()];
    }
}
