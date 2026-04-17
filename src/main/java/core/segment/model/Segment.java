package core.segment.model;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One slot in the double-buffer.
 * Covers ID range [max - step, max).
 * value is the cursor; IDs are issued by getAndIncrement() until value >= max.
 */
@Getter
@Setter
public class Segment {
    private final AtomicLong value = new AtomicLong(0);
    private volatile long max;
    private volatile int step;

    public long remaining() {
        return max - value.get();
    }
}
