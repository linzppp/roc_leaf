package core.segment;

import core.IdGenerate;
import core.segment.entity.LeafAlloc;
import core.segment.model.Segment;
import core.segment.model.SegmentBuffer;
import core.segment.repository.LeafAllocRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Segment (sequential) ID generator.
 *
 * <p>Design summary:
 * <ul>
 *   <li>One {@link SegmentBuffer} per biz_tag, stored in {@code cache}.</li>
 *   <li>Each buffer holds two {@link Segment} slots (double-buffer).
 *       One slot is active; the other is either being loaded or ready for the next switch.</li>
 *   <li>When a slot crosses 80% usage, a single-thread async loader pre-fetches the next slot.</li>
 *   <li>When a slot is exhausted (cursor ≥ max), the buffer switches to the pre-loaded slot.
 *       If the pre-loaded slot isn't ready yet, the switch thread waits via a {@code Condition}
 *       and falls back to a synchronous DB fetch only when no async load is in progress.</li>
 * </ul>
 *
 * <p>Thread safety:
 * <ul>
 *   <li>ID increment is lock-free via {@code AtomicLong.getAndIncrement()}.</li>
 *   <li>Slot switch is serialised by {@code SegmentBuffer.lock} (ReentrantLock).</li>
 *   <li>{@code preloadPending} (AtomicBoolean CAS) prevents duplicate async submissions.</li>
 * </ul>
 */
@Slf4j
@Service("segmentIdGenerate")
public class SegmentIdGenerate implements IdGenerate {

    @Autowired
    private LeafAllocRepository repo;

    @Autowired
    private SegmentDbFetcher dbFetcher;

    /** One SegmentBuffer per biz_tag row in leaf_alloc. Populated once in init(), read-only after. */
    private final Map<String, SegmentBuffer> cache = new ConcurrentHashMap<>();

    /** Single-thread executor for async preloads — sequential execution prevents concurrent slot writes. */
    private ExecutorService asyncLoader;

    @PostConstruct
    @Override
    public Boolean init() {
        log.info("Initializing SegmentIdGenerate...");
        asyncLoader = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "segment-async-loader");
            t.setDaemon(true);
            return t;
        });

        List<LeafAlloc> allocs = repo.findAll();
        for (LeafAlloc alloc : allocs) {
            String key = alloc.getBizTag();
            SegmentBuffer buffer = new SegmentBuffer();
            buffer.setKey(key);
            loadSegment(buffer.current(), key);
            cache.put(key, buffer);
            log.info("Loaded segment for key={}, range=[{}, {})",
                    key,
                    buffer.current().getMax() - buffer.current().getStep(),
                    buffer.current().getMax());
        }
        log.info("SegmentIdGenerate initialized with {} keys", cache.size());
        return true;
    }

    @Override
    public Long getId(String key) {
        SegmentBuffer buffer = cache.get(key);
        if (buffer == null) {
            throw new IllegalArgumentException("Unknown biz_tag: " + key);
        }
        return getIdFromBuffer(buffer, key);
    }

    // -------------------------------------------------------------------------
    // Core ID generation loop
    // -------------------------------------------------------------------------

    private Long getIdFromBuffer(SegmentBuffer buffer, String key) {
        while (true) {
            Segment seg = buffer.current();
            long id = seg.getValue().getAndIncrement();

            if (id < seg.getMax()) {
                // 80% used → ≤ 20% remaining: trigger async preload of the next slot
                if (seg.getMax() - id <= seg.getStep() * 0.2
                        && buffer.getPreloadPending().compareAndSet(false, true)) {
                    submitAsyncPreload(buffer, key);
                }
                return id;
            }

            // Current slot is exhausted — wait if needed, then switch
            waitAndSwitch(buffer, seg, key);
            // Loop: re-read buffer.current() after switch
        }
    }

    // -------------------------------------------------------------------------
    // Slot switch (called when current slot is exhausted)
    // -------------------------------------------------------------------------

    /**
     * Acquires the buffer lock, ensures the next slot is loaded, then switches.
     *
     * <p>Double-check on entry: if another thread already switched (buffer.current() != exhaustedSeg),
     * returns immediately so the caller re-reads the new current slot.
     */
    private void waitAndSwitch(SegmentBuffer buffer, Segment exhaustedSeg, String key) {
        buffer.getLock().lock();
        try {
            // Double-check: another thread may have already performed the switch
            if (buffer.current() != exhaustedSeg) {
                return;
            }

            if (!buffer.isNextReady()) {
                if (!buffer.getPreloadPending().compareAndSet(false, true)) {
                    // An async load is in progress; wait for it to signal completion
                    while (!buffer.isNextReady()) {
                        try {
                            buffer.getSegmentLoadedCondition().await(10, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while waiting for segment load", e);
                        }
                    }
                } else {
                    // No async load running; load synchronously (preloadPending now owned by us)
                    try {
                        loadSegment(buffer.next(), key);
                        buffer.setNextReady(true);
                        log.warn("Synchronous fallback load triggered for key={}", key);
                    } catch (Exception e) {
                        buffer.getPreloadPending().set(false);
                        throw new RuntimeException("Failed to load segment for key: " + key, e);
                    }
                }
            }

            // Switch: make the next slot active
            int oldIndex = buffer.getCurrentIndex();
            buffer.setCurrentIndex(buffer.nextIndex());

            // Clear the old slot so it can be reused in the next preload cycle
            Segment old = buffer.getSegments()[oldIndex];
            old.getValue().set(0);
            old.setMax(0);
            old.setStep(0);

            buffer.setNextReady(false);
            buffer.getPreloadPending().set(false);

        } finally {
            buffer.getLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Async preload
    // -------------------------------------------------------------------------

    private void submitAsyncPreload(SegmentBuffer buffer, String key) {
        asyncLoader.submit(() -> {
            try {
                // Capture next() now; if a switch happened first, nextIndex() still points
                // to the slot that needs to be filled for the current active segment.
                Segment nextSeg = buffer.next();
                loadSegment(nextSeg, key);

                // Signal under lock so waitAndSwitch() can wake up
                buffer.getLock().lock();
                try {
                    buffer.setNextReady(true);
                    buffer.getSegmentLoadedCondition().signalAll();
                } finally {
                    buffer.getLock().unlock();
                }
            } catch (Exception e) {
                log.error("Async preload failed for key={}", key, e);
                // Reset flag so the next 80%-crossing can retry
                buffer.getPreloadPending().set(false);
            }
        });
    }

    // -------------------------------------------------------------------------
    // DB fetch
    // -------------------------------------------------------------------------

    /**
     * Fetches a new segment from the DB and writes it into {@code segment}.
     * Segment range after return: [maxId - step, maxId).
     */
    private void loadSegment(Segment segment, String key) {
        LeafAlloc alloc = dbFetcher.updateAndGet(key);
        int step = alloc.getStep();
        if (step <= 0) {
            throw new IllegalStateException("Invalid step=" + step + " for key=" + key);
        }
        segment.setStep(step);
        segment.setMax(alloc.getMaxId());
        segment.getValue().set(alloc.getMaxId() - step);
    }
}
