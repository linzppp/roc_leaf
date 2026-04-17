package core.segment;

import core.segment.entity.LeafAlloc;
import core.segment.repository.LeafAllocRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SegmentIdGenerateTest {

    @Mock
    private LeafAllocRepository repo;

    @Mock
    private SegmentDbFetcher dbFetcher;

    @InjectMocks
    private SegmentIdGenerate generate;

    private static final String KEY = "test_key";
    private static final int STEP = 1000;

    /** Simulates the DB state: each updateAndGet call advances max_id by STEP. */
    private final AtomicLong dbMaxId = new AtomicLong(0);

    @BeforeEach
    void setUp() {
        // repo.findAll() tells init() which keys exist
        LeafAlloc seed = new LeafAlloc();
        seed.setBizTag(KEY);
        seed.setStep(STEP);
        when(repo.findAll()).thenReturn(List.of(seed));

        // Each DB fetch advances max_id by step; segment range = [prev, newMax)
        when(dbFetcher.updateAndGet(anyString())).thenAnswer(inv -> {
            long newMax = dbMaxId.addAndGet(STEP);
            LeafAlloc alloc = new LeafAlloc();
            alloc.setBizTag(KEY);
            alloc.setMaxId(newMax);
            alloc.setStep(STEP);
            return alloc;
        });

        generate.init();
        // After init: first segment loaded, dbMaxId = 1000, range [0, 1000)
    }

    // -------------------------------------------------------------------------
    // Basic correctness
    // -------------------------------------------------------------------------

    @Test
    void testSequentialIds_strictlyIncreasing() {
        long prev = -1;
        for (int i = 0; i < 100; i++) {
            long id = generate.getId(KEY);
            assertTrue(id > prev, "Expected " + id + " > " + prev);
            prev = id;
        }
    }

    @Test
    void testGetId_unknownKey_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> generate.getId("no_such_key"));
    }

    // -------------------------------------------------------------------------
    // Buffer switch
    // -------------------------------------------------------------------------

    /**
     * Exhausts the first segment (1000 IDs) and generates some IDs from the second.
     * Verifies that IDs continue past the first segment boundary without gaps or resets.
     */
    @Test
    void testBufferSwitch_idsContinuePastFirstSegment() {
        long prev = -1;
        for (int i = 0; i < STEP + 100; i++) {
            long id = generate.getId(KEY);
            assertTrue(id > prev,
                    "ID regression at i=" + i + ": got " + id + ", previous was " + prev);
            prev = id;
        }
        // At least one ID must have come from the second segment
        assertTrue(prev >= STEP, "Expected IDs beyond first segment, last=" + prev);
    }

    /**
     * After a buffer switch the old slot must be cleared (value=0, max=0).
     * We verify indirectly: generating STEP*2+1 IDs forces two switches;
     * if old slots weren't cleared, the AtomicLong would overflow and we'd
     * see non-monotonic values.
     */
    @Test
    void testOldSlotClearedAfterSwitch_noRegression() {
        long prev = -1;
        for (int i = 0; i < STEP * 2 + 50; i++) {
            long id = generate.getId(KEY);
            assertTrue(id > prev, "ID regression after double switch at i=" + i);
            prev = id;
        }
    }

    // -------------------------------------------------------------------------
    // Concurrency — no duplicates
    // -------------------------------------------------------------------------

    /**
     * 50 threads × 1 000 calls = 50 000 IDs.
     * All must be distinct (no duplicates).
     */
    @Test
    void testNoDuplicates_concurrent() throws InterruptedException {
        int threads = 50;
        int perThread = 1000;
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    ready.await();
                    for (int j = 0; j < perThread; j++) {
                        ids.add(generate.getId(KEY));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        ready.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "Test timed out");
        assertEquals(threads * perThread, ids.size(),
                "Expected " + threads * perThread + " unique IDs, got " + ids.size());
    }
}
