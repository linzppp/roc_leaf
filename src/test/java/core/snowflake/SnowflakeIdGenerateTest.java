package core.snowflake;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnowflakeIdGenerateTest {

    @Mock
    private WorkerIdAssigner workerIdAssigner;

    @InjectMocks
    private SnowflakeIdGenerate generate;

    private static final long WORKER_ID = 5L;
    private static final long EPOCH     = 1609459200000L; //

    @BeforeEach
    void setUp() throws IOException {
        when(workerIdAssigner.assign()).thenReturn(WORKER_ID);
        generate.init();
    }

    // -------------------------------------------------------------------------
    // Basic correctness
    // -------------------------------------------------------------------------

    @Test
    void testAllIdsPositive() {
        for (int i = 0; i < 100; i++) {
            long id = generate.getId("any");
            assertTrue(id > 0, "ID should be positive: " + id);
        }
    }

    @Test
    void testSequentialIds_strictlyIncreasing() {
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < 100; i++) {
            long id = generate.getId("any");
            assertTrue(id > prev, "Expected " + id + " > " + prev + " at i=" + i);
            prev = id;
        }
    }

    // -------------------------------------------------------------------------
    // Bit-field extraction
    // -------------------------------------------------------------------------

    /**
     * Verifies that the workerId is embedded in bits 21-12 (10 bits).
     */
    @Test
    void testWorkerIdEmbeddedInBits() {
        long id = generate.getId("any");
        long extractedWorkerId = (id >> 12) & 0x3FFL;
        assertEquals(WORKER_ID, extractedWorkerId,
                "workerId not correctly embedded. id=" + Long.toBinaryString(id));
    }

    /**
     * Verifies that the timestamp embedded in bits 62-22 (41 bits) falls within the
     * window [before, after] of the actual generation time.
     */
    @Test
    void testTimestampEmbeddedInBits() {
        long before = System.currentTimeMillis();
        long id = generate.getId("any");
        long after = System.currentTimeMillis();

        long embeddedTs = (id >> 22) + EPOCH;
        assertTrue(embeddedTs >= before && embeddedTs <= after,
                "Embedded timestamp " + embeddedTs + " not in [" + before + ", " + after + "]");
    }

    /**
     * Verifies that the sequence field (bits 11-0, 12 bits) is in range [0, 4095].
     */
    @Test
    void testSequenceFieldInRange() {
        for (int i = 0; i < 200; i++) {
            long id = generate.getId("any");
            long seq = id & 0xFFFL;
            assertTrue(seq >= 0 && seq <= 4095, "Sequence out of range: " + seq);
        }
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    /**
     * Simulates a clock going backwards by setting lastMs to a future value via reflection.
     * Expects a RuntimeException.
     */
    @Test
    void testClockBackwards_throwsRuntimeException() throws Exception {
        Field lastMsField = SnowflakeIdGenerate.class.getDeclaredField("lastMs");
        lastMsField.setAccessible(true);
        lastMsField.set(generate, System.currentTimeMillis() + 10_000L); // 10 s in the future

        assertThrows(RuntimeException.class, () -> generate.getId("any"),
                "Expected RuntimeException when clock moves backwards");
    }

    /**
     * Verifies that the sequence resets to 0 on a new millisecond.
     * Sets lastMs to a past value (forcing a new-ms branch) and checks sequence bits.
     */
    @Test
    void testSequenceResets_onNewMillisecond() throws Exception {
        Field lastMsField = SnowflakeIdGenerate.class.getDeclaredField("lastMs");
        Field seqField    = SnowflakeIdGenerate.class.getDeclaredField("sequence");
        lastMsField.setAccessible(true);
        seqField.setAccessible(true);

        // Force a new-millisecond scenario
        seqField.set(generate, 42L);
        lastMsField.set(generate, System.currentTimeMillis() - 100L);

        long id = generate.getId("any");
        long seq = id & 0xFFFL;
        assertEquals(0L, seq, "Sequence should reset to 0 on a new millisecond");
    }

    // -------------------------------------------------------------------------
    // Concurrency — no duplicates
    // -------------------------------------------------------------------------

    /**
     * 10 threads × 1 000 calls = 10 000 IDs.
     * getId is synchronized, so this exercises the lock path from multiple threads.
     * All IDs must be distinct.
     */
    @Test
    void testNoDuplicates_concurrent() throws InterruptedException {
        int threads = 10;
        int perThread = 1000;
        Set<Long> ids = ConcurrentHashMap.newKeySet();
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                try {
                    ready.await();
                    for (int j = 0; j < perThread; j++) {
                        ids.add(generate.getId("any"));
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
