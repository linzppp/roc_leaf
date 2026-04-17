package core.snowflake;

import core.IdGenerate;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Snowflake ID generator.
 *
 * <p>64-bit layout:
 * <pre>
 *  Bit 63     : 0  (sign, always positive)
 *  Bits 62-22 : 41 bits — milliseconds since EPOCH (2021-01-01 UTC)
 *  Bits 21-12 : 10 bits — workerId (0-1023), read from machine config file
 *  Bits 11-0  : 12 bits — per-ms sequence (0-4095)
 * </pre>
 *
 * <p>Max throughput per worker: 4096 IDs/ms (~4M IDs/s).
 * Valid until ~2090.
 */
@Slf4j
@Service("snowflakeIdGenerate")
public class SnowflakeIdGenerate implements IdGenerate {

    private static final long EPOCH           = 1609459200000L; // 2021-01-01 00:00:00 UTC
    private static final int  WORKER_BITS     = 10;
    private static final int  SEQUENCE_BITS   = 12;
    private static final long SEQUENCE_MASK   = (1L << SEQUENCE_BITS) - 1; // 4095
    private static final int  WORKER_SHIFT    = SEQUENCE_BITS;              // 12
    private static final int  TIMESTAMP_SHIFT = WORKER_BITS + SEQUENCE_BITS; // 22

    @Autowired
    private WorkerIdAssigner workerIdAssigner;

    private long workerId;
    private long sequence = 0L;
    private long lastMs   = -1L;

    @PostConstruct
    @Override
    public Boolean init() {
        try {
            workerId = workerIdAssigner.assign();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read worker ID file", e);
        }
        log.info("SnowflakeIdGenerate initialized with workerId={}", workerId);
        return true;
    }

    /**
     * Generates a globally unique 64-bit snowflake ID.
     * The {@code key} parameter is not used; it exists only to satisfy the {@link IdGenerate} contract.
     */
    @Override
    public synchronized Long getId(String key) {
        long now = System.currentTimeMillis();

        if (now < lastMs) {
            throw new RuntimeException(
                    "Clock moved backwards by " + (lastMs - now) + "ms, refusing to generate ID");
        }

        if (now == lastMs) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // Sequence overflow in this millisecond — wait for the next one
                now = waitForNextMs(lastMs);
            }
        } else {
            sequence = 0L;
        }

        lastMs = now;

        return ((now - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_SHIFT)
                | sequence;
    }

    private long waitForNextMs(long lastMs) {
        long ms = System.currentTimeMillis();
        while (ms <= lastMs) {
            ms = System.currentTimeMillis();
        }
        return ms;
    }
}
