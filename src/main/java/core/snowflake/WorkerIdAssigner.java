package core.snowflake;

import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Reads the snowflake workerId (0-1023) from a machine-local config file.
 * The file path is configured via {@code leaf.snowflake.worker-id-file}.
 * The file must contain a single integer on one line.
 */
@Slf4j
@Component
public class WorkerIdAssigner {

    @Value("${leaf.snowflake.worker-id-file}")
    private String workerIdFilePath;

    @Value("${leaf.snowflake.worker-id:-1}")
    private long workerId;

    public long assign() throws IOException {
        long localId = -1;
        if (workerId >= 0) {
            localId = workerId;
        } else if (StringUtils.isNotBlank(workerIdFilePath)) {
            String content = Files.readString(Paths.get(workerIdFilePath)).trim();
            try {
                localId = Long.parseLong(content);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Worker ID file contains invalid value: '" + content + "'", e);
            }
        }

        if (localId < 0 || localId > 1023) {
            throw new IllegalArgumentException(
                    "workerId must be 0-1023, got: " + localId);
        }
        log.info("Snowflake workerId={} loaded from {}", localId, workerIdFilePath);
        return localId;
    }
}
