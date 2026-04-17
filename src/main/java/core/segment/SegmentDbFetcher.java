package core.segment;

import core.segment.entity.LeafAlloc;
import core.segment.repository.LeafAllocRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Performs the atomic fetch of a new segment from the database.
 *
 * <p>This is a separate bean (not a private method on {@code SegmentIdGenerate}) so that
 * Spring's {@code @Transactional} proxy intercepts the call correctly — self-invocation
 * via {@code this.method()} would bypass the proxy and skip the transaction boundary.
 *
 * <p>Protocol (single transaction):
 * <ol>
 *   <li>UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = ?</li>
 *   <li>SELECT the updated row (the SELECT sees its own UPDATE)</li>
 * </ol>
 * The returned {@code maxId} is the new upper bound; the segment range is
 * {@code [maxId - step, maxId)}.
 */
@Service
@Transactional
public class SegmentDbFetcher {

    @Autowired
    private LeafAllocRepository repo;

    public LeafAlloc updateAndGet(String bizTag) {
        repo.updateMaxId(bizTag);
        return repo.findByBizTag(bizTag)
                .orElseThrow(() -> new IllegalArgumentException("Unknown biz_tag: " + bizTag));
    }
}
